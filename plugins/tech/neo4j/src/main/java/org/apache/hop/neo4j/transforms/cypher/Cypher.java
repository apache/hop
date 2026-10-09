/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.neo4j.transforms.cypher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopConfigException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.neo4j.core.data.GraphData;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.apache.hop.neo4j.shared.NeoHopData;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

public class Cypher extends BaseTransform<CypherMeta, CypherData> {

  public Cypher(
      TransformMeta transformMeta,
      CypherMeta meta,
      CypherData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {

    // Is the transform getting input?
    //
    List<TransformMeta> transform = getPipelineMeta().findPreviousTransforms(getTransformMeta());
    data.hasInput = !Utils.isEmpty(transform);

    // Connect to Neo4j
    //
    if (StringUtils.isEmpty(resolve(meta.getConnectionName()))) {
      logError("You need to specify a Neo4j connection to use in this transform");
      return false;
    }
    try {
      NamedGraphConnection graphConnection =
          NeoConnectionUtils.findGraphConnection(
              metadataProvider, resolve(meta.getConnectionName()));
      if (graphConnection != null && !NeoConnectionUtils.isBolt(graphConnection)) {
        // Not Bolt: work through the generic graph connection
        //
        data.batchSize = Const.toLongExpanded(resolve(meta.getBatchSize()), 1);
        data.attempts = 1 + Math.max(0, Const.toInt(resolve(meta.getNrRetriesOnError()), 0));
        data.graphConnection = graphConnection.connect(getLogChannel(), this);
        int attempts =
            getAttempts(
                data.attempts, meta.isReadOnly(), data.graphConnection.isSupportingTransactions());
        if (attempts != data.attempts) {
          // Without transactions a failed attempt can leave part of its changes behind: retrying
          // it would apply them twice.
          //
          logBasic(
              "Warning: graph database connection '"
                  + graphConnection.name()
                  + "' doesn't support transactions: statements which change data are not retried"
                  + " on an error");
          data.attempts = attempts;
        }
        return super.init();
      }
      data.neoConnection =
          NeoConnectionUtils.loadConnection(metadataProvider, resolve(meta.getConnectionName()));
      if (data.neoConnection == null) {
        logError(
            "Connection '"
                + resolve(meta.getConnectionName())
                + "' could not be found in the metadata: "
                + metadataProvider.getDescription());
        return false;
      }
    } catch (HopException e) {
      logError(
          "Could not gencsv Neo4j connection '"
              + resolve(meta.getConnectionName())
              + "' from the metastore",
          e);
      return false;
    }

    data.batchSize = Const.toLongExpanded(resolve(meta.getBatchSize()), 1);

    // Try at least once and then do retries as needed
    //
    int retries = Const.toInt(resolve(meta.getNrRetriesOnError()), 0);
    if (retries < 0) {
      logError("The number of retries on an error should be larger than 0, not " + retries);
      return false;
    }
    data.attempts = 1 + retries;

    try {
      createDriverSession();
    } catch (Exception e) {
      logError(
          "Unable to get or create Neo4j database driver for database '"
              + data.neoConnection.getName()
              + "'",
          e);
      return false;
    }

    return super.init();
  }

  /**
   * The number of attempts to execute statements. Without transactions a failed attempt can leave
   * part of its changes behind, so statements which change data are tried once.
   *
   * @param attempts The configured number of attempts
   * @param readOnly True if the statements only read data
   * @param supportingTransactions True if the connection rolls back failed transactions
   * @return The number of attempts to use
   */
  public static int getAttempts(int attempts, boolean readOnly, boolean supportingTransactions) {
    if (attempts > 1 && !readOnly && !supportingTransactions) {
      return 1;
    }
    return attempts;
  }

  @Override
  public void dispose() {

    wrapUpTransaction();
    closeSessionDriver();

    super.dispose();
  }

  private void closeSessionDriver() {
    if (data.graphConnection != null) {
      try {
        data.graphConnection.close();
      } catch (HopException e) {
        logError("Error closing the graph database connection", e);
      }
      data.graphConnection = null;
    }
    if (data.session != null) {
      data.session.close();
    }
    if (data.driver != null) {
      data.driver.close();
    }
  }

  private void createDriverSession() throws HopConfigException {
    data.driver = data.neoConnection.getDriver(getLogChannel(), this);
    data.session = data.neoConnection.getSession(getLogChannel(), data.driver, this);
  }

  private void reconnect() throws HopConfigException {
    closeSessionDriver();

    if (isBasic()) {
      logBasic("RECONNECTING to database");
    }

    // Wait for 30 seconds before reconnecting.
    // Let's give the server a breath of fresh air.
    try {
      Thread.sleep(30000);
    } catch (InterruptedException e) {
      // ignore sleep interrupted.
    }

    createDriverSession();
  }

  @Override
  public boolean processRow() throws HopException {

    // Input row
    //
    Object[] row = new Object[0];

    // Only if we actually have previous transform to read from...
    // This way the transform also acts as an GraphOutput query transform
    //
    if (data.hasInput) {
      // Get a row of data from previous transform...
      //
      row = getRow();
      if (row == null) {

        // See if there's anything left to write...
        //
        wrapUpTransaction();

        // Signal next transform(s) we're done processing
        //
        setOutputDone();
        return false;
      }
    }

    if (first) {
      first = false;

      // get the output fields...
      //
      data.outputRowMeta = data.hasInput ? getInputRowMeta().clone() : new RowMeta();
      meta.getFields(
          data.outputRowMeta, getTransformName(), null, getTransformMeta(), this, metadataProvider);

      if (!meta.getParameterMappings().isEmpty() && getInputRowMeta() == null) {
        throw new HopException(
            "Please provide this transform with input if you want to set parameters");
      }
      data.fieldIndexes = new int[meta.getParameterMappings().size()];
      for (int i = 0; i < meta.getParameterMappings().size(); i++) {
        String field = meta.getParameterMappings().get(i).getField();
        data.fieldIndexes[i] = getInputRowMeta().indexOfValue(field);
        if (data.fieldIndexes[i] < 0) {
          throw new HopTransformException("Unable to find parameter field '" + field);
        }
      }

      data.cypherFieldIndex = -1;
      if (data.hasInput) {
        data.cypherFieldIndex = getInputRowMeta().indexOfValue(meta.getCypherField());
        if (meta.isCypherFromField() && data.cypherFieldIndex < 0) {
          throw new HopTransformException(
              "Unable to find cypher field '" + meta.getCypherField() + "'");
        }
      }
      data.cypher = resolve(meta.getCypher());

      data.unwindList = new ArrayList<>();
      data.unwindMapName = resolve(meta.getUnwindMapName());

      data.cypherStatements = new ArrayList<>();
    }

    if (meta.isCypherFromField()) {
      data.cypher = getInputRowMeta().getString(row, data.cypherFieldIndex);
      logDetailed("Cypher statement from field is: " + data.cypher);
    }

    // Do the value mapping and conversion to the parameters
    //
    Map<String, Object> parameters = new HashMap<>();
    for (int i = 0; i < meta.getParameterMappings().size(); i++) {
      ParameterMapping mapping = meta.getParameterMappings().get(i);
      IValueMeta valueMeta = getInputRowMeta().getValueMeta(data.fieldIndexes[i]);
      Object valueData = row[data.fieldIndexes[i]];
      GraphPropertyType propertyType = GraphPropertyType.parseCode(mapping.getNeoType());
      if (propertyType == null) {
        throw new HopException(
            "Unable to convert to unknown property type for field '"
                + valueMeta.toStringMeta()
                + "'");
      }
      Object neoValue = propertyType.convertFromHop(valueMeta, valueData);
      parameters.put(mapping.getParameter(), neoValue);
    }

    // Create a map between the return value and the source type so we can do the appropriate
    // mapping later...
    //
    data.returnSourceTypeMap = new HashMap<>();
    for (ReturnValue returnValue : meta.getReturnValues()) {
      if (StringUtils.isNotEmpty(returnValue.getSourceType())) {
        String name = returnValue.getName();
        GraphPropertyDataType type = GraphPropertyDataType.parseCode(returnValue.getSourceType());
        data.returnSourceTypeMap.put(name, type);
      }
    }

    if (meta.isUsingUnwind()) {
      data.unwindList.add(parameters);
      data.outputCount++;

      if (data.outputCount >= data.batchSize) {
        writeUnwindList();
      }
    } else {

      // Execute the cypher with all the parameters...
      //
      try {
        runCypherStatement(row, data.cypher, parameters);
      } catch (ServiceUnavailableException e) {
        // retry once after reconnecting.
        // This can fix certain time-out issues
        //
        if (meta.isRetryingOnDisconnect()) {
          reconnect();
          runCypherStatement(row, data.cypher, parameters);
        } else {
          throw e;
        }
      } catch (HopException e) {
        setErrors(1);
        stopAll();
        throw e;
      }
    }

    // Only keep executing if we have input rows...
    //
    if (data.hasInput) {
      return true;
    } else {
      setOutputDone();
      return false;
    }
  }

  private void runCypherStatement(Object[] row, String cypher, Map<String, Object> parameters)
      throws HopException {
    data.cypherStatements.add(new CypherStatement(row, cypher, parameters));
    if (data.cypherStatements.size() >= data.batchSize || !data.hasInput) {
      runCypherStatementsBatch();
    }
  }

  private void runCypherStatementsBatch() throws HopException {

    if (Utils.isEmpty(data.cypherStatements)) {
      // Nothing to see here, move along
      return;
    }

    if (data.graphConnection != null) {
      runGenericStatementsBatch();
      return;
    }

    // Statements the database doesn't run in a transaction, like SHOW INDEX INFO on Memgraph, run
    // on their own. The statements between them run in transactions, in the same order.
    //
    IGraphDialect dialect = data.neoConnection.getDialect();
    if (data.cypherStatements.stream()
        .anyMatch(statement -> dialect.isRequiringAutoCommit(statement.getCypher()))) {
      List<CypherStatement> statements = new ArrayList<>(data.cypherStatements);
      List<CypherStatement> inTransaction = new ArrayList<>();
      for (CypherStatement statement : statements) {
        if (dialect.isRequiringAutoCommit(statement.getCypher())) {
          runStatementsInTransaction(inTransaction);
          inTransaction.clear();
          runAutoCommitStatement(statement);
        } else {
          inTransaction.add(statement);
        }
      }
      runStatementsInTransaction(inTransaction);
      data.cypherStatements.clear();
      return;
    }

    runStatementsInTransaction(data.cypherStatements);
    data.cypherStatements.clear();
  }

  /** Run statements in one transaction, with the configured retries. */
  private void runStatementsInTransaction(List<CypherStatement> cypherStatements)
      throws HopException {
    if (cypherStatements.isEmpty()) {
      return;
    }

    beginWork();

    // Execute all the statements in there in one transaction...
    //
    TransactionCallback<Integer> transactionWork =
        transaction -> {
          // The driver can call this again on a transient error: drop the rows of the failed call
          startAttempt();
          for (CypherStatement cypherStatement : cypherStatements) {
            Result result =
                transaction.run(cypherStatement.getCypher(), cypherStatement.getParameters());
            try {
              getResultRows(result, cypherStatement.getRow(), false);
            } catch (Exception e) {
              throw new HopRuntimeException(
                  "Error parsing result of cypher statement '" + cypherStatement.getCypher() + "'",
                  e);
            }
          }

          return cypherStatements.size();
        };

    try {
      int nrProcessed = 0;
      for (int attempt = 0; attempt < data.attempts; attempt++) {
        try {
          if (meta.isReadOnly()) {
            nrProcessed = data.session.executeRead(transactionWork);
            setLinesInput(getLinesInput() + cypherStatements.size());
          } else {
            nrProcessed = data.session.executeWrite(transactionWork);
            setLinesOutput(getLinesOutput() + cypherStatements.size());
          }
          // If all went as expected we can stop retrying...
          //
          break;
        } catch (Exception e) {
          if (attempt + 1 >= data.attempts) {
            throw e;
          } else {
            logBasic("Retrying unwind after error: " + e.getMessage());
          }
        }
      }

      if (isDebug()) {
        logDebug("Processed " + nrProcessed + " statements");
      }

    } catch (Exception e) {
      dropAttemptRows();
      throw new HopException(
          "Unable to execute batch of cypher statements (" + cypherStatements.size() + ")", e);
    }
    flushOutputRows();
  }

  /** Run a statement on its own in an auto-commit transaction, with the configured retries. */
  private void runAutoCommitStatement(CypherStatement cypherStatement) throws HopException {
    beginWork();
    for (int attempt = 0; attempt < data.attempts; attempt++) {
      try {
        startAttempt();
        Result result =
            data.session.run(cypherStatement.getCypher(), cypherStatement.getParameters());
        getResultRows(result, cypherStatement.getRow(), false);
        if (meta.isReadOnly()) {
          incrementLinesInput();
        } else {
          incrementLinesOutput();
        }
        break;
      } catch (Exception e) {
        dropAttemptRows();
        if (attempt + 1 >= data.attempts) {
          throw new HopException(
              "Unable to execute cypher statement '" + cypherStatement.getCypher() + "'", e);
        }
        logBasic("Retrying after attempt #" + (attempt + 1) + " with error : " + e.getMessage());
      }
    }
    flushOutputRows();
  }

  private List<Object[]> writeUnwindList() throws HopException {
    if (data.graphConnection != null) {
      writeGenericUnwindList();
      return null;
    }
    HashMap<String, Object> unwindMap = new HashMap<>();
    unwindMap.put(data.unwindMapName, data.unwindList);
    List<Object[]> resultRows = null;
    CypherTransactionWork cypherTransactionWork =
        new CypherTransactionWork(this, new Object[0], true, data.cypher, unwindMap);

    beginWork();
    try {
      for (int attempt = 0; attempt < data.attempts; attempt++) {
        if (attempt > 0) {
          if (isBasic()) {
            logBasic("Attempt #" + (attempt + 1) + "/" + data.attempts + " on Neo4j transaction");
          }
        }
        try {
          if (meta.isReadOnly()) {
            data.session.executeRead(cypherTransactionWork);
          } else {
            data.session.executeWrite(cypherTransactionWork);
          }
          // Stop the attempts now
          //
          break;
        } catch (Exception e) {
          if (attempt + 1 >= data.attempts) {
            throw e;
          } else {
            logBasic(
                "Retrying transaction after attempt #"
                    + (attempt + 1)
                    + " with error : "
                    + e.getMessage());
          }
        }
      }
    } catch (ServiceUnavailableException e) {
      // retry once after reconnecting.
      // This can fix certain time-out issues
      //
      if (meta.isRetryingOnDisconnect()) {
        reconnect();
        if (meta.isReadOnly()) {
          data.session.executeRead(cypherTransactionWork);
        } else {
          data.session.executeWrite(cypherTransactionWork);
        }
      } else {
        throw e;
      }

    } catch (Exception e) {
      dropAttemptRows();
      data.session.close();
      stopAll();
      setErrors(1L);
      setOutputDone();
      throw new HopException("Unexpected error writing unwind list to Neo4j", e);
    }
    flushOutputRows();
    setLinesOutput(getLinesOutput() + data.unwindList.size());
    data.unwindList.clear();
    data.outputCount = 0;
    return resultRows;
  }

  /** Execute the batch of statements over a graph connection which isn't Bolt. */
  void runGenericStatementsBatch() throws HopException {
    executeGeneric(
        () -> {
          data.graphConnection.executeWrite(
              transaction -> {
                startAttempt();
                for (CypherStatement cypherStatement : data.cypherStatements) {
                  List<Map<String, Object>> rows =
                      transaction.execute(
                          cypherStatement.getCypher(), cypherStatement.getParameters());
                  getGenericResultRows(rows, cypherStatement.getRow(), false);
                }
                return null;
              });
          if (meta.isReadOnly()) {
            setLinesInput(getLinesInput() + data.cypherStatements.size());
          } else {
            setLinesOutput(getLinesOutput() + data.cypherStatements.size());
          }
        });
    flushOutputRows();
    data.cypherStatements.clear();
  }

  /** Execute the unwind statement over a graph connection which isn't Bolt. */
  private void writeGenericUnwindList() throws HopException {
    Map<String, Object> unwindMap = new HashMap<>();
    unwindMap.put(data.unwindMapName, data.unwindList);
    executeGeneric(
        () ->
            data.graphConnection.executeWrite(
                transaction -> {
                  startAttempt();
                  getGenericResultRows(
                      transaction.execute(data.cypher, unwindMap), new Object[0], true);
                  return null;
                }));
    flushOutputRows();
    setLinesOutput(getLinesOutput() + data.unwindList.size());
    data.unwindList.clear();
    data.outputCount = 0;
  }

  /** Work to retry the configured number of times. */
  @FunctionalInterface
  interface GenericWork {
    void execute() throws HopException;
  }

  /**
   * Execute work the configured number of times until it succeeds. The work starts with {@link
   * #startAttempt()}, so that only the output rows of the attempt which succeeded remain, to be
   * passed on with {@link #flushOutputRows()}. Without retries the rows can stream instead, see
   * {@link #isStreamingRows()}.
   */
  void executeGeneric(GenericWork work) throws HopException {
    beginWork();
    for (int attempt = 0; attempt < data.attempts; attempt++) {
      try {
        work.execute();
        return;
      } catch (HopException e) {
        dropAttemptRows();
        if (attempt + 1 >= data.attempts) {
          throw e;
        }
        logBasic("Retrying after attempt #" + (attempt + 1) + " with error : " + e.getMessage());
      }
    }
  }

  /**
   * Pass the result rows of a graph connection which isn't Bolt to the next transforms. The values
   * are plain Java values: they are converted to the types of the return values.
   */
  private void getGenericResultRows(List<Map<String, Object>> rows, Object[] row, boolean unwind)
      throws HopException {
    if (meta.isReturningGraph()) {
      // One row with the graph of all the nodes, relationships and paths in the results
      GraphData graphData = GraphData.fromRows(rows);
      graphData.setSourcePipelineName(getPipelineMeta().getName());
      graphData.setSourceTransformName(getTransformName());
      Object[] outputRow;
      if (unwind) {
        outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      } else {
        outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      }
      outputRow[data.hasInput && !unwind ? getInputRowMeta().size() : 0] = graphData;
      addOutputRow(outputRow);
      return;
    }
    if (meta.getReturnValues().isEmpty()) {
      if (!unwind) {
        addOutputRow(row);
      }
      return;
    }
    for (Map<String, Object> resultRow : rows) {
      Object[] outputRow;
      if (unwind) {
        outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      } else {
        outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      }
      int index = data.hasInput && !unwind ? getInputRowMeta().size() : 0;
      for (ReturnValue returnValue : meta.getReturnValues()) {
        IValueMeta targetValueMeta = data.outputRowMeta.getValueMeta(index);
        outputRow[index++] =
            NeoHopData.convertToHopValue(
                returnValue.getName(), resultRow.get(returnValue.getName()), targetValueMeta);
      }
      addOutputRow(outputRow);
    }
  }

  public void getResultRows(Result result, Object[] row, boolean unwind) throws HopException {

    if (result != null) {

      if (meta.isReturningGraph()) {

        GraphData graphData = new GraphData(result);
        graphData.setSourcePipelineName(getPipelineMeta().getName());
        graphData.setSourceTransformName(getTransformName());

        // Create output row
        Object[] outputRowData;
        if (unwind) {
          outputRowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
        } else {
          outputRowData = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
        }
        int index = data.hasInput && !unwind ? getInputRowMeta().size() : 0;

        outputRowData[index] = graphData;
        addOutputRow(outputRowData);

      } else {
        // Are we returning values?
        //
        if (meta.getReturnValues().isEmpty()) {
          // If we're not returning any values then we simply need to pass the input rows without
          // We're consuming any optional results below
          //
          addOutputRow(row);
        } else {
          // If we're returning values we pass all result records per input row.
          // This can be 0, 1 or more per input row
          //
          while (result.hasNext()) {
            Record record = result.next();

            // Create output row
            Object[] outputRow;
            if (unwind) {
              outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
            } else {
              outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
            }

            // add result values...
            //
            int index = data.hasInput && !unwind ? getInputRowMeta().size() : 0;
            for (ReturnValue returnValue : meta.getReturnValues()) {
              Value recordValue = record.get(returnValue.getName());
              IValueMeta targetValueMeta = data.outputRowMeta.getValueMeta(index);
              GraphPropertyDataType neoType = data.returnSourceTypeMap.get(returnValue.getName());
              Object value =
                  NeoHopData.convertNeoToHopValue(
                      returnValue.getName(), recordValue, neoType, targetValueMeta);

              outputRow[index++] = value;
            }

            // Pass the rows to the next transform once the transaction succeeded
            //
            addOutputRow(outputRow);
          }
        }
      }

      // Now that all result rows are consumed we can log the notifications of the result.
      //
      if (!meta.isUsingUnwind()) {
        NeoConnectionUtils.logNotifications(
            getLogChannel(), result.consume(), data.loggedNotifications);
      }
    }
  }

  /**
   * Whether the output rows of the work about to be executed are passed on right away, or kept in
   * memory until the work succeeded.
   *
   * <p>Work is executed again when it is retried after an error: by the configured retries
   * (attempts larger than 1), by the Neo4j driver on transient errors in managed transactions
   * (session.executeWrite/executeRead), by graph connections which retry in executeWrite, and after
   * reconnecting on a disconnect. Rows passed on by a failed execution can't be taken back, so a
   * retried execution would output them twice. Keeping them in memory until the work succeeded
   * avoids that, but a query returning millions of rows then has to fit in memory.
   *
   * <p>The rule:
   *
   * <ul>
   *   <li>With retries configured (attempts larger than 1), rows are kept until the attempt
   *       succeeded.
   *   <li>Without retries, rows stream when the statements only read data or when the transform has
   *       no input: the one statement without input can return any number of rows. A re-execution
   *       by the driver or after a reconnect is allowed as long as no row was passed on yet. After
   *       that it fails, see {@link #startAttempt()}.
   *   <li>Otherwise, statements which change data from input rows: rows are kept for each batch, so
   *       that the driver can still retry a write on a transient error, like a deadlock. The batch
   *       size limits the number of rows in memory.
   * </ul>
   */
  boolean isStreamingRows() {
    return isStreamingRows(data.attempts, meta.isReadOnly(), data.hasInput);
  }

  /**
   * @see #isStreamingRows()
   */
  static boolean isStreamingRows(int attempts, boolean readOnly, boolean hasInput) {
    return attempts <= 1 && (readOnly || !hasInput);
  }

  /**
   * Start executing work: decide whether its output rows stream, see {@link #isStreamingRows()}.
   */
  void beginWork() {
    data.streamingRows = isStreamingRows();
    data.streamedRows = 0;
    data.attemptRows.clear();
  }

  /**
   * Start an attempt to execute statements: the output rows of a previous attempt are dropped. When
   * rows stream and some were passed on already, the work can't be executed again without
   * outputting them twice: that fails.
   */
  public void startAttempt() {
    if (data.streamingRows && data.streamedRows > 0) {
      throw new HopRuntimeException(
          "The statements can't be executed again after an error: "
              + data.streamedRows
              + " result rows were passed on already. Set a number of retries on error to retry"
              + " them safely.");
    }
    data.attemptRows.clear();
  }

  /** Drop the output rows of an attempt which failed. */
  private void dropAttemptRows() {
    data.attemptRows.clear();
  }

  /**
   * Pass an output row on right away when rows stream. Otherwise keep it until the attempt
   * producing it succeeded.
   */
  private void addOutputRow(Object[] outputRow) throws HopTransformException {
    if (data.streamingRows) {
      data.streamedRows++;
      putRow(data.outputRowMeta, outputRow);
    } else {
      data.attemptRows.add(outputRow);
    }
  }

  /** Pass the output rows of the attempt which succeeded to the next transforms. */
  void flushOutputRows() throws HopTransformException {
    List<Object[]> rows = new ArrayList<>(data.attemptRows);
    data.attemptRows.clear();
    for (Object[] outputRow : rows) {
      putRow(data.outputRowMeta, outputRow);
    }
  }

  @Override
  public void batchComplete() throws HopException {
    try {
      wrapUpTransaction();
    } catch (Exception e) {
      setErrors(getErrors() + 1);
      stopAll();
      throw new HopException("Unable to complete batch of records", e);
    }
  }

  private void wrapUpTransaction() {

    if (!isStopped()) {
      try {
        if (meta.isUsingUnwind() && data.unwindList != null) {
          if (!data.unwindList.isEmpty()) {
            writeUnwindList();
          }
        } else {
          // See if there are statements left to execute...
          //
          if (!Utils.isEmpty(data.cypherStatements)) {
            runCypherStatementsBatch();
          }
        }
      } catch (Exception e) {
        setErrors(getErrors() + 1);
        stopAll();
        throw new HopRuntimeException("Unable to run batch of cypher statements", e);
      }
    }

    // At the end of each batch, do a commit.
    //
    if (data.outputCount > 0) {

      // With UNWIND we don't have to end a transaction
      //
      if (data.transaction != null) {
        if (getErrors() == 0) {
          data.transaction.commit();
        } else {
          data.transaction.rollback();
        }
        data.transaction.close();
      }
      data.outputCount = 0;
    }
  }
}
