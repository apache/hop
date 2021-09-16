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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.neo4j.core.data.GraphData;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.apache.hop.neo4j.core.value.ValueMetaGraph;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.json.simple.JSONValue;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.ResultSummary;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Cypher extends BaseTransform<CypherMeta, CypherData>
    implements ITransform<CypherMeta, CypherData> {

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
    data.hasInput = transform != null && transform.size() > 0;

    // Connect to Neo4j
    //
    if (StringUtils.isEmpty(resolve(meta.getConnectionName()))) {
      log.logError("You need to specify a Neo4j connection to use in this transform");
      return false;
    }
    try {
      data.neoConnection =
          metadataProvider
              .getSerializer(NeoConnection.class)
              .load(resolve(meta.getConnectionName()));
      if (data.neoConnection == null) {
        log.logError(
            "Connection '"
                + resolve(meta.getConnectionName())
                + "' could not be found in the metadata: "
                + metadataProvider.getDescription());
        return false;
      }
    } catch (HopException e) {
      log.logError(
          "Could not gencsv Neo4j connection '"
              + resolve(meta.getConnectionName())
              + "' from the metastore",
          e);
      return false;
    }

    data.batchSize = Const.toLong(resolve(meta.getBatchSize()), 1);

    // Try at least once and then do retries as needed
    //
    int retries = Const.toInt(resolve(meta.getNrRetriesOnError()), 0);
    if (retries < 0) {
      log.logError("The number of retries on an error should be larger than 0, not " + retries);
      return false;
    }
    data.attempts = 1 + retries;

    try {
      createDriverSession();
    } catch (Exception e) {
      log.logError(
          "Unable to get or create Neo4j database driver for database '"
              + data.neoConnection.getName()
              + "'",
          e);
      return false;
    }

    return super.init();
  }

  @Override
  public void dispose() {

    wrapUpTransaction();
    closeSessionDriver();

    super.dispose();
  }

  private void closeSessionDriver() {
    if (data.session != null) {
      data.session.close();
    }
    if (data.driver != null) {
      data.driver.close();
    }
  }

  private void createDriverSession() {
    data.driver = data.neoConnection.getDriver(log, this);
    data.session = data.neoConnection.getSession(log, data.driver, this);
  }

  private void reconnect() {
    closeSessionDriver();

    log.logBasic("RECONNECTING to database");

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
      log.logDetailed("Cypher statement from field is: " + data.cypher);
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

    if (data.cypherStatements == null || data.cypherStatements.size() == 0) {
      // Nothing to see here, move along
      return;
    }

    // Execute all the statements in there in one transaction...
    //
    TransactionWork<Integer> transactionWork =
        transaction -> {
          for (CypherStatement cypherStatement : data.cypherStatements) {
            Result result =
                transaction.run(cypherStatement.getCypher(), cypherStatement.getParameters());
            try {
              getResultRows(result, cypherStatement.getRow(), false);
            } catch (Exception e) {
              throw new RuntimeException(
                  "Error parsing result of cypher statement '" + cypherStatement.getCypher() + "'",
                  e);
            }
          }

          return data.cypherStatements.size();
        };

    try {
      int nrProcessed = 0;
      for (int attempt = 0; attempt < data.attempts; attempt++) {
        try {
          if (meta.isReadOnly()) {
            nrProcessed = data.session.readTransaction(transactionWork);
            setLinesInput(getLinesInput() + data.cypherStatements.size());
          } else {
            nrProcessed = data.session.writeTransaction(transactionWork);
            setLinesOutput(getLinesOutput() + data.cypherStatements.size());
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

      if (log.isDebug()) {
        logDebug("Processed " + nrProcessed + " statements");
      }

      // Clear out the batch of statements.
      //
      data.cypherStatements.clear();

    } catch (Exception e) {
      throw new HopException(
          "Unable to execute batch of cypher statements (" + data.cypherStatements.size() + ")", e);
    }
  }

  private List<Object[]> writeUnwindList() throws HopException {
    HashMap<String, Object> unwindMap = new HashMap<>();
    unwindMap.put(data.unwindMapName, data.unwindList);
    List<Object[]> resultRows = null;
    CypherTransactionWork cypherTransactionWork =
        new CypherTransactionWork(this, new Object[0], true, data.cypher, unwindMap);

    try {
      for (int attempt = 0; attempt < data.attempts; attempt++) {
        if (attempt > 0) {
          log.logBasic("Attempt #" + (attempt + 1) + "/" + data.attempts + " on Neo4j transaction");
        }
        try {
          if (meta.isReadOnly()) {
            data.session.readTransaction(cypherTransactionWork);
          } else {
            data.session.writeTransaction(cypherTransactionWork);
          }
          // Stop the attempts now
          //
          break;
        } catch (Exception e) {
          if (attempt + 1 >= data.attempts) {
            throw e;
          } else {
            log.logBasic(
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
          data.session.readTransaction(cypherTransactionWork);
        } else {
          data.session.writeTransaction(cypherTransactionWork);
        }
      } else {
        throw e;
      }

    } catch (Exception e) {
      data.session.close();
      stopAll();
      setErrors(1L);
      setOutputDone();
      throw new HopException("Unexpected error writing unwind list to Neo4j", e);
    }
    setLinesOutput(getLinesOutput() + data.unwindList.size());
    data.unwindList.clear();
    data.outputCount = 0;
    return resultRows;
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
        putRow(data.outputRowMeta, outputRowData);

      } else {
        // Are we returning values?
        //
        if (meta.getReturnValues().isEmpty()) {
          // If we're not returning any values then we simply need to pass the input rows without
          // We're consuming any optional results below
          //
          putRow(data.outputRowMeta, row);
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
              Object value = null;
              GraphPropertyDataType neoType = data.returnSourceTypeMap.get(returnValue.getName());
              if (recordValue != null && !recordValue.isNull()) {
                try {
                  switch (targetValueMeta.getType()) {
                    case IValueMeta.TYPE_STRING:
                      value = convertToString(recordValue, neoType);
                      break;
                    case ValueMetaGraph.TYPE_GRAPH:
                      // This is for Node, Path and Relationship
                      value = convertToGraphData(recordValue, neoType);
                      break;
                    case IValueMeta.TYPE_INTEGER:
                      value = recordValue.asLong();
                      break;
                    case IValueMeta.TYPE_NUMBER:
                      value = recordValue.asDouble();
                      break;
                    case IValueMeta.TYPE_BOOLEAN:
                      value = recordValue.asBoolean();
                      break;
                    case IValueMeta.TYPE_BIGNUMBER:
                      value = new BigDecimal(recordValue.asString());
                      break;
                    case IValueMeta.TYPE_DATE:
                      if (neoType != null) {
                        // Standard...
                        switch (neoType) {
                          case LocalDateTime:
                            {
                              LocalDateTime localDateTime = recordValue.asLocalDateTime();
                              value = java.sql.Date.valueOf(localDateTime.toLocalDate());
                              break;
                            }
                          case Date:
                            {
                              LocalDate localDate = recordValue.asLocalDate();
                              value = java.sql.Date.valueOf(localDate);
                              break;
                            }
                          case DateTime:
                            {
                              ZonedDateTime zonedDateTime = recordValue.asZonedDateTime();
                              value = Date.from(zonedDateTime.toInstant());
                              break;
                            }
                          default:
                            throw new HopException(
                                "Conversion from Neo4j daa type "
                                    + neoType.name()
                                    + " to a Hop Date isn't supported yet");
                        }
                      } else {
                        LocalDate localDate = recordValue.asLocalDate();
                        value = java.sql.Date.valueOf(localDate);
                      }
                      break;
                    case IValueMeta.TYPE_TIMESTAMP:
                      LocalDateTime localDateTime = recordValue.asLocalDateTime();
                      value = java.sql.Timestamp.valueOf(localDateTime);
                      break;
                    default:
                      throw new HopException(
                          "Unable to convert Neo4j data to type " + targetValueMeta.toStringMeta());
                  }
                } catch (Exception e) {
                  throw new HopException(
                      "Unable to convert Neo4j record value '"
                          + returnValue.getName()
                          + "' to type : "
                          + targetValueMeta.getTypeDesc(),
                      e);
                }
              }
              outputRow[index++] = value;
            }

            // Pass the rows to the next transform
            //
            putRow(data.outputRowMeta, outputRow);
          }
        }
      }

      // Now that all result rows are consumed we can evaluate the result summary.
      //
      if (processSummary(result)) {
        setErrors(1L);
        stopAll();
        setOutputDone();
        throw new HopException("Error found in executing cypher statement");
      }
    }
  }

  /**
   * Convert the given record value to String. For complex data types it's a conversion to JSON.
   *
   * @param recordValue The record value to convert to String
   * @param sourceType The Neo4j source type
   * @return The String value of the record value
   */
  private String convertToString(Value recordValue, GraphPropertyDataType sourceType) {
    if (recordValue == null) {
      return null;
    }
    if (sourceType == null) {
      return JSONValue.toJSONString(recordValue.asObject());
    }
    switch (sourceType) {
      case String:
        return recordValue.asString();
      case List:
        return JSONValue.toJSONString(recordValue.asList());
      case Map:
        return JSONValue.toJSONString(recordValue.asMap());
      case Node:
        {
          GraphData graphData = new GraphData();
          graphData.update(recordValue.asNode());
          return graphData.toJson().toJSONString();
        }
      case Path:
        {
          GraphData graphData = new GraphData();
          graphData.update(recordValue.asPath());
          return graphData.toJson().toJSONString();
        }
      default:
        return JSONValue.toJSONString(recordValue.asObject());
    }
  }

  /**
   * Convert the given record value to String. For complex data types it's a conversion to JSON.
   *
   * @param recordValue The record value to convert to String
   * @param sourceType The Neo4j source type
   * @return The String value of the record value
   */
  private GraphData convertToGraphData(Value recordValue, GraphPropertyDataType sourceType)
      throws HopException {
    if (recordValue == null) {
      return null;
    }
    if (sourceType == null) {
      throw new HopException(
          "Please specify a Neo4j source data type to convert to Graph.  NODE, RELATIONSHIP and PATH are supported.");
    }
    GraphData graphData;
    switch (sourceType) {
      case Node:
        graphData = new GraphData();
        graphData.update(recordValue.asNode());
        break;

      case Path:
        graphData = new GraphData();
        graphData.update(recordValue.asPath());
        break;

      default:
        throw new HopException(
            "We can only convert NODE, PATH and RELATIONSHIP source values to a Graph data type, not "
                + sourceType.name());
    }
    return graphData;
  }

  private boolean processSummary(Result result) {
    if (meta.isUsingUnwind()) {
      return false;
    } else {
      boolean error = false;
      ResultSummary summary = result.consume();
      for (Notification notification : summary.notifications()) {
        if ("WARNING".equalsIgnoreCase(notification.severity())) {
          // Log it
          log.logBasic(
              notification.severity()
                  + " : "
                  + notification.title()
                  + " : "
                  + notification.code()
                  + " : "
                  + notification.description()
                  + ", position "
                  + notification.position());
        } else {
          // This is an error
          //
          log.logError(notification.severity() + " : " + notification.title());
          log.logError(
              notification.code()
                  + " : "
                  + notification.description()
                  + ", position "
                  + notification.position());
          error = true;
        }
      }
      return error;
    }
  }

  @Override
  public void batchComplete() {
    try {
      wrapUpTransaction();
    } catch (Exception e) {
      setErrors(getErrors() + 1);
      stopAll();
      throw new RuntimeException("Unable to complete batch of records", e);
    }
  }

  private void wrapUpTransaction() {

    if (!isStopped()) {
      try {
        if (meta.isUsingUnwind() && data.unwindList != null) {
          if (data.unwindList.size() > 0) {
            writeUnwindList();
          }
        } else {
          // See if there are statements left to execute...
          //
          if (data.cypherStatements != null && data.cypherStatements.size() > 0) {
            runCypherStatementsBatch();
          }
        }
      } catch (Exception e) {
        setErrors(getErrors() + 1);
        stopAll();
        throw new RuntimeException("Unable to run batch of cypher statements", e);
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
