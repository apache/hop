/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.neo4j.transforms.cypherbuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.shared.NamedGraphConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.apache.hop.neo4j.shared.NeoHopData;
import org.apache.hop.neo4j.transforms.cypher.Cypher;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.neo4j.driver.Result;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.Value;

public class CypherBuilder extends BaseTransform<CypherBuilderMeta, CypherBuilderData> {

  public CypherBuilder(
      TransformMeta transformMeta,
      CypherBuilderMeta meta,
      CypherBuilderData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    // Connect to Neo4j
    //
    String connectionName = resolve(meta.getConnectionName());
    try {
      NamedGraphConnection graphConnection =
          NeoConnectionUtils.findGraphConnection(metadataProvider, connectionName);
      if (graphConnection != null && !graphConnection.getDialect().isCypher()) {
        logError(
            "The Cypher builder generates Cypher, which connection '"
                + connectionName
                + "' doesn't speak. Use Graph query or Graph output instead.");
        return false;
      }
      if (graphConnection != null && !NeoConnectionUtils.isBolt(graphConnection)) {
        // Not Bolt: work through the generic graph connection
        //
        data.graphConnection = graphConnection.connect(getLogChannel(), this);
        return super.init();
      }
      data.connection = NeoConnectionUtils.loadConnection(metadataProvider, connectionName);
      data.driver = data.connection.getDriver(getLogChannel(), this);
      data.driver.verifyConnectivity();
      data.session = data.connection.getSession(getLogChannel(), data.driver, this);
    } catch (Exception e) {
      setErrors(1);
      logError("Error connecting to graph database '" + connectionName + "'", e);
      return false;
    }

    return super.init();
  }

  @Override
  public void dispose() {
    try {
      if (data.graphConnection != null) {
        data.graphConnection.close();
        data.graphConnection = null;
      }
      if (data.session != null) {
        data.session.close();
      }
      if (data.driver != null) {
        data.driver.close();
      }
    } catch (Exception e) {
      logError("Error closing the graph database connection", e);
    }

    super.dispose();
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      emptyRowParametersList();
      setOutputDone();
      return false;
    }
    if (first) {
      first = false;

      prepareOnFirstRow(row);
    }

    // Add the input row to a buffer for safekeeping
    //
    data.inputRowsList.add(row);

    // Create the parameters map for this row
    //
    Map<String, Object> rowParameters = new HashMap<>();
    for (int i = 0; i < meta.getParameters().size(); i++) {
      Parameter parameter = meta.getParameters().get(i);
      int index = data.parameterIndexes.get(i);
      IValueMeta valueMeta = getInputRowMeta().getValueMeta(index);
      Object valueData = row[index];

      GraphPropertyType propertyType = data.parameterTypes.get(i);
      Object neoData = propertyType.convertFromHop(valueMeta, valueData);

      rowParameters.put(parameter.getName(), neoData);
    }
    data.rowParametersList.add(rowParameters);

    if (data.rowParametersList.size() >= data.batchSize) {
      emptyRowParametersList();
    }

    return true;
  }

  private void prepareOnFirstRow(Object[] row) throws HopException {
    data.rowParametersList = new ArrayList<>();
    data.inputRowsList = new ArrayList<>();

    data.outputRowMeta = getInputRowMeta().clone();
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

    // Go over all the rowParameters, see where they are in the input row.
    //
    data.parameterIndexes = new ArrayList<>();
    data.parameterTypes = new ArrayList<>();

    for (Parameter parameter : meta.getParameters()) {
      int index = getInputRowMeta().indexOfValue(parameter.getInputFieldName());
      if (index < 0) {
        throw new HopException(
            "Input field of parameter " + parameter.getName() + " could not be found");
      }
      data.parameterIndexes.add(index);
      // This defaults to String
      GraphPropertyType propertyType =
          GraphPropertyType.parseCode(Const.NVL(parameter.getNeoType(), ""));
      if (propertyType == null) {
        throw new HopException(
            "Unable to convert to unknown property type for parameter '"
                + parameter.getName()
                + "', input field: "
                + parameter.getInputFieldName());
      }

      data.parameterTypes.add(propertyType);
    }

    data.unwindAlias = resolve(meta.getUnwindAlias());

    // Look for return values in the operations...
    //
    data.outputValues = new ArrayList<>();
    data.outputIndexes = new ArrayList<>();
    data.neoTypes = new ArrayList<>();
    int startIndex;
    if (StringUtils.isEmpty(data.unwindAlias)) {
      startIndex = getInputRowMeta().size();
    } else {
      startIndex = 0;
    }
    for (int index = startIndex; index < data.outputRowMeta.size(); index++) {
      data.outputIndexes.add(index);
      IValueMeta valueMeta = data.outputRowMeta.getValueMeta(index);
      data.outputValues.add(valueMeta);
      // No error here, defaults to String
      GraphPropertyDataType neoType = GraphPropertyDataType.parseCode(valueMeta.getComments());
      data.neoTypes.add(neoType);
    }

    data.batchSize = Const.toIntExpanded(resolve(meta.getBatchSize()), 1);

    data.cypher = meta.getCypher(this);

    data.needsWriteTransaction = meta.needsWriteTransaction();

    int retries = Const.toInt(resolve(meta.getRetries()), 0);
    if (retries < 0) {
      throw new HopException(
          "The number of retries on an error should be larger than or equal to 0, not " + retries);
    }
    data.attempts = getAttempts(retries);
  }

  /**
   * The number of attempts to execute a batch: the retries plus one. Without transactions a failed
   * attempt can leave part of its changes behind, so a batch which changes data is tried once, as
   * in Graph query.
   *
   * @param retries The number of retries on an error
   * @return The number of attempts to use
   */
  int getAttempts(int retries) {
    int attempts = 1 + retries;
    if (data.graphConnection == null) {
      // Bolt: transactions are rolled back on an error
      return attempts;
    }
    int allowed =
        Cypher.getAttempts(
            attempts, !data.needsWriteTransaction, data.graphConnection.isSupportingTransactions());
    if (allowed != attempts) {
      logBasic(
          "Warning: graph database connection '"
              + resolve(meta.getConnectionName())
              + "' doesn't support transactions: statements which change data are not retried"
              + " on an error");
    }
    return allowed;
  }

  private void emptyRowParametersList() throws HopException {
    if (data.rowParametersList == null || data.rowParametersList.isEmpty()) {
      // Nothing to do here.
      return;
    }
    if (data.graphConnection != null) {
      try {
        emptyGenericRowParametersList();
      } finally {
        data.rowParametersList.clear();
        data.inputRowsList.clear();
      }
      return;
    }
    try {

      for (int attempt = 0; attempt < data.attempts; attempt++) {
        try {
          if (StringUtils.isEmpty(data.unwindAlias)) {
            // No UNWIND. We're just going to create a "write" transaction and send statements.
            //
            HopException hopException;
            if (data.needsWriteTransaction) {
              hopException = data.session.executeWrite(new RowsTransaction());
            } else {
              hopException = data.session.executeRead(new RowsTransaction());
            }
            if (hopException != null) {
              throw hopException;
            }
          } else {
            // One UNWIND statement with all the data in a new Map in a "rows" parameter.
            //
            Result result =
                data.session.run(
                    data.cypher,
                    Map.of(CypherBuilderMeta.ROWS_UNWIND_MAP_ENTRY, data.rowParametersList));

            // Parse the results and send its data to the next transforms.
            //
            putResultRows(new Object[0], result);
          }
          // Break out of the attempts loop if all went well.
          //
          break;
        } catch (Exception e) {
          // Retry?
          if (attempt + 1 >= data.attempts) {
            throw new HopException("Failed transaction after " + data.attempts + " attempts", e);
          } else {
            logDetailed("Retrying unwind after error: " + e.getMessage());
          }
        }
      }
    } catch (Exception e) {
      throw new HopException("Error writing batch of rows to Neo4j", e);
    } finally {
      data.rowParametersList.clear();
      data.inputRowsList.clear();
    }
  }

  /**
   * Execute the batch over a graph connection which isn't Bolt. The result rows are passed on after
   * the batch succeeded, so a retry doesn't send rows twice.
   */
  private void emptyGenericRowParametersList() throws HopException {
    for (int attempt = 0; attempt < data.attempts; attempt++) {
      try {
        List<Object[]> outputRows =
            data.graphConnection.executeWrite(
                transaction -> {
                  List<Object[]> rows = new ArrayList<>();
                  if (StringUtils.isEmpty(data.unwindAlias)) {
                    for (int i = 0; i < data.inputRowsList.size(); i++) {
                      addGenericResultRows(
                          rows,
                          data.inputRowsList.get(i),
                          transaction.execute(data.cypher, data.rowParametersList.get(i)));
                    }
                  } else {
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put(
                        CypherBuilderMeta.ROWS_UNWIND_MAP_ENTRY,
                        new ArrayList<>(data.rowParametersList));
                    addGenericResultRows(
                        rows, new Object[0], transaction.execute(data.cypher, parameters));
                  }
                  return rows;
                });
        for (Object[] outputRow : outputRows) {
          putRow(data.outputRowMeta, outputRow);
        }
        return;
      } catch (HopException e) {
        if (attempt + 1 >= data.attempts) {
          throw new HopException("Failed transaction after " + data.attempts + " attempts", e);
        }
        logDetailed("Retrying after error: " + e.getMessage());
      }
    }
  }

  private void addGenericResultRows(
      List<Object[]> outputRows, Object[] inputRowData, List<Map<String, Object>> results)
      throws HopException {
    for (Map<String, Object> result : results) {
      Object[] outputRow = RowDataUtil.createResizedCopy(inputRowData, data.outputRowMeta.size());
      for (int i = 0; i < data.outputIndexes.size(); i++) {
        IValueMeta valueMeta = data.outputValues.get(i);
        outputRow[data.outputIndexes.get(i)] =
            NeoHopData.convertToHopValue(
                valueMeta.getName(), result.get(valueMeta.getName()), valueMeta);
      }
      outputRows.add(outputRow);
    }
  }

  private void putResultRows(Object[] inputRowData, Result result) throws HopException {
    while (result.hasNext()) {
      org.neo4j.driver.Record record = result.next();

      // For every record we create a copy of the input data and append the extra fields.
      //
      Object[] outputRow = RowDataUtil.createResizedCopy(inputRowData, data.outputRowMeta.size());

      for (int i = 0; i < data.outputIndexes.size(); i++) {
        int index = data.outputIndexes.get(i);
        IValueMeta valueMeta = data.outputValues.get(i);
        GraphPropertyDataType neoType = data.neoTypes.get(i);
        Value value = record.get(valueMeta.getName());

        // Convert the Neo4j data type to the selected Hop data type
        //
        outputRow[index] =
            NeoHopData.convertNeoToHopValue(valueMeta.getName(), value, neoType, valueMeta);
      }

      // Send the row on its merry way.
      //
      putRow(data.outputRowMeta, outputRow);
    }
  }

  @Override
  public void batchComplete() throws HopException {
    if (!data.isBeamContext()) {
      emptyRowParametersList();
    }
  }

  @Override
  public void finishBundle() throws HopException {
    emptyRowParametersList();
  }

  private final class RowsTransaction implements TransactionCallback<HopException> {

    public RowsTransaction() {
      // Do nothing
    }

    @Override
    public HopException execute(TransactionContext tx) {
      try {
        for (int i = 0; i < data.inputRowsList.size(); i++) {
          Object[] inputRow = data.inputRowsList.get(i);
          Map<String, Object> rowParameters = data.rowParametersList.get(i);
          Result result = tx.run(data.cypher, rowParameters);
          putResultRows(inputRow, result);
        }
        // Transaction is automatically committed/rolled back by executeWrite/executeRead
        return null;
      } catch (Exception e) {
        return new HopException("Error writing to Neo4j", e);
      }
    }
  }
}
