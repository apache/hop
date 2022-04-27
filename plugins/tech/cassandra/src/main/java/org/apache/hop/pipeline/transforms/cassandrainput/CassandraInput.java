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
 *
 */
package org.apache.hop.pipeline.transforms.cassandrainput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.databases.cassandra.metadata.CassandraConnection;
import org.apache.hop.databases.cassandra.spi.CqlRowHandler;
import org.apache.hop.databases.cassandra.spi.ITableMetaData;
import org.apache.hop.databases.cassandra.util.CassandraUtils;
import org.apache.hop.databases.cassandra.util.Compression;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.HashMap;
import java.util.Map;

/**
 * Class providing an input transform for reading data from a table in Cassandra. Accesses the
 * schema information stored in Cassandra for type information.
 */
public class CassandraInput extends BaseTransform<CassandraInputMeta, CassandraInputData> {

  public CassandraInput(
      TransformMeta transformMeta,
      CassandraInputMeta meta,
      CassandraInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {

    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /** Column meta data and schema information */
  protected ITableMetaData tableMetaData;

  /** Handler for CQL-based row fetching */
  protected CqlRowHandler cqlHandler;

  /**
   * map of indexes into the output field structure (key is special - it's always the first field in
   * the output row meta
   */
  protected Map<String, Integer> outputFormatMap = new HashMap<>();

  /** Current input row being processed (if executing for each row) */
  protected Object[] currentInputRowDrivingQuery = null;

  /** Column family name */
  protected String tableName;

  @Override
  public boolean processRow() throws HopException {

    if (!isStopped()) {

      if (meta.isExecuteForEachIncomingRow() && currentInputRowDrivingQuery == null) {
        currentInputRowDrivingQuery = getRow();

        if (currentInputRowDrivingQuery == null) {
          // no more input, no more queries to make
          setOutputDone();
          return false;
        }

        if (!first) {
          initQuery();
        }
      }

      if (first) {
        first = false;

        String connectionName = resolve(meta.getConnectionName());
        data.cassandraConnection =
            metadataProvider.getSerializer(CassandraConnection.class).load(connectionName);

        // Get the connection to Cassandra
        String hostname = resolve(data.cassandraConnection.getHostname());
        String port = resolve(data.cassandraConnection.getPort());
        String maxLength = resolve(meta.getMaxLength());
        String keyspace = resolve(data.cassandraConnection.getKeyspace());

        if (Utils.isEmpty(hostname) || Utils.isEmpty(port) || Utils.isEmpty(keyspace)) {
          throw new HopException("Some connection details are missing!!");
        }

        logBasic(
            BaseMessages.getString(
                CassandraInputMeta.PKG,
                "CassandraInput.Info.Connecting",
                hostname,
                port,
                keyspace));

        Map<String, String> connectionOptions = data.cassandraConnection.getOptionsMap(variables);

        if (!Utils.isEmpty(maxLength)) {
          connectionOptions.put(CassandraUtils.ConnectionOptions.MAX_LENGTH, maxLength);
        }

        if (connectionOptions.size() > 0) {
          logBasic(
              BaseMessages.getString(
                  CassandraInputMeta.PKG,
                  "CassandraInput.Info.UsingConnectionOptions",
                  CassandraUtils.optionsToString(connectionOptions)));
        }

        try {
          data.connection =
              data.cassandraConnection.createConnection(this, connectionOptions, false);
          data.keyspace = data.connection.getKeyspace(keyspace);
        } catch (Exception ex) {
          closeConnection();
          throw new HopException(ex.getMessage(), ex);
        }

        String cqlQuery = resolve(meta.getCqlSelectQuery());

        // check the source table first
        tableName = CassandraUtils.getTableNameFromCQLSelectQuery(cqlQuery);

        if (Utils.isEmpty(tableName)) {
          throw new HopException(
              BaseMessages.getString(
                  CassandraInputMeta.PKG,
                  "CassandraInput.Error.NonExistentTable",
                  tableName,
                  keyspace));
        }

        try {
          if (!data.keyspace.tableExists(tableName)) {
            throw new HopException(
                BaseMessages.getString(
                    CassandraInputMeta.PKG,
                    "CassandraInput.Error.NonExistentTable",
                    CassandraUtils.removeQuotes(tableName),
                    keyspace));
          }
        } catch (Exception ex) {
          closeConnection();

          throw new HopException(ex.getMessage(), ex);
        }

        // set up the output row meta
        data.outputRowMeta = new RowMeta();
        meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

        // check that there are some outgoing fields!
        if (data.outputRowMeta.size() == 0) {
          throw new HopException(
              BaseMessages.getString(
                  CassandraInputMeta.PKG, "CassandraInput.Error.QueryWontProduceOutputFields"));
        }

        // set up the lookup map
        for (int i = 0; i < data.outputRowMeta.size(); i++) {
          String fieldName = data.outputRowMeta.getValueMeta(i).getName();
          outputFormatMap.put(fieldName, i);
        }

        // table name (key) is the first field output: get the metadata
        try {
          tableMetaData = data.keyspace.getTableMetaData(tableName);
        } catch (Exception e) {
          closeConnection();
          throw new HopException(e.getMessage(), e);
        }

        initQuery();
      }

      Object[][] outRowData = new Object[1][];
      try {
        outRowData = cqlHandler.getNextOutputRow(data.outputRowMeta, outputFormatMap);
      } catch (Exception e) {
        throw new HopException(e.getMessage(), e);
      }

      if (outRowData != null) {
        for (Object[] r : outRowData) {
          putRow(data.outputRowMeta, r);
        }

        if (log.isRowLevel()) {
          log.logRowlevel(toString(), "Outputted row #" + getProcessed() + " : " + outRowData);
        }

        if (checkFeedback(getProcessed())) {
          logBasic("Read " + getProcessed() + " rows from Cassandra");
        }
      }

      if (outRowData == null) {
        if (!meta.isExecuteForEachIncomingRow()) {
          // we're done now
          closeConnection();
          setOutputDone();
          return false;
        } else {
          currentInputRowDrivingQuery = null; // finished with this row
        }
      }
    } else {
      closeConnection();
      return false;
    }

    return true;
  }

  protected void initQuery() throws HopException {
    String queryS = resolve(meta.getCqlSelectQuery());
    if (meta.isExecuteForEachIncomingRow()) {
      queryS = resolve(queryS, getInputRowMeta(), currentInputRowDrivingQuery);
    }
    boolean usingCompression = data.cassandraConnection.isUsingCompression();
    Compression compression = usingCompression ? Compression.GZIP : Compression.NONE;
    try {
      logBasic(
          BaseMessages.getString(
              CassandraInputMeta.PKG,
              "CassandraInput.Info.ExecutingQuery",
              queryS,
              (usingCompression
                  ? BaseMessages.getString(
                      CassandraInputMeta.PKG, "CassandraInput.Info.UsingGZIPCompression")
                  : "")));
      if (cqlHandler == null) {
        cqlHandler = data.keyspace.getCQLRowHandler();
      }
      cqlHandler.newRowQuery(this, tableName, queryS, compression.name(), "", log);
    } catch (Exception e) {
      closeConnection();

      throw new HopException(e.getMessage(), e);
    }
  }

  @Override
  public void setStopped(boolean stopped) {
    if (isStopped() && stopped) {
      return;
    }
    super.setStopped(stopped);
  }

  @Override
  public void dispose() {
    try {
      closeConnection();
    } catch (HopException e) {
      e.printStackTrace();
    }
  }

  protected void closeConnection() throws HopException {
    if (data.connection != null) {
      logBasic(
          BaseMessages.getString(CassandraInputMeta.PKG, "CassandraInput.Info.ClosingConnection"));
      try {
        data.connection.closeConnection();
        data.connection = null;
      } catch (Exception e) {
        throw new HopException(e.getMessage(), e);
      }
    }
  }
}
