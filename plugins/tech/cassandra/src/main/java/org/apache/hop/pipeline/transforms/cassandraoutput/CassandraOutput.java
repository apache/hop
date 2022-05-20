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
package org.apache.hop.pipeline.transforms.cassandraoutput;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.databases.cassandra.ConnectionFactory;
import org.apache.hop.databases.cassandra.datastax.DriverCqlRowHandler;
import org.apache.hop.databases.cassandra.metadata.CassandraConnection;
import org.apache.hop.databases.cassandra.spi.Connection;
import org.apache.hop.databases.cassandra.spi.CqlRowHandler;
import org.apache.hop.databases.cassandra.spi.ITableMetaData;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.databases.cassandra.util.CassandraUtils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Class providing an output transform for writing data to a cassandra table. Can create the
 * specified table (if it doesn't already exist) and can update table meta data.
 */
public class CassandraOutput extends BaseTransform<CassandraOutputMeta, CassandraOutputData> {

  public CassandraOutput(
      TransformMeta transformMeta,
      CassandraOutputMeta meta,
      CassandraOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {

    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  protected CqlRowHandler cqlHandler = null;

  /** Column meta data and schema information */
  protected ITableMetaData cassandraMeta;

  /** Holds batch insert CQL statement */
  protected StringBuilder batchInsertCql;

  /** Current batch of rows to insert */
  protected List<Object[]> batch;

  /** The number of rows seen so far for this batch */
  protected int rowsSeen;

  /** The batch size to use */
  protected int batchSize = 100;

  /** The consistency to use - null means to use the cassandra default */
  protected String consistency = null;

  /** The name of the table to write to */
  protected String tableName;

  /** The name of the keyspace */
  protected String keyspaceName;

  /** The index of the key field in the incoming rows */
  protected List<Integer> keyIndexes = null;

  protected int cqlBatchInsertTimeout = 0;

  /** Default batch split factor */
  protected int batchSplitFactor = 10;

  /** Consistency level to use */
  protected String consistencyLevel;

  /** Options for keyspace and row handlers */
  protected Map<String, String> options;

  protected void initialize() throws HopException {

    first = false;
    rowsSeen = 0;

    String connectionName = resolve(getMeta().getConnectionName());
    if (StringUtils.isEmpty(connectionName)) {
      throw new HopException("Please specify a Cassandra connection to use");
    }
    CassandraConnection cassandraConnection =
        metadataProvider.getSerializer(CassandraConnection.class).load(connectionName);
    if (cassandraConnection == null) {
      throw new HopException(
          "Cassandra connection '" + connectionName + "' couldn't be found in the metadata");
    }

    // Verify a number of settings before running...
    //
    String batchTimeoutS = resolve(getMeta().getCqlBatchInsertTimeout());
    String batchSplitFactor = resolve(getMeta().getBatchSize());

    keyspaceName = resolve(cassandraConnection.getKeyspace());
    tableName = CassandraUtils.cql3MixedCaseQuote(resolve(getMeta().getTableName()));
    consistencyLevel = resolve(getMeta().getConsistency());

    String keyField = resolve(getMeta().getKeyField());

    try {

      if (!Utils.isEmpty(batchTimeoutS)) {
        try {
          cqlBatchInsertTimeout = Integer.parseInt(batchTimeoutS);
          if (cqlBatchInsertTimeout < 500) {
            logBasic(
                BaseMessages.getString(
                    CassandraOutputMeta.PKG, "CassandraOutput.Message.MinimumTimeout"));
            cqlBatchInsertTimeout = 500;
          }
        } catch (NumberFormatException e) {
          logError(
              BaseMessages.getString(
                  CassandraOutputMeta.PKG, "CassandraOutput.Error.CantParseTimeout"));
          cqlBatchInsertTimeout = 10000;
        }
      }

      if (!Utils.isEmpty(batchSplitFactor)) {
        try {
          this.batchSplitFactor = Integer.parseInt(batchSplitFactor);
        } catch (NumberFormatException e) {
          logError(
              BaseMessages.getString(
                  CassandraOutputMeta.PKG, "CassandraOutput.Error.CantParseSubBatchSize"));
        }
      }

      if (Utils.isEmpty(keyspaceName)) {
        throw new HopException(
            BaseMessages.getString(
                CassandraOutputMeta.PKG, "CassandraOutput.Error.MissingConnectionDetails"));
      }

      if (Utils.isEmpty(tableName)) {
        throw new HopException(
            BaseMessages.getString(
                CassandraOutputMeta.PKG, "CassandraOutput.Error.NoTableSpecified"));
      }

      if (Utils.isEmpty(keyField)) {
        throw new HopException(
            BaseMessages.getString(
                CassandraOutputMeta.PKG, "CassandraOutput.Error.NoIncomingKeySpecified"));
      }

      // check that the specified key field is present in the incoming data
      String[] keyParts = keyField.split(",");
      keyIndexes = new ArrayList<>();
      for (String keyPart : keyParts) {
        int index = getInputRowMeta().indexOfValue(keyPart.trim());
        if (index < 0) {
          throw new HopException(
              BaseMessages.getString(
                  CassandraOutputMeta.PKG, "CassandraOutput.Error.CantFindKeyField", keyField));
        }
        keyIndexes.add(index);
      }

      logBasic(
          BaseMessages.getString(
              CassandraOutputMeta.PKG,
              "CassandraOutput.Message.ConnectingForSchemaOperations",
              cassandraConnection.getSchemaHostname(),
              cassandraConnection.getSchemaPort(),
              keyspaceName));

      Connection connection = null;

      // open up a connection to perform any schema changes
      try {
        connection = cassandraConnection.createConnection(this, true);
        Keyspace keyspace = connection.getKeyspace(keyspaceName);

        if (!keyspace.tableExists(tableName)) {
          if (getMeta().isCreateTable()) {
            // create the table
            boolean result =
                keyspace.createTable(
                    tableName,
                    getInputRowMeta(),
                    keyIndexes,
                    resolve(getMeta().getCreateTableWithClause()),
                    log);

            if (!result) {
              throw new HopException(
                  BaseMessages.getString(
                      CassandraOutputMeta.PKG,
                      "CassandraOutput.Error.NeedAtLeastOneFieldAppartFromKey"));
            }
          } else {
            throw new HopException(
                BaseMessages.getString(
                    CassandraOutputMeta.PKG,
                    "CassandraOutput.Error.TableDoesNotExist",
                    tableName,
                    keyspaceName));
          }
        }

        if (getMeta().isUpdateCassandraMeta()) {
          // Update cassandra meta data for unknown incoming fields?
          keyspace.updateTableCQL3(tableName, getInputRowMeta(), keyIndexes, log);
        }

        // get the table meta data
        logBasic(
            BaseMessages.getString(
                CassandraOutputMeta.PKG, "CassandraOutput.Message.GettingMetaData", tableName));

        cassandraMeta = keyspace.getTableMetaData(tableName);

        // output (downstream) is the same as input
        data.outputRowMeta = getInputRowMeta();

        String batchSize = resolve(getMeta().getBatchSize());
        if (!Utils.isEmpty(batchSize)) {
          try {
            this.batchSize = Integer.parseInt(batchSize);
          } catch (NumberFormatException e) {
            logError(
                BaseMessages.getString(
                    CassandraOutputMeta.PKG, "CassandraOutput.Error.CantParseBatchSize"));
            this.batchSize = 100;
          }
        } else {
          throw new HopException(
              BaseMessages.getString(
                  CassandraOutputMeta.PKG, "CassandraOutput.Error.NoBatchSizeSet"));
        }

        // Truncate (remove all data from) table first?
        if (getMeta().isTruncateTable()) {
          keyspace.truncateTable(tableName, log);
        }
      } finally {
        if (connection != null) {
          closeConnection(connection);
          data.connection = null;
        }
      }

      consistency = resolve(getMeta().getConsistency());
      batchInsertCql = CassandraUtils.newCQLBatch(batchSize, getMeta().isUseUnloggedBatch());

      batch = new ArrayList<>();

      // now open the main connection to use
      openConnection(false);

    } catch (Exception ex) {
      logError(
          BaseMessages.getString(
              CassandraOutputMeta.PKG, "CassandraOutput.Error.InitializationProblem"),
          ex);
    }
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow();

    if (r == null) {
      // no more output

      // flush the last batch
      if (rowsSeen > 0 && !isStopped()) {
        doBatch();
      }
      batchInsertCql = null;
      batch = null;

      closeConnection(data.connection);
      data.connection = null;
      data.keyspace = null;
      cqlHandler = null;

      setOutputDone();
      return false;
    }

    if (!isStopped()) {
      if (first) {
        initialize();
      }

      batch.add(r);
      rowsSeen++;

      if (rowsSeen == batchSize) {
        doBatch();
      }
    } else {
      closeConnection(data.connection);
      return false;
    }

    return true;
  }

  protected void doBatch() throws HopException {

    try {
      doBatch(batch);
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              CassandraOutputMeta.PKG,
              "CassandraOutput.Error.CommitFailed",
              batchInsertCql.toString(),
              e));
      throw new HopException(e.fillInStackTrace());
    }

    // ready for a new batch
    batch.clear();
    rowsSeen = 0;
  }

  protected void doBatch(List<Object[]> batch) throws Exception {
    // stopped?
    if (isStopped()) {
      logDebug(
          BaseMessages.getString(
              CassandraOutputMeta.PKG, "CassandraOutput.Message.StoppedSkippingBatch"));
      return;
    }
    // ignore empty batch
    if (batch == null || batch.isEmpty()) {
      logDebug(
          BaseMessages.getString(
              CassandraOutputMeta.PKG, "CassandraOutput.Message.SkippingEmptyBatch"));
      return;
    }
    // construct CQL/thrift batch and commit
    int size = batch.size();
    try {
      // construct CQL
      batchInsertCql = CassandraUtils.newCQLBatch(batchSize, getMeta().isUseUnloggedBatch());
      int rowsAdded = 0;
      batch = CassandraUtils.fixBatchMismatchedTypes(batch, getInputRowMeta(), cassandraMeta);
      DriverCqlRowHandler handler = (DriverCqlRowHandler) cqlHandler;
      validateTtlField(handler, options.get(CassandraUtils.BatchOptions.TTL));
      handler.setUnloggedBatch(getMeta().isUseUnloggedBatch());
      handler.batchInsert(
          getInputRowMeta(),
          batch,
          cassandraMeta,
          consistencyLevel,
          getMeta().isInsertFieldsNotInMeta(),
          getLogChannel());
      // commit
      if (data.connection == null) {
        openConnection(false);
      }

      logDetailed(
          BaseMessages.getString(
              CassandraOutputMeta.PKG,
              "CassandraOutput.Message.CommittingBatch",
              tableName,
              "" + rowsAdded));
    } catch (Exception e) {
      logError(e.getLocalizedMessage(), e);
      setErrors(getErrors() + 1);
      closeConnection(data.connection);
      data.connection = null;
      logDetailed(
          BaseMessages.getString(
              CassandraOutputMeta.PKG, "CassandraOutput.Error.FailedToInsertBatch", "" + size),
          e);

      logDetailed(
          BaseMessages.getString(
              CassandraOutputMeta.PKG,
              "CassandraOutput.Message.WillNowTrySplittingIntoSubBatches"));

      // is it possible to divide and conquer?
      if (size == 1) {
        // single error row - found it!
        if (getTransformMeta().isDoingErrorHandling()) {
          putError(getInputRowMeta(), batch.get(0), 1L, e.getMessage(), null, "ERR_INSERT01");
        }
      } else if (size > batchSplitFactor) {
        // split into smaller batches and try separately
        List<Object[]> subBatch = new ArrayList<>();
        while (batch.size() > batchSplitFactor) {
          while (subBatch.size() < batchSplitFactor && batch.size() > 0) {
            // remove from the right - avoid internal shifting
            subBatch.add(batch.remove(batch.size() - 1));
          }
          doBatch(subBatch);
          subBatch.clear();
        }
        doBatch(batch);
      } else {
        // try each row individually
        List<Object[]> subBatch = new ArrayList<>();
        while (batch.size() > 0) {
          subBatch.clear();
          // remove from the right - avoid internal shifting
          subBatch.add(batch.remove(batch.size() - 1));
          doBatch(subBatch);
        }
      }
    }

    // Pass the rows forward unchanged after each batch...
    //
    for (Object[] row : batch) {
      putRow(getInputRowMeta(), row);
    }
  }

  @VisibleForTesting
  void validateTtlField(DriverCqlRowHandler handler, String ttl) {
    if (!Utils.isEmpty(ttl)) {
      try {
        handler.setTtlSec(Integer.parseInt(ttl));
      } catch (NumberFormatException e) {
        logDebug(
            BaseMessages.getString(
                CassandraOutputMeta.PKG, "CassandraOutput.Error.CantParseTTL", ttl));
      }
    }
  }

  @Override
  public void setStopped(boolean stopped) {
    if (isStopped() && stopped) {
      return;
    }
    super.setStopped(stopped);
  }

  protected Connection openConnection(boolean forSchemaChanges) throws HopException {

    String connectionName = resolve(meta.getConnectionName());
    if (StringUtils.isEmpty(connectionName)) {
      throw new HopException("Please specify a Cassandra connection to use");
    }
    CassandraConnection cassandraConnection =
        metadataProvider.getSerializer(CassandraConnection.class).load(connectionName);
    if (cassandraConnection == null) {
      throw new HopException(
          "Cassandra connection '" + connectionName + "' couldn't be found in the metadata");
    }

    options = cassandraConnection.getOptionsMap(this);
    options.put(CassandraUtils.BatchOptions.BATCH_TIMEOUT, "" + cqlBatchInsertTimeout);
    options.put(
        CassandraUtils.CQLOptions.DATASTAX_DRIVER_VERSION, CassandraUtils.CQLOptions.CQL3_STRING);

    // Set TTL if specified
    setTTLIfSpecified();

    if (options.size() > 0) {
      logBasic(
          BaseMessages.getString(
              CassandraOutputMeta.PKG,
              "CassandraOutput.Message.UsingConnectionOptions",
              CassandraUtils.optionsToString(options)));
    }

    // Get the connection to Cassandra
    String hostS = resolve(cassandraConnection.getHostname());
    String portS = resolve(cassandraConnection.getPort());
    String userS = resolve(cassandraConnection.getUsername());
    String passS = resolve(cassandraConnection.getPassword());
    String schemaHostS = resolve(getMeta().getSchemaHost());
    String schemaPortS = resolve(getMeta().getSchemaPort());
    if (Utils.isEmpty(schemaHostS)) {
      schemaHostS = hostS;
    }
    if (Utils.isEmpty(schemaPortS)) {
      schemaPortS = portS;
    }

    Connection connection = null;

    try {

      String actualHostToUse = forSchemaChanges ? schemaHostS : hostS;
      String actualPortToUse = forSchemaChanges ? schemaPortS : portS;

      connection =
          CassandraUtils.getCassandraConnection(
              actualHostToUse,
              Integer.parseInt(actualPortToUse),
              userS,
              passS,
              ConnectionFactory.Driver.BINARY_CQL3_PROTOCOL,
              options);

      // set the global connection only if this connection is not being used
      // just for schema changes
      if (!forSchemaChanges) {
        data.connection = connection;
        data.keyspace = data.connection.getKeyspace(keyspaceName);
        cqlHandler = data.keyspace.getCQLRowHandler();
      }
    } catch (Exception ex) {
      closeConnection(connection);
      throw new HopException(ex.getMessage(), ex);
    }

    return connection;
  }

  @VisibleForTesting
  void setTTLIfSpecified() {
    String ttl = getMeta().getTtl();
    ttl = resolve(ttl);
    if (!Utils.isEmpty(ttl) && !ttl.startsWith("-")) {
      String ttlUnit = getMeta().getTtlUnit();
      CassandraOutputMeta.TtlUnits theUnit = CassandraOutputMeta.TtlUnits.NONE;
      for (CassandraOutputMeta.TtlUnits u : CassandraOutputMeta.TtlUnits.values()) {
        if (ttlUnit.equals(u.toString())) {
          theUnit = u;
          break;
        }
      }
      int value = -1;
      try {
        value = Integer.parseInt(ttl);
        value = theUnit.convertToSeconds(value);
        options.put(CassandraUtils.BatchOptions.TTL, "" + value);
      } catch (NumberFormatException e) {
        logDebug(
            BaseMessages.getString(
                CassandraOutputMeta.PKG, "CassandraOutput.Error.CantParseTTL", ttl));
      }
    }
  }

  @Override
  public void dispose() {
    try {
      closeConnection(data.connection);
    } catch (HopException e) {
      e.printStackTrace();
    }

    super.dispose();
  }

  protected void closeConnection(Connection conn) throws HopException {
    if (conn != null) {
      logBasic(
          BaseMessages.getString(
              CassandraOutputMeta.PKG, "CassandraOutput.Message.ClosingConnection"));
      try {
        conn.closeConnection();
      } catch (Exception e) {
        throw new HopException(e);
      }
    }
  }
}
