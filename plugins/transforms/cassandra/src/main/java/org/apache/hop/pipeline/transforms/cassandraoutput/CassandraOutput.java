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
 */
package org.apache.hop.pipeline.transforms.cassandraoutput;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.databases.cassandra.ConnectionFactory;
import org.apache.hop.databases.cassandra.driver.datastax.DriverCQLRowHandler;
import org.apache.hop.databases.cassandra.spi.CQLRowHandler;
import org.apache.hop.databases.cassandra.spi.Connection;
import org.apache.hop.databases.cassandra.spi.ITableMetaData;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.databases.cassandra.util.CassandraUtils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Class providing an output step for writing data to a cassandra table. Can create the specified
 * table (if it doesn't already exist) and can update table meta data.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class CassandraOutput extends BaseTransform<CassandraOutputMeta, CassandraOutputData>
    implements ITransform<CassandraOutputMeta, CassandraOutputData> {

  protected CassandraOutputMeta m_meta;
  protected CassandraOutputData m_data;

  public CassandraOutput(
      TransformMeta stepMeta,
      CassandraOutputMeta meta,
      CassandraOutputData data,
      int copyNr,
      PipelineMeta transMeta,
      Pipeline trans) {

    super(stepMeta, meta, data, copyNr, transMeta, trans);
    this.m_meta = meta;
    this.m_data = data;
  }

  protected Connection m_connection;

  protected Keyspace m_keyspace;

  protected CQLRowHandler m_cqlHandler = null;

  /** Column meta data and schema information */
  protected ITableMetaData m_cassandraMeta;

  /** Holds batch insert CQL statement */
  protected StringBuilder m_batchInsertCQL;

  /** Current batch of rows to insert */
  protected List<Object[]> m_batch;

  /** The number of rows seen so far for this batch */
  protected int m_rowsSeen;

  /** The batch size to use */
  protected int m_batchSize = 100;

  /** The consistency to use - null means to use the cassandra default */
  protected String m_consistency = null;

  /** The name of the table to write to */
  protected String m_tableName;

  /** The name of the keyspace */
  protected String m_keyspaceName;

  /** The index of the key field in the incoming rows */
  protected List<Integer> m_keyIndexes = null;

  protected int m_cqlBatchInsertTimeout = 0;

  /** Default batch split factor */
  protected int m_batchSplitFactor = 10;

  /** Consistency level to use */
  protected String m_consistencyLevel;

  /** Options for keyspace and row handlers */
  protected Map<String, String> m_opts;

  protected void initialize() throws HopException {

    first = false;
    m_rowsSeen = 0;

    // Get the connection to Cassandra
    String hostS = environmentSubstitute(m_meta.getCassandraHost());
    String portS = environmentSubstitute(m_meta.getCassandraPort());
    String userS = m_meta.getUsername();
    String passS = m_meta.getPassword();
    String batchTimeoutS = environmentSubstitute(m_meta.getCQLBatchInsertTimeout());
    String batchSplitFactor = environmentSubstitute(m_meta.getCQLSubBatchSize());
    String schemaHostS = environmentSubstitute(m_meta.getSchemaHost());
    String schemaPortS = environmentSubstitute(m_meta.getSchemaPort());
    if (Utils.isEmpty(schemaHostS)) {
      schemaHostS = hostS;
    }
    if (Utils.isEmpty(schemaPortS)) {
      schemaPortS = portS;
    }

    if (!Utils.isEmpty(userS) && !Utils.isEmpty(passS)) {
      userS = environmentSubstitute(userS);
      passS = environmentSubstitute(passS);
    }
    m_keyspaceName = environmentSubstitute(m_meta.getCassandraKeyspace());
    m_tableName = CassandraUtils.cql3MixedCaseQuote(environmentSubstitute(m_meta.getTableName()));
    m_consistencyLevel = environmentSubstitute(m_meta.getConsistency());

    String keyField = environmentSubstitute(m_meta.getKeyField());

    try {

      if (!Utils.isEmpty(batchTimeoutS)) {
        try {
          m_cqlBatchInsertTimeout = Integer.parseInt(batchTimeoutS);
          if (m_cqlBatchInsertTimeout < 500) {
            logBasic(
                BaseMessages.getString(
                    CassandraOutputMeta.PKG,
                    "CassandraOutput.Message.MinimumTimeout")); //$NON-NLS-1$
            m_cqlBatchInsertTimeout = 500;
          }
        } catch (NumberFormatException e) {
          logError(
              BaseMessages.getString(
                  CassandraOutputMeta.PKG,
                  "CassandraOutput.Error.CantParseTimeout")); //$NON-NLS-1$
          m_cqlBatchInsertTimeout = 10000;
        }
      }

      if (!Utils.isEmpty(batchSplitFactor)) {
        try {
          m_batchSplitFactor = Integer.parseInt(batchSplitFactor);
        } catch (NumberFormatException e) {
          logError(
              BaseMessages.getString(
                  CassandraOutputMeta.PKG,
                  "CassandraOutput.Error.CantParseSubBatchSize")); //$NON-NLS-1$
        }
      }

      if (Utils.isEmpty(hostS) || Utils.isEmpty(portS) || Utils.isEmpty(m_keyspaceName)) {
        throw new HopException(
            BaseMessages.getString(
                CassandraOutputMeta.PKG,
                "CassandraOutput.Error.MissingConnectionDetails")); //$NON-NLS-1$
      }

      if (Utils.isEmpty(m_tableName)) {
        throw new HopException(
            BaseMessages.getString(
                CassandraOutputMeta.PKG, "CassandraOutput.Error.NoTableSpecified")); // $NON-NLS-1$
      }

      if (Utils.isEmpty(keyField)) {
        throw new HopException(
            BaseMessages.getString(
                CassandraOutputMeta.PKG,
                "CassandraOutput.Error.NoIncomingKeySpecified")); //$NON-NLS-1$
      }

      // check that the specified key field is present in the incoming data
      String[] kparts = keyField.split(","); // $NON-NLS-1$
      m_keyIndexes = new ArrayList<Integer>();
      for (String kpart : kparts) {
        int index = getInputRowMeta().indexOfValue(kpart.trim());
        if (index < 0) {
          throw new HopException(
              BaseMessages.getString(
                  CassandraOutputMeta.PKG,
                  "CassandraOutput.Error.CantFindKeyField",
                  keyField)); //$NON-NLS-1$
        }
        m_keyIndexes.add(index);
      }

      logBasic(
          BaseMessages.getString(
              CassandraOutputMeta.PKG,
              "CassandraOutput.Message.ConnectingForSchemaOperations",
              schemaHostS, //$NON-NLS-1$
              schemaPortS,
              m_keyspaceName));

      Connection connection = null;

      // open up a connection to perform any schema changes
      try {
        connection = openConnection(true);
        Keyspace keyspace = connection.getKeyspace(m_keyspaceName);

        // Try to execute any apriori CQL commands?
        if (!Utils.isEmpty(m_meta.getAprioriCQL())) {
          String aprioriCQL = environmentSubstitute(m_meta.getAprioriCQL());
          List<String> statements = CassandraUtils.splitCQLStatements(aprioriCQL);

          logBasic(
              BaseMessages.getString(
                  CassandraOutputMeta.PKG,
                  "CassandraOutput.Message.ExecutingAprioriCQL", //$NON-NLS-1$
                  m_tableName,
                  aprioriCQL));

          String compression =
              m_meta.getUseCompression() ? "gzip" : ""; // $NON-NLS-1$ //$NON-NLS-2$

          for (String cqlS : statements) {
            try {
              keyspace.executeCQL(cqlS, compression, m_consistencyLevel, log);
            } catch (Exception e) {
              if (m_meta.getDontComplainAboutAprioriCQLFailing()) {
                // just log and continue
                logBasic("WARNING: " + e.toString()); // $NON-NLS-1$
              } else {
                throw e;
              }
            }
          }
        }

        if (!keyspace.tableExists(m_tableName)) {
          if (m_meta.getCreateTable()) {
            // create the table
            boolean result =
                keyspace.createTable(
                    m_tableName,
                    getInputRowMeta(),
                    m_keyIndexes,
                    environmentSubstitute(m_meta.getCreateTableWithClause()),
                    log);

            if (!result) {
              throw new HopException(
                  BaseMessages.getString(
                      CassandraOutputMeta.PKG,
                      "CassandraOutput.Error.NeedAtLeastOneFieldAppartFromKey")); //$NON-NLS-1$
            }
          } else {
            throw new HopException(
                BaseMessages.getString(
                    CassandraOutputMeta.PKG,
                    "CassandraOutput.Error.TableDoesNotExist", //$NON-NLS-1$
                    m_tableName,
                    m_keyspaceName));
          }
        }

        if (m_meta.getUpdateCassandraMeta()) {
          // Update cassandra meta data for unknown incoming fields?
          keyspace.updateTableCQL3(m_tableName, getInputRowMeta(), m_keyIndexes, log);
        }

        // get the table meta data
        logBasic(
            BaseMessages.getString(
                CassandraOutputMeta.PKG,
                "CassandraOutput.Message.GettingMetaData",
                m_tableName)); //$NON-NLS-1$

        m_cassandraMeta = keyspace.getTableMetaData(m_tableName);

        // output (downstream) is the same as input
        m_data.setOutputRowMeta(getInputRowMeta());

        String batchSize = environmentSubstitute(m_meta.getBatchSize());
        if (!Utils.isEmpty(batchSize)) {
          try {
            m_batchSize = Integer.parseInt(batchSize);
          } catch (NumberFormatException e) {
            logError(
                BaseMessages.getString(
                    CassandraOutputMeta.PKG,
                    "CassandraOutput.Error.CantParseBatchSize")); //$NON-NLS-1$
            m_batchSize = 100;
          }
        } else {
          throw new HopException(
              BaseMessages.getString(
                  CassandraOutputMeta.PKG, "CassandraOutput.Error.NoBatchSizeSet")); // $NON-NLS-1$
        }

        // Truncate (remove all data from) table first?
        if (m_meta.getTruncateTable()) {
          keyspace.truncateTable(m_tableName, log);
        }
      } finally {
        if (connection != null) {
          closeConnection(connection);
          connection = null;
        }
      }

      m_consistency = environmentSubstitute(m_meta.getConsistency());
      m_batchInsertCQL = CassandraUtils.newCQLBatch(m_batchSize, m_meta.getUseUnloggedBatch());

      m_batch = new ArrayList<Object[]>();

      // now open the main connection to use
      openConnection(false);

    } catch (Exception ex) {
      logError(
          BaseMessages.getString(
              CassandraOutputMeta.PKG, "CassandraOutput.Error.InitializationProblem"),
          ex); //$NON-NLS-1$
    }
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow();

    if (r == null) {
      // no more output

      // flush the last batch
      if (m_rowsSeen > 0 && !isStopped()) {
        doBatch();
      }
      m_batchInsertCQL = null;
      m_batch = null;

      closeConnection(m_connection);
      m_connection = null;
      m_keyspace = null;
      m_cqlHandler = null;

      setOutputDone();
      return false;
    }

    if (!isStopped()) {
      if (first) {
        initialize();
      }

      m_batch.add(r);
      m_rowsSeen++;

      if (m_rowsSeen == m_batchSize) {
        doBatch();
      }
    } else {
      closeConnection(m_connection);
      return false;
    }

    return true;
  }

  protected void doBatch() throws HopException {

    try {
      doBatch(m_batch);
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              CassandraOutputMeta.PKG,
              "CassandraOutput.Error.CommitFailed",
              m_batchInsertCQL.toString(),
              e)); //$NON-NLS-1$
      throw new HopException(e.fillInStackTrace());
    }

    // ready for a new batch
    m_batch.clear();
    m_rowsSeen = 0;
  }

  protected void doBatch(List<Object[]> batch) throws Exception {
    // stopped?
    if (isStopped()) {
      logDebug(
          BaseMessages.getString(
              CassandraOutputMeta.PKG,
              "CassandraOutput.Message.StoppedSkippingBatch")); //$NON-NLS-1$
      return;
    }
    // ignore empty batch
    if (batch == null || batch.isEmpty()) {
      logDebug(
          BaseMessages.getString(
              CassandraOutputMeta.PKG,
              "CassandraOutput.Message.SkippingEmptyBatch")); //$NON-NLS-1$
      return;
    }
    // construct CQL/thrift batch and commit
    int size = batch.size();
    try {
      // construct CQL
      m_batchInsertCQL = CassandraUtils.newCQLBatch(m_batchSize, m_meta.getUseUnloggedBatch());
      int rowsAdded = 0;
      batch = CassandraUtils.fixBatchMismatchedTypes(batch, getInputRowMeta(), m_cassandraMeta);
      DriverCQLRowHandler handler = (DriverCQLRowHandler) m_cqlHandler;
      validateTtlField(handler, m_opts.get(CassandraUtils.BatchOptions.TTL));
      handler.setUnloggedBatch(m_meta.getUseUnloggedBatch());
      handler.batchInsert(
          getInputRowMeta(),
          batch,
          m_cassandraMeta,
          m_consistencyLevel,
          m_meta.getInsertFieldsNotInMeta(),
          getLogChannel());
      // commit
      if (m_connection == null) {
        openConnection(false);
      }

      logDetailed(
          BaseMessages.getString(
              CassandraOutputMeta.PKG,
              "CassandraOutput.Message.CommittingBatch",
              m_tableName,
              "" //$NON-NLS-1$ //$NON-NLS-2$
                  + rowsAdded));
    } catch (Exception e) {
      logError(e.getLocalizedMessage(), e);
      setErrors(getErrors() + 1);
      closeConnection(m_connection);
      m_connection = null;
      logDetailed(
          BaseMessages.getString(
              CassandraOutputMeta.PKG, "CassandraOutput.Error.FailedToInsertBatch", "" + size),
          e); //$NON-NLS-1$ //$NON-NLS-2$

      logDetailed(
          BaseMessages.getString(
              CassandraOutputMeta.PKG,
              "CassandraOutput.Message.WillNowTrySplittingIntoSubBatches")); //$NON-NLS-1$

      // is it possible to divide and conquer?
      if (size == 1) {
        // single error row - found it!
        if (getTransformMeta().isDoingErrorHandling()) {
          putError(
              getInputRowMeta(),
              batch.get(0),
              1L,
              e.getMessage(),
              null,
              "ERR_INSERT01"); //$NON-NLS-1$
        }
      } else if (size > m_batchSplitFactor) {
        // split into smaller batches and try separately
        List<Object[]> subBatch = new ArrayList<Object[]>();
        while (batch.size() > m_batchSplitFactor) {
          while (subBatch.size() < m_batchSplitFactor && batch.size() > 0) {
            // remove from the right - avoid internal shifting
            subBatch.add(batch.remove(batch.size() - 1));
          }
          doBatch(subBatch);
          subBatch.clear();
        }
        doBatch(batch);
      } else {
        // try each row individually
        List<Object[]> subBatch = new ArrayList<Object[]>();
        while (batch.size() > 0) {
          subBatch.clear();
          // remove from the right - avoid internal shifting
          subBatch.add(batch.remove(batch.size() - 1));
          doBatch(subBatch);
        }
      }
    }
  }

  @VisibleForTesting
  void validateTtlField(DriverCQLRowHandler handler, String ttl) {
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
    if (isStopped() && stopped == true) {
      return;
    }
    super.setStopped(stopped);
  }

  protected Connection openConnection(boolean forSchemaChanges) throws HopException {
    // Get the connection to Cassandra
    String hostS = environmentSubstitute(m_meta.getCassandraHost());
    String portS = environmentSubstitute(m_meta.getCassandraPort());
    String userS = m_meta.getUsername();
    String passS = m_meta.getPassword();
    String timeoutS = environmentSubstitute(m_meta.getSocketTimeout());
    String schemaHostS = environmentSubstitute(m_meta.getSchemaHost());
    String schemaPortS = environmentSubstitute(m_meta.getSchemaPort());
    if (Utils.isEmpty(schemaHostS)) {
      schemaHostS = hostS;
    }
    if (Utils.isEmpty(schemaPortS)) {
      schemaPortS = portS;
    }

    if (!Utils.isEmpty(userS) && !Utils.isEmpty(passS)) {
      userS = environmentSubstitute(userS);
      passS = environmentSubstitute(passS);
    }

    m_opts = new HashMap<String, String>();
    if (!Utils.isEmpty(timeoutS)) {
      m_opts.put(CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT, timeoutS);
    }

    m_opts.put(
        CassandraUtils.BatchOptions.BATCH_TIMEOUT,
        "" //$NON-NLS-1$
            + m_cqlBatchInsertTimeout);

    m_opts.put(
        CassandraUtils.CQLOptions.DATASTAX_DRIVER_VERSION, CassandraUtils.CQLOptions.CQL3_STRING);

    // Set TTL if specified
    setTTLIfSpecified();

    if (m_opts.size() > 0) {
      logBasic(
          BaseMessages.getString(
              CassandraOutputMeta.PKG,
              "CassandraOutput.Message.UsingConnectionOptions", //$NON-NLS-1$
              CassandraUtils.optionsToString(m_opts)));
    }

    Connection connection = null;

    try {

      String actualHostToUse = forSchemaChanges ? schemaHostS : hostS;

      connection =
          CassandraUtils.getCassandraConnection(
              actualHostToUse,
              Integer.parseInt(portS),
              userS,
              passS,
              ConnectionFactory.Driver.BINARY_CQL3_PROTOCOL,
              m_opts);

      // set the global connection only if this connection is not being used
      // just for schema changes
      if (!forSchemaChanges) {
        m_connection = connection;
        m_keyspace = m_connection.getKeyspace(m_keyspaceName);
        m_cqlHandler = m_keyspace.getCQLRowHandler();
      }
    } catch (Exception ex) {
      closeConnection(connection);
      throw new HopException(ex.getMessage(), ex);
    }

    return connection;
  }

  @VisibleForTesting
  void setTTLIfSpecified() {
    String ttl = m_meta.getTTL();
    ttl = environmentSubstitute(ttl);
    if (!Utils.isEmpty(ttl) && !ttl.startsWith("-")) {
      String ttlUnit = m_meta.getTTLUnit();
      CassandraOutputMeta.TTLUnits theUnit = CassandraOutputMeta.TTLUnits.NONE;
      for (CassandraOutputMeta.TTLUnits u : CassandraOutputMeta.TTLUnits.values()) {
        if (ttlUnit.equals(u.toString())) {
          theUnit = u;
          break;
        }
      }
      int value = -1;
      try {
        value = Integer.parseInt(ttl);
        value = theUnit.convertToSeconds(value);
        m_opts.put(CassandraUtils.BatchOptions.TTL, "" + value);
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
      closeConnection(m_connection);
    } catch (HopException e) {
      e.printStackTrace();
    }

    super.dispose();
  }

  protected void closeConnection(Connection conn) throws HopException {
    if (conn != null) {
      logBasic(
          BaseMessages.getString(
              CassandraOutputMeta.PKG, "CassandraOutput.Message.ClosingConnection")); // $NON-NLS-1$
      try {
        conn.closeConnection();
      } catch (Exception e) {
        throw new HopException(e);
      }
    }
  }
}
