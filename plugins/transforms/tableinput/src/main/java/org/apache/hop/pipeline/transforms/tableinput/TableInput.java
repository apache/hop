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

package org.apache.hop.pipeline.transforms.tableinput;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageRelationalIoEmitter;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.jetbrains.annotations.Nullable;

/** Reads information from a database table by using freehand SQL */
public class TableInput extends BaseTransform<TableInputMeta, TableInputData> {
  private static final Class<?> PKG = TableInputMeta.class;

  public TableInput(
      TransformMeta transformMeta,
      TableInputMeta meta,
      TableInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /**
   * Drain every incoming hop and concatenate the rows into one parameter list (legacy {@code IN
   * (?,?,?)}).
   */
  private RowMetaAndData readAllParameterRows() throws HopException {
    IRowMeta parametersMeta = new RowMeta();
    Object[] parametersData = new Object[] {};

    Object[] rowData = getRow();
    while (rowData != null) {
      IRowMeta rowMeta = getInputRowMeta();
      parametersData =
          TableInputParameters.append(parametersMeta, parametersData, rowMeta, rowData);
      rowData = getRow();
    }

    return new RowMetaAndData(parametersMeta, parametersData);
  }

  @Override
  public boolean processRow() throws HopException {
    if (first) { // we just got started
      first = false;

      Object[] parameters;
      IRowMeta parametersMeta;

      if (meta.isExecuteEachInputRow()) {
        if (isDetailed()) {
          logDetailed("Reading a parameter row from incoming hops");
        }
        parameters = getRow();
        parametersMeta = getInputRowMeta();
        if (parameters == null || parametersMeta == null || parametersMeta.isEmpty()) {
          setOutputDone();
          return false;
        }
      } else {
        if (isDetailed()) {
          logDetailed("Reading all parameter rows from incoming hops");
        }
        RowMetaAndData assembled = readAllParameterRows();
        parameters = assembled.getData();
        parametersMeta = assembled.getRowMeta();
        if (parameters == null) {
          parameters = new Object[] {};
        }
        if (parametersMeta == null) {
          parametersMeta = new RowMeta();
        }
        if (!Utils.isEmpty(meta.getLookup()) && parametersMeta.isEmpty()) {
          throw new HopException(
              "Expected to read parameters from incoming hops (Insert data from transform: "
                  + meta.getLookup()
                  + ") but none were found.");
        }
      }
      if (parameters != null && !parametersMeta.isEmpty() && isDetailed()) {
        logDetailed("Query parameters found = " + parametersMeta.getString(parameters));
      }

      boolean success = doQuery(parametersMeta, parameters);
      if (!success) {
        return false;
      }
    } else {
      if (data.thisRow != null) { // We can expect more rows
        try {
          data.nextRow = readConvertedRow(false);
        } catch (HopDatabaseException e) {
          if (e.getCause() instanceof SQLException && isStopped()) {
            // This exception indicates we tried reading a row after the statement
            // (for this transform) was canceled.
            // This is expected and ok so do not pass the exception up.
            //
            logDebug(e.getMessage());
            return false;
          } else {
            logError("Error reading row from database result set: " + e.getMessage());
            if (isDebug()) {
              logDebug(Const.getStackTracker(e));
            }
            setErrors(1);
            throw e;
          }
        }
        if (data.nextRow != null) {
          incrementLinesInput();
        }
      }
    }

    if (data.thisRow == null) { // Finished reading?
      Boolean done = determineDoneReading();
      if (done == null) {
        return false;
      }
      if (done) {
        setOutputDone(); // signal end to receiver(s)
        return false; // end of data or error.
      }
    } else {
      putRow(data.rowMeta, data.thisRow); // fill the rowset(s). (wait for empty)
      data.thisRow = data.nextRow;

      if (checkFeedback(getLinesInput()) && isBasic()) {
        logBasic("linenr " + getLinesInput());
      }
    }

    return true;
  }

  private @Nullable Boolean determineDoneReading() throws HopException {
    boolean done = false;
    if (meta.isExecuteEachInputRow()) {
      Object[] nextRow = getRow();
      if (nextRow == null) {
        done = true;
      } else {
        closePreviousQuery();

        boolean success = doQuery(getInputRowMeta(), nextRow);
        if (!success) {
          return null;
        }

        if (data.thisRow != null) {
          putRow(data.rowMeta, data.thisRow); // fill the rowset(s). (wait for empty)
          data.thisRow = data.nextRow;

          if (checkFeedback(getLinesInput()) && isBasic()) {
            logBasic("linenr " + getLinesInput());
          }
        }
      }
    } else {
      done = true;
    }
    return done;
  }

  private void closePreviousQuery() throws HopDatabaseException {
    if (data.db != null) {
      data.db.closeQuery(data.rs);
    }
  }

  private boolean doQuery(IRowMeta parametersMeta, Object[] parameters) throws HopException {
    boolean success = true;

    // Open the query with the optional parameters received from the source transforms.
    String sql;
    try {
      sql = meta.getEffectiveSql(variables);
    } catch (HopException e) {
      logError("Could not get SQL: " + e.getMessage());
      setErrors(1);
      stopAll();
      return false;
    }
    if (meta.isVariableReplacementActive()) {
      sql = resolve(sql);
    }

    TableInputSql.Bound bound;
    try {
      bound = TableInputSql.prepare(meta.isUseNamedParameters(), sql, parametersMeta, parameters);
    } catch (HopException e) {
      logError(e.getMessage());
      setErrors(1);
      stopAll();
      return false;
    }
    sql = bound.getJdbcSql();
    IRowMeta boundMeta = bound.getParameterMeta();
    Object[] boundData = bound.getParameterData();

    if (isDetailed()) {
      logDetailed("SQL query : " + sql);
    }

    try {
      if (boundMeta == null || boundMeta.isEmpty()) {
        data.rs = data.db.openQuery(sql, null, null, ResultSet.FETCH_FORWARD, false);
      } else {
        data.rs = data.db.openQuery(sql, boundMeta, boundData, ResultSet.FETCH_FORWARD, false);
      }
    } catch (HopDatabaseException ex) {
      Throwable root = ex.getCause();
      if (root instanceof SQLException) {
        logError(
            "SQL query failed. Please verify the SQL syntax and referenced tables/columns: "
                + ex.getMessage());
      } else {
        logError("Failed to execute query: " + ex.getMessage());
      }
      if (isDebug()) {
        logDebug(Const.getStackTracker(ex));
      }
      setErrors(1);
      stopAll();
      return false;
    }

    if (data.rs == null) {
      logError("Couldn't open Query. Please verify the SQL syntax: " + sql);
      setErrors(1);
      stopAll();
      success = false;
    } else {
      // Keep the metadata
      data.jdbcRowMeta = data.db.getReturnRowMeta();
      data.specifiedMapping = null;
      try {
        if (meta.isSpecifyFields()) {
          data.rowMeta = meta.createSpecifiedRowMeta(getTransformName(), this);
          data.specifiedMapping =
              meta.createSpecifiedMapping(
                  data.jdbcRowMeta, data.rowMeta, meta.isValidateSpecifiedFields());
        } else {
          data.rowMeta = data.jdbcRowMeta;
        }
      } catch (HopException e) {
        logError(e.getMessage());
        setErrors(1);
        stopAll();
        return false;
      }

      // Set the origin on the row metadata...
      if (data.rowMeta != null) {
        for (IValueMeta valueMeta : data.rowMeta.getValueMetaList()) {
          valueMeta.setOrigin(getTransformName());
        }
      }

      // Lineage: the SQL's source tables are recovered by the sink (parsed); the return row meta
      // gives the read column schema.
      LineageRelationalIoEmitter.emitTransformRelationalRead(
          this, data.db.getDatabaseMeta(), sql, null, data.rowMeta, true, null);

      // Get the first row...
      try {
        data.thisRow = readConvertedRow(true);
        if (data.thisRow != null) {
          incrementLinesInput();
          data.nextRow = readConvertedRow(true);
          if (data.nextRow != null) {
            incrementLinesInput();
          }
        }
      } catch (HopDatabaseException ex) {
        logError("Error reading rows from query result: " + ex.getMessage());
        if (isDebug()) {
          logDebug(Const.getStackTracker(ex));
        }
        setErrors(1);
        stopAll();
        return false;
      }

      if (isRowLevel()) {
        logRowlevel("SQL statement executed: " + sql);
        if (data.rowMeta != null) {
          logRowlevel("Columns returned: " + Arrays.toString(data.rowMeta.getFieldNames()));
        }
      }
    }
    return success;
  }

  @Override
  public void dispose() {
    if (isBasic()) {
      logBasic("Finished reading query, closing connection.");
    }
    try {
      closePreviousQuery();
    } catch (HopException e) {
      logError("Unexpected error closing query: " + e.getMessage());
      if (isDebug()) {
        logDebug(Const.getStackTracker(e));
      }
      setErrors(1);
      stopAll();
    } finally {
      if (data.db != null) {
        data.db.disconnect();
        data.db = null;
      }
    }

    super.dispose();
  }

  /** Stop the running query */
  @Override
  public synchronized void stopRunning() throws HopException {
    if (this.isStopped() || data.isDisposed()) {
      return;
    }

    setStopped(true);

    if (data.db != null && data.db.getConnection() != null && !data.isCanceled) {
      data.db.cancelQuery();
      data.isCanceled = true;
    }
  }

  @Override
  public boolean init() {

    if (super.init()) {
      // Verify some basic things first...
      //
      boolean passed = true;
      if (Utils.isEmpty(meta.getSql()) && Utils.isEmpty(meta.getSqlFromFile())) {
        logError(BaseMessages.getString(PKG, "TableInput.Exception.SQLIsNeeded"));
        passed = false;
      }

      if (meta.isSpecifyFields() && Utils.isEmpty(meta.getFields())) {
        logError(BaseMessages.getString(PKG, "TableInput.Exception.SpecifyFieldsEmpty"));
        passed = false;
      }

      if (meta.getConnection() == null) {
        logError(BaseMessages.getString(PKG, "TableInput.Exception.DatabaseConnectionsIsNeeded"));
        passed = false;
      }
      if (!passed) {
        return false;
      }

      data.infoStream = meta.getTransformIOMeta().getInfoStreams().get(0);
      if (meta.getLookup() != null) {
        // Set reference to input transform
        data.infoStream.setSubject(meta.getLookup());
      }

      DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
      if (databaseMeta == null) {
        logError(
            "Relational database connection '"
                + meta.getConnection()
                + "' not found. Please verify the connection name in the transform configuration.");
        setErrors(1);
        return false;
      }

      data.db = new Database(this, this, databaseMeta);
      data.db.setQueryLimit(Const.toIntExpanded(resolve(meta.getRowLimit()), 0));
      // Statement timeout is for transform dialog / pipeline preview only (Hop GUI sets preview).
      // Normal pipeline runs use JDBC driver default (0 = no explicit timeout on the statement).
      if (getPipeline() != null && getPipeline().isPreview()) {
        String raw =
            getVariable(
                Const.HOP_QUERY_PREVIEW_TIMEOUT,
                EnvUtil.getSystemProperty(Const.HOP_QUERY_PREVIEW_TIMEOUT, "0"));
        int statementQueryTimeoutSeconds = Math.max(0, Const.toInt(resolve(raw), 0));
        if (statementQueryTimeoutSeconds > 0) {
          data.db.setStatementQueryTimeoutSeconds(statementQueryTimeoutSeconds);
        }
      }

      try {
        data.db.connect();
        if (databaseMeta.isRequiringTransactionsOnQueries()) {
          data.db.setCommit(100); // needed for PGSQL it seems...
        }
        if (isDetailed()) {
          logDetailed("Connected to database...");
        }

        if (isDebug()) {
          logDebug(
              "Database connection details - hostname: "
                  + databaseMeta.getHostname()
                  + ", port: "
                  + databaseMeta.getPort()
                  + ", database: "
                  + databaseMeta.getDatabaseName());
        }

        return true;
      } catch (HopException e) {
        Throwable root = e.getCause();
        if (root instanceof UnknownHostException) {
          logError(
              "Database hostname could not be resolved. Please verify the hostname in the connection settings: "
                  + e.getMessage());
        } else if (root instanceof ConnectException) {
          logError(
              "Unable to connect to the database server. Please verify the hostname, port, and that the server is running: "
                  + e.getMessage());
        } else if (root instanceof SQLException) {
          logError(
              "Database connection failed. Please verify the connection credentials and database name: "
                  + e.getMessage());
        } else {
          logError(
              "An error occurred while connecting to the database, processing will be stopped: "
                  + e.getMessage());
        }
        if (isDebug()) {
          logDebug(Const.getStackTracker(e));
        }
        setErrors(1);
        stopAll();
      }
    }

    return false;
  }

  public boolean isWaitingForData() {
    return true;
  }

  private Object[] readConvertedRow(boolean firstRows) throws HopDatabaseException, HopException {
    Object[] jdbcRow = firstRows ? data.db.getRow(data.rs) : data.db.getRow(data.rs, false);
    return convertSpecifiedRow(jdbcRow);
  }

  private Object[] convertSpecifiedRow(Object[] jdbcRow) throws HopException {
    if (jdbcRow == null || data.specifiedMapping == null || data.rowMeta == null) {
      return jdbcRow;
    }
    Object[] output = RowDataUtil.allocateRowData(data.rowMeta.size());
    for (int i = 0; i < data.specifiedMapping.length; i++) {
      int sourceIndex = data.specifiedMapping[i];
      IValueMeta sourceMeta = data.jdbcRowMeta.getValueMeta(sourceIndex);
      IValueMeta targetMeta = data.rowMeta.getValueMeta(i);
      try {
        output[i] = targetMeta.convertData(sourceMeta, jdbcRow[sourceIndex]);
      } catch (HopValueException e) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "TableInput.Exception.SpecifiedFieldConversionError",
                targetMeta.getName(),
                e.getMessage()),
            e);
      }
    }
    return output;
  }
}
