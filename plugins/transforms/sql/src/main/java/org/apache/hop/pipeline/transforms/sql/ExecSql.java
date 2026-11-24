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

package org.apache.hop.pipeline.transforms.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.SqlQueryClassifier;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageRelationalIoEmitter;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Execute one or more SQL statements in a script, one time or parameterised (for every row) */
public class ExecSql extends BaseTransform<ExecSqlMeta, ExecSqlData> {

  private static final Class<?> PKG = ExecSqlMeta.class;

  public ExecSql(
      TransformMeta transformMeta,
      ExecSqlMeta meta,
      ExecSqlData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  public static final RowMetaAndData getResultRow(
      Result result, String upd, String ins, String del, String read) {
    RowMetaAndData resultRow = new RowMetaAndData();

    if (!Utils.isEmpty(upd)) {
      IValueMeta meta = new ValueMetaInteger(upd);
      meta.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      resultRow.addValue(meta, result.getNrLinesUpdated());
    }

    if (!Utils.isEmpty(ins)) {
      IValueMeta meta = new ValueMetaInteger(ins);
      meta.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      resultRow.addValue(meta, result.getNrLinesOutput());
    }

    if (!Utils.isEmpty(del)) {
      IValueMeta meta = new ValueMetaInteger(del);
      meta.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      resultRow.addValue(meta, result.getNrLinesDeleted());
    }

    if (!Utils.isEmpty(read)) {
      IValueMeta meta = new ValueMetaInteger(read);
      meta.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      resultRow.addValue(meta, result.getNrLinesRead());
    }

    return resultRow;
  }

  /** Reflect the rows the statements read and affected in the transform's own counters. */
  private void addResultToStats(Result result) {
    setLinesInput(getLinesInput() + result.getNrLinesRead());
    setLinesOutput(getLinesOutput() + result.getNrLinesOutput());
    setLinesUpdated(getLinesUpdated() + result.getNrLinesUpdated() + result.getNrLinesDeleted());
  }

  /** Keep the counts of a row in the current transaction until it is committed. */
  private void addResultToPendingStats(Result result) {
    data.pendingLinesRead += result.getNrLinesRead();
    data.pendingLinesOutput += result.getNrLinesOutput();
    data.pendingLinesUpdated += result.getNrLinesUpdated() + result.getNrLinesDeleted();
  }

  @Override
  public boolean processRow() throws HopException {

    if (!meta.isExecutedEachInputRow()) {
      RowMetaAndData resultRow =
          getResultRow(
              data.result,
              meta.getUpdateField(),
              meta.getInsertField(),
              meta.getDeleteField(),
              meta.getReadField());
      putRow(resultRow.getRowMeta(), resultRow.getData());
      setOutputDone(); // Stop processing, this is all we do!
      return false;
    }

    Object[] row = getRow();
    if (row == null) { // no more input to be expected...

      if (data.commitSize > 0) {
        // The last rows are passed on once they are committed, before the output is closed.
        commitPending();
      }
      setOutputDone();
      return false;
    }

    if (first) { // we just got started

      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Find the indexes of the arguments
      data.argumentIndexes = new int[meta.getArguments().size()];
      for (int i = 0; i < meta.getArguments().size(); i++) {
        String arg = meta.getArguments().get(i).getName();
        data.argumentIndexes[i] = this.getInputRowMeta().indexOfValue(arg);
        if (data.argumentIndexes[i] < 0) {
          logError(BaseMessages.getString(PKG, "ExecSql.Log.ErrorFindingField") + arg + "]");
          throw new HopTransformException(
              BaseMessages.getString(PKG, "ExecSql.Exception.CouldNotFindField", arg));
        }
        if (meta.isParams()) {
          if (i == 0) {
            // Define parameters meta
            data.paramsMeta = new RowMeta();
          }
          data.paramsMeta.addValueMeta(getInputRowMeta().getValueMeta(data.argumentIndexes[i]));
        }
      }

      if (!meta.isParams()) {
        // We replace the '?' markers by the argument values, last marker first so the earlier
        // positions stay valid. A '?' in a literal, quoted identifier or comment is not a marker.
        data.markerPositions =
            new ArrayList<>(data.db.getSqlParameterSpec(data.sql).getMarkerPositions());
        Collections.reverse(data.markerPositions);
        if (data.markerPositions.size() > data.argumentIndexes.length) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG,
                  "ExecSql.Exception.MoreMarkersThanArguments",
                  data.markerPositions.size(),
                  data.argumentIndexes.length));
        }
      }
    }

    String sql;
    Object[] paramsData = null;
    if (meta.isParams()) {
      // Get parameters data
      paramsData = new Object[data.argumentIndexes.length];
      sql = this.data.sql;
      for (int i = 0; i < this.data.argumentIndexes.length; i++) {
        paramsData[i] = row[data.argumentIndexes[i]];
      }
    } else {
      int numMarkers = data.markerPositions.size();
      if (numMarkers > 0) {
        StringBuilder buf = new StringBuilder(data.sql);

        // Replace the values in the SQL string...
        //
        DatabaseMeta dbMeta = data.db.getDatabaseMeta();
        for (int i = 0; i < numMarkers; i++) {
          // Get the appropriate value from the input row...
          //
          int index = data.argumentIndexes[data.markerPositions.size() - i - 1];
          IValueMeta valueMeta = getInputRowMeta().getValueMeta(index);
          Object valueData = row[index];

          // replace the '?' with the String in the row.
          //
          int pos = data.markerPositions.get(i);
          String replaceValue = valueMeta.getString(valueData);
          replaceValue = Const.NVL(replaceValue, "");
          if (meta.isQuoteString() && (valueMeta.getType() == IValueMeta.TYPE_STRING)) {
            // Have the database dialect do the quoting.
            // This also adds the quotes around the string
            replaceValue = dbMeta.quoteSqlString(replaceValue);
          }
          buf.replace(pos, pos + 1, replaceValue);
        }
        sql = buf.toString();
      } else {
        sql = data.sql;
      }
    }
    if (isRowLevel()) {
      logRowlevel(BaseMessages.getString(PKG, "ExecSql.Log.ExecutingSQLScript") + Const.CR + sql);
    }

    boolean sendToErrorRow = false;
    boolean commitDue = false;
    String errorMessage = null;
    try {

      data.result = executeRow(sql, paramsData);

      // The counts of this row alone, also when it runs in a larger transaction.
      RowMetaAndData add =
          getResultRow(
              data.result,
              meta.getUpdateField(),
              meta.getInsertField(),
              meta.getDeleteField(),
              meta.getReadField());

      row = RowDataUtil.addRowData(row, getInputRowMeta().size(), add.getData());

      if (data.commitSize > 0) {
        // In a transaction: the row is passed on when the transaction is committed.
        addResultToPendingStats(data.result);
        data.pendingRows.add(row);
        data.rowsSinceCommit++;
        commitDue = data.rowsSinceCommit >= data.commitSize;
      } else {
        addResultToStats(data.result);

        if (!data.db.isAutoCommit()) {
          data.db.commit();
        }

        putRow(data.outputRowMeta, row); // send it out!

        if (checkFeedback(getLinesWritten()) && isBasic()) {
          logBasic(BaseMessages.getString(PKG, "ExecSql.Log.LineNumber") + getLinesWritten());
        }
      }
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        // Undo the rows of the open transaction right away: none of them is passed on.
        abandonPending();
        throw new HopTransformException(
            BaseMessages.getString(PKG, "ExecSql.Log.ErrorInTransform"), e);
      }

      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), row, 1, errorMessage, null, "ExecSql001");
      }
    }
    if (commitDue) {
      // Outside the row's error handling: a failed commit is not the fault of this row. It fails
      // the transform, and dispose() rolls the transaction back.
      commitPending();
    }
    return true;
  }

  /**
   * Runs the SQL of one row. With a savepoint per row (see {@link #prepareTransaction()}) a failing
   * row is undone on its own, so the rows before it in the same transaction stay and the
   * transaction is not left aborted.
   */
  private Result executeRow(String sql, Object[] paramsData) throws HopException {
    if (!data.useSavepoints) {
      return executeStatements(sql, paramsData);
    }
    return data.db.runInSavepoint(() -> executeStatements(sql, paramsData));
  }

  private Result executeStatements(String sql, Object[] paramsData) throws HopException {
    if (meta.isSingleStatement()) {
      return execSingleStatement(sql, data.paramsMeta, paramsData);
    }
    return data.db.execStatements(sql, data.paramsMeta, paramsData);
  }

  /**
   * Decides how the rows are committed when the SQL runs for each row: with a commit size the
   * connection leaves auto-commit and the rows run in transactions of that many rows. With an error
   * hop, on a database where a failed statement aborts the transaction, every row gets a savepoint.
   */
  private void prepareTransaction() throws HopException {
    data.commitSize = meta.resolveCommitSize(this);
    if (data.commitSize <= 0) {
      data.commitSize = 0;
      return;
    }
    data.db.setCommit(data.commitSize);
    data.useSavepoints = data.db.isUsingRowSavepoints(getTransformMeta().isDoingErrorHandling());
  }

  /** Commits the open transaction, then passes on its rows and counts them in the metrics. */
  private void commitPending() throws HopException {
    data.db.commit();
    data.rowsSinceCommit = 0;

    setLinesInput(getLinesInput() + data.pendingLinesRead);
    setLinesOutput(getLinesOutput() + data.pendingLinesOutput);
    setLinesUpdated(getLinesUpdated() + data.pendingLinesUpdated);
    data.pendingLinesRead = 0;
    data.pendingLinesOutput = 0;
    data.pendingLinesUpdated = 0;

    List<Object[]> rows = new ArrayList<>(data.pendingRows);
    data.pendingRows.clear();
    for (Object[] row : rows) {
      putRow(data.outputRowMeta, row);
      if (checkFeedback(getLinesWritten()) && isBasic()) {
        logBasic(BaseMessages.getString(PKG, "ExecSql.Log.LineNumber") + getLinesWritten());
      }
    }
  }

  /** Rolls back the open transaction. Its rows are dropped, never passed on. */
  private void abandonPending() {
    if (data.commitSize <= 0 || data.db == null) {
      return;
    }
    int dropped = data.pendingRows.size();
    data.pendingRows.clear();
    data.rowsSinceCommit = 0;
    data.pendingLinesRead = 0;
    data.pendingLinesOutput = 0;
    data.pendingLinesUpdated = 0;
    if (dropped > 0) {
      logError(BaseMessages.getString(PKG, "ExecSql.Log.RowsDiscarded", String.valueOf(dropped)));
    }
    try {
      data.db.rollback();
    } catch (HopDatabaseException e) {
      logError(BaseMessages.getString(PKG, "ExecSql.Log.ErrorOccurred") + e.getMessage());
    }
  }

  /**
   * Execute one statement for the current row. As long as the SQL text is the same as for the
   * previous row, one prepared statement is kept open and only the parameters are bound again.
   *
   * <p>With bind parameters the text never changes, so the statement is prepared once. Without bind
   * parameters ("?" replaced by the field values, or no markers at all) the text is prepared the
   * second time in a row it comes by, and only when it holds no "?" a driver would read as a
   * parameter marker. A text which differs from the previous row runs as a plain statement, like
   * before.
   */
  Result execSingleStatement(String sql, IRowMeta paramsMeta, Object[] paramsData)
      throws HopDatabaseException {
    if (data.preparedStatement == null || !sql.equals(data.preparedSql)) {
      boolean prepare =
          paramsMeta != null || (sql.equals(data.previousSql) && sql.indexOf('?') < 0);
      data.previousSql = sql;
      closePreparedStatement();
      if (!prepare) {
        return data.db.execStatement(sql, paramsMeta, paramsData);
      }
      prepareStatement(sql);
    }
    return executePreparedStatement(paramsMeta, paramsData);
  }

  private void prepareStatement(String sql) throws HopDatabaseException {
    DatabaseMeta databaseMeta = data.db.getDatabaseMeta();
    // The same clean-up as Database.execStatement(), done once per distinct text.
    String cleanSql =
        databaseMeta.getIDatabase().createSqlScriptParser().removeComments(sql).trim();
    try {
      data.preparedStatement =
          data.db.getConnection().prepareStatement(databaseMeta.stripCR(cleanSql));
    } catch (SQLException ex) {
      throw new HopDatabaseException("Couldn't execute SQL: " + cleanSql + Const.CR, ex);
    }
    data.preparedSql = sql;
    data.preparedCleanSql = cleanSql;
    data.preparedCountTarget = SqlQueryClassifier.affectedRows(cleanSql, databaseMeta);
    data.preparedSchemaChange = SqlQueryClassifier.isSchemaChange(cleanSql);
  }

  private Result executePreparedStatement(IRowMeta paramsMeta, Object[] paramsData)
      throws HopDatabaseException {
    Result result = new Result();
    PreparedStatement statement = data.preparedStatement;
    boolean success = false;
    try {
      if (paramsMeta != null) {
        data.db.setValues(paramsMeta, paramsData, statement);
      }
      if (statement.execute()) {
        // A query: the rows are not read, as with Database.execStatement()
        try (ResultSet ignored = statement.getResultSet()) {
          // close it right away
        }
      } else {
        data.preparedCountTarget.addTo(result, statement.getUpdateCount());
      }
      if (data.preparedSchemaChange) {
        DbCache.getInstance().clear(data.db.getDatabaseMeta().getName());
      }
      success = true;
    } catch (SQLException ex) {
      throw new HopDatabaseException(
          "Couldn't execute SQL: " + data.preparedCleanSql + Const.CR, ex);
    } catch (Exception e) {
      throw new HopDatabaseException("Unexpected error executing SQL: " + Const.CR, e);
    } finally {
      if (!success) {
        // Don't reuse a statement in an unknown state: the next row prepares a fresh one.
        closePreparedStatementQuietly();
      }
    }
    return result;
  }

  private void closePreparedStatement() throws HopDatabaseException {
    PreparedStatement statement = data.preparedStatement;
    data.preparedStatement = null;
    data.preparedSql = null;
    data.preparedCleanSql = null;
    data.preparedCountTarget = null;
    data.preparedSchemaChange = false;
    if (statement != null) {
      data.db.closePreparedStatement(statement);
    }
  }

  private void closePreparedStatementQuietly() {
    try {
      closePreparedStatement();
    } catch (HopDatabaseException e) {
      logError("Error closing prepared statement", e);
    }
  }

  /** At the end: commits the open transaction, or rolls it back when the transform failed. */
  private void settlePending() {
    if (getErrors() > 0 || isStopped()) {
      // Never keep, or pass on, work of a transform that failed or was stopped.
      abandonPending();
    } else {
      try {
        commitPending();
      } catch (HopException e) {
        logError(BaseMessages.getString(PKG, "ExecSql.Log.ErrorInTransform"), e);
        setErrors(1);
        abandonPending();
      }
    }
  }

  @Override
  public void dispose() {

    if (isBasic()) {
      logBasic(BaseMessages.getString(PKG, "ExecSql.Log.FinishingReadingQuery"));
    }

    if (data.db != null) {
      closePreparedStatementQuietly();
      if (data.commitSize > 0) {
        settlePending();
      }
      data.db.disconnect();
    }

    super.dispose();
  }

  @Override
  public void batchComplete() throws HopException {
    // Single threaded and transactional executions: end the transaction at the end of a batch.
    if (data.commitSize > 0 && data.db != null && getErrors() == 0) {
      commitPending();
    }
  }

  /** Stop the running query */
  @Override
  public void stopRunning() throws HopException {

    if (data.db != null && !data.isCanceled) {
      synchronized (data.db) {
        data.db.cancelQuery();
        PreparedStatement statement = data.preparedStatement;
        if (statement != null) {
          data.db.cancelStatement(statement);
        }
      }
      data.isCanceled = true;
    }
  }

  @Override
  public boolean init() {

    if (super.init()) {

      if (meta.getConnection() == null) {
        logError(BaseMessages.getString(PKG, "ExecSql.Init.ConnectionMissing", getTransformName()));
        return false;
      }
      DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
      if (databaseMeta == null) {
        logError(
            BaseMessages.getString(
                PKG,
                "ExecSql.Init.ConnectionNotFound",
                variables.resolve(meta.getConnection()),
                getTransformName()));
        return false;
      }
      data.db = new Database(this, this, databaseMeta);

      // Connect to the database
      try {
        data.db.connect();

        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ExecSql.Log.ConnectedToDB"));
        }

        String sqlToUse = meta.getEffectiveSql(this);
        if (meta.isReplaceVariables()) {
          data.sql = resolve(sqlToUse);
        } else {
          data.sql = sqlToUse;
        }
        if (meta.isExecutedEachInputRow()) {
          prepareTransaction();
        }
        // If the SQL needs to be executed once, this is a starting transform
        // somewhere.
        if (!meta.isExecutedEachInputRow()) {
          if (meta.isSingleStatement()) {
            data.result = data.db.execStatement(data.sql);
          } else {
            data.result = data.db.execStatements(data.sql);
          }
          addResultToStats(data.result);
          if (!data.db.isAutoCommit()) {
            data.db.commit();
          }
          // Lineage: source/target tables (and column lineage) are recovered by parsing the SQL in
          // the sink, which the engine cannot do without a SQL grammar.
          LineageRelationalIoEmitter.emitTransformRelationalExec(
              this, data.db.getDatabaseMeta(), data.sql, null, null, true, null);
        }
        return true;
      } catch (HopException e) {
        logError(BaseMessages.getString(PKG, "ExecSql.Log.ErrorOccurred") + e.getMessage());
        setErrors(1);
        stopAll();
      }
    }

    return false;
  }
}
