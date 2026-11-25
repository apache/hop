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

package org.apache.hop.pipeline.transforms.execsqlrow;

import java.nio.charset.Charset;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageRelationalIoEmitter;
import org.apache.hop.lineage.LineageSinkRoutes;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Execute one or more SQL statements in a script, one time or parameterised (for every row) */
public class ExecSqlRow extends BaseTransform<ExecSqlRowMeta, ExecSqlRowData> {

  private static final Class<?> PKG = ExecSqlRowMeta.class;

  /** The most distinct scripts a run reports the tables of. */
  static final int MAX_LINEAGE_SCRIPTS = 1000;

  /** The most characters of SQL a run keeps to report the tables of. */
  static final int MAX_LINEAGE_CHARS = 1024 * 1024;

  public ExecSqlRow(
      TransformMeta transformMeta,
      ExecSqlRowMeta meta,
      ExecSqlRowData data,
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

  @Override
  public boolean processRow() throws HopException {

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] row = getRow();
    if (row == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if (first) { // we just got started

      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Check is SQL field is provided
      if (Utils.isEmpty(meta.getSqlFieldName())) {
        throw new HopException(
            BaseMessages.getString(PKG, "ExecSqlRow.Error.SQLFieldFieldMissing"));
      }

      // cache the position of the field
      if (data.indexOfSqlFieldname < 0) {
        data.indexOfSqlFieldname = this.getInputRowMeta().indexOfValue(meta.getSqlFieldName());
        if (data.indexOfSqlFieldname < 0) {
          // The field is unreachable !
          throw new HopException(
              BaseMessages.getString(
                  PKG, "ExecSqlRow.Exception.CouldnotFindField", meta.getSqlFieldName()));
        }
      }
    }

    // get SQL
    String sql = getInputRowMeta().getString(row, data.indexOfSqlFieldname);

    try {
      data.result = data.useSavepoints ? data.db.runInSavepoint(() -> execute(sql)) : execute(sql);
      addResultToStats(data.result);
      recordLineageScript(sql);

      RowMetaAndData add =
          getResultRow(
              data.result,
              meta.getUpdateField(),
              meta.getInsertField(),
              meta.getDeleteField(),
              meta.getReadField());
      row = RowDataUtil.addRowData(row, getInputRowMeta().size(), add.getData());

      if (meta.getCommitSize() > 0 && !data.db.isAutoCommit()) {
        if (meta.getCommitSize() == 1) {
          data.db.commit();
        } else if (getLinesWritten() % meta.getCommitSize() == 0) {
          data.db.commit();
        }
      }

      putRow(data.outputRowMeta, row); // send it out!

      if (checkFeedback(getLinesWritten()) && isBasic()) {
        logBasic(BaseMessages.getString(PKG, "ExecSqlRow.Log.LineNumber") + getLinesWritten());
      }
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(BaseMessages.getString(PKG, "ExecSqlRow.Log.ErrorInTransform") + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), row, 1, errorMessage, null, "ExecSqlRow001");
      }
    }
    return true;
  }

  /** Runs the SQL of one row: a script file, one statement or several. */
  private Result execute(String sql) throws HopException {
    if (meta.isSqlFromfile()) {
      if (Utils.isEmpty(sql)) {
        // empty filename
        throw new HopException(BaseMessages.getString(PKG, "ExecSqlRow.Log.EmptySQLFromFile"));
      }
      if (isDebug()) {
        logDebug(BaseMessages.getString(PKG, "ExecSqlRow.Log.ExecutingSQLFromFile", sql));
      }
      return data.db.execStatementsFromFile(sql, meta.isSendOneStatement());
    }
    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "ExecSqlRow.Log.ExecutingSQLScript") + Const.CR + sql);
    }
    if (meta.isSendOneStatement()) {
      return data.db.execStatement(sql);
    }
    return data.db.execStatements(sql);
  }

  /**
   * Keeps a script that ran, for the lineage reported on dispose. For a script file the file is
   * read now, once per file and as {@link
   * org.apache.hop.core.database.Database#execStatementsFromFile} read it, so a temporary file
   * deleted later still has its tables reported. Does nothing unless lineage is on. A script per
   * row with values in it must not grow what is kept without bound, so past {@link
   * #MAX_LINEAGE_SCRIPTS} distinct scripts or {@link #MAX_LINEAGE_CHARS} characters the tables of
   * the other scripts are not reported; the scripts still run.
   */
  void recordLineageScript(String sqlOrFile) {
    if (Utils.isEmpty(sqlOrFile)
        || data.lineageTruncated
        || data.lineageScripts.contains(sqlOrFile)
        || !LineageSinkRoutes.isEnabled(this)) {
      return;
    }
    String sql = sqlOrFile;
    if (meta.isSqlFromfile()) {
      if (!data.lineageFiles.add(sqlOrFile)) {
        return;
      }
      try {
        sql = HopVfs.getTextFileContent(sqlOrFile, Charset.defaultCharset());
      } catch (Exception e) {
        // Lineage never fails the transform: this file's tables are not reported
        logDetailed("No lineage for SQL file " + sqlOrFile + ": " + e.getMessage());
        return;
      }
      if (data.lineageScripts.contains(sql)) {
        return;
      }
    }
    if (data.lineageScripts.size() >= MAX_LINEAGE_SCRIPTS
        || data.lineageChars + sql.length() > MAX_LINEAGE_CHARS) {
      data.lineageTruncated = true;
      logBasic(
          "More than "
              + MAX_LINEAGE_SCRIPTS
              + " distinct SQL scripts or "
              + MAX_LINEAGE_CHARS
              + " characters of SQL; lineage is reported for the scripts before that only");
      return;
    }
    data.lineageScripts.add(sql);
    data.lineageChars += sql.length();
  }

  /**
   * One {@code RELATIONAL_IO} event with the distinct scripts that ran, as one script: the sink
   * finds the tables they read and write from the SQL, one statement at a time, as for Execute SQL
   * script, and doesn't give every table the row counts of all of them.
   */
  private void emitRelationalLineage() {
    if (data.db == null || data.lineageScripts.isEmpty()) {
      return;
    }
    String sql = lineageSql();
    if (!sql.isEmpty()) {
      LineageRelationalIoEmitter.emitTransformRelationalExec(
          this, data.db.getDatabaseMeta(), sql, null, null, getErrors() == 0, null);
    }
  }

  /** The distinct scripts that ran, each ended by a semicolon, as one script. */
  String lineageSql() {
    StringBuilder all = new StringBuilder(data.lineageChars + 2 * data.lineageScripts.size());
    for (String script : data.lineageScripts) {
      String statements = script.strip();
      while (statements.endsWith(";")) {
        statements = statements.substring(0, statements.length() - 1).strip();
      }
      if (!statements.isEmpty()) {
        all.append(statements).append(";\n");
      }
    }
    return all.toString();
  }

  @Override
  public void dispose() {

    if (isBasic()) {
      logBasic(BaseMessages.getString(PKG, "ExecSqlRow.Log.FinishingReadingQuery"));
    }

    emitRelationalLineage();

    if (data.db != null) {
      try {
        if (!data.db.isAutoCommit()) {
          if (getErrors() == 0) {
            data.db.commit();
          } else {
            data.db.rollback();
          }
        }
      } catch (HopDatabaseException e) {
        logError(
            BaseMessages.getString(PKG, "Update.Log.UnableToCommitUpdateConnection")
                + data.db
                + "] :"
                + e.toString());
        setErrors(1);
      } finally {
        data.db.disconnect();
      }
    }

    super.dispose();
  }

  /** Stop the running query */
  @Override
  public void stopRunning() throws HopException {
    if (data.db != null) {
      data.db.cancelQuery();
    }
  }

  @Override
  public boolean init() {
    if (super.init()) {
      DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
      if (databaseMeta == null) {
        logError(
            BaseMessages.getString(PKG, "ExecSqlRow.Init.ConnectionMissing", getTransformName()));
        return false;
      }
      data.db = new Database(this, this, databaseMeta);

      // Connect to the database
      try {
        data.db.connect();

        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ExecSqlRow.Log.ConnectedToDB"));
        }

        if (meta.getCommitSize() >= 1) {
          data.db.setCommit(meta.getCommitSize());
        }
        data.useSavepoints =
            data.db.isUsingRowSavepoints(getTransformMeta().isDoingErrorHandling());
        return true;
      } catch (HopException e) {
        logError(BaseMessages.getString(PKG, "ExecSqlRow.Log.ErrorOccurred") + e.getMessage());
        setErrors(1);
        stopAll();
      }
    }

    return false;
  }
}
