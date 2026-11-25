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

package org.apache.hop.pipeline.transforms.mergeintotable;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.batch.BatchSupport;
import org.apache.hop.core.database.batch.IBatchRowListener;
import org.apache.hop.core.database.batch.RecoverableBatch;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlColumns;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlStatementBuilder;
import org.apache.hop.core.database.dml.DmlSupport;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Writes every input row with one upsert statement: inserted when the key is new, updated when it
 * exists, as the database decides. No lookup query is run.
 *
 * <p>With batch updates on a connection whose batch failure contract is validated, the statements
 * go through a {@link RecoverableBatch}: a row is passed on, or sent to the error hop, only once
 * the database has committed or refused it. Otherwise every row runs as a statement of its own,
 * behind a savepoint on a database where a failed statement aborts the transaction and the
 * transform has an error hop.
 *
 * <p>JDBC reports one count per statement, the same for an insert and for an update, so every
 * merged row is counted as written (lines output); inserts and updates can't be told apart.
 */
public class MergeIntoTable extends BaseTransform<MergeIntoTableMeta, MergeIntoTableData> {
  private static final Class<?> PKG = MergeIntoTableMeta.class;

  public MergeIntoTable(
      TransformMeta transformMeta,
      MergeIntoTableMeta meta,
      MergeIntoTableData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    if (Utils.isEmpty(meta.getConnection())) {
      logError(
          BaseMessages.getString(PKG, "MergeIntoTable.Init.ConnectionMissing", getTransformName()));
      return false;
    }
    DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
    if (databaseMeta == null) {
      logError(
          BaseMessages.getString(PKG, "MergeIntoTable.Init.ConnectionMissing", getTransformName()));
      return false;
    }

    // Refuse a database without the upsert here, before any row is read, rather than on the first
    // row.
    String reason = DmlSupport.unsupportedReason(databaseMeta, meta.getRequestedUpsertForm());
    if (reason != null) {
      logError(reason);
      return false;
    }
    if (meta.getKeyFields().isEmpty()) {
      logError(BaseMessages.getString(PKG, "MergeIntoTable.Init.NoKeys"));
      return false;
    }
    if (Utils.isEmpty(resolve(meta.getTableName()))) {
      logError(BaseMessages.getString(PKG, "MergeIntoTable.Init.NoTable"));
      return false;
    }

    data.db = new Database(this, variables, databaseMeta);
    try {
      data.db.connect();
      reason =
          DmlSupport.unsupportedReason(
              databaseMeta, data.db.getDatabaseMetaData(), meta.getRequestedUpsertForm());
      if (reason != null) {
        logError(reason);
        data.db.disconnect();
        data.db = null;
        return false;
      }
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "MergeIntoTable.Log.ConnectedToDB"));
      }
      data.commitSize = meta.getCommitSize(this);
      data.db.setCommit(data.commitSize);
      chooseWriteMode();
      return true;
    } catch (HopException e) {
      logError(BaseMessages.getString(PKG, "MergeIntoTable.Log.ErrorOccurred") + e.getMessage());
      if (data.db != null) {
        data.db.disconnect();
        data.db = null;
      }
      setErrors(1);
      stopAll();
    }
    return false;
  }

  /**
   * Batches only when asked for, with a commit size, on a connection that can batch, isn't shared
   * by a transactional pipeline, and whose batch failure contract is validated: only then is every
   * row of a failed batch accounted for. Otherwise every row is a statement of its own.
   */
  private void chooseWriteMode() throws HopException {
    boolean errorHandling = getTransformMeta().isDoingErrorHandling();
    data.batchSupport = BatchSupport.of(data.db.getDatabaseMeta().getIDatabase(), this);
    boolean canBatch =
        meta.isUseBatchUpdate()
            && data.commitSize > 0
            && !data.db.isAutoCommit()
            && data.db.getUseBatchInsert(true);
    data.batchMode = canBatch && data.batchSupport.isRecovered();
    data.useSavepoints = !data.batchMode && data.db.isUsingRowSavepoints(errorHandling);
    // Row by row with an error hop on a connection without savepoints (DuckDB), a failed statement
    // can abort the transaction and nothing can take back only that row: every row is committed on
    // its own, so a failure is rolled back without the rows before it.
    data.commitEveryRow =
        !data.batchMode
            && errorHandling
            && !data.useSavepoints
            && !data.db.isAutoCommit()
            && Utils.isEmpty(data.db.getConnectionGroup())
            && !supportsSavepoints();
    if (data.commitEveryRow && isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "MergeIntoTable.Log.CommitEveryRow"));
    }
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG, "MergeIntoTable.Log.BatchSupport", data.batchSupport.describe()));
    }
    if (canBatch && !data.batchMode && isBasic()) {
      logBasic(
          BaseMessages.getString(
              PKG, "MergeIntoTable.Log.RowByRow", data.batchSupport.contract().name()));
    }
  }

  private boolean supportsSavepoints() throws HopDatabaseException {
    try {
      return data.db.getDatabaseMetaData().supportsSavepoints();
    } catch (SQLException e) {
      throw new HopDatabaseException(e);
    }
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow();
    if (r == null) {
      // The rows of the last batch are passed on once committed, so before the output is closed.
      if (data.batch != null && !flushBatch()) {
        return false;
      }
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      prepareMerge(getInputRowMeta());
      if (data.batchMode) {
        data.batch =
            RecoverableBatch.of(
                    data.db,
                    data.prepStatement,
                    (statement, bindRow) -> data.db.setValues(data.bindRowMeta, bindRow, statement),
                    data.batchSupport)
                .withStatementFactory(
                    () -> data.db.getConnection().prepareStatement(data.statement.getSql()));
      }
    }

    Object[] bindRow = new Object[data.bindIndexes.length];
    for (int i = 0; i < bindRow.length; i++) {
      bindRow[i] = r[data.bindIndexes[i]];
    }
    if (isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(
              PKG, "MergeIntoTable.Log.MergeRow", data.bindRowMeta.getString(bindRow)));
    }

    if (data.batch != null) {
      return batchRow(r, bindRow);
    }

    try {
      mergeRow(bindRow);
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        if (data.commitEveryRow) {
          // Only the failed row is in the transaction.
          data.db.rollback();
        }
        prepareAgainIfClosed();
        putError(getInputRowMeta(), r, 1, e.toString(), null, "MIT001");
        return true;
      }
      logError(BaseMessages.getString(PKG, "MergeIntoTable.Log.ErrorInTransform") + e.getMessage());
      setErrors(1);
      stopAll();
      setOutputDone();
      return false;
    }
    putRow(data.outputRowMeta, r);
    incrementLinesOutput();
    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic(BaseMessages.getString(PKG, "MergeIntoTable.Log.LineNumber") + getLinesRead());
    }
    // The commit comes after the row is written and its savepoint released: committing ends it.
    commitEveryCommitSize();
    return true;
  }

  /**
   * Adds a row to the batch, and runs the batch once it holds a commit's worth of rows. The row is
   * passed on when the batch is flushed and the database has committed or refused it.
   *
   * @return false if the transform stopped
   */
  private boolean batchRow(Object[] r, Object[] bindRow) throws HopException {
    try {
      data.batch.add(bindRow, r);
    } catch (HopDatabaseException e) {
      // The row could not even be bound, so it never reached the batch. The rows before it are
      // passed on first, so every row leaves in its turn.
      if (!flushBatch()) {
        return false;
      }
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(getInputRowMeta(), r, 1, e.toString(), null, "MIT001");
        return true;
      }
      logError(BaseMessages.getString(PKG, "MergeIntoTable.Log.ErrorInTransform") + e.getMessage());
      data.db.rollback();
      setErrors(1);
      stopAll();
      setOutputDone();
      return false;
    }
    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic(BaseMessages.getString(PKG, "MergeIntoTable.Log.LineNumber") + getLinesRead());
    }
    if (data.batch.size() >= data.commitSize) {
      return flushBatch();
    }
    return true;
  }

  /** Runs the upsert of one row on its own, behind a savepoint where the database needs one. */
  private void mergeRow(Object[] bindRow) throws HopException {
    PreparedStatement statement = data.prepStatement;
    if (data.useSavepoints) {
      // A failed statement aborts the whole transaction on these databases. The savepoint takes
      // back
      // only the failed row, so the rows before and after it are still written.
      data.db.runInSavepoint(
          () -> {
            executeMerge(statement, bindRow);
            return null;
          });
    } else {
      executeMerge(statement, bindRow);
    }
  }

  private void executeMerge(PreparedStatement statement, Object[] bindRow)
      throws HopDatabaseException {
    data.db.setValues(data.bindRowMeta, bindRow, statement);
    data.db.insertRow(statement, false, false);
  }

  /** DuckDB closes a prepared statement when it fails: prepare it again for the next row. */
  private void prepareAgainIfClosed() throws HopDatabaseException {
    try {
      if (data.prepStatement.isClosed()) {
        data.prepStatement = data.db.getConnection().prepareStatement(data.statement.getSql());
      }
    } catch (SQLException e) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "MergeIntoTable.Exception.Prepare", data.statement.getSql()),
          e);
    }
  }

  /** Commits every commit size rows when the connection is not in auto-commit mode. */
  private void commitEveryCommitSize() throws HopDatabaseException {
    if (data.db.isAutoCommit()) {
      return;
    }
    data.rowsSinceCommit++;
    if (data.commitEveryRow || data.rowsSinceCommit >= data.commitSize) {
      data.db.commit();
      data.rowsSinceCommit = 0;
    }
  }

  /**
   * Works out which stream field feeds which column, chooses the upsert form for the connection and
   * the table, builds the statement and prepares it.
   */
  void prepareMerge(IRowMeta inputRowMeta) throws HopException {
    DatabaseMeta databaseMeta = data.db.getDatabaseMeta();
    DmlSyntax syntax = databaseMeta.getIDatabase().getDmlSyntax();
    String schema = resolve(meta.getSchemaName());
    String table = resolve(meta.getTableName());

    // Column name (upper case) -> stream field index. A key wins over a value field that writes
    // the same column, as the statement only binds that column once, as a key.
    Map<String, Integer> streamIndexes = new HashMap<>();
    List<String> keyColumns = new ArrayList<>();
    for (MergeKeyField keyField : meta.getKeyFields()) {
      int index = streamIndex(inputRowMeta, resolve(keyField.getStreamField()));
      String column = resolve(keyField.getTableColumn());
      streamIndexes.putIfAbsent(upper(column), index);
      keyColumns.add(column);
    }
    List<String> insertedColumns = new ArrayList<>();
    List<String> updatedColumns = new ArrayList<>();
    for (MergeValueField valueField : meta.getValueFields()) {
      int index = streamIndex(inputRowMeta, resolve(valueField.getStreamField()));
      String column = resolve(valueField.getTableColumn());
      streamIndexes.putIfAbsent(upper(column), index);
      insertedColumns.add(column);
      if (meta.isUpdateExisting() && valueField.isUpdate()) {
        updatedColumns.add(column);
      }
    }

    boolean cast = syntax != null && syntax.castParameters();
    boolean onlyChanged = meta.isUpdateExisting() && meta.isUpdateOnlyChanged();
    String quotedTable = databaseMeta.getQuotedSchemaTableCombination(this, schema, table);
    DmlSpec spec =
        DmlSpec.upsert(
            quotedTable,
            columns(cast, schema, table, keyColumns),
            columns(cast, schema, table, insertedColumns),
            onlyChanged
                ? comparedColumns(cast, schema, table, updatedColumns)
                : columns(cast, schema, table, updatedColumns),
            onlyChanged);

    data.plan =
        DmlSupport.plan(
            databaseMeta,
            data.db.getDatabaseMetaData(),
            spec,
            schema,
            table,
            meta.getRequestedUpsertForm());
    if (!data.plan.isSupported()) {
      throw new HopException(data.plan.getUnsupportedReason());
    }
    data.statement = DmlStatementBuilder.upsert(databaseMeta, data.plan, spec);
    // ON DUPLICATE KEY doesn't compare: MySQL leaves an unchanged row alone on its own.
    List<DmlColumn> uncomparable = spec.getUncomparableSetColumns();
    if (onlyChanged
        && !uncomparable.isEmpty()
        && data.plan.getUpsert().form() != UpsertForm.ON_DUPLICATE_KEY) {
      logBasic(
          BaseMessages.getString(
              PKG,
              "MergeIntoTable.Log.ChangedOnlyNotComparable",
              String.join(", ", uncomparable.stream().map(DmlColumn::column).toList())));
    }
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG, "MergeIntoTable.Log.Plan", data.plan.getUpsert().form().name()));
      logDetailed(
          BaseMessages.getString(PKG, "MergeIntoTable.Log.Statement", data.statement.getSql()));
    }

    // The values of a row, in the order of the statement's parameters.
    List<DmlColumn> parameters = data.statement.getRowColumns();
    data.bindIndexes = new int[parameters.size()];
    data.bindRowMeta = new RowMeta();
    for (int i = 0; i < parameters.size(); i++) {
      int index = streamIndexes.get(upper(parameters.get(i).column()));
      data.bindIndexes[i] = index;
      data.bindRowMeta.addValueMeta(inputRowMeta.getValueMeta(index).clone());
    }
    try {
      data.prepStatement = data.db.getConnection().prepareStatement(data.statement.getSql());
    } catch (SQLException e) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "MergeIntoTable.Exception.Prepare", data.statement.getSql()),
          e);
    }
  }

  private static int streamIndex(IRowMeta inputRowMeta, String streamField)
      throws HopTransformException {
    int index = inputRowMeta.indexOfValue(streamField);
    if (index < 0) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "MergeIntoTable.Exception.FieldRequired", streamField));
    }
    return index;
  }

  /**
   * The columns as the statement needs them: with their native types, read from the table, when the
   * dialect casts its parameters, and as named otherwise.
   */
  private List<DmlColumn> columns(boolean cast, String schema, String table, List<String> names)
      throws HopDatabaseException {
    if (names.isEmpty()) {
      return List.of();
    }
    if (cast) {
      return DmlColumns.fromTable(data.db, schema, table, names);
    }
    List<DmlColumn> result = new ArrayList<>();
    for (String name : names) {
      result.add(DmlColumn.of(name));
    }
    return result;
  }

  /**
   * The updated columns for an update of changed rows only, which compares each of them: with
   * whether the database can compare its type, read from the table on every database. A table the
   * metadata doesn't show (an Oracle synonym, say) leaves them as named, comparable as before.
   */
  private List<DmlColumn> comparedColumns(
      boolean cast, String schema, String table, List<String> names) throws HopDatabaseException {
    if (cast || names.isEmpty()) {
      return columns(cast, schema, table, names);
    }
    try {
      return DmlColumns.fromTable(data.db, schema, table, names);
    } catch (HopDatabaseException e) {
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "MergeIntoTable.Log.ColumnTypesUnknown", table, e.getMessage()));
      }
      return columns(false, schema, table, names);
    }
  }

  private static String upper(String column) {
    return column == null ? "" : column.toUpperCase(Locale.ROOT);
  }

  /**
   * Runs and commits the pending batch. With an error hop every row is passed on or sent to the
   * error hop according to what the database did with it. Without one a failure rolls back and
   * stops the transform, and none of the batch's rows are passed on.
   *
   * @return false if the transform failed
   */
  private boolean flushBatch() throws HopException {
    if (data.batch == null || data.batch.isEmpty()) {
      return true;
    }
    try {
      if (getTransformMeta().isDoingErrorHandling()) {
        data.batch.flush(batchRowListener());
      } else {
        int pending = data.batch.size();
        try {
          data.batch.flushOrFail(batchRowListener());
        } catch (HopDatabaseException e) {
          logError(
              BaseMessages.getString(PKG, "MergeIntoTable.Log.ErrorInTransform") + e.getMessage());
          logError(
              BaseMessages.getString(
                  PKG, "MergeIntoTable.Log.BatchDiscarded", String.valueOf(pending)));
          setErrors(1);
          stopAll();
          setOutputDone();
          return false;
        }
      }
    } finally {
      // A driver that closed the statement after a failed batch got a new one from the batch.
      data.prepStatement = data.batch.getStatement();
    }
    return true;
  }

  private IBatchRowListener batchRowListener() {
    return new IBatchRowListener() {
      @Override
      public void rowWritten(Object[] outputRow) throws HopException {
        putRow(data.outputRowMeta, outputRow);
        incrementLinesOutput();
      }

      @Override
      public void rowRejected(Object[] outputRow, String message, boolean attempted)
          throws HopException {
        putError(getInputRowMeta(), outputRow, 1, message, null, attempted ? "MIT002" : "MIT003");
      }
    };
  }

  /** Drops the rows of the batch that are not committed: they are never passed on. */
  private void discardBatch() {
    if (data.batch == null) {
      return;
    }
    int dropped = data.batch.discard().size();
    if (dropped > 0) {
      logError(
          BaseMessages.getString(
              PKG, "MergeIntoTable.Log.BatchDiscarded", String.valueOf(dropped)));
    }
  }

  @Override
  public void batchComplete() throws HopException {
    // A pending batch is run and committed, not just committed: its rows would otherwise only
    // reach the database in dispose().
    if (data.batch != null && getErrors() == 0) {
      flushBatch();
    }
    commit(false);
  }

  @Override
  public void dispose() {
    if (data.batch != null && data.db != null) {
      if (getErrors() > 0 || isStopped()) {
        // Never pass on a row that is not committed.
        discardBatch();
      } else {
        try {
          flushBatch();
        } catch (HopException e) {
          logError(
              BaseMessages.getString(PKG, "MergeIntoTable.Log.ErrorInTransform") + e.getMessage());
          discardBatch();
          setErrors(1);
        }
      }
      data.batch.close();
      data.prepStatement = data.batch.getStatement();
    }
    commit(true);
    super.dispose();
  }

  private void commit(boolean dispose) {
    if (data.db == null) {
      return;
    }
    try {
      if (!data.db.isAutoCommit()) {
        if (getErrors() == 0 && !(dispose && data.batch != null && isStopped())) {
          data.db.commit();
        } else {
          data.db.rollback();
        }
      }
      if (dispose && data.prepStatement != null) {
        data.db.closePreparedStatement(data.prepStatement);
        data.prepStatement = null;
      }
    } catch (HopDatabaseException e) {
      logError(
          BaseMessages.getString(PKG, "MergeIntoTable.Log.UnableToCommit") + " " + e.getMessage());
      setErrors(1);
    } finally {
      if (dispose) {
        data.db.disconnect();
        data.db = null;
      }
    }
  }
}
