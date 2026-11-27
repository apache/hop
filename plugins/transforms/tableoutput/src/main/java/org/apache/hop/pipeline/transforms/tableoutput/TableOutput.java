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

package org.apache.hop.pipeline.transforms.tableoutput;

import com.google.common.annotations.VisibleForTesting;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.batch.BatchSupport;
import org.apache.hop.core.database.batch.IBatchRowListener;
import org.apache.hop.core.database.batch.IRowBatch;
import org.apache.hop.core.database.batch.MultiRowBatch;
import org.apache.hop.core.database.batch.RecoverableBatch;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlColumns;
import org.apache.hop.core.database.dml.DmlPlan;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlStatements;
import org.apache.hop.core.database.dml.DmlSupport;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageRelationalIoEmitter;
import org.apache.hop.lineage.LineageSinkRoutes;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Writes rows to a database table. */
public class TableOutput extends BaseTransform<TableOutputMeta, TableOutputData> {

  private static final Class<?> PKG = TableOutputMeta.class;
  public static final String NO_INCOMING_STREAM_FIELDS_AVAILABLE =
      "No incoming stream fields available";
  public static final String COULD_NOT_CLEAR_DATABASE_CACHE = "Could not clear database cache: ";
  public static final String COULD_NOT_RETRIEVE_TABLE_STRUCTURE_FOR =
      "Could not retrieve table structure for: ";
  public static final String COULD_NOT_ROLLBACK_TRANSACTION = "Could not rollback transaction: ";

  /**
   * Upper bound on the distinct target tables remembered for lineage in the dynamic table-name
   * mode. The table name comes from a row field, so its cardinality is unbounded in principle.
   */
  private static final int MAX_DYNAMIC_LINEAGE_TABLES = 1000;

  public TableOutput(
      TransformMeta transformMeta,
      TableOutputMeta meta,
      TableOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // this also waits for a previous transform to be finished.
    if (r == null) { // no more input to be expected...
      if (first && meta.isTruncateTable() && !meta.isOnlyWhenHaveRows() && !isPreparedOnce()) {
        truncateTable();
      }
      // The end of the rows: write what is still buffered and say so, as processRow() must. A
      // single-threaded run only disposes the transforms once all of them are done, so the
      // transforms after this one would otherwise wait for more rows
      emptyAndCommitBatchBuffers(false);
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      // The engine did the truncate and table changes before the run (see prepareOnce), unless
      // the table is only truncated once rows arrive
      if (meta.isTruncateTable() && (!isPreparedOnce() || meta.isOnlyWhenHaveRows())) {
        truncateTable();
      }

      // Handle automatic table structure updates
      if (meta.isAutoUpdateTableStructure() && !isPreparedOnce()) {
        updateTableStructure();

        // Clear prepared statement cache after DDL changes
        // This ensures INSERT statements are rebuilt with the new table structure
        if (data.preparedStatements != null) {
          data.preparedStatements.clear();
          if (isDetailed()) {
            logDetailed("Cleared prepared statement cache after table structure updates");
          }
        }
      }

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      if (!meta.isSpecifyFields()) {
        // Just take the input row
        data.insertRowMeta = getInputRowMeta().clone();
      } else {

        data.insertRowMeta = new RowMeta();

        //
        // Cache the position of the compare fields in Row row
        //
        data.valuenrs = new int[meta.getFields().size()];
        for (int i = 0; i < meta.getFields().size(); i++) {
          TableOutputField tf = meta.getFields().get(i);
          data.valuenrs[i] = getInputRowMeta().indexOfValue(tf.getFieldStream());
          if (data.valuenrs[i] < 0) {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG, "TableOutput.Exception.FieldRequired", tf.getFieldStream()));
          }
        }

        for (int i = 0; i < meta.getFields().size(); i++) {
          TableOutputField tf = meta.getFields().get(i);
          IValueMeta insValue = getInputRowMeta().searchValueMeta(tf.getFieldStream());
          if (insValue != null) {
            IValueMeta insertValue = insValue.clone();
            insertValue.setName(tf.getFieldDatabase());
            data.insertRowMeta.addValueMeta(insertValue);
          } else {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG, "TableOutput.Exception.FailedToFindField", tf.getFieldStream()));
          }
        }
      }
    }

    try {
      Object[] outputRowData = writeToTable(getInputRowMeta(), r);
      if (outputRowData != null) {
        putRow(data.outputRowMeta, outputRowData); // in case we want it go further...
        incrementLinesOutput();
      }

      if (checkFeedback(getLinesRead()) && isBasic()) {
        logBasic("linenr " + getLinesRead());
      }
    } catch (HopException e) {
      logError("Because of an error, this transform can't continue: ", e);
      setErrors(1);
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }

    return true;
  }

  /**
   * Remembers a target seen in the per-row dynamic table-name mode, for the lineage emitted on
   * dispose.
   *
   * <p>This is the one piece of lineage bookkeeping on the per-row path, so it is guarded twice: it
   * does nothing at all unless lineage is switched on (the default is off, and every pipeline would
   * otherwise pay for a feature it is not using), and it stops accumulating past {@link
   * #MAX_DYNAMIC_LINEAGE_TABLES} so a high-cardinality table-name field cannot grow the set without
   * bound. Hitting the cap costs lineage for the tables beyond it, never the write.
   */
  @VisibleForTesting
  void recordDynamicLineageTarget(String tableName) {
    if (Utils.isEmpty(tableName) || !LineageSinkRoutes.isEnabled(this)) {
      return;
    }
    if (data.dynamicTablesWritten.size() >= MAX_DYNAMIC_LINEAGE_TABLES) {
      if (!data.dynamicLineageTruncated) {
        data.dynamicLineageTruncated = true;
        logBasic(
            "More than "
                + MAX_DYNAMIC_LINEAGE_TABLES
                + " distinct target tables; lineage is reported for the first "
                + MAX_DYNAMIC_LINEAGE_TABLES
                + " only");
      }
      return;
    }
    data.dynamicTablesWritten.add(tableName);
  }

  /**
   * Emits one {@code RELATIONAL_IO} write per distinct table seen in the per-row dynamic table-name
   * mode. Called on dispose, once all rows (and therefore all targets) have been processed.
   */
  private void emitDynamicRelationalLineage() {
    if (!meta.isTableNameInField() || data.dynamicTablesWritten.isEmpty()) {
      return;
    }
    String database =
        data.databaseMeta == null ? null : resolve(data.databaseMeta.getDatabaseName());
    String schema = resolve(meta.getSchemaName());
    for (String table : data.dynamicTablesWritten) {
      LineageRelationalIoEmitter.emitTransformRelationalWrite(
          this, data.databaseMeta, database, schema, table, data.insertRowMeta, true, null);
    }
  }

  protected Object[] writeToTable(IRowMeta rowMeta, Object[] r) throws HopException {

    if (r == null) { // Stop: last line or error encountered
      if (isDetailed()) {
        logDetailed("Last line inserted: stop");
      }
      return null;
    }

    PreparedStatement insertStatement = null;
    Object[] insertRowData;
    Object[] outputRowData = r;

    String tableName = null;

    boolean sendToErrorRow = false;
    String errorMessage = null;
    boolean rowIsSafe = false;
    int[] updateCounts = null;
    List<Exception> exceptionsList = null;
    boolean batchProblem = false;
    Object generatedKey = null;

    if (meta.isTableNameInField()) {
      // Cache the position of the table name field
      if (data.indexOfTableNameField < 0) {
        String realTablename = resolve(meta.getTableNameField());
        data.indexOfTableNameField = rowMeta.indexOfValue(realTablename);
        if (data.indexOfTableNameField < 0) {
          String message = "Unable to find table name field [" + realTablename + "] in input row";
          logError(message);
          throw new HopTransformException(message);
        }
        if (!meta.isTableNameInTable() && !meta.isSpecifyFields()) {
          data.insertRowMeta.removeValueMeta(data.indexOfTableNameField);
        }
      }
      tableName = rowMeta.getString(r, data.indexOfTableNameField);
      recordDynamicLineageTarget(tableName);
      if (!meta.isTableNameInTable() && !meta.isSpecifyFields()) {
        // If the name of the table should not be inserted itself, remove the table name
        // from the input row data as well. This forcibly creates a copy of r
        //
        insertRowData = RowDataUtil.removeItem(rowMeta.cloneRow(r), data.indexOfTableNameField);
      } else {
        insertRowData = r;
      }
    } else if (meta.isPartitioningEnabled()
        && (meta.isPartitioningDaily() || meta.isPartitioningMonthly())
        && (!Utils.isEmpty(meta.getPartitioningField()))) {
      // Initialize some stuff!
      if (data.indexOfPartitioningField < 0) {
        data.indexOfPartitioningField = rowMeta.indexOfValue(resolve(meta.getPartitioningField()));
        if (data.indexOfPartitioningField < 0) {
          throw new HopTransformException(
              "Unable to find field [" + meta.getPartitioningField() + "] in the input row!");
        }

        if (meta.isPartitioningDaily()) {
          data.dateFormater = new SimpleDateFormat("yyyyMMdd");
        } else {
          data.dateFormater = new SimpleDateFormat("yyyyMM");
        }
      }

      IValueMeta partitioningValue = rowMeta.getValueMeta(data.indexOfPartitioningField);
      if (!partitioningValue.isDate() || r[data.indexOfPartitioningField] == null) {
        throw new HopTransformException(
            "Sorry, the partitioning field needs to contain a data value and can't be empty!");
      }

      Object partitioningValueData = rowMeta.getDate(r, data.indexOfPartitioningField);
      tableName =
          resolve(meta.getTableName())
              + "_"
              + data.dateFormater.format((Date) partitioningValueData);
      insertRowData = r;
    } else {
      tableName = data.tableName;
      insertRowData = r;
    }

    if (meta.isSpecifyFields()) {
      //
      // The values to insert are those in the fields sections
      //
      insertRowData = new Object[data.valuenrs.length];
      for (int idx = 0; idx < data.valuenrs.length; idx++) {
        insertRowData[idx] = r[data.valuenrs[idx]];
      }
    }

    if (Utils.isEmpty(tableName)) {
      throw new HopTransformException("The tablename is not defined (empty)");
    }

    if (data.recoveredBatches) {
      addToRecoveredBatch(rowMeta, r, tableName, insertRowData, outputRowData);
      // The row leaves once its batch is flushed and its fate is known.
      return null;
    }

    insertStatement = data.preparedStatements.get(tableName);
    if (insertStatement == null) {
      String sql =
          data.db.getInsertStatement(resolve(meta.getSchemaName()), tableName, data.insertRowMeta);
      if (isDetailed()) {
        logDetailed("Prepared statement : " + sql);
      }
      insertStatement = data.db.prepareSql(sql, meta.isReturningGeneratedKeys());
      data.preparedStatements.put(tableName, insertStatement);
    }

    try {
      // For PG & GP, the row runs behind a savepoint, so a failed row is undone on its own and
      // doesn't abort the transaction. Only the INSERT is inside: the commit below ends the
      // savepoint, and on a connection shared by a transactional pipeline the other transforms
      // wait while the section is open.
      //
      PreparedStatement statement = insertStatement;
      Object[] bindRow = insertRowData;
      if (data.useSafePoints) {
        data.db.runInSavepoint(
            () -> {
              insertOneRow(statement, bindRow);
              return null;
            });
      } else {
        insertOneRow(statement, insertRowData);
      }
      if (isRowLevel()) {
        logRowlevel("Written row: " + data.insertRowMeta.getString(insertRowData));
      }

      // Get a commit counter per prepared statement to keep track of separate tables, etc.
      //
      Integer commitCounter = data.commitCounterMap.get(tableName);
      if (commitCounter == null) {
        commitCounter = 1;
      } else {
        commitCounter++;
      }
      data.commitCounterMap.put(tableName, commitCounter);

      // Perform a commit if needed
      //

      if ((data.commitSize > 0) && ((commitCounter % data.commitSize) == 0)) {
        if (data.db.getUseBatchInsert(data.batchMode)) {
          try {
            insertStatement.executeBatch();
            data.db.commit();
            insertStatement.clearBatch();
          } catch (SQLException ex) {
            throw Database.createHopDatabaseBatchException("Error updating batch", ex);
          } catch (Exception ex) {
            throw new HopDatabaseException("Unexpected error inserting row", ex);
          }
        } else {
          // insertRow normal commit
          data.db.commit();
        }
        // Clear the batch/commit counter...
        //
        data.commitCounterMap.put(tableName, 0);
        rowIsSafe = true;
      } else {
        rowIsSafe = false;
      }

      // See if we need to get back the keys as well...
      if (meta.isReturningGeneratedKeys()) {
        RowMetaAndData extraKeys = data.db.getGeneratedKeys(insertStatement);

        if (!extraKeys.getRowMeta().isEmpty()) {
          // Send out the good word!
          // Only 1 key at the moment. (should be enough for now :-)
          generatedKey = extraKeys.getRowMeta().getInteger(extraKeys.getData(), 0);
        } else {
          // we have to throw something here, else we don't know what the
          // type is of the returned key(s) and we would violate our own rule
          // that a hop should always contain rows of the same type.
          throw new HopTransformException(
              "No generated keys while \"return generated keys\" is active!");
        }
      }
    } catch (HopDatabaseBatchException be) {
      errorMessage = be.toString();
      batchProblem = true;
      sendToErrorRow = true;
      updateCounts = be.getUpdateCounts();
      exceptionsList = be.getExceptionsList();

      if (getTransformMeta().isDoingErrorHandling()) {
        data.db.clearBatch(insertStatement);
        data.db.commit(true);
      } else {
        data.db.clearBatch(insertStatement);
        data.db.rollback();
        StringBuilder msg =
            new StringBuilder("Error batch inserting rows into table [" + tableName + "].");
        msg.append(Const.CR);
        msg.append("Errors encountered (first 10):").append(Const.CR);
        for (int x = 0; x < be.getExceptionsList().size() && x < 10; x++) {
          Exception exception = be.getExceptionsList().get(x);
          if (exception.getMessage() != null) {
            msg.append(exception.getMessage()).append(Const.CR);
          }
        }
        throw new HopException(msg.toString(), be);
      }
    } catch (HopDatabaseException dbe) {
      if (getTransformMeta().isDoingErrorHandling()) {
        if (isRowLevel()) {
          logRowlevel("Written row to error handling : " + getInputRowMeta().getString(r));
        }

        sendToErrorRow = true;
        errorMessage = dbe.toString();
      } else {
        if (meta.isIgnoreErrors()) {
          if (data.warnings < 20) {
            if (isBasic()) {
              logBasic(
                  "WARNING: Couldn't insert row into table: "
                      + rowMeta.getString(r)
                      + Const.CR
                      + dbe.getMessage());
            }
          } else if (data.warnings == 20 && isBasic()) {
            logBasic(
                "FINAL WARNING (no more then 20 displayed): Couldn't insert row into table: "
                    + rowMeta.getString(r)
                    + Const.CR
                    + dbe.getMessage());
          }
          data.warnings++;
        } else {
          setErrors(getErrors() + 1);
          data.db.rollback();
          throw new HopException(
              "Error inserting row into table ["
                  + tableName
                  + "] with values: "
                  + rowMeta.getString(r),
              dbe);
        }
      }
    }

    // We need to add a key
    if (generatedKey != null) {
      outputRowData = RowDataUtil.addValueData(outputRowData, rowMeta.size(), generatedKey);
    }

    if (data.batchMode) {
      if (sendToErrorRow) {
        if (batchProblem) {
          data.batchBuffer.add(outputRowData);
          data.batchBindBuffer.add(insertRowData);
          outputRowData = null;

          processBatchException(
              insertStatement, tableName, errorMessage, updateCounts, exceptionsList);
        } else {
          // Simply add this row to the error row
          putError(rowMeta, r, 1L, errorMessage, null, "TOP001");
          outputRowData = null;
        }
      } else {
        data.batchBuffer.add(outputRowData);
        data.batchBindBuffer.add(insertRowData);
        outputRowData = null;

        if (rowIsSafe) { // A commit was done and the rows are all safe (no error)
          for (int i = 0; i < data.batchBuffer.size(); i++) {
            Object[] row = data.batchBuffer.get(i);
            putRow(data.outputRowMeta, row);
            incrementLinesOutput();
          }
          // Clear the buffer
          data.batchBuffer.clear();
          data.batchBindBuffer.clear();
        }
      }
    } else {
      if (sendToErrorRow) {
        putError(rowMeta, r, 1, errorMessage, null, "TOP001");
        outputRowData = null;
      }
    }

    return outputRowData;
  }

  /**
   * Route every row of a failed batch to the regular output or to the error stream.
   *
   * <p>JDBC lets a driver react to a failing statement in more than one way, and the update counts
   * we get back mean different things as a result:
   *
   * <ul>
   *   <li>Some drivers execute the whole batch and hand back one count per row, with {@link
   *       Statement#EXECUTE_FAILED} at the rows that failed. SQL Server, MySQL and H2 do this.
   *   <li>Some stop at the first failure. The array is then <em>shorter</em> than the batch: it
   *       holds only the rows that succeeded before the failure, the row at {@code
   *       updateCounts.length} is the one that failed, and the rows behind it were never sent to
   *       the database at all. Oracle and Derby do this, and a failure on the very first row of the
   *       batch gives us a zero-length array rather than a null one.
   *   <li>Some report nothing usable and we get no counts at all.
   * </ul>
   *
   * <p>Whichever we get, every row in the buffer has to leave here on exactly one of the two
   * streams. Rows the counts array does not account for are not in the database, so they are
   * rejects - silently dropping them is what used to make rows disappear on Oracle.
   */
  @VisibleForTesting
  void processBatchException(
      PreparedStatement insertStatement,
      String tableName,
      String errorMessage,
      int[] updateCounts,
      List<Exception> exceptionsList)
      throws HopException {

    int bufferSize = data.batchBuffer.size();

    // More counts than rows means the counts belong to a different batch than the one we buffered,
    // so the row a count refers to can't be worked out. Reporting the wrong rows on the wrong
    // stream is worse than failing here, where the mismatch is still visible.
    //
    if (updateCounts != null && updateCounts.length > bufferSize) {
      throw new HopException(
          "Unable to attribute batch errors to rows: the database returned "
              + updateCounts.length
              + " update counts for a batch of "
              + bufferSize
              + " buffered rows.");
    }

    // Re-driving the untried rows means binding them again, which is only unambiguous when the
    // whole buffer belongs to the one statement that just failed. With the table name in a field or
    // date partitioning the buffer interleaves rows for several tables, so the tail is reported
    // rather than retried.
    //
    boolean canRetryTail =
        insertStatement != null
            && !meta.isTableNameInField()
            && !meta.isPartitioningEnabled()
            && data.batchBindBuffer.size() == bufferSize;

    int from = 0;
    int[] counts = updateCounts;
    String message = errorMessage;
    List<Exception> exceptions = exceptionsList == null ? List.of() : exceptionsList;

    while (from < bufferSize) {

      // Rows the database told us about, relative to where this attempt started.
      //
      int counted = counts == null ? 0 : counts.length;
      int errNr = 0;
      for (int i = 0; counts != null && i < counted; i++) {
        Object[] row = data.batchBuffer.get(from + i);
        if (counts[i] != Statement.EXECUTE_FAILED) {
          // Anything that isn't EXECUTE_FAILED is a success: a row count, or SUCCESS_NO_INFO from a
          // driver that applied the row without saying how many rows it touched.
          //
          putRow(data.outputRowMeta, row);
          incrementLinesOutput();
        } else {
          String exMessage = message;
          if (errNr < exceptions.size()) {
            exMessage = exceptions.get(errNr).toString();
            errNr++;
          }
          putError(data.outputRowMeta, row, 1L, exMessage, null, "TOP0002");
        }
      }
      from += counted;

      if (from >= bufferSize) {
        break;
      }

      if (counts == null) {
        // The driver gave us nothing to go on: it never said which rows it applied, so retrying
        // could write a row twice. Everything still unaccounted for is reported instead.
        //
        for (int i = from; i < bufferSize; i++) {
          putError(data.outputRowMeta, data.batchBuffer.get(i), 1L, message, null, "TOP0003");
        }
        break;
      }

      // A short counts array means the driver stopped here. This row is the one it refused, and it
      // gets the database's own message.
      //
      putError(data.outputRowMeta, data.batchBuffer.get(from), 1L, message, null, "TOP0002");
      from++;

      if (from >= bufferSize) {
        break;
      }

      if (!canRetryTail) {
        for (int i = from; i < bufferSize; i++) {
          putError(
              data.outputRowMeta,
              data.batchBuffer.get(i),
              1L,
              notAttemptedMessage(message),
              null,
              "TOP0003");
        }
        break;
      }

      // The rows behind the failure were never sent, so they are still owed a write. Re-drive them
      // as one batch: a batch that fails again comes back round this loop, which costs one extra
      // round trip per bad row rather than a round trip per row.
      //
      try {
        counts = retryBatch(insertStatement, from, bufferSize);
        message = errorMessage;
        exceptions = List.of();
      } catch (HopDatabaseBatchException be) {
        counts = be.getUpdateCounts();
        message = be.toString();
        exceptions = be.getExceptionsList() == null ? List.of() : be.getExceptionsList();
      }
    }

    if (isDetailed()) {
      logDetailed(
          "Recovered a failed batch of " + bufferSize + " rows on table [" + tableName + "]");
    }

    // Clear the buffers afterwards...
    data.batchBuffer.clear();
    data.batchBindBuffer.clear();
  }

  /**
   * Re-binds and re-executes rows {@code [from, to)} of the batch buffer on the same prepared
   * statement, and commits them.
   *
   * @return one update count per row submitted, so the caller can attribute them from {@code from}
   * @throws HopDatabaseBatchException if this attempt fails in its turn, carrying whatever the
   *     driver reported about it
   */
  private int[] retryBatch(PreparedStatement insertStatement, int from, int to)
      throws HopException {
    int[] counts;
    try {
      for (int i = from; i < to; i++) {
        data.db.setValues(data.insertRowMeta, data.batchBindBuffer.get(i), insertStatement);
        insertStatement.addBatch();
      }
      counts = insertStatement.executeBatch();
      data.db.commit();
      insertStatement.clearBatch();
    } catch (SQLException ex) {
      data.db.clearBatch(insertStatement);
      data.db.commit(true);
      throw Database.createHopDatabaseBatchException("Error updating batch", ex);
    }
    // executeBatch may return fewer counts than rows submitted; the caller handles that the same
    // way it handles the original short array.
    return counts;
  }

  private String notAttemptedMessage(String errorMessage) {
    return BaseMessages.getString(
        PKG, "TableOutput.Error.RowNotAttempted", Const.NVL(errorMessage, ""));
  }

  /**
   * Adds a row to its table's recovered batch, and flushes that batch once it holds a commit's
   * worth of rows. Only used when the connection's batch failure contract has been validated.
   */
  private void addToRecoveredBatch(
      IRowMeta rowMeta, Object[] r, String tableName, Object[] insertRowData, Object[] outputRow)
      throws HopException {
    // A multi-row batch binds the row when it is flushed, and reports a value that can't be bound
    // then, through rowNotBound. A one-row batch binds it now.
    DmlPlan plan = insertPlan(tableName);
    IRowBatch batch =
        plan != null ? getMultiRowBatch(tableName, plan) : getRecoveredBatch(tableName);
    TableOutputData.PendingRow pending = new TableOutputData.PendingRow(outputRow, rowMeta);
    data.pendingRows.add(pending);
    data.pendingByRow.put(outputRow, pending);
    try {
      batch.add(insertRowData, outputRow);
    } catch (HopDatabaseException dbe) {
      // The row could not even be bound: it never reached the batch.
      data.pendingByRow.remove(outputRow);
      handleUnbatchedRowError(rowMeta, r, tableName, dbe, pending);
      releaseSettledRows();
      return;
    }
    if (isRowLevel()) {
      logRowlevel("Batched row: " + data.insertRowMeta.getString(insertRowData));
    }
    if (batch.size() >= data.commitSize) {
      // Rows are passed on in input order, so a row in the batch of a table that rarely gets one
      // would hold back every row after it. Flushing every table's batch keeps the rows held to
      // less than the commit size per table.
      flushRecoveredBatches();
    }
  }

  /** Runs and commits the batch of every table that has rows waiting, in the order they began. */
  private void flushRecoveredBatches() throws HopException {
    for (Map.Entry<String, IRowBatch> entry : data.batches.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        flushRecoveredBatch(entry.getKey(), entry.getValue());
      }
    }
  }

  /** The recovered batch of a table, created on its first row together with its statement. */
  private IRowBatch getRecoveredBatch(String tableName) throws HopException {
    IRowBatch batch = data.batches.get(tableName);
    if (batch != null) {
      return batch;
    }
    String sql =
        data.db.getInsertStatement(resolve(meta.getSchemaName()), tableName, data.insertRowMeta);
    if (isDetailed()) {
      logDetailed("Prepared statement : " + sql);
    }
    PreparedStatement statement = data.db.prepareSql(sql, false);
    data.preparedStatements.put(tableName, statement);
    batch =
        RecoverableBatch.of(
                data.db,
                statement,
                (st, row) -> data.db.setValues(data.insertRowMeta, row, st),
                data.batchSupport)
            .withStatementFactory(
                () -> {
                  // The driver closed the statement after a failed batch (DuckDB does).
                  PreparedStatement again = data.db.prepareSql(sql, false);
                  data.preparedStatements.put(tableName, again);
                  return again;
                });
    data.batches.put(tableName, batch);
    return batch;
  }

  /**
   * The plan of the INSERT statements of several rows into a table, made on its first batched row,
   * when the insert columns are known; null when the table gets one row per statement. Each table
   * is planned on its own: the database can rule out several rows per statement for one table and
   * not for another (a MySQL table without transactions, say).
   */
  private DmlPlan insertPlan(String tableName) {
    if (!data.multiRowCandidate) {
      return null;
    }
    if (data.insertPlans.containsKey(tableName)) {
      return data.insertPlans.get(tableName);
    }
    int asked = meta.getRowsPerStatement(this);
    String reason = null;
    DmlPlan plan = null;
    DmlSpec spec = null;
    try {
      spec = insertSpec(tableName);
    } catch (HopDatabaseException e) {
      // The one-row INSERT needs no column types, so it still runs.
      reason = "unable to read the column types of table " + tableName + ": " + e.getMessage();
    }
    if (spec != null && spec.getSourceColumns().size() != data.insertRowMeta.size()) {
      // Two fields with the same column name, ignoring case: the one-row INSERT names them both,
      // as it always did, and lets the database decide.
      reason = "two fields write the same column";
    } else if (spec != null
        && !Utils.isEmpty(
            data.databaseMeta
                .getIDatabase()
                .getSqlInsertClauseBeforeFields(data.db, spec.getQuotedTable()))) {
      reason = "the database needs a clause before the column list of an INSERT";
    } else if (spec != null) {
      // Note: on PostgreSQL, reWriteBatchedInserts=true also folds a JDBC batch of these
      // multi-row INSERTs into bigger multi-row INSERTs (in powers of two). The rows come out
      // the same, but the driver then reports SUCCESS_NO_INFO for most entries, and a failure
      // names no batch entry (PostgreSqlDatabaseMeta.getFailedBatchEntry gives -1 for an INSERT
      // of several rows), so a failed JDBC batch is recovered row by row. MultiRowBatch counts
      // a row of a multi-row INSERT as 1 whatever the driver reports.
      try {
        plan =
            DmlSupport.planStatements(
                data.databaseMeta, data.db.getDatabaseMetaData(), spec, asked, this);
        if (plan.getRowsPerStatement() <= 1) {
          reason = plan.getNote() != null ? plan.getNote() : plan.getUnsupportedReason();
          plan = null;
        }
      } catch (HopDatabaseException e) {
        reason = e.getMessage();
        plan = null;
      }
    }
    data.insertPlans.put(tableName, plan);
    if (isDetailed()) {
      if (plan == null) {
        logDetailed(
            "Inserting one row per statement into table "
                + tableName
                + " instead of "
                + asked
                + ": "
                + reason);
      } else {
        logDetailed(
            "Inserting "
                + plan.getRowsPerStatement()
                + " rows per statement into table "
                + tableName
                + (plan.getMaxStatementBytes() > 0
                    ? ", " + plan.getMaxStatementBytes() + " bytes per statement at most"
                    : "")
                + (plan.getNote() == null ? "" : ": " + plan.getNote()));
      }
    }
    return plan;
  }

  /**
   * The INSERT of a table: its columns are the fields of the insert row, in that order, with their
   * types read from the table when the dialect casts the VALUES list of several rows (SQL Server).
   * Made once per table.
   */
  private DmlSpec insertSpec(String tableName) throws HopDatabaseException {
    DmlSpec spec = data.insertSpecs.get(tableName);
    if (spec != null) {
      return spec;
    }
    List<String> names = new ArrayList<>();
    for (IValueMeta valueMeta : data.insertRowMeta.getValueMetaList()) {
      names.add(valueMeta.getName());
    }
    String schema = resolve(meta.getSchemaName());
    DmlSyntax syntax = data.databaseMeta.getIDatabase().getDmlSyntax();
    List<DmlColumn> columns;
    if (syntax != null && syntax.castParameters() && syntax.castValuesLists()) {
      columns = DmlColumns.fromTable(data.db, schema, tableName, names);
    } else {
      columns = names.stream().map(DmlColumn::of).toList();
    }
    String quotedTable = data.databaseMeta.getQuotedSchemaTableCombination(this, schema, tableName);
    spec = DmlSpec.insert(quotedTable, columns);
    data.insertSpecs.put(tableName, spec);
    return spec;
  }

  /** The multi-row batch of a table, created on its first row. */
  private IRowBatch getMultiRowBatch(String tableName, DmlPlan plan) throws HopException {
    IRowBatch batch = data.batches.get(tableName);
    if (batch != null) {
      return batch;
    }
    DmlStatements statements =
        DmlStatements.of(data.db, plan, insertSpec(tableName), data.insertRowMeta);
    if (isDetailed()) {
      // The statement of one row: the one of several rows repeats its VALUES list.
      logDetailed(
          "Prepared statement : "
              + statements.statement(1).getSql()
              + " (up to "
              + plan.getRowsPerStatement()
              + " rows per statement)");
    }
    batch = statements.newBatch(data.db, data.batchSupport);
    data.batches.put(tableName, batch);
    return batch;
  }

  /**
   * Runs and commits a table's recovered batch. With an error hop every row goes to the output or
   * the error stream once its fate is known; without one a failure rolls back and stops the
   * transform.
   */
  private void flushRecoveredBatch(String tableName, IRowBatch batch) throws HopException {
    if (getTransformMeta().isDoingErrorHandling()) {
      batch.flush(recoveredBatchListener());
      releaseSettledRows();
      return;
    }
    try {
      batch.flushOrFail(recoveredBatchListener());
      releaseSettledRows();
    } catch (HopDatabaseBatchException be) {
      setErrors(1);
      discardRecoveredBatches();
      data.db.rollback();
      StringBuilder msg =
          new StringBuilder("Error batch inserting rows into table [" + tableName + "].");
      msg.append(Const.CR);
      msg.append("Errors encountered (first 10):").append(Const.CR);
      List<Exception> exceptions = be.getExceptionsList();
      for (int x = 0; exceptions != null && x < exceptions.size() && x < 10; x++) {
        Exception exception = exceptions.get(x);
        if (exception.getMessage() != null) {
          msg.append(exception.getMessage()).append(Const.CR);
        }
      }
      throw new HopException(msg.toString(), be);
    } catch (MultiRowBatch.UnboundRowException unbound) {
      // A value that can't be bound stops the transform, as it does with one row per statement
      // when the row arrives (handleUnbatchedRowError); a multi-row batch finds it when it binds.
      TableOutputData.PendingRow pending = data.pendingByRow.get(unbound.getOutputRow());
      setErrors(getErrors() + 1);
      discardRecoveredBatches();
      data.db.rollback();
      throw new HopException(
          "Error inserting row into table [" + tableName + "] with values: " + rowValues(pending),
          unbound.getCause());
    }
  }

  /**
   * The values of a waiting row for a message, as its fields show them, or as the plain values when
   * a value doesn't fit its field (which may be why it couldn't be bound).
   */
  private static String rowValues(TableOutputData.PendingRow pending) {
    if (pending == null) {
      return "";
    }
    try {
      return pending.rowMeta.getString(pending.row);
    } catch (HopValueException e) {
      return Arrays.toString(pending.row);
    }
  }

  /**
   * Drops every row still waiting in a recovered batch, without running it.
   *
   * @return the number of rows dropped
   */
  private int discardRecoveredBatches() {
    int dropped = 0;
    if (data.batches != null) {
      for (IRowBatch batch : data.batches.values()) {
        dropped += batch.discard().size();
      }
    }
    // Rows waiting for an earlier row are dropped too: nothing of a failed run is passed on.
    data.pendingRows.clear();
    data.pendingByRow.clear();
    return dropped;
  }

  /**
   * Passes on, in input order, every row at the front of the queue whose fate is known: written
   * rows to the main output, refused rows to the error hop. A row waits for the rows before it,
   * also when those are in the batch of another table.
   */
  private void releaseSettledRows() throws HopException {
    while (!data.pendingRows.isEmpty() && data.pendingRows.peekFirst().settled) {
      TableOutputData.PendingRow pending = data.pendingRows.pollFirst();
      if (pending.written) {
        putRow(data.outputRowMeta, pending.row);
        incrementLinesOutput();
      } else {
        putError(pending.rowMeta, pending.row, 1L, pending.message, null, pending.errorCode);
      }
    }
  }

  /** Receives the verdict on every row of a flushed recovered batch. */
  private IBatchRowListener recoveredBatchListener() {
    return new IBatchRowListener() {
      @Override
      public void rowWritten(Object[] outputRow) {
        TableOutputData.PendingRow pending = data.pendingByRow.remove(outputRow);
        pending.settled = true;
        pending.written = true;
      }

      @Override
      public void rowRejected(Object[] outputRow, String message, boolean attempted) {
        TableOutputData.PendingRow pending = data.pendingByRow.remove(outputRow);
        pending.settled = true;
        pending.message = message;
        pending.errorCode = attempted ? "TOP0002" : "TOP0003";
      }

      @Override
      public void rowNotBound(Object[] outputRow, HopDatabaseException error) {
        // The same verdict as for a row that can't be bound when it arrives, see
        // handleUnbatchedRowError: a multi-row batch binds its rows when it is flushed.
        TableOutputData.PendingRow pending = data.pendingByRow.remove(outputRow);
        pending.settled = true;
        pending.message = error.toString();
        pending.errorCode = "TOP001";
      }
    };
  }

  /**
   * A row that could not be bound, so it never reached a recovered batch: it goes to the error hop,
   * in its turn, or stops the transform. "Ignore insert errors" is not available with batch
   * inserts, as documented.
   */
  private void handleUnbatchedRowError(
      IRowMeta rowMeta,
      Object[] r,
      String tableName,
      HopException dbe,
      TableOutputData.PendingRow pending)
      throws HopException {
    if (getTransformMeta().isDoingErrorHandling()) {
      if (isRowLevel()) {
        logRowlevel("Written row to error handling : " + getInputRowMeta().getString(r));
      }
      // Passed on in its turn, after the rows before it.
      pending.settled = true;
      pending.message = dbe.toString();
      pending.errorCode = "TOP001";
    } else {
      setErrors(getErrors() + 1);
      discardRecoveredBatches();
      data.db.rollback();
      throw new HopException(
          "Error inserting row into table [" + tableName + "] with values: " + rowMeta.getString(r),
          dbe);
    }
  }

  /** Binds and sends one row; the commit is handled by this transform, not by insertRow. */
  private void insertOneRow(PreparedStatement statement, Object[] insertRowData)
      throws HopDatabaseException {
    data.db.setValues(data.insertRowMeta, insertRowData, statement);
    data.db.insertRow(statement, data.batchMode, false);
  }

  @Override
  public boolean init() {

    if (super.init()) {
      try {
        String commitSize = resolve(meta.getCommitSize());
        String expandedCommitSize = Const.expandIntegerString(commitSize);
        data.commitSize =
            Integer.parseInt(expandedCommitSize != null ? expandedCommitSize : commitSize);

        if (Utils.isEmpty(meta.getConnection()))
          throw new HopException(BaseMessages.getString(PKG, "TableOutput.Init.ConnectionMissing"));

        data.databaseMeta = this.getPipelineMeta().findDatabase(meta.getConnection(), variables);
        IDatabase dbInterface = data.databaseMeta.getIDatabase();

        if (!dbInterface.supportsStandardTableOutput()) {
          throw new HopException(dbInterface.getUnsupportedTableOutputMessage());
        }

        data.db = new Database(this, this, data.databaseMeta);
        data.db.connect();

        if (isBasic()) {
          logBasic(
              "Connected to database ["
                  + variables.resolve(meta.getConnection())
                  + "] (commit="
                  + data.commitSize
                  + ")");
        }

        boolean unlimitedCommit = data.commitSize <= 0;

        // Postpone commit as long as possible.
        //
        if (data.commitSize == 0) {
          data.commitSize = Integer.MAX_VALUE;
        }
        data.db.setCommit(data.commitSize);

        // How a failed batch is handled on this connection. Only a contract validated against the
        // database (or chosen on the connection) recovers batches with RecoverableBatch; every
        // other connection keeps the batch handling below, exactly as it always was.
        //
        data.batchSupport = BatchSupport.of(dbInterface, this);
        if (isDetailed()) {
          logDetailed("Batch support: " + data.batchSupport.describe());
        }
        data.recoveredBatches =
            data.batchSupport.isRecovered()
                && meta.isUseBatchUpdate()
                && !unlimitedCommit
                && !meta.isReturningGeneratedKeys()
                && !data.db.isAutoCommit()
                && data.db.getUseBatchInsert(true);

        if (data.recoveredBatches) {
          data.useSafePoints = false;
          data.batchMode = true;
          data.multiRowCandidate = multiRowCandidate();
        } else {
          // Batch updates are not supported on PostgreSQL (and look-a-likes)
          // together with error handling
          // For these situations we can use savepoints to help out.
          //
          data.useSafePoints =
              data.databaseMeta.getIDatabase().isUseSafePoints()
                  && getTransformMeta().isDoingErrorHandling();

          // Disable batch mode in case
          // - we use an unlimited commit size
          // - if we need to pick up auto-generated keys
          // - if you are running the pipeline as a single database transaction (unique connections)
          // - if we are reverting to save-points
          //
          data.batchMode =
              meta.isUseBatchUpdate()
                  && !unlimitedCommit
                  && !meta.isReturningGeneratedKeys()
                  && !data.useSafePoints;

          // give a warning that batch mode operation in combination with transform error handling
          // can lead to incorrectly processed rows.
          //
          if (getTransformMeta().isDoingErrorHandling()
              && !dbInterface.IsSupportsErrorHandlingOnBatchUpdates()
              && isBasic()) {
            logBasic(
                BaseMessages.getString(
                    PKG,
                    "TableOutput.Warning.ErrorHandlingIsNotFullySupportedWithBatchProcessing"));
          }
        }

        if (!meta.isPartitioningEnabled() && !meta.isTableNameInField()) {
          data.tableName = resolve(meta.getTableName());
        }

        return true;
      } catch (HopException e) {
        logError("An error occurred initializing this transform: " + e.getMessage());
        stopAll();
        setErrors(1);
      }
    }
    return false;
  }

  /**
   * Truncate the table and update its structure once for the whole run, before any copy, Spark
   * partition or Beam bundle writes a row: otherwise every one of them would, and they would
   * truncate each other's rows or change the table at the same time.
   */
  @Override
  public void prepareOnce() throws HopException {
    if (meta.isTruncateTable() && !meta.isOnlyWhenHaveRows()) {
      truncateTable();
    }
    if (meta.isAutoUpdateTableStructure()) {
      updateTableStructure();
      if (data.preparedStatements != null) {
        data.preparedStatements.clear();
      }
    }
  }

  /**
   * Whether several rows per INSERT may be used; the dialect is asked on the first batched row.
   * Otherwise the detailed log says why one row per statement is written.
   */
  private boolean multiRowCandidate() throws HopDatabaseException {
    int asked = meta.getRowsPerStatement(this);
    if (asked <= 1) {
      return false;
    }
    String reason = null;
    if (meta.isIgnoreErrors()) {
      // A row that can't be bound is ignored with a warning on the one-row path; a multi-row
      // batch only finds out when it is flushed.
      reason = "insert errors are ignored";
    } else if (!Utils.isEmpty(data.db.getConnectionGroup())) {
      reason = "the connection is shared by a transactional pipeline";
    } else {
      try {
        if (!data.db.getDatabaseMetaData().supportsTransactions()) {
          reason = "the database has no transactions to recover a failed statement with";
        }
      } catch (SQLException e) {
        reason = "unable to find out if the database supports transactions: " + e.getMessage();
      }
    }
    if (reason != null) {
      if (isDetailed()) {
        logDetailed("Inserting one row per statement instead of " + asked + ": " + reason);
      }
      return false;
    }
    return true;
  }

  void truncateTable() throws HopDatabaseException {
    if (!meta.isPartitioningEnabled()
        && !meta.isTableNameInField()
        && (meta.isTruncateTable() && ((getCopy() == 0) || !Utils.isEmpty(getPartitionId())))) {
      // Only the first one truncates in a non-partitioned transform copy
      //
      data.db.truncateTable(resolve(meta.getSchemaName()), resolve(meta.getTableName()));
    }
  }

  void updateTableStructure() throws HopException {
    if (!meta.isPartitioningEnabled() && !meta.isTableNameInField() && (getCopy() == 0)
        || !Utils.isEmpty(getPartitionId())) {
      // Only the first one updates table structure in a non-partitioned transform copy
      String schemaName = resolve(meta.getSchemaName());
      String tableName = resolve(meta.getTableName());
      String fullTableName =
          data.databaseMeta.getQuotedSchemaTableCombination(this, schemaName, tableName);

      // Handle drop and recreate option
      if (meta.isAlwaysDropAndRecreate()) {
        dropTable(fullTableName);
      }

      // Ensure table exists (same logic for both options)
      ensureTableExists(fullTableName, schemaName, tableName);

      // Add missing columns if option is enabled
      if (meta.isAddColumns()) {
        addMissingColumns(fullTableName, schemaName, tableName);
      }

      // Drop surplus columns if option is enabled
      if (meta.isDropColumns()) {
        dropSurplusColumns(fullTableName, schemaName, tableName);
      }

      // Change column types if option is enabled
      if (meta.isChangeColumnTypes()) {
        changeColumnTypes(fullTableName, schemaName, tableName);
      }
    }
  }

  private void dropTable(String fullTableName) throws HopException {
    if (isBasic()) {
      logBasic("Dropping table: " + fullTableName);
    }
    String dropSql = data.databaseMeta.getDropTableIfExistsStatement(fullTableName);
    if (!Utils.isEmpty(dropSql)) {
      try {
        data.db.execStatement(dropSql);
        // Commit the DDL to finalize table drop
        data.db.commit();
        if (isBasic()) {
          logBasic("Dropped table: " + fullTableName);
        }
      } catch (Exception e) {
        logDetailed("Drop table failed (may not exist): " + e.getMessage());
        // Rollback transaction to clear any aborted state
        try {
          data.db.rollback();
          logDetailed("Rolled back transaction after drop table failure");
        } catch (Exception rollbackException) {
          logDetailed(COULD_NOT_ROLLBACK_TRANSACTION + rollbackException.getMessage());
        }
      }
    }
  }

  void ensureTableExists(String fullTableName, String schemaName, String tableName)
      throws HopException {
    // Try to create the table - if it already exists, the creation will fail
    // This avoids the transaction-aborting table existence check
    try {
      createTable(fullTableName);
    } catch (HopException e) {
      // If creation failed, rollback and verify the table actually exists
      // This handles the case where table creation failed for a reason other than "already exists"
      if (isDetailed()) {
        logDetailed("Table creation failed or table already exists, verifying...");
      }

      // Rollback to clear any aborted transaction state before checking existence
      try {
        data.db.rollback();
        if (isDetailed()) {
          logDetailed("Rolled back transaction after create table failure in ensureTableExists");
        }
      } catch (Exception rollbackException) {
        if (isDetailed()) {
          logDetailed(COULD_NOT_ROLLBACK_TRANSACTION + rollbackException.getMessage());
        }
      }

      boolean tableExists = checkTableExists(schemaName, tableName, true);
      if (!tableExists) {
        // Table still doesn't exist after failed creation attempt - this is a real error
        throw new HopException(
            "Failed to create table " + fullTableName + " and table does not exist", e);
      } else {
        if (isDetailed()) {
          logDetailed("Table already exists: " + fullTableName);
        }
      }
    }
  }

  private boolean checkTableExists(String schemaName, String tableName, boolean bypassCache)
      throws HopException {
    if (bypassCache) {
      // Clear cache to ensure fresh state
      try {
        org.apache.hop.core.DbCache.getInstance().clear(data.databaseMeta.getName());
        if (isDetailed()) {
          logDetailed("Cleared database cache to ensure fresh table state");
        }
      } catch (Exception cacheException) {
        logError(COULD_NOT_CLEAR_DATABASE_CACHE + cacheException.getMessage());
      }
    }

    try {
      return data.db.checkTableExists(schemaName, tableName);
    } catch (Exception e) {
      logDetailed("Table existence check failed, assuming table doesn't exist: " + e.getMessage());
      // Rollback transaction to clear any aborted state
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after table existence check failure");
      } catch (Exception rollbackException) {
        logDetailed(COULD_NOT_ROLLBACK_TRANSACTION + rollbackException.getMessage());
      }
      return false; // Assume table doesn't exist if check fails
    }
  }

  private void createTable(String fullTableName) throws HopException {
    if (isBasic()) {
      logBasic("Creating table: " + fullTableName);
    }
    IRowMeta inputRowMeta = getInputRowMeta();
    String createSql =
        data.db.getCreateTableStatement(fullTableName, inputRowMeta, null, false, null, true);
    if (!Utils.isEmpty(createSql)) {
      if (isDetailed()) {
        logDetailed("CREATE TABLE SQL: " + createSql);
      }
      data.db.execStatement(createSql);
      // Commit the DDL to finalize table creation
      data.db.commit();
      if (isBasic()) {
        logBasic("Successfully created and committed table: " + fullTableName);
      }
      // Clear cache after successful creation to ensure fresh state
      try {
        org.apache.hop.core.DbCache.getInstance().clear(data.databaseMeta.getName());
        if (isDetailed()) {
          logDetailed("Cleared database cache after table creation");
        }
      } catch (Exception cacheException) {
        logError(COULD_NOT_CLEAR_DATABASE_CACHE + cacheException.getMessage());
      }
    }
  }

  private void addMissingColumns(String fullTableName, String schemaName, String tableName)
      throws HopException {
    if (isDetailed()) {
      logDetailed("Checking for missing columns in table: " + fullTableName);
    }

    try {
      // Get the current table structure from the database
      IRowMeta tableFields = data.db.getTableFieldsMeta(schemaName, tableName);
      if (tableFields == null) {
        if (isDetailed()) {
          logDetailed(COULD_NOT_RETRIEVE_TABLE_STRUCTURE_FOR + fullTableName);
        }
        return;
      }

      // Get the incoming stream structure
      IRowMeta streamFields = getInputRowMeta();
      if (streamFields == null) {
        if (isDetailed()) {
          logDetailed(NO_INCOMING_STREAM_FIELDS_AVAILABLE);
        }
        return;
      }

      // Find columns that exist in the stream but not in the table
      List<IValueMeta> missingColumns = new ArrayList<>();
      for (IValueMeta streamField : streamFields.getValueMetaList()) {
        String fieldName = streamField.getName();
        if (tableFields.searchValueMeta(fieldName) == null) {
          missingColumns.add(streamField);
          if (isBasic()) {
            logBasic("Found missing column: " + fieldName + " (" + streamField.getTypeDesc() + ")");
          }
        }
      }

      // Add each missing column
      if (!missingColumns.isEmpty()) {
        for (IValueMeta missingColumn : missingColumns) {
          addColumn(fullTableName, missingColumn);
        }
        if (isBasic()) {
          logBasic(
              "Added " + missingColumns.size() + " missing column(s) to table: " + fullTableName);
        }

        // Clear cache after modifications
        try {
          org.apache.hop.core.DbCache.getInstance().clear(data.databaseMeta.getName());
          if (isDetailed()) {
            logDetailed("Cleared database cache after adding columns");
          }
        } catch (Exception cacheException) {
          logError(COULD_NOT_CLEAR_DATABASE_CACHE + cacheException.getMessage());
        }
      } else {
        if (isDetailed()) {
          logDetailed("No missing columns found in table: " + fullTableName);
        }
      }

    } catch (Exception e) {
      logError("Error checking/adding missing columns: " + e.getMessage(), e);
      // Rollback to clear any aborted transaction state
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after add columns failure");
      } catch (Exception rollbackException) {
        logDetailed(COULD_NOT_ROLLBACK_TRANSACTION + rollbackException.getMessage());
      }
      throw new HopException("Failed to add missing columns to table " + fullTableName, e);
    }
  }

  private void addColumn(String fullTableName, IValueMeta column) throws HopException {
    if (isDetailed()) {
      logDetailed("Adding column: " + column.getName() + " to table: " + fullTableName);
    }

    try {
      String addColumnStatement =
          data.databaseMeta.getAddColumnStatement(fullTableName, column, null, false, null, false);

      if (!Utils.isEmpty(addColumnStatement)) {
        if (isDetailed()) {
          logDetailed("ALTER TABLE SQL: " + addColumnStatement);
        }
        data.db.execStatement(addColumnStatement);
        // Commit the DDL to finalize column addition
        data.db.commit();
        if (isBasic()) {
          logBasic("Successfully added column: " + column.getName());
        }
      }
    } catch (Exception e) {
      logError("Error adding column " + column.getName() + ": " + e.getMessage(), e);
      // Rollback to clear any aborted transaction state
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after add column failure");
      } catch (Exception rollbackException) {
        logDetailed(COULD_NOT_ROLLBACK_TRANSACTION + rollbackException.getMessage());
      }
      throw new HopException("Failed to add column " + column.getName(), e);
    }
  }

  private void dropSurplusColumns(String fullTableName, String schemaName, String tableName)
      throws HopException {
    if (isDetailed()) {
      logDetailed("Checking for surplus columns in table: " + fullTableName);
    }

    try {
      // Get the current table structure from the database
      IRowMeta tableFields = data.db.getTableFieldsMeta(schemaName, tableName);
      if (tableFields == null) {
        if (isDetailed()) {
          logDetailed(COULD_NOT_RETRIEVE_TABLE_STRUCTURE_FOR + fullTableName);
        }
        return;
      }

      // Get the incoming stream structure
      IRowMeta streamFields = getInputRowMeta();
      if (streamFields == null) {
        if (isDetailed()) {
          logDetailed(NO_INCOMING_STREAM_FIELDS_AVAILABLE);
        }
        return;
      }

      // Find columns that exist in the table but not in the stream
      List<IValueMeta> surplusColumns = new ArrayList<>();
      for (IValueMeta tableField : tableFields.getValueMetaList()) {
        String fieldName = tableField.getName();
        if (streamFields.searchValueMeta(fieldName) == null) {
          surplusColumns.add(tableField);
          if (isBasic()) {
            logBasic("Found surplus column: " + fieldName + " (" + tableField.getTypeDesc() + ")");
          }
        }
      }

      // Drop each surplus column
      if (!surplusColumns.isEmpty()) {
        if (isBasic()) {
          logBasic(
              "WARNING: Dropping "
                  + surplusColumns.size()
                  + " column(s) from table: "
                  + fullTableName
                  + " - THIS WILL RESULT IN DATA LOSS");
        }
        for (IValueMeta surplusColumn : surplusColumns) {
          dropColumn(fullTableName, surplusColumn);
        }
        if (isBasic()) {
          logBasic(
              "Dropped "
                  + surplusColumns.size()
                  + " surplus column(s) from table: "
                  + fullTableName);
        }

        // Clear cache after modifications
        try {
          org.apache.hop.core.DbCache.getInstance().clear(data.databaseMeta.getName());
          if (isDetailed()) {
            logDetailed("Cleared database cache after dropping columns");
          }
        } catch (Exception cacheException) {
          logError(COULD_NOT_CLEAR_DATABASE_CACHE + cacheException.getMessage());
        }
      } else {
        if (isDetailed()) {
          logDetailed("No surplus columns found in table: " + fullTableName);
        }
      }

    } catch (Exception e) {
      logError("Error checking/dropping surplus columns: " + e.getMessage(), e);
      // Rollback to clear any aborted transaction state
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after drop columns failure");
      } catch (Exception rollbackException) {
        logDetailed(COULD_NOT_ROLLBACK_TRANSACTION + rollbackException.getMessage());
      }
      throw new HopException("Failed to drop surplus columns from table " + fullTableName, e);
    }
  }

  private void dropColumn(String fullTableName, IValueMeta column) throws HopException {
    if (isDetailed()) {
      logDetailed("Dropping column: " + column.getName() + " from table: " + fullTableName);
    }

    try {
      String dropColumnStatement =
          data.databaseMeta.getDropColumnStatement(fullTableName, column, null, false, null, false);

      if (!Utils.isEmpty(dropColumnStatement)) {
        if (isDetailed()) {
          logDetailed("ALTER TABLE SQL: " + dropColumnStatement);
        }
        data.db.execStatement(dropColumnStatement);
        // Commit the DDL to finalize column drop
        data.db.commit();
        if (isBasic()) {
          logBasic("Successfully dropped column: " + column.getName());
        }
      }
    } catch (Exception e) {
      logError("Error dropping column " + column.getName() + ": " + e.getMessage(), e);
      // Rollback to clear any aborted transaction state
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after drop column failure");
      } catch (Exception rollbackException) {
        logDetailed(COULD_NOT_ROLLBACK_TRANSACTION + rollbackException.getMessage());
      }
      throw new HopException("Failed to drop column " + column.getName(), e);
    }
  }

  private void changeColumnTypes(String fullTableName, String schemaName, String tableName)
      throws HopException {
    if (isDetailed()) {
      logDetailed("Checking for column type mismatches in table: " + fullTableName);
    }

    try {
      // Get the current table structure from the database
      IRowMeta tableFields = data.db.getTableFieldsMeta(schemaName, tableName);
      if (tableFields == null) {
        if (isDetailed()) {
          logDetailed(COULD_NOT_RETRIEVE_TABLE_STRUCTURE_FOR + fullTableName);
        }
        return;
      }

      // Get the incoming stream structure
      IRowMeta streamFields = getInputRowMeta();
      if (streamFields == null) {
        if (isDetailed()) {
          logDetailed(NO_INCOMING_STREAM_FIELDS_AVAILABLE);
        }
        return;
      }

      // Find columns where types don't match
      List<IValueMeta> columnsToModify = new ArrayList<>();
      for (IValueMeta streamField : streamFields.getValueMetaList()) {
        String fieldName = streamField.getName();
        IValueMeta tableField = tableFields.searchValueMeta(fieldName);

        if (tableField != null && !typesAreCompatible(tableField, streamField)) {
          // Column exists in both, check if types are compatible
          columnsToModify.add(streamField);
          if (isBasic()) {
            logBasic(
                "Found type mismatch for column: "
                    + fieldName
                    + " (table: "
                    + tableField.getTypeDesc()
                    + ", stream: "
                    + streamField.getTypeDesc()
                    + ")");
          }
        }
      }

      // Modify each column that needs type change
      if (!columnsToModify.isEmpty()) {
        if (isBasic()) {
          logBasic(
              "WARNING: Changing data types for "
                  + columnsToModify.size()
                  + " column(s) in table: "
                  + fullTableName
                  + " - THIS MAY RESULT IN DATA LOSS");
        }
        for (IValueMeta columnToModify : columnsToModify) {
          modifyColumn(fullTableName, columnToModify);
        }
        if (isBasic()) {
          logBasic(
              "Modified " + columnsToModify.size() + " column type(s) in table: " + fullTableName);
        }

        // Clear cache after modifications
        try {
          org.apache.hop.core.DbCache.getInstance().clear(data.databaseMeta.getName());
          if (isDetailed()) {
            logDetailed("Cleared database cache after modifying column types");
          }
        } catch (Exception cacheException) {
          logError(COULD_NOT_CLEAR_DATABASE_CACHE + cacheException.getMessage());
        }
      } else {
        if (isDetailed()) {
          logDetailed("No column type mismatches found in table: " + fullTableName);
        }
      }

    } catch (Exception e) {
      logError("Error checking/changing column types: " + e.getMessage(), e);
      // Rollback to clear any aborted transaction state
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after change column types failure");
      } catch (Exception rollbackException) {
        logDetailed(COULD_NOT_ROLLBACK_TRANSACTION + rollbackException.getMessage());
      }
      throw new HopException("Failed to change column types in table " + fullTableName, e);
    }
  }

  boolean typesAreCompatible(IValueMeta tableField, IValueMeta streamField) {
    // Same type - compatible
    if (tableField.getType() == streamField.getType()) {
      // For strings, check if stream length is greater than table length
      if (tableField.getType() == IValueMeta.TYPE_STRING) {
        int tableLength = tableField.getLength();
        int streamLength = streamField.getLength();
        // If stream has larger or undefined length, types are not compatible
        if (streamLength > tableLength || (streamLength < 0 && tableLength > 0)) {
          return false;
        }
      }
      // For numbers with precision, check if precision/scale differs
      if ((tableField.getType() == IValueMeta.TYPE_NUMBER
              || tableField.getType() == IValueMeta.TYPE_BIGNUMBER)
          && (tableField.getPrecision() != streamField.getPrecision()
              || tableField.getLength() != streamField.getLength())) {
        return false;
      }
      return true;
    }

    // Different types - generally not compatible, but some conversions are safe
    // e.g., INTEGER -> BIGINT, but we'll require explicit type match for safety
    return false;
  }

  private void modifyColumn(String fullTableName, IValueMeta column) throws HopException {
    if (isDetailed()) {
      logDetailed(
          "Modifying column: "
              + column.getName()
              + " to type: "
              + column.getTypeDesc()
              + " in table: "
              + fullTableName);
    }

    try {
      // For type changes, use drop/recreate approach as it's simpler and more reliable
      // The complex modify statement from DatabaseMeta often fails with type conversions
      if (isDetailed()) {
        logDetailed("Using drop/recreate approach for column type change: " + column.getName());
      }

      // Drop the existing column
      String dropColumnStatement =
          data.databaseMeta.getDropColumnStatement(fullTableName, column, null, false, null, false);
      if (!Utils.isEmpty(dropColumnStatement)) {
        if (isDetailed()) {
          logDetailed("DROP COLUMN SQL: " + dropColumnStatement);
        }
        data.db.execStatement(dropColumnStatement);
        data.db.commit();
        if (isDetailed()) {
          logDetailed("Dropped column: " + column.getName());
        }
      }

      // Add the column with new type
      String addColumnStatement =
          data.databaseMeta.getAddColumnStatement(fullTableName, column, null, false, null, false);
      if (!Utils.isEmpty(addColumnStatement)) {
        if (isDetailed()) {
          logDetailed("ADD COLUMN SQL: " + addColumnStatement);
        }
        data.db.execStatement(addColumnStatement);
        data.db.commit();
        if (isDetailed()) {
          logDetailed("Added column with new type: " + column.getName());
        }
      }

      if (isBasic()) {
        logBasic("Successfully modified column type (via drop/recreate): " + column.getName());
      }
    } catch (Exception e) {
      logError("Error modifying column " + column.getName() + ": " + e.getMessage(), e);
      // Rollback to clear any aborted transaction state
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after modify column failure");
      } catch (Exception rollbackException) {
        logDetailed(COULD_NOT_ROLLBACK_TRANSACTION + rollbackException.getMessage());
      }
      throw new HopException("Failed to modify column " + column.getName(), e);
    }
  }

  @Override
  public void dispose() {

    emitDynamicRelationalLineage();

    if (data.db != null) {
      try {
        emptyAndCommitBatchBuffers(true);
      } finally {
        // A multi-row batch prepared its statements itself; a one-row batch's statement is closed
        // with the others below.
        if (data.batches != null) {
          for (IRowBatch batch : data.batches.values()) {
            batch.close();
          }
        }
        try {
          // close prepared statements
          for (Map.Entry<String, PreparedStatement> preparedStatement :
              data.preparedStatements.entrySet()) {
            preparedStatement.getValue().close();
          }
        } catch (Exception e) {
          logError("An error occurred closing the prepared statements: " + e.getMessage());
        }

        data.db.disconnect();
        // Free data structures to enable GC
        data.db = null;
        data.preparedStatements = null;
        data.batchBuffer = null;
        data.batchBindBuffer = null;
        data.batches = null;
        data.commitCounterMap = null;
        data.outputRowMeta = null;
      }
    }

    super.dispose();
  }

  /**
   * At the end of the rows (or of a single-threaded iteration): runs what is still waiting in the
   * recovered batches, or drops it when the transform failed or was stopped.
   */
  private void flushOrDiscardRecoveredBatches(boolean dispose) {
    try {
      if (getErrors() > 0 || isStopped()) {
        int dropped = discardRecoveredBatches();
        if (dropped > 0) {
          logError(dropped + " batched rows were not written: the transform failed or was stopped");
        }
      } else {
        flushRecoveredBatches();
        data.db.commit();
      }
    } catch (Exception e) {
      logError("Unexpected error writing the last batch of rows.", e);
      setErrors(1);
      discardRecoveredBatches();
      stopAll();
    } finally {
      // Only at the end of the run: batchComplete() flushes after every iteration of a
      // single-threaded run, and the transforms after this one still read what follows
      if (dispose) {
        setOutputDone();
      }
      if (getErrors() > 0) {
        try {
          data.db.rollback();
        } catch (HopDatabaseException e) {
          logError("Unexpected error rolling back the database connection.", e);
        }
      }
    }
  }

  // Force the batched up rows to the database in a single-threaded scenario
  // (Beam as well)
  //
  @Override
  public void batchComplete() throws HopException {
    emptyAndCommitBatchBuffers(false);
  }

  private void emptyAndCommitBatchBuffers(boolean dispose) {
    if (data.recoveredBatches) {
      flushOrDiscardRecoveredBatches(dispose);
      return;
    }
    // Which statement was being flushed when it threw, so a failure can be recovered against the
    // statement that actually produced it.
    PreparedStatement flushingStatement = null;
    String flushingTable = null;
    try {
      for (String schemaTable : data.preparedStatements.keySet()) {
        // Get a commit counter per prepared statement to keep track of separate tables, etc.
        //
        Integer batchCounter = data.commitCounterMap.get(schemaTable);
        if (batchCounter == null || batchCounter == 0) {
          continue; // Skip this one, no work required
        }

        PreparedStatement insertStatement = data.preparedStatements.get(schemaTable);
        flushingStatement = insertStatement;
        flushingTable = schemaTable;
        data.db.emptyAndCommit(insertStatement, data.batchMode, batchCounter, dispose);
        data.commitCounterMap.put(schemaTable, 0);
      }
      for (int i = 0; i < data.batchBuffer.size(); i++) {
        Object[] row = data.batchBuffer.get(i);
        putRow(data.outputRowMeta, row);
        incrementLinesOutput();
      }
      // Clear the buffer
      data.batchBuffer.clear();
      data.batchBindBuffer.clear();
    } catch (HopDatabaseBatchException be) {
      if (getTransformMeta().isDoingErrorHandling()) {
        // Right at the back we are experiencing a batch commit problem...
        // OK, we have the numbers...
        try {
          // The statement is still open here: Database.emptyAndCommit only closes it after a
          // successful executeBatch, so a failure leaves it usable and dispose()'s own finally
          // closes it afterwards. The last partial batch gets the same recovery as any other.
          processBatchException(
              flushingStatement,
              flushingTable,
              be.toString(),
              be.getUpdateCounts(),
              be.getExceptionsList());
        } catch (HopException e) {
          logError("Unexpected error processing batch error", e);
          setErrors(1);
          stopAll();
        }
      } else {
        logError("Unexpected batch update error committing the database connection.", be);
        setErrors(1);
        stopAll();
      }
    } catch (Exception dbe) {
      logError("Unexpected error committing the database connection.", dbe);
      logError(Const.getStackTracker(dbe));
      setErrors(1);
      stopAll();
    } finally {
      // Only at the end of the run: batchComplete() flushes after every iteration of a
      // single-threaded run, and the transforms after this one still read what follows
      if (dispose) {
        setOutputDone();
      }

      if (getErrors() > 0) {
        try {
          data.db.rollback();
        } catch (HopDatabaseException e) {
          logError("Unexpected error rolling back the database connection.", e);
        }
      }
    }
  }
}
