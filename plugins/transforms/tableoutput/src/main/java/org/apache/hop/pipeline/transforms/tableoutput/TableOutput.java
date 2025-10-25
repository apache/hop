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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Writes rows to a database table. */
public class TableOutput extends BaseTransform<TableOutputMeta, TableOutputData> {

  private static final Class<?> PKG = TableOutputMeta.class;

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
      if (first && meta.isTruncateTable() && !meta.isOnlyWhenHaveRows()) {
        truncateTable();
      }
      return false;
    }

    if (first) {
      first = false;
      if (meta.isTruncateTable()) {
        truncateTable();
      }

      // Handle automatic table structure updates
      if (meta.isAutoUpdateTableStructure()) {
        updateTableStructure();
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

        if (Boolean.TRUE.equals(meta.isPartitioningDaily())) {
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
      // For PG & GP, we add a savepoint before the row.
      // Then revert to the savepoint afterwards... (not a transaction, so hopefully still fast)
      //
      if (data.useSafePoints) {
        data.savepoint = data.db.setSavepoint();
      }
      data.db.setValues(data.insertRowMeta, insertRowData, insertStatement);
      data.db.insertRow(
          insertStatement,
          data.batchMode,
          false); // false: no commit, it is handled in this transform differently
      if (isRowLevel()) {
        logRowlevel("Written row: " + data.insertRowMeta.getString(insertRowData));
      }

      // Get a commit counter per prepared statement to keep track of separate tables, etc.
      //
      Integer commitCounter = data.commitCounterMap.get(tableName);
      if (commitCounter == null) {
        commitCounter = Integer.valueOf(1);
      } else {
        commitCounter++;
      }
      data.commitCounterMap.put(tableName, commitCounter);

      // Release the savepoint if needed
      //
      if (data.useSafePoints && data.releaseSavepoint) {
        data.db.releaseSavepoint(data.savepoint);
      }

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
        data.commitCounterMap.put(tableName, Integer.valueOf(0));
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

        if (data.useSafePoints) {
          data.db.rollback(data.savepoint);
          if (data.releaseSavepoint) {
            data.db.releaseSavepoint(data.savepoint);
          }
          // data.db.commit(true); // force a commit on the connection too.
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
          outputRowData = null;

          processBatchException(errorMessage, updateCounts, exceptionsList);
        } else {
          // Simply add this row to the error row
          putError(rowMeta, r, 1L, errorMessage, null, "TOP001");
          outputRowData = null;
        }
      } else {
        data.batchBuffer.add(outputRowData);
        outputRowData = null;

        if (rowIsSafe) { // A commit was done and the rows are all safe (no error)
          for (int i = 0; i < data.batchBuffer.size(); i++) {
            Object[] row = data.batchBuffer.get(i);
            putRow(data.outputRowMeta, row);
            incrementLinesOutput();
          }
          // Clear the buffer
          data.batchBuffer.clear();
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

  private void processBatchException(
      String errorMessage, int[] updateCounts, List<Exception> exceptionsList) throws HopException {
    // There was an error with the commit
    // We should put all the failing rows out there...
    //
    if (updateCounts != null) {
      int errNr = 0;
      for (int i = 0; i < updateCounts.length; i++) {
        Object[] row = data.batchBuffer.get(i);
        if (updateCounts[i] > 0) {
          // send the error foward
          putRow(data.outputRowMeta, row);
          incrementLinesOutput();
        } else {
          String exMessage = errorMessage;
          if (errNr < exceptionsList.size()) {
            SQLException se = (SQLException) exceptionsList.get(errNr);
            errNr++;
            exMessage = se.toString();
          }
          putError(data.outputRowMeta, row, 1L, exMessage, null, "TOP0002");
        }
      }
    } else {
      // If we don't have update counts, it probably means the DB doesn't support it.
      // In this case we don't have a choice but to consider all inserted rows to be error rows.
      //
      for (int i = 0; i < data.batchBuffer.size(); i++) {
        Object[] row = data.batchBuffer.get(i);
        putError(data.outputRowMeta, row, 1L, errorMessage, null, "TOP0003");
      }
    }

    // Clear the buffer afterwards...
    data.batchBuffer.clear();
  }

  @Override
  public boolean init() {

    if (super.init()) {
      try {
        data.commitSize = Integer.parseInt(resolve(meta.getCommitSize()));

        if (Utils.isEmpty(meta.getConnection()))
          throw new HopException(BaseMessages.getString(PKG, "TableOutput.Init.ConnectionMissing"));

        data.databaseMeta = this.getPipelineMeta().findDatabase(meta.getConnection(), variables);
        IDatabase dbInterface = data.databaseMeta.getIDatabase();

        // Batch updates are not supported on PostgreSQL (and look-a-likes)
        // together with error handling
        // For these situations we can use savepoints to help out.
        //
        data.useSafePoints =
            data.databaseMeta.getIDatabase().isUseSafePoints()
                && getTransformMeta().isDoingErrorHandling();

        // Get the boolean that indicates whether or not we can/should release
        // savepoints during data load.
        //
        data.releaseSavepoint = dbInterface.isReleaseSavepoint();

        // Disable batch mode in case
        // - we use an unlimited commit size
        // - if we need to pick up auto-generated keys
        // - if you are running the pipeline as a single database transaction (unique connections)
        // - if we are reverting to save-points
        //
        data.batchMode =
            meta.isUseBatchUpdate()
                && data.commitSize > 0
                && !meta.isReturningGeneratedKeys()
                && !data.useSafePoints;

        // give a warning that batch mode operation in combination with transform error handling can
        // lead to
        // incorrectly processed rows.
        //
        if (getTransformMeta().isDoingErrorHandling()
            && !dbInterface.IsSupportsErrorHandlingOnBatchUpdates()) {
          logBasic(
              BaseMessages.getString(
                  PKG, "TableOutput.Warning.ErrorHandlingIsNotFullySupportedWithBatchProcessing"));
        }

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

        // Postpone commit as long as possible.
        //
        if (data.commitSize == 0) {
          data.commitSize = Integer.MAX_VALUE;
        }
        data.db.setCommit(data.commitSize);

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

  void truncateTable() throws HopDatabaseException {
    if (!meta.isPartitioningEnabled() && !meta.isTableNameInField()) {
      // Only the first one truncates in a non-partitioned transform copy
      //
      if (meta.isTruncateTable() && ((getCopy() == 0) || !Utils.isEmpty(getPartitionId()))) {
        data.db.truncateTable(resolve(meta.getSchemaName()), resolve(meta.getTableName()));
      }
    }
  }

  void updateTableStructure() throws HopException {
    if (!meta.isPartitioningEnabled() && !meta.isTableNameInField()) {
      // Only the first one updates table structure in a non-partitioned transform copy
      if ((getCopy() == 0) || !Utils.isEmpty(getPartitionId())) {
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
  }

  private void dropTable(String fullTableName) throws HopException {
    logBasic("Dropping table: " + fullTableName);
    String dropSql = data.databaseMeta.getDropTableIfExistsStatement(fullTableName);
    if (!Utils.isEmpty(dropSql)) {
      try {
        data.db.execStatement(dropSql);
        // Commit the DDL to finalize table drop
        data.db.commit();
        logBasic("Dropped table: " + fullTableName);
      } catch (Exception e) {
        logDetailed("Drop table failed (may not exist): " + e.getMessage());
        // Rollback transaction to clear any aborted state
        try {
          data.db.rollback();
          logDetailed("Rolled back transaction after drop table failure");
        } catch (Exception rollbackException) {
          logDetailed("Could not rollback transaction: " + rollbackException.getMessage());
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
      logDetailed("Table creation failed or table already exists, verifying...");

      // Rollback to clear any aborted transaction state before checking existence
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after create table failure in ensureTableExists");
      } catch (Exception rollbackException) {
        logDetailed("Could not rollback transaction: " + rollbackException.getMessage());
      }

      boolean tableExists = checkTableExists(schemaName, tableName, true);
      if (!tableExists) {
        // Table still doesn't exist after failed creation attempt - this is a real error
        throw new HopException(
            "Failed to create table " + fullTableName + " and table does not exist", e);
      } else {
        logDetailed("Table already exists: " + fullTableName);
      }
    }
  }

  private boolean checkTableExists(String schemaName, String tableName, boolean bypassCache)
      throws HopException {
    if (bypassCache) {
      // Clear cache to ensure fresh state
      try {
        org.apache.hop.core.DbCache.getInstance().clear(data.databaseMeta.getName());
        logDetailed("Cleared database cache to ensure fresh table state");
      } catch (Exception cacheException) {
        logError("Could not clear database cache: " + cacheException.getMessage());
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
        logDetailed("Could not rollback transaction: " + rollbackException.getMessage());
      }
      return false; // Assume table doesn't exist if check fails
    }
  }

  private void createTable(String fullTableName) throws HopException {
    logBasic("Creating table: " + fullTableName);
    IRowMeta inputRowMeta = getInputRowMeta();
    String createSql =
        data.db.getCreateTableStatement(fullTableName, inputRowMeta, null, false, null, true);
    if (!Utils.isEmpty(createSql)) {
      logDetailed("CREATE TABLE SQL: " + createSql);
      data.db.execStatement(createSql);
      // Commit the DDL to finalize table creation
      data.db.commit();
      logBasic("Successfully created and committed table: " + fullTableName);

      // Clear cache after successful creation to ensure fresh state
      try {
        org.apache.hop.core.DbCache.getInstance().clear(data.databaseMeta.getName());
        logDetailed("Cleared database cache after table creation");
      } catch (Exception cacheException) {
        logError("Could not clear database cache: " + cacheException.getMessage());
      }
    }
  }

  private void addMissingColumns(String fullTableName, String schemaName, String tableName)
      throws HopException {
    logDetailed("Checking for missing columns in table: " + fullTableName);

    try {
      // Get the current table structure from the database
      IRowMeta tableFields = data.db.getTableFieldsMeta(schemaName, tableName);
      if (tableFields == null) {
        logDetailed("Could not retrieve table structure for: " + fullTableName);
        return;
      }

      // Get the incoming stream structure
      IRowMeta streamFields = getInputRowMeta();
      if (streamFields == null) {
        logDetailed("No incoming stream fields available");
        return;
      }

      // Find columns that exist in the stream but not in the table
      List<IValueMeta> missingColumns = new ArrayList<>();
      for (IValueMeta streamField : streamFields.getValueMetaList()) {
        String fieldName = streamField.getName();
        if (tableFields.searchValueMeta(fieldName) == null) {
          missingColumns.add(streamField);
          logBasic("Found missing column: " + fieldName + " (" + streamField.getTypeDesc() + ")");
        }
      }

      // Add each missing column
      if (!missingColumns.isEmpty()) {
        for (IValueMeta missingColumn : missingColumns) {
          addColumn(fullTableName, missingColumn);
        }
        logBasic(
            "Added " + missingColumns.size() + " missing column(s) to table: " + fullTableName);

        // Clear cache after modifications
        try {
          org.apache.hop.core.DbCache.getInstance().clear(data.databaseMeta.getName());
          logDetailed("Cleared database cache after adding columns");
        } catch (Exception cacheException) {
          logError("Could not clear database cache: " + cacheException.getMessage());
        }
      } else {
        logDetailed("No missing columns found in table: " + fullTableName);
      }

    } catch (Exception e) {
      logError("Error checking/adding missing columns: " + e.getMessage(), e);
      // Rollback to clear any aborted transaction state
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after add columns failure");
      } catch (Exception rollbackException) {
        logDetailed("Could not rollback transaction: " + rollbackException.getMessage());
      }
      throw new HopException("Failed to add missing columns to table " + fullTableName, e);
    }
  }

  private void addColumn(String fullTableName, IValueMeta column) throws HopException {
    logDetailed("Adding column: " + column.getName() + " to table: " + fullTableName);

    try {
      String addColumnStatement =
          data.databaseMeta.getAddColumnStatement(fullTableName, column, null, false, null, false);

      if (!Utils.isEmpty(addColumnStatement)) {
        logDetailed("ALTER TABLE SQL: " + addColumnStatement);
        data.db.execStatement(addColumnStatement);
        // Commit the DDL to finalize column addition
        data.db.commit();
        logBasic("Successfully added column: " + column.getName());
      }
    } catch (Exception e) {
      logError("Error adding column " + column.getName() + ": " + e.getMessage(), e);
      // Rollback to clear any aborted transaction state
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after add column failure");
      } catch (Exception rollbackException) {
        logDetailed("Could not rollback transaction: " + rollbackException.getMessage());
      }
      throw new HopException("Failed to add column " + column.getName(), e);
    }
  }

  private void dropSurplusColumns(String fullTableName, String schemaName, String tableName)
      throws HopException {
    logDetailed("Checking for surplus columns in table: " + fullTableName);

    try {
      // Get the current table structure from the database
      IRowMeta tableFields = data.db.getTableFieldsMeta(schemaName, tableName);
      if (tableFields == null) {
        logDetailed("Could not retrieve table structure for: " + fullTableName);
        return;
      }

      // Get the incoming stream structure
      IRowMeta streamFields = getInputRowMeta();
      if (streamFields == null) {
        logDetailed("No incoming stream fields available");
        return;
      }

      // Find columns that exist in the table but not in the stream
      List<IValueMeta> surplusColumns = new ArrayList<>();
      for (IValueMeta tableField : tableFields.getValueMetaList()) {
        String fieldName = tableField.getName();
        if (streamFields.searchValueMeta(fieldName) == null) {
          surplusColumns.add(tableField);
          logBasic("Found surplus column: " + fieldName + " (" + tableField.getTypeDesc() + ")");
        }
      }

      // Drop each surplus column
      if (!surplusColumns.isEmpty()) {
        logBasic(
            "WARNING: Dropping "
                + surplusColumns.size()
                + " column(s) from table: "
                + fullTableName
                + " - THIS WILL RESULT IN DATA LOSS");
        for (IValueMeta surplusColumn : surplusColumns) {
          dropColumn(fullTableName, surplusColumn);
        }
        logBasic(
            "Dropped " + surplusColumns.size() + " surplus column(s) from table: " + fullTableName);

        // Clear cache after modifications
        try {
          org.apache.hop.core.DbCache.getInstance().clear(data.databaseMeta.getName());
          logDetailed("Cleared database cache after dropping columns");
        } catch (Exception cacheException) {
          logError("Could not clear database cache: " + cacheException.getMessage());
        }
      } else {
        logDetailed("No surplus columns found in table: " + fullTableName);
      }

    } catch (Exception e) {
      logError("Error checking/dropping surplus columns: " + e.getMessage(), e);
      // Rollback to clear any aborted transaction state
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after drop columns failure");
      } catch (Exception rollbackException) {
        logDetailed("Could not rollback transaction: " + rollbackException.getMessage());
      }
      throw new HopException("Failed to drop surplus columns from table " + fullTableName, e);
    }
  }

  private void dropColumn(String fullTableName, IValueMeta column) throws HopException {
    logDetailed("Dropping column: " + column.getName() + " from table: " + fullTableName);

    try {
      String dropColumnStatement =
          data.databaseMeta.getDropColumnStatement(fullTableName, column, null, false, null, false);

      if (!Utils.isEmpty(dropColumnStatement)) {
        logDetailed("ALTER TABLE SQL: " + dropColumnStatement);
        data.db.execStatement(dropColumnStatement);
        // Commit the DDL to finalize column drop
        data.db.commit();
        logBasic("Successfully dropped column: " + column.getName());
      }
    } catch (Exception e) {
      logError("Error dropping column " + column.getName() + ": " + e.getMessage(), e);
      // Rollback to clear any aborted transaction state
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after drop column failure");
      } catch (Exception rollbackException) {
        logDetailed("Could not rollback transaction: " + rollbackException.getMessage());
      }
      throw new HopException("Failed to drop column " + column.getName(), e);
    }
  }

  private void changeColumnTypes(String fullTableName, String schemaName, String tableName)
      throws HopException {
    logDetailed("Checking for column type mismatches in table: " + fullTableName);

    try {
      // Get the current table structure from the database
      IRowMeta tableFields = data.db.getTableFieldsMeta(schemaName, tableName);
      if (tableFields == null) {
        logDetailed("Could not retrieve table structure for: " + fullTableName);
        return;
      }

      // Get the incoming stream structure
      IRowMeta streamFields = getInputRowMeta();
      if (streamFields == null) {
        logDetailed("No incoming stream fields available");
        return;
      }

      // Find columns where types don't match
      List<IValueMeta> columnsToModify = new ArrayList<>();
      for (IValueMeta streamField : streamFields.getValueMetaList()) {
        String fieldName = streamField.getName();
        IValueMeta tableField = tableFields.searchValueMeta(fieldName);

        if (tableField != null) {
          // Column exists in both, check if types are compatible
          if (!typesAreCompatible(tableField, streamField)) {
            columnsToModify.add(streamField);
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
        logBasic(
            "WARNING: Changing data types for "
                + columnsToModify.size()
                + " column(s) in table: "
                + fullTableName
                + " - THIS MAY RESULT IN DATA LOSS");
        for (IValueMeta columnToModify : columnsToModify) {
          modifyColumn(fullTableName, columnToModify);
        }
        logBasic(
            "Modified " + columnsToModify.size() + " column type(s) in table: " + fullTableName);

        // Clear cache after modifications
        try {
          org.apache.hop.core.DbCache.getInstance().clear(data.databaseMeta.getName());
          logDetailed("Cleared database cache after modifying column types");
        } catch (Exception cacheException) {
          logError("Could not clear database cache: " + cacheException.getMessage());
        }
      } else {
        logDetailed("No column type mismatches found in table: " + fullTableName);
      }

    } catch (Exception e) {
      logError("Error checking/changing column types: " + e.getMessage(), e);
      // Rollback to clear any aborted transaction state
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after change column types failure");
      } catch (Exception rollbackException) {
        logDetailed("Could not rollback transaction: " + rollbackException.getMessage());
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
      if (tableField.getType() == IValueMeta.TYPE_NUMBER
          || tableField.getType() == IValueMeta.TYPE_BIGNUMBER) {
        if (tableField.getPrecision() != streamField.getPrecision()
            || tableField.getLength() != streamField.getLength()) {
          return false;
        }
      }
      return true;
    }

    // Different types - generally not compatible, but some conversions are safe
    // e.g., INTEGER -> BIGINT, but we'll require explicit type match for safety
    return false;
  }

  private void modifyColumn(String fullTableName, IValueMeta column) throws HopException {
    logDetailed(
        "Modifying column: "
            + column.getName()
            + " to type: "
            + column.getTypeDesc()
            + " in table: "
            + fullTableName);

    try {
      // For type changes, use drop/recreate approach as it's simpler and more reliable
      // The complex modify statement from DatabaseMeta often fails with type conversions
      logDetailed("Using drop/recreate approach for column type change: " + column.getName());

      // Drop the existing column
      String dropColumnStatement =
          data.databaseMeta.getDropColumnStatement(fullTableName, column, null, false, null, false);
      if (!Utils.isEmpty(dropColumnStatement)) {
        logDetailed("DROP COLUMN SQL: " + dropColumnStatement);
        data.db.execStatement(dropColumnStatement);
        data.db.commit();
        logDetailed("Dropped column: " + column.getName());
      }

      // Add the column with new type
      String addColumnStatement =
          data.databaseMeta.getAddColumnStatement(fullTableName, column, null, false, null, false);
      if (!Utils.isEmpty(addColumnStatement)) {
        logDetailed("ADD COLUMN SQL: " + addColumnStatement);
        data.db.execStatement(addColumnStatement);
        data.db.commit();
        logDetailed("Added column with new type: " + column.getName());
      }

      logBasic("Successfully modified column type (via drop/recreate): " + column.getName());
    } catch (Exception e) {
      logError("Error modifying column " + column.getName() + ": " + e.getMessage(), e);
      // Rollback to clear any aborted transaction state
      try {
        data.db.rollback();
        logDetailed("Rolled back transaction after modify column failure");
      } catch (Exception rollbackException) {
        logDetailed("Could not rollback transaction: " + rollbackException.getMessage());
      }
      throw new HopException("Failed to modify column " + column.getName(), e);
    }
  }

  @Override
  public void dispose() {

    if (data.db != null) {
      try {
        emptyAndCommitBatchBuffers(true);
      } finally {
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
        data.commitCounterMap = null;
        data.outputRowMeta = null;
      }
    }

    super.dispose();
  }

  // Force the batched up rows to the database in a single-threaded scenario
  // (Beam as well)
  //
  @Override
  public void batchComplete() throws HopException {
    emptyAndCommitBatchBuffers(false);
  }

  private void emptyAndCommitBatchBuffers(boolean dispose) {
    try {
      for (String schemaTable : data.preparedStatements.keySet()) {
        // Get a commit counter per prepared statement to keep track of separate tables, etc.
        //
        Integer batchCounter = data.commitCounterMap.get(schemaTable);
        if (batchCounter == null || batchCounter == 0) {
          continue; // Skip this one, no work required
        }

        PreparedStatement insertStatement = data.preparedStatements.get(schemaTable);
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
    } catch (HopDatabaseBatchException be) {
      if (getTransformMeta().isDoingErrorHandling()) {
        // Right at the back we are experiencing a batch commit problem...
        // OK, we have the numbers...
        try {
          processBatchException(be.toString(), be.getUpdateCounts(), be.getExceptionsList());
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
      setOutputDone();

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
