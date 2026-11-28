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

package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.batch.BatchSupport;
import org.apache.hop.core.database.batch.IBatchRowListener;
import org.apache.hop.core.database.batch.PendingKeys;
import org.apache.hop.core.database.batch.RecoverableBatch;
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeData.PendingRow;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeData.RecoveredStatement;
import org.jetbrains.annotations.Nullable;

public class SynchronizeAfterMerge
    extends BaseTransform<SynchronizeAfterMergeMeta, SynchronizeAfterMergeData> {
  private static final String CONST_SUYNC001 = "SUYNC001";
  private static final String CONST_SUYNC002 = "SUYNC002";

  /** A row of a recovered batch that was never sent to the database, which could not be asked. */
  private static final String CONST_SUYNC003 = "SUYNC003";

  private static final Class<?> PKG = SynchronizeAfterMergeMeta.class;
  public static final String CONST_IS_NULL = "IS NULL";
  public static final String CONST_IS_NOT_NULL = "IS NOT NULL";

  /** = ~NULL: equal, or both null. */
  private static final String CONST_EQUALS_OR_NULL = "= ~NULL";

  public static final String CONST_SYNCHRONIZE_AFTER_MERGE_EXCEPTION_FIELD_REQUIRED =
      "SynchronizeAfterMerge.Exception.FieldRequired";
  public static final String CONST_INSERT = "insert";
  public static final String CONST_LOOKUP = "lookup";
  public static final String CONST_UPDATE = "update";
  public static final String CONST_DELETE = "delete";
  public static final String CONST_BETWEEN = "BETWEEN";
  public static final String CONST_BETWEEN_AND = " BETWEEN ? AND ? ";

  public SynchronizeAfterMerge(
      TransformMeta transformMeta,
      SynchronizeAfterMergeMeta meta,
      SynchronizeAfterMergeData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private synchronized void lookupValues(Object[] row) throws HopException {
    // get operation for the current
    // do we insert, update or delete ?
    String operation = data.inputRowMeta.getString(row, data.indexOfOperationOrderField);

    boolean rowIsSafe = false;
    boolean sendToErrorRow = false;
    String errorMessage = null;
    int[] updateCounts = null;
    List<Exception> exceptionsList = null;
    boolean batchProblem = false;

    data.lookupFailure = false;
    boolean performInsert = false;
    boolean performUpdate = false;
    boolean performDelete = false;
    boolean lineSkipped = false;

    try {
      if (operation == null) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "SynchronizeAfterMerge.Log.OperationFieldEmpty",
                meta.getOperationOrderField()));
      }

      if (meta.isTableNameInField()) {
        // get dynamic table name
        data.realTableName = data.inputRowMeta.getString(row, data.indexOfTableNameField);
        if (Utils.isEmpty(data.realTableName)) {
          throw new HopTransformException("The name of the table is not specified!");
        }
        data.realSchemaTable =
            data.db
                .getDatabaseMeta()
                .getQuotedSchemaTableCombination(this, data.realSchemaName, data.realTableName);
      }

      if (operation.equals(data.insertValue)) {
        // directly insert data into table
        /*
         *
         * INSERT ROW
         */

        if (isRowLevel()) {
          logRowlevel(
              BaseMessages.getString(PKG, "SynchronizeAfterMerge.InsertRow", Arrays.toString(row)));
        }

        // The values to insert are those in the update section
        //
        Object[] insertRowData = new Object[data.valueIndexes.length];
        for (int i = 0; i < data.valueIndexes.length; i++) {
          insertRowData[i] = row[data.valueIndexes[i]];
        }

        if (meta.isTableNameInField()) {
          data.insertStatement = data.preparedStatements.get(data.realSchemaTable + CONST_INSERT);
          if (data.insertStatement == null) {
            String sql =
                data.db.getInsertStatement(
                    data.realSchemaName, data.realTableName, data.insertRowMeta);

            if (isDebug()) {
              logDebug("Preparation of the insert SQL statement: " + sql);
            }

            data.insertStatement = data.db.prepareSql(sql);
            data.preparedStatements.put(data.realSchemaTable + CONST_INSERT, data.insertStatement);
            data.statementSql.put(data.realSchemaTable + CONST_INSERT, sql);
          }
        }

        if (data.recoveredBatches) {
          addToRecoveredBatch(
              CONST_INSERT, data.insertStatement, data.insertRowMeta, insertRowData, row);
        } else {
          write(data.insertRowMeta, insertRowData, data.insertStatement);
        }
        performInsert = true;
        if (!data.batchMode && !data.recoveredBatches) {
          incrementLinesOutput();
        }
        if (isRowLevel()) {
          logRowlevel("Written row: " + data.insertRowMeta.getString(insertRowData));
        }

      } else {

        Object[] lookupRow = new Object[data.keyIndexes.length];
        int lookupIndex = 0;
        for (int i = 0; i < data.keyIndexes.length; i++) {
          if (data.keyIndexes[i] >= 0) {
            lookupRow[lookupIndex] = row[data.keyIndexes[i]];
            lookupIndex++;
          }
          if (data.keyIndexes2[i] >= 0) {
            lookupRow[lookupIndex] = row[data.keyIndexes2[i]];
            lookupIndex++;
          }
        }
        boolean updateorDelete = false;
        if (meta.isPerformingLookup()) {

          // LOOKUP

          if (meta.isTableNameInField()) {
            // Prepare Lookup statement
            data.lookupStatement = data.preparedStatements.get(data.realSchemaTable + CONST_LOOKUP);
            if (data.lookupStatement == null) {
              String sql = getLookupStatement(data.inputRowMeta);

              if (isDebug()) {
                logDebug("Preparating SQL for insert: " + sql);
              }

              data.lookupStatement = data.db.prepareSql(sql);
              data.preparedStatements.put(
                  data.realSchemaTable + CONST_LOOKUP, data.lookupStatement);
              data.statementSql.put(data.realSchemaTable + CONST_LOOKUP, sql);
            }
          }

          data.db.setValues(data.lookupParameterRowMeta, lookupRow, data.lookupStatement);
          if (isRowLevel()) {
            logRowlevel(
                BaseMessages.getString(
                    PKG,
                    "SynchronizeAfterMerge.Log.ValuesSetForLookup",
                    data.lookupParameterRowMeta.getString(lookupRow)));
          }
          Object[] add = data.db.getLookup(data.lookupStatement);
          incrementLinesInput();

          if (add == null) {
            // nothing was found:

            if (data.stringErrorKeyNotFound == null) {
              data.stringErrorKeyNotFound =
                  BaseMessages.getString(PKG, "SynchronizeAfterMerge.Exception.KeyCouldNotFound")
                      + data.lookupParameterRowMeta.getString(lookupRow);
              data.stringFieldNames = "";
              for (int i = 0; i < data.lookupParameterRowMeta.size(); i++) {
                if (i > 0) {
                  data.stringFieldNames += ", ";
                }
                data.stringFieldNames += data.lookupParameterRowMeta.getValueMeta(i).getName();
              }
            }
            data.lookupFailure = true;
            data.keyNotFoundMessage =
                BaseMessages.getString(PKG, "SynchronizeAfterMerge.Exception.KeyCouldNotFound")
                    + data.lookupParameterRowMeta.getString(lookupRow);
            throw new HopDatabaseException(
                BaseMessages.getString(
                    PKG,
                    "SynchronizeAfterMerge.Exception.KeyCouldNotFound",
                    data.lookupParameterRowMeta.getString(lookupRow)));
          } else {
            if (isRowLevel()) {
              logRowlevel(
                  BaseMessages.getString(
                      PKG,
                      "SynchronizeAfterMerge.Log.FoundRowForUpdate",
                      data.insertRowMeta.getString(row)));
            }

            for (int i = 0; i < data.valueIndexes.length; i++) {
              SynchronizeAfterMergeMeta.ValueUpdate valueUpdate =
                  meta.getLookup().getValueUpdates().get(i);
              if (valueUpdate.isUpdate()) {
                IValueMeta valueMeta = data.inputRowMeta.getValueMeta(data.valueIndexes[i]);
                IValueMeta retMeta = data.db.getReturnRowMeta().getValueMeta(i);

                Object rowValue = row[data.valueIndexes[i]];
                Object returnValue = add[i];
                if (valueMeta.compare(rowValue, retMeta, returnValue) != 0) {
                  updateorDelete = true;
                }
              }
            }
          }
        } // end if perform lookup

        if (operation.equals(data.updateValue)) {
          if (!meta.isPerformingLookup() || updateorDelete) {
            // UPDATE :

            if (meta.isTableNameInField()) {
              data.updateStatement =
                  data.preparedStatements.get(data.realSchemaTable + CONST_UPDATE);
              if (data.updateStatement == null) {
                String sql = getUpdateStatement(data.inputRowMeta);

                data.updateStatement = data.db.prepareSql(sql);
                data.preparedStatements.put(
                    data.realSchemaTable + CONST_UPDATE, data.updateStatement);
                data.statementSql.put(data.realSchemaTable + CONST_UPDATE, sql);
                if (isDebug()) {
                  logDebug("Preparation of the Update SQL statement : " + sql);
                }
              }
            }

            // Create the update row...
            Object[] updateRow = new Object[data.updateParameterRowMeta.size()];
            int j = 0;
            for (int i = 0; i < data.valueIndexes.length; i++) {
              SynchronizeAfterMergeMeta.ValueUpdate valueUpdate =
                  meta.getLookup().getValueUpdates().get(i);
              if (valueUpdate.isUpdate()) {
                updateRow[j] = row[data.valueIndexes[i]]; // the setters
                j++;
              }
            }

            // add the where clause parameters, they are exactly the same for lookup and update
            for (int i = 0; i < lookupRow.length; i++) {
              updateRow[j + i] = lookupRow[i];
            }

            if (isRowLevel()) {
              logRowlevel(
                  BaseMessages.getString(
                      PKG,
                      "SynchronizeAfterMerge.Log.SetValuesForUpdate",
                      data.updateParameterRowMeta.getString(updateRow),
                      data.inputRowMeta.getString(row)));
            }
            if (data.recoveredBatches) {
              addToRecoveredBatch(
                  CONST_UPDATE, data.updateStatement, data.updateParameterRowMeta, updateRow, row);
            } else {
              write(data.updateParameterRowMeta, updateRow, data.updateStatement);
              incrementLinesUpdated();
            }
            performUpdate = true;

          } else {
            // end if operation update
            incrementLinesSkipped();
            lineSkipped = true;
          }
        } else if (operation.equals(data.deleteValue)) {
          // DELETE

          if (meta.isTableNameInField()) {
            data.deleteStatement = data.preparedStatements.get(data.realSchemaTable + CONST_DELETE);

            if (data.deleteStatement == null) {
              String sql = getDeleteStatement(data.inputRowMeta);
              data.deleteStatement = data.db.prepareSql(sql);
              data.preparedStatements.put(
                  data.realSchemaTable + CONST_DELETE, data.deleteStatement);
              data.statementSql.put(data.realSchemaTable + CONST_DELETE, sql);
              if (isDebug()) {
                logDebug("Preparation of the Delete SQL statement : " + sql);
              }
            }
          }

          Object[] deleteRow = new Object[data.deleteParameterRowMeta.size()];
          int deleteIndex = 0;

          for (int i = 0; i < data.keyIndexes.length; i++) {
            if (data.keyIndexes[i] >= 0) {
              deleteRow[deleteIndex] = row[data.keyIndexes[i]];
              deleteIndex++;
            }
            if (data.keyIndexes2[i] >= 0) {
              deleteRow[deleteIndex] = row[data.keyIndexes2[i]];
              deleteIndex++;
            }
          }

          if (isRowLevel()) {
            logRowlevel(
                BaseMessages.getString(
                    PKG,
                    "SynchronizeAfterMerge.Log.SetValuesForDelete",
                    data.deleteParameterRowMeta.getString(deleteRow),
                    data.inputRowMeta.getString(row)));
          }
          if (data.recoveredBatches) {
            addToRecoveredBatch(
                CONST_DELETE, data.deleteStatement, data.deleteParameterRowMeta, deleteRow, row);
          } else {
            write(data.deleteParameterRowMeta, deleteRow, data.deleteStatement);
            incrementLinesUpdated();
          }
          performDelete = true;
        } else {
          // endif operation delete
          incrementLinesSkipped();
          lineSkipped = true;
        }
      } // endif operation insert

      // If we skip a line we need to empty the buffer and skip the line in question.
      // The skipped line is never added to the buffer!
      //
      if (performInsert
          || performUpdate
          || performDelete
          || (!data.batchBuffer.isEmpty() && lineSkipped)) {
        // Get a commit counter per prepared statement to keep track of separate tables, etc.
        //
        String tableName = data.realSchemaTable;
        if (performInsert) {
          tableName += CONST_INSERT;
        } else if (performUpdate) {
          tableName += CONST_UPDATE;
        }
        if (performDelete) {
          tableName += CONST_DELETE;
        }

        Integer commitCounter = data.commitCounterMap.get(tableName);
        if (commitCounter == null) {
          commitCounter = 0;
        }
        data.commitCounterMap.put(tableName, commitCounter + 1);

        // Perform a commit if needed
        //
        if (commitCounter > 0 && (commitCounter % data.commitSize) == 0) {
          if (data.recoveredBatches) {
            // The batch this transform ran here runs after the row, outside the try
            data.batchToRun = tableName;
          } else if (data.batchMode) {
            commitBatchMode(performInsert, performUpdate, performDelete);
          } else {
            // insertRow normal commit
            data.db.commit();
          }
          // Clear the batch/commit counter...
          //
          data.commitCounterMap.put(tableName, 0);
          rowIsSafe = true;
        }
      }
    } catch (HopDatabaseBatchException be) {
      errorMessage = be.toString();
      batchProblem = true;
      sendToErrorRow = true;
      updateCounts = be.getUpdateCounts();
      exceptionsList = be.getExceptionsList();

      if (data.insertStatement != null) {
        data.db.clearBatch(data.insertStatement);
      }
      if (data.updateStatement != null) {
        data.db.clearBatch(data.updateStatement);
      }
      if (data.deleteStatement != null) {
        data.db.clearBatch(data.deleteStatement);
      }

      if (getTransformMeta().isDoingErrorHandling()) {
        data.db.commit(true);
      } else {
        data.db.rollback();
        StringBuilder msg =
            new StringBuilder(
                "Error batch inserting rows into table [" + data.realTableName + "].");
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
          logRowlevel("Written row to error handling : " + getInputRowMeta().getString(row));
        }
        // A failed INSERT, UPDATE or DELETE was already rolled back to its savepoint by
        // write(). A failed lookup ran no statement that needs undoing.
        sendToErrorRow = true;
        errorMessage = dbe.toString();
      } else {
        setErrors(getErrors() + 1);
        data.db.rollback();
        throw new HopException(
            "Error inserting row into table ["
                + data.realTableName
                + "] with values: "
                + data.inputRowMeta.getString(row),
            dbe);
      }
    }

    if (data.recoveredBatches) {
      putRecoveredRow(
          row, sendToErrorRow, errorMessage, performInsert || performUpdate || performDelete);
    } else if (data.batchMode) {
      if (sendToErrorRow) {
        if (batchProblem) {
          data.batchBuffer.add(row);
          processBatchException(errorMessage, updateCounts, exceptionsList);
        } else {
          // Simply add this row to the error row
          putError(data.inputRowMeta, row, 1L, errorMessage, null, CONST_SUYNC002);
        }
      } else {
        if (!lineSkipped) {
          data.batchBuffer.add(row);
        }

        if (rowIsSafe) { // A commit was done and the rows are all safe (no error)
          for (int i = 0; i < data.batchBuffer.size(); i++) {
            Object[] rowb = data.batchBuffer.get(i);
            putRow(data.outputRowMeta, rowb);
            if (data.inputRowMeta
                .getString(rowb, data.indexOfOperationOrderField)
                .equals(data.insertValue)) {
              incrementLinesOutput();
            }
          }
          // Clear the buffer
          data.batchBuffer.clear();
        }

        // Don't forget to pass this line to the following transforms
        //
        if (lineSkipped) {
          putRow(data.outputRowMeta, row);
        }
      }
    } else {
      if (sendToErrorRow) {
        if (data.lookupFailure) {
          putError(
              data.inputRowMeta,
              row,
              1,
              data.stringErrorKeyNotFound,
              data.stringFieldNames,
              "SUYNC001");
        } else {
          putError(data.inputRowMeta, row, 1, errorMessage, null, "SUYNC001");
        }
      } else {
        // A row goes to the error hop or to the output, never to both.
        putRow(data.outputRowMeta, row);
      }
    }
  }

  /**
   * Binds and runs the INSERT, UPDATE or DELETE of one row. With a savepoint per row only the
   * statement is inside it: the commit comes after it, from lookupValues(), as committing would end
   * the savepoint, and on a connection shared by a transactional pipeline the other transforms wait
   * while it is open.
   */
  private void write(IRowMeta rowMeta, Object[] values, PreparedStatement statement)
      throws HopException {
    if (data.useSavepoints) {
      data.db.runInSavepoint(
          () -> {
            data.db.setValues(rowMeta, values, statement);
            data.db.insertRow(statement, false, false);
            return null;
          });
    } else {
      data.db.setValues(rowMeta, values, statement);
      data.db.insertRow(statement, data.batchMode);
    }
  }

  private void commitBatchMode(boolean performInsert, boolean performUpdate, boolean performDelete)
      throws HopDatabaseException {
    try {
      if (performInsert) {
        data.insertStatement.executeBatch();
        data.db.commit();
        data.insertStatement.clearBatch();
      } else if (performUpdate) {
        data.updateStatement.executeBatch();
        data.db.commit();
        data.updateStatement.clearBatch();
      } else if (performDelete) {
        data.deleteStatement.executeBatch();
        data.db.commit();
        data.deleteStatement.clearBatch();
      }
    } catch (SQLException ex) {
      throw Database.createHopDatabaseBatchException(
          BaseMessages.getString(PKG, "SynchronizeAfterMerge.Error.UpdatingBatch"), ex);
    } catch (Exception ex) {
      throw new HopDatabaseException("Unexpected error inserting row", ex);
    }
  }

  /**
   * Route every row of a failed batch to the regular output or to the error stream.
   *
   * <p>Drivers do not agree on what {@link java.sql.BatchUpdateException#getUpdateCounts()} holds
   * after a failure. Some report one count per row, marking the failures in place. Some stop at the
   * first failure, so the array is <em>shorter</em> than the batch: the row at {@code
   * updateCounts.length} is the one that failed and the rows behind it were never sent at all
   * (Oracle and Derby; a failure on the first row gives a zero-length array, not a null one). Some
   * report nothing usable.
   *
   * <p>Whatever comes back, every buffered row has to leave here on one of the two streams. Walking
   * only the counts array and then clearing the buffer is what used to make rows disappear without
   * an error row or a log line - see <a href="https://github.com/apache/hop/issues/5758">issue
   * 5758</a>, which reported it against Table Output.
   *
   * <p>Unlike Table Output, this transform does not re-drive the rows the database never attempted.
   * It buffers rows for its insert, update and delete statements in one interleaved list, so a
   * buffer position does not identify which statement the row belongs to and the values to re-bind
   * cannot be worked out. Those rows are reported as rejects instead, which keeps them out of the
   * silent-loss category but does mean this transform writes fewer rows than Table Output would for
   * the same input on a driver that abandons the rest of the batch.
   *
   * <p>Package-private so the accounting can be exercised directly against each driver shape,
   * without a database.
   */
  void processBatchException(
      String errorMessage, int[] updateCounts, List<Exception> exceptionsList) throws HopException {

    List<Exception> exceptions = exceptionsList == null ? List.of() : exceptionsList;
    int bufferSize = data.batchBuffer.size();

    // More counts than rows means the counts belong to a different batch than the one buffered, so
    // no row can be matched to a count. Reporting the wrong rows on the wrong stream would be worse
    // than failing here, where the mismatch is still visible.
    //
    if (updateCounts != null && updateCounts.length > bufferSize) {
      throw new HopException(
          "Unable to attribute batch errors to rows: the database returned "
              + updateCounts.length
              + " update counts for a batch of "
              + bufferSize
              + " buffered rows.");
    }

    int counted = updateCounts == null ? 0 : updateCounts.length;
    int errNr = 0;

    for (int i = 0; i < counted; i++) {
      Object[] row = data.batchBuffer.get(i);
      if (updateCounts[i] != Statement.EXECUTE_FAILED) {
        // Anything that isn't EXECUTE_FAILED is a success. That includes SUCCESS_NO_INFO, and it
        // includes a count of zero: this transform issues updates and deletes, and a statement that
        // matched no rows still ran.
        //
        putRow(data.outputRowMeta, row);
        incrementLinesOutput();
      } else {
        String exMessage = errorMessage;
        if (errNr < exceptions.size()) {
          exMessage = exceptions.get(errNr).toString();
          errNr++;
        }
        putError(data.outputRowMeta, row, 1L, exMessage, null, CONST_SUYNC002);
      }
    }

    // Whatever the driver did not account for. With a short array the first of these is the row it
    // actually refused, so it keeps the database's own message; the rest were never attempted.
    //
    for (int i = counted; i < bufferSize; i++) {
      Object[] row = data.batchBuffer.get(i);
      if (i == counted && updateCounts != null) {
        putError(data.outputRowMeta, row, 1L, errorMessage, null, CONST_SUYNC002);
      } else {
        putError(
            data.outputRowMeta,
            row,
            1L,
            BaseMessages.getString(
                PKG, "SynchronizeAfterMerge.Error.RowNotAttempted", Const.NVL(errorMessage, "")),
            null,
            "SUYNC003");
      }
    }

    // Clear the buffer afterwards...
    data.batchBuffer.clear();
  }

  // Lookup certain fields in a table
  public String getLookupStatement(IRowMeta rowMeta) throws HopDatabaseException {
    data.lookupParameterRowMeta = new RowMeta();
    data.lookupReturnRowMeta = new RowMeta();

    StringBuilder sql = new StringBuilder();

    sql.append("SELECT ");

    for (int i = 0; i < meta.getLookup().getValueUpdates().size(); i++) {
      SynchronizeAfterMergeMeta.ValueUpdate valueUpdate = meta.getLookup().getValueUpdates().get(i);
      if (i != 0) {
        sql.append(", ");
      }
      sql.append(data.databaseMeta.quoteField(valueUpdate.getColumnName()));
      data.lookupReturnRowMeta.addValueMeta(
          rowMeta.searchValueMeta(valueUpdate.getFieldName()).clone());
    }

    sql.append(" FROM ").append(data.realSchemaTable).append(" WHERE ");

    for (int i = 0; i < meta.getLookup().getKeyConditions().size(); i++) {
      SynchronizeAfterMergeMeta.KeyCondition keyCondition =
          meta.getLookup().getKeyConditions().get(i);
      if (i != 0) {
        sql.append(" AND ");
      }
      sql.append(data.databaseMeta.quoteField(keyCondition.getColumnName()));
      if (CONST_BETWEEN.equalsIgnoreCase(keyCondition.getCondition())) {
        sql.append(CONST_BETWEEN_AND);
        data.lookupParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyCondition.getFieldName()));
        data.lookupParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyCondition.getFieldName2()));
      } else {
        if (CONST_IS_NULL.equalsIgnoreCase(keyCondition.getCondition())
            || CONST_IS_NOT_NULL.equalsIgnoreCase(keyCondition.getCondition())) {
          sql.append(" ").append(keyCondition.getCondition()).append(" ");
        } else {
          sql.append(" ").append(keyCondition.getCondition()).append(" ? ");
          data.lookupParameterRowMeta.addValueMeta(
              rowMeta.searchValueMeta(keyCondition.getFieldName()));
        }
      }
    }
    return sql.toString();
  }

  // Lookup certain fields in a table
  public String getUpdateStatement(IRowMeta rowMeta) throws HopDatabaseException {
    data.updateParameterRowMeta = new RowMeta();

    StringBuilder sql = new StringBuilder();

    sql.append("UPDATE ").append(data.realSchemaTable).append(Const.CR);
    sql.append("SET ");

    boolean comma = false;

    for (SynchronizeAfterMergeMeta.ValueUpdate valueUpdate : meta.getLookup().getValueUpdates()) {
      if (valueUpdate.isUpdate()) {
        if (comma) {
          sql.append(",   ");
        } else {
          comma = true;
        }

        sql.append(data.databaseMeta.quoteField(valueUpdate.getColumnName()));
        sql.append(" = ?").append(Const.CR);
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(valueUpdate.getFieldName()).clone());
      }
    }

    sql.append("WHERE ");

    for (int i = 0; i < meta.getLookup().getKeyConditions().size(); i++) {
      SynchronizeAfterMergeMeta.KeyCondition keyCondition =
          meta.getLookup().getKeyConditions().get(i);
      if (i != 0) {
        sql.append("AND   ");
      }
      sql.append(data.databaseMeta.quoteField(keyCondition.getColumnName()));
      if (CONST_BETWEEN.equalsIgnoreCase(keyCondition.getCondition())) {
        sql.append(CONST_BETWEEN_AND);
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyCondition.getFieldName()));
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyCondition.getFieldName2()));
      } else if (CONST_IS_NULL.equalsIgnoreCase(keyCondition.getCondition())
          || CONST_IS_NOT_NULL.equalsIgnoreCase(keyCondition.getCondition())) {
        sql.append(" ").append(keyCondition.getCondition()).append(" ");
      } else {
        sql.append(" ").append(keyCondition.getCondition()).append(" ? ");
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyCondition.getFieldName()).clone());
      }
    }
    return sql.toString();
  }

  public String getDeleteStatement(IRowMeta rowMeta) throws HopDatabaseException {
    data.deleteParameterRowMeta = new RowMeta();

    StringBuilder sql = new StringBuilder();

    sql.append("DELETE FROM ").append(data.realSchemaTable).append(Const.CR);

    sql.append("WHERE ");

    for (int i = 0; i < meta.getLookup().getKeyConditions().size(); i++) {
      SynchronizeAfterMergeMeta.KeyCondition keyCondition =
          meta.getLookup().getKeyConditions().get(i);
      if (i != 0) {
        sql.append("AND   ");
      }
      sql.append(data.databaseMeta.quoteField(keyCondition.getColumnName()));
      if (CONST_BETWEEN.equalsIgnoreCase(keyCondition.getCondition())) {
        sql.append(CONST_BETWEEN_AND);
        data.deleteParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyCondition.getFieldName()));
        data.deleteParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyCondition.getFieldName2()));
      } else if (CONST_IS_NULL.equalsIgnoreCase(keyCondition.getCondition())
          || CONST_IS_NOT_NULL.equalsIgnoreCase(keyCondition.getCondition())) {
        sql.append(" ").append(keyCondition.getCondition()).append(" ");
      } else {
        sql.append(" ").append(keyCondition.getCondition()).append(" ? ");
        data.deleteParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyCondition.getFieldName()));
      }
    }
    return sql.toString();
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] nextRow = getRow(); // Get row from input rowset & set row busy!
    if (nextRow == null) { // no more input to be expected...
      if (data.recoveredBatches) {
        finishRecoveredTransform();
      } else {
        finishTransform();
      }
      return false;
    }

    if (first) {
      processRowInitialize(nextRow);
      if (data.recoveredBatches) {
        prepareWaitingKeys();
      }
    }

    // Outside the try below: a failed batch is about the rows in it, not about this row.
    if (data.recoveredBatches && keyWaitsInABatch(nextRow)) {
      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG,
                "SynchronizeAfterMerge.Log.KeyWaitsInBatch",
                data.inputRowMeta.getString(nextRow)));
      }
      if (!runAllRecoveredBatchesOrStop()) {
        setOutputDone();
        return false;
      }
    }

    try {
      // Passes the row on to the output or the error hop, in batch mode once it is committed.
      lookupValues(nextRow);

      if (checkFeedback(getLinesRead()) && isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "SynchronizeAfterMerge.Log.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {
      if (data.recoveredBatches) {
        // The rows that are committed are passed on before the transform stops
        passOnSettledRows();
      }
      logError("Because of an error, this transform can't continue: ", e);
      setErrors(1);
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }

    // Outside the try above: a failed batch is about the rows in it, not about this row.
    if (data.recoveredBatches && !runDueRecoveredBatchesOrStop()) {
      setOutputDone();
      return false;
    }
    return true;
  }

  private void processRowInitialize(Object[] row)
      throws HopTransformException, HopDatabaseException {
    first = false;
    data.outputRowMeta = getInputRowMeta().clone();
    data.inputRowMeta = data.outputRowMeta;
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

    if (meta.isTableNameInField()) {
      // ICache the position of the table name field
      if (data.indexOfTableNameField < 0) {
        data.indexOfTableNameField = data.inputRowMeta.indexOfValue(meta.getTableNameField());
        if (data.indexOfTableNameField < 0) {
          String message =
              "It was not possible to find table ["
                  + meta.getTableNameField()
                  + "] in the input fields.";
          logError(message);
          throw new HopTransformException(message);
        }
      }
    } else {
      data.realTableName = resolve(meta.getLookup().getTableName());
      if (Utils.isEmpty(data.realTableName)) {
        throw new HopTransformException(
            "The table name is not specified (or the input field is empty)");
      }
      data.realSchemaTable =
          data.db
              .getDatabaseMeta()
              .getQuotedSchemaTableCombination(this, data.realSchemaName, data.realTableName);
    }

    // ICache the position of the operation order field
    if (data.indexOfOperationOrderField < 0) {
      data.indexOfOperationOrderField =
          data.inputRowMeta.indexOfValue(meta.getOperationOrderField());
      if (data.indexOfOperationOrderField < 0) {
        String message =
            "It was not possible to find operation field ["
                + meta.getOperationOrderField()
                + "] in the input stream!";
        logError(message);
        throw new HopTransformException(message);
      }
    }

    data.insertValue = resolve(meta.getOrderInsert());
    data.updateValue = resolve(meta.getOrderUpdate());
    data.deleteValue = resolve(meta.getOrderDelete());

    data.insertRowMeta = new RowMeta();

    // lookup the values!
    if (isDebug()) {
      logDebug(
          BaseMessages.getString(PKG, "SynchronizeAfterMerge.Log.CheckingRow")
              + Arrays.toString(row));
    }

    data.keyIndexes = new int[meta.getLookup().getKeyConditions().size()];
    data.keyIndexes2 = new int[meta.getLookup().getKeyConditions().size()];
    for (int i = 0; i < data.keyIndexes.length; i++) {
      SynchronizeAfterMergeMeta.KeyCondition keyCondition =
          meta.getLookup().getKeyConditions().get(i);
      data.keyIndexes[i] = data.inputRowMeta.indexOfValue(keyCondition.getFieldName());
      if (data.keyIndexes[i] < 0
          && // couldn't find field!
          !CONST_IS_NULL.equalsIgnoreCase(keyCondition.getCondition())
          && // No field needed!
          !CONST_IS_NOT_NULL.equalsIgnoreCase(keyCondition.getCondition()) // No field needed!
      ) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG,
                CONST_SYNCHRONIZE_AFTER_MERGE_EXCEPTION_FIELD_REQUIRED,
                keyCondition.getFieldName()));
      }
      data.keyIndexes2[i] = data.inputRowMeta.indexOfValue(keyCondition.getFieldName2());
      if (data.keyIndexes2[i] < 0
          && // couldn't find field!
          CONST_BETWEEN.equalsIgnoreCase(keyCondition.getCondition()) // 2 fields needed!
      ) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG,
                CONST_SYNCHRONIZE_AFTER_MERGE_EXCEPTION_FIELD_REQUIRED,
                keyCondition.getFieldName2()));
      }

      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                    PKG,
                    "SynchronizeAfterMerge.Log.FieldHasDataNumbers",
                    keyCondition.getFieldName())
                + data.keyIndexes[i]);
      }
    }

    // Insert the update fields: just names. Type doesn't matter!
    for (int i = 0; i < meta.getLookup().getValueUpdates().size(); i++) {
      SynchronizeAfterMergeMeta.ValueUpdate valueUpdate = meta.getLookup().getValueUpdates().get(i);
      IValueMeta insValue = data.insertRowMeta.searchValueMeta(valueUpdate.getColumnName());
      if (insValue == null) { // Don't add twice!
        // we already checked that this value exists so it's probably safe to ignore lookup
        // failure...
        IValueMeta insertValue =
            data.inputRowMeta.searchValueMeta(valueUpdate.getFieldName()).clone();
        insertValue.setName(valueUpdate.getColumnName());
        data.insertRowMeta.addValueMeta(insertValue);
      } else {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "SynchronizeAfterMerge.Error.SameColumnInsertedTwice", insValue.getName()));
      }
    }

    // ICache the position of the compare fields in Row row
    //
    data.valueIndexes = new int[meta.getLookup().getValueUpdates().size()];
    for (int i = 0; i < data.valueIndexes.length; i++) {
      SynchronizeAfterMergeMeta.ValueUpdate valueUpdate = meta.getLookup().getValueUpdates().get(i);
      data.valueIndexes[i] = data.inputRowMeta.indexOfValue(valueUpdate.getFieldName());
      if (data.valueIndexes[i] < 0) { // couldn't find field!
        throw new HopTransformException(
            BaseMessages.getString(
                PKG,
                CONST_SYNCHRONIZE_AFTER_MERGE_EXCEPTION_FIELD_REQUIRED,
                valueUpdate.getFieldName()));
      }
      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                    PKG,
                    "SynchronizeAfterMerge.Log.FieldHasDataNumbers",
                    valueUpdate.getFieldName())
                + data.valueIndexes[i]);
      }
    }

    if (!meta.isTableNameInField()) {
      // Prepare Lookup statement
      if (meta.isPerformingLookup()) {
        data.lookupStatement = data.preparedStatements.get(data.realSchemaTable + CONST_LOOKUP);
        if (data.lookupStatement == null) {
          String sql = getLookupStatement(data.inputRowMeta);
          if (isDebug()) {
            logDebug("Preparation of the lookup SQL statement : " + sql);
          }

          data.lookupStatement = data.db.prepareSql(sql);
          data.preparedStatements.put(data.realSchemaTable + CONST_LOOKUP, data.lookupStatement);
          data.statementSql.put(data.realSchemaTable + CONST_LOOKUP, sql);
        }
      }

      // Prepare Insert statement
      data.insertStatement = data.preparedStatements.get(data.realSchemaTable + CONST_INSERT);
      if (data.insertStatement == null) {
        String sql =
            data.db.getInsertStatement(data.realSchemaName, data.realTableName, data.insertRowMeta);

        if (isDebug()) {
          logDebug("Preparation of the Insert SQL statement : " + sql);
        }

        data.insertStatement = data.db.prepareSql(sql);
        data.preparedStatements.put(data.realSchemaTable + CONST_INSERT, data.insertStatement);
        data.statementSql.put(data.realSchemaTable + CONST_INSERT, sql);
      }

      // Prepare Update Statement

      data.updateStatement = data.preparedStatements.get(data.realSchemaTable + CONST_UPDATE);
      if (data.updateStatement == null) {
        String sql = getUpdateStatement(data.inputRowMeta);

        data.updateStatement = data.db.prepareSql(sql);
        data.preparedStatements.put(data.realSchemaTable + CONST_UPDATE, data.updateStatement);
        data.statementSql.put(data.realSchemaTable + CONST_UPDATE, sql);
        if (isDebug()) {
          logDebug("Preparation of the Update SQL statement : " + sql);
        }
      }

      // Prepare delete statement
      data.deleteStatement = data.preparedStatements.get(data.realSchemaTable + CONST_DELETE);
      if (data.deleteStatement == null) {
        String sql = getDeleteStatement(data.inputRowMeta);

        data.deleteStatement = data.db.prepareSql(sql);
        data.preparedStatements.put(data.realSchemaTable + CONST_DELETE, data.deleteStatement);
        data.statementSql.put(data.realSchemaTable + CONST_DELETE, sql);
        if (isDebug()) {
          logDebug("Preparation of the Delete SQL statement : " + sql);
        }
      }
    }
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    try {
      DatabaseMeta databaseMeta = validateDatabaseConnection();
      if (databaseMeta == null) {
        return false;
      }
      if (validateSchemaName()) {
        return false;
      }

      String commitSize = resolve(meta.getCommitSize());
      String expandedCommitSize = Const.expandIntegerString(commitSize);
      data.commitSize =
          Integer.parseInt(expandedCommitSize != null ? expandedCommitSize : commitSize);

      data.databaseMeta = databaseMeta;
      data.db = new Database(this, this, databaseMeta);
      data.db.connect();
      data.db.setCommit(data.commitSize);

      // How a failed batch is handled on this connection. Only a contract validated against the
      // database (or chosen on the connection) recovers batches with RecoverableBatch; every other
      // connection keeps the handling below, exactly as it always was.
      data.batchSupport = BatchSupport.of(databaseMeta.getIDatabase(), this);
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "SynchronizeAfterMerge.Log.BatchSupport", data.batchSupport.describe()));
      }
      data.recoveredBatches =
          data.batchSupport.isRecovered()
              && meta.isUsingBatchUpdates()
              && data.commitSize > 0
              && !data.db.isAutoCommit()
              && data.db.getUseBatchInsert(true);
      if (data.recoveredBatches) {
        // Batching stays on with an error hop, without a savepoint per row: the contract tells
        // which rows of a failed batch were refused.
        data.batchMode = false;
        data.useSavepoints = false;
        return true;
      }

      data.batchMode = data.commitSize > 0 && meta.isUsingBatchUpdates();

      // Batch updates are not supported on PostgreSQL (and look-a-likes) together with error
      // handling
      //
      data.specialErrorHandling =
          getTransformMeta().isDoingErrorHandling()
              && databaseMeta.supportsErrorHandlingOnBatchUpdates();

      if (data.batchMode && data.specialErrorHandling) {
        data.batchMode = false;
        if (isBasic()) {
          logBasic(BaseMessages.getString(PKG, "SynchronizeAfterMerge.Log.BatchModeDisabled"));
        }
      }

      // A failed statement aborts the whole transaction on PostgreSQL and its look-alikes: with an
      // error hop every row runs behind a savepoint, so only the failed row is undone.
      //
      data.useSavepoints = data.db.isUsingRowSavepoints(getTransformMeta().isDoingErrorHandling());

      return true;
    } catch (HopException ke) {
      logError(
          BaseMessages.getString(
                  PKG, "SynchronizeAfterMerge.Log.ErrorOccurredDuringTransformInitialize")
              + ke.getMessage());
    }

    return false;
  }

  private boolean validateSchemaName() {
    data.realSchemaName = resolve(meta.getLookup().getSchemaName());
    if (meta.isTableNameInField() && Utils.isEmpty(meta.getTableNameField())) {
      logError(BaseMessages.getString(PKG, "SynchronizeAfterMerge.Log.Error.TableFieldnameEmpty"));
      return true;
    }
    return false;
  }

  private @Nullable DatabaseMeta validateDatabaseConnection() {
    DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
    if (databaseMeta == null) {
      logError(
          BaseMessages.getString(
              PKG, "SynchronizeAfterMerge.Init.ConnectionMissing", getTransformName()));
      return null;
    }
    return databaseMeta;
  }

  /**
   * A single-threaded (streaming) pipeline never sends the end-of-input signal that {@link
   * #processRow()} flushes on; it calls this after every batch of rows instead. Commit what is
   * pending now, so it does not sit uncommitted - and, on databases like Oracle, locked - until the
   * stream ends. See <a href="https://github.com/apache/hop/issues/8288">issue 8288</a>.
   */
  @Override
  public void batchComplete() throws HopException {
    if (data.db == null || data.db.getConnection() == null) {
      return;
    }
    if (data.recoveredBatches) {
      try {
        runOrDiscardRecoveredBatches();
      } catch (HopException e) {
        passOnSettledRows();
        throw e;
      }
      return;
    }
    try {
      emptyBatchBuffer(false);
    } catch (HopDatabaseBatchException be) {
      // The statements stay open for the next batch, so drop what failed before going on. The rest
      // is the recovery a failure in the middle of the stream gets.
      for (PreparedStatement statement : data.preparedStatements.values()) {
        data.db.clearBatch(statement);
      }
      if (getTransformMeta().isDoingErrorHandling()) {
        data.db.commit(true);
        processBatchException(be.toString(), be.getUpdateCounts(), be.getExceptionsList());
      } else {
        data.db.rollback();
        throw new HopException(
            BaseMessages.getString(PKG, "SynchronizeAfterMerge.Error.UpdatingBatch"), be);
      }
    } catch (SQLException e) {
      throw new HopDatabaseException("Unexpected error committing the database connection.", e);
    }
  }

  /**
   * The end-of-input path in {@link #processRow()} has normally flushed and disconnected by now. It
   * is skipped when the transform is stopped or fails in the middle of a row, and a single-threaded
   * (streaming) pipeline never sends end-of-input at all. In both cases the connection stayed open,
   * and with it the uncommitted transaction and every row lock it holds. See <a
   * href="https://github.com/apache/hop/issues/8288">issue 8288</a>.
   *
   * <p>A graceful stop leaves {@code getErrors() == 0}, so the pending batch is committed rather
   * than rolled back - deliberately, and in line with Table Output, Update and Delete: this
   * transform already commits every {@code commitSize} rows, so committing the final partial batch
   * on a stop keeps the same all-or-a-multiple-of-commitSize contract. Only a real error rolls
   * back. Note that {@link #emptyBatchBuffer(boolean)} still calls {@code putRow} for the committed
   * rows; on a stop {@code putRow} is a no-op (nothing reads downstream anyway), while the rows are
   * safely in the table - the same behaviour Table Output has.
   */
  @Override
  public void dispose() {
    if (data.db != null) {
      if (data.recoveredBatches) {
        // Runs what waits, or drops it when the transform failed or was stopped
        finishRecoveredTransform();
      } else if (data.db.getConnection() != null) {
        if (getErrors() > 0) {
          // The transform failed: nothing that is still pending may reach the table.
          rollback();
          data.db.disconnect();
        } else {
          finishTransform();
        }
      }
      data.db = null;
    }
    super.dispose();
  }

  private void finishTransform() {
    if (data.db != null && data.db.getConnection() != null) {
      try {
        emptyBatchBuffer(true);
      } catch (HopDatabaseBatchException be) {
        finishTransformErrorHandling(be);
      } catch (Exception dbe) {
        logError("Unexpected error committing the database connection.", dbe);
        logError(Const.getStackTracker(dbe));
        setErrors(1);
        stopAll();
      } finally {
        setOutputDone();

        if (getErrors() > 0) {
          rollback();
        }

        data.db.disconnect();
      }
    }
  }

  private void rollback() {
    try {
      data.db.rollback();
    } catch (HopDatabaseException e) {
      logError("Unexpected error rolling back the database connection.", e);
    }
  }

  /**
   * Execute and commit the pending batch of every prepared statement and pass the buffered rows on.
   *
   * @param closeStatements true at the end of the transform; false between the batches of a
   *     single-threaded pipeline, where the statements are reused.
   */
  private void emptyBatchBuffer(boolean closeStatements)
      throws SQLException, HopDatabaseException, HopTransformException, HopValueException {
    if (!data.db.getConnection().isClosed()) {
      for (String schemaTable : data.preparedStatements.keySet()) {
        // Get a commit counter per prepared statement to keep track of separate tables, etc.
        //
        Integer batchCounter = data.commitCounterMap.get(schemaTable);
        if (batchCounter == null) {
          batchCounter = 0;
        }

        // Between batches there is nothing to commit for a statement that took no rows this batch.
        // Skip it; at final completion we still fall through so emptyAndCommit closes the
        // statement.
        if (!closeStatements && batchCounter == 0) {
          continue;
        }

        PreparedStatement statement = data.preparedStatements.get(schemaTable);

        data.db.emptyAndCommit(statement, data.batchMode, batchCounter, closeStatements);
        data.commitCounterMap.put(schemaTable, 0);
      }
      for (int i = 0; i < data.batchBuffer.size(); i++) {
        Object[] row = data.batchBuffer.get(i);
        putRow(data.outputRowMeta, row);
        if (data.inputRowMeta
            .getString(row, data.indexOfOperationOrderField)
            .equals(data.insertValue)) {
          incrementLinesOutput();
        }
      }
      // Clear the buffer
      data.batchBuffer.clear();
    }
  }

  private void finishTransformErrorHandling(HopDatabaseBatchException be) {
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
  }

  // ----------------------------------------------------------------------------------------------
  // Recovered batches: the connection's batch failure contract is validated
  // ----------------------------------------------------------------------------------------------

  /**
   * Adds the statement of a row to the recovered batch of its table and statement, and queues the
   * row until the batch has run. Like the JDBC batches of batch mode, the database runs the batch
   * of the row with which it counts {@code commit size} rows added to any of the batches.
   */
  private void addToRecoveredBatch(
      String statementName,
      PreparedStatement statement,
      IRowMeta bindRowMeta,
      Object[] bindRow,
      Object[] row)
      throws HopDatabaseException, HopValueException {
    String key = data.realSchemaTable + statementName;
    RecoveredStatement recovered = data.recoveredStatements.get(key);
    if (recovered == null) {
      recovered =
          new RecoveredStatement(
              key,
              data.realSchemaTable,
              statementName,
              createRecoveredBatch(key, statement, bindRowMeta));
      data.recoveredStatements.put(key, recovered);
    }
    recovered.batch.add(bindRow, row);
    PendingRow pending = PendingRow.batched(row, statementName);
    recovered.rows.add(pending);
    data.pendingRows.add(pending);
    registerWaitingKeys(recovered, statementName, row);

    data.rowsAddedToBatches++;
    if (data.rowsAddedToBatches >= data.commitSize) {
      data.rowsAddedToBatches = 0;
      data.batchToRun = key;
    }
  }

  /**
   * Creates the recovered batch of a prepared INSERT, UPDATE or DELETE. Some drivers (DuckDB) close
   * the statement when a batch fails; the batch then prepares it again.
   */
  private RecoverableBatch createRecoveredBatch(
      String key, PreparedStatement statement, IRowMeta bindRowMeta) throws HopDatabaseException {
    return RecoverableBatch.of(
            data.db,
            statement,
            (ps, bindRow) -> data.db.setValues(bindRowMeta, bindRow, ps),
            data.batchSupport)
        .withStatementFactory(() -> prepareAgain(key));
  }

  /** Prepares the statement of a key again, after the driver closed it. */
  private PreparedStatement prepareAgain(String key) throws HopDatabaseException {
    PreparedStatement closed = data.preparedStatements.get(key);
    PreparedStatement again = data.db.prepareSql(data.statementSql.get(key));
    // The same key keeps its place in the Hashtable, and so in the order the batches run
    data.preparedStatements.put(key, again);
    if (data.insertStatement == closed) {
      data.insertStatement = again;
    }
    if (data.updateStatement == closed) {
      data.updateStatement = again;
    }
    if (data.deleteStatement == closed) {
      data.deleteStatement = again;
    }
    if (data.lookupStatement == closed) {
      data.lookupStatement = again;
    }
    return again;
  }

  /**
   * Passes the current row on in its place: a row in a batch waits until the batch has run; a
   * skipped row and a failed row go at once when nothing waits in front of them.
   */
  private void putRecoveredRow(Object[] row, boolean failed, String errorMessage, boolean batched)
      throws HopException {
    if (failed) {
      // Between two batches the transaction holds no INSERT, UPDATE or DELETE, only lookups, so
      // rolling it back loses nothing. Where a failed statement aborts the transaction (PostgreSQL,
      // DuckDB) the lookups of the next rows can run again.
      data.db.rollback();
      prepareLookupAgainIfClosed();
      if (data.lookupFailure) {
        putErrorInOrder(row, data.keyNotFoundMessage, data.stringFieldNames, CONST_SUYNC001);
      } else {
        putErrorInOrder(row, errorMessage, null, CONST_SUYNC001);
      }
    } else if (!batched) {
      putRowInOrder(row);
    }
  }

  private void putRowInOrder(Object[] row) throws HopTransformException {
    if (data.pendingRows.isEmpty()) {
      putRow(data.outputRowMeta, row);
    } else {
      data.pendingRows.add(PendingRow.output(row));
      data.settledPendingRows++;
    }
  }

  private void putErrorInOrder(
      Object[] row, String errorMessage, String errorFields, String errorCode)
      throws HopTransformException {
    if (data.pendingRows.isEmpty()) {
      putError(data.inputRowMeta, row, 1L, errorMessage, errorFields, errorCode);
    } else {
      data.pendingRows.add(PendingRow.error(row, errorMessage, errorFields, errorCode));
      data.settledPendingRows++;
    }
  }

  /**
   * Runs the batch the current row asked for, the way batch mode ran it here, and passes on what it
   * settled. When {@code commit size} rows wait behind a batch that has not run yet, every batch
   * runs, as at the end of a single-threaded iteration, so that they don't pile up.
   *
   * @return false when the transform was stopped
   */
  private boolean runDueRecoveredBatchesOrStop() {
    try {
      if (data.batchToRun != null) {
        RecoveredStatement recovered = data.recoveredStatements.get(data.batchToRun);
        data.batchToRun = null;
        runRecoveredBatch(recovered);
        passOnSettledHead();
      }
      if (data.settledPendingRows >= data.commitSize) {
        runAllRecoveredBatches();
      }
      return true;
    } catch (HopException e) {
      stopAfterAFailedBatch(e);
      return false;
    }
  }

  /**
   * Runs every recovered batch, stopping the transform when that fails.
   *
   * @return false when the transform was stopped
   */
  private boolean runAllRecoveredBatchesOrStop() {
    try {
      runAllRecoveredBatches();
      return true;
    } catch (HopException e) {
      stopAfterAFailedBatch(e);
      return false;
    }
  }

  /** Passes on what is committed, then stops the transform. */
  private void stopAfterAFailedBatch(HopException e) {
    passOnSettledRows();
    logError("Because of an error, this transform can't continue: ", e);
    setErrors(1);
    stopAll();
  }

  /**
   * Runs and commits a recovered batch. With an error hop the connection's batch failure contract
   * tells which rows of a failed batch the database refused: each row gets its verdict once.
   * Without an error hop a failed batch is rolled back and the failure is thrown, as with any
   * batch.
   */
  private void runRecoveredBatch(RecoveredStatement recovered) throws HopException {
    if (recovered == null || recovered.batch.isEmpty()) {
      return;
    }
    IBatchRowListener listener = recoveredBatchListener(recovered);
    try {
      if (getTransformMeta().isDoingErrorHandling()) {
        recovered.batch.flush(listener);
      } else {
        recovered.batch.flushOrFail(listener);
      }
    } catch (HopDatabaseBatchException e) {
      throw new HopDatabaseBatchException(
          BaseMessages.getString(
              PKG,
              "SynchronizeAfterMerge.Exception.BatchFailed",
              recovered.statementName,
              recovered.schemaTable),
          e);
    } finally {
      // The batch is empty now, written or not
      recovered.waitingKeys.clear();
      recovered.unknownKey = false;
    }
  }

  /**
   * Settles the rows of a recovered batch: the batch gives every row its verdict once, in the order
   * the rows were added. {@link #passOnSettledHead()} passes them on afterwards.
   */
  private IBatchRowListener recoveredBatchListener(RecoveredStatement recovered) {
    return new IBatchRowListener() {
      @Override
      public void rowWritten(Object[] outputRow) {
        recovered.rows.poll().written();
        data.settledPendingRows++;
      }

      @Override
      public void rowRejected(Object[] outputRow, String message, boolean attempted) {
        recovered.rows.poll().fail(message, null, attempted ? CONST_SUYNC002 : CONST_SUYNC003);
        data.settledPendingRows++;
      }
    };
  }

  /**
   * Runs every recovered batch that holds rows, in the order the batches always ran at the end of
   * the input: the order in which {@link SynchronizeAfterMergeData#preparedStatements} lists the
   * statements. As there, the commit counters of the statements start over. Then passes on what was
   * settled.
   */
  private void runAllRecoveredBatches() throws HopException {
    for (String key : data.preparedStatements.keySet()) {
      runRecoveredBatch(data.recoveredStatements.get(key));
      if (data.commitCounterMap.containsKey(key)) {
        data.commitCounterMap.put(key, 0);
      }
    }
    passOnSettledHead();
  }

  /** Passes on the waiting rows in input order, up to the first one whose batch hasn't run. */
  private void passOnSettledHead() throws HopTransformException {
    while (!data.pendingRows.isEmpty() && data.pendingRows.peekFirst().isSettled()) {
      passOn(data.pendingRows.pollFirst());
    }
  }

  /**
   * When the transform fails: passes on the waiting rows that are settled, in input order, those
   * behind a row whose batch never ran as well, since they are committed. The rows still in a batch
   * stay, until they are dropped.
   */
  private void passOnSettledRows() {
    try {
      Iterator<PendingRow> rows = data.pendingRows.iterator();
      while (rows.hasNext()) {
        PendingRow row = rows.next();
        if (row.isSettled()) {
          rows.remove();
          passOn(row);
        }
      }
    } catch (HopTransformException e) {
      logError("Unable to pass on the rows that were written before the failure", e);
    }
  }

  private void passOn(PendingRow row) throws HopTransformException {
    data.settledPendingRows--;
    if (row.isFailed()) {
      putError(
          data.inputRowMeta,
          row.getRow(),
          1L,
          row.getErrorMessage(),
          row.getErrorFields(),
          row.getErrorCode());
      return;
    }
    putRow(data.outputRowMeta, row.getRow());
    if (CONST_INSERT.equals(row.getStatement())) {
      incrementLinesOutput();
    } else if (row.isBatched()) {
      incrementLinesUpdated();
    }
  }

  /**
   * At the end of a single-threaded iteration, at the end of the input and when the transform is
   * disposed: runs what waits in the recovered batches, or drops it when the transform failed or
   * was stopped, so that no row is committed that is not passed on.
   */
  private void runOrDiscardRecoveredBatches() throws HopException {
    if (getErrors() > 0 || isStopped()) {
      // What is committed is passed on, as far as a stopped transform still passes rows on
      passOnSettledRows();
      int dropped = 0;
      for (RecoveredStatement recovered : data.recoveredStatements.values()) {
        dropped += recovered.batch.discard().size();
        recovered.rows.clear();
        recovered.waitingKeys.clear();
        recovered.unknownKey = false;
      }
      data.pendingRows.clear();
      data.settledPendingRows = 0;
      if (dropped > 0) {
        logError(
            BaseMessages.getString(
                PKG, "SynchronizeAfterMerge.Log.BatchDiscarded", String.valueOf(dropped)));
      }
      return;
    }
    runAllRecoveredBatches();
  }

  /**
   * The end of the input or of the transform with recovered batches: runs or drops what waits,
   * closes the statements, and commits, or rolls back when the transform failed.
   */
  private void finishRecoveredTransform() {
    if (data.db == null || data.db.getConnection() == null) {
      return;
    }
    try {
      runOrDiscardRecoveredBatches();
    } catch (HopException e) {
      passOnSettledRows();
      logError("Because of an error, this transform can't continue: ", e);
      setErrors(1);
      stopAll();
    } finally {
      for (PreparedStatement statement : data.preparedStatements.values()) {
        try {
          data.db.closePreparedStatement(statement);
        } catch (HopDatabaseException e) {
          logError("Unable to close a prepared statement", e);
        }
      }
      setOutputDone();
      if (getErrors() > 0) {
        rollback();
      }
      data.db.disconnect();
    }
  }

  /**
   * Some drivers (DuckDB) close a prepared statement when it fails. After a row failed, the lookup
   * statement is prepared again if the driver closed it, so that the next rows can still be looked
   * up instead of all failing on the closed statement.
   */
  private void prepareLookupAgainIfClosed() throws HopDatabaseException {
    if (data.lookupStatement == null || !isClosed(data.lookupStatement)) {
      return;
    }
    prepareAgain(data.realSchemaTable + CONST_LOOKUP);
  }

  /**
   * @return true if the driver says it closed the statement; a driver that can't say is taken to
   *     keep it open
   */
  private static boolean isClosed(Statement statement) {
    try {
      return statement.isClosed();
    } catch (SQLException e) {
      return false;
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Keys that wait in a batch: operations on one key run in input order
  // ----------------------------------------------------------------------------------------------

  /**
   * Finds where the key columns get their values, for the keys of the rows in the batches: the
   * fields the = conditions compare (IS NULL and IS NOT NULL compare no value), and the fields the
   * INSERT and the UPDATE write into those columns.
   */
  private void prepareWaitingKeys() {
    List<SynchronizeAfterMergeMeta.KeyCondition> conditions = meta.getLookup().getKeyConditions();
    List<SynchronizeAfterMergeMeta.ValueUpdate> values = meta.getLookup().getValueUpdates();
    List<Integer> keyIndexes = new ArrayList<>();
    List<Integer> insertIndexes = new ArrayList<>();
    List<Integer> updateIndexes = new ArrayList<>();
    boolean insertKnown = true;
    boolean updateWritesKey = false;
    boolean equalities = true;
    for (int i = 0; i < conditions.size(); i++) {
      SynchronizeAfterMergeMeta.KeyCondition condition = conditions.get(i);
      String comparator = Const.NVL(condition.getCondition(), "").trim();
      if (CONST_IS_NULL.equalsIgnoreCase(comparator)
          || CONST_IS_NOT_NULL.equalsIgnoreCase(comparator)) {
        continue;
      }
      if (!"=".equals(comparator) && !CONST_EQUALS_OR_NULL.equalsIgnoreCase(comparator)) {
        equalities = false;
        continue;
      }
      int keyIndex = data.keyIndexes[i];
      keyIndexes.add(keyIndex);

      int inserted = valueWriting(values, condition.getColumnName(), false);
      insertKnown &= inserted >= 0;
      insertIndexes.add(inserted < 0 ? keyIndex : data.valueIndexes[inserted]);

      int updated = valueWriting(values, condition.getColumnName(), true);
      updateWritesKey |= updated >= 0;
      updateIndexes.add(updated < 0 ? keyIndex : data.valueIndexes[updated]);
    }
    data.keyFieldIndexes = toArray(keyIndexes);
    data.insertKeyIndexes = insertKnown ? toArray(insertIndexes) : null;
    data.updateKeyIndexes = toArray(updateIndexes);
    data.updateWritesKey = updateWritesKey;
    data.keysAreEqualities = equalities;
  }

  private static int[] toArray(List<Integer> list) {
    int[] array = new int[list.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = list.get(i);
    }
    return array;
  }

  /**
   * @param column a column of the table
   * @param updatedOnly only the fields the UPDATE sets
   * @return the position of the value field that writes the column, the same name first and then
   *     one that only differs in case; -1 when no value field writes it
   */
  private static int valueWriting(
      List<SynchronizeAfterMergeMeta.ValueUpdate> values, String column, boolean updatedOnly) {
    int ignoringCase = -1;
    for (int i = 0; i < values.size(); i++) {
      SynchronizeAfterMergeMeta.ValueUpdate value = values.get(i);
      if (updatedOnly && !value.isUpdate()) {
        continue;
      }
      String target = value.getColumnName();
      if (target != null && target.equals(column)) {
        return i;
      }
      if (ignoringCase < 0 && target != null && target.equalsIgnoreCase(column)) {
        ignoringCase = i;
      }
    }
    return ignoringCase;
  }

  /**
   * Remembers the keys of a row that waits in a batch: the key a DELETE or UPDATE writes by, the
   * key an UPDATE gives the row, the key an INSERT writes, or that the INSERT's key is unknown.
   */
  private void registerWaitingKeys(RecoveredStatement recovered, String statementName, Object[] row)
      throws HopValueException {
    if (!data.keysAreEqualities) {
      // Every row of another statement waits for this batch anyway
      return;
    }
    IRowMeta rowMeta = data.inputRowMeta;
    if (CONST_INSERT.equals(statementName)) {
      if (data.insertKeyIndexes == null) {
        recovered.unknownKey = true;
      } else {
        recovered.waitingKeys.add(PendingKeys.key(rowMeta, row, data.insertKeyIndexes));
      }
      return;
    }
    recovered.waitingKeys.add(PendingKeys.key(rowMeta, row, data.keyFieldIndexes));
    if (CONST_UPDATE.equals(statementName) && data.updateWritesKey) {
      recovered.waitingKeys.add(PendingKeys.key(rowMeta, row, data.updateKeyIndexes));
    }
  }

  /**
   * Whether the pending batches must run before this row, so that the statements of one key run in
   * input order: a batch of another statement of the same table waits to write a row with its key.
   * Repeats within one statement need not wait, a batch runs in the order its rows were added; but
   * a row that is looked up first ("Perform lookup") waits for every batch with its key, since the
   * lookup can't see the rows in them. A key condition that is not an equality can match any
   * waiting row, so then any batch of another statement of the table runs first.
   */
  private boolean keyWaitsInABatch(Object[] row) throws HopException {
    if (data.pendingRows.isEmpty()) {
      return false;
    }
    String operation = data.inputRowMeta.getString(row, data.indexOfOperationOrderField);
    if (operation == null) {
      return false;
    }
    String statement = null;
    if (operation.equals(data.insertValue)) {
      statement = CONST_INSERT;
    } else if (operation.equals(data.updateValue)) {
      statement = CONST_UPDATE;
    } else if (operation.equals(data.deleteValue)) {
      statement = CONST_DELETE;
    }
    boolean lookedUp = meta.isPerformingLookup() && !CONST_INSERT.equals(statement);
    if (statement == null && !lookedUp) {
      return false;
    }
    String schemaTable = schemaTableOf(row);
    if (schemaTable == null) {
      return false;
    }
    List<List<Object>> keys = rowKeys(statement, row);
    for (String other : new String[] {CONST_INSERT, CONST_UPDATE, CONST_DELETE}) {
      RecoveredStatement waiting = data.recoveredStatements.get(schemaTable + other);
      if (waiting == null || waiting.batch.isEmpty()) {
        continue;
      }
      if (other.equals(statement) && !lookedUp) {
        continue;
      }
      if (!data.keysAreEqualities || waiting.unknownKey) {
        return true;
      }
      for (List<Object> key : keys) {
        if (waiting.waitingKeys.contains(key)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * The keys a row is about: the key an INSERT writes (none when the database gives it), the key an
   * UPDATE or DELETE writes by, and the key an UPDATE gives the row.
   */
  private List<List<Object>> rowKeys(String statement, Object[] row) throws HopValueException {
    IRowMeta rowMeta = data.inputRowMeta;
    if (CONST_INSERT.equals(statement)) {
      return data.insertKeyIndexes == null
          ? List.of()
          : List.of(PendingKeys.key(rowMeta, row, data.insertKeyIndexes));
    }
    List<Object> key = PendingKeys.key(rowMeta, row, data.keyFieldIndexes);
    if (CONST_UPDATE.equals(statement) && data.updateWritesKey) {
      return List.of(key, PendingKeys.key(rowMeta, row, data.updateKeyIndexes));
    }
    return List.of(key);
  }

  /** The table a row is written to, quoted as in the statements; null when its name is empty. */
  private String schemaTableOf(Object[] row) throws HopValueException {
    if (!meta.isTableNameInField()) {
      return data.realSchemaTable;
    }
    String tableName = data.inputRowMeta.getString(row, data.indexOfTableNameField);
    if (Utils.isEmpty(tableName)) {
      return null;
    }
    return data.db
        .getDatabaseMeta()
        .getQuotedSchemaTableCombination(this, data.realSchemaName, tableName);
  }
}
