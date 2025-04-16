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
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class SynchronizeAfterMerge
    extends BaseTransform<SynchronizeAfterMergeMeta, SynchronizeAfterMergeData> {

  private static final Class<?> PKG = SynchronizeAfterMergeMeta.class;
  public static final String CONST_IS_NULL = "IS NULL";
  public static final String CONST_IS_NOT_NULL = "IS NOT NULL";
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
        Object[] insertRowData = new Object[data.valuenrs.length];
        for (int i = 0; i < data.valuenrs.length; i++) {
          insertRowData[i] = row[data.valuenrs[i]];
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
          }
        }

        // For PG & GP, we add a savepoint before the row.
        // Then revert to the savepoint afterwards... (not a transaction, so hopefully still fast)
        //
        if (data.specialErrorHandling && data.supportsSavepoints) {
          data.savepoint = data.db.setSavepoint();
        }

        // Set the values on the prepared statement...
        data.db.setValues(data.insertRowMeta, insertRowData, data.insertStatement);
        data.db.insertRow(data.insertStatement, data.batchMode);
        performInsert = true;
        if (!data.batchMode) {
          incrementLinesOutput();
        }
        if (isRowLevel()) {
          logRowlevel("Written row: " + data.insertRowMeta.getString(insertRowData));
        }

      } else {

        Object[] lookupRow = new Object[data.keynrs.length];
        int lookupIndex = 0;
        for (int i = 0; i < meta.getKeyStream().length; i++) {
          if (data.keynrs[i] >= 0) {
            lookupRow[lookupIndex] = row[data.keynrs[i]];
            lookupIndex++;
          }
          if (data.keynrs2[i] >= 0) {
            lookupRow[lookupIndex] = row[data.keynrs2[i]];
            lookupIndex++;
          }
        }
        boolean updateorDelete = false;
        if (meta.isPerformLookup()) {

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
              data.stringFieldnames = "";
              for (int i = 0; i < data.lookupParameterRowMeta.size(); i++) {
                if (i > 0) {
                  data.stringFieldnames += ", ";
                }
                data.stringFieldnames += data.lookupParameterRowMeta.getValueMeta(i).getName();
              }
            }
            data.lookupFailure = true;
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

            for (int i = 0; i < data.valuenrs.length; i++) {
              if (meta.getUpdate()[i].booleanValue()) {
                IValueMeta valueMeta = data.inputRowMeta.getValueMeta(data.valuenrs[i]);
                IValueMeta retMeta = data.db.getReturnRowMeta().getValueMeta(i);

                Object rowvalue = row[data.valuenrs[i]];
                Object retvalue = add[i];

                if (valueMeta.compare(rowvalue, retMeta, retvalue) != 0) {
                  updateorDelete = true;
                }
              }
            }
          }
        } // end if perform lookup

        if (operation.equals(data.updateValue)) {
          if (!meta.isPerformLookup() || updateorDelete) {
            // UPDATE :

            if (meta.isTableNameInField()) {
              data.updateStatement =
                  data.preparedStatements.get(data.realSchemaTable + CONST_UPDATE);
              if (data.updateStatement == null) {
                String sql = getUpdateStatement(data.inputRowMeta);

                data.updateStatement = data.db.prepareSql(sql);
                data.preparedStatements.put(
                    data.realSchemaTable + CONST_UPDATE, data.updateStatement);
                if (isDebug()) {
                  logDebug("Preparation of the Update SQL statement : " + sql);
                }
              }
            }

            // Create the update row...
            Object[] updateRow = new Object[data.updateParameterRowMeta.size()];
            int j = 0;
            for (int i = 0; i < data.valuenrs.length; i++) {
              if (meta.getUpdate()[i].booleanValue()) {
                updateRow[j] = row[data.valuenrs[i]]; // the setters
                j++;
              }
            }

            // add the where clause parameters, they are exactly the same for lookup and update
            for (int i = 0; i < lookupRow.length; i++) {
              updateRow[j + i] = lookupRow[i];
            }

            // For PG & GP, we add a savepoint before the row.
            // Then revert to the savepoint afterwards... (not a transaction, so hopefully still
            // fast)
            //
            if (data.specialErrorHandling && data.supportsSavepoints) {
              data.savepoint = data.db.setSavepoint();
            }
            data.db.setValues(data.updateParameterRowMeta, updateRow, data.updateStatement);
            if (isRowLevel()) {
              logRowlevel(
                  BaseMessages.getString(
                      PKG,
                      "SynchronizeAfterMerge.Log.SetValuesForUpdate",
                      data.updateParameterRowMeta.getString(updateRow),
                      data.inputRowMeta.getString(row)));
            }
            data.db.insertRow(data.updateStatement, data.batchMode);
            performUpdate = true;
            incrementLinesUpdated();

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
              if (isDebug()) {
                logDebug("Preparation of the Delete SQL statement : " + sql);
              }
            }
          }

          Object[] deleteRow = new Object[data.deleteParameterRowMeta.size()];
          int deleteIndex = 0;

          for (int i = 0; i < meta.getKeyStream().length; i++) {
            if (data.keynrs[i] >= 0) {
              deleteRow[deleteIndex] = row[data.keynrs[i]];
              deleteIndex++;
            }
            if (data.keynrs2[i] >= 0) {
              deleteRow[deleteIndex] = row[data.keynrs2[i]];
              deleteIndex++;
            }
          }

          // For PG & GP, we add a savepoint before the row.
          // Then revert to the savepoint afterwards... (not a transaction, so hopefully still fast)
          //
          if (data.specialErrorHandling && data.supportsSavepoints) {
            data.savepoint = data.db.setSavepoint();
          }
          data.db.setValues(data.deleteParameterRowMeta, deleteRow, data.deleteStatement);
          if (isRowLevel()) {
            logRowlevel(
                BaseMessages.getString(
                    PKG,
                    "SynchronizeAfterMerge.Log.SetValuesForDelete",
                    data.deleteParameterRowMeta.getString(deleteRow),
                    data.inputRowMeta.getString(row)));
          }
          data.db.insertRow(data.deleteStatement, data.batchMode);
          performDelete = true;
          incrementLinesUpdated();
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
          commitCounter = Integer.valueOf(0);
        }
        data.commitCounterMap.put(tableName, Integer.valueOf(commitCounter.intValue() + 1));

        // Release the savepoint if needed
        //
        if (data.specialErrorHandling && data.supportsSavepoints && data.releaseSavepoint) {
          data.db.releaseSavepoint(data.savepoint);
        }

        // Perform a commit if needed
        //
        if (commitCounter > 0 && (commitCounter % data.commitSize) == 0) {
          if (data.batchMode) {
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

        if (data.specialErrorHandling
            && data.supportsSavepoints
            && (data.savepoint != null || !data.lookupFailure)) {
          // do this when savepoint was set, and this is not lookup failure
          data.db.rollback(data.savepoint);
          if (data.releaseSavepoint) {
            data.db.releaseSavepoint(data.savepoint);
          }
        }
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

    if (data.batchMode) {
      if (sendToErrorRow) {
        if (batchProblem) {
          data.batchBuffer.add(row);
          processBatchException(errorMessage, updateCounts, exceptionsList);
        } else {
          // Simply add this row to the error row
          putError(data.inputRowMeta, row, 1L, errorMessage, null, "SUYNC002");
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
              data.stringFieldnames,
              "SUYNC001");
        } else {
          putError(data.inputRowMeta, row, 1, errorMessage, null, "SUYNC001");
        }
      }
    }
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
          // send the error forward
          putRow(data.outputRowMeta, row);
          incrementLinesOutput();
        } else {
          String exMessage = errorMessage;
          if (errNr < exceptionsList.size()) {
            SQLException se = (SQLException) exceptionsList.get(errNr);
            errNr++;
            exMessage = se.toString();
          }
          putError(data.outputRowMeta, row, 1L, exMessage, null, "SUYNC002");
        }
      }
    } else {
      // If we don't have update counts, it probably means the DB doesn't support it.
      // In this case we don't have a choice but to consider all inserted rows to be error rows.
      //
      for (int i = 0; i < data.batchBuffer.size(); i++) {
        Object[] row = data.batchBuffer.get(i);
        putError(data.outputRowMeta, row, 1L, errorMessage, null, "SUYNC003");
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

    for (int i = 0; i < meta.getUpdateLookup().length; i++) {
      if (i != 0) {
        sql.append(", ");
      }
      sql.append(data.databaseMeta.quoteField(meta.getUpdateLookup()[i]));
      data.lookupReturnRowMeta.addValueMeta(
          rowMeta.searchValueMeta(meta.getUpdateStream()[i]).clone());
    }

    sql.append(" FROM ").append(data.realSchemaTable).append(" WHERE ");

    for (int i = 0; i < meta.getKeyLookup().length; i++) {
      if (i != 0) {
        sql.append(" AND ");
      }
      sql.append(data.databaseMeta.quoteField(meta.getKeyLookup()[i]));
      if (CONST_BETWEEN.equalsIgnoreCase(meta.getKeyCondition()[i])) {
        sql.append(CONST_BETWEEN_AND);
        data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(meta.getKeyStream()[i]));
        data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(meta.getKeyStream2()[i]));
      } else {
        if (CONST_IS_NULL.equalsIgnoreCase(meta.getKeyCondition()[i])
            || CONST_IS_NOT_NULL.equalsIgnoreCase(meta.getKeyCondition()[i])) {
          sql.append(" ").append(meta.getKeyCondition()[i]).append(" ");
        } else {
          sql.append(" ").append(meta.getKeyCondition()[i]).append(" ? ");
          data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(meta.getKeyStream()[i]));
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

    for (int i = 0; i < meta.getUpdateLookup().length; i++) {
      if (meta.getUpdate()[i].booleanValue()) {
        if (comma) {
          sql.append(",   ");
        } else {
          comma = true;
        }

        sql.append(data.databaseMeta.quoteField(meta.getUpdateLookup()[i]));
        sql.append(" = ?").append(Const.CR);
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(meta.getUpdateStream()[i]).clone());
      }
    }

    sql.append("WHERE ");

    for (int i = 0; i < meta.getKeyLookup().length; i++) {
      if (i != 0) {
        sql.append("AND   ");
      }
      sql.append(data.databaseMeta.quoteField(meta.getKeyLookup()[i]));
      if (CONST_BETWEEN.equalsIgnoreCase(meta.getKeyCondition()[i])) {
        sql.append(CONST_BETWEEN_AND);
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(meta.getKeyStream()[i]));
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(meta.getKeyStream2()[i]));
      } else if (CONST_IS_NULL.equalsIgnoreCase(meta.getKeyCondition()[i])
          || CONST_IS_NOT_NULL.equalsIgnoreCase(meta.getKeyCondition()[i])) {
        sql.append(" ").append(meta.getKeyCondition()[i]).append(" ");
      } else {
        sql.append(" ").append(meta.getKeyCondition()[i]).append(" ? ");
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(meta.getKeyStream()[i]).clone());
      }
    }
    return sql.toString();
  }

  public String getDeleteStatement(IRowMeta rowMeta) throws HopDatabaseException {
    data.deleteParameterRowMeta = new RowMeta();

    StringBuilder sql = new StringBuilder();

    sql.append("DELETE FROM ").append(data.realSchemaTable).append(Const.CR);

    sql.append("WHERE ");

    for (int i = 0; i < meta.getKeyLookup().length; i++) {
      if (i != 0) {
        sql.append("AND   ");
      }
      sql.append(data.databaseMeta.quoteField(meta.getKeyLookup()[i]));
      if (CONST_BETWEEN.equalsIgnoreCase(meta.getKeyCondition()[i])) {
        sql.append(CONST_BETWEEN_AND);
        data.deleteParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(meta.getKeyStream()[i]));
        data.deleteParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(meta.getKeyStream2()[i]));
      } else if (CONST_IS_NULL.equalsIgnoreCase(meta.getKeyCondition()[i])
          || CONST_IS_NOT_NULL.equalsIgnoreCase(meta.getKeyCondition()[i])) {
        sql.append(" ").append(meta.getKeyCondition()[i]).append(" ");
      } else {
        sql.append(" ").append(meta.getKeyCondition()[i]).append(" ? ");
        data.deleteParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(meta.getKeyStream()[i]));
      }
    }
    return sql.toString();
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] nextRow = getRow(); // Get row from input rowset & set row busy!
    if (nextRow == null) { // no more input to be expected...
      finishTransform();
      return false;
    }

    if (first) {
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
        data.realTableName = resolve(meta.getTableName());
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
                + Arrays.toString(nextRow));
      }

      data.keynrs = new int[meta.getKeyStream().length];
      data.keynrs2 = new int[meta.getKeyStream().length];
      for (int i = 0; i < meta.getKeyStream().length; i++) {
        data.keynrs[i] = data.inputRowMeta.indexOfValue(meta.getKeyStream()[i]);
        if (data.keynrs[i] < 0
            && // couldn't find field!
            !CONST_IS_NULL.equalsIgnoreCase(meta.getKeyCondition()[i])
            && // No field needed!
            !CONST_IS_NOT_NULL.equalsIgnoreCase(meta.getKeyCondition()[i]) // No field needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG,
                  CONST_SYNCHRONIZE_AFTER_MERGE_EXCEPTION_FIELD_REQUIRED,
                  meta.getKeyStream()[i]));
        }
        data.keynrs2[i] = data.inputRowMeta.indexOfValue(meta.getKeyStream2()[i]);
        if (data.keynrs2[i] < 0
            && // couldn't find field!
            CONST_BETWEEN.equalsIgnoreCase(meta.getKeyCondition()[i]) // 2 fields needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG,
                  CONST_SYNCHRONIZE_AFTER_MERGE_EXCEPTION_FIELD_REQUIRED,
                  meta.getKeyStream2()[i]));
        }

        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                      PKG, "SynchronizeAfterMerge.Log.FieldHasDataNumbers", meta.getKeyStream()[i])
                  + data.keynrs[i]);
        }
      }

      // Insert the update fields: just names. Type doesn't matter!
      for (int i = 0; i < meta.getUpdateLookup().length; i++) {
        IValueMeta insValue = data.insertRowMeta.searchValueMeta(meta.getUpdateLookup()[i]);
        if (insValue == null) { // Don't add twice!
          // we already checked that this value exists so it's probably safe to ignore lookup
          // failure...
          IValueMeta insertValue =
              data.inputRowMeta.searchValueMeta(meta.getUpdateStream()[i]).clone();
          insertValue.setName(meta.getUpdateLookup()[i]);
          data.insertRowMeta.addValueMeta(insertValue);
        } else {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "SynchronizeAfterMerge.Error.SameColumnInsertedTwice", insValue.getName()));
        }
      }

      // ICache the position of the compare fields in Row row
      //
      data.valuenrs = new int[meta.getUpdateLookup().length];
      for (int i = 0; i < meta.getUpdateLookup().length; i++) {
        data.valuenrs[i] = data.inputRowMeta.indexOfValue(meta.getUpdateStream()[i]);
        if (data.valuenrs[i] < 0) { // couldn't find field!
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG,
                  CONST_SYNCHRONIZE_AFTER_MERGE_EXCEPTION_FIELD_REQUIRED,
                  meta.getUpdateStream()[i]));
        }
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                      PKG,
                      "SynchronizeAfterMerge.Log.FieldHasDataNumbers",
                      meta.getUpdateStream()[i])
                  + data.valuenrs[i]);
        }
      }

      if (!meta.isTableNameInField()) {
        // Prepare Lookup statement
        if (meta.isPerformLookup()) {
          data.lookupStatement = data.preparedStatements.get(data.realSchemaTable + CONST_LOOKUP);
          if (data.lookupStatement == null) {
            String sql = getLookupStatement(data.inputRowMeta);
            if (isDebug()) {
              logDebug("Preparation of the lookup SQL statement : " + sql);
            }

            data.lookupStatement = data.db.prepareSql(sql);
            data.preparedStatements.put(data.realSchemaTable + CONST_LOOKUP, data.lookupStatement);
          }
        }

        // Prepare Insert statement
        data.insertStatement = data.preparedStatements.get(data.realSchemaTable + CONST_INSERT);
        if (data.insertStatement == null) {
          String sql =
              data.db.getInsertStatement(
                  data.realSchemaName, data.realTableName, data.insertRowMeta);

          if (isDebug()) {
            logDebug("Preparation of the Insert SQL statement : " + sql);
          }

          data.insertStatement = data.db.prepareSql(sql);
          data.preparedStatements.put(data.realSchemaTable + CONST_INSERT, data.insertStatement);
        }

        // Prepare Update Statement

        data.updateStatement = data.preparedStatements.get(data.realSchemaTable + CONST_UPDATE);
        if (data.updateStatement == null) {
          String sql = getUpdateStatement(data.inputRowMeta);

          data.updateStatement = data.db.prepareSql(sql);
          data.preparedStatements.put(data.realSchemaTable + CONST_UPDATE, data.updateStatement);
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
          if (isDebug()) {
            logDebug("Preparation of the Delete SQL statement : " + sql);
          }
        }
      }
    } // end if first

    try {
      lookupValues(nextRow); // add new values to the row in rowset[0].
      if (!data.batchMode) {
        putRow(data.outputRowMeta, nextRow); // copy row to output rowset(s)
      }

      if (checkFeedback(getLinesRead()) && isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "SynchronizeAfterMerge.Log.LineNumber") + getLinesRead());
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

  @Override
  public boolean init() {
    if (super.init()) {
      try {

        DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
        if (databaseMeta == null) {
          logError(
              BaseMessages.getString(
                  PKG, "SynchronizeAfterMerge.Init.ConnectionMissing", getTransformName()));
          return false;
        }

        meta.normalizeAllocationFields();
        data.realSchemaName = resolve(meta.getSchemaName());
        if (meta.isTableNameInField() && Utils.isEmpty(meta.getTableNameField())) {
          logError(
              BaseMessages.getString(PKG, "SynchronizeAfterMerge.Log.Error.TableFieldnameEmpty"));
          return false;
        }

        // if we are using Oracle then set releaseSavepoint to false
        // TODO: change when we remove those variants of IDatabase
        if (databaseMeta.getIDatabase().isOracleVariant()) {
          data.releaseSavepoint = false;
        }

        data.commitSize = Integer.parseInt(resolve(meta.getCommitSize()));
        data.batchMode = data.commitSize > 0 && meta.useBatchUpdate();

        // Batch updates are not supported on PostgreSQL (and look-a-likes) together with error
        // handling
        //
        data.specialErrorHandling =
            getTransformMeta().isDoingErrorHandling()
                && databaseMeta.supportsErrorHandlingOnBatchUpdates();

        data.supportsSavepoints = databaseMeta.getIDatabase().isUseSafePoints();

        if (data.batchMode && data.specialErrorHandling) {
          data.batchMode = false;
          if (isBasic()) {
            logBasic(BaseMessages.getString(PKG, "SynchronizeAfterMerge.Log.BatchModeDisabled"));
          }
        }

        data.databaseMeta = databaseMeta;
        data.db = new Database(this, this, databaseMeta);
        data.db.connect();
        data.db.setCommit(data.commitSize);

        return true;
      } catch (HopException ke) {
        logError(
            BaseMessages.getString(
                    PKG, "SynchronizeAfterMerge.Log.ErrorOccurredDuringTransformInitialize")
                + ke.getMessage());
      }
    }
    return false;
  }

  private void finishTransform() {
    if (data.db != null && data.db.getConnection() != null) {
      try {
        if (!data.db.getConnection().isClosed()) {
          for (String schemaTable : data.preparedStatements.keySet()) {
            // Get a commit counter per prepared statement to keep track of separate tables, etc.
            //
            Integer batchCounter = data.commitCounterMap.get(schemaTable);
            if (batchCounter == null) {
              batchCounter = 0;
            }

            PreparedStatement insertStatement = data.preparedStatements.get(schemaTable);

            data.db.emptyAndCommit(insertStatement, data.batchMode, batchCounter);
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

        data.db.disconnect();
      }
    }
  }
}
