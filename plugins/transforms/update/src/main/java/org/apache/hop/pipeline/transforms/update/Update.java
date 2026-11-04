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

package org.apache.hop.pipeline.transforms.update;

import com.google.common.annotations.VisibleForTesting;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
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

/** Update data in a database table, does NOT ever perform an insert. */
public class Update extends BaseTransform<UpdateMeta, UpdateData> {
  private static final Class<?> PKG = UpdateMeta.class;
  public static final String CONST_IS_NULL = "IS NULL";
  public static final String CONST_UPDATE_EXCEPTION_FIELD_REQUIRED =
      "Update.Exception.FieldRequired";
  public static final String CONST_IS_NOT_NULL = "IS NOT NULL";
  public static final String CONST_NULL = "= ~NULL";
  public static final String CONST_BETWEEN = "BETWEEN";

  public Update(
      TransformMeta transformMeta,
      UpdateMeta meta,
      UpdateData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /**
   * @return the row for the output, or null when it goes to the error hop or waits in the batch
   */
  private synchronized Object[] lookupValues(IRowMeta rowMeta, Object[] row) throws HopException {
    Object[] outputRow = row;
    Object[] add;
    boolean batched = false;

    // Create the output row and copy the input values. add flag field!
    if (!Utils.isEmpty(meta.getIgnoreFlagField())) {

      outputRow = new Object[data.outputRowMeta.size()];
      if (rowMeta.size() >= 0) {
        System.arraycopy(row, 0, outputRow, 0, rowMeta.size());
      }
    }

    // OK, now do the lookup.
    // We need the lookup values for that.
    Object[] lookupRow = new Object[data.lookupParameterRowMeta.size()];
    int lookupIndex = 0;

    for (int i = 0; i < data.keynrs.length; i++) {
      if (data.keynrs[i] >= 0) {
        lookupRow[lookupIndex] = row[data.keynrs[i]];
        lookupIndex++;
      }
      if (data.keynrs2[i] >= 0) {
        lookupRow[lookupIndex] = row[data.keynrs2[i]];
        lookupIndex++;
      }
    }
    IRowMeta returnRowMeta = null;
    if (!meta.isSkipLookup()) {
      data.db.setValues(data.lookupParameterRowMeta, lookupRow, data.prepStatementLookup);
      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG,
                "Update.Log.ValuesSetForLookup",
                data.lookupParameterRowMeta.getString(lookupRow),
                rowMeta.getString(row)));
      }
      add = data.db.getLookup(data.prepStatementLookup);
      returnRowMeta = data.db.getReturnRowMeta();
    } else {
      add = null;
    }

    incrementLinesInput();

    if (add == null && !meta.isSkipLookup()) {
      /*
       * nothing was found: throw error!
       */
      if (!meta.isErrorIgnored()) {
        if (getTransformMeta().isDoingErrorHandling()) {
          outputRow = null;
          if (data.stringErrorKeyNotFound == null) {
            data.stringErrorKeyNotFound =
                BaseMessages.getString(PKG, "Update.Exception.KeyCouldNotFound")
                    + data.lookupParameterRowMeta.getString(lookupRow);
            data.stringFieldnames = "";
            for (int i = 0; i < data.lookupParameterRowMeta.size(); i++) {
              if (i > 0) {
                data.stringFieldnames += ", ";
              }
              data.stringFieldnames += data.lookupParameterRowMeta.getValueMeta(i).getName();
            }
          }
          // Sent to the error hop by processRow(), after the savepoint section of this row.
          data.keyNotFound = true;
        } else {
          throw new HopDatabaseException(
              BaseMessages.getString(PKG, "Update.Exception.KeyCouldNotFound")
                  + data.lookupParameterRowMeta.getString(lookupRow));
        }
      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "Update.Log.KeyCouldNotFound")
                  + data.lookupParameterRowMeta.getString(lookupRow));
        }
        if (!Utils.isEmpty(meta.getIgnoreFlagField())) { // set flag field!

          outputRow[rowMeta.size()] = Boolean.FALSE;
        }
      }
    } else {
      if (!meta.isSkipLookup() && isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(PKG, "Update.Log.FoundRow")
                + data.lookupReturnRowMeta.getString(add));
      }

      /*
       * Row was found:
       *
       * UPDATE row or do nothing?
       */
      boolean update = false;

      if (meta.isSkipLookup()) {
        // Update fields directly
        update = true;
      } else {
        for (int i = 0; i < data.valuenrs.length; i++) {
          IValueMeta valueMeta = rowMeta.getValueMeta(data.valuenrs[i]);
          Object rowValue = row[data.valuenrs[i]];
          IValueMeta returnValueMeta = returnRowMeta.getValueMeta(i);
          Object retValue = add[i];

          if (returnValueMeta.compare(retValue, valueMeta, rowValue) != 0) {
            update = true;
          }
        }
      }

      if (update) {
        // Create the update row...
        Object[] updateRow = new Object[data.updateParameterRowMeta.size()];
        for (int i = 0; i < data.valuenrs.length; i++) {
          updateRow[i] = row[data.valuenrs[i]]; // the setters
        }
        // add the where clause parameters, they are exactly the same for lookup and update
        for (int i = 0; i < lookupRow.length; i++) {
          updateRow[data.valuenrs.length + i] = lookupRow[i];
        }

        if (isRowLevel()) {
          logRowlevel(
              BaseMessages.getString(PKG, "Update.Log.UpdateRow")
                  + data.lookupParameterRowMeta.getString(lookupRow));
        }
        data.db.setValues(data.updateParameterRowMeta, updateRow, data.prepStatementUpdate);
        if (data.batchMode) {
          // Only added to the batch: flushBatch() runs it, then passes the row on to the output or
          // to the error hop, depending on what the database says about this row.
          data.db.insertRow(data.prepStatementUpdate, true, false);
          data.batchBuffer.add(new UpdateData.BatchedRow(row, outputRow, updateRow));
          batched = true;
        } else {
          data.db.insertRow(data.prepStatementUpdate, false, !data.useSavepoints);
          incrementLinesUpdated();
        }
      } else {
        incrementLinesSkipped();
      }

      if (!Utils.isEmpty(meta.getIgnoreFlagField())) { // add flag field!

        outputRow[rowMeta.size()] = Boolean.TRUE;
      }
    }

    return batched ? null : outputRow;
  }

  @Override
  public boolean processRow() throws HopException {

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) { // no more input to be expected...
      if (data.batchMode) {
        flushBatchOrStop();
      }
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      // What's the output Row format?
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
      data.schemaTable =
          databaseMeta.getQuotedSchemaTableCombination(
              this, meta.getLookupField().getSchemaName(), meta.getLookupField().getTableName());

      // lookup the values!
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "Update.Log.CheckingRow") + getInputRowMeta().getString(r));
      }

      ArrayList<Integer> keynrs = new ArrayList<>(meta.getLookupField().getLookupKeys().size());
      ArrayList<Integer> keynrs2 = new ArrayList<>(meta.getLookupField().getLookupKeys().size());

      for (int i = 0; i < meta.getLookupField().getLookupKeys().size(); i++) {
        UpdateKeyField keyItem = meta.getLookupField().getLookupKeys().get(i);
        int keynr = getInputRowMeta().indexOfValue(keyItem.getKeyStream());

        if (keynr < 0
            && // couldn't find field!
            !CONST_IS_NULL.equalsIgnoreCase(keyItem.getKeyCondition())
            && // No field needed!
            !CONST_IS_NOT_NULL.equalsIgnoreCase(keyItem.getKeyCondition()) // No field needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, CONST_UPDATE_EXCEPTION_FIELD_REQUIRED, keyItem.getKeyStream()));
        }
        keynrs.add(keynr);

        // this operator needs two bindings
        if (CONST_NULL.equalsIgnoreCase(keyItem.getKeyCondition())) {
          keynrs.add(keynr);
          keynrs2.add(-1);
        }

        int keynr2 = getInputRowMeta().indexOfValue(keyItem.getKeyStream2());
        if (keynr2 < 0
            && // couldn't find field!
            CONST_BETWEEN.equalsIgnoreCase(keyItem.getKeyCondition()) // 2 fields needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, CONST_UPDATE_EXCEPTION_FIELD_REQUIRED, keyItem.getKeyStream2()));
        }
        keynrs2.add(keynr2);

        if (isDebug()) {
          logDebug(
              BaseMessages.getString(PKG, "Update.Log.FieldHasDataNumbers", keyItem.getKeyStream())
                  + ""
                  + keynrs.get(keynrs.size() - 1));
        }
      }

      data.keynrs = ArrayUtils.toPrimitive(keynrs.toArray(new Integer[0]));
      data.keynrs2 = ArrayUtils.toPrimitive(keynrs2.toArray(new Integer[0]));

      // ICache the position of the compare fields in Row row
      //
      data.valuenrs = new int[meta.getLookupField().getUpdateFields().size()];
      for (int i = 0; i < meta.getLookupField().getUpdateFields().size(); i++) {
        UpdateField fieldItem = meta.getLookupField().getUpdateFields().get(i);
        data.valuenrs[i] = getInputRowMeta().indexOfValue(fieldItem.getUpdateStream());
        if (data.valuenrs[i] < 0) { // couldn't find field!

          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, CONST_UPDATE_EXCEPTION_FIELD_REQUIRED, fieldItem.getUpdateStream()));
        }
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                      PKG, "Update.Log.FieldHasDataNumbers", fieldItem.getUpdateStream())
                  + ""
                  + data.valuenrs[i]);
        }
      }
      if (meta.isSkipLookup()) {
        // We skip lookup
        // but we need fields for update
        data.lookupParameterRowMeta = new RowMeta();
        for (int i = 0; i < meta.getLookupField().getLookupKeys().size(); i++) {
          UpdateKeyField keyItem = meta.getLookupField().getLookupKeys().get(i);
          if (CONST_BETWEEN.equalsIgnoreCase(keyItem.getKeyCondition())) {
            data.lookupParameterRowMeta.addValueMeta(
                getInputRowMeta().searchValueMeta(keyItem.getKeyStream()));
            data.lookupParameterRowMeta.addValueMeta(
                getInputRowMeta().searchValueMeta(keyItem.getKeyStream2()));
          } else {
            if (CONST_NULL.equalsIgnoreCase(keyItem.getKeyCondition())) {
              data.lookupParameterRowMeta.addValueMeta(
                  getInputRowMeta().searchValueMeta(keyItem.getKeyStream()));
              data.lookupParameterRowMeta.addValueMeta(
                  getInputRowMeta().searchValueMeta(keyItem.getKeyStream()).clone());
            } else if (!CONST_IS_NULL.equalsIgnoreCase(keyItem.getKeyCondition())
                && !CONST_IS_NOT_NULL.equalsIgnoreCase(keyItem.getKeyCondition())) {
              data.lookupParameterRowMeta.addValueMeta(
                  getInputRowMeta().searchValueMeta(keyItem.getKeyStream()));
            }
          }
        }
      } else {
        setLookup(getInputRowMeta());
      }
      prepareUpdate(getInputRowMeta());
    }

    try {
      data.keyNotFound = false;
      IRowMeta inputRowMeta = getInputRowMeta();
      Object[] outputRow =
          data.useSavepoints
              ? data.db.runInSavepoint(() -> lookupValues(inputRowMeta, r))
              : lookupValues(inputRowMeta, r); // add new values to the row in rowset[0].
      if (data.useSavepoints) {
        commitEveryCommitSize();
      }
      if (data.keyNotFound) {
        putError(inputRowMeta, r, 1L, data.stringErrorKeyNotFound, data.stringFieldnames, "UPD001");
      }
      if (outputRow != null) {
        putRow(data.outputRowMeta, outputRow); // copy non-ignored rows to output rowset(s)
      }
      if (checkFeedback(getLinesRead()) && isBasic()) {
        logBasic(BaseMessages.getString(PKG, "Update.Log.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(BaseMessages.getString(PKG, "Update.Log.ErrorInTransform"), e);
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }

      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), r, 1, errorMessage, null, "UPD001");
      }
    }

    // Outside the try above: a failed batch is about the rows in it, not about this row.
    if (data.batchMode && data.batchBuffer.size() >= data.commitSize && !flushBatchOrStop()) {
      setOutputDone();
      return false;
    }

    return true;
  }

  /**
   * Runs the batch, stopping the transform when that fails without an error hop.
   *
   * @return false when the transform was stopped
   */
  private boolean flushBatchOrStop() {
    try {
      flushBatch();
      return true;
    } catch (HopException e) {
      logError(BaseMessages.getString(PKG, "Update.Log.ErrorInTransform"), e);
      setErrors(1);
      stopAll();
      return false;
    }
  }

  /**
   * Runs and commits the JDBC batch, then passes every row in it on: to the output when the
   * database updated it, to the error hop when it didn't.
   *
   * <p>Before, a row went to the output as soon as it was added to the batch, and a failed batch
   * only surfaced on the row that happened to run it, at the commit size. So the error hop got that
   * row (100, 200, ...) instead of the one that failed (97, 194, ...), which had already gone to
   * the output.
   *
   * @throws HopException when the batch fails without an error hop: the transaction is rolled back
   */
  @VisibleForTesting
  void flushBatch() throws HopException {
    if (data.batchBuffer.isEmpty()) {
      return;
    }
    PreparedStatement statement = data.prepStatementUpdate;
    try {
      statement.executeBatch();
      data.db.commit();
      statement.clearBatch();
    } catch (SQLException e) {
      HopDatabaseBatchException be =
          Database.createHopDatabaseBatchException(
              BaseMessages.getString(PKG, "Update.Exception.BatchFailed", data.schemaTable), e);
      data.db.clearBatch(statement);
      if (!getTransformMeta().isDoingErrorHandling()) {
        data.db.rollback();
        data.batchBuffer.clear();
        throw be;
      }
      // Keep what the database did apply, then sort out the rows.
      data.db.commit(true);
      routeFailedBatch(be.toString(), be.getUpdateCounts(), be.getExceptionsList());
      return;
    }
    for (UpdateData.BatchedRow row : data.batchBuffer) {
      putRow(data.outputRowMeta, row.outputRow());
      incrementLinesUpdated();
    }
    data.batchBuffer.clear();
  }

  /**
   * Passes every row of a failed batch on to exactly one stream, whatever shape the update counts
   * of the driver have.
   *
   * <ul>
   *   <li>One count per row, with {@link Statement#EXECUTE_FAILED} at the rows that failed: SQL
   *       Server, MySQL, H2.
   *   <li>Fewer counts than rows: the driver stopped at the first failure. The row at {@code
   *       counts.length} failed and the rows after it were never sent, so they are sent again as a
   *       new batch, which can fail in its turn: Oracle, Derby. A failure on the first row gives a
   *       zero-length array.
   *   <li>No counts: nothing says which rows were applied, and sending them again could update a
   *       row twice, so they all go to the error hop.
   * </ul>
   *
   * @throws HopException when there are more counts than rows: they can't belong to this batch
   */
  @VisibleForTesting
  void routeFailedBatch(String errorMessage, int[] updateCounts, List<Exception> exceptionsList)
      throws HopException {
    List<UpdateData.BatchedRow> buffer = data.batchBuffer;
    int size = buffer.size();
    if (updateCounts != null && updateCounts.length > size) {
      buffer.clear();
      throw new HopException(
          BaseMessages.getString(
              PKG, "Update.Exception.BatchCountsMismatch", updateCounts.length, size));
    }

    int from = 0;
    int[] counts = updateCounts;
    String message = errorMessage;
    List<Exception> exceptions = exceptionsList == null ? List.of() : exceptionsList;

    while (from < size) {
      int counted = counts == null ? 0 : counts.length;
      int exceptionNr = 0;
      for (int i = 0; i < counted; i++) {
        UpdateData.BatchedRow row = buffer.get(from + i);
        // SUCCESS_NO_INFO and a count of zero (no row matched the key) are not failures.
        if (counts[i] != Statement.EXECUTE_FAILED) {
          putRow(data.outputRowMeta, row.outputRow());
          incrementLinesUpdated();
        } else {
          String rowMessage = message;
          if (exceptionNr < exceptions.size()) {
            rowMessage = exceptions.get(exceptionNr++).toString();
          }
          putError(getInputRowMeta(), row.inputRow(), 1L, rowMessage, null, "UPD002");
        }
      }
      from += counted;
      if (from >= size) {
        break;
      }

      if (counts == null) {
        for (int i = from; i < size; i++) {
          putError(getInputRowMeta(), buffer.get(i).inputRow(), 1L, message, null, "UPD002");
        }
        break;
      }

      // The driver stopped here: this is the row it refused.
      putError(getInputRowMeta(), buffer.get(from).inputRow(), 1L, message, null, "UPD002");
      from++;
      if (from >= size) {
        break;
      }

      // The rows after it were never sent.
      try {
        counts = retryBatch(from, size);
        message = errorMessage;
        exceptions = List.of();
      } catch (HopDatabaseBatchException be) {
        counts = be.getUpdateCounts();
        message = be.toString();
        exceptions = be.getExceptionsList() == null ? List.of() : be.getExceptionsList();
      }
    }
    buffer.clear();
  }

  /**
   * Sends rows {@code [from, to)} of the batch again as one batch, and commits them.
   *
   * @return the update counts, relative to {@code from}
   */
  private int[] retryBatch(int from, int to) throws HopException {
    PreparedStatement statement = data.prepStatementUpdate;
    try {
      for (int i = from; i < to; i++) {
        data.db.setValues(
            data.updateParameterRowMeta, data.batchBuffer.get(i).bindValues(), statement);
        statement.addBatch();
      }
      int[] counts = statement.executeBatch();
      data.db.commit();
      statement.clearBatch();
      return counts;
    } catch (SQLException e) {
      data.db.clearBatch(statement);
      data.db.commit(true);
      throw Database.createHopDatabaseBatchException(
          BaseMessages.getString(PKG, "Update.Exception.BatchFailed", data.schemaTable), e);
    }
  }

  /**
   * The lookup keys drive the WHERE clause of both the lookup SELECT and the UPDATE. Without a
   * single key we would generate a statement ending in a dangling WHERE, so fail with a
   * configuration error instead of letting the database report invalid SQL.
   */
  private void verifyLookupKeys() throws HopTransformException {
    if (meta.getLookupField().getLookupKeys().isEmpty()) {
      throw new HopTransformException(BaseMessages.getString(PKG, "Update.Exception.NoKeyFields"));
    }
  }

  public void setLookup(IRowMeta rowMeta) throws HopException {
    verifyLookupKeys();

    data.lookupParameterRowMeta = new RowMeta();
    data.lookupReturnRowMeta = new RowMeta();

    DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);

    StringBuilder sql = new StringBuilder();

    sql.append("SELECT ");

    for (int i = 0; i < meta.getLookupField().getUpdateFields().size(); i++) {
      UpdateField fieldItem = meta.getLookupField().getUpdateFields().get(i);

      if (i != 0) {
        sql.append(", ");
      }
      sql.append(databaseMeta.quoteField(fieldItem.getUpdateLookup()));
      data.lookupReturnRowMeta.addValueMeta(rowMeta.searchValueMeta(fieldItem.getUpdateStream()));
    }

    sql.append(" FROM " + data.schemaTable + " WHERE ");

    for (int i = 0; i < meta.getLookupField().getLookupKeys().size(); i++) {
      UpdateKeyField keyItem = meta.getLookupField().getLookupKeys().get(i);
      if (i != 0) {
        sql.append(" AND ");
      }

      sql.append(" ( ( ");

      sql.append(databaseMeta.quoteField(keyItem.getKeyLookup()));
      if (CONST_BETWEEN.equalsIgnoreCase(keyItem.getKeyCondition())) {
        sql.append(" BETWEEN ? AND ? ");
        data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream()));
        data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream2()));
      } else {
        if (CONST_IS_NULL.equalsIgnoreCase(keyItem.getKeyCondition())
            || CONST_IS_NOT_NULL.equalsIgnoreCase(keyItem.getKeyCondition())) {
          sql.append(" ").append(keyItem.getKeyCondition()).append(" ");
        } else if (CONST_NULL.equalsIgnoreCase(keyItem.getKeyCondition())) {

          sql.append(" IS NULL AND ");

          if (databaseMeta.requiresCastToVariousForIsNull()) {
            sql.append("CAST(? AS VARCHAR(256)) IS NULL");
          } else {
            sql.append("? IS NULL");
          }
          // null check
          data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream()));
          sql.append(" ) OR ( ")
              .append(databaseMeta.quoteField(keyItem.getKeyLookup()))
              .append(" = ?");
          // equality check, cloning so auto-rename because of adding same fieldname does not cause
          // problems
          data.lookupParameterRowMeta.addValueMeta(
              rowMeta.searchValueMeta(keyItem.getKeyStream()).clone());

        } else {
          sql.append(" ").append(keyItem.getKeyCondition()).append(" ? ");
          data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream()));
        }
      }
      sql.append(" ) ) ");
    }

    try {
      if (isDetailed()) {
        logDetailed("Setting preparedStatement to [" + sql + "]");
      }
      data.prepStatementLookup =
          data.db.getConnection().prepareStatement(databaseMeta.stripCR(sql));
    } catch (SQLException ex) {
      throw new HopDatabaseException(
          "Unable to prepare statement for SQL statement [" + sql + "]", ex);
    }
  }

  // Lookup certain fields in a table
  public void prepareUpdate(IRowMeta rowMeta) throws HopException {
    verifyLookupKeys();

    DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
    data.updateParameterRowMeta = new RowMeta();

    StringBuilder sql = new StringBuilder();

    sql.append("UPDATE ").append(data.schemaTable).append(Const.CR);
    sql.append("SET ");

    for (int i = 0; i < meta.getLookupField().getUpdateFields().size(); i++) {

      UpdateField fieldItem = meta.getLookupField().getUpdateFields().get(i);

      if (i != 0) {
        sql.append(",   ");
      }
      sql.append(databaseMeta.quoteField(fieldItem.getUpdateLookup()));
      sql.append(" = ?").append(Const.CR);
      data.updateParameterRowMeta.addValueMeta(
          rowMeta.searchValueMeta(fieldItem.getUpdateStream()));
    }

    sql.append("WHERE ");

    for (int i = 0; i < meta.getLookupField().getLookupKeys().size(); i++) {

      UpdateKeyField keyItem = meta.getLookupField().getLookupKeys().get(i);

      if (i != 0) {
        sql.append("AND   ");
      }
      sql.append(" ( ( ");
      sql.append(databaseMeta.quoteField(keyItem.getKeyLookup()));
      if (CONST_BETWEEN.equalsIgnoreCase(keyItem.getKeyCondition())) {
        sql.append(" BETWEEN ? AND ? ");
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream()));
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream2()));
      } else if (CONST_IS_NULL.equalsIgnoreCase(keyItem.getKeyCondition())
          || CONST_IS_NOT_NULL.equalsIgnoreCase(keyItem.getKeyCondition())) {
        sql.append(" ").append(keyItem.getKeyCondition()).append(" ");
      } else if (CONST_NULL.equalsIgnoreCase(keyItem.getKeyCondition())) {

        sql.append(" IS NULL AND ");

        if (databaseMeta.requiresCastToVariousForIsNull()) {
          sql.append("CAST(? AS VARCHAR(256)) IS NULL");
        } else {
          sql.append("? IS NULL");
        }
        // null check
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream()));
        sql.append(" ) OR ( ")
            .append(databaseMeta.quoteField(keyItem.getKeyLookup()))
            .append(" = ?");
        // equality check, cloning so auto-rename because of adding same fieldname does not cause
        // problems
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyItem.getKeyStream()).clone());

      } else {
        sql.append(" ").append(keyItem.getKeyCondition()).append(" ? ");
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream()));
      }
      sql.append(" ) ) ");
    }
    try {
      if (isDetailed()) {
        logDetailed("Setting update preparedStatement to [" + sql + "]");
      }
      data.prepStatementUpdate =
          data.db.getConnection().prepareStatement(databaseMeta.stripCR(sql));
    } catch (SQLException ex) {
      throw new HopDatabaseException(
          "Unable to prepare statement for SQL statement [" + sql + "]", ex);
    }
  }

  /**
   * Commits when the commit size is reached, for rows that run behind a savepoint: those don't let
   * {@link Database#insertRow} commit, since a commit inside the savepoint section would end it.
   */
  private void commitEveryCommitSize() throws HopException {
    data.rowsSinceCommit++;
    if (data.rowsSinceCommit >= data.commitSize) {
      data.db.commit();
      data.rowsSinceCommit = 0;
    }
  }

  @Override
  public boolean init() {

    if (super.init()) {

      if (Utils.isEmpty(meta.getConnection())) {
        logError(BaseMessages.getString(PKG, "Update.Init.ConnectionMissing", getTransformName()));
        return false;
      }

      DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
      data.db = new Database(this, this, databaseMeta);

      try {
        data.db.connect();

        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "Update.Log.ConnectedToDB"));
        }

        data.commitSize = meta.getCommitSize(this);
        data.db.setCommit(data.commitSize);
        data.useSavepoints =
            data.db.isUsingRowSavepoints(getTransformMeta().isDoingErrorHandling());
        // A failed row in a batch can't be undone on its own: with savepoints, no batch.
        data.batchMode =
            meta.isUseBatchUpdate()
                && !data.useSavepoints
                && data.commitSize > 0
                && data.db.getUseBatchInsert(true);
        if (meta.isUseBatchUpdate() && data.useSavepoints) {
          logBasic(BaseMessages.getString(PKG, "Update.Log.BatchOffForErrorHandling"));
        }

        return true;
      } catch (HopException ke) {
        logError(BaseMessages.getString(PKG, "Update.Log.ErrorOccurred") + ke.getMessage());
        setErrors(1);
        stopAll();
      }
    }
    return false;
  }

  @Override
  public void batchComplete() throws HopException {
    // This is needed to properly handle transactions when we are using SingleThreadedExecutor
    if (data.batchMode) {
      flushBatch();
    }
    commitBatch(false);
  }

  @Override
  public void dispose() {
    // This is needed to properly handle transactions when we are using SingleThreadedExecutor
    commitBatch(true);
    super.dispose();
  }

  private void commitBatch(boolean dispose) {
    if (data.db != null) {
      try {
        if (!data.db.isAutoCommit()) {
          if (getErrors() == 0) {
            if (dispose) {
              if (data.batchMode && !data.batchBuffer.isEmpty()) {
                // Stopped before the end of the input: run what is left, as before.
                flushBatchOrStop();
              }
              // The batch is empty by now: only commit and close the statement.
              data.db.emptyAndCommit(data.prepStatementUpdate, false);
              data.prepStatementUpdate = null;
            } else {
              data.db.commit();
            }
          } else {
            data.db.rollback();
          }
        }
        if (dispose) {
          // Check if prepStatementLookup is closed correctly
          if (data.prepStatementLookup != null) {
            data.db.closePreparedStatement(data.prepStatementLookup);
          }
          data.prepStatementLookup = null;
        }
      } catch (HopDatabaseException e) {
        logError(
            BaseMessages.getString(PKG, "Update.Log.UnableToCommitUpdateConnection")
                + data.db
                + "] :"
                + e.toString());
        setErrors(1);
      } finally {
        if (dispose) {
          data.db.disconnect();
          data.db = null;
        }
      }
    }
  }
}
