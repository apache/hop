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

package org.apache.hop.pipeline.transforms.insertupdate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateData.PendingRow;

/**
 * Performs a lookup in a database table. If the key doesn't exist it inserts values into the table,
 * otherwise it performs an update of the changed values. If nothing changed, do nothing.
 */
public class InsertUpdate extends BaseTransform<InsertUpdateMeta, InsertUpdateData> {
  private static final Class<?> PKG = InsertUpdateMeta.class;

  /** A row that failed on its own: its lookup, insert or update. */
  private static final String CONST_ISU001 = "ISU001";

  /** A row of a recovered batch that the database refused. */
  private static final String CONST_ISU002 = "ISU002";

  /** A row of a recovered batch that was never sent to the database, which could not be asked. */
  private static final String CONST_ISU003 = "ISU003";

  public static final String CONST_IS_NULL = "IS NULL";
  public static final String CONST_IS_NOT_NULL = "IS NOT NULL";
  public static final String CONST_INSERT_UPDATE_EXCEPTION_FIELD_REQUIRED =
      "InsertUpdate.Exception.FieldRequired";
  public static final String CONST_NULL = "= ~NULL";
  public static final String CONST_BETWEEN = "BETWEEN";

  public InsertUpdate(
      TransformMeta transformMeta,
      InsertUpdateMeta meta,
      InsertUpdateData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  protected synchronized void lookupValues(IRowMeta rowMeta, Object[] row) throws HopException {
    // OK, now do the lookup.
    // We need the lookupvalues for that.
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

    data.db.setValues(data.lookupParameterRowMeta, lookupRow, data.prepStatementLookup);

    if (isDebug()) {
      logDebug(
          BaseMessages.getString(PKG, "InsertUpdate.Log.ValuesSetForLookup")
              + data.lookupParameterRowMeta.getString(lookupRow));
    }
    Object[] add = data.db.getLookup(data.prepStatementLookup);
    incrementLinesInput();

    if (add == null) {
      /*
       * nothing was found:
       *
       * INSERT ROW
       */
      if (isRowLevel()) {
        logRowlevel(BaseMessages.getString(PKG, "InsertUpdate.InsertRow") + rowMeta.getString(row));
      }

      // The values to insert are those in the update section (all fields should be specified)
      // For the others, we have no definite mapping!
      //
      Object[] insertRow = new Object[data.valuenrs.length];
      for (int i = 0; i < data.valuenrs.length; i++) {
        insertRow[i] = row[data.valuenrs[i]];
      }

      if (data.recoveredBatches) {
        // Bound and run by the recovered batch: flushRecoveredBatches() passes the row on once
        // the database has committed or refused its insert.
        addToRecoveredBatch(PendingRow.Kind.INSERT, row, insertRow);
      } else {
        // Set the values on the prepared statement...
        data.db.setValuesInsert(data.insertRowMeta, insertRow);

        // Insert the row
        data.db.insertRow(data.db.getPrepStatementInsert(), false, !data.useSavepoints);

        incrementLinesOutput();
      }
    } else {
      if (!meta.isUpdateBypassed()) {
        if (isRowLevel()) {
          logRowlevel(
              BaseMessages.getString(PKG, "InsertUpdate.Log.FoundRowForUpdate")
                  + rowMeta.getString(row));
        }

        /*
         * Row was found:
         *
         * UPDATE row or do nothing?
         */
        boolean update = false;
        for (int i = 0; i < data.valuenrs.length; i++) {
          InsertUpdateValue valueField = meta.getInsertUpdateLookupField().getValueFields().get(i);
          if (valueField.isUpdate()) {
            IValueMeta valueMeta = rowMeta.getValueMeta(data.valuenrs[i]);
            IValueMeta retMeta = data.db.getReturnRowMeta().getValueMeta(i);

            Object rowvalue = row[data.valuenrs[i]];
            Object retvalue = add[i];

            if (retMeta.compare(retvalue, valueMeta, rowvalue) != 0) {
              update = true;
            }
          }
        }
        if (update) {
          // Create the update row...
          Object[] updateRow = new Object[data.updateParameterRowMeta.size()];
          int j = 0;
          for (int i = 0; i < data.valuenrs.length; i++) {
            InsertUpdateValue valueField =
                meta.getInsertUpdateLookupField().getValueFields().get(i);
            if (valueField.isUpdate()) {
              updateRow[j] = row[data.valuenrs[i]]; // the setters
              j++;
            }
          }
          // add the where clause parameters, they are exactly the same for lookup and update
          for (int i = 0; i < lookupRow.length; i++) {
            updateRow[j + i] = lookupRow[i];
          }

          if (isRowLevel()) {
            logRowlevel(
                BaseMessages.getString(PKG, "InsertUpdate.Log.UpdateRow")
                    + data.lookupParameterRowMeta.getString(lookupRow));
          }
          if (data.recoveredBatches) {
            // Passed on by flushRecoveredBatches() once the database has committed or refused it
            addToRecoveredBatch(PendingRow.Kind.UPDATE, row, updateRow);
          } else {
            data.db.setValues(data.updateParameterRowMeta, updateRow, data.prepStatementUpdate);
            data.db.insertRow(data.prepStatementUpdate, false, !data.useSavepoints);
            incrementLinesUpdated();
          }
        } else {
          incrementLinesSkipped();
        }
      } else {
        if (isRowLevel()) {
          logRowlevel(
              BaseMessages.getString(PKG, "InsertUpdate.Log.UpdateBypassed")
                  + rowMeta.getString(row));
        }
        incrementLinesSkipped();
      }
    }
  }

  @Override
  public boolean processRow() throws HopException {

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) {
      // no more input to be expected...
      if (data.recoveredBatches) {
        // The rows of the last batches are passed on once committed, so before the output is done
        flushRecoveredBatchesOrStop();
      }

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.schemaTable =
          databaseMeta.getQuotedSchemaTableCombination(
              this, meta.getSchemaName(), meta.getTableName());

      // lookup the values!
      if (isDebug()) {
        logDebug(
            BaseMessages.getString(PKG, "InsertUpdate.Log.CheckingRow")
                + getInputRowMeta().getString(r));
      }

      ArrayList<Integer> keynrs =
          new ArrayList<>(meta.getInsertUpdateLookupField().getLookupKeys().size());
      ArrayList<Integer> keynrs2 =
          new ArrayList<>(meta.getInsertUpdateLookupField().getLookupKeys().size());

      for (int i = 0; i < meta.getInsertUpdateLookupField().getLookupKeys().size(); i++) {
        InsertUpdateKeyField keyField = meta.getInsertUpdateLookupField().getLookupKeys().get(i);
        int keynr = getInputRowMeta().indexOfValue(keyField.getKeyStream());

        if (keynr < 0
            && // couldn't find field!
            !CONST_IS_NULL.equalsIgnoreCase(keyField.getKeyCondition())
            && // No field needed!
            !CONST_IS_NOT_NULL.equalsIgnoreCase(keyField.getKeyCondition()) // No field needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, CONST_INSERT_UPDATE_EXCEPTION_FIELD_REQUIRED, keyField.getKeyStream()));
        }
        keynrs.add(keynr);

        // this operator needs two bindings
        if (CONST_NULL.equalsIgnoreCase(keyField.getKeyCondition())) {
          keynrs.add(keynr);
          keynrs2.add(-1);
        }

        int keynr2 = getInputRowMeta().indexOfValue(keyField.getKeyStream2());
        if (keynr2 < 0
            && // couldn't find field!
            CONST_BETWEEN.equalsIgnoreCase(keyField.getKeyCondition()) // 2 fields needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, CONST_INSERT_UPDATE_EXCEPTION_FIELD_REQUIRED, keyField.getKeyStream2()));
        }
        keynrs2.add(keynr2);

        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                      PKG, "InsertUpdate.Log.FieldHasDataNumbers", keyField.getKeyStream())
                  + ""
                  + keynrs.get(keynrs.size() - 1));
        }
      }

      data.keynrs = ArrayUtils.toPrimitive(keynrs.toArray(new Integer[0]));
      data.keynrs2 = ArrayUtils.toPrimitive(keynrs2.toArray(new Integer[0]));

      // ICache the position of the compare fields in Row row
      //
      data.valuenrs = new int[meta.getInsertUpdateLookupField().getValueFields().size()];
      for (int i = 0; i < meta.getInsertUpdateLookupField().getValueFields().size(); i++) {
        InsertUpdateValue valueField = meta.getInsertUpdateLookupField().getValueFields().get(i);
        data.valuenrs[i] = getInputRowMeta().indexOfValue(valueField.getUpdateStream());
        if (data.valuenrs[i] < 0) {
          // couldn't find field!

          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, CONST_INSERT_UPDATE_EXCEPTION_FIELD_REQUIRED, valueField.getUpdateStream()));
        }
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                      PKG, "InsertUpdate.Log.FieldHasDataNumbers", valueField.getUpdateStream())
                  + data.valuenrs[i]);
        }
      }

      setLookup(getInputRowMeta());

      data.insertRowMeta = new RowMeta();

      // Insert the update fields: just names. Type doesn't matter!
      for (int i = 0; i < meta.getInsertUpdateLookupField().getValueFields().size(); i++) {
        InsertUpdateValue valueField = meta.getInsertUpdateLookupField().getValueFields().get(i);
        IValueMeta insValue = data.insertRowMeta.searchValueMeta(valueField.getUpdateLookup());
        if (insValue == null) {
          // Don't add twice!

          // we already checked that this value exists so it's probably safe to ignore lookup
          // failure...
          IValueMeta insertValue =
              getInputRowMeta().searchValueMeta(valueField.getUpdateStream()).clone();
          insertValue.setName(valueField.getUpdateLookup());
          data.insertRowMeta.addValueMeta(insertValue);
        } else {
          throw new HopTransformException(
              "The same column can't be inserted into the target row twice: " + insValue.getName());
        }
      }
      data.db.prepareInsert(
          data.insertRowMeta, resolve(meta.getSchemaName()), resolve(meta.getTableName()));

      if (!meta.isUpdateBypassed()) {
        List<String> updateColumns = new ArrayList<>();
        for (int i = 0; i < meta.getInsertUpdateLookupField().getValueFields().size(); i++) {
          InsertUpdateValue valueField = meta.getInsertUpdateLookupField().getValueFields().get(i);
          if (valueField.isUpdate()) {
            updateColumns.add(valueField.getUpdateLookup());
          }
        }
        prepareUpdate(getInputRowMeta());
      }
      if (data.recoveredBatches) {
        createRecoveredBatches();
      }
    }

    if (data.recoveredBatches) {
      return processRowInRecoveredBatches(r);
    }

    try {
      IRowMeta inputRowMeta = getInputRowMeta();
      if (data.useSavepoints) {
        data.db.runInSavepoint(
            () -> {
              lookupValues(inputRowMeta, r);
              return null;
            });
        commitEveryCommitSize();
      } else {
        lookupValues(inputRowMeta, r); // add new values to the row in rowset[0].
      }
      putRow(
          data.outputRowMeta,
          r); // Nothing changed to the input, return the same row, pass a "cloned" metadata
      // row.

      if (checkFeedback(getLinesRead()) && isBasic()) {
        logBasic(BaseMessages.getString(PKG, "InsertUpdate.Log.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(BaseMessages.getString(PKG, "InsertUpdate.Log.ErrorInTransform"), e);
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }

      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), r, 1, errorMessage, null, "ISU001");
      }
    }

    return true;
  }

  /**
   * Looks one row up and adds its insert or update to a recovered batch. The row is passed on once
   * the batch it is in is committed, after the rows that came in before it; a row that needs no
   * write, or fails on its own, waits for those rows as well.
   *
   * <p>The lookup can't see the rows that wait in a batch, so when the key of this row is one of
   * theirs, the batches are written first.
   *
   * @return false when the transform stopped
   */
  private boolean processRowInRecoveredBatches(Object[] r) throws HopException {
    IRowMeta inputRowMeta = getInputRowMeta();
    try {
      data.rowKey = PendingKeys.key(inputRowMeta, r, data.keyFieldIndexes);
      if (isKeyPending(data.rowKey)) {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG, "InsertUpdate.Log.KeyWaitsInBatch", inputRowMeta.getString(r)));
        }
        if (!flushRecoveredBatchesOrStop()) {
          setOutputDone();
          return false;
        }
      }
      data.rowInBatch = false;
      lookupValues(inputRowMeta, r);
      if (!data.rowInBatch) {
        // Nothing to write: passed on in its place between the rows of the batches
        putRowInOrder(r);
      }

      if (checkFeedback(getLinesRead()) && isBasic()) {
        logBasic(BaseMessages.getString(PKG, "InsertUpdate.Log.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {
      if (!getTransformMeta().isDoingErrorHandling()) {
        logError(BaseMessages.getString(PKG, "InsertUpdate.Log.ErrorInTransform"), e);
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      // Between two flushes the transaction holds no inserts or updates, only lookups: the batches
      // run when they are flushed. So rolling it back loses nothing, and where a failed statement
      // aborts the transaction (PostgreSQL, DuckDB) the lookups of the next rows can run again.
      data.db.rollback();
      prepareLookupAgainIfClosed();
      putErrorInOrder(r, e.toString(), null, CONST_ISU001);
    }

    // The rows waiting behind the batches count as well, so that memory stays at one commit size
    // when few rows need a write: the batches then run before they are full.
    if (data.pendingRows.size() >= data.commitSize && !flushRecoveredBatchesOrStop()) {
      setOutputDone();
      return false;
    }
    return true;
  }

  /**
   * @return true if a row with this key can't be looked up before the batches are written: a row in
   *     them has this key, before or after its write, or a key the transform can't tell
   */
  private boolean isKeyPending(List<Object> key) {
    return !data.pendingRows.isEmpty()
        && (data.pendingKeyUnknown || data.pendingKeys.contains(key));
  }

  /**
   * Finds where the key columns get their values, for the keys of the rows in the batches: the
   * fields the key conditions compare (those of =, = ~NULL; IS NULL and IS NOT NULL compare no
   * value), and the fields the INSERT and the UPDATE write into those columns.
   */
  private void prepareRowKeys() {
    List<InsertUpdateKeyField> keyFields = meta.getInsertUpdateLookupField().getLookupKeys();
    List<InsertUpdateValue> valueFields = meta.getInsertUpdateLookupField().getValueFields();
    List<Integer> keyIndexes = new ArrayList<>();
    List<Integer> insertIndexes = new ArrayList<>();
    List<Integer> updateIndexes = new ArrayList<>();
    boolean insertKnown = true;
    for (int i = 0; i < keyFields.size(); i++) {
      InsertUpdateKeyField keyField = keyFields.get(i);
      String condition = Const.NVL(keyField.getKeyCondition(), "").trim();
      if (CONST_IS_NULL.equalsIgnoreCase(condition)
          || CONST_IS_NOT_NULL.equalsIgnoreCase(condition)) {
        continue;
      }
      int keyIndex = data.keynrs[keyNumberOf(i)];
      keyIndexes.add(keyIndex);

      int inserted = valueFieldWriting(valueFields, keyField.getKeyLookup(), false);
      insertKnown &= inserted >= 0;
      insertIndexes.add(inserted < 0 ? keyIndex : data.valuenrs[inserted]);

      int updated =
          meta.isUpdateBypassed()
              ? -1
              : valueFieldWriting(valueFields, keyField.getKeyLookup(), true);
      updateIndexes.add(updated < 0 ? keyIndex : data.valuenrs[updated]);
    }
    data.keyFieldIndexes = ArrayUtils.toPrimitive(keyIndexes.toArray(new Integer[0]));
    data.insertKeyIndexes =
        insertKnown ? ArrayUtils.toPrimitive(insertIndexes.toArray(new Integer[0])) : null;
    data.updateKeyIndexes = ArrayUtils.toPrimitive(updateIndexes.toArray(new Integer[0]));
  }

  /**
   * @param keyNumber the position of a key condition
   * @return its position in {@link InsertUpdateData#keynrs}, where a = ~NULL condition takes two
   */
  private int keyNumberOf(int keyNumber) {
    int position = 0;
    List<InsertUpdateKeyField> keyFields = meta.getInsertUpdateLookupField().getLookupKeys();
    for (int i = 0; i < keyNumber; i++) {
      position += CONST_NULL.equalsIgnoreCase(keyFields.get(i).getKeyCondition()) ? 2 : 1;
    }
    return position;
  }

  /**
   * @param column a column of the table
   * @param updatedOnly only the fields the UPDATE sets
   * @return the position of the value field that writes the column, the same name first and then
   *     one that only differs in case; -1 when no value field writes it
   */
  private static int valueFieldWriting(
      List<InsertUpdateValue> valueFields, String column, boolean updatedOnly) {
    int ignoringCase = -1;
    for (int i = 0; i < valueFields.size(); i++) {
      InsertUpdateValue valueField = valueFields.get(i);
      if (updatedOnly && !valueField.isUpdate()) {
        continue;
      }
      String target = valueField.getUpdateLookup();
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
   * A row waiting in a batch can only be told apart from an incoming row by its key when every key
   * condition is an equality, or a condition without a value. With a range or a pattern any waiting
   * row could match any lookup, so those transforms write row by row.
   */
  private boolean keysAreEqualities() {
    for (InsertUpdateKeyField keyField : meta.getInsertUpdateLookupField().getLookupKeys()) {
      String condition = Const.NVL(keyField.getKeyCondition(), "").trim();
      if (!"=".equals(condition)
          && !CONST_NULL.equalsIgnoreCase(condition)
          && !CONST_IS_NULL.equalsIgnoreCase(condition)
          && !CONST_IS_NOT_NULL.equalsIgnoreCase(condition)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Creates the recovered batches on the INSERT and the UPDATE statement. Some drivers (DuckDB)
   * close a statement when its batch fails; a batch then prepares it again.
   */
  private void createRecoveredBatches() throws HopException {
    prepareRowKeys();
    data.insertSql =
        data.db.getInsertStatement(
            resolve(meta.getSchemaName()), resolve(meta.getTableName()), data.insertRowMeta);
    data.insertBatch =
        RecoverableBatch.of(
                data.db,
                data.db.getPrepStatementInsert(),
                (statement, bindRow) -> data.db.setValues(data.insertRowMeta, bindRow, statement),
                data.batchSupport)
            .withStatementFactory(() -> data.db.prepareSql(data.insertSql));
    if (data.prepStatementUpdate != null) {
      String sql = data.updateSql;
      data.updateBatch =
          RecoverableBatch.of(
                  data.db,
                  data.prepStatementUpdate,
                  (statement, bindRow) ->
                      data.db.setValues(data.updateParameterRowMeta, bindRow, statement),
                  data.batchSupport)
              .withStatementFactory(
                  () -> {
                    PreparedStatement again = data.db.getConnection().prepareStatement(sql);
                    data.prepStatementUpdate = again;
                    return again;
                  });
    }
  }

  /**
   * Adds the insert or update of a row to its recovered batch. The row waits in {@link
   * InsertUpdateData#pendingRows} until the batch has run, and its key in {@link
   * InsertUpdateData#pendingKeys}: the key it is looked up by, and the key it has after its write.
   */
  private void addToRecoveredBatch(PendingRow.Kind kind, Object[] row, Object[] bindRow)
      throws HopException {
    boolean insert = kind == PendingRow.Kind.INSERT;
    RecoverableBatch batch = insert ? data.insertBatch : data.updateBatch;
    batch.add(bindRow, row);
    PendingRow pending = PendingRow.batched(kind, row);
    (insert ? data.insertBuffer : data.updateBuffer).add(pending);
    data.pendingRows.add(pending);
    data.rowInBatch = true;

    data.pendingKeys.add(data.rowKey);
    if (insert && data.insertKeyIndexes == null) {
      data.pendingKeyUnknown = true;
    } else {
      int[] written = insert ? data.insertKeyIndexes : data.updateKeyIndexes;
      data.pendingKeys.add(PendingKeys.key(getInputRowMeta(), row, written));
    }
  }

  /**
   * Passes a row on to the output, after the rows of the pending batches that came in before it.
   */
  private void putRowInOrder(Object[] row) throws HopTransformException {
    if (data.pendingRows.isEmpty()) {
      putRow(data.outputRowMeta, row);
    } else {
      data.pendingRows.add(PendingRow.output(row));
    }
  }

  /**
   * Passes a row on to the error hop, after the rows of the pending batches that came in before it.
   */
  private void putErrorInOrder(
      Object[] row, String errorMessage, String errorFields, String errorCode)
      throws HopTransformException {
    if (data.pendingRows.isEmpty()) {
      putError(getInputRowMeta(), row, 1L, errorMessage, errorFields, errorCode);
    } else {
      data.pendingRows.add(PendingRow.error(row, errorMessage, errorFields, errorCode));
    }
  }

  /**
   * Passes every pending row on in the order the rows came in: a failed row to the error hop, the
   * others to the output, counting the inserts and updates the database committed.
   */
  private void releasePendingRows() throws HopTransformException {
    for (PendingRow row : data.pendingRows) {
      release(row);
    }
    clearPendingRows();
  }

  /**
   * After a flush that failed: passes on, in the order the rows came in, the rows of the batches
   * the database already gave a verdict, such as the inserts it committed before the updates
   * failed. Every other pending row is dropped, as a failed batch drops its rows.
   */
  private void releaseSettledRows() throws HopTransformException {
    for (PendingRow row : data.pendingRows) {
      if (row.isBatched() && (row.isWritten() || row.isFailed())) {
        release(row);
      }
    }
    clearPendingRows();
  }

  private void release(PendingRow row) throws HopTransformException {
    if (row.isFailed()) {
      putError(
          getInputRowMeta(),
          row.getRow(),
          1L,
          row.getErrorMessage(),
          row.getErrorFields(),
          row.getErrorCode());
      return;
    }
    putRow(data.outputRowMeta, row.getRow());
    if (row.getKind() == PendingRow.Kind.INSERT) {
      incrementLinesOutput();
    } else if (row.getKind() == PendingRow.Kind.UPDATE) {
      incrementLinesUpdated();
    }
  }

  private void clearPendingRows() {
    data.pendingRows.clear();
    data.insertBuffer.clear();
    data.updateBuffer.clear();
    data.pendingKeys.clear();
    data.pendingKeyUnknown = false;
  }

  /**
   * Runs the recovered batches, stopping the transform when that fails.
   *
   * @return false when the transform was stopped
   */
  private boolean flushRecoveredBatchesOrStop() {
    try {
      flushRecoveredBatches();
      return true;
    } catch (HopException e) {
      logError(BaseMessages.getString(PKG, "InsertUpdate.Log.ErrorInTransform"), e);
      setErrors(1);
      stopAll();
      return false;
    }
  }

  /**
   * Runs and commits the pending inserts, then the pending updates, then passes every pending row
   * on in the order the rows came in.
   *
   * <p>Each batch is run, committed and, when it fails, recovered before the other one sends a
   * single row to the database: until then the rows of a batch are only in its JDBC batch, on the
   * client. So the rollbacks of one batch's recovery never reach the rows of the other, and the
   * transaction holds nothing else when a batch starts. The inserts go first, so that an update can
   * refer to a row inserted in the same window. Two rows of the window never have the same key: a
   * row whose key is already waiting writes the batches before it is looked up.
   *
   * <p>With an error hop the connection's batch failure contract tells which rows of a failed batch
   * the database refused: they go to the error hop, the others are committed and go to the output,
   * each exactly once. Without an error hop a failed batch is rolled back, none of its rows is
   * passed on, and the failure is thrown; when that is the update batch, the inserts committed
   * before it are passed on.
   */
  private void flushRecoveredBatches() throws HopException {
    if (data.pendingRows.isEmpty()) {
      return;
    }
    try {
      flushRecoveredBatch(
          data.insertBatch, data.insertBuffer, "InsertUpdate.Exception.InsertBatchFailed");
      flushRecoveredBatch(
          data.updateBatch, data.updateBuffer, "InsertUpdate.Exception.UpdateBatchFailed");
    } catch (HopException e) {
      discardRecoveredBatches();
      releaseSettledRows();
      throw e;
    }
    releasePendingRows();
  }

  private void flushRecoveredBatch(
      RecoverableBatch batch, List<PendingRow> buffer, String failureMessage) throws HopException {
    if (batch == null || batch.isEmpty()) {
      return;
    }
    try {
      if (getTransformMeta().isDoingErrorHandling()) {
        batch.flush(recoveredBatchListener(buffer));
      } else {
        batch.flushOrFail(recoveredBatchListener(buffer));
      }
    } catch (HopDatabaseBatchException e) {
      throw new HopDatabaseBatchException(
          BaseMessages.getString(PKG, failureMessage, data.schemaTable), e);
    } finally {
      if (data.updateBatch != null) {
        data.prepStatementUpdate = data.updateBatch.getStatement();
      }
    }
  }

  /**
   * Settles the rows of a batch's buffer: the batch gives every row its verdict once, in the order
   * the rows were added. {@link #releasePendingRows()} passes them on afterwards.
   */
  private static IBatchRowListener recoveredBatchListener(List<PendingRow> buffer) {
    return new IBatchRowListener() {
      private int settled;

      @Override
      public void rowWritten(Object[] outputRow) {
        buffer.get(settled++).written();
      }

      @Override
      public void rowRejected(Object[] outputRow, String message, boolean attempted) {
        buffer.get(settled++).fail(message, null, attempted ? CONST_ISU002 : CONST_ISU003);
      }
    };
  }

  /** Drops the rows still in the JDBC batches, without running them. */
  private int discardRecoveredBatches() {
    int dropped = 0;
    if (data.insertBatch != null) {
      dropped += data.insertBatch.discard().size();
    }
    if (data.updateBatch != null) {
      dropped += data.updateBatch.discard().size();
    }
    return dropped;
  }

  /**
   * At the end of a single-threaded iteration, and when the transform is disposed: runs what waits
   * in the recovered batches, or drops it when the transform failed or was stopped, so that no row
   * is committed that is not passed on.
   */
  private void flushOrDiscardRecoveredBatches() throws HopException {
    if (data.insertBatch == null) {
      return;
    }
    if (getErrors() > 0 || isStopped()) {
      int dropped = discardRecoveredBatches();
      clearPendingRows();
      if (dropped > 0) {
        logError(
            BaseMessages.getString(
                PKG, "InsertUpdate.Log.BatchDiscarded", String.valueOf(dropped)));
      }
      return;
    }
    flushRecoveredBatches();
  }

  /**
   * Some drivers (DuckDB) close a prepared statement when it fails. After a row failed, the lookup
   * statement is prepared again if the driver closed it, so that the next rows can still be looked
   * up instead of all failing on the closed statement. A driver that keeps the statement open keeps
   * it as it is.
   */
  private void prepareLookupAgainIfClosed() throws HopDatabaseException {
    if (data.prepStatementLookup == null || !isClosed(data.prepStatementLookup)) {
      return;
    }
    try {
      data.prepStatementLookup = data.db.getConnection().prepareStatement(data.lookupSql);
    } catch (SQLException e) {
      throw new HopDatabaseException(
          "Unable to prepare statement for SQL statement [" + data.lookupSql + "]", e);
    }
  }

  /**
   * @return true if the driver says it closed the statement; a driver that can't say is taken to
   *     keep it open, as before
   */
  private static boolean isClosed(Statement statement) {
    try {
      return statement.isClosed();
    } catch (SQLException e) {
      return false;
    }
  }

  public void setLookup(IRowMeta rowMeta) throws HopDatabaseException {
    data.lookupParameterRowMeta = new RowMeta();
    data.lookupReturnRowMeta = new RowMeta();

    DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);

    StringBuilder sql = new StringBuilder();

    sql.append("SELECT ");

    for (int i = 0; i < meta.getInsertUpdateLookupField().getValueFields().size(); i++) {
      InsertUpdateValue valueField = meta.getInsertUpdateLookupField().getValueFields().get(i);
      if (i != 0) {
        sql.append(", ");
      }
      sql.append(databaseMeta.quoteField(valueField.getUpdateLookup()));
      data.lookupReturnRowMeta.addValueMeta(
          rowMeta.searchValueMeta(valueField.getUpdateStream()).clone());
    }

    sql.append(" FROM ").append(data.schemaTable).append(" WHERE ");

    for (int i = 0; i < meta.getInsertUpdateLookupField().getLookupKeys().size(); i++) {
      InsertUpdateKeyField keyField = meta.getInsertUpdateLookupField().getLookupKeys().get(i);
      if (i != 0) {
        sql.append(" AND ");
      }

      sql.append(" ( ( ");

      sql.append(databaseMeta.quoteField(keyField.getKeyLookup()));
      if (CONST_BETWEEN.equalsIgnoreCase(keyField.getKeyCondition())) {
        sql.append(" BETWEEN ? AND ? ");
        data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyField.getKeyStream()));
        data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyField.getKeyStream2()));
      } else {
        if (CONST_IS_NULL.equalsIgnoreCase(keyField.getKeyCondition())
            || CONST_IS_NOT_NULL.equalsIgnoreCase(keyField.getKeyCondition())) {
          sql.append(" ").append(keyField.getKeyCondition()).append(" ");
        } else if (CONST_NULL.equalsIgnoreCase(keyField.getKeyCondition())) {

          sql.append(" IS NULL AND ");

          if (databaseMeta.requiresCastToVariousForIsNull()) {
            sql.append(" CAST(? AS VARCHAR(256)) IS NULL ");
          } else {
            sql.append(" ? IS NULL ");
          }
          // null check
          data.lookupParameterRowMeta.addValueMeta(
              rowMeta.searchValueMeta(keyField.getKeyStream()));
          sql.append(" ) OR ( ")
              .append(databaseMeta.quoteField(keyField.getKeyLookup()))
              .append(" = ? ");
          // equality check, cloning so auto-rename because of adding same fieldname does not cause
          // problems
          data.lookupParameterRowMeta.addValueMeta(
              rowMeta.searchValueMeta(keyField.getKeyStream()).clone());

        } else {
          sql.append(" ").append(keyField.getKeyCondition()).append(" ? ");
          data.lookupParameterRowMeta.addValueMeta(
              rowMeta.searchValueMeta(keyField.getKeyStream()));
        }
      }
      sql.append(" ) ) ");
    }

    try {
      if (isDetailed()) {
        logDetailed("Setting preparedStatement to [" + sql + "]");
      }
      data.lookupSql = databaseMeta.stripCR(sql);
      data.prepStatementLookup = data.db.getConnection().prepareStatement(data.lookupSql);
    } catch (SQLException ex) {
      throw new HopDatabaseException(
          "Unable to prepare statement for SQL statement [" + sql + "]", ex);
    }
  }

  // Lookup certain fields in a table
  public void prepareUpdate(IRowMeta rowMeta) throws HopDatabaseException {

    DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
    data.updateParameterRowMeta = new RowMeta();

    StringBuilder sql = new StringBuilder();

    sql.append("UPDATE ").append(data.schemaTable).append(Const.CR);
    sql.append("SET ");

    boolean comma = false;

    for (int i = 0; i < meta.getInsertUpdateLookupField().getValueFields().size(); i++) {
      InsertUpdateValue valueField = meta.getInsertUpdateLookupField().getValueFields().get(i);
      if (valueField.isUpdate()) {
        if (comma) {
          sql.append(",   ");
        } else {
          comma = true;
        }

        sql.append(databaseMeta.quoteField(valueField.getUpdateLookup()));
        sql.append(" = ?").append(Const.CR);
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(valueField.getUpdateStream()).clone());
      }
    }

    sql.append("WHERE ");

    for (int i = 0; i < meta.getInsertUpdateLookupField().getLookupKeys().size(); i++) {
      InsertUpdateKeyField keyField = meta.getInsertUpdateLookupField().getLookupKeys().get(i);
      if (i != 0) {
        sql.append("AND   ");
      }
      sql.append(" ( ( ");
      sql.append(databaseMeta.quoteField(keyField.getKeyLookup()));
      if (CONST_BETWEEN.equalsIgnoreCase(keyField.getKeyCondition())) {
        sql.append(" BETWEEN ? AND ? ");
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyField.getKeyStream()));
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyField.getKeyStream2()));
      } else if (CONST_IS_NULL.equalsIgnoreCase(keyField.getKeyCondition())
          || CONST_IS_NOT_NULL.equalsIgnoreCase(keyField.getKeyCondition())) {
        sql.append(" ").append(keyField.getKeyCondition()).append(" ");
      } else if (CONST_NULL.equalsIgnoreCase(keyField.getKeyCondition())) {

        sql.append(" IS NULL AND ");

        if (databaseMeta.requiresCastToVariousForIsNull()) {
          sql.append("CAST(? AS VARCHAR(256)) IS NULL");
        } else {
          sql.append("? IS NULL");
        }
        // null check
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyField.getKeyStream()));
        sql.append(" ) OR ( ")
            .append(databaseMeta.quoteField(keyField.getKeyLookup()))
            .append(" = ?");
        // equality check, cloning so auto-rename because of adding same fieldname does not cause
        // problems
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyField.getKeyStream()).clone());

      } else {
        sql.append(" ").append(keyField.getKeyCondition()).append(" ? ");
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyField.getKeyStream()).clone());
      }
      sql.append(" ) ) ");
    }

    try {
      if (isDetailed()) {
        logDetailed("Setting update preparedStatement to [" + sql + "]");
      }
      data.updateSql = databaseMeta.stripCR(sql);
      data.prepStatementUpdate = data.db.getConnection().prepareStatement(data.updateSql);
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
      DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);

      try {
        if (databaseMeta == null) {
          logError(
              BaseMessages.getString(
                  PKG, "InsertUpdate.Init.ConnectionMissing", getTransformName()));
          return false;
        }
        data.db = new Database(this, this, databaseMeta);
        data.db.connect();
        data.commitSize = meta.getCommitSizeVar(this);
        data.db.setCommit(data.commitSize);

        // How a failed batch is handled on this connection. Only a contract validated against the
        // database (or chosen on the connection) writes the rows in batches, recovered with
        // RecoverableBatch; every other connection keeps the handling below, exactly as it was.
        data.batchSupport = BatchSupport.of(databaseMeta.getIDatabase(), this);
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "InsertUpdate.Log.BatchSupport", data.batchSupport.describe()));
        }
        String rowByRow = meta.isUseBatchUpdate() ? whyRowByRow() : null;
        data.recoveredBatches = meta.isUseBatchUpdate() && rowByRow == null;

        if (data.recoveredBatches) {
          // Batching stays on with an error hop, without a savepoint per row: the contract tells
          // which rows of a failed batch were refused.
          data.useSavepoints = false;
        } else {
          if (rowByRow != null) {
            logBasic(BaseMessages.getString(PKG, "InsertUpdate.Log.BatchesNotUsed", rowByRow));
          }
          data.useSavepoints =
              data.db.isUsingRowSavepoints(getTransformMeta().isDoingErrorHandling());
        }

        return true;
      } catch (HopException ke) {
        logError(
            BaseMessages.getString(PKG, "InsertUpdate.Log.ErrorOccurredDuringTransformInitialize")
                + ke.getMessage());
      }
    }
    return false;
  }

  /**
   * Why "Use batch updates" can't write in batches on this transform, see {@link
   * InsertUpdateData#recoveredBatches}.
   *
   * @return the reason, or null when the rows go out in recovered batches
   */
  private String whyRowByRow() throws HopDatabaseException {
    if (!keysAreEqualities()) {
      return BaseMessages.getString(PKG, "InsertUpdate.Log.BatchesNotUsed.KeyConditions");
    }
    if (data.commitSize <= 0) {
      return BaseMessages.getString(PKG, "InsertUpdate.Log.BatchesNotUsed.CommitSize");
    }
    if (!data.batchSupport.isRecovered()) {
      return BaseMessages.getString(
          PKG, "InsertUpdate.Log.BatchesNotUsed.Contract", data.batchSupport.describe());
    }
    if (data.db.isAutoCommit()) {
      return BaseMessages.getString(PKG, "InsertUpdate.Log.BatchesNotUsed.AutoCommit");
    }
    if (!data.db.getUseBatchInsert(true)) {
      return BaseMessages.getString(PKG, "InsertUpdate.Log.BatchesNotUsed.NoBatches");
    }
    return null;
  }

  @Override
  public void batchComplete() throws HopException {
    // This is needed to properly handle transactions when we are using SingleThreadedExecutor
    if (data.recoveredBatches) {
      flushOrDiscardRecoveredBatches();
    }
    commitBatch(false);
  }

  @Override
  public void dispose() {
    if (data.recoveredBatches && data.db != null) {
      try {
        flushOrDiscardRecoveredBatches();
      } catch (HopException e) {
        logError(BaseMessages.getString(PKG, "InsertUpdate.Log.ErrorInTransform"), e);
        setErrors(1);
      }
    }
    commitBatch(true);
    super.dispose();
  }

  private void commitBatch(boolean dispose) {
    if (data.db != null) {
      try {
        if (!data.db.isAutoCommit()) {
          if (getErrors() == 0) {
            data.db.commit();
          } else {
            data.db.rollback();
          }
        }
        if (dispose) {
          if (data.insertBatch != null
              && data.insertBatch.getStatement() != data.db.getPrepStatementInsert()) {
            // The batch prepared the INSERT again after the driver closed the first one
            data.db.closePreparedStatement(data.insertBatch.getStatement());
          }
          data.db.closeUpdate();
          data.db.closeInsert();
        }
      } catch (HopDatabaseException e) {
        logError(
            BaseMessages.getString(PKG, "InsertUpdate.Log.UnableToCommitConnection")
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
