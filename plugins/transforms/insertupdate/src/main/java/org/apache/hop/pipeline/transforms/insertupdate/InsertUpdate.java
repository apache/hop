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

import org.apache.commons.lang.ArrayUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Performs a lookup in a database table. If the key doesn't exist it inserts values into the table,
 * otherwise it performs an update of the changed values. If nothing changed, do nothing.
 */
public class InsertUpdate extends BaseTransform<InsertUpdateMeta, InsertUpdateData> {
  private static final Class<?> PKG = InsertUpdateMeta.class; // For Translator

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

    if (log.isDebug()) {
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
      if (log.isRowLevel()) {
        logRowlevel(BaseMessages.getString(PKG, "InsertUpdate.InsertRow") + rowMeta.getString(row));
      }

      // The values to insert are those in the update section (all fields should be specified)
      // For the others, we have no definite mapping!
      //
      Object[] insertRow = new Object[data.valuenrs.length];
      for (int i = 0; i < data.valuenrs.length; i++) {
        insertRow[i] = row[data.valuenrs[i]];
      }

      // Set the values on the prepared statement...
      data.db.setValuesInsert(data.insertRowMeta, insertRow);

      // Insert the row
      data.db.insertRow();

      incrementLinesOutput();
    } else {
      if (!meta.isUpdateBypassed()) {
        if (log.isRowLevel()) {
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

          if (log.isRowLevel()) {
            logRowlevel(
                BaseMessages.getString(PKG, "InsertUpdate.Log.UpdateRow")
                    + data.lookupParameterRowMeta.getString(lookupRow));
          }
          data.db.setValues(data.updateParameterRowMeta, updateRow, data.prepStatementUpdate);
          data.db.insertRow(data.prepStatementUpdate);
          incrementLinesUpdated();
        } else {
          incrementLinesSkipped();
        }
      } else {
        if (log.isRowLevel()) {
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
      if (log.isDebug()) {
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
            !"IS NULL".equalsIgnoreCase(keyField.getKeyCondition())
            && // No field needed!
            !"IS NOT NULL".equalsIgnoreCase(keyField.getKeyCondition()) // No field needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "InsertUpdate.Exception.FieldRequired", keyField.getKeyStream()));
        }
        keynrs.add(keynr);

        // this operator needs two bindings
        if ("= ~NULL".equalsIgnoreCase(keyField.getKeyCondition())) {
          keynrs.add(keynr);
          keynrs2.add(-1);
        }

        int keynr2 = getInputRowMeta().indexOfValue(keyField.getKeyStream2());
        if (keynr2 < 0
            && // couldn't find field!
            "BETWEEN".equalsIgnoreCase(keyField.getKeyCondition()) // 2 fields needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "InsertUpdate.Exception.FieldRequired", keyField.getKeyStream2()));
        }
        keynrs2.add(keynr2);

        if (log.isDebug()) {
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
                  PKG, "InsertUpdate.Exception.FieldRequired", valueField.getUpdateStream()));
        }
        if (log.isDebug()) {
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
              "The same column can't be inserted into the target row twice: "
                  + insValue.getName()); // TODO i18n
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
    }

    try {
      lookupValues(getInputRowMeta(), r); // add new values to the row in rowset[0].
      putRow(
          data.outputRowMeta,
          r); // Nothing changed to the input, return the same row, pass a "cloned" metadata
      // row.

      if (checkFeedback(getLinesRead())) {
        if (log.isBasic()) {
          logBasic(BaseMessages.getString(PKG, "InsertUpdate.Log.LineNumber") + getLinesRead());
        }
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

  public void setLookup(IRowMeta rowMeta) throws HopDatabaseException {
    data.lookupParameterRowMeta = new RowMeta();
    data.lookupReturnRowMeta = new RowMeta();

    DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);

    String sql = "SELECT ";

    for (int i = 0; i < meta.getInsertUpdateLookupField().getValueFields().size(); i++) {
      InsertUpdateValue valueField = meta.getInsertUpdateLookupField().getValueFields().get(i);
      if (i != 0) {
        sql += ", ";
      }
      sql += databaseMeta.quoteField(valueField.getUpdateLookup());
      data.lookupReturnRowMeta.addValueMeta(
          rowMeta.searchValueMeta(valueField.getUpdateStream()).clone());
    }

    sql += " FROM " + data.schemaTable + " WHERE ";

    for (int i = 0; i < meta.getInsertUpdateLookupField().getLookupKeys().size(); i++) {
      InsertUpdateKeyField keyField = meta.getInsertUpdateLookupField().getLookupKeys().get(i);
      if (i != 0) {
        sql += " AND ";
      }

      sql += " ( ( ";

      sql += databaseMeta.quoteField(keyField.getKeyLookup());
      if ("BETWEEN".equalsIgnoreCase(keyField.getKeyCondition())) {
        sql += " BETWEEN ? AND ? ";
        data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyField.getKeyStream()));
        data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyField.getKeyStream2()));
      } else {
        if ("IS NULL".equalsIgnoreCase(keyField.getKeyCondition())
            || "IS NOT NULL".equalsIgnoreCase(keyField.getKeyCondition())) {
          sql += " " + keyField.getKeyCondition() + " ";
        } else if ("= ~NULL".equalsIgnoreCase(keyField.getKeyCondition())) {

          sql += " IS NULL AND ";

          if (databaseMeta.requiresCastToVariousForIsNull()) {
            sql += " CAST(? AS VARCHAR(256)) IS NULL ";
          } else {
            sql += " ? IS NULL ";
          }
          // null check
          data.lookupParameterRowMeta.addValueMeta(
              rowMeta.searchValueMeta(keyField.getKeyStream()));
          sql += " ) OR ( " + databaseMeta.quoteField(keyField.getKeyLookup()) + " = ? ";
          // equality check, cloning so auto-rename because of adding same fieldname does not cause
          // problems
          data.lookupParameterRowMeta.addValueMeta(
              rowMeta.searchValueMeta(keyField.getKeyStream()).clone());

        } else {
          sql += " " + keyField.getKeyCondition() + " ? ";
          data.lookupParameterRowMeta.addValueMeta(
              rowMeta.searchValueMeta(keyField.getKeyStream()));
        }
      }
      sql += " ) ) ";
    }

    try {
      if (log.isDetailed()) {
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
  public void prepareUpdate(IRowMeta rowMeta) throws HopDatabaseException {

    DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
    data.updateParameterRowMeta = new RowMeta();

    String sql = "UPDATE " + data.schemaTable + Const.CR;
    sql += "SET ";

    boolean comma = false;

    for (int i = 0; i < meta.getInsertUpdateLookupField().getValueFields().size(); i++) {
      InsertUpdateValue valueField = meta.getInsertUpdateLookupField().getValueFields().get(i);
      if (valueField.isUpdate()) {
        if (comma) {
          sql += ",   ";
        } else {
          comma = true;
        }

        sql += databaseMeta.quoteField(valueField.getUpdateLookup());
        sql += " = ?" + Const.CR;
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(valueField.getUpdateStream()).clone());
      }
    }

    sql += "WHERE ";

    for (int i = 0; i < meta.getInsertUpdateLookupField().getLookupKeys().size(); i++) {
      InsertUpdateKeyField keyField = meta.getInsertUpdateLookupField().getLookupKeys().get(i);
      if (i != 0) {
        sql += "AND   ";
      }
      sql += " ( ( ";
      sql += databaseMeta.quoteField(keyField.getKeyLookup());
      if ("BETWEEN".equalsIgnoreCase(keyField.getKeyCondition())) {
        sql += " BETWEEN ? AND ? ";
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyField.getKeyStream()));
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyField.getKeyStream2()));
      } else if ("IS NULL".equalsIgnoreCase(keyField.getKeyCondition())
          || "IS NOT NULL".equalsIgnoreCase(keyField.getKeyCondition())) {
        sql += " " + keyField.getKeyCondition() + " ";
      } else if ("= ~NULL".equalsIgnoreCase(keyField.getKeyCondition())) {

        sql += " IS NULL AND ";

        if (databaseMeta.requiresCastToVariousForIsNull()) {
          sql += "CAST(? AS VARCHAR(256)) IS NULL";
        } else {
          sql += "? IS NULL";
        }
        // null check
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyField.getKeyStream()));
        sql += " ) OR ( " + databaseMeta.quoteField(keyField.getKeyLookup()) + " = ?";
        // equality check, cloning so auto-rename because of adding same fieldname does not cause
        // problems
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyField.getKeyStream()).clone());

      } else {
        sql += " " + keyField.getKeyCondition() + " ? ";
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyField.getKeyStream()).clone());
      }
      sql += " ) ) ";
    }

    try {
      if (log.isDetailed()) {
        logDetailed("Setting update preparedStatement to [" + sql + "]");
      }
      data.prepStatementUpdate =
          data.db.getConnection().prepareStatement(databaseMeta.stripCR(sql));
    } catch (SQLException ex) {
      throw new HopDatabaseException(
          "Unable to prepare statement for SQL statement [" + sql + "]", ex);
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
        data.db.setCommit(meta.getCommitSizeVar(this));

        return true;
      } catch (HopException ke) {
        logError(
            BaseMessages.getString(PKG, "InsertUpdate.Log.ErrorOccurredDuringTransformInitialize")
                + ke.getMessage());
      }
    }
    return false;
  }

  @Override
  public void batchComplete() throws HopException {
    commitBatch(false);
  }

  @Override
  public void dispose() {
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
