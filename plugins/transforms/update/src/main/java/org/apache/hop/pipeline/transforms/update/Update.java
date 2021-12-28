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
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.sql.SQLException;
import java.util.ArrayList;

/** Update data in a database table, does NOT ever perform an insert. */
public class Update extends BaseTransform<UpdateMeta, UpdateData>
    implements ITransform<UpdateMeta, UpdateData> {
  private static final Class<?> PKG = UpdateMeta.class; // For Translator

  public Update(
      TransformMeta transformMeta,
      UpdateMeta meta,
      UpdateData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private synchronized Object[] lookupValues(IRowMeta rowMeta, Object[] row) throws HopException {
    Object[] outputRow = row;
    Object[] add;

    // Create the output row and copy the input values
    if (!Utils.isEmpty(meta.getIgnoreFlagField())) { // add flag field!

      outputRow = new Object[data.outputRowMeta.size()];
      for (int i = 0; i < rowMeta.size(); i++) {
        outputRow[i] = row[i];
      }
    }

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
    IRowMeta returnRowMeta = null;
    if (!meta.isSkipLookup()) {
      data.db.setValues(data.lookupParameterRowMeta, lookupRow, data.prepStatementLookup);
      if (log.isDebug()) {
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
          putError(rowMeta, row, 1L, data.stringErrorKeyNotFound, data.stringFieldnames, "UPD001");
        } else {
          throw new HopDatabaseException(
              BaseMessages.getString(PKG, "Update.Exception.KeyCouldNotFound")
                  + data.lookupParameterRowMeta.getString(lookupRow));
        }
      } else {
        if (log.isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "Update.Log.KeyCouldNotFound")
                  + data.lookupParameterRowMeta.getString(lookupRow));
        }
        if (!Utils.isEmpty(meta.getIgnoreFlagField())) { // set flag field!

          outputRow[rowMeta.size()] = Boolean.FALSE;
        }
      }
    } else {
      if (!meta.isSkipLookup()) {
        if (log.isRowLevel()) {
          logRowlevel(
              BaseMessages.getString(PKG, "Update.Log.FoundRow")
                  + data.lookupReturnRowMeta.getString(add));
        }
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
          Object rowvalue = row[data.valuenrs[i]];
          IValueMeta returnValueMeta = returnRowMeta.getValueMeta(i);
          Object retvalue = add[i];

          if (returnValueMeta.compare(retvalue, valueMeta, rowvalue) != 0) {
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

        if (log.isRowLevel()) {
          logRowlevel(
              BaseMessages.getString(PKG, "Update.Log.UpdateRow")
                  + data.lookupParameterRowMeta.getString(lookupRow));
        }
        data.db.setValues(data.updateParameterRowMeta, updateRow, data.prepStatementUpdate);
        data.db.insertRow(data.prepStatementUpdate, meta.isUseBatchUpdate(), true);
        incrementLinesUpdated();
      } else {
        incrementLinesSkipped();
      }

      if (!Utils.isEmpty(meta.getIgnoreFlagField())) { // add flag field!

        outputRow[rowMeta.size()] = Boolean.TRUE;
      }
    }

    return outputRow;
  }

  @Override
  public boolean processRow() throws HopException {

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) { // no more input to be expected...

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
      if (log.isDetailed()) {
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
            !"IS NULL".equalsIgnoreCase(keyItem.getKeyCondition())
            && // No field needed!
            !"IS NOT NULL".equalsIgnoreCase(keyItem.getKeyCondition()) // No field needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "Update.Exception.FieldRequired", keyItem.getKeyStream()));
        }
        keynrs.add(keynr);

        // this operator needs two bindings
        if ("= ~NULL".equalsIgnoreCase(keyItem.getKeyCondition())) {
          keynrs.add(keynr);
          keynrs2.add(-1);
        }

        int keynr2 = getInputRowMeta().indexOfValue(keyItem.getKeyStream2());
        if (keynr2 < 0
            && // couldn't find field!
            "BETWEEN".equalsIgnoreCase(keyItem.getKeyCondition()) // 2 fields needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "Update.Exception.FieldRequired", keyItem.getKeyStream2()));
        }
        keynrs2.add(keynr2);

        if (log.isDebug()) {
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
                  PKG, "Update.Exception.FieldRequired", fieldItem.getUpdateStream()));
        }
        if (log.isDebug()) {
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
          if ("BETWEEN".equalsIgnoreCase(keyItem.getKeyCondition())) {
            data.lookupParameterRowMeta.addValueMeta(
                getInputRowMeta().searchValueMeta(keyItem.getKeyStream()));
            data.lookupParameterRowMeta.addValueMeta(
                getInputRowMeta().searchValueMeta(keyItem.getKeyStream2()));
          } else {
            if ("= ~NULL".equalsIgnoreCase(keyItem.getKeyCondition())) {
              data.lookupParameterRowMeta.addValueMeta(
                  getInputRowMeta().searchValueMeta(keyItem.getKeyStream()));
              data.lookupParameterRowMeta.addValueMeta(
                  getInputRowMeta().searchValueMeta(keyItem.getKeyStream()).clone());
            } else if (!"IS NULL".equalsIgnoreCase(keyItem.getKeyCondition())
                && !"IS NOT NULL".equalsIgnoreCase(keyItem.getKeyCondition())) {
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
      Object[] outputRow =
          lookupValues(getInputRowMeta(), r); // add new values to the row in rowset[0].
      if (outputRow != null) {
        putRow(data.outputRowMeta, outputRow); // copy non-ignored rows to output rowset(s)
      }
      if (checkFeedback(getLinesRead())) {
        if (log.isBasic()) {
          logBasic(BaseMessages.getString(PKG, "Update.Log.LineNumber") + getLinesRead());
        }
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

    return true;
  }

  public void setLookup(IRowMeta rowMeta) throws HopDatabaseException {
    data.lookupParameterRowMeta = new RowMeta();
    data.lookupReturnRowMeta = new RowMeta();

    DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);

    String sql = "SELECT ";

    for (int i = 0; i < meta.getLookupField().getUpdateFields().size(); i++) {
      UpdateField fieldItem = meta.getLookupField().getUpdateFields().get(i);

      if (i != 0) {
        sql += ", ";
      }
      sql += databaseMeta.quoteField(fieldItem.getUpdateLookup());
      data.lookupReturnRowMeta.addValueMeta(rowMeta.searchValueMeta(fieldItem.getUpdateStream()));
    }

    sql += " FROM " + data.schemaTable + " WHERE ";

    for (int i = 0; i < meta.getLookupField().getLookupKeys().size(); i++) {
      UpdateKeyField keyItem = meta.getLookupField().getLookupKeys().get(i);
      if (i != 0) {
        sql += " AND ";
      }

      sql += " ( ( ";

      sql += databaseMeta.quoteField(keyItem.getKeyLookup());
      if ("BETWEEN".equalsIgnoreCase(keyItem.getKeyCondition())) {
        sql += " BETWEEN ? AND ? ";
        data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream()));
        data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream2()));
      } else {
        if ("IS NULL".equalsIgnoreCase(keyItem.getKeyCondition())
            || "IS NOT NULL".equalsIgnoreCase(keyItem.getKeyCondition())) {
          sql += " " + keyItem.getKeyCondition() + " ";
        } else if ("= ~NULL".equalsIgnoreCase(keyItem.getKeyCondition())) {

          sql += " IS NULL AND ";

          if (databaseMeta.requiresCastToVariousForIsNull()) {
            sql += "CAST(? AS VARCHAR(256)) IS NULL";
          } else {
            sql += "? IS NULL";
          }
          // null check
          data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream()));
          sql += " ) OR ( " + databaseMeta.quoteField(keyItem.getKeyLookup()) + " = ?";
          // equality check, cloning so auto-rename because of adding same fieldname does not cause
          // problems
          data.lookupParameterRowMeta.addValueMeta(
              rowMeta.searchValueMeta(keyItem.getKeyStream()).clone());

        } else {
          sql += " " + keyItem.getKeyCondition() + " ? ";
          data.lookupParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream()));
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

    for (int i = 0; i < meta.getLookupField().getUpdateFields().size(); i++) {

      UpdateField fieldItem = meta.getLookupField().getUpdateFields().get(i);

      if (i != 0) {
        sql += ",   ";
      }
      sql += databaseMeta.quoteField(fieldItem.getUpdateLookup());
      sql += " = ?" + Const.CR;
      data.updateParameterRowMeta.addValueMeta(
          rowMeta.searchValueMeta(fieldItem.getUpdateStream()));
    }

    sql += "WHERE ";

    for (int i = 0; i < meta.getLookupField().getLookupKeys().size(); i++) {

      UpdateKeyField keyItem = meta.getLookupField().getLookupKeys().get(i);

      if (i != 0) {
        sql += "AND   ";
      }
      sql += " ( ( ";
      sql += databaseMeta.quoteField(keyItem.getKeyLookup());
      if ("BETWEEN".equalsIgnoreCase(keyItem.getKeyCondition())) {
        sql += " BETWEEN ? AND ? ";
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream()));
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream2()));
      } else if ("IS NULL".equalsIgnoreCase(keyItem.getKeyCondition())
          || "IS NOT NULL".equalsIgnoreCase(keyItem.getKeyCondition())) {
        sql += " " + keyItem.getKeyCondition() + " ";
      } else if ("= ~NULL".equalsIgnoreCase(keyItem.getKeyCondition())) {

        sql += " IS NULL AND ";

        if (databaseMeta.requiresCastToVariousForIsNull()) {
          sql += "CAST(? AS VARCHAR(256)) IS NULL";
        } else {
          sql += "? IS NULL";
        }
        // null check
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream()));
        sql += " ) OR ( " + databaseMeta.quoteField(keyItem.getKeyLookup()) + " = ?";
        // equality check, cloning so auto-rename because of adding same fieldname does not cause
        // problems
        data.updateParameterRowMeta.addValueMeta(
            rowMeta.searchValueMeta(keyItem.getKeyStream()).clone());

      } else {
        sql += " " + keyItem.getKeyCondition() + " ? ";
        data.updateParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(keyItem.getKeyStream()));
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

      if (Utils.isEmpty(meta.getConnection())) {
        logError(BaseMessages.getString(PKG, "Update.Init.ConnectionMissing", getTransformName()));
        return false;
      }

      DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
      data.db = new Database(this, this, databaseMeta);

      try {
        data.db.connect();

        if (log.isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "Update.Log.ConnectedToDB"));
        }

        data.db.setCommit(meta.getCommitSize(this));

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
              data.db.emptyAndCommit(data.prepStatementUpdate, meta.isUseBatchUpdate());
            } else {
              data.db.commit();
            }
          } else {
            data.db.rollback();
          }
        }
        if (dispose) {
          data.db.closePreparedStatement(data.prepStatementUpdate);
          data.db.closePreparedStatement(data.prepStatementLookup);
        }
      } catch (HopDatabaseException e) {
        logError(
            BaseMessages.getString(PKG, "Update.Log.UnableToCommitUpdateConnection")
                + data.db
                + "] :"
                + e.toString());
        setErrors(1);
      } finally {
        if (dispose) data.db.disconnect();
      }
    }
  }
}
