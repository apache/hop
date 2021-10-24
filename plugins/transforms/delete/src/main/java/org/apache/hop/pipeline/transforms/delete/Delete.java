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

package org.apache.hop.pipeline.transforms.delete;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
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
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.sql.SQLException;
import java.util.List;

/** Delete data in a database table. */
public class Delete extends BaseTransform<DeleteMeta, DeleteData>
    implements ITransform<DeleteMeta, DeleteData> {

  private static final Class<?> PKG = DeleteMeta.class; // For Translator

  public Delete(
      TransformMeta transformMeta,
      DeleteMeta meta,
      DeleteData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private synchronized void deleteValues(IRowMeta rowMeta, Object[] row) throws HopException {
    // OK, now do the lookup.
    // We need the lookupvalues for that.
    Object[] deleteRow = new Object[data.deleteParameterRowMeta.size()];
    int deleteIndex = 0;

    List<DeleteKeyField> keyFields = meta.getLookup().getFields();
    for (int i = 0; i < keyFields.size(); i++) {
      if (data.keynrs[i] >= 0) {
        deleteRow[deleteIndex] = row[data.keynrs[i]];
        deleteIndex++;
      }
      if (data.keynrs2[i] >= 0) {
        deleteRow[deleteIndex] = row[data.keynrs2[i]];
        deleteIndex++;
      }
    }

    data.db.setValues(data.deleteParameterRowMeta, deleteRow, data.prepStatementDelete);

    if (log.isDebug()) {
      logDebug(
          BaseMessages.getString(
              PKG,
              "Delete.Log.SetValuesForDelete",
              data.deleteParameterRowMeta.getString(deleteRow),
              rowMeta.getString(row)));
    }

    data.db.insertRow(data.prepStatementDelete);
    incrementLinesUpdated();
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
              this, meta.getLookup().getSchemaName(), meta.getLookup().getTableName());

      // lookup the values!
      if (log.isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "Delete.Log.CheckingRow") + getInputRowMeta().getString(r));
      }

      List<DeleteKeyField> keyFields = meta.getLookup().getFields();
      data.keynrs = new int[keyFields.size()];
      data.keynrs2 = new int[keyFields.size()];
      for (int i = 0; i < keyFields.size(); i++) {
        DeleteKeyField dlf = keyFields.get(i);
        data.keynrs[i] = getInputRowMeta().indexOfValue(dlf.getKeyStream());
        if (data.keynrs[i] < 0
            && // couldn't find field!
            !"IS NULL".equalsIgnoreCase(dlf.getKeyCondition())
            && // No field needed!
            !"IS NOT NULL".equalsIgnoreCase(dlf.getKeyCondition()) // No field needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "Delete.Exception.FieldRequired", dlf.getKeyStream()));
        }

        data.keynrs2[i] =
            (dlf.getKeyStream2() == null
                ? -1
                : getInputRowMeta().indexOfValue(dlf.getKeyStream2()));
        if (data.keynrs2[i] < 0
            && // couldn't find field!
            "BETWEEN".equalsIgnoreCase(dlf.getKeyCondition()) // 2 fields needed!
        ) {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "Delete.Exception.FieldRequired", dlf.getKeyStream2()));
        }

        if (log.isDebug()) {
          logDebug(
              BaseMessages.getString(PKG, "Delete.Log.FieldInfo", dlf.getKeyStream())
                  + data.keynrs[i]);
        }
      }

      prepareDelete(getInputRowMeta());
    }

    try {
      deleteValues(getInputRowMeta(), r); // add new values to the row in rowset[0].
      putRow(
          data.outputRowMeta, r); // output the same rows of data, but with a copy of the metadata

      if (checkFeedback(getLinesRead()) && log.isBasic()) {
        logBasic(BaseMessages.getString(PKG, "Delete.Log.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {

      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {

        logError(BaseMessages.getString(PKG, "Delete.Log.ErrorInTransform") + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }

      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), r, 1, errorMessage, null, "DEL001");
      }
    }

    return true;
  }

  // Lookup certain fields in a table
  public void prepareDelete(IRowMeta rowMeta) throws HopDatabaseException {
    DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);

    data.deleteParameterRowMeta = new RowMeta();
    String sql = "DELETE FROM " + data.schemaTable + Const.CR;

    sql += "WHERE ";
    List<DeleteKeyField> keyFields = meta.getLookup().getFields();

    for (int i = 0; i < keyFields.size(); i++) {
      DeleteKeyField dlkf = keyFields.get(i);
      if (i != 0) {
        sql += "AND   ";
      }
      sql += databaseMeta.quoteField(dlkf.getKeyLookup());
      if ("BETWEEN".equalsIgnoreCase(dlkf.getKeyCondition())) {
        sql += " BETWEEN ? AND ? ";
        data.deleteParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(dlkf.getKeyStream()));
        data.deleteParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(dlkf.getKeyStream2()));
      } else if ("IS NULL".equalsIgnoreCase(dlkf.getKeyCondition())
          || "IS NOT NULL".equalsIgnoreCase(dlkf.getKeyCondition())) {
        sql += " " + dlkf.getKeyCondition() + " ";
      } else {
        sql += " " + dlkf.getKeyCondition() + " ? ";
        data.deleteParameterRowMeta.addValueMeta(rowMeta.searchValueMeta(dlkf.getKeyStream()));
      }
    }

    try {
      if (log.isDetailed()) {
        logDetailed("Setting delete preparedStatement to [" + sql + "]");
      }
      data.prepStatementDelete =
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
        logError(BaseMessages.getString(PKG, "Delete.Init.ConnectionMissing", getTransformName()));
        return false;
      }

      DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
      if (databaseMeta == null) {
        logError(BaseMessages.getString(PKG, "Delete.Init.ConnectionMissing", getTransformName()));
        return false;
      }

      data.db = new Database(this, variables, databaseMeta);

      try {
        data.db.connect();

        if (log.isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "Delete.Log.ConnectedToDB"));
        }

        data.db.setCommit(meta.getCommitSize(this));

        return true;
      } catch (HopException ke) {
        logError(BaseMessages.getString(PKG, "Delete.Log.ErrorOccurred") + ke.getMessage());
        setErrors(1);
        stopAll();
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
        if (dispose) data.db.closeUpdate();
      } catch (HopDatabaseException e) {
        logError(
            BaseMessages.getString(PKG, "Delete.Log.UnableToCommitUpdateConnection")
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
