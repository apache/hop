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

package org.apache.hop.pipeline.transforms.columnexists;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Check if a column exists in table on a specified connection * */
public class ColumnExists extends BaseTransform<ColumnExistsMeta, ColumnExistsData> {

  private static final Class<?> PKG = ColumnExistsMeta.class; // For Translator

  private static final String PKG_COULD_NOT_FIND_FIELD = "ColumnExists.Exception.CouldnotFindField";

  public ColumnExists(
      TransformMeta transformMeta,
      ColumnExistsMeta meta,
      ColumnExistsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    // if no more input to be expected set done
    if (r == null) {
      setOutputDone();
      return false;
    }

    boolean columnexists = false;

    if (first) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Check is columnname field is provided
      if (Utils.isEmpty(meta.getColumnnamefield())) {
        logError(BaseMessages.getString(PKG, "ColumnExists.Error.ColumnnameFieldMissing"));
        throw new HopException(
            BaseMessages.getString(PKG, "ColumnExists.Error.ColumnnameFieldMissing"));
      }
      if (meta.isTablenameInfield()) {
        // Check is tablename field is provided
        if (Utils.isEmpty(meta.getTablenamefield())) {
          logError(BaseMessages.getString(PKG, "ColumnExists.Error.TablenameFieldMissing"));
          throw new HopException(
              BaseMessages.getString(PKG, "ColumnExists.Error.TablenameFieldMissing"));
        }

        // cache the position of the field
        if (data.indexOfTablename < 0) {
          data.indexOfTablename = getInputRowMeta().indexOfValue(meta.getTablenamefield());
          if (data.indexOfTablename < 0) {
            // The field is unreachable !
            logError(
                BaseMessages.getString(PKG, PKG_COULD_NOT_FIND_FIELD)
                    + "["
                    + meta.getTablenamefield()
                    + "]");
            throw new HopException(
                BaseMessages.getString(PKG, PKG_COULD_NOT_FIND_FIELD, meta.getTablenamefield()));
          }
        }
      } else {
        if (!Utils.isEmpty(data.schemaname)) {
          data.tableName =
              data.db
                  .getDatabaseMeta()
                  .getQuotedSchemaTableCombination(this, data.schemaname, data.tableName);
        } else {
          data.tableName = data.db.getDatabaseMeta().quoteField(data.tableName);
        }
      }

      // cache the position of the column field
      if (data.indexOfColumnname < 0) {
        data.indexOfColumnname = getInputRowMeta().indexOfValue(meta.getColumnnamefield());
        if (data.indexOfColumnname < 0) {
          // The field is unreachable !
          logError(
              BaseMessages.getString(PKG, PKG_COULD_NOT_FIND_FIELD)
                  + "["
                  + meta.getColumnnamefield()
                  + "]");
          throw new HopException(
              BaseMessages.getString(PKG, PKG_COULD_NOT_FIND_FIELD, meta.getColumnnamefield()));
        }
      }

      // End If first
    }

    try {
      // get tablename
      if (meta.isTablenameInfield()) {
        data.tableName = getInputRowMeta().getString(r, data.indexOfTablename);
          data.tableName = data.db.getDatabaseMeta().quoteField(data.tableName);
      }
      // get columnname
      String columnname = getInputRowMeta().getString(r, data.indexOfColumnname);
      columnname = data.db.getDatabaseMeta().quoteField(columnname);

      // Check if table exists on the specified connection
      columnexists = data.db.checkColumnExists(data.schemaname, data.tableName, columnname);

      Object[] outputRowData = RowDataUtil.addValueData(r, getInputRowMeta().size(), columnexists);

      // add new values to the row.
      putRow(data.outputRowMeta, outputRowData); // copy row to output rowset(s)

      if (log.isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(
                PKG,
                "ColumnExists.LineNumber",
                getLinesRead() + " : " + getInputRowMeta().getString(r)));
      }
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(
            BaseMessages.getString(
                PKG, "ColumnExists.ErrorInTransformRunning" + " : " + e.getMessage()));
        throw new HopTransformException(
            BaseMessages.getString(PKG, "ColumnExists.Log.ErrorInTransform"), e);
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(
            getInputRowMeta(), r, 1, errorMessage, meta.getResultfieldname(), "ColumnExists001");
      }
    }

    return true;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      if (!meta.isTablenameInfield()) {
        if (Utils.isEmpty(meta.getTableName())) {
          logError(BaseMessages.getString(PKG, "ColumnExists.Error.TablenameMissing"));
          return false;
        }
        data.tableName = resolve(meta.getTableName());
      }
      data.schemaname = meta.getSchemaname();
      if (!Utils.isEmpty(data.schemaname)) {
        data.schemaname = resolve(data.schemaname);
      }

      if (Utils.isEmpty(meta.getResultfieldname())) {
        logError(BaseMessages.getString(PKG, "ColumnExists.Error.ResultFieldMissing"));
        return false;
      }

      data.db =
          new Database(
              this, this, getPipelineMeta().findDatabase(meta.getDatabaseName(), variables));
      try {
        data.db.connect();

        if (log.isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ColumnExists.Log.ConnectedToDB"));
        }

        return true;
      } catch (HopException e) {
        logError(BaseMessages.getString(PKG, "ColumnExists.Log.DBException") + e.getMessage());
        if (data.db != null) {
          data.db.disconnect();
        }
      }
    }
    return false;
  }

  @Override
  public void dispose() {
    if (data.db != null) {
      data.db.disconnect();
    }
    super.dispose();
  }
}
