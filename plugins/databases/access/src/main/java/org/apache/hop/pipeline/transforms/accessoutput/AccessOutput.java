/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.pipeline.transforms.accessoutput;

import io.github.spannm.jackcess.Column;
import io.github.spannm.jackcess.ColumnBuilder;
import io.github.spannm.jackcess.Cursor;
import io.github.spannm.jackcess.CursorBuilder;
import io.github.spannm.jackcess.DataType;
import io.github.spannm.jackcess.Database.FileFormat;
import io.github.spannm.jackcess.DatabaseBuilder;
import io.github.spannm.jackcess.TableBuilder;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class AccessOutput extends BaseTransform<AccessOutputMeta, AccessOutputData> {
  private static final Class<?> PKG =
      AccessOutput.class; // for i18n purposes, needed by Translator2!!

  public AccessOutput(
      TransformMeta transformMeta,
      AccessOutputMeta meta,
      AccessOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] row = getRow(); // this also waits for a previous step to be finished.
    if (row == null) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if (first && meta.isWaitFirstRowToCreateFile()) {
      try {
        if (!openFile()) {
          return false;
        }

      } catch (Exception e) {
        logError("An error occurred intialising this transformation: " + e.getMessage());
        stopAll();
        setErrors(1);
      }
    }
    try {
      writeToTable(row);
      putRow(data.outputRowMeta, row); // in case we want it go further...

      if (checkFeedback(getLinesOutput()) && isBasic()) {
        logBasic("linenr " + getLinesOutput());
      }
    } catch (HopException e) {
      logError("Because of an error, this transformation can't continue: " + e.getMessage());
      setErrors(1);
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }

    return true;
  }

  protected boolean writeToTable(Object[] row) throws HopValueException {
    if (row == null) {
      // Stop: last line or error encountered
      if (isDetailed()) {
        logDetailed("Last line inserted: stop");
      }
      return false;
    }

    if (first) {
      first = false;

      data.outputRowMeta = getInputRowMeta();

      // First open or create the table
      try {
        String tableName = resolve(meta.getTableName());
        data.table = data.db.getTable(tableName);
        if (data.table == null) {
          if (meta.isCreateTable()) {

            if (isBasic()) {
              logBasic(
                  BaseMessages.getString(PKG, "AccessOutput.Log.CreateDatabaseTable", tableName));
            }

            // Create the table
            List<ColumnBuilder> columns = prepareTableColumns(data.outputRowMeta);

            data.table = new TableBuilder(tableName).addColumns(columns).toTable(data.db);

            if (isDebug()) {
              for (Column column : data.table.getColumns()) {
                logDebug(BaseMessages.getString(PKG, "AccessOutput.Log.TableColumn", column));
              }
            }

          } else {
            logError(
                BaseMessages.getString(PKG, "AccessOutput.Error.TableDoesNotExist", tableName));
            setErrors(1);
            stopAll();
            return false;
          }
        } else if (meta.isTruncateTable()) {
          truncateTable();
        }
        // All OK: we have an open database and a table to write to.
        //
        // Apparently it's not yet possible to remove rows from the table
        // So truncate is out for the moment as well.

      } catch (Exception e) {
        logError(
            BaseMessages.getString(
                PKG, "AccessOutput.Exception.UnexpectedErrorCreatingTable", e.toString()));
        logError(Const.getStackTracker(e));
        setErrors(1);
        stopAll();
        return false;
      }
    }

    // Let's write a row to the database.
    Object[] values = createRowValues(data.outputRowMeta, row);
    try {
      data.rows.add(values);
      if (meta.getCommitSize() > 0) {
        if (data.rows.size() >= meta.getCommitSize()) {
          data.table.addRows(data.rows);
          data.rows.clear();
        }
      } else {
        data.table.addRow(values);
      }
    } catch (IOException e) {
      logError(
          BaseMessages.getString(
              PKG,
              "AccessOutput.Exception.UnexpectedErrorWritingRow",
              data.outputRowMeta.getString(row)));
      logError(Const.getStackTracker(e));
      setErrors(1);
      stopAll();
      return false;
    }

    return true;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      if (!meta.isWaitFirstRowToCreateFile()) {
        try {
          return openFile();
        } catch (Exception e) {
          logError("An error occurred intialising this transformation: " + e.getMessage());
          stopAll();
          setErrors(1);
        }
      } else {
        return true;
      }
    }
    return false;
  }

  protected boolean openFile() throws Exception {
    data.oneFileOpened = true;
    String fileName = resolve(meta.getFileName());
    if (isBasic()) {
      logBasic(BaseMessages.getString(PKG, "AccessOutput.log.WritingToFile", fileName));
    }
    FileObject fileObject = HopVfs.getFileObject(fileName);
    File file = FileUtils.toFile(fileObject.getURL());

    // First open or create the access file
    if (!file.exists()) {
      if (meta.isCreateFile()) {
        if (isBasic()) {
          logBasic(BaseMessages.getString(PKG, "AccessOutput.Log.CreateDatabaseFile", fileName));
        }
        data.db = DatabaseBuilder.create(FileFormat.V2019, file);
      } else {
        logError(BaseMessages.getString(PKG, "AccessOutput.Error.FileDoesNotExist", fileName));
        return false;
      }
    } else {
      if (isBasic()) {
        logBasic(BaseMessages.getString(PKG, "AccessOutput.Log.OpenDatabaseFile", fileName));
      }
      data.db = DatabaseBuilder.open(file);
    }

    // Add the filename to the result object...
    //
    if (meta.isAddToResultFile()) {
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL, fileObject, getPipelineMeta().getName(), toString());
      resultFile.setComment("This file was created with an Microsoft Access output transformation");
      addResultFile(resultFile);
    }

    return true;
  }

  protected void truncateTable() throws IOException {
    if (data.table == null) {
      return;
    }
    Cursor tableRows = CursorBuilder.createCursor(data.table);
    while (tableRows.moveToNextRow()) {
      tableRows.deleteCurrentRow();
    }
  }

  public Object[] createRowValues(IRowMeta rowMeta, Object[] rowData) throws HopValueException {
    Object[] values = new Object[rowMeta.size()];
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      Object valueData = rowData[i];

      // Prevent a NullPointerException below
      if (valueData == null || valueMeta == null) {
        values[i] = null;
        continue;
      }

      int length = valueMeta.getLength();

      switch (valueMeta.getType()) {
        case IValueMeta.TYPE_INTEGER:
          if (length < 3) {
            values[i] = valueMeta.getInteger(valueData).byteValue();
          } else {
            if (length < 5) {
              values[i] = valueMeta.getInteger(valueData).shortValue();
            } else {
              values[i] = valueMeta.getInteger(valueData);
            }
          }
          break;
        case IValueMeta.TYPE_NUMBER:
          values[i] = valueMeta.getNumber(valueData);
          break;
        case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP:
          values[i] = valueMeta.getDate(valueData);
          break;
        case IValueMeta.TYPE_STRING:
          values[i] = valueMeta.getString(valueData);
          break;
        case IValueMeta.TYPE_BINARY:
          values[i] = valueMeta.getBinary(valueData);
          break;
        case IValueMeta.TYPE_BOOLEAN:
          values[i] = valueMeta.getBoolean(valueData);
          break;
        case IValueMeta.TYPE_BIGNUMBER:
          values[i] = valueMeta.getNumber(valueData);
          break;
        default:
          break;
      }
    }
    return values;
  }

  public List<ColumnBuilder> prepareTableColumns(IRowMeta row) {
    List<ColumnBuilder> columns = new ArrayList<>();

    for (int i = 0; i < row.size(); i++) {
      IValueMeta valueMeta = row.getValueMeta(i);

      ColumnBuilder column = new ColumnBuilder(valueMeta.getName());

      int length = valueMeta.getLength();

      switch (valueMeta.getType()) {
        case IValueMeta.TYPE_INTEGER:
          if (length < 3) {

            column.withType(DataType.BYTE);
            column.withLength(DataType.BYTE.getFixedSize());
          } else {
            if (length < 5) {
              column.withType(DataType.INT);
              column.withLength(DataType.INT.getFixedSize());
            } else {
              column.withType(DataType.LONG);
              column.withLength(DataType.LONG.getFixedSize());
            }
          }
          break;
        case IValueMeta.TYPE_NUMBER:
          column.withType(DataType.DOUBLE);
          column.withLength(DataType.DOUBLE.getFixedSize());
          break;
        case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP:
          column.withType(DataType.SHORT_DATE_TIME);
          column.withLength(DataType.SHORT_DATE_TIME.getFixedSize());
          break;
        case IValueMeta.TYPE_STRING:
          length *= DataType.TEXT.getUnitSize();
          if (length <= DataType.TEXT.getMaxSize()) {
            column.withType(DataType.TEXT);
          } else {
            column.withType(DataType.MEMO);
          }
          column.withLength(length);
          break;
        case IValueMeta.TYPE_BINARY:
          column.withType(DataType.BINARY);
          break;
        case IValueMeta.TYPE_BOOLEAN:
          column.withType(DataType.BOOLEAN);
          column.withLength(DataType.BOOLEAN.getFixedSize());
          break;
        case IValueMeta.TYPE_BIGNUMBER:
          column.withType(DataType.NUMERIC);
          column.withLength(DataType.NUMERIC.getFixedSize());
          break;
        default:
          break;
      }

      if (valueMeta.getPrecision() >= 1 && valueMeta.getPrecision() <= 28) {
        column.withPrecision(valueMeta.getPrecision());
      }

      columns.add(column);
    }

    return columns;
  }

  @Override
  public void dispose() {
    if (data.oneFileOpened) {
      try {
        // Put the last records in the table as well!
        if (data.table != null) {
          data.table.addRows(data.rows);
        }

        // Just for good measure.
        data.rows.clear();

        if (data.db != null) {
          data.db.close();
          data.db = null;
        }
      } catch (IOException e) {
        logError(
            BaseMessages.getString(
                PKG, "AccessOutput.Exception.UnexpectedErrorClosingDatabase", e));
        setErrors(1);
        stopAll();
      }
    }
    super.dispose();
  }
}
