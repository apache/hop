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
package org.apache.hop.pipeline.transforms.databasevaluevalidation;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.validation.ColumnValueConstraints;
import org.apache.hop.core.database.validation.ColumnValueError;
import org.apache.hop.core.database.validation.ColumnValueValidator;
import org.apache.hop.core.database.validation.TableValueConstraints;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class DatabaseValueValidation
    extends BaseTransform<DatabaseValueValidationMeta, DatabaseValueValidationData> {
  private static final Class<?> PKG = DatabaseValueValidationMeta.class;

  public DatabaseValueValidation(
      TransformMeta transformMeta,
      DatabaseValueValidationMeta meta,
      DatabaseValueValidationData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    data.separator = Const.NVL(resolve(meta.getConcatenationSeparator()), "; ");
    data.omitValues = meta.isOmitValues();

    String connectionName = resolve(Const.NVL(meta.getConnectionName(), ""));
    if (Utils.isEmpty(connectionName)) {
      logError(BaseMessages.getString(PKG, "DatabaseValueValidation.Error.ConnectionMissing"));
      return false;
    }
    String tableName = resolve(Const.NVL(meta.getTableName(), ""));
    if (Utils.isEmpty(tableName)) {
      logError(BaseMessages.getString(PKG, "DatabaseValueValidation.Error.TableMissing"));
      return false;
    }
    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(connectionName);
      if (databaseMeta == null) {
        logError(BaseMessages.getString(PKG, "DatabaseValueValidation.Error.ConnectionMissing"));
        return false;
      }
      String schemaName = resolve(Const.NVL(meta.getSchemaName(), ""));
      try (Database db = new Database(this, this, databaseMeta)) {
        db.connect();
        data.tableConstraints = TableValueConstraints.load(db, schemaName, tableName);
      }
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "DatabaseValueValidation.Log.LoadedConstraints",
                Integer.toString(data.tableConstraints.getColumns().size()),
                Const.NVL(data.tableConstraints.getCharacterSet(), "")));
        for (ColumnValueConstraints column : data.tableConstraints.getColumns()) {
          logDetailed(column.toString());
        }
      }
      return true;
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "DatabaseValueValidation.Error.LoadConstraints"), e);
      return false;
    }
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      logValidationSummary();
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      resolveMapping(getInputRowMeta());
    }

    data.rowsChecked++;
    List<ColumnValueError> errors = new ArrayList<>();
    IRowMeta inputMeta = getInputRowMeta();
    for (int i = 0; i < data.streamIndexes.length; i++) {
      int index = data.streamIndexes[i];
      Object value = index >= 0 && index < row.length ? row[index] : null;
      IValueMeta streamMeta = inputMeta.getValueMeta(index);
      errors.addAll(
          ColumnValueValidator.validate(
              data.fieldConstraints[i],
              data.streamFieldNames[i],
              streamMeta,
              value,
              data.omitValues));
    }

    if (errors.isEmpty()) {
      putRow(data.outputRowMeta, row);
    } else {
      data.rowsRejected++;
      for (ColumnValueError error : errors) {
        data.errorsByColumn.merge(error.columnName(), 1L, Long::sum);
      }
      String descriptions = join(errors, ColumnValueError::message);
      String fields = join(errors, ColumnValueError::streamFieldName);
      String codes = join(errors, e -> e.code().getCode());
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(inputMeta, row, errors.size(), descriptions, fields, codes);
      } else {
        throw new HopException(descriptions);
      }
    }

    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic(
          BaseMessages.getString(
              PKG, "DatabaseValueValidation.Log.LineNumber", Long.toString(getLinesRead())));
    }
    return true;
  }

  private void resolveMapping(IRowMeta inputMeta) throws HopTransformException {
    List<DatabaseValueValidationField> mappings = meta.getFields();
    if (mappings == null || mappings.isEmpty()) {
      mappings = matchByName(inputMeta);
    }

    List<Integer> indexes = new ArrayList<>();
    List<ColumnValueConstraints> constraints = new ArrayList<>();
    List<String> names = new ArrayList<>();
    Set<String> mappedColumns = new LinkedHashSet<>();

    for (DatabaseValueValidationField mapping : mappings) {
      String streamName = resolve(Const.NVL(mapping.getFieldStream(), ""));
      String columnName = resolve(Const.NVL(mapping.getFieldDatabase(), ""));
      if (Utils.isEmpty(streamName) && Utils.isEmpty(columnName)) {
        continue;
      }
      if (Utils.isEmpty(streamName) || Utils.isEmpty(columnName)) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "DatabaseValueValidation.Error.IncompleteMapping", streamName, columnName));
      }
      int index = inputMeta.indexOfValue(streamName);
      if (index < 0) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "DatabaseValueValidation.Error.StreamFieldMissing", streamName));
      }
      ColumnValueConstraints column = data.tableConstraints.findColumn(columnName);
      if (column == null) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "DatabaseValueValidation.Error.TableColumnMissing", columnName));
      }
      indexes.add(index);
      constraints.add(column);
      names.add(streamName);
      mappedColumns.add(column.getColumnName().toLowerCase(Locale.ROOT));
    }

    if (indexes.isEmpty()) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "DatabaseValueValidation.Error.NoMapping"));
    }

    if (meta.isFailIfRequiredColumnsUnmapped()) {
      List<String> missing = new ArrayList<>();
      for (ColumnValueConstraints column : data.tableConstraints.getColumns()) {
        if (column.isRequiredWithoutDefault()
            && !mappedColumns.contains(column.getColumnName().toLowerCase(Locale.ROOT))) {
          missing.add(column.getColumnName());
        }
      }
      if (!missing.isEmpty()) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "DatabaseValueValidation.Error.RequiredUnmapped", String.join(", ", missing)));
      }
    }

    data.streamIndexes = indexes.stream().mapToInt(Integer::intValue).toArray();
    data.fieldConstraints = constraints.toArray(ColumnValueConstraints[]::new);
    data.streamFieldNames = names.toArray(String[]::new);
  }

  private List<DatabaseValueValidationField> matchByName(IRowMeta inputMeta) {
    List<DatabaseValueValidationField> mappings = new ArrayList<>();
    for (int i = 0; i < inputMeta.size(); i++) {
      String name = inputMeta.getValueMeta(i).getName();
      if (data.tableConstraints.findColumn(name) != null) {
        mappings.add(new DatabaseValueValidationField(name, name));
      }
    }
    return mappings;
  }

  private String join(
      List<ColumnValueError> errors, java.util.function.Function<ColumnValueError, String> getter) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < errors.size(); i++) {
      if (i > 0) {
        builder.append(data.separator);
      }
      builder.append(Const.NVL(getter.apply(errors.get(i)), ""));
    }
    return builder.toString();
  }

  private void logValidationSummary() {
    if (!isBasic()) {
      return;
    }
    StringBuilder perColumn = new StringBuilder();
    data.errorsByColumn.forEach(
        (column, count) -> {
          if (!perColumn.isEmpty()) {
            perColumn.append(", ");
          }
          perColumn.append(column).append("=").append(count);
        });
    logBasic(
        BaseMessages.getString(
            PKG,
            "DatabaseValueValidation.Log.Summary",
            Long.toString(data.rowsChecked),
            Long.toString(data.rowsRejected),
            perColumn.isEmpty() ? "-" : perColumn.toString()));
  }
}
