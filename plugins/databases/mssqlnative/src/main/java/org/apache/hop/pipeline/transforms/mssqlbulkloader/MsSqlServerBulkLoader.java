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

package org.apache.hop.pipeline.transforms.mssqlbulkloader;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerSortOrder;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Loads pipeline rows into a SQL Server table through the driver's bulk copy API.
 *
 * <p>Rows are buffered until the configured batch size is reached and then handed to {@code
 * SQLServerBulkCopy} as a {@link RowBufferBulkData}. Each batch is committed on its own, so the
 * batch size is both the memory bound and the commit granularity.
 */
public class MsSqlServerBulkLoader
    extends BaseTransform<MsSqlServerBulkLoaderMeta, MsSqlServerBulkLoaderData> {

  private static final Class<?> PKG = MsSqlServerBulkLoaderMeta.class;

  public MsSqlServerBulkLoader(
      TransformMeta transformMeta,
      MsSqlServerBulkLoaderMeta meta,
      MsSqlServerBulkLoaderData data,
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

    try {
      if (Utils.isEmpty(meta.getConnection())) {
        throw new HopException(
            BaseMessages.getString(PKG, "MsSqlServerBulkLoader.Exception.ConnectionNotDefined"));
      }
      data.databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
      if (data.databaseMeta == null) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "MsSqlServerBulkLoader.Exception.ConnectionNotFound",
                resolve(meta.getConnection())));
      }

      data.batchSize = resolveBatchSize();

      data.db = new Database(this, this, data.databaseMeta);
      data.db.connect();
      data.db.setAutoCommit(false);
      data.connection = data.db.getConnection();

      if (isBasic()) {
        logBasic(
            BaseMessages.getString(
                PKG, "MsSqlServerBulkLoader.Log.ConnectedToDatabase", data.databaseMeta.getName()));
      }
      return true;
    } catch (HopException e) {
      logError(
          BaseMessages.getString(PKG, "MsSqlServerBulkLoader.Exception.InitFailed", e.getMessage()),
          e);
      setErrors(1);
      stopAll();
      return false;
    }
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow();

    if (r == null) {
      // An empty stream still truncates, unless the transform was told not to.
      if (first && meta.isTruncateTable() && !meta.isOnlyWhenHaveRows()) {
        truncateTable();
      }
      if (!first) {
        flush();
      }
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      if (meta.isTruncateTable()) {
        truncateTable();
      }

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      buildColumnMapping();
      openBulkCopy();
    }

    data.buffer.add(convertRow(r));
    putRow(getInputRowMeta(), r);

    if (data.buffer.size() >= data.batchSize) {
      flush();
    }

    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic(BaseMessages.getString(PKG, "MsSqlServerBulkLoader.Log.LineNumber", getLinesRead()));
    }
    return true;
  }

  private int resolveBatchSize() throws HopException {
    String realBatchSize = resolve(meta.getBatchSize());
    if (Utils.isEmpty(realBatchSize)) {
      realBatchSize = MsSqlServerBulkLoaderMeta.DEFAULT_BATCH_SIZE;
    }
    try {
      int size = Integer.parseInt(realBatchSize);
      if (size <= 0) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "MsSqlServerBulkLoader.Exception.InvalidBatchSize", realBatchSize));
      }
      return size;
    } catch (NumberFormatException e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoader.Exception.InvalidBatchSize", realBatchSize));
    }
  }

  /**
   * Works out which input field feeds which target column, and what the driver needs to know about
   * each of those columns.
   */
  private void buildColumnMapping() throws HopException {
    String realSchemaName = resolve(meta.getSchemaName());
    String realTableName = resolve(meta.getTableName());

    if (Utils.isEmpty(realTableName)) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "MsSqlServerBulkLoader.Exception.TableNotSpecified"));
    }

    data.schemaTable =
        data.databaseMeta.getQuotedSchemaTableCombination(this, realSchemaName, realTableName);

    Map<String, RowBufferBulkData.Column> tableColumns =
        readTableColumns(realSchemaName, realTableName);

    List<Integer> streamIndexes = new ArrayList<>();
    List<IValueMeta> streamValueMeta = new ArrayList<>();
    List<RowBufferBulkData.Column> targetColumns = new ArrayList<>();
    List<String> alreadyMapped = new ArrayList<>();

    if (meta.isSpecifyFields()) {
      for (MsSqlServerBulkLoaderMeta.Field field : meta.getFields()) {
        int index = getInputRowMeta().indexOfValue(field.getFieldStream());
        if (index < 0) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG,
                  "MsSqlServerBulkLoader.Exception.FieldNotFoundInStream",
                  field.getFieldStream()));
        }
        RowBufferBulkData.Column column = lookupColumn(tableColumns, field.getFieldTable());
        if (alreadyMapped.contains(column.name())) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "MsSqlServerBulkLoader.Exception.DuplicateTableField", column.name()));
        }
        alreadyMapped.add(column.name());

        streamIndexes.add(index);
        streamValueMeta.add(getInputRowMeta().getValueMeta(index));
        targetColumns.add(column);
      }
    } else {
      for (int i = 0; i < getInputRowMeta().size(); i++) {
        IValueMeta valueMeta = getInputRowMeta().getValueMeta(i);
        streamIndexes.add(i);
        streamValueMeta.add(valueMeta);
        targetColumns.add(lookupColumn(tableColumns, valueMeta.getName()));
      }
    }

    if (targetColumns.isEmpty()) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "MsSqlServerBulkLoader.Exception.NoFieldsToLoad"));
    }

    data.streamIndexes = streamIndexes.stream().mapToInt(Integer::intValue).toArray();
    data.streamValueMeta = streamValueMeta.toArray(new IValueMeta[0]);
    data.targetColumns = targetColumns.toArray(new RowBufferBulkData.Column[0]);
  }

  private RowBufferBulkData.Column lookupColumn(
      Map<String, RowBufferBulkData.Column> tableColumns, String name)
      throws HopTransformException {
    RowBufferBulkData.Column column =
        name == null ? null : tableColumns.get(name.toUpperCase(Locale.ROOT));
    if (column == null) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoader.Exception.FieldNotFoundInTable", name, data.schemaTable));
    }
    return column;
  }

  /**
   * Reads the target table's columns straight from the JDBC metadata, which gives the exact type,
   * size and scale the bulk copy protocol expects. Querying the table for them would need read
   * access on a table this transform only ever writes to.
   */
  private Map<String, RowBufferBulkData.Column> readTableColumns(
      String schemaName, String tableName) throws HopException {

    Map<String, RowBufferBulkData.Column> columns = new LinkedHashMap<>();
    try {
      DatabaseMetaData metaData = data.connection.getMetaData();
      // getColumns takes LIKE patterns, so a table called "sales_2024" would otherwise also match
      // "salesX2024".
      String escape = metaData.getSearchStringEscape();
      // An empty schema means the connection's own default schema, not "any schema": a null
      // pattern matches every one of them, so two schemas holding a same-named table would
      // collapse into one column map.
      String realSchemaName = Utils.isEmpty(schemaName) ? data.connection.getSchema() : schemaName;
      String schemaPattern =
          Utils.isEmpty(realSchemaName) ? null : escapePattern(realSchemaName, escape);
      String tablePattern = escapePattern(tableName, escape);

      try (ResultSet rs =
          metaData.getColumns(data.connection.getCatalog(), schemaPattern, tablePattern, null)) {
        while (rs.next()) {
          String name = rs.getString("COLUMN_NAME");
          columns.put(
              name.toUpperCase(Locale.ROOT),
              new RowBufferBulkData.Column(
                  name,
                  rs.getInt("DATA_TYPE"),
                  rs.getInt("COLUMN_SIZE"),
                  rs.getInt("DECIMAL_DIGITS")));
        }
      }
    } catch (SQLException e) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoader.Exception.CouldNotReadTableMetadata", data.schemaTable),
          e);
    }

    if (columns.isEmpty()) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoader.Exception.TableNotFound", data.schemaTable));
    }
    return columns;
  }

  private static String escapePattern(String value, String escape) {
    if (Utils.isEmpty(escape)) {
      return value;
    }
    StringBuilder escaped = new StringBuilder(value.length());
    for (char c : value.toCharArray()) {
      if (c == '_' || c == '%') {
        escaped.append(escape);
      }
      escaped.append(c);
    }
    return escaped.toString();
  }

  private void openBulkCopy() throws HopException {
    try {
      SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
      options.setBatchSize(data.batchSize);
      options.setTableLock(meta.isTableLock());
      options.setKeepIdentity(meta.isKeepIdentity());
      options.setKeepNulls(meta.isKeepNulls());
      options.setCheckConstraints(meta.isCheckConstraints());
      options.setFireTriggers(meta.isFireTriggers());
      options.setAllowEncryptedValueModifications(meta.isAllowEncryptedValueModifications());
      options.setBulkCopyTimeout(resolveTimeout());
      // The transform commits every batch itself; letting the driver open its own transaction as
      // well would take that decision away.
      options.setUseInternalTransaction(false);

      data.bulkCopy = new SQLServerBulkCopy(data.connection);
      data.bulkCopy.setBulkCopyOptions(options);
      data.bulkCopy.setDestinationTableName(data.schemaTable);

      for (int i = 0; i < data.targetColumns.length; i++) {
        data.bulkCopy.addColumnMapping(i + 1, data.targetColumns[i].name());
      }

      addColumnOrderHints();
    } catch (SQLException e) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoader.Exception.CouldNotStartBulkCopy", data.schemaTable),
          e);
    }
  }

  private void addColumnOrderHints() throws SQLException {
    if (!meta.isSpecifyFields()) {
      return;
    }
    for (MsSqlServerBulkLoaderMeta.Field field : meta.getFields()) {
      switch (field.getOrderHint()) {
        case ASCENDING ->
            data.bulkCopy.addColumnOrderHint(field.getFieldTable(), SQLServerSortOrder.ASCENDING);
        case DESCENDING ->
            data.bulkCopy.addColumnOrderHint(field.getFieldTable(), SQLServerSortOrder.DESCENDING);
        case NONE -> {
          // No hint: let SQL Server decide.
        }
      }
    }
  }

  private int resolveTimeout() throws HopException {
    String realTimeout = resolve(meta.getBulkCopyTimeout());
    if (Utils.isEmpty(realTimeout)) {
      return 0;
    }
    try {
      int timeout = Integer.parseInt(realTimeout);
      if (timeout < 0) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "MsSqlServerBulkLoader.Exception.InvalidTimeout", realTimeout));
      }
      return timeout;
    } catch (NumberFormatException e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoader.Exception.InvalidTimeout", realTimeout));
    }
  }

  /**
   * Turns one pipeline row into the objects the driver will send. Values keep their type all the
   * way down - nothing is rendered to text - so a null stays a null and no value can be confused
   * for a separator.
   */
  private Object[] convertRow(Object[] r) throws HopException {
    Object[] converted = new Object[data.targetColumns.length];
    for (int i = 0; i < converted.length; i++) {
      IValueMeta valueMeta = data.streamValueMeta[i];
      Object value = r[data.streamIndexes[i]];
      converted[i] = convertValue(valueMeta, value, data.targetColumns[i].sqlType());
    }
    return converted;
  }

  /**
   * Turns one pipeline value into the object the driver expects for that column.
   *
   * <p>The target column type decides the shape, not the Hop type: {@code SQLServerBulkCopy} casts
   * each value straight to the class its JDBC type uses, so a Hop integer - always a {@code Long} -
   * handed to an {@code int} column throws a ClassCastException rather than being narrowed.
   */
  static Object convertValue(IValueMeta valueMeta, Object value, int targetSqlType)
      throws HopException {
    if (valueMeta.isNull(value)) {
      return null;
    }
    return switch (targetSqlType) {
      case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> toInteger(valueMeta, value);
      case Types.BIGINT -> valueMeta.getInteger(value);
      case Types.BIT, Types.BOOLEAN -> valueMeta.getBoolean(value);
      case Types.REAL, Types.FLOAT -> toFloat(valueMeta, value);
      case Types.DOUBLE -> valueMeta.getNumber(value);
      case Types.DECIMAL, Types.NUMERIC -> valueMeta.getBigNumber(value);
      case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> valueMeta.getBinary(value);
      case Types.DATE -> {
        Date date = valueMeta.getDate(value);
        yield date == null ? null : new java.sql.Date(date.getTime());
      }
      case Types.TIME -> {
        Date date = valueMeta.getDate(value);
        yield date == null ? null : new Time(date.getTime());
      }
      case Types.TIMESTAMP -> toTimestamp(valueMeta, value);
        // The character types, and every SQL Server specific one the driver reads as text:
        // uniqueidentifier, xml, json and datetimeoffset all arrive here.
      default -> valueMeta.getString(value);
    };
  }

  private static Integer toInteger(IValueMeta valueMeta, Object value) throws HopException {
    Long number = valueMeta.getInteger(value);
    if (number == null) {
      return null;
    }
    try {
      return Math.toIntExact(number);
    } catch (ArithmeticException e) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG,
              "MsSqlServerBulkLoader.Exception.IntegerOutOfRange",
              valueMeta.getName(),
              String.valueOf(number)));
    }
  }

  private static Float toFloat(IValueMeta valueMeta, Object value) throws HopException {
    Double number = valueMeta.getNumber(value);
    return number == null ? null : number.floatValue();
  }

  private static Timestamp toTimestamp(IValueMeta valueMeta, Object value) throws HopException {
    Date date = valueMeta.getDate(value);
    if (date == null) {
      return null;
    }
    // A ValueMetaTimestamp already hands us a Timestamp, nanoseconds and all.
    return date instanceof Timestamp timestamp ? timestamp : new Timestamp(date.getTime());
  }

  /** Sends everything buffered so far to the server and commits it. */
  private void flush() throws HopException {
    if (data.buffer.isEmpty()) {
      return;
    }
    int rowCount = data.buffer.size();
    try {
      data.bulkCopy.writeToServer(new RowBufferBulkData(data.buffer, data.targetColumns));
      // Through the Hop Database rather than the raw connection: when the connection belongs to a
      // transaction group the pipeline commits it as a whole, and committing here would break that.
      data.db.commit();
    } catch (SQLException e) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoader.Exception.BulkCopyFailed", data.schemaTable),
          e);
    }
    data.buffer.clear();
    setLinesOutput(getLinesOutput() + rowCount);
  }

  /** Only one copy of the transform may truncate, or the copies would wipe each other's rows. */
  private void truncateTable() throws HopDatabaseException {
    if (getCopy() != 0) {
      return;
    }
    data.db.truncateTable(resolve(meta.getSchemaName()), resolve(meta.getTableName()));
    data.db.commit();
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoader.Log.TruncatedTable", resolve(meta.getTableName())));
    }
  }

  @Override
  public void dispose() {
    if (data.bulkCopy != null) {
      data.bulkCopy.close();
      data.bulkCopy = null;
    }

    if (data.db != null) {
      try {
        if (getErrors() > 0) {
          data.db.rollback();
        }
      } catch (HopDatabaseException e) {
        logError(BaseMessages.getString(PKG, "MsSqlServerBulkLoader.Exception.RollbackFailed"), e);
      }
      data.db.disconnect();
      data.db = null;
    }

    data.connection = null;
    data.buffer = null;
    data.targetColumns = null;
    data.streamValueMeta = null;

    super.dispose();
  }
}
