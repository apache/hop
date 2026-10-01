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
package org.apache.hop.core.database.validation;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.Getter;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.types.DatabaseColumn;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;

/**
 * Column constraints for one table, loaded once at transform init. After this object exists the
 * connection can be closed.
 */
@Getter
public final class TableValueConstraints {

  private final String characterSet;
  private final List<ColumnValueConstraints> columns;
  private final Map<String, ColumnValueConstraints> columnsByName;

  public TableValueConstraints(String characterSet, List<ColumnValueConstraints> columns) {
    this.characterSet = characterSet;
    this.columns = List.copyOf(columns);
    Map<String, ColumnValueConstraints> byName = new LinkedHashMap<>();
    for (ColumnValueConstraints column : this.columns) {
      if (column.getColumnName() != null) {
        byName.put(column.getColumnName().toLowerCase(Locale.ROOT), column);
      }
    }
    this.columnsByName = Map.copyOf(byName);
  }

  public ColumnValueConstraints findColumn(String name) {
    if (name == null) {
      return null;
    }
    return columnsByName.get(name.toLowerCase(Locale.ROOT));
  }

  /**
   * Load Hop table field types, JDBC nullability/defaults, then let the dialect enrich the spec.
   */
  public static TableValueConstraints load(Database database, String schemaName, String tableName)
      throws HopDatabaseException {
    if (database == null || database.getDatabaseMeta() == null) {
      throw new HopDatabaseException("No database connection is available to load table columns");
    }
    if (Utils.isEmpty(tableName)) {
      throw new HopDatabaseException("No table name is specified");
    }
    DatabaseMeta databaseMeta = database.getDatabaseMeta();
    IDatabase iDatabase = databaseMeta.getIDatabase();
    String characterSet = iDatabase.getDatabaseCharacterSet(database);

    IRowMeta tableFields = database.getTableFieldsMeta(schemaName, tableName);
    if (tableFields == null || tableFields.isEmpty()) {
      throw new HopDatabaseException(
          "Unable to read columns for table "
              + databaseMeta.getQuotedSchemaTableCombination(database, schemaName, tableName));
    }

    Map<String, JdbcColumnExtras> extras = loadJdbcExtras(database, schemaName, tableName);

    List<ColumnValueConstraints> columns = new ArrayList<>(tableFields.size());
    for (int i = 0; i < tableFields.size(); i++) {
      IValueMeta valueMeta = tableFields.getValueMeta(i);
      JdbcColumnExtras extra = extras.get(key(valueMeta.getName()));
      ColumnValueConstraints spec = new ColumnValueConstraints();
      spec.setColumnName(valueMeta.getName());
      spec.setNativeTypeName(
          extra != null && extra.nativeTypeName != null
              ? extra.nativeTypeName
              : valueMeta.getOriginalColumnTypeName());
      spec.setSqlType(extra != null ? extra.sqlType : valueMeta.getOriginalColumnType());
      spec.setHopType(valueMeta.getType());
      spec.setTargetValueMeta(valueMeta);
      spec.setNullable(extra == null || extra.nullable);
      spec.setHasDefault(extra != null && extra.hasDefault);
      spec.setCharacterSet(characterSet);

      if (valueMeta.getType() == IValueMeta.TYPE_STRING
          && ColumnValueValidator.hasLimitedLength(valueMeta.getLength())) {
        spec.setStringMaxLength(valueMeta.getLength());
      }
      if (isNumericHopType(valueMeta.getType())
          && valueMeta.getLength() > 0
          && valueMeta.getPrecision() >= 0) {
        spec.setNumericPrecision(valueMeta.getLength());
        spec.setNumericScale(valueMeta.getPrecision());
      }

      DatabaseColumn column =
          extra != null
              ? extra.column
              : DatabaseColumn.of(
                  valueMeta.getName(),
                  valueMeta.getOriginalColumnType(),
                  valueMeta.getOriginalColumnTypeName(),
                  valueMeta.getLength(),
                  valueMeta.getPrecision(),
                  valueMeta.getLength());
      iDatabase.enrichColumnValueConstraints(spec, column, characterSet);
      columns.add(spec);
    }
    return new TableValueConstraints(characterSet, columns);
  }

  private static boolean isNumericHopType(int hopType) {
    return hopType == IValueMeta.TYPE_BIGNUMBER || hopType == IValueMeta.TYPE_NUMBER;
  }

  private static Map<String, JdbcColumnExtras> loadJdbcExtras(
      Database database, String schemaName, String tableName) {
    Map<String, JdbcColumnExtras> extras = new LinkedHashMap<>();
    try {
      DatabaseMetaData metaData = database.getDatabaseMetaData();
      String catalog = null;
      try {
        if (database.getConnection() != null) {
          catalog = database.getConnection().getCatalog();
        }
      } catch (Exception ignored) {
        // Driver will not give a catalog; getColumns still works with null.
      }
      String schemaPattern = Utils.isEmpty(schemaName) ? null : schemaName;
      try (ResultSet columns = metaData.getColumns(catalog, schemaPattern, tableName, null)) {
        while (columns.next()) {
          DatabaseColumn column = DatabaseColumn.ofColumnsRow(columns);
          int nullableFlag = columns.getInt("NULLABLE");
          String isNullable = columns.getString("IS_NULLABLE");
          boolean nullable =
              nullableFlag != DatabaseMetaData.columnNoNulls && !"NO".equalsIgnoreCase(isNullable);
          String defaultValue = columns.getString("COLUMN_DEF");
          boolean hasDefault = defaultValue != null;
          extras.put(
              key(column.getName()),
              new JdbcColumnExtras(
                  nullable, hasDefault, column.getSqlType(), column.getNativeTypeName(), column));
        }
      }
    } catch (Exception ignored) {
      // Hop types from getTableFieldsMeta are enough; nullability stays the permissive default.
    }
    return extras;
  }

  private static String key(String name) {
    return name == null ? "" : name.toLowerCase(Locale.ROOT);
  }

  private record JdbcColumnExtras(
      boolean nullable,
      boolean hasDefault,
      int sqlType,
      String nativeTypeName,
      DatabaseColumn column) {}
}
