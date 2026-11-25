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

package org.apache.hop.junit.dml;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * {@link DatabaseMetaData} of a few tables in a few schemas or databases, answering the calls that
 * {@link org.apache.hop.core.database.dml.DmlColumns} and {@link
 * org.apache.hop.core.database.dml.DmlSupport} make as real drivers do, their quirks included:
 *
 * <ul>
 *   <li>a null catalog or schema means every one, and {@code _} and {@code %} in a pattern are
 *       wildcards;
 *   <li>{@link #schemaArgumentIgnored()}: Connector/J, which names its databases catalogs and
 *       ignores the schema argument, so without a catalog it searches every database;
 *   <li>{@link #namesIgnoringCase()}: a case-insensitive SQL Server, which matches names ignoring
 *       case and returns them as they were created;
 *   <li>the primary key is listed by {@code getIndexInfo} too, as an index named {@code PRIMARY} in
 *       every table, as MySQL names it.
 * </ul>
 */
public final class FakeCatalog {

  /** A table and its keys. */
  public static final class Table {
    private final String catalog;
    private final String schema;
    private final String name;
    private final List<ColumnMetaDataStub.Column> columns = new ArrayList<>();
    private final List<String> primaryKey = new ArrayList<>();
    private final Map<String, List<String>> uniqueIndexes = new LinkedHashMap<>();
    private final Map<String, String> filters = new HashMap<>();

    private Table(String catalog, String schema, String name) {
      this.catalog = catalog;
      this.schema = schema;
      this.name = name;
    }

    public Table column(String column, String typeName, int dataType, int size, Integer digits) {
      columns.add(new ColumnMetaDataStub.Column(column, typeName, dataType, size, digits));
      return this;
    }

    public Table primaryKey(String... keyColumns) {
      primaryKey.addAll(List.of(keyColumns));
      return this;
    }

    public Table uniqueIndex(String index, String... indexColumns) {
      uniqueIndexes.put(index, List.of(indexColumns));
      return this;
    }

    /** A unique index with a predicate: {@code CREATE UNIQUE INDEX ... WHERE filter}. */
    public Table partialUniqueIndex(String index, String filter, String... indexColumns) {
      uniqueIndex(index, indexColumns);
      filters.put(index, filter);
      return this;
    }
  }

  private final List<Table> tables = new ArrayList<>();
  private String currentSchema;
  private String currentCatalog;
  private boolean schemaArgumentIgnored;
  private boolean namesIgnoringCase;
  private boolean indexInfo = true;
  private int major = 1;
  private int minor;
  private Throwable versionFailure;
  private String productName = "Fake";
  private String productVersion = "1.0";

  public Table table(String catalog, String schema, String name) {
    Table table = new Table(catalog, schema, name);
    tables.add(table);
    return table;
  }

  /** What {@link Connection#getSchema()} returns. */
  public FakeCatalog currentSchema(String schema) {
    this.currentSchema = schema;
    return this;
  }

  /** What {@link Connection#getCatalog()} returns. */
  public FakeCatalog currentCatalog(String catalog) {
    this.currentCatalog = catalog;
    return this;
  }

  /** Connector/J: the schema argument is ignored, a null catalog is every database. */
  public FakeCatalog schemaArgumentIgnored() {
    this.schemaArgumentIgnored = true;
    return this;
  }

  /** A case-insensitive SQL Server: names match ignoring case. */
  public FakeCatalog namesIgnoringCase() {
    this.namesIgnoringCase = true;
    return this;
  }

  /** {@code getIndexInfo} fails, as on a driver that can't list indexes. */
  public FakeCatalog noIndexInfo() {
    this.indexInfo = false;
    return this;
  }

  public FakeCatalog version(int major, int minor) {
    this.major = major;
    this.minor = minor;
    return this;
  }

  /** Asking for the version throws this. */
  public FakeCatalog failVersion(Throwable failure) {
    this.versionFailure = failure;
    return this;
  }

  public FakeCatalog product(String name, String version) {
    this.productName = name;
    this.productVersion = version;
    return this;
  }

  public DatabaseMetaData metaData() {
    return (DatabaseMetaData)
        Proxy.newProxyInstance(
            FakeCatalog.class.getClassLoader(),
            new Class<?>[] {DatabaseMetaData.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "getDatabaseMajorVersion" -> version(major);
                  case "getDatabaseMinorVersion" -> version(minor);
                  case "getDatabaseProductName" -> productName;
                  case "getDatabaseProductVersion" -> productVersion;
                  case "getConnection" -> connection();
                  case "getColumns" ->
                      columns((String) args[0], (String) args[1], (String) args[2]);
                  case "getPrimaryKeys" ->
                      primaryKeys((String) args[0], (String) args[1], (String) args[2]);
                  case "getIndexInfo" ->
                      indexInfo((String) args[0], (String) args[1], (String) args[2]);
                  case "toString" -> "fake catalog of " + tables.size() + " tables";
                  default ->
                      throw new UnsupportedOperationException(
                          "Not faked: DatabaseMetaData." + method.getName());
                });
  }

  private int version(int value) throws Throwable {
    if (versionFailure != null) {
      throw versionFailure;
    }
    return value;
  }

  private Connection connection() {
    return (Connection)
        Proxy.newProxyInstance(
            FakeCatalog.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "getSchema" -> currentSchema;
                  case "getCatalog" -> currentCatalog;
                  default ->
                      throw new UnsupportedOperationException(
                          "Not faked: Connection." + method.getName());
                });
  }

  /** The tables in the catalog and schema of a call; a pattern call matches the table name too. */
  private List<Table> matching(String catalog, String schema, String table, boolean pattern) {
    List<Table> result = new ArrayList<>();
    for (Table candidate : tables) {
      if (catalog != null && !matches(catalog, candidate.catalog, false)) {
        continue;
      }
      if (!schemaArgumentIgnored && schema != null && !matches(schema, candidate.schema, pattern)) {
        continue;
      }
      if (table != null && !matches(table, candidate.name, pattern)) {
        continue;
      }
      result.add(candidate);
    }
    return result;
  }

  private boolean matches(String argument, String stored, boolean pattern) {
    if (stored == null) {
      return false;
    }
    if (!pattern) {
      return namesIgnoringCase ? argument.equalsIgnoreCase(stored) : argument.equals(stored);
    }
    StringBuilder regex = new StringBuilder();
    for (char c : argument.toCharArray()) {
      switch (c) {
        case '%' -> regex.append(".*");
        case '_' -> regex.append('.');
        default -> regex.append(Pattern.quote(String.valueOf(c)));
      }
    }
    int flags = namesIgnoringCase ? Pattern.CASE_INSENSITIVE : 0;
    return Pattern.compile(regex.toString(), flags).matcher(stored).matches();
  }

  private ResultSet columns(String catalog, String schemaPattern, String tablePattern) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Table table : matching(catalog, schemaPattern, tablePattern, true)) {
      for (ColumnMetaDataStub.Column column : table.columns) {
        Map<String, Object> row = location(table);
        row.put("COLUMN_NAME", column.name());
        row.put("TYPE_NAME", column.typeName());
        row.put("DATA_TYPE", column.dataType());
        row.put("COLUMN_SIZE", column.columnSize());
        row.put("DECIMAL_DIGITS", column.decimalDigits());
        rows.add(row);
      }
    }
    return resultSet(rows);
  }

  private ResultSet primaryKeys(String catalog, String schema, String name) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Table table : matching(catalog, schema, name, false)) {
      for (String column : table.primaryKey) {
        Map<String, Object> row = location(table);
        row.put("COLUMN_NAME", column);
        rows.add(row);
      }
    }
    return resultSet(rows);
  }

  private ResultSet indexInfo(String catalog, String schema, String name) throws SQLException {
    if (!indexInfo) {
      throw new SQLException("getIndexInfo is not supported");
    }
    List<Map<String, Object>> rows = new ArrayList<>();
    for (Table table : matching(catalog, schema, name, false)) {
      Map<String, List<String>> indexes = new LinkedHashMap<>();
      if (!table.primaryKey.isEmpty()) {
        indexes.put("PRIMARY", table.primaryKey);
      }
      indexes.putAll(table.uniqueIndexes);
      for (Map.Entry<String, List<String>> index : indexes.entrySet()) {
        for (String column : index.getValue()) {
          Map<String, Object> row = location(table);
          row.put("INDEX_NAME", index.getKey());
          row.put("COLUMN_NAME", column);
          row.put("NON_UNIQUE", false);
          row.put("FILTER_CONDITION", table.filters.get(index.getKey()));
          rows.add(row);
        }
      }
    }
    return resultSet(rows);
  }

  private static Map<String, Object> location(Table table) {
    Map<String, Object> row = new HashMap<>();
    row.put("TABLE_CAT", table.catalog);
    row.put("TABLE_SCHEM", table.schema);
    row.put("TABLE_NAME", table.name);
    return row;
  }

  private static ResultSet resultSet(List<Map<String, Object>> rows) {
    int[] position = {-1};
    boolean[] wasNull = {false};
    return (ResultSet)
        Proxy.newProxyInstance(
            FakeCatalog.class.getClassLoader(),
            new Class<?>[] {ResultSet.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "next":
                  return ++position[0] < rows.size();
                case "close":
                  return null;
                case "wasNull":
                  return wasNull[0];
                case "getString":
                  {
                    Object value = rows.get(position[0]).get(key(args[0]));
                    wasNull[0] = value == null;
                    return value == null ? null : value.toString();
                  }
                case "getInt":
                  {
                    Object value = rows.get(position[0]).get(key(args[0]));
                    wasNull[0] = value == null;
                    return value == null ? 0 : ((Number) value).intValue();
                  }
                case "getBoolean":
                  {
                    Object value = rows.get(position[0]).get(key(args[0]));
                    wasNull[0] = value == null;
                    return Boolean.TRUE.equals(value);
                  }
                default:
                  throw new UnsupportedOperationException(
                      "Not faked: ResultSet." + method.getName());
              }
            });
  }

  private static String key(Object column) {
    return ((String) column).toUpperCase(Locale.ROOT);
  }
}
