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

package org.apache.hop.core.database.dml;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.util.Utils;

/**
 * Finds the one table a DML statement writes to in the JDBC metadata, so that its column types and
 * unique keys are read from that table and from no other.
 *
 * <p>The metadata calls take patterns and are not reliable about where they look: {@code _} and
 * {@code %} are wildcards, MySQL's Connector/J ignores the schema and with a null catalog searches
 * every database on the server, and a case-insensitive database returns the table under the case it
 * was created with. So every row is checked:
 *
 * <ul>
 *   <li>the table name must be the one asked for: exactly, or else ignoring case;
 *   <li>with a schema, the table must be in that schema (the catalog, on a driver that has no
 *       schemas, such as MySQL);
 *   <li>without a schema, the table the database itself would use: the one in the connection's
 *       current schema (or catalog). When that has no such table and several others do, the table
 *       is ambiguous and refused, since the database would not write to any of them.
 * </ul>
 *
 * <p>The table and schema are tried as given, then in upper and in lower case, since databases
 * store unquoted names differently.
 */
final class DmlTable {

  private DmlTable() {}

  /**
   * Where a table is stored, as the metadata names it.
   *
   * @param catalog TABLE_CAT, may be null
   * @param schema TABLE_SCHEM, may be null
   * @param table TABLE_NAME
   */
  record Location(String catalog, String schema, String table) {

    /**
     * @return the namespace that holds the table: the schema, or the catalog on a driver without
     *     schemas (MySQL calls its databases catalogs)
     */
    String container() {
      return container(catalog, schema);
    }

    static String container(String catalog, String schema) {
      return Utils.isEmpty(schema) ? Utils.isEmpty(catalog) ? null : catalog : schema;
    }

    /**
     * @return the catalog to pass to a metadata call that takes exact names: only on a driver that
     *     uses catalogs as its namespace. Other drivers get null, as some refuse their own catalog.
     */
    String catalogArgument() {
      return Utils.isEmpty(schema) ? catalog : null;
    }

    /**
     * @return whether a row of another metadata call, with these TABLE_CAT and TABLE_SCHEM, belongs
     *     to this table's namespace. A row that names no namespace is accepted: the call was
     *     already given this one.
     */
    boolean holds(String rowCatalog, String rowSchema, String rowTable) {
      if (!table.equals(rowTable)) {
        return false;
      }
      String rowContainer = container(rowCatalog, rowSchema);
      return rowContainer == null || rowContainer.equals(container());
    }

    @Override
    public String toString() {
      String container = container();
      return container == null ? table : container + "." + table;
    }
  }

  /** A column as {@link DatabaseMetaData#getColumns} reports it. */
  record ColumnRow(String name, String typeName, int dataType, int columnSize, int decimalDigits) {}

  /**
   * The table and its columns.
   *
   * @param location where it is stored
   * @param columns its columns, in table order
   */
  record Found(Location location, List<ColumnRow> columns) {}

  /** The table name matches several tables and none of them is the one the database would use. */
  static final class AmbiguousTableException extends SQLException {
    AmbiguousTableException(String table, Set<String> tables) {
      super(
          "Table "
              + table
              + " is ambiguous: there is "
              + String.join(" and ", tables)
              + ", and none of them is in the connection's current schema. Fill in the schema.");
    }
  }

  /**
   * @param metaData the metadata of an open connection
   * @param schema the schema, unquoted; null or empty for the one the database uses by default
   * @param table the table, unquoted
   * @return the table and its columns, or null if there is no such table
   * @throws AmbiguousTableException without a schema, when the table is in several schemas and not
   *     in the current one
   */
  static Found find(DatabaseMetaData metaData, String schema, String table) throws SQLException {
    String wantedSchema = Utils.isEmpty(schema) ? null : schema;
    for (String schemaPattern : variants(wantedSchema)) {
      for (String tablePattern : variants(table)) {
        Map<Location, List<ColumnRow>> tables = readColumns(metaData, schemaPattern, tablePattern);
        List<Location> named = byName(tables.keySet(), tablePattern, table);
        if (named.isEmpty()) {
          continue;
        }
        List<Location> chosen =
            wantedSchema != null
                ? inContainer(named, schemaPattern, wantedSchema)
                : withoutSchema(named, metaData, table);
        if (chosen.isEmpty()) {
          continue;
        }
        if (chosen.size() > 1) {
          Set<String> names = new LinkedHashSet<>();
          chosen.forEach(location -> names.add(location.toString()));
          throw new AmbiguousTableException(table, names);
        }
        Location location = chosen.get(0);
        return new Found(location, tables.get(location));
      }
    }
    return null;
  }

  private static Map<Location, List<ColumnRow>> readColumns(
      DatabaseMetaData metaData, String schemaPattern, String tablePattern) throws SQLException {
    Map<Location, List<ColumnRow>> tables = new LinkedHashMap<>();
    try (ResultSet resultSet = metaData.getColumns(null, schemaPattern, tablePattern, null)) {
      while (resultSet.next()) {
        Location location =
            new Location(
                resultSet.getString("TABLE_CAT"),
                resultSet.getString("TABLE_SCHEM"),
                resultSet.getString("TABLE_NAME"));
        if (location.table() == null) {
          continue;
        }
        String name = resultSet.getString("COLUMN_NAME");
        int decimalDigits = resultSet.getInt("DECIMAL_DIGITS");
        if (resultSet.wasNull()) {
          decimalDigits = -1;
        }
        tables
            .computeIfAbsent(location, key -> new ArrayList<>())
            .add(
                new ColumnRow(
                    name,
                    resultSet.getString("TYPE_NAME"),
                    resultSet.getInt("DATA_TYPE"),
                    resultSet.getInt("COLUMN_SIZE"),
                    decimalDigits));
      }
    }
    return tables;
  }

  /** The tables named exactly as the pattern, or else those named as the table ignoring case. */
  private static List<Location> byName(Set<Location> tables, String pattern, String table) {
    List<Location> exact = new ArrayList<>();
    List<Location> ignoringCase = new ArrayList<>();
    for (Location location : tables) {
      if (location.table().equals(pattern)) {
        exact.add(location);
      } else if (location.table().equalsIgnoreCase(table)) {
        ignoringCase.add(location);
      }
    }
    return exact.isEmpty() ? ignoringCase : exact;
  }

  /** The tables in the schema asked for: exactly as the pattern, or else ignoring case. */
  private static List<Location> inContainer(
      List<Location> tables, String pattern, String wantedSchema) {
    List<Location> exact = new ArrayList<>();
    List<Location> ignoringCase = new ArrayList<>();
    for (Location location : tables) {
      String container = location.container();
      if (container == null) {
        continue;
      }
      if (container.equals(pattern)) {
        exact.add(location);
      } else if (container.equalsIgnoreCase(wantedSchema)) {
        ignoringCase.add(location);
      }
    }
    return exact.isEmpty() ? ignoringCase : exact;
  }

  /**
   * Without a schema: the only table there is, or else the one in the current schema, which is only
   * asked for then (PostgreSQL runs a query for it). Several in other schemas are ambiguous.
   */
  private static List<Location> withoutSchema(
      List<Location> tables, DatabaseMetaData metaData, String table)
      throws AmbiguousTableException {
    Set<String> containers = new LinkedHashSet<>();
    for (Location location : tables) {
      containers.add(String.valueOf(location.container()));
    }
    if (containers.size() == 1) {
      return tables;
    }
    String current = currentContainer(metaData);
    if (current != null) {
      List<Location> exact = new ArrayList<>();
      List<Location> ignoringCase = new ArrayList<>();
      for (Location location : tables) {
        if (current.equals(location.container())) {
          exact.add(location);
        } else if (current.equalsIgnoreCase(location.container())) {
          ignoringCase.add(location);
        }
      }
      if (!exact.isEmpty()) {
        return exact;
      }
      if (!ignoringCase.isEmpty()) {
        return ignoringCase;
      }
    }
    Set<String> names = new LinkedHashSet<>();
    tables.forEach(location -> names.add(location.toString()));
    throw new AmbiguousTableException(table, names);
  }

  /**
   * @return the connection's current schema, or its current catalog on a driver without schemas;
   *     null when the driver can't tell
   */
  static String currentContainer(DatabaseMetaData metaData) {
    Connection connection;
    try {
      connection = metaData.getConnection();
    } catch (SQLException | RuntimeException e) {
      return null;
    }
    if (connection == null) {
      return null;
    }
    String schema = null;
    try {
      schema = connection.getSchema();
    } catch (SQLException | RuntimeException | AbstractMethodError e) {
      // A JDBC 4.0 driver, or one that doesn't know.
    }
    if (!Utils.isEmpty(schema)) {
      return schema;
    }
    try {
      String catalog = connection.getCatalog();
      return Utils.isEmpty(catalog) ? null : catalog;
    } catch (SQLException | RuntimeException e) {
      return null;
    }
  }

  static List<String> variants(String name) {
    if (name == null) {
      List<String> none = new ArrayList<>();
      none.add(null);
      return none;
    }
    Set<String> names = new LinkedHashSet<>();
    names.add(name);
    names.add(name.toUpperCase(Locale.ROOT));
    names.add(name.toLowerCase(Locale.ROOT));
    return new ArrayList<>(names);
  }
}
