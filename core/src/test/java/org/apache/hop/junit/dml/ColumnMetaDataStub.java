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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * {@link DatabaseMetaData} of one table, as a driver reports its columns: enough for {@link
 * org.apache.hop.core.database.dml.DmlColumns#fromTable} to read the cast types without a database.
 */
public final class ColumnMetaDataStub {

  private ColumnMetaDataStub() {}

  /**
   * A column as {@link DatabaseMetaData#getColumns} reports it.
   *
   * @param decimalDigits DECIMAL_DIGITS, or null when the driver reports none
   */
  public record Column(
      String name, String typeName, int dataType, int columnSize, Integer decimalDigits) {}

  /**
   * @param productName what {@link DatabaseMetaData#getDatabaseProductName()} returns
   * @param table the table the columns belong to
   */
  public static DatabaseMetaData of(String productName, String table, List<Column> columns) {
    return (DatabaseMetaData)
        Proxy.newProxyInstance(
            ColumnMetaDataStub.class.getClassLoader(),
            new Class<?>[] {DatabaseMetaData.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "getDatabaseProductName" -> productName;
                  case "getColumns" ->
                      table.equals(args[2])
                          ? resultSet(table, columns)
                          : resultSet(table, List.of());
                  case "toString" -> "metadata of " + productName;
                  default ->
                      throw new UnsupportedOperationException(
                          "Not stubbed: DatabaseMetaData." + method.getName());
                });
  }

  private static ResultSet resultSet(String table, List<Column> columns) {
    Iterator<Column> iterator = columns.iterator();
    Column[] current = new Column[1];
    boolean[] wasNull = new boolean[1];
    return (ResultSet)
        Proxy.newProxyInstance(
            ColumnMetaDataStub.class.getClassLoader(),
            new Class<?>[] {ResultSet.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "next":
                  current[0] = iterator.hasNext() ? iterator.next() : null;
                  return current[0] != null;
                case "close":
                  return null;
                case "wasNull":
                  return wasNull[0];
                case "getString":
                  wasNull[0] = false;
                  return Map.of(
                          "TABLE_NAME", table,
                          "COLUMN_NAME", current[0].name(),
                          "TYPE_NAME", current[0].typeName())
                      .get((String) args[0]);
                case "getInt":
                  Integer value =
                      switch ((String) args[0]) {
                        case "DATA_TYPE" -> current[0].dataType();
                        case "COLUMN_SIZE" -> current[0].columnSize();
                        case "DECIMAL_DIGITS" -> current[0].decimalDigits();
                        default -> throw new IllegalArgumentException((String) args[0]);
                      };
                  wasNull[0] = value == null;
                  return value == null ? 0 : value;
                default:
                  throw new UnsupportedOperationException(
                      "Not stubbed: ResultSet." + method.getName());
              }
            });
  }
}
