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
package org.apache.hop.junit.database;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.types.DatabaseColumn;

/**
 * Builds the JDBC metadata a dialect's type rules are matched against, so that each database plugin
 * can check its own rules against a real dialect instance rather than a mocked one.
 */
public final class TypeRuleFixture {

  private TypeRuleFixture() {
    // Utility class.
  }

  /** A connection on the given dialect, with no connection properties and no special shapes. */
  public static DatabaseMeta meta(IDatabase dialect) {
    return meta(dialect, new Properties(), false);
  }

  /** A connection on the given dialect. */
  public static DatabaseMeta meta(
      IDatabase dialect, Properties connectionProperties, boolean displaySizeTwiceThePrecision) {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    when(meta.getIDatabase()).thenReturn(dialect);
    when(meta.getConnectionProperties(any())).thenReturn(connectionProperties);
    when(meta.isDisplaySizeTwiceThePrecision()).thenReturn(displaySizeTwiceThePrecision);
    when(meta.supportsTimestampDataType()).thenReturn(true);
    return meta;
  }

  /** Connection properties holding a single entry. */
  public static Properties properties(String key, String value) {
    Properties properties = new Properties();
    properties.setProperty(key, value);
    return properties;
  }

  /** A numeric column, described the way a driver would report it on a result set. */
  public static DatabaseColumn numericColumn(
      int sqlType, String nativeTypeName, int precision, int scale) throws SQLException {
    ResultSetMetaData rm = mock(ResultSetMetaData.class);
    when(rm.getColumnName(1)).thenReturn("COL");
    when(rm.getColumnLabel(1)).thenReturn("COL");
    when(rm.getColumnType(1)).thenReturn(sqlType);
    when(rm.getColumnTypeName(1)).thenReturn(nativeTypeName);
    when(rm.getPrecision(1)).thenReturn(precision);
    when(rm.getScale(1)).thenReturn(scale);
    when(rm.getColumnDisplaySize(1)).thenReturn(precision);
    when(rm.isSigned(1)).thenReturn(true);
    return DatabaseColumn.of(rm, 1);
  }

  /** A column, described the way a driver would report it on a result set. */
  public static DatabaseColumn column(
      int sqlType, String nativeTypeName, int precision, int displaySize) throws SQLException {
    ResultSetMetaData rm = mock(ResultSetMetaData.class);
    when(rm.getColumnName(1)).thenReturn("COL");
    when(rm.getColumnLabel(1)).thenReturn("COL");
    when(rm.getColumnType(1)).thenReturn(sqlType);
    when(rm.getColumnTypeName(1)).thenReturn(nativeTypeName);
    when(rm.getPrecision(1)).thenReturn(precision);
    when(rm.getScale(1)).thenReturn(0);
    when(rm.getColumnDisplaySize(1)).thenReturn(displaySize);
    when(rm.isSigned(1)).thenReturn(true);
    return DatabaseColumn.of(rm, 1);
  }
}
