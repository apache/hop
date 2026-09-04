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

package org.apache.hop.databases.mssql;

import static org.apache.hop.junit.database.TypeRuleFixture.column;
import static org.apache.hop.junit.database.TypeRuleFixture.meta;
import static org.apache.hop.junit.database.TypeRuleFixture.numericColumn;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Types;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.types.ColumnContext;
import org.apache.hop.core.database.types.DatabaseTypeMapper;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** SQL Server write rules for date/time and Unicode string round-trips. */
class MsSqlServerTypeRulesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void dateColumnsStayDate() throws Exception {
    assertEquals("DATE", write(column(Types.DATE, "date", 0, 10)));
  }

  @Test
  void timeColumnsStayTime() throws Exception {
    assertEquals("TIME", write(column(Types.TIME, "time", 0, 16)));
  }

  @Test
  void datetime2IsPreserved() throws Exception {
    IValueMeta valueMeta = new ValueMetaTimestamp("COL");
    valueMeta.setOriginalColumnTypeName("datetime2");
    assertEquals("DATETIME2", write(valueMeta));
  }

  @Test
  void aPlainDateStillWritesDatetime() {
    assertEquals("DATETIME", write(new ValueMetaDate("COL")));
  }

  @Test
  void aPlainTimestampWritesDatetime2() {
    assertEquals("DATETIME2", write(new ValueMetaTimestamp("COL")));
  }

  @Test
  void nvarcharRoundTrips() throws Exception {
    assertEquals("NVARCHAR(20)", write(column(Types.NVARCHAR, "nvarchar", 20, 20)));
  }

  @Test
  void ncharRoundTrips() throws Exception {
    assertEquals("NCHAR(10)", write(column(Types.NCHAR, "nchar", 10, 10)));
  }

  @Test
  void longNationalStringsUseNvarcharMax() throws Exception {
    assertEquals("NVARCHAR(MAX)", write(column(Types.NVARCHAR, "nvarchar", 8000, 8000)));
  }

  @Test
  void longAnsiStringsUseVarcharMax() {
    IValueMeta valueMeta = new ValueMetaString("COL", 10_000, 0);
    assertEquals("VARCHAR(MAX)", write(valueMeta));
  }

  @Test
  void aPlainStringStillWritesVarchar() {
    IValueMeta valueMeta = new ValueMetaString("COL", 50, 0);
    assertEquals("VARCHAR(50)", write(valueMeta));
  }

  @Test
  void aScaledDecimalRoundTripsAsTheSameDeclaration() throws Exception {
    assertEquals("DECIMAL(5,2)", numericRoundTrip(5, 2));
    assertEquals("DECIMAL(24,15)", numericRoundTrip(24, 15));
    assertEquals("DECIMAL(16,1)", numericRoundTrip(16, 1));
  }

  private static String numericRoundTrip(int precision, int scale) throws Exception {
    MsSqlServerDatabaseMeta dialect = new MsSqlServerDatabaseMeta();
    IValueMeta valueMeta =
        DatabaseTypeMapper.getValueMeta(
            new Variables(),
            meta(dialect),
            numericColumn(Types.DECIMAL, "decimal", precision, scale),
            false,
            false);
    return dialect.getFieldDefinition(valueMeta, null, null, false, false, false);
  }

  private static String write(org.apache.hop.core.database.types.DatabaseColumn column)
      throws Exception {
    MsSqlServerDatabaseMeta dialect = new MsSqlServerDatabaseMeta();
    IValueMeta valueMeta =
        DatabaseTypeMapper.getValueMeta(new Variables(), meta(dialect), column, false, false);
    return write(valueMeta);
  }

  private static String write(IValueMeta valueMeta) {
    return new MsSqlServerDatabaseMeta()
        .getColumnDefinition(
            valueMeta, null, null, false, false, false, ColumnContext.Purpose.CREATE);
  }
}
