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

package org.apache.hop.core.database.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSetMetaData;
import java.sql.Types;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DatabaseColumnTest {

  @BeforeAll
  static void setUp() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void calculatesDefinitionsForStandardTypes() {
    // Exact examples from issue #8464:
    // varchar(100), bigint, int identity, bool, float, timestamp(6)
    assertEquals("varchar(100)", DatabaseColumn.calculateDefinition("varchar", 100, 0, 100));
    assertEquals("VARCHAR(50)", DatabaseColumn.calculateDefinition("VARCHAR", 50, 0, 50));
    assertEquals("bigint", DatabaseColumn.calculateDefinition("bigint", 19, 0, 15));
    assertEquals("int identity", DatabaseColumn.calculateDefinition("int identity", 10, 0, 9));
    assertEquals("bool", DatabaseColumn.calculateDefinition("bool", 1, 0, -1));
    assertEquals("float", DatabaseColumn.calculateDefinition("float", 53, 0, -1));
    assertEquals("timestamp(6)", DatabaseColumn.calculateDefinition("timestamp", 26, 6, 6));
  }

  @Test
  void calculatesNumericAndDecimalDefinitions() {
    assertEquals("numeric(10, 2)", DatabaseColumn.calculateDefinition("numeric", 10, 2, 10));
    assertEquals("DECIMAL(12, 4)", DatabaseColumn.calculateDefinition("DECIMAL", 12, 4, 12));
    assertEquals("NUMBER(10)", DatabaseColumn.calculateDefinition("NUMBER", 10, 0, 10));
    assertEquals("numeric(50)", DatabaseColumn.calculateDefinition("numeric", 50, 0, 50));
    assertEquals("numeric(50, 2)", DatabaseColumn.calculateDefinition("numeric", 50, 2, 50));
    assertEquals("numeric", DatabaseColumn.calculateDefinition("numeric", 0, 0, -1));
  }

  @Test
  void calculatesDateTimeDefinitions() {
    assertEquals("datetime2(7)", DatabaseColumn.calculateDefinition("datetime2", 27, 7, 7));
    assertEquals("time(3)", DatabaseColumn.calculateDefinition("time", 12, 3, 3));
    assertEquals("timestamptz(6)", DatabaseColumn.calculateDefinition("timestamptz", 29, 6, 6));
    assertEquals(
        "timestamp(6) without time zone",
        DatabaseColumn.calculateDefinition("timestamp without time zone", 29, 6, 6));
    assertEquals(
        "timestamp(6) with local time zone",
        DatabaseColumn.calculateDefinition("timestamp with local time zone", 29, 6, 6));
    assertEquals(
        "TIMESTAMP(6) WITH LOCAL TIME ZONE",
        DatabaseColumn.calculateDefinition("TIMESTAMP WITH LOCAL TIME ZONE", 29, 6, 6));
    assertEquals("timestamp", DatabaseColumn.calculateDefinition("timestamp", 19, 0, 0));
    assertEquals("date", DatabaseColumn.calculateDefinition("date", 10, 0, -1));
    assertEquals("datetime", DatabaseColumn.calculateDefinition("datetime", 23, 3, 3));
    assertEquals("smalldatetime", DatabaseColumn.calculateDefinition("smalldatetime", 16, 0, 0));
  }

  @Test
  void calculatesSizedCharacterAndBinaryDefinitions() {
    assertEquals("char(10)", DatabaseColumn.calculateDefinition("char", 10, 0, 10));
    assertEquals("nvarchar(50)", DatabaseColumn.calculateDefinition("nvarchar", 50, 0, 50));
    assertEquals("varbinary(64)", DatabaseColumn.calculateDefinition("varbinary", 64, 0, 64));
    assertEquals("binary(16)", DatabaseColumn.calculateDefinition("binary", 16, 0, 16));
    assertEquals("bit", DatabaseColumn.calculateDefinition("bit", 1, 0, 1));
    assertEquals("bit", DatabaseColumn.calculateDefinition("bit", 0, 0, -1));
    assertEquals("bit(8)", DatabaseColumn.calculateDefinition("bit", 8, 0, 8));
    assertEquals("varbit(16)", DatabaseColumn.calculateDefinition("varbit", 16, 0, 16));
  }

  @Test
  void preservesUnsizedAndComplexDefinitions() {
    assertEquals("text", DatabaseColumn.calculateDefinition("text", 2147483647, 0, -1));
    assertEquals("clob", DatabaseColumn.calculateDefinition("clob", 2147483647, 0, -1));
    assertEquals("blob", DatabaseColumn.calculateDefinition("blob", 2147483647, 0, -1));
    assertEquals("json", DatabaseColumn.calculateDefinition("json", 0, 0, -1));
    assertEquals("jsonb", DatabaseColumn.calculateDefinition("jsonb", 0, 0, -1));
    assertEquals("uuid", DatabaseColumn.calculateDefinition("uuid", 0, 0, -1));
    assertEquals(
        "uniqueidentifier", DatabaseColumn.calculateDefinition("uniqueidentifier", 36, 0, 36));
    assertEquals("money", DatabaseColumn.calculateDefinition("money", 19, 4, 19));
    assertEquals("smallmoney", DatabaseColumn.calculateDefinition("smallmoney", 10, 4, 10));
    assertEquals("varchar(100)", DatabaseColumn.calculateDefinition("varchar(100)", 100, 0, 100));
    assertEquals("enum('a', 'b')", DatabaseColumn.calculateDefinition("enum('a', 'b')", 0, 0, -1));
  }

  @Test
  void calculatesDefinitionFromDatabaseColumnInstance() {
    DatabaseColumn col = DatabaseColumn.of("c_name", Types.VARCHAR, "varchar", 100, 0, 100);
    assertEquals("varchar(100)", col.getDefinition());
    assertFalse(col.isAutoIncrement());

    DatabaseColumn colAi = DatabaseColumn.of("id", Types.INTEGER, "int", 10, 0, 9, true);
    assertEquals("int", colAi.getDefinition());
    assertTrue(colAi.isAutoIncrement());

    DatabaseColumn colIdentity =
        DatabaseColumn.of("id", Types.INTEGER, "int identity", 10, 0, 9, true);
    assertEquals("int identity", colIdentity.getDefinition());
  }

  @Test
  void calculatesDefinitionFromValueMeta() {
    IValueMeta vmString = new ValueMetaString("name");
    vmString.setOriginalColumnTypeName("varchar");
    vmString.setOriginalColumnType(Types.VARCHAR);
    vmString.setLength(100);
    assertEquals("varchar(100)", DatabaseColumn.calculateDefinition(vmString));

    IValueMeta vmInt = new ValueMetaInteger("id");
    vmInt.setOriginalColumnTypeName("bigint");
    vmInt.setOriginalColumnType(Types.BIGINT);
    vmInt.setLength(15);
    assertEquals("bigint", DatabaseColumn.calculateDefinition(vmInt));

    IValueMeta vmNumeric = new ValueMetaNumber("amount");
    vmNumeric.setOriginalColumnTypeName("numeric");
    vmNumeric.setOriginalColumnType(Types.NUMERIC);
    vmNumeric.setLength(10);
    vmNumeric.setPrecision(2);
    assertEquals("numeric(10, 2)", DatabaseColumn.calculateDefinition(vmNumeric));

    IValueMeta vmTs = new ValueMetaTimestamp("ts");
    vmTs.setOriginalColumnTypeName("timestamp");
    vmTs.setOriginalColumnType(Types.TIMESTAMP);
    vmTs.setLength(6);
    assertEquals("timestamp(6)", DatabaseColumn.calculateDefinition(vmTs));

    // Fallback to Hop type description when originalColumnTypeName is null
    IValueMeta vmFallback = new ValueMetaString("raw_field");
    assertEquals("String", DatabaseColumn.calculateDefinition(vmFallback));
  }

  @Test
  void aColumnWithAnotherSqlTypeKeepsEverythingElse() throws Exception {
    ResultSetMetaData rm = mock(ResultSetMetaData.class);
    when(rm.getColumnName(2)).thenReturn("id");
    when(rm.getColumnLabel(2)).thenReturn("id");
    when(rm.getTableName(2)).thenReturn("producttype");
    when(rm.getColumnType(2)).thenReturn(Types.INTEGER);
    when(rm.getColumnTypeName(2)).thenReturn("DECIMAL");
    when(rm.getPrecision(2)).thenReturn(10);
    when(rm.getScale(2)).thenReturn(2);
    when(rm.getColumnDisplaySize(2)).thenReturn(12);
    DatabaseColumn reported = DatabaseColumn.of(rm, 2);

    DatabaseColumn corrected = reported.withSqlType(Types.DECIMAL);

    assertEquals(Types.DECIMAL, corrected.getSqlType());
    assertEquals(Types.INTEGER, reported.getSqlType());
    assertEquals("id", corrected.getName());
    assertEquals("producttype", corrected.getTableName());
    assertEquals("DECIMAL", corrected.getNativeTypeName());
    assertEquals(10, corrected.getPrecision());
    assertEquals(2, corrected.getScale());
    assertEquals(12, corrected.getDisplaySize());
    assertSame(rm, corrected.getResultSetMetaData());
    assertEquals(2, corrected.getColumnIndex());
  }
}
