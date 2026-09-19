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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    assertEquals(
        "varchar(100)",
        DatabaseColumn.calculateDefinition("varchar", Types.VARCHAR, 100, 0, 100, false));
    assertEquals(
        "VARCHAR(50)",
        DatabaseColumn.calculateDefinition("VARCHAR", Types.VARCHAR, 50, 0, 50, false));
    assertEquals(
        "bigint", DatabaseColumn.calculateDefinition("bigint", Types.BIGINT, 19, 0, 15, false));
    assertEquals(
        "int identity",
        DatabaseColumn.calculateDefinition("int identity", Types.INTEGER, 10, 0, 9, true));
    assertEquals(
        "bool", DatabaseColumn.calculateDefinition("bool", Types.BOOLEAN, 1, 0, -1, false));
    assertEquals(
        "float", DatabaseColumn.calculateDefinition("float", Types.FLOAT, 53, 0, -1, false));
    assertEquals(
        "timestamp(6)",
        DatabaseColumn.calculateDefinition("timestamp", Types.TIMESTAMP, 26, 6, 6, false));
  }

  @Test
  void calculatesNumericAndDecimalDefinitions() {
    assertEquals(
        "numeric(10, 2)",
        DatabaseColumn.calculateDefinition("numeric", Types.NUMERIC, 10, 2, 10, false));
    assertEquals(
        "DECIMAL(12, 4)",
        DatabaseColumn.calculateDefinition("DECIMAL", Types.DECIMAL, 12, 4, 12, false));
    assertEquals(
        "NUMBER(10)",
        DatabaseColumn.calculateDefinition("NUMBER", Types.NUMERIC, 10, 0, 10, false));
    assertEquals(
        "numeric", DatabaseColumn.calculateDefinition("numeric", Types.NUMERIC, 0, 0, -1, false));
  }

  @Test
  void calculatesDateTimeDefinitions() {
    assertEquals(
        "datetime2(7)",
        DatabaseColumn.calculateDefinition("datetime2", Types.TIMESTAMP, 27, 7, 7, false));
    assertEquals(
        "time(3)", DatabaseColumn.calculateDefinition("time", Types.TIME, 12, 3, 3, false));
    assertEquals(
        "timestamptz(6)",
        DatabaseColumn.calculateDefinition("timestamptz", Types.TIMESTAMP, 29, 6, 6, false));
    assertEquals(
        "timestamp(6) without time zone",
        DatabaseColumn.calculateDefinition(
            "timestamp without time zone", Types.TIMESTAMP, 29, 6, 6, false));
    assertEquals(
        "timestamp(6) with local time zone",
        DatabaseColumn.calculateDefinition(
            "timestamp with local time zone", Types.TIMESTAMP, 29, 6, 6, false));
    assertEquals(
        "TIMESTAMP(6) WITH LOCAL TIME ZONE",
        DatabaseColumn.calculateDefinition(
            "TIMESTAMP WITH LOCAL TIME ZONE", Types.TIMESTAMP, 29, 6, 6, false));
    assertEquals(
        "timestamp",
        DatabaseColumn.calculateDefinition("timestamp", Types.TIMESTAMP, 19, 0, 0, false));
    assertEquals("date", DatabaseColumn.calculateDefinition("date", Types.DATE, 10, 0, -1, false));
    assertEquals(
        "datetime",
        DatabaseColumn.calculateDefinition("datetime", Types.TIMESTAMP, 23, 3, 3, false));
    assertEquals(
        "smalldatetime",
        DatabaseColumn.calculateDefinition("smalldatetime", Types.TIMESTAMP, 16, 0, 0, false));
  }

  @Test
  void calculatesSizedCharacterAndBinaryDefinitions() {
    assertEquals(
        "char(10)", DatabaseColumn.calculateDefinition("char", Types.CHAR, 10, 0, 10, false));
    assertEquals(
        "nvarchar(50)",
        DatabaseColumn.calculateDefinition("nvarchar", Types.NVARCHAR, 50, 0, 50, false));
    assertEquals(
        "varbinary(64)",
        DatabaseColumn.calculateDefinition("varbinary", Types.VARBINARY, 64, 0, 64, false));
    assertEquals(
        "binary(16)", DatabaseColumn.calculateDefinition("binary", Types.BINARY, 16, 0, 16, false));
  }

  @Test
  void preservesUnsizedAndComplexDefinitions() {
    assertEquals(
        "text",
        DatabaseColumn.calculateDefinition("text", Types.VARCHAR, 2147483647, 0, -1, false));
    assertEquals(
        "clob", DatabaseColumn.calculateDefinition("clob", Types.CLOB, 2147483647, 0, -1, false));
    assertEquals(
        "blob", DatabaseColumn.calculateDefinition("blob", Types.BLOB, 2147483647, 0, -1, false));
    assertEquals("json", DatabaseColumn.calculateDefinition("json", Types.OTHER, 0, 0, -1, false));
    assertEquals(
        "jsonb", DatabaseColumn.calculateDefinition("jsonb", Types.OTHER, 0, 0, -1, false));
    assertEquals("uuid", DatabaseColumn.calculateDefinition("uuid", Types.OTHER, 0, 0, -1, false));
    assertEquals(
        "uniqueidentifier",
        DatabaseColumn.calculateDefinition("uniqueidentifier", Types.CHAR, 36, 0, 36, false));
    assertEquals(
        "money", DatabaseColumn.calculateDefinition("money", Types.DECIMAL, 19, 4, 19, false));
    assertEquals(
        "smallmoney",
        DatabaseColumn.calculateDefinition("smallmoney", Types.DECIMAL, 10, 4, 10, false));
    assertEquals(
        "varchar(100)",
        DatabaseColumn.calculateDefinition("varchar(100)", Types.VARCHAR, 100, 0, 100, false));
    assertEquals(
        "enum('a', 'b')",
        DatabaseColumn.calculateDefinition("enum('a', 'b')", Types.VARCHAR, 0, 0, -1, false));
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
}
