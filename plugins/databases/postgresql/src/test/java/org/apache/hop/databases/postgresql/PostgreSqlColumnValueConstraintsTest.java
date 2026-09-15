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
package org.apache.hop.databases.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Types;
import org.apache.hop.core.database.types.DatabaseColumn;
import org.apache.hop.core.database.validation.ColumnValueConstraints;
import org.apache.hop.core.database.validation.StringLengthUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PostgreSqlColumnValueConstraintsTest {

  private PostgreSqlDatabaseMeta postgres;

  @BeforeEach
  void setUp() {
    postgres = new PostgreSqlDatabaseMeta();
  }

  @Test
  void varcharRejectsNulAndCountsCharacters() {
    ColumnValueConstraints spec = enrich("email", Types.VARCHAR, "varchar", 255, 0);
    assertTrue(spec.isRejectNulChar());
    assertEquals(StringLengthUnit.CHARACTERS, spec.getLengthUnit());
    assertEquals(255, spec.getStringMaxLength());
  }

  @Test
  void textHasNoLengthLimit() {
    ColumnValueConstraints spec = enrich("body", Types.VARCHAR, "text", 2147483647, 0);
    assertEquals(-1, spec.getStringMaxLength());
    assertTrue(spec.isRejectNulChar());
  }

  @Test
  void uuidAndJsonFlags() {
    assertTrue(enrich("id", Types.OTHER, "uuid", 0, 0).isUuid());
    assertTrue(enrich("doc", Types.OTHER, "jsonb", 0, 0).isJson());
    assertTrue(enrich("doc", Types.OTHER, "json", 0, 0).isJson());
  }

  @Test
  void integerRangesFromNativeType() {
    ColumnValueConstraints int2 = enrich("s", Types.SMALLINT, "int2", 0, 0);
    assertEquals(-32768L, int2.getIntegerMin());
    assertEquals(32767L, int2.getIntegerMax());

    ColumnValueConstraints int4 = enrich("i", Types.INTEGER, "int4", 0, 0);
    assertEquals((long) Integer.MIN_VALUE, int4.getIntegerMin());
    assertEquals((long) Integer.MAX_VALUE, int4.getIntegerMax());

    ColumnValueConstraints serial = enrich("id", Types.INTEGER, "serial", 0, 0);
    assertEquals((long) Integer.MAX_VALUE, serial.getIntegerMax());
  }

  @Test
  void unlimitedNumericClearsPrecision() {
    ColumnValueConstraints spec = enrich("n", Types.NUMERIC, "numeric", 0, 0);
    assertEquals(-1, spec.getNumericPrecision());
  }

  @Test
  void numericKeepsDeclaredPrecision() {
    ColumnValueConstraints spec = enrich("n", Types.NUMERIC, "numeric", 5, 2);
    assertEquals(5, spec.getNumericPrecision());
    assertEquals(2, spec.getNumericScale());
    assertFalse(spec.isUuid());
  }

  private ColumnValueConstraints enrich(
      String name, int sqlType, String nativeType, int precision, int scale) {
    ColumnValueConstraints spec = new ColumnValueConstraints();
    spec.setColumnName(name);
    spec.setNativeTypeName(nativeType);
    postgres.enrichColumnValueConstraints(
        spec, DatabaseColumn.of(name, sqlType, nativeType, precision, scale, precision), "UTF8");
    return spec;
  }
}
