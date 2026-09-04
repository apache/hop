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

package org.apache.hop.databases.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The column types Postgres spells its own way. These used to be isPostgresVariant() checks inside
 * ValueMetaJson and ValueMetaInternetAddress, which meant a value type had to know the list of
 * databases.
 */
class PostgreSqlColumnDefinitionTest {

  private DatabaseMeta databaseMeta;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new PostgreSqlDatabaseMeta());
  }

  private String definition(IValueMeta valueMeta) {
    return databaseMeta.getFieldDefinition(valueMeta, null, null, false, true, false);
  }

  @Test
  void jsonIsSpelledJsonb() {
    assertEquals("payload JSONB", definition(new ValueMetaJson("payload")));
  }

  @Test
  void anInternetAddressHasItsOwnColumnType() {
    assertEquals("origin INET", definition(new ValueMetaInternetAddress("origin")));
  }

  @Test
  void theNameAndCarriageReturnFlagsStillApply() {
    assertEquals(
        "JSONB",
        databaseMeta.getFieldDefinition(new ValueMetaJson("p"), null, null, false, false, false));
    assertEquals(
        "p JSONB" + System.lineSeparator(),
        databaseMeta.getFieldDefinition(new ValueMetaJson("p"), null, null, false, true, true));
  }

  @Test
  void ordinaryTypesAreUntouchedByTheDialectRules() {
    ValueMetaString column = new ValueMetaString("name");
    column.setLength(20);
    assertEquals("name VARCHAR(20)", definition(column));
  }

  private IValueMeta integer(int length) {
    ValueMetaInteger column = new ValueMetaInteger("id");
    column.setLength(length);
    return column;
  }

  /**
   * Issue #4174. A Hop Integer is a 64 bit Long whatever its length says, so an unstated length
   * means the full width. It used to mean DOUBLE PRECISION, which cannot hold a large Long exactly.
   */
  @Test
  void anIntegerWithNoLengthIsWrittenAsBigint() {
    assertEquals("id BIGINT", definition(integer(-1)));
    assertEquals("id BIGINT", definition(integer(0)));
  }

  @Test
  void anIntegerWithALengthKeepsTheSizeItAskedFor() {
    assertEquals("id SMALLINT", definition(integer(4)));
    assertEquals("id INTEGER", definition(integer(9)));
    assertEquals("id BIGINT", definition(integer(15)));
    assertEquals("id BIGINT", definition(integer(18)));
    assertEquals("id NUMERIC(20, 0)", definition(integer(20)));
  }

  /** A key column has its own spelling, and the unsized integer rule must not take it over. */
  @Test
  void aKeyColumnIsStillASerial() {
    assertEquals(
        "id BIGSERIAL",
        databaseMeta.getFieldDefinition(integer(-1), "id", null, false, true, false));
    assertEquals(
        "id BIGSERIAL",
        databaseMeta.getFieldDefinition(integer(-1), null, "id", false, true, false));
  }

  /** Only Integer changes. A Number with no length really is a floating point value. */
  @Test
  void anUnsizedNumberIsStillDoublePrecision() {
    assertEquals("rate DOUBLE PRECISION", definition(new ValueMetaNumber("rate")));
  }

  /**
   * PostgreSQL refuses a declared precision above 1000, so the CLOB_LENGTH marker used to generate
   * DDL the server rejects outright: NUMERIC(1073741824, 0). Unbounded is what it meant, and
   * unbounded is what an unconstrained NUMERIC is.
   */
  @Test
  void aNumericTooWideToDeclareIsLeftUnconstrained() {
    assertEquals("id NUMERIC", definition(integer(DatabaseMeta.CLOB_LENGTH)));

    ValueMetaBigNumber amount = new ValueMetaBigNumber("amount");
    amount.setLength(DatabaseMeta.CLOB_LENGTH);
    assertEquals("amount NUMERIC", definition(amount));
  }
}
