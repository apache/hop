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

package org.apache.hop.pipeline.transforms.tableinput;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TableInputSqlTest {
  @BeforeAll
  static void setUpClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void parseNamedParametersInOrder() throws Exception {
    TableInputSql.Parsed parsed =
        TableInputSql.parse(
            "SELECT * FROM t WHERE id = {key} AND changed >= {fromDate} AND changed < {toDate}");

    assertEquals(
        "SELECT * FROM t WHERE id = ? AND changed >= ? AND changed < ?", parsed.getJdbcSql());
    assertEquals(3, parsed.getNamedParameters().size());
    assertEquals("key", parsed.getNamedParameters().get(0));
    assertEquals("fromDate", parsed.getNamedParameters().get(1));
    assertEquals("toDate", parsed.getNamedParameters().get(2));
    assertEquals(0, parsed.getPositionalParameterCount());
    assertTrue(parsed.hasNamedParameters());
  }

  @Test
  void parseRepeatedNamedParameter() throws Exception {
    TableInputSql.Parsed parsed =
        TableInputSql.parse("SELECT * FROM t WHERE dt >= {fromDate} AND dt <= {fromDate}");

    assertEquals("SELECT * FROM t WHERE dt >= ? AND dt <= ?", parsed.getJdbcSql());
    assertEquals(java.util.List.of("fromDate", "fromDate"), parsed.getNamedParameters());
  }

  @Test
  void parseLeavesHopVariablesUntouched() throws Exception {
    TableInputSql.Parsed parsed =
        TableInputSql.parse("SELECT * FROM t WHERE id = {key} AND name = '${VAR}'");

    assertEquals("SELECT * FROM t WHERE id = ? AND name = '${VAR}'", parsed.getJdbcSql());
    assertEquals(java.util.List.of("key"), parsed.getNamedParameters());
  }

  @Test
  void parseIgnoresBracesInsideQuotes() throws Exception {
    TableInputSql.Parsed parsed =
        TableInputSql.parse("SELECT '{key}' AS literal, col FROM t WHERE id = {key}");

    assertEquals("SELECT '{key}' AS literal, col FROM t WHERE id = ?", parsed.getJdbcSql());
    assertEquals(java.util.List.of("key"), parsed.getNamedParameters());
  }

  @Test
  void parseIgnoresNamedParameterInComments() throws Exception {
    TableInputSql.Parsed parsed =
        TableInputSql.parse("SELECT * FROM t -- {ignored}\nWHERE id = {key} /* {also} */");

    assertEquals("SELECT * FROM t -- {ignored}\nWHERE id = ? /* {also} */", parsed.getJdbcSql());
    assertEquals(java.util.List.of("key"), parsed.getNamedParameters());
  }

  @Test
  void parsePositionalQuestionMarks() throws Exception {
    TableInputSql.Parsed parsed = TableInputSql.parse("SELECT * FROM t WHERE id = ? AND name = ?");

    assertEquals("SELECT * FROM t WHERE id = ? AND name = ?", parsed.getJdbcSql());
    assertFalse(parsed.hasNamedParameters());
    assertEquals(2, parsed.getPositionalParameterCount());
  }

  @Test
  void parseMixedPlaceholdersThrows() {
    HopException exception =
        assertThrows(
            HopException.class,
            () -> TableInputSql.parse("SELECT * FROM t WHERE id = {key} AND x = ?"));
    assertTrue(exception.getMessage().toLowerCase().contains("mix"));
  }

  @Test
  void parseUnclosedNamedParameterThrows() {
    assertThrows(HopException.class, () -> TableInputSql.parse("SELECT * FROM t WHERE id = {key"));
  }

  @Test
  void bindUsesNamedFieldValuesNotStreamOrder() throws Exception {
    IRowMeta incoming = new RowMeta();
    incoming.addValueMeta(new ValueMetaString("ignored"));
    incoming.addValueMeta(new ValueMetaInteger("fromDate"));
    incoming.addValueMeta(new ValueMetaString("key"));
    Object[] row = new Object[] {"unused", 42L, "10"};

    TableInputSql.Bound bound =
        TableInputSql.prepare(
            "SELECT * FROM t WHERE key = {key} AND dt >= {fromDate}", incoming, row);

    assertEquals("SELECT * FROM t WHERE key = ? AND dt >= ?", bound.getJdbcSql());
    assertEquals(2, bound.getParameterMeta().size());
    assertEquals("key", bound.getParameterMeta().getValueMeta(0).getName());
    assertEquals("fromDate", bound.getParameterMeta().getValueMeta(1).getName());
    assertArrayEquals(new Object[] {"10", 42L}, bound.getParameterData());
  }

  @Test
  void bindMissingFieldThrows() {
    IRowMeta incoming = new RowMeta();
    incoming.addValueMeta(new ValueMetaString("other"));
    assertThrows(
        HopException.class,
        () ->
            TableInputSql.prepare(
                "SELECT * FROM t WHERE id = {key}", incoming, new Object[] {"x"}));
  }

  @Test
  void bindIgnoresUnusedIncomingFieldsForPositionalSql() throws Exception {
    IRowMeta incoming = new RowMeta();
    incoming.addValueMeta(new ValueMetaString("key"));
    Object[] row = new Object[] {"10"};

    TableInputSql.Bound bound =
        TableInputSql.prepare("SELECT * FROM t WHERE id = ?", incoming, row);

    assertEquals("SELECT * FROM t WHERE id = ?", bound.getJdbcSql());
    assertEquals(incoming, bound.getParameterMeta());
    assertArrayEquals(row, bound.getParameterData());
  }

  @Test
  void prepareDisabledLeavesBracesLiteral() throws Exception {
    IRowMeta incoming = new RowMeta();
    incoming.addValueMeta(new ValueMetaString("key"));
    Object[] row = new Object[] {"10"};
    String sql = "SELECT * FROM t WHERE json = '{key}' OR id = {key}";

    TableInputSql.Bound bound = TableInputSql.prepare(false, sql, incoming, row);

    assertEquals(sql, bound.getJdbcSql());
    assertEquals(incoming, bound.getParameterMeta());
    assertArrayEquals(row, bound.getParameterData());
  }

  @Test
  void parseAllowsSpacesInFieldName() throws Exception {
    TableInputSql.Parsed parsed = TableInputSql.parse("SELECT * FROM t WHERE id = {from date}");
    assertEquals(java.util.List.of("from date"), parsed.getNamedParameters());
  }
}
