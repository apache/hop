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

package org.apache.hop.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

class CalciteSqlFormatterTest {

  @Test
  void formatsSelectListOntoSeparateLines() throws Exception {
    String formatted = CalciteSqlFormatter.format("select a,b,c from customers where id=1");
    assertTrue(formatted.contains("SELECT"));
    assertTrue(formatted.contains("FROM"));
    assertTrue(formatted.contains("WHERE"));
    assertTrue(formatted.contains("\n"));
    assertFalse(formatted.equalsIgnoreCase("select a,b,c from customers where id=1"));
  }

  @Test
  void preservesHopVariables() throws Exception {
    String formatted = CalciteSqlFormatter.format("select * from ${SOURCE_TABLE} where id = ${ID}");
    assertTrue(formatted.contains("${SOURCE_TABLE}"));
    assertTrue(formatted.contains("${ID}"));
    assertFalse(formatted.contains("HOPVAR"));
  }

  @Test
  void formatsCreateTable() throws Exception {
    String formatted =
        CalciteSqlFormatter.format("create table demo (id int not null, name varchar(40))");
    assertTrue(formatted.toUpperCase().contains("CREATE TABLE"));
    assertTrue(
        formatted.toUpperCase().contains("INTEGER") || formatted.toUpperCase().contains("INT"));
  }

  @Test
  void formatsMultipleStatements() throws Exception {
    String formatted = CalciteSqlFormatter.format("select 1 from dual; select 2 from dual;");
    assertTrue(formatted.contains("SELECT"));
    long selects = formatted.toUpperCase().split("SELECT", -1).length - 1;
    assertEquals(2, selects);
  }

  @Test
  void formatsQueryableStatementNextToUnparseable() throws Exception {
    String formatted = CalciteSqlFormatter.format("select a from t; this is not sql at all;");
    assertTrue(formatted.toUpperCase().contains("SELECT"));
    assertTrue(formatted.contains("this is not sql at all"));
  }

  @Test
  void rejectsGarbage() {
    assertThrows(SqlParseException.class, () -> CalciteSqlFormatter.format("definitely not sql"));
  }

  @Test
  void blankInputIsUnchanged() throws Exception {
    assertEquals("  ", CalciteSqlFormatter.format("  "));
    assertEquals("", CalciteSqlFormatter.format(""));
  }

  @Test
  void mysqlStyleUsesBackticksWhenRequested() throws Exception {
    String formatted = CalciteSqlFormatter.format("select `name` from `customers`", "MYSQL");
    assertTrue(formatted.contains("SELECT"));
    assertTrue(formatted.contains("`name`") || formatted.contains("name"));
  }

  @Test
  void formatsDoubleQuotedPostgresIdentifiers() throws Exception {
    String sql = "SELECT * FROM \"public\".customer_address limit 1000";
    String formatted = CalciteSqlFormatter.format(sql, "POSTGRESQL");
    assertTrue(formatted.toUpperCase().contains("CUSTOMER_ADDRESS"));
    assertTrue(formatted.contains("public") || formatted.contains("PUBLIC"));
  }

  @Test
  void formatsDoubleQuotedIdentifiersWithoutPluginId() throws Exception {
    String sql = "SELECT * FROM \"public\".customer_address limit 1000";
    String formatted = CalciteSqlFormatter.format(sql);
    assertTrue(formatted.toUpperCase().contains("CUSTOMER_ADDRESS"));
  }

  @Test
  void formatsDoubleQuotedIdentifiersWhenDialectLexUsesBackticks() throws Exception {
    String formatted =
        CalciteSqlFormatter.format("SELECT * FROM \"public\".customer_address limit 1000", "H2");
    assertTrue(formatted.toUpperCase().contains("CUSTOMER_ADDRESS"));
  }

  @Test
  void splitStatementsSkipsSemicolonInQuotes() {
    List<String> parts =
        CalciteSqlFormatter.splitStatements("select 'a;b' from t; select 2 from t");
    assertEquals(2, parts.size());
    assertTrue(parts.get(0).contains("'a;b'"));
  }
}
