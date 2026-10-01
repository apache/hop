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

package org.apache.hop.lineage.openlineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.lineage.LineageRelationalIdentity;
import org.junit.jupiter.api.Test;

/**
 * Exercises the real (native) OpenLineage SQL parser so a regression in the extraction path — or a
 * broken native binary on the build platform — is caught. Asserts on the contract dataset name so
 * the tables the parser yields stitch with the {@code database.schema.table} identity a writer
 * emits.
 */
class RelationalSqlParserTest {

  @Test
  void parsesQualifiedSourceTableFromSelect() {
    RelationalSqlParser.Tables tables =
        RelationalSqlParser.create(true, null)
            .parse("SELECT id, name FROM analytics.staging.orders", "postgres");

    assertEquals(1, tables.inputs().size());
    assertEquals(
        "analytics.staging.orders", LineageRelationalIdentity.datasetName(tables.inputs().get(0)));
    assertTrue(tables.outputs().isEmpty());
  }

  @Test
  void parsesBothSidesOfInsertSelect() {
    RelationalSqlParser.Tables tables =
        RelationalSqlParser.create(true, null)
            .parse(
                "INSERT INTO analytics.marts.daily_orders "
                    + "SELECT * FROM analytics.staging.orders",
                "postgres");

    assertEquals(
        "analytics.staging.orders", LineageRelationalIdentity.datasetName(tables.inputs().get(0)));
    assertEquals(
        "analytics.marts.daily_orders",
        LineageRelationalIdentity.datasetName(tables.outputs().get(0)));
  }

  @Test
  void parsesMultipleSourcesFromJoin() {
    RelationalSqlParser.Tables tables =
        RelationalSqlParser.create(true, null)
            .parse(
                "SELECT o.id FROM analytics.staging.orders o "
                    + "JOIN analytics.staging.customers c ON o.cid = c.id",
                "postgres");

    assertEquals(2, tables.inputs().size());
  }

  @Test
  void disabledParserReturnsEmpty() {
    RelationalSqlParser.Tables tables =
        RelationalSqlParser.create(false, null)
            .parse("SELECT id FROM analytics.staging.orders", "postgres");
    assertTrue(tables.isEmpty());
  }

  @Test
  void blankStatementReturnsEmpty() {
    assertTrue(RelationalSqlParser.create(true, null).parse("   ", "postgres").isEmpty());
    assertTrue(RelationalSqlParser.create(true, null).parse(null, "postgres").isEmpty());
  }

  @Test
  void unparseableStatementReturnsEmptyAndDoesNotThrow() {
    RelationalSqlParser.Tables tables =
        RelationalSqlParser.create(true, null).parse("not a sql statement at all", "postgres");
    assertTrue(tables.isEmpty());
  }

  @Test
  void parsesColumnLineageFromInsertSelect() {
    RelationalSqlParser.Tables tables =
        RelationalSqlParser.create(true, null)
            .parse(
                "INSERT INTO analytics.marts.daily_orders "
                    + "SELECT o.id, o.amount FROM analytics.staging.orders o",
                "postgres");

    assertFalse(tables.columnLineage().isEmpty());
    RelationalSqlParser.ColumnEdge idEdge =
        tables.columnLineage().stream()
            .filter(e -> "id".equals(e.outputField()))
            .findFirst()
            .orElseThrow();
    assertEquals(
        "analytics.staging.orders",
        LineageRelationalIdentity.datasetName(idEdge.inputs().get(0).table()));
    assertEquals("id", idEdge.inputs().get(0).field());
  }
}
