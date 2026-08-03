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

package org.apache.hop.lineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.lineage.model.RelationalTable;
import org.junit.jupiter.api.Test;

/**
 * Locks the OpenLineage relational identity rules of the "OpenLineage dataset identity" page of the
 * user manual so a regression here is caught before it silently splits a table into two lineage
 * nodes.
 */
public class LineageRelationalIdentityTest {

  // §3.1 — namespace scheme is mapped from the Hop database type, not the JDBC sub-protocol.
  @Test
  public void postgresNamespaceUsesSpecSchemeNotJdbcSubprotocol() {
    assertEquals(
        "postgres://db:5432",
        LineageRelationalIdentity.buildNamespace("POSTGRESQL", "db", "5432", 5432));
  }

  @Test
  public void mysqlAndMariaDbShareTheMysqlScheme() {
    assertEquals(
        "mysql://h:3306", LineageRelationalIdentity.buildNamespace("MYSQL", "h", "3306", 3306));
    assertEquals(
        "mysql://h:3306", LineageRelationalIdentity.buildNamespace("MARIADB", "h", "3306", 3306));
  }

  // §3.1 — host is lowercased so casing differences do not fork the namespace.
  @Test
  public void hostIsLowercasedAndTrimmed() {
    assertEquals(
        "postgres://prod-db:5432",
        LineageRelationalIdentity.buildNamespace("POSTGRESQL", "  PROD-DB  ", " 5432 ", 5432));
  }

  // §3.1 — "any default port included explicitly when known": blank port falls back to the default.
  @Test
  public void blankPortFallsBackToDefaultPort() {
    assertEquals(
        "postgres://db:5432",
        LineageRelationalIdentity.buildNamespace("POSTGRESQL", "db", "", 5432));
    assertEquals(
        "postgres://db:5432",
        LineageRelationalIdentity.buildNamespace("POSTGRESQL", "db", null, 5432));
  }

  @Test
  public void blankPortWithNoDefaultOmitsPort() {
    assertEquals("generic://db", LineageRelationalIdentity.buildNamespace("GENERIC", "db", "", 0));
  }

  // §3.1 "Generic JDBC" row — unknown types fall back to the lowercased Hop type id as the scheme.
  @Test
  public void unknownTypeFallsBackToLowercasedTypeId() {
    assertEquals(
        "clickhouse://db:8123",
        LineageRelationalIdentity.buildNamespace("CLICKHOUSE", "db", "8123", 8123));
  }

  @Test
  public void blankHostYieldsNoNamespace() {
    assertNull(LineageRelationalIdentity.buildNamespace("POSTGRESQL", "", "5432", 5432));
    assertNull(LineageRelationalIdentity.buildNamespace("POSTGRESQL", null, "5432", 5432));
  }

  // §3.2 — fully qualified name is database.schema.table when all segments are present.
  @Test
  public void datasetNameJoinsAllThreeSegments() {
    assertEquals(
        "analytics.staging.orders",
        LineageRelationalIdentity.datasetName(
            new RelationalTable("analytics", "staging", "orders")));
  }

  // §3.2 — databases without a catalog level use schema.table.
  @Test
  public void datasetNameOmitsBlankCatalog() {
    assertEquals(
        "staging.orders",
        LineageRelationalIdentity.datasetName(new RelationalTable(null, "staging", "orders")));
    assertEquals(
        "staging.orders",
        LineageRelationalIdentity.datasetName(new RelationalTable("  ", "staging", "orders")));
  }

  @Test
  public void datasetNameOmitsBlankSchema() {
    assertEquals(
        "orders", LineageRelationalIdentity.datasetName(new RelationalTable(null, null, "orders")));
  }

  // §3.2 — casing is preserved verbatim (Snowflake UPPER vs Postgres lower resolve upstream).
  @Test
  public void datasetNamePreservesSegmentCasing() {
    assertEquals(
        "ANALYTICS.STAGING.ORDERS",
        LineageRelationalIdentity.datasetName(
            new RelationalTable("ANALYTICS", "STAGING", "ORDERS")));
  }

  @Test
  public void datasetNameIsNullWhenTableBlank() {
    assertNull(LineageRelationalIdentity.datasetName(null));
  }

  // §3.2 — a table recovered from SQL (schema.table, no catalog) is qualified with the connection
  // catalog so it matches a write of catalog.schema.table for the same table.
  @Test
  public void withDefaultCatalogFillsBlankCatalogSoReadMatchesWrite() {
    RelationalTable parsedRead = new RelationalTable(null, "staging", "orders");
    String readName =
        LineageRelationalIdentity.datasetName(
            LineageRelationalIdentity.withDefaultCatalog(parsedRead, "analytics"));
    String writeName =
        LineageRelationalIdentity.datasetName(
            new RelationalTable("analytics", "staging", "orders"));
    assertEquals("analytics.staging.orders", readName);
    assertEquals(writeName, readName);
  }

  @Test
  public void withDefaultCatalogLeavesExistingCatalogUntouched() {
    RelationalTable qualified =
        LineageRelationalIdentity.withDefaultCatalog(
            new RelationalTable("other", "staging", "orders"), "analytics");
    assertEquals("other.staging.orders", LineageRelationalIdentity.datasetName(qualified));
  }

  // §3.2 — a bare table from a SELECT is filled with both the connection catalog and schema so it
  // matches an explicitly catalog.schema-qualified write.
  @Test
  public void withDefaultsFillsBlankCatalogAndSchema() {
    RelationalTable bare = new RelationalTable(null, null, "orders");
    assertEquals(
        "analytics.staging.orders",
        LineageRelationalIdentity.datasetName(
            LineageRelationalIdentity.withDefaults(bare, "analytics", "staging")));
  }

  @Test
  public void withDefaultsLeavesPresentSegmentsUntouched() {
    RelationalTable qualified =
        LineageRelationalIdentity.withDefaults(
            new RelationalTable(null, "marts", "orders"), "analytics", "staging");
    // schema present ("marts") is kept; only the blank catalog is filled.
    assertEquals("analytics.marts.orders", LineageRelationalIdentity.datasetName(qualified));
  }

  @Test
  public void withDefaultCatalogNoOpWhenCatalogBlank() {
    RelationalTable table = new RelationalTable(null, "staging", "orders");
    assertEquals("staging.orders", LineageRelationalIdentity.datasetName(table));
    assertEquals(
        "staging.orders",
        LineageRelationalIdentity.datasetName(
            LineageRelationalIdentity.withDefaultCatalog(table, "  ")));
  }
}
