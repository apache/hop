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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.lineage.LineageRelationalIdentity;
import org.apache.hop.lineage.model.RelationalTable;
import org.apache.hop.lineage.model.RelationalWriteColumn;
import org.junit.jupiter.api.Test;

class RelationalColumnLineageCorrelatorTest {

  private static final RelationalTable SOURCE =
      new RelationalTable("hop_database", "public", "orders_source");

  private static RelationalSqlParser.ColumnEdge edge(String field, String sourceColumn) {
    return new RelationalSqlParser.ColumnEdge(
        field, List.of(new RelationalSqlParser.FieldRef(SOURCE, sourceColumn)));
  }

  // A read's stream fields, resolved through a write column's origin transform, become
  // targetColumn <- sourceColumn edges.
  @Test
  void correlatesWriteColumnsToSourceViaOriginTransform() {
    RelationalColumnLineageCorrelator correlator = new RelationalColumnLineageCorrelator();
    correlator.registerRead(
        "run1",
        "Read source",
        List.of(edge("id", "id"), edge("amount", "amount")),
        List.of(SOURCE));

    List<RelationalSqlParser.ColumnEdge> edges =
        correlator.correlate(
            "run1",
            List.of(
                new RelationalWriteColumn("id", "id", "Read source"),
                new RelationalWriteColumn("amount", "amount", "Read source")));

    assertEquals(2, edges.size());
    assertEquals("id", edges.get(0).outputField());
    assertEquals(
        "hop_database.public.orders_source",
        LineageRelationalIdentity.datasetName(edges.get(0).inputs().get(0).table()));
    assertEquals("id", edges.get(0).inputs().get(0).field());
  }

  // An aliased projection (SELECT customer_id AS cust) resolves the write column back to the
  // original source column via the read's parsed lineage.
  @Test
  void correlatesAliasedColumnViaParsedLineage() {
    RelationalColumnLineageCorrelator correlator = new RelationalColumnLineageCorrelator();
    // stream field "cust" derives from source column "customer_id"
    correlator.registerRead(
        "run1", "Read source", List.of(edge("cust", "customer_id")), List.of(SOURCE));

    List<RelationalSqlParser.ColumnEdge> edges =
        correlator.correlate(
            "run1", List.of(new RelationalWriteColumn("cust", "cust", "Read source")));

    assertEquals(1, edges.size());
    assertEquals("customer_id", edges.get(0).inputs().get(0).field());
  }

  // A field the parser did not resolve falls back to a direct column of the sole source table.
  @Test
  void nameMatchFallbackForUnparsedField() {
    RelationalColumnLineageCorrelator correlator = new RelationalColumnLineageCorrelator();
    correlator.registerRead("run1", "Read source", List.of(), List.of(SOURCE));

    List<RelationalSqlParser.ColumnEdge> edges =
        correlator.correlate("run1", List.of(new RelationalWriteColumn("id", "id", "Read source")));

    assertEquals(1, edges.size());
    assertEquals("id", edges.get(0).inputs().get(0).field());
  }

  // Columns whose origin transform is not a registered read (e.g. a computed field) are omitted.
  @Test
  void skipsColumnsWithUnknownOrigin() {
    RelationalColumnLineageCorrelator correlator = new RelationalColumnLineageCorrelator();
    correlator.registerRead("run1", "Read source", List.of(edge("id", "id")), List.of(SOURCE));

    List<RelationalSqlParser.ColumnEdge> edges =
        correlator.correlate(
            "run1",
            List.of(
                new RelationalWriteColumn("id", "id", "Read source"),
                new RelationalWriteColumn("total", "total", "Calculator")));

    assertEquals(1, edges.size());
    assertEquals("id", edges.get(0).outputField());
  }

  // A transform emits its relational I/O when it finishes, which can reach the sink after the
  // pipeline's own completion event. Read state therefore has to outlive the run completing,
  // otherwise every such write silently loses its column lineage.
  @Test
  void readStateOutlivesTheRunItBelongsTo() {
    RelationalColumnLineageCorrelator correlator = new RelationalColumnLineageCorrelator();
    correlator.registerRead("run1", "Read source", List.of(edge("id", "id")), List.of(SOURCE));

    // Whatever else happens to the run, a late write still correlates.
    for (int i = 0; i < 5; i++) {
      assertEquals(
          1,
          correlator
              .correlate("run1", List.of(new RelationalWriteColumn("id", "id", "Read source")))
              .size());
    }
  }

  // Memory is bounded by evicting the least recently used run rather than by the run lifecycle.
  @Test
  void theLeastRecentlyUsedRunIsEvictedOnceTheCacheIsFull() {
    RelationalColumnLineageCorrelator correlator = new RelationalColumnLineageCorrelator();
    correlator.registerRead("oldest", "Read source", List.of(edge("id", "id")), List.of(SOURCE));
    for (int i = 0; i < RelationalColumnLineageCorrelator.MAX_RUNS; i++) {
      correlator.registerRead("run" + i, "Read source", List.of(edge("id", "id")), List.of(SOURCE));
    }

    List<RelationalWriteColumn> write =
        List.of(new RelationalWriteColumn("id", "id", "Read source"));
    assertTrue(correlator.correlate("oldest", write).isEmpty());
    assertEquals(
        1,
        correlator
            .correlate("run" + (RelationalColumnLineageCorrelator.MAX_RUNS - 1), write)
            .size());
  }

  @Test
  void differentRunsAreIsolated() {
    RelationalColumnLineageCorrelator correlator = new RelationalColumnLineageCorrelator();
    correlator.registerRead("run1", "Read source", List.of(edge("id", "id")), List.of(SOURCE));

    assertTrue(
        correlator
            .correlate("run2", List.of(new RelationalWriteColumn("id", "id", "Read source")))
            .isEmpty());
  }
}
