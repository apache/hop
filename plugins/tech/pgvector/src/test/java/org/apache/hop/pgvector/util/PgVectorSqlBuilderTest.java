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
package org.apache.hop.pgvector.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertMeta;
import org.junit.jupiter.api.Test;

class PgVectorSqlBuilderTest {

  @Test
  void buildsUpsertSql() {
    PgVectorUpsertMeta meta = new PgVectorUpsertMeta();
    meta.getColumnMappings().add(new PgVectorColumnMapping("embedding_model", "embedding_model"));
    List<PgVectorTableColumn> columns = PgVectorSchemaBuilder.tableColumns(meta);
    String sql = PgVectorSqlBuilder.upsertSql("\"public\".\"hop_rag_chunks\"", columns);
    assertTrue(sql.contains("ON CONFLICT (\"id\") DO UPDATE"));
    assertTrue(sql.contains("?::vector"));
    assertTrue(sql.contains("\"embedding_model\""));
  }

  @Test
  void buildsSearchSqlWithCosineMetricAndFilter() {
    String sql =
        PgVectorSqlBuilder.searchSql(
            "\"public\".\"hop_rag_chunks\"",
            VectorDistanceMetric.COSINE,
            List.of(new PgVectorSearchFilter("embedding_model", "embedding_model")));
    assertTrue(sql.contains("WHERE \"embedding_model\" = ?"));
    assertTrue(sql.contains("1 - (embedding <=> ?::vector) AS similarity"));
    assertTrue(sql.indexOf("1 - (embedding <=> ?::vector)") < sql.indexOf("WHERE"));
    assertTrue(sql.contains("ORDER BY embedding <=> ?::vector"));
    assertTrue(sql.contains("LIMIT ?"));
  }

  @Test
  void buildsHnswIndexSql() {
    String sql =
        PgVectorSqlBuilder.createHnswIndexSql(
            "\"public\".\"hop_rag_chunks\"", VectorDistanceMetric.COSINE);
    assertTrue(sql.contains("USING hnsw"));
    assertTrue(sql.contains("vector_cosine_ops"));
  }

  @Test
  void buildsAddColumnSql() {
    String sql =
        PgVectorSqlBuilder.addColumnSql(
            "\"public\".\"hop_rag_chunks\"",
            new PgVectorTableColumn("embedding_model", PgVectorColumnType.TEXT, false),
            768);
    assertTrue(sql.contains("ADD COLUMN IF NOT EXISTS"));
    assertTrue(sql.contains("\"embedding_model\""));
  }

  /**
   * All three metrics have to produce a higher-is-better score, otherwise the minimum-score filter
   * silently inverts when the metric is changed: cosine returns a similarity, raw L2 returns a
   * distance, and pgvector's {@code <#>} returns a negated inner product.
   */
  @Test
  void everyMetricProducesAHigherIsBetterScore() {
    assertEquals("1 - (embedding <=> ?::vector)", VectorDistanceMetric.COSINE.getScoreExpression());
    assertEquals(
        "1 / (1 + (embedding <-> ?::vector))", VectorDistanceMetric.L2.getScoreExpression());
    assertEquals(
        "(-1 * (embedding <#> ?::vector))",
        VectorDistanceMetric.INNER_PRODUCT.getScoreExpression());
  }

  @Test
  void searchSqlBindsOneParameterPerFilterBetweenTheScoreAndTheLimit() {
    String sql =
        PgVectorSqlBuilder.searchSql(
            "\"public\".\"chunks\"",
            VectorDistanceMetric.COSINE,
            List.of(
                new PgVectorSearchFilter("source_type", "wanted"),
                new PgVectorSearchFilter("origin", "wanted_origin")));

    assertEquals(
        5,
        sql.chars().filter(c -> c == '?').count(),
        "score vector + 2 filters + order-by vector + limit");
    assertTrue(sql.contains("WHERE \"source_type\" = ? AND \"origin\" = ?"));
    assertTrue(sql.endsWith("LIMIT ?"));
  }

  /** Identifiers are quoted and embedded quotes are doubled, so a crafted name cannot break out. */
  @Test
  void quotesAndEscapesIdentifiers() {
    String sql = PgVectorSqlBuilder.qualifiedTable("pu\"blic", "cho\"nks");

    assertEquals("\"pu\"\"blic\".\"cho\"\"nks\"", sql);
  }
}
