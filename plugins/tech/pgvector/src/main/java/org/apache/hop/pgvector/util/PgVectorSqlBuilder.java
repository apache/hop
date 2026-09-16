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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public final class PgVectorSqlBuilder {

  private PgVectorSqlBuilder() {}

  public static String qualifiedTable(String schemaName, String tableName) {
    String table = quoteIdentifier(tableName);
    if (StringUtils.isNotBlank(schemaName)) {
      return quoteIdentifier(schemaName.trim()) + "." + table;
    }
    return table;
  }

  public static String upsertSql(String qualifiedTable, List<PgVectorTableColumn> columns) {
    String columnList =
        columns.stream().map(c -> quoteIdentifier(c.name())).collect(Collectors.joining(", "));
    String placeholders =
        columns.stream().map(PgVectorTableColumn::placeholder).collect(Collectors.joining(", "));
    String updates =
        columns.stream()
            .filter(c -> !c.primaryKey())
            .map(c -> quoteIdentifier(c.name()) + " = EXCLUDED." + quoteIdentifier(c.name()))
            .collect(Collectors.joining(", "));
    return "INSERT INTO "
        + qualifiedTable
        + " ("
        + columnList
        + ") VALUES ("
        + placeholders
        + ") ON CONFLICT ("
        + quoteIdentifier("id")
        + ") DO UPDATE SET "
        + updates;
  }

  public static String searchSql(
      String qualifiedTable, VectorDistanceMetric metric, List<PgVectorSearchFilter> filters) {
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT id, document_id, chunk_index, content, ")
        .append(metric.getScoreExpression())
        .append(" AS similarity FROM ")
        .append(qualifiedTable);
    if (filters != null && !filters.isEmpty()) {
      sql.append(" WHERE ");
      for (int i = 0; i < filters.size(); i++) {
        if (i > 0) {
          sql.append(" AND ");
        }
        sql.append(
                quoteIdentifier(
                    PgVectorSchemaBuilder.normalizeColumnName(filters.get(i).getColumnName())))
            .append(" = ?");
      }
    }
    sql.append(" ORDER BY embedding ").append(metric.getOperator()).append(" ?::vector LIMIT ?");
    return sql.toString();
  }

  public static String createTableSql(
      String qualifiedTable, List<PgVectorTableColumn> columns, int dimensions) {
    String body =
        columns.stream()
            .map(
                c -> {
                  String definition = quoteIdentifier(c.name()) + " " + c.sqlType(dimensions);
                  if (c.primaryKey()) {
                    definition += " PRIMARY KEY";
                  } else if ("content".equals(c.name())) {
                    definition += " NOT NULL";
                  }
                  return definition;
                })
            .collect(Collectors.joining(", "));
    return "CREATE TABLE IF NOT EXISTS " + qualifiedTable + " (" + body + ")";
  }

  public static String addColumnSql(
      String qualifiedTable, PgVectorTableColumn column, int dimensions) {
    return "ALTER TABLE "
        + qualifiedTable
        + " ADD COLUMN IF NOT EXISTS "
        + quoteIdentifier(column.name())
        + " "
        + column.sqlType(dimensions);
  }

  public static String createExtensionSql() {
    return "CREATE EXTENSION IF NOT EXISTS vector";
  }

  public static String createHnswIndexSql(String qualifiedTable, VectorDistanceMetric metric) {
    String indexName = indexNameFor(qualifiedTable);
    return "CREATE INDEX IF NOT EXISTS "
        + quoteIdentifier(indexName)
        + " ON "
        + qualifiedTable
        + " USING hnsw ("
        + quoteIdentifier("embedding")
        + " "
        + hnswOps(metric)
        + ")";
  }

  public static String deleteByDocumentIdSql(String qualifiedTable) {
    return "DELETE FROM " + qualifiedTable + " WHERE " + quoteIdentifier("document_id") + " = ?";
  }

  private static String indexNameFor(String qualifiedTable) {
    return qualifiedTable.replace("\"", "").replace('.', '_') + "_embedding_hnsw_idx";
  }

  private static String hnswOps(VectorDistanceMetric metric) {
    return switch (metric) {
      case L2 -> "vector_l2_ops";
      case INNER_PRODUCT -> "vector_ip_ops";
      default -> "vector_cosine_ops";
    };
  }

  private static String quoteIdentifier(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }
}
