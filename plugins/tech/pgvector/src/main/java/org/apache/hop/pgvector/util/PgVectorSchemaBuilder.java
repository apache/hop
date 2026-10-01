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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertMeta;

/** Builds pgvector table layouts from upsert transform configuration. */
public final class PgVectorSchemaBuilder {

  private static final Set<String> RESERVED =
      Set.of("id", "document_id", "chunk_index", "content", "embedding");

  private PgVectorSchemaBuilder() {}

  public static List<PgVectorTableColumn> tableColumns(PgVectorUpsertMeta meta) {
    List<PgVectorTableColumn> columns = new ArrayList<>();
    columns.add(new PgVectorTableColumn("id", PgVectorColumnType.TEXT, true));
    columns.add(new PgVectorTableColumn("document_id", PgVectorColumnType.TEXT, false));
    columns.add(new PgVectorTableColumn("chunk_index", PgVectorColumnType.INTEGER, false));
    columns.add(new PgVectorTableColumn("content", PgVectorColumnType.TEXT, false));
    columns.add(new PgVectorTableColumn("embedding", PgVectorColumnType.VECTOR, false));

    if (meta.getColumnMappings() != null) {
      Set<String> seen = new HashSet<>(RESERVED);
      for (PgVectorColumnMapping mapping : meta.getColumnMappings()) {
        if (mapping == null
            || Utils.isEmpty(mapping.getColumnName())
            || Utils.isEmpty(mapping.getStreamField())) {
          continue;
        }
        String column = normalizeColumnName(mapping.getColumnName());
        if (seen.add(column)) {
          columns.add(new PgVectorTableColumn(column, PgVectorColumnType.TEXT, false));
        }
      }
    }
    return columns;
  }

  public static String normalizeColumnName(String columnName) {
    return columnName.trim().toLowerCase(Locale.ROOT);
  }

  public static boolean isReservedColumn(String columnName) {
    return RESERVED.contains(normalizeColumnName(columnName));
  }
}
