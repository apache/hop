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

import org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertMeta;
import org.junit.jupiter.api.Test;

class PgVectorSchemaBuilderTest {

  @Test
  void includesMappedMetadataColumns() {
    PgVectorUpsertMeta meta = new PgVectorUpsertMeta();
    meta.getColumnMappings().add(new PgVectorColumnMapping("embedding_model", "embedding_model"));
    meta.getColumnMappings().add(new PgVectorColumnMapping("source_url", "source_url"));

    assertEquals(7, PgVectorSchemaBuilder.tableColumns(meta).size());
    assertTrue(
        PgVectorSchemaBuilder.tableColumns(meta).stream()
            .anyMatch(column -> "embedding_model".equals(column.name())));
  }
}
