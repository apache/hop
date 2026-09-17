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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.database.Database;
import org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertMeta;
import org.junit.jupiter.api.Test;

class PgVectorDatabaseTest {

  /**
   * CREATE EXTENSION needs elevated privileges and is only warned about in check() when the
   * transform creates the table, so it must not be issued for the other schema options.
   */
  @Test
  void createsTheExtensionOnlyWhenItAlsoCreatesTheTable() throws Exception {
    PgVectorUpsertMeta meta = new PgVectorUpsertMeta();
    meta.setDefault();
    meta.setCreateTableIfMissing(false);
    meta.setCreateHnswIndex(true);
    meta.setColumnMappings(List.of(new PgVectorColumnMapping("source_url", "url")));

    List<String> statements = executedStatements(meta);

    assertFalse(
        statements.stream().anyMatch(sql -> sql.contains("CREATE EXTENSION")),
        "an existing table already has the extension: " + statements);
    assertTrue(
        statements.stream().anyMatch(sql -> sql.contains("CREATE INDEX")),
        "the index option must still run: " + statements);
    assertTrue(
        statements.stream().anyMatch(sql -> sql.contains("ADD COLUMN")),
        "the mapped column must still be added: " + statements);
  }

  @Test
  void createsTheExtensionWhenItCreatesTheTable() throws Exception {
    PgVectorUpsertMeta meta = new PgVectorUpsertMeta();
    meta.setDefault();
    meta.setCreateTableIfMissing(true);

    List<String> statements = executedStatements(meta);

    assertTrue(
        statements.stream().anyMatch(sql -> sql.contains("CREATE EXTENSION")),
        "a new vector column needs the extension: " + statements);
  }

  private static List<String> executedStatements(PgVectorUpsertMeta meta) throws Exception {
    Database database = mock(Database.class);
    List<String> statements = new ArrayList<>();
    doAnswer(
            invocation -> {
              statements.add(invocation.getArgument(0));
              return null;
            })
        .when(database)
        .execStatement(org.mockito.ArgumentMatchers.anyString());

    PgVectorDatabase.ensureSchema(
        database, meta, "public", "chunks", VectorDistanceMetric.COSINE, 768);
    return statements;
  }
}
