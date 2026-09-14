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

package org.apache.hop.ui.hopgui.perspective.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.variables.IVariables;
import org.junit.jupiter.api.Test;

class DatabaseTableInfoTabTest {

  @Test
  void withIndexStatementsAppendsCreateIndex() {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    IVariables variables = mock(IVariables.class);
    when(meta.getQuotedSchemaTableCombination(any(), eq("public"), eq("t")))
        .thenReturn("\"public\".t");
    when(meta.quoteField("idx_name")).thenReturn("idx_name");
    when(meta.quoteField("name")).thenReturn("name");

    DatabaseIndexInfo index = new DatabaseIndexInfo();
    index.setName("idx_name");
    index.setUnique(true);
    index.getColumns().add("name");

    String ddl =
        DatabaseTableInfoTab.withIndexStatements(
            meta,
            variables,
            "public",
            "t",
            "CREATE TABLE \"public\".t (id INTEGER);",
            List.of(index));
    assertTrue(ddl.contains("CREATE UNIQUE INDEX idx_name ON \"public\".t (name);"));
  }

  @Test
  void withIndexStatementsSkipsCatalogDdlThatAlreadyHasKeys() {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    IVariables variables = mock(IVariables.class);
    DatabaseIndexInfo index = new DatabaseIndexInfo();
    index.setName("PRIMARY");
    index.getColumns().add("id");
    String catalog = "CREATE TABLE t (\n  id INT,\n  PRIMARY KEY (id)\n);";
    assertEquals(
        catalog,
        DatabaseTableInfoTab.withIndexStatements(
            meta, variables, null, "t", catalog, List.of(index)));
  }

  @Test
  void catalogDdlIncludesIndexesDetectsShowCreate() {
    assertTrue(
        DatabaseTableInfoTab.catalogDdlIncludesIndexes(
            "CREATE TABLE `t` (\n  `id` int,\n  KEY `idx` (`id`)\n)"));
    assertFalse(DatabaseTableInfoTab.catalogDdlIncludesIndexes("CREATE TABLE t (id INTEGER);"));
  }
}
