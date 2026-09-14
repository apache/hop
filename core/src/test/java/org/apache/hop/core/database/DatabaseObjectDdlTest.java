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
package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

class DatabaseObjectDdlTest {

  @Test
  void extractDefinitionPrefersCreateColumn() throws Exception {
    RowMeta meta = new RowMeta();
    meta.addValueMeta(new ValueMetaString("Table"));
    meta.addValueMeta(new ValueMetaString("Create Table"));
    RowMetaAndData row = new RowMetaAndData(meta, "customer", "CREATE TABLE customer (id INT)");
    assertEquals("CREATE TABLE customer (id INT)", DatabaseObjectDdl.extractDefinition(row));
  }

  @Test
  void extractDefinitionUsesViewDefinitionColumn() throws Exception {
    RowMeta meta = new RowMeta();
    meta.addValueMeta(new ValueMetaString("VIEW_DEFINITION"));
    RowMetaAndData row = new RowMetaAndData(meta, "SELECT id FROM t");
    assertEquals("SELECT id FROM t", DatabaseObjectDdl.extractDefinition(row));
  }

  @Test
  void asCreateViewStatementWrapsSelect() {
    String ddl = DatabaseObjectDdl.asCreateViewStatement("\"public\".v", "SELECT 1 AS x");
    assertTrue(ddl.startsWith("CREATE VIEW \"public\".v AS"));
    assertTrue(ddl.contains("SELECT 1 AS x"));
    assertTrue(ddl.endsWith(";"));
  }

  @Test
  void asCreateViewStatementKeepsFullCreate() {
    assertEquals(
        "CREATE VIEW v AS SELECT 1;",
        DatabaseObjectDdl.asCreateViewStatement("v", "CREATE VIEW v AS SELECT 1"));
  }

  @Test
  void startsWithCreateIsCaseInsensitive() {
    assertTrue(DatabaseObjectDdl.startsWithCreate("create table t (id int)"));
    assertFalse(DatabaseObjectDdl.startsWithCreate("SELECT 1"));
  }

  @Test
  void synthesizeCreateViewListsColumns() {
    RowMeta fields = new RowMeta();
    fields.addValueMeta(new ValueMetaString("id"));
    fields.addValueMeta(new ValueMetaString("name"));
    String ddl = DatabaseObjectDdl.synthesizeCreateView("v", fields, "no catalog");
    assertTrue(ddl.contains("-- no catalog"));
    assertTrue(ddl.contains("CREATE VIEW v AS"));
    assertTrue(ddl.contains("id"));
    assertTrue(ddl.contains("name"));
  }
}
