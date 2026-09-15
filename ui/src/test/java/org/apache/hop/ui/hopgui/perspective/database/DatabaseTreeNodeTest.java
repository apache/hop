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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DatabaseTreeNodeTest {

  @Test
  void kindOfPrefersViewThenSynonymThenTable() {
    assertEquals(
        DatabaseTreeNode.Kind.VIEW,
        DatabaseTreeNode.kindOf("orders_v", Set.of("orders_v"), Set.of()));
    assertEquals(
        DatabaseTreeNode.Kind.VIEW,
        DatabaseTreeNode.kindOf("ORDERS_V", Set.of("orders_v"), Set.of()));
    assertEquals(
        DatabaseTreeNode.Kind.SYNONYM,
        DatabaseTreeNode.kindOf("orders_s", Set.of(), Set.of("orders_s")));
    assertEquals(
        DatabaseTreeNode.Kind.TABLE,
        DatabaseTreeNode.kindOf("orders", Set.of("orders_v"), Set.of()));
  }

  @Test
  void namesForSchemaMatchesIgnoreCaseAndEmptyKeys() {
    Map<String, java.util.Collection<String>> map =
        Map.of("Public", List.of("v1"), "", List.of("root_view"));
    assertEquals(List.of("v1"), DatabaseWorkbench.namesForSchema(map, "public"));
    assertEquals(List.of("root_view"), DatabaseWorkbench.namesForSchema(map, null));
    assertTrue(DatabaseWorkbench.namesForSchema(Map.of(), "public").isEmpty());
  }

  @Test
  void schemaAndCatalogExpandStateIsRemembered() {
    assertTrue(DatabaseWorkbench.remembersExpandState(DatabaseTreeNode.Kind.SCHEMA));
    assertTrue(DatabaseWorkbench.remembersExpandState(DatabaseTreeNode.Kind.CATALOG));
    assertFalse(DatabaseWorkbench.remembersExpandState(DatabaseTreeNode.Kind.CONNECTION));
    assertFalse(DatabaseWorkbench.remembersExpandState(DatabaseTreeNode.Kind.FOLDER));
    assertFalse(DatabaseWorkbench.remembersExpandState(DatabaseTreeNode.Kind.TABLE));
  }

  @Test
  void containsIgnoreCase() {
    assertTrue(DatabaseTreeNode.containsIgnoreCase(List.of("Alpha"), "alpha"));
    assertFalse(DatabaseTreeNode.containsIgnoreCase(List.of("Alpha"), "beta"));
    assertFalse(DatabaseTreeNode.containsIgnoreCase(null, "alpha"));
  }
}
