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
package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.local.LocalAuditManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TableViewColumnViewManagerTest {

  @TempDir Path testFolder;

  @BeforeEach
  void resetAuditManager() throws HopException {
    AuditManager.getInstance()
        .setActiveAuditManager(new LocalAuditManager(testFolder.toAbsolutePath().toString()));
  }

  @Test
  void saveListLoadAndDelete() {
    String group = "test-project";

    TableViewColumnViewManager.save(
        group, new TableViewColumnView("Customer keys", List.of("id", "name", "email")));
    TableViewColumnViewManager.save(
        group, new TableViewColumnView("Order dates", List.of("order_id", "created_at")));

    List<TableViewColumnView> views = TableViewColumnViewManager.list(group);
    assertEquals(2, views.size());
    assertEquals("Customer keys", views.get(0).getName());
    assertEquals("Order dates", views.get(1).getName());

    TableViewColumnView loaded = TableViewColumnViewManager.load(group, "Customer keys");
    assertEquals(List.of("id", "name", "email"), loaded.getColumnNames());

    TableViewColumnViewManager.save(
        group, new TableViewColumnView("Customer keys", List.of("id", "email")));
    loaded = TableViewColumnViewManager.load(group, "Customer keys");
    assertEquals(List.of("id", "email"), loaded.getColumnNames());

    TableViewColumnViewManager.delete(group, "Customer keys");
    assertNull(TableViewColumnViewManager.load(group, "Customer keys"));
    views = TableViewColumnViewManager.list(group);
    assertEquals(1, views.size());
    assertEquals("Order dates", views.get(0).getName());
  }

  @Test
  void emptyGroupReturnsNothing() {
    assertTrue(TableViewColumnViewManager.list("").isEmpty());
    assertTrue(TableViewColumnViewManager.list(null).isEmpty());
    assertNull(TableViewColumnViewManager.load("group", null));
    assertNull(TableViewColumnViewManager.load(null, "name"));
  }
}
