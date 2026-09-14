/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditState;
import org.apache.hop.history.AuditStateMap;
import org.junit.jupiter.api.Test;

class DatabaseSqlTabMemoryTest {

  @Test
  void snapshotRoundTripKeepsConnectionFileAndDirtyBuffer() {
    DatabaseSqlTabMemory.Snapshot original =
        new DatabaseSqlTabMemory.Snapshot(
            "warehouse", "/tmp/load.sql", "CREATE TABLE t (id INT);", true, "load.sql");
    AuditStateMap map = new AuditStateMap();
    map.add(new AuditState("sql-0", original.toStateMap()));
    List<DatabaseSqlTabMemory.Snapshot> restored =
        DatabaseSqlTabMemory.snapshotsFromAudit(new AuditList(List.of("sql-0")), map);
    assertEquals(1, restored.size());
    DatabaseSqlTabMemory.Snapshot snapshot = restored.get(0);
    assertEquals("warehouse", snapshot.connection);
    assertEquals("/tmp/load.sql", snapshot.filename);
    assertEquals("CREATE TABLE t (id INT);", snapshot.sql);
    assertTrue(snapshot.dirty);
    assertEquals("load.sql", snapshot.name);
  }

  @Test
  void snapshotWithoutConnectionIsSkipped() {
    AuditStateMap map = new AuditStateMap();
    map.add(new AuditState("sql-0", originalEmptyConnection()));
    List<DatabaseSqlTabMemory.Snapshot> restored =
        DatabaseSqlTabMemory.snapshotsFromAudit(new AuditList(List.of("sql-0")), map);
    assertTrue(restored.isEmpty());
  }

  @Test
  void longSqlIsCapped() {
    String huge = "x".repeat(DatabaseSqlTabMemory.MAX_SQL_CHARS + 50);
    DatabaseSqlTabMemory.Snapshot snapshot =
        new DatabaseSqlTabMemory.Snapshot("db", "", huge, true, "SQL");
    String stored = (String) snapshot.toStateMap().get(DatabaseSqlTabMemory.PROP_SQL);
    assertEquals(DatabaseSqlTabMemory.MAX_SQL_CHARS, stored.length());
    assertFalse(stored.length() > DatabaseSqlTabMemory.MAX_SQL_CHARS);
  }

  @Test
  void jsonNumbersAreAcceptedForSelectionIndex() {
    assertEquals(2, DatabaseSqlTabMemory.number(2, 0));
    assertEquals(2, DatabaseSqlTabMemory.number(2L, 0));
    assertEquals(2, DatabaseSqlTabMemory.number("2", 0));
    assertEquals(0, DatabaseSqlTabMemory.number("nope", 0));
  }

  private static java.util.Map<String, Object> originalEmptyConnection() {
    java.util.Map<String, Object> map = new java.util.LinkedHashMap<>();
    map.put(DatabaseSqlTabMemory.PROP_CONNECTION, "");
    map.put(DatabaseSqlTabMemory.PROP_SQL, "SELECT 1");
    return map;
  }
}
