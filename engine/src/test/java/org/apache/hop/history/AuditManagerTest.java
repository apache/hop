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
package org.apache.hop.history;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.history.local.LocalAuditManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

class AuditManagerTest {
  @TempDir Path testFolder;

  /**
   * Reset the AuditManager singleton to a fresh LocalAuditManager pointed at a per-test temp
   * folder, and ensure no leftover events from prior tests are visible.
   */
  @BeforeEach
  void resetAuditManager() throws HopException {
    AuditManager.getInstance()
        .setActiveAuditManager(new LocalAuditManager(testFolder.toAbsolutePath().toString()));
    AuditManager.clearEvents();
  }

  @Test
  void testSingleton() {
    AuditManager instance1 = AuditManager.getInstance();
    AuditManager instance2 = AuditManager.getInstance();
    assertEquals(instance1, instance2);
  }

  @Test
  void testHasAnActiveAuditManager() {
    assertNotNull(AuditManager.getActive());
  }

  @Test
  void testRegisterEvent() throws HopException {
    IAuditManager mockManager = Mockito.mock(IAuditManager.class);
    AuditManager.getInstance().setActiveAuditManager(mockManager);
    AuditManager.registerEvent("", "", "", "");
    verify(mockManager, times(1)).storeEvent(any());
  }

  @Test
  void testEvents() throws HopException {
    String group = "testEvents";
    IAuditManager mockManager = Mockito.mock(IAuditManager.class);
    AuditManager.getInstance().setActiveAuditManager(mockManager);

    List<AuditEvent> events = new ArrayList<>();
    events.add(new AuditEvent(group, "type1", "name1", "operation1", new Date()));
    events.add(new AuditEvent(group, "type1", "name2", "operation1", new Date()));

    when(mockManager.findEvents(group, "type1", false)).thenReturn(events);
    List<AuditEvent> allEvents = AuditManager.findEvents(group, "type1", "operation1", 10, false);
    assertEquals(2, allEvents.size(), "Not getting all events");
  }

  @Test
  void testFindUniqueEvents() throws HopException {
    String group = "testFindUniqueEvents";
    IAuditManager mockManager = Mockito.mock(IAuditManager.class);
    AuditManager.getInstance().setActiveAuditManager(mockManager);

    List<AuditEvent> events = new ArrayList<>();
    events.add(new AuditEvent(group, "type1", "name1", "operation1", new Date()));
    events.add(new AuditEvent(group, "type1", "name2", "operation1", new Date()));

    when(mockManager.findEvents(group, "type1", true)).thenReturn(events);
    List<AuditEvent> uniqueEvents = AuditManager.findEvents(group, "type1", "operation1", 10, true);
    assertEquals(2, uniqueEvents.size(), "Not getting unique events");
  }

  @Test
  void testFindAllEventsWithDefaultAuditManager() throws HopException {
    String group = "testFindAllEventsWithDefaultAuditManager";
    // LocalAuditManager stores events keyed by (timestamp-ms + operation), so events with the
    // same name+operation registered within a single millisecond collide on disk. Use distinct
    // operations so each register produces a separate file.
    AuditManager.registerEvent(group, "type1", "name1", "operationA");
    AuditManager.registerEvent(group, "type1", "name1", "operationB");
    AuditManager.registerEvent(group, "type1", "name1", "operationC");
    AuditManager.registerEvent(group, "type1", "name2", "operationD");
    AuditManager.registerEvent(group, "type2", "name2", "operationE");

    List<AuditEvent> allEvents = AuditManager.findEvents(group, "type1", null, 10, false);
    assertEquals(4, allEvents.size(), "Not getting all events");
  }

  @Test
  void testFindUniqueEventsWithDefaultAuditManager() throws HopException {
    String group = "testFindUniqueEventsWithDefaultAuditManager";
    AuditManager.registerEvent(group, "type1", "name1", "operationA");
    AuditManager.registerEvent(group, "type1", "name1", "operationB");

    List<AuditEvent> uniqueEvents = AuditManager.findEvents(group, "type1", null, 10, true);
    assertEquals(1, uniqueEvents.size(), "Not getting unique events");
  }

  @Test
  void testFindMaxEvents() throws HopException {
    String group = "testFindMaxEvents";
    IAuditManager mockManager = Mockito.mock(IAuditManager.class);
    AuditManager.getInstance().setActiveAuditManager(mockManager);

    List<AuditEvent> events = new ArrayList<>();
    events.add(new AuditEvent(group, "type1", "name1", "operation1", new Date()));
    events.add(new AuditEvent(group, "type1", "name2", "operation1", new Date()));
    events.add(new AuditEvent(group, "type1", "name3", "operation1", new Date()));
    events.add(new AuditEvent(group, "type1", "name4", "operation1", new Date()));
    when(mockManager.findEvents(group, "type1", false)).thenReturn(events);
    List<AuditEvent> maxEvents = AuditManager.findEvents(group, "type1", "operation1", 2, false);
    assertEquals(2, maxEvents.size(), "Not getting unique events");
  }

  @Test
  void testClearEvents() throws HopException {
    String group = "testClearEvents";
    // Use distinct operations so each register produces a separate on-disk file (see comment on
    // testFindAllEventsWithDefaultAuditManager).
    AuditManager.registerEvent(group, "type1", "name1", "operationA");
    AuditManager.registerEvent(group, "type1", "name1", "operationB");
    assertEquals(
        2,
        AuditManager.findEvents(group, "type1", null, 10, false).size(),
        "Problem in registering events");

    AuditManager.clearEvents();
    assertEquals(
        0,
        AuditManager.findEvents(group, "type1", null, 10, false).size(),
        "Problem in clearing events");
  }
}
