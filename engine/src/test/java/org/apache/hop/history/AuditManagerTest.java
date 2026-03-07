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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

class AuditManagerTest {
  @TempDir Path testFolder;

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

  //  Race condition with other test data, works fine when run stand-alone
  @Test
  @Disabled("This test needs to be reviewed")
  void testFindAllEventsWithDefaultAuditManager() throws HopException {
    AuditManager.getInstance()
        .setActiveAuditManager(new LocalAuditManager(testFolder.toAbsolutePath().toString()));
    String group = "testFindAllEventsWithDefaultAuditManager";
    AuditManager.clearEvents();
    AuditManager.registerEvent(group, "type1", "name1", "operation1");
    AuditManager.registerEvent(group, "type1", "name1", "operation1");
    AuditManager.registerEvent(group, "type1", "name1", "operation1");
    AuditManager.registerEvent(group, "type1", "name2", "operation1");
    AuditManager.registerEvent(group, "type2", "name2", "operation1");

    List<AuditEvent> allEvents = AuditManager.findEvents(group, "type1", "operation1", 10, false);
    assertEquals(4, allEvents.size(), "Not getting unique events");
    AuditManager.clearEvents();
  }

  @Test
  void testFindUniqueEventsWithDefaultAuditManager() throws HopException {
    AuditManager.getInstance()
        .setActiveAuditManager(new LocalAuditManager(testFolder.toAbsolutePath().toString()));
    String group = "testFindUniqueEventsWithDefaultAuditManager";
    AuditManager.clearEvents();
    AuditManager.registerEvent(group, "type1", "name1", "operation1");
    AuditManager.registerEvent(group, "type1", "name1", "operation1");

    List<AuditEvent> uniqueEvents = AuditManager.findEvents(group, "type1", "operation1", 10, true);
    assertEquals(1, uniqueEvents.size(), "Not getting unique events");
    AuditManager.clearEvents();
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

  // Figure out why this sometimes fails in windows and to a lesser extent Linux.
  // It's likely an initialization issue which occurs for this testing scenario.
  //
  @Test
  @Disabled("This test needs to be reviewed")
  void testClearEvents() throws HopException {
    AuditManager.getInstance()
        .setActiveAuditManager(new LocalAuditManager(testFolder.toAbsolutePath().toString()));

    // Repeat the test 100 times.
    //
    for (int i = 0; i < 100; i++) {
      AuditManager.getActive().clearEvents();

      String group = "testClearEvents";
      AuditManager.registerEvent(group, "type1", "name1", "operation1");
      AuditManager.registerEvent(group, "type1", "name1", "operation1");
      assertEquals(
          2,
          AuditManager.findEvents(group, "type1", "operation1", 10, false).size(),
          "Problem in registering event");
      AuditManager.clearEvents();
      assertEquals(
          0,
          AuditManager.findEvents(group, "type1", "operation1", 10, false).size(),
          "Problem in clearning event");
    }
  }
}
