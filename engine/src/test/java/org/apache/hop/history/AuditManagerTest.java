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
package org.apache.hop.history;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.history.local.LocalAuditManager;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;


public class AuditManagerTest {
  
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void testSingleton() {
        AuditManager instance1 = AuditManager.getInstance();
        AuditManager instance2 = AuditManager.getInstance();
        assertEquals(instance1, instance2);
    }

    @Test
    public void testHasAnActiveAuditManager() {
      assertNotNull(AuditManager.getActive());
    }

    @Test
    public void testRegisterEvent() throws HopException {
        IAuditManager mockManager = Mockito.mock(IAuditManager.class);
        AuditManager.getInstance().setActiveAuditManager(mockManager);
        AuditManager.registerEvent("", "", "", "");
        verify(mockManager, times(1)).storeEvent(any());
    }

    @Test
    public void testEvents() throws HopException {
        String group = "testEvents";
        IAuditManager mockManager = Mockito.mock(IAuditManager.class);
        AuditManager.getInstance().setActiveAuditManager(mockManager);

        List<AuditEvent> events = new ArrayList<>();
        events.add(new AuditEvent(group, "type1", "name1", "operation1", new Date()));
        events.add(new AuditEvent(group, "type1", "name2", "operation1", new Date()));

        when(mockManager.findEvents(group, "type1", false)).thenReturn(events);
        List<AuditEvent> allEvents =
                AuditManager.findEvents(group, "type1", "operation1", 10, false);
        assertEquals("Not getting all events", 2, allEvents.size());
    }

    @Test
    public void testFindUniqueEvents() throws HopException {
        String group = "testFindUniqueEvents";
        IAuditManager mockManager = Mockito.mock(IAuditManager.class);
        AuditManager.getInstance().setActiveAuditManager(mockManager);

        List<AuditEvent> events = new ArrayList<>();
        events.add(new AuditEvent(group, "type1", "name1", "operation1", new Date()));
        events.add(new AuditEvent(group, "type1", "name2", "operation1", new Date()));

        when(mockManager.findEvents(group, "type1", true)).thenReturn(events);
        List<AuditEvent> uniqueEvents =
                AuditManager.findEvents(group, "type1", "operation1", 10, true);
        assertEquals("Not getting unique events", 2, uniqueEvents.size());
    }

    @Ignore
    @Test
    public void testFindAllEventsWithDefaultAuditManager() throws HopException {
        AuditManager.getInstance().setActiveAuditManager(new LocalAuditManager(testFolder.getRoot().getAbsolutePath()));
        String group = "testFindAllEventsWithDefaultAuditManager";
        AuditManager.clearEvents();
        AuditManager.registerEvent(group, "type1", "name1", "operation1");
        AuditManager.registerEvent(group, "type1", "name1", "operation1");
        AuditManager.registerEvent(group, "type1", "name1", "operation1");
        AuditManager.registerEvent(group, "type1", "name2", "operation1");
        AuditManager.registerEvent(group, "type2", "name2", "operation1");

        List<AuditEvent> allEvents =
                AuditManager.findEvents(group, "type1", "operation1", 10, false);
        assertEquals("Not getting unique events", 4, allEvents.size());
        AuditManager.clearEvents();
    }

    @Test
    public void testFindUniqueEventsWithDefaultAuditManager() throws HopException {
        AuditManager.getInstance().setActiveAuditManager(new LocalAuditManager(testFolder.getRoot().getAbsolutePath()));
        String group = "testFindUniqueEventsWithDefaultAuditManager";
        AuditManager.clearEvents();
        AuditManager.registerEvent(group, "type1", "name1", "operation1");
        AuditManager.registerEvent(group, "type1", "name1", "operation1");

        List<AuditEvent> uniqueEvents =
                AuditManager.findEvents(group, "type1", "operation1", 10, true);
        assertEquals("Not getting unique events", 1, uniqueEvents.size());
        AuditManager.clearEvents();
    }

    @Test
    public void testFindMaxEvents() throws HopException {
        String group = "testFindMaxEvents";
        IAuditManager mockManager = Mockito.mock(IAuditManager.class);
        AuditManager.getInstance().setActiveAuditManager(mockManager);

        List<AuditEvent> events = new ArrayList<>();
        events.add(new AuditEvent(group, "type1", "name1", "operation1", new Date()));
        events.add(new AuditEvent(group, "type1", "name2", "operation1", new Date()));
        events.add(new AuditEvent(group, "type1", "name3", "operation1", new Date()));
        events.add(new AuditEvent(group, "type1", "name4", "operation1", new Date()));
        when(mockManager.findEvents(group, "type1", false)).thenReturn(events);
        List<AuditEvent> maxEvents =
                AuditManager.findEvents(group, "type1", "operation1", 2, false);
        assertEquals("Not getting unique events", 2, maxEvents.size());
    }

    @Test
    public void testClearEvents() throws HopException {
        AuditManager.getInstance().setActiveAuditManager(new LocalAuditManager(testFolder.getRoot().getAbsolutePath()));
        String group = "testClearEvents";
        AuditManager.registerEvent(group, "type1", "name1", "operation1");
        AuditManager.registerEvent(group, "type1", "name1", "operation1");
        assertEquals(
                "Problem in registering event",
                2,
                AuditManager.findEvents(group, "type1", "operation1", 10, false).size());
        AuditManager.clearEvents();
        assertEquals(
                "Problem in clearning event",
                0,
                AuditManager.findEvents(group, "type1", "operation1", 10, false).size());
    }
}
