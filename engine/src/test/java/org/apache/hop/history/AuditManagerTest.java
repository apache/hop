/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.hop.history;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.junit.Test;
import org.mockito.Mockito;

public class AuditManagerTest {

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
        IAuditManager mockManager = Mockito.mock(IAuditManager.class);
        AuditManager.getInstance().setActiveAuditManager(mockManager);

        List<AuditEvent> events = new ArrayList<>();
        events.add(new AuditEvent("group1", "type1", "name1", "operation1", new Date()));
        events.add(new AuditEvent("group1", "type1", "name1", "operation1", new Date()));

        when(mockManager.findEvents("group1", "type1", false)).thenReturn(events);
        List<AuditEvent> allEvents =
                AuditManager.findEvents("group1", "type1", "operation1", 10, false);
        assertEquals("Not getting all events", 2, allEvents.size());
    }

    @Test
    public void testFindUniqueEvents() throws HopException {
        IAuditManager mockManager = Mockito.mock(IAuditManager.class);
        AuditManager.getInstance().setActiveAuditManager(mockManager);

        List<AuditEvent> events = new ArrayList<>();
        events.add(new AuditEvent("group1", "type1", "name1", "operation1", new Date()));
        events.add(new AuditEvent("group1", "type1", "name1", "operation1", new Date()));

        when(mockManager.findEvents("group1", "type1", true)).thenReturn(events);
        List<AuditEvent> uniqueEvents =
                AuditManager.findEvents("group1", "type1", "operation1", 10, true);
        assertEquals("Not getting unique events", 1, uniqueEvents.size());
    }

    @Test
    public void testFindMaxEvents() throws HopException {
        IAuditManager mockManager = Mockito.mock(IAuditManager.class);
        AuditManager.getInstance().setActiveAuditManager(mockManager);

        List<AuditEvent> events = new ArrayList<>();
        AuditEvent auditEvent =
                new AuditEvent("group1", "type1", "name1", "operation1", new Date());
        events.add(auditEvent);
        events.add(auditEvent);
        events.add(auditEvent);
        when(mockManager.findEvents("group1", "type1", false)).thenReturn(events);
        List<AuditEvent> maxEvents =
                AuditManager.findEvents("group1", "type1", "operation1", 2, false);
        assertEquals("Not getting unique events", 2, maxEvents.size());
    }
}
