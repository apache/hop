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

package org.apache.hop.ui.core.bus;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class HopGuiEventsHandlerTest {

  private HopGuiEventsHandler events;

  @Before
  public void setup() throws Exception {
    events = new HopGuiEventsHandler();
  }

  @Test
  public void testAddRemoveListener() throws Exception {
    Map<String, Map<String, IHopGuiEventListener>> guiEventListenerMap =
        events.getGuiEventListenerMap();

    events.addEventListener("guiId-1", e -> {}, "eventId-1");

    Map<String, IHopGuiEventListener> eventListenerMap = guiEventListenerMap.get("guiId-1");
    assertNotNull(eventListenerMap);
    assertFalse(eventListenerMap.isEmpty());

    events.removeEventListener("guiId-1", "eventId-1");
    assertTrue(eventListenerMap.isEmpty());

    // See that the sub-map is cleaned out
    //
    assertNull(guiEventListenerMap.get("guiId-1"));
  }

  @Test
  public void testAddRemoveListeners() throws Exception {
    Map<String, Map<String, IHopGuiEventListener>> guiEventListenerMap =
      events.getGuiEventListenerMap();

    events.addEventListener("guiId-1", e -> {}, "eventId-1");
    events.addEventListener("guiId-1", e -> {}, "eventId-2");
    events.addEventListener("guiId-1", e -> {}, "eventId-3", "eventId-4");

    Map<String, IHopGuiEventListener> eventListenerMap = guiEventListenerMap.get("guiId-1");
    assertNotNull(eventListenerMap);
    assertEquals(4, eventListenerMap.size());

    events.removeEventListeners("guiId-1");

    // See that this map is cleaned out
    //
    assertNull(guiEventListenerMap.get("guiId-1"));
  }

  @Test
  public void testFiringListeners() throws Exception {
    AtomicInteger counter = new AtomicInteger(0);

    events.<AtomicInteger>addEventListener(
        "guiId-1", e -> e.getSubject().incrementAndGet(), "eventId-1");
    events.fire(counter, "eventId-1");

    assertEquals(1, counter.get());
  }

  @Test
  public void testAddEventExceptions() throws Exception {
    // The GUI ID can't be null or empty
    //
    assertThrows(RuntimeException.class, () -> events.addEventListener(null, e -> {}, "eventId-1"));
    assertThrows(RuntimeException.class, () -> events.addEventListener("", e -> {}, "eventId-1"));

    // We need at least 1 non-null event ID
    //
    assertThrows(RuntimeException.class, () -> events.addEventListener("guiId-1", e -> {}));
    assertThrows(RuntimeException.class, () -> events.addEventListener("guiId-1", e -> {}, null));
    assertThrows(RuntimeException.class, () -> events.addEventListener("guiId-1", e -> {}, ""));
  }

  @Test
  public void testFireEventExceptions() throws Exception {
    // The GUI ID can't be null or empty
    //
    assertThrows(RuntimeException.class, () -> events.fire());
    assertThrows(RuntimeException.class, () -> events.fire(""));
    assertThrows(RuntimeException.class, () -> events.fire(null));
  }

  @Test
  public void testFiringMultipleListenerOnOneOrAllEvents() throws Exception {

    AtomicInteger counter = new AtomicInteger(0);
    List<String> eventIdList = new ArrayList<>();

    // We want to refresh something when any of a list of events happens...
    // Multiple places in the code might add a refresh method for different events
    //
    IHopGuiEventListener<AtomicInteger> listener =
        event -> {
          event.getSubject().incrementAndGet();
          eventIdList.add(event.getId());
        };
    events.addEventListener("guiId-1", listener, "eventId-1");
    events.addEventListener("guiId-1", listener, "eventId-2");
    events.addEventListener("guiId-1", listener, "eventId-3");

    // Fire once when one of the 3 events is fired.
    //
    events.fire(counter, "eventId-2", "eventId-1", "eventId-3");
    assertEquals(1, counter.get());
    assertEquals(1, eventIdList.size());
    assertEquals("eventId-2", eventIdList.get(0));

    // Fire 2 more events
    //
    events.fire(counter, true, "eventId-2", "eventId-1");
    assertEquals(3, counter.get());
    assertEquals(3, eventIdList.size());
    assertEquals("eventId-2", eventIdList.get(1));
    assertEquals("eventId-1", eventIdList.get(2));
  }

  @Test
  public void testFiringOneListenerOnOneOrAllEvents() throws Exception {

    AtomicInteger counter = new AtomicInteger(0);
    List<String> eventIdList = new ArrayList<>();

    // We want to refresh something when any of a list of events happens...
    // This listener is usually registered once when the GUI is initialized (perspective)
    //
    IHopGuiEventListener<AtomicInteger> listener =
        event -> {
          event.getSubject().incrementAndGet();
          eventIdList.add(event.getId());
        };
    events.addEventListener("guiId-1", listener, "eventId-1", "eventId-2", "eventId-3");

    // Fire once
    //
    events.fire(counter, "eventId-3", "eventId-1", "eventId-1");
    assertEquals(1, counter.get());
    assertEquals(1, eventIdList.size());
    assertEquals("eventId-3", eventIdList.get(0));

    // Fire 2 more events
    //
    events.fire(counter, true, "eventId-2", "eventId-1");
    assertEquals(3, counter.get());
    assertEquals(3, eventIdList.size());
    assertEquals("eventId-2", eventIdList.get(1));
    assertEquals("eventId-1", eventIdList.get(2));
  }
}
