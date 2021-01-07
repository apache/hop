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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;

import java.util.HashMap;
import java.util.Map;

/**
 * This event handler contains event listeners per event ID and GUI ID. Suggestions for event IDs
 * are present in {@link HopGuiEvents} The GUI ID can be anything you want but we suggest using the
 * class name of the dialog or perspective you're using.
 *
 * <p>When you no longer need to listener for events make sure to remove a listener when you close
 * GUI component like a dialog for example. This way you're not going to try to update a dialog
 * which was disposed off earlier.
 */
public class HopGuiEventsHandler {

  private Map<String, Map<String, IHopGuiEventListener>> guiEventListenerMap;

  public HopGuiEventsHandler() {
    this.guiEventListenerMap = new HashMap<>();
  }

  public <T> void addEventListener(
      String guiId, IHopGuiEventListener<T> listener, String... eventIds) {
    if (StringUtils.isEmpty(guiId)) {
      throw new RuntimeException(
          "Please provide a GUI id to allow you to remove the listener when you're no longer need it");
    }
    if (eventIds == null || eventIds.length == 0) {
      throw new RuntimeException(
          "Please provide an event ID so we know what to be on the lookout for");
    }
    for (String eventId : eventIds) {
      if (StringUtils.isEmpty(eventId)) {
        throw new RuntimeException("Please only use event IDs which are not null or empty");
      }
    }

    // This gives us the map for the GUI component
    //
    Map<String, IHopGuiEventListener> eventListenerMap = guiEventListenerMap.get(guiId);
    if (eventListenerMap == null) {
      eventListenerMap = new HashMap<>();
      guiEventListenerMap.put(guiId, eventListenerMap);
    }

    // Add the listener to all the event IDs
    //
    for (String eventId : eventIds) {
      eventListenerMap.put(eventId, listener);
    }
  }

  /**
   * Remove the event listeners for the given GUI ID and event IDs
   *
   * @param guiId
   * @param eventIds
   */
  public void removeEventListener(String guiId, String... eventIds) {
    if (StringUtils.isEmpty(guiId)) {
      throw new RuntimeException(
          "Please provide a GUI ID of the component you're done with being informed about.");
    }
    if (eventIds == null || eventIds.length == 0) {
      throw new RuntimeException("Please provide the event IDs of the listeners to remove.");
    }
    for (String eventId : eventIds) {
      if (StringUtils.isEmpty(eventId)) {
        throw new RuntimeException("Please only use event IDs which are not null or empty");
      }
    }

    // This gives us the map for the GUI component
    //
    Map<String, IHopGuiEventListener> eventListenerMap = guiEventListenerMap.get(guiId);
    if (eventListenerMap == null) {
      return; // nothing to remove
    }

    // Add the listener to all the event IDs
    //
    for (String eventId : eventIds) {
      eventListenerMap.remove(eventId);
    }

    // If the map is empty, remove it to reduce leakage
    //
    if (eventListenerMap.isEmpty()) {
      guiEventListenerMap.remove(guiId);
    }
  }

  /**
   * Remove all the event listeners for the given GUI ID
   *
   * @param guiId The unique ID of the GUI component
   */
  public void removeEventListeners(String guiId) {
    if (StringUtils.isEmpty(guiId)) {
      throw new RuntimeException(
          "Please provide a GUI ID of the component you're done with being informed about.");
    }

    // We simply remove all the listeners from our GUI event listeners map
    //
    guiEventListenerMap.remove(guiId);
  }

  /**
   * Fire on one or more of the specified events with the provided subject.
   *
   * @param eventSubject The subject to pass in the event
   * @param all Set to true if you want to fire on all the given eventIds. Use false if you only
   *     want to stop after you found the first registered listener
   * @param eventIds The event IDs to look up
   * @param <T> The type of the event subject
   * @throws HopException In case something goes wrong executing the listener lambdas.
   */
  public <T> void fire(T eventSubject, boolean all, String... eventIds) throws HopException {
    if (eventIds == null || eventIds.length == 0) {
      throw new RuntimeException("Please provide the IDs of the events you want to fire");
    }
    for (String eventId : eventIds) {
      if (StringUtils.isEmpty(eventId)) {
        throw new RuntimeException("Please only use event IDs which are not null or empty");
      }
    }

    // Execute the listeners for the given event IDs in all the parts of the GUI
    //
    for (Map<String, IHopGuiEventListener> eventListenerMap : guiEventListenerMap.values()) {
      for (String eventId : eventIds) {
        IHopGuiEventListener<T> listener = eventListenerMap.get(eventId);
        if (listener != null) {
          // We found the listener for the event ID!   Fire it!
          //
          listener.event(new HopGuiEvent<>(eventId, eventSubject));
          if (!all) {
            break; // Only execute once per GUI component
          }
        }
      }
    }
  }

  /**
   * Fire one or more events without a subject.
   *
   * @param all Set to true if you want to fire on all the given eventIds. Use false if you only
   *     want to stop after you found the first registered listener
   * @param eventIds The event IDs to look up
   * @throws HopException In case something goes wrong executing the listener lambdas.
   */
  public void fire(boolean all, String... eventIds) throws HopException {
    fire(null, all, eventIds);
  }

  /**
   * Fire once for a list of possible events once with the provided subject. Stop executing after
   * the first event is found and the listener is executed.
   *
   * @param eventSubject The subject to pass in the event
   * @param eventIds The event IDs to look up
   * @param <T> The type of the event subject
   * @throws HopException In case something goes wrong executing the listener lambda.
   */
  public <T> void fire(T eventSubject, String... eventIds) throws HopException {
    fire(eventSubject, false, eventIds);
  }

  /**
   * Fire once for a list of possible events once without a provided subject. Stop executing after
   * the first event is found and the listener is executed.
   *
   * @param eventIds The event IDs to look up
   * @throws HopException In case something goes wrong executing the listener lambda.
   */
  public void fire(String... eventIds) throws HopException {
    fire(false, eventIds);
  }

  /**
   * Gets guiEventListenerMap
   *
   * @return value of guiEventListenerMap
   */
  public Map<String, Map<String, IHopGuiEventListener>> getGuiEventListenerMap() {
    return guiEventListenerMap;
  }

  /** @param guiEventListenerMap The guiEventListenerMap to set */
  public void setGuiEventListenerMap(
      Map<String, Map<String, IHopGuiEventListener>> guiEventListenerMap) {
    this.guiEventListenerMap = guiEventListenerMap;
  }
}
