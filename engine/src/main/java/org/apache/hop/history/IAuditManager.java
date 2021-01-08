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

import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;

/** This interface describes all the things you can do with a history manager */
public interface IAuditManager {

    /**
     * Add an event
     *
     * @param event
     */
    void storeEvent(AuditEvent event) throws HopException;

    /**
     * Find all the events for a certain group and of a given type.
     *
     * @param group The event group
     * @param type The event type
     * @param unique Set to true if you want to have a unique list by name
     * @return The matching events reverse sorted by event date (last events first).
     */
    List<AuditEvent> findEvents(String group, String type, boolean unique) throws HopException;

    /**
     * Store a list
     *
     * @param group The group to which the list belongs
     * @param type The type of list you want to store
     * @param auditList The list to be stored
     * @throws HopException
     */
    void storeList(String group, String type, AuditList auditList) throws HopException;

    /**
     * Retrieve a list of items of a certain group and type
     *
     * @param group The group to which the list belongs
     * @param type The type of list you want retrieved
     * @return The list
     * @throws HopException
     */
    AuditList retrieveList(String group, String type) throws HopException;

    /**
     * Store the state of an object
     *
     * @param group the group to which the state belongs (namespace, environment, ...)
     * @param type the type of state (shell, perspective, ...)
     * @param auditState The audit state
     * @throws HopException In case something goes wrong
     */
    void storeState(String group, String type, AuditState auditState) throws HopException;

    /**
     * Retrieve the state of an object with a particular name
     *
     * @param group The group (namespace, environment)
     * @param type The type (file, window, perspective,...)
     * @param name filename, shell name, ...
     * @return The state
     * @throws HopException in case something goes wrong
     */
    AuditState retrieveState(String group, String type, String name) throws HopException;

    /**
     * Load the audit state map of a map of objects
     *
     * @param group
     * @param type
     * @return
     * @throws HopException
     */
    AuditStateMap loadAuditStateMap(String group, String type) throws HopException;

    /**
     * Save the given audit state map for a map of objects.
     *
     * @param group
     * @param type
     * @param auditStateMap
     * @throws HopException
     */
    void saveAuditStateMap(String group, String type, AuditStateMap auditStateMap)
            throws HopException;

    /**
     * Load a map of String values for a given group for a given type
     *
     * @param group
     * @param type
     * @return The map
     * @throws HopException
     */
    Map<String, String> loadMap(String group, String type) throws HopException;

    /**
     * Store a Strings Map
     *
     * @param group
     * @param type
     * @param map The map to save
     * @throws HopException
     */
    void saveMap(String group, String type, Map<String, String> map) throws HopException;

    /**
     * Clear all events.
     *
     * @throws HopException
     */
    void clearEvents() throws HopException;
}
