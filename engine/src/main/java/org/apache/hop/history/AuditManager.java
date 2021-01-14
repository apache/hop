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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.history.local.LocalAuditManager;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuditManager {
    private static AuditManager instance;

    private IAuditManager activeAuditManager;

    private AuditManager() {
        activeAuditManager = new LocalAuditManager();
    }

    public static final AuditManager getInstance() {
        if (instance == null) {
            instance = new AuditManager();
        }
        return instance;
    }

    public static final IAuditManager getActive() {
        return getInstance().getActiveAuditManager();
    }

    /**
     * Gets activeAuditManager
     *
     * @return value of activeAuditManager
     */
    public IAuditManager getActiveAuditManager() {
        return activeAuditManager;
    }

    /** @param activeAuditManager The activeAuditManager to set */
    public void setActiveAuditManager(IAuditManager activeAuditManager) {
        this.activeAuditManager = activeAuditManager;
    }

    // Convenience methods...
    //
    public static final void registerEvent(String group, String type, String name, String operation)
            throws HopException {
        getActive().storeEvent(new AuditEvent(group, type, name, operation, new Date()));
    }

    public static final List<AuditEvent> findEvents(
            String group, String type, String operation, int maxNrEvents, boolean unique)
            throws HopException {
        List<AuditEvent> events =
                getActive()
                        .findEvents(
                                group, type,
                                unique); // We are getting events based on unique or not.
        Set<String> names = new HashSet<>();

        if (operation == null) {
            return events;
        }
        // Filter out the specified operation only (File open)
        //
        List<AuditEvent> operationEvents = new ArrayList<>();
        for (AuditEvent event : events) {
            if (event.getOperation().equalsIgnoreCase(operation)) {

                if (unique) { // Why to check again?
                    if (!names.contains(event.getName())) {
                        operationEvents.add(event);
                        names.add(event.getName());
                    }
                } else {
                    operationEvents.add(event);
                }
                if (maxNrEvents > 0 && operationEvents.size() >= maxNrEvents) {
                    break;
                }
            }
        }
        return operationEvents;
    }

    public static final void storeState(
            ILogChannel log,
            String group,
            String type,
            String name,
            Map<String, Object> stateProperties) {
        AuditState auditState = new AuditState(name, stateProperties);
        try {
            getActive().storeState(group, type, auditState);
        } catch (Exception e) {
            log.logError("Error writing audit state of type " + type, e);
        }
    }

    public static final AuditState retrieveState(
            ILogChannel log, String group, String type, String name) {
        try {
            return getActive().retrieveState(group, type, name);
        } catch (Exception e) {
            log.logError("Error retrieving state of type " + type);
            return null;
        }
    }

    /**
     * Convenience method for clearing events.
     *
     * @throws HopException
     */
    public static final void clearEvents() throws HopException {
        getActive().clearEvents();
    }
}
