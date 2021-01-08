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

import java.util.HashMap;
import java.util.Map;

public class AuditStateMap {
    private Map<String, AuditState> nameStateMap;

    public AuditStateMap() {
        nameStateMap = new HashMap<>();
    }

    public AuditStateMap(Map<String, AuditState> nameStateMap) {
        this.nameStateMap = nameStateMap;
    }

    /**
     * Gets nameStateMap
     *
     * @return value of nameStateMap
     */
    public Map<String, AuditState> getNameStateMap() {
        return nameStateMap;
    }

    /** @param nameStateMap The nameStateMap to set */
    public void setNameStateMap(Map<String, AuditState> nameStateMap) {
        this.nameStateMap = nameStateMap;
    }

    public void add(AuditState auditState) {
        AuditState existing = get(auditState.getName());
        if (existing != null) {
            // Add the states to the existing states
            //
            existing.getStateMap().putAll(auditState.getStateMap());
        } else {
            // Store a new set of properties
            //
            nameStateMap.put(auditState.getName(), auditState);
        }
    }

    public AuditState get(String name) {
        return nameStateMap.get(name);
    }
}
