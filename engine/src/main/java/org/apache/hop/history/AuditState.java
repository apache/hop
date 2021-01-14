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
import java.util.Objects;

/** This class allows you to describe the state of objects like loaded files, windows and so on */
public class AuditState {

    // The name of the parent (filename, window name, ...)
    private String name;

    // The various states of the parent properties (width, height, active, zoom, ...)
    //
    private Map<String, Object> stateMap;

    public AuditState() {
        stateMap = new HashMap<>();
    }

    public AuditState(String name, Map<String, Object> stateMap) {
        this.name = name;
        this.stateMap = stateMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AuditState that = (AuditState) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
        return name;
    }

    /** @param name The name to set */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets stateMap
     *
     * @return value of stateMap
     */
    public Map<String, Object> getStateMap() {
        return stateMap;
    }

    /** @param stateMap The stateMap to set */
    public void setStateMap(Map<String, Object> stateMap) {
        this.stateMap = stateMap;
    }
}
