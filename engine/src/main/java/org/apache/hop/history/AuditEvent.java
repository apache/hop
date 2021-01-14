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

import java.util.Date;

public class AuditEvent {
    private String group;
    private String type;
    private String name;
    private String operation;
    private Date date;

    public AuditEvent() {
        date = new Date();
    }

    public AuditEvent(String group, String type, String name, String operation, Date date) {
        this();
        this.group = group;
        this.type = type;
        this.name = name;
        this.operation = operation;
        this.date = date;
    }

    /**
     * Gets group
     *
     * @return value of group
     */
    public String getGroup() {
        return group;
    }

    /** @param group The group to set */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * Gets type
     *
     * @return value of type
     */
    public String getType() {
        return type;
    }

    /** @param type The type to set */
    public void setType(String type) {
        this.type = type;
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
     * Gets operation
     *
     * @return value of operation
     */
    public String getOperation() {
        return operation;
    }

    /** @param operation The operation to set */
    public void setOperation(String operation) {
        this.operation = operation;
    }

    /**
     * Gets date
     *
     * @return value of date
     */
    public Date getDate() {
        return date;
    }

    /** @param date The date to set */
    public void setDate(Date date) {
        this.date = date;
    }
}
