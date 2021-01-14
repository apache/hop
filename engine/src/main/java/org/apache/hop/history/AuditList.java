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

import java.util.ArrayList;
import java.util.List;

/**
 * This is simply a list of things you want to store using the audit manager. You can store a list
 * of filenames for example. You can store the list per group and you can specify the type of list.
 */
public class AuditList {

    private List<String> names;

    public AuditList() {
        names = new ArrayList<>();
    }

    public AuditList(List<String> names) {
        this.names = names;
    }

    /**
     * Gets names
     *
     * @return value of names
     */
    public List<String> getNames() {
        return names;
    }

    /** @param names The names to set */
    public void setNames(List<String> names) {
        this.names = names;
    }
}
