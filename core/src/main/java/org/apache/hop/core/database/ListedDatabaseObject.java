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

package org.apache.hop.core.database;

/**
 * A table or view as listed by the JDBC metadata, with the name that {@link
 * Database#getTablenames(String, boolean)} and {@link Database#getViews(String, boolean)} return
 * for it.
 *
 * @param object the catalog, schema and unquoted name, for lookups that need them apart
 * @param outputName the name to show or write, with the schema and quoting of the database
 */
public record ListedDatabaseObject(DatabaseObjectName object, String outputName) {}
