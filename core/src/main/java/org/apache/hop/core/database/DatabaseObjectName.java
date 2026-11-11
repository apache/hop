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

import org.apache.hop.core.util.Utils;

/**
 * A table or view as listed by the JDBC metadata, with its catalog and schema kept apart.
 *
 * @param catalog the TABLE_CAT value, may be null or empty
 * @param schema the TABLE_SCHEM value, may be null or empty
 * @param name the unquoted table or view name
 */
public record DatabaseObjectName(String catalog, String schema, String name) {

  /**
   * The schema, or the catalog when the object has no schema (MySQL, MariaDB, ...). This is the key
   * that {@link Database#getTableMap(String)} and {@link Database#getViewMap(String)} use.
   */
  public String schemaOrCatalog() {
    return Utils.isEmpty(schema) ? catalog : schema;
  }
}
