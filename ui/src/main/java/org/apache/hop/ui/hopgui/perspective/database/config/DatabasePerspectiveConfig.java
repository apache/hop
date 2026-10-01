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

package org.apache.hop.ui.hopgui.perspective.database.config;

import lombok.Getter;
import lombok.Setter;

/** Persisted Database perspective options (hop-config.json). */
@Getter
@Setter
public class DatabasePerspectiveConfig {

  public static final String HOP_CONFIG_KEY = "database-perspective";

  /** Default cap on rows shown for a SELECT in the SQL editor and for table preview. */
  public static final int DEFAULT_QUERY_ROW_LIMIT = 1000;

  /**
   * When true, executing SQL connects the tab's database without asking if the tree is not already
   * connected.
   */
  private boolean autoConnectWhenExecutingSql;

  /** When true, the SQL editor selects the statement(s) that were just executed. */
  private boolean selectExecutedSql = true;

  /**
   * Maximum rows shown for a query or table preview. A SQL {@code LIMIT} above this value is still
   * capped here (JDBC {@code setMaxRows}). Values {@code <= 0} fall back to {@link
   * #DEFAULT_QUERY_ROW_LIMIT}.
   */
  private Integer queryRowLimit = DEFAULT_QUERY_ROW_LIMIT;

  public DatabasePerspectiveConfig() {}

  public DatabasePerspectiveConfig(DatabasePerspectiveConfig other) {
    this.autoConnectWhenExecutingSql = other.autoConnectWhenExecutingSql;
    this.selectExecutedSql = other.selectExecutedSql;
    this.queryRowLimit = other.queryRowLimit;
  }

  /** Positive row cap, never {@code 0} (that would mean unlimited in {@code Database.getRows}). */
  public int resolvedQueryRowLimit() {
    if (queryRowLimit == null || queryRowLimit <= 0) {
      return DEFAULT_QUERY_ROW_LIMIT;
    }
    return queryRowLimit;
  }
}
