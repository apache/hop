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

package org.apache.hop.ui.hopgui.search.config;

import lombok.Getter;
import lombok.Setter;

/**
 * Persistent Hop GUI search settings. Stored under {@link #HOP_CONFIG_SEARCH_KEY} in hop-config.
 *
 * <p>Large data files (million-row CSV/JSON, …) are intentionally excluded from GUI search via
 * {@link #maxTextFileSizeMb}; use dedicated tools for those.
 */
@Getter
@Setter
public class SearchConfig {

  public static final String HOP_CONFIG_SEARCH_KEY = "search";

  public static final String DEFAULT_MIN_CONTENT_QUERY_LENGTH = "3";
  public static final String DEFAULT_MAX_RESULTS = "500";
  public static final String DEFAULT_MAX_MATCHES_PER_FILE = "20";
  public static final String DEFAULT_MAX_TEXT_FILE_SIZE_MB = "1";
  public static final String DEFAULT_DEBOUNCE_MS = "300";

  /** Minimum characters before file/content matching runs (names still match below this). */
  private String minContentQueryLength;

  /** Hard cap on total search results built during analysis (GUI only). */
  private String maxResults;

  /** Cap on content matches reported per text file. */
  private String maxMatchesPerFile;

  /**
   * Maximum size in megabytes of a text file that may be loaded for content search. Larger files
   * are skipped entirely.
   */
  private String maxTextFileSizeMb;

  /**
   * When false, project text files (CSV, JSON, …) are not content-searched; open files still are.
   */
  private Boolean includeProjectTextFiles;

  /** When true, search runs while typing (debounced); Enter always searches. */
  private Boolean searchAsYouType;

  /** Debounce in milliseconds between keystrokes and running a live search. */
  private String debounceMs;

  public SearchConfig() {
    this.minContentQueryLength = DEFAULT_MIN_CONTENT_QUERY_LENGTH;
    this.maxResults = DEFAULT_MAX_RESULTS;
    this.maxMatchesPerFile = DEFAULT_MAX_MATCHES_PER_FILE;
    this.maxTextFileSizeMb = DEFAULT_MAX_TEXT_FILE_SIZE_MB;
    this.includeProjectTextFiles = true;
    this.searchAsYouType = true;
    this.debounceMs = DEFAULT_DEBOUNCE_MS;
  }

  public SearchConfig(SearchConfig other) {
    this();
    if (other == null) {
      return;
    }
    this.minContentQueryLength = other.minContentQueryLength;
    this.maxResults = other.maxResults;
    this.maxMatchesPerFile = other.maxMatchesPerFile;
    this.maxTextFileSizeMb = other.maxTextFileSizeMb;
    this.includeProjectTextFiles = other.includeProjectTextFiles;
    this.searchAsYouType = other.searchAsYouType;
    this.debounceMs = other.debounceMs;
  }
}
