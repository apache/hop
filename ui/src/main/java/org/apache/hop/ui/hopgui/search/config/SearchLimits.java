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
import org.apache.hop.core.Const;

/**
 * Parsed, immutable snapshot of {@link SearchConfig} for one search run. Defaults match the product
 * safety limits so the GUI never attempts million-row data-file content search.
 */
@Getter
public final class SearchLimits {

  public static final int DEFAULT_MIN_CONTENT_QUERY_LENGTH =
      Integer.parseInt(SearchConfig.DEFAULT_MIN_CONTENT_QUERY_LENGTH);
  public static final int DEFAULT_MAX_RESULTS = Integer.parseInt(SearchConfig.DEFAULT_MAX_RESULTS);
  public static final int DEFAULT_MAX_MATCHES_PER_FILE =
      Integer.parseInt(SearchConfig.DEFAULT_MAX_MATCHES_PER_FILE);
  public static final int DEFAULT_MAX_TEXT_FILE_SIZE_MB =
      Integer.parseInt(SearchConfig.DEFAULT_MAX_TEXT_FILE_SIZE_MB);
  public static final int DEFAULT_DEBOUNCE_MS = Integer.parseInt(SearchConfig.DEFAULT_DEBOUNCE_MS);

  private final int minContentQueryLength;
  private final int maxResults;
  private final int maxMatchesPerFile;
  private final long maxTextFileSizeBytes;
  private final boolean includeProjectTextFiles;
  private final boolean searchAsYouType;
  private final int debounceMs;

  private SearchLimits(
      int minContentQueryLength,
      int maxResults,
      int maxMatchesPerFile,
      long maxTextFileSizeBytes,
      boolean includeProjectTextFiles,
      boolean searchAsYouType,
      int debounceMs) {
    this.minContentQueryLength = Math.max(1, minContentQueryLength);
    this.maxResults = Math.max(1, maxResults);
    this.maxMatchesPerFile = Math.max(1, maxMatchesPerFile);
    this.maxTextFileSizeBytes = Math.max(0L, maxTextFileSizeBytes);
    this.includeProjectTextFiles = includeProjectTextFiles;
    this.searchAsYouType = searchAsYouType;
    this.debounceMs = Math.max(0, debounceMs);
  }

  public static SearchLimits defaults() {
    return fromConfig(new SearchConfig());
  }

  public static SearchLimits fromConfig() {
    return fromConfig(SearchConfigSingleton.getConfig());
  }

  public static SearchLimits fromConfig(SearchConfig config) {
    SearchConfig c = config == null ? new SearchConfig() : config;
    int minLen = (int) Const.toLong(c.getMinContentQueryLength(), DEFAULT_MIN_CONTENT_QUERY_LENGTH);
    int maxResults = (int) Const.toLong(c.getMaxResults(), DEFAULT_MAX_RESULTS);
    int maxPerFile = (int) Const.toLong(c.getMaxMatchesPerFile(), DEFAULT_MAX_MATCHES_PER_FILE);
    int maxMb = (int) Const.toLong(c.getMaxTextFileSizeMb(), DEFAULT_MAX_TEXT_FILE_SIZE_MB);
    int debounce = (int) Const.toLong(c.getDebounceMs(), DEFAULT_DEBOUNCE_MS);
    boolean includeText = c.getIncludeProjectTextFiles() == null || c.getIncludeProjectTextFiles();
    boolean asYouType = c.getSearchAsYouType() == null || c.getSearchAsYouType();
    return new SearchLimits(
        minLen, maxResults, maxPerFile, maxMb * 1024L * 1024L, includeText, asYouType, debounce);
  }

  /**
   * Whether content (including text-file line matching) is allowed for this query string. Name
   * matching is always allowed.
   */
  public boolean allowsContentSearch(String query) {
    if (query == null) {
      return false;
    }
    return query.trim().length() >= minContentQueryLength;
  }
}
