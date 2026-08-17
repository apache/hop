/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.widget;

import java.util.Locale;
import org.apache.commons.lang3.StringUtils;

/** Pure string helpers for find / replace in an {@link IFindReplaceTarget}. */
public final class TextFindSupport {

  private TextFindSupport() {}

  /**
   * Find the next occurrence of {@code query} in {@code text} at or after {@code from}.
   *
   * @return start index of the match, or -1 if not found
   */
  public static int findNext(String text, String query, int from, boolean caseSensitive) {
    if (StringUtils.isEmpty(text) || StringUtils.isEmpty(query)) {
      return -1;
    }
    int start = Math.max(0, from);
    if (start > text.length()) {
      return -1;
    }
    if (caseSensitive) {
      return text.indexOf(query, start);
    }
    return text.toLowerCase(Locale.ROOT).indexOf(query.toLowerCase(Locale.ROOT), start);
  }

  /**
   * Find the previous occurrence of {@code query} in {@code text} ending at or before {@code from}.
   *
   * @return start index of the match, or -1 if not found
   */
  public static int findPrevious(String text, String query, int from, boolean caseSensitive) {
    if (StringUtils.isEmpty(text) || StringUtils.isEmpty(query) || from < 0) {
      return -1;
    }
    int end = Math.min(text.length(), from);
    if (caseSensitive) {
      return text.lastIndexOf(query, end);
    }
    return text.toLowerCase(Locale.ROOT).lastIndexOf(query.toLowerCase(Locale.ROOT), end);
  }

  /**
   * Replace every occurrence of {@code query} with {@code replacement}.
   *
   * @return number of replacements performed
   */
  public static ReplaceAllResult replaceAll(
      String text, String query, String replacement, boolean caseSensitive) {
    if (StringUtils.isEmpty(text) || StringUtils.isEmpty(query)) {
      return new ReplaceAllResult(text, 0);
    }
    String safeReplacement = replacement != null ? replacement : "";
    if (caseSensitive) {
      int count = 0;
      int from = 0;
      StringBuilder result = new StringBuilder();
      int idx;
      while ((idx = text.indexOf(query, from)) >= 0) {
        result.append(text, from, idx);
        result.append(safeReplacement);
        from = idx + query.length();
        count++;
      }
      result.append(text, from, text.length());
      return new ReplaceAllResult(result.toString(), count);
    }

    String lowerText = text.toLowerCase(Locale.ROOT);
    String lowerQuery = query.toLowerCase(Locale.ROOT);
    int count = 0;
    int from = 0;
    StringBuilder result = new StringBuilder();
    int idx;
    while ((idx = lowerText.indexOf(lowerQuery, from)) >= 0) {
      result.append(text, from, idx);
      result.append(safeReplacement);
      from = idx + query.length();
      count++;
    }
    result.append(text, from, text.length());
    return new ReplaceAllResult(result.toString(), count);
  }

  public record ReplaceAllResult(String text, int count) {}
}
