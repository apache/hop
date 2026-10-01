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

package org.apache.hop.ui.core.widget;

import org.apache.commons.lang3.StringUtils;

/**
 * Headless find / replace steps used by {@link org.apache.hop.ui.core.dialog.FindReplaceDialog}.
 */
public final class FindReplaceOperations {

  private FindReplaceOperations() {}

  /**
   * Find the next or previous match, select it, and focus the editor.
   *
   * @return {@code true} when a match was selected
   */
  public static boolean find(
      IFindReplaceTarget target, String query, boolean caseSensitive, boolean forward) {
    if (target == null || target.isDisposed() || StringUtils.isEmpty(query)) {
      return false;
    }
    String content = target.getText();
    if (content == null) {
      content = "";
    }
    int caret = target.getCaretPosition();
    int selectionLen = target.getSelectionCount();

    int found;
    if (forward) {
      int from = caret;
      String selected = target.getSelectionText();
      if (selectionLen > 0 && matches(selected, query, caseSensitive)) {
        from = caret;
      }
      found = TextFindSupport.findNext(content, query, from, caseSensitive);
      if (found < 0 && from > 0) {
        found = TextFindSupport.findNext(content, query, 0, caseSensitive);
      }
    } else {
      int from = caret - selectionLen - 1;
      if (from < 0) {
        from = content.length();
      }
      found = TextFindSupport.findPrevious(content, query, from, caseSensitive);
      if (found < 0) {
        found = TextFindSupport.findPrevious(content, query, content.length(), caseSensitive);
      }
    }

    if (found < 0) {
      return false;
    }
    target.setSelection(found, found + query.length());
    target.setFocus();
    return true;
  }

  /**
   * Replace the current selection when it matches {@code query}, then find the next match.
   *
   * @return {@code true} when a following match was selected
   */
  public static boolean replaceOne(
      IFindReplaceTarget target, String query, String replacement, boolean caseSensitive) {
    if (target == null || target.isDisposed() || !target.isEditable()) {
      return false;
    }
    if (StringUtils.isEmpty(query)) {
      return false;
    }
    if (matches(target.getSelectionText(), query, caseSensitive)) {
      target.insert(replacement != null ? replacement : "");
      target.updateToolbar();
    }
    return find(target, query, caseSensitive, true);
  }

  /**
   * Replace every occurrence of {@code query}.
   *
   * @return number of replacements performed
   */
  public static int replaceAll(
      IFindReplaceTarget target, String query, String replacement, boolean caseSensitive) {
    if (target == null || target.isDisposed() || !target.isEditable()) {
      return 0;
    }
    if (StringUtils.isEmpty(query)) {
      return 0;
    }
    TextFindSupport.ReplaceAllResult result =
        TextFindSupport.replaceAll(target.getText(), query, replacement, caseSensitive);
    if (result.count() > 0) {
      int caret = target.getCaretPosition();
      target.setText(result.text());
      target.setCaretPosition(Math.min(caret, result.text().length()));
      target.updateToolbar();
    }
    return result.count();
  }

  static boolean matches(String value, String query, boolean caseSensitive) {
    if (value == null || query == null) {
      return false;
    }
    return caseSensitive ? value.equals(query) : value.equalsIgnoreCase(query);
  }
}
