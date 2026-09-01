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

package org.apache.hop.ui.hopgui.perspective.database;

/**
 * Picks the SQL text to execute from an editor buffer. A non-blank selection wins; otherwise the
 * current statement is the blank-line-delimited block around the caret. Semicolon splitting is left
 * to {@link org.apache.hop.core.database.IDatabase#getSqlScriptStatements(String)}.
 */
public final class SqlExecuteRange {

  private SqlExecuteRange() {}

  /**
   * @param text full editor text (may be {@code null})
   * @param selection selected text (may be {@code null} or blank)
   * @param caretOffset caret when there is no selection
   * @return script to run, never {@code null}
   */
  public static String scriptToExecute(String text, String selection, int caretOffset) {
    if (selection != null && !selection.isBlank()) {
      return selection;
    }
    return blankLineBlock(text == null ? "" : text, caretOffset);
  }

  static String blankLineBlock(String text, int caretOffset) {
    if (text.isEmpty()) {
      return "";
    }
    int caret = Math.max(0, Math.min(caretOffset, text.length()));

    int lineStart = startOfLine(text, caret);
    int lineEnd = endOfLine(text, caret);

    if (isBlank(text, lineStart, lineEnd)) {
      int aboveEnd = previousNonBlankLineEnd(text, lineStart);
      if (aboveEnd >= 0) {
        lineStart = startOfLine(text, aboveEnd);
        lineEnd = endOfLine(text, aboveEnd);
      } else {
        int belowStart =
            nextNonBlankLineStart(text, lineEnd < text.length() ? lineEnd + 1 : lineEnd);
        if (belowStart < 0) {
          return "";
        }
        lineStart = belowStart;
        lineEnd = endOfLine(text, belowStart);
      }
    }

    int blockStart = lineStart;
    while (blockStart > 0) {
      int prevEnd = blockStart - 1; // newline before this line
      int prevStart = startOfLine(text, prevEnd);
      if (isBlank(text, prevStart, prevEnd)) {
        break;
      }
      blockStart = prevStart;
    }

    int blockEnd = lineEnd;
    while (blockEnd < text.length()) {
      if (text.charAt(blockEnd) != '\n') {
        blockEnd = endOfLine(text, blockEnd);
        continue;
      }
      int nextStart = blockEnd + 1;
      int nextEnd = endOfLine(text, nextStart);
      if (isBlank(text, nextStart, nextEnd)) {
        break;
      }
      blockEnd = nextEnd;
    }

    return text.substring(blockStart, blockEnd).trim();
  }

  private static int startOfLine(String text, int offset) {
    int i = Math.max(0, Math.min(offset, text.length()));
    while (i > 0 && text.charAt(i - 1) != '\n') {
      i--;
    }
    return i;
  }

  private static int endOfLine(String text, int offset) {
    int i = Math.max(0, Math.min(offset, text.length()));
    while (i < text.length() && text.charAt(i) != '\n') {
      i++;
    }
    return i;
  }

  private static boolean isBlank(String text, int start, int end) {
    for (int i = start; i < end; i++) {
      char c = text.charAt(i);
      if (c != ' ' && c != '\t' && c != '\r') {
        return false;
      }
    }
    return true;
  }

  private static int previousNonBlankLineEnd(String text, int lineStart) {
    int i = lineStart;
    while (i > 0) {
      int prevEnd = i - 1;
      int prevStart = startOfLine(text, prevEnd);
      if (!isBlank(text, prevStart, prevEnd)) {
        return Math.max(prevStart, prevEnd - 1);
      }
      i = prevStart;
    }
    return -1;
  }

  private static int nextNonBlankLineStart(String text, int from) {
    int i = from;
    while (i < text.length()) {
      int end = endOfLine(text, i);
      if (!isBlank(text, i, end)) {
        return i;
      }
      if (end >= text.length()) {
        return -1;
      }
      i = end + 1;
    }
    return -1;
  }
}
