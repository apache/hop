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

import java.util.List;
import org.apache.hop.core.database.SqlQueryClassifier;
import org.apache.hop.core.database.SqlScriptStatement;

/**
 * Picks the SQL text to execute from an editor buffer. A non-blank selection wins; otherwise the
 * current statement is the blank-line-delimited block around the caret. Semicolon splitting is left
 * to {@link org.apache.hop.core.database.IDatabase#getSqlScriptStatements(String)}. {@link
 * #statementsToExecute} then keeps the statement at the caret (Run) or every statement (Run all /
 * selection).
 */
public final class SqlExecuteRange {

  /** Pass to {@link #statementsToExecute} to keep every parsed statement. */
  public static final int ALL_STATEMENTS = -1;

  /**
   * Script to run and its {@code [start, end)} offsets in the editor buffer.
   *
   * @param script SQL to parse and execute, never {@code null}
   * @param start inclusive offset in the editor
   * @param end exclusive offset in the editor
   */
  public record Range(String script, int start, int end) {
    static Range empty() {
      return new Range("", 0, 0);
    }

    boolean isBlank() {
      return script == null || script.isBlank();
    }
  }

  private SqlExecuteRange() {}

  /**
   * @param text full editor text (may be {@code null})
   * @param selection selected text (may be {@code null} or blank)
   * @param caretOffset caret when there is no selection
   * @return script to run, never {@code null}
   */
  public static String scriptToExecute(String text, String selection, int caretOffset) {
    return rangeToExecute(text, selection, caretOffset).script();
  }

  /**
   * Same as {@link #scriptToExecute(String, String, int)} plus the offsets of that script in {@code
   * text}.
   */
  public static Range rangeToExecute(String text, String selection, int caretOffset) {
    String buffer = text == null ? "" : text;
    if (selection != null && !selection.isBlank()) {
      int start = indexOfAroundCaret(buffer, selection, caretOffset);
      if (start < 0) {
        start = 0;
      }
      return new Range(selection, start, start + selection.length());
    }
    return blankLineBlockRange(buffer, caretOffset);
  }

  /**
   * Statements to send to the database.
   *
   * @param caretInEditor caret in the editor buffer, or {@link #ALL_STATEMENTS} for every parsed
   *     statement (selection or Run all)
   */
  public static List<SqlScriptStatement> statementsToExecute(
      Range range, List<SqlScriptStatement> statements, int caretInEditor) {
    if (statements == null || statements.isEmpty()) {
      return List.of();
    }
    if (caretInEditor == ALL_STATEMENTS || range == null) {
      return statements;
    }
    SqlScriptStatement picked = statementAtCaret(range, statements, caretInEditor);
    return picked == null ? statements : List.of(picked);
  }

  /**
   * Statement that contains the caret. If that fragment is not a complete SQL statement (a leftover
   * {@code WHERE} after a semicolon, for example), the previous statement is used.
   */
  static SqlScriptStatement statementAtCaret(
      Range range, List<SqlScriptStatement> statements, int caretInEditor) {
    if (range == null || statements == null || statements.isEmpty()) {
      return null;
    }
    int caret = caretInEditor - range.start();
    int idx = indexContaining(statements, range.script(), caret);
    SqlScriptStatement current = statements.get(idx);
    if (!SqlQueryClassifier.isExecutableStatement(current.getStatement()) && idx > 0) {
      return statements.get(idx - 1);
    }
    return current;
  }

  static int indexContaining(List<SqlScriptStatement> statements, String script, int caret) {
    String buffer = script == null ? "" : script;
    for (int i = 0; i < statements.size(); i++) {
      SqlScriptStatement statement = statements.get(i);
      int from = Math.max(0, statement.getFromIndex());
      int to = exclusiveEnd(buffer, statement);
      if (caret >= from && caret < to) {
        return i;
      }
    }
    if (caret <= 0) {
      return 0;
    }
    return statements.size() - 1;
  }

  static int exclusiveEnd(String script, SqlScriptStatement statement) {
    int to = statement.getToIndex();
    if (to >= 0 && to < script.length() && script.charAt(to) == ';') {
      to++;
    }
    return Math.max(0, to);
  }

  /**
   * Map parsed statement offsets (relative to {@code range.script()}) to editor offsets, including
   * a trailing semicolon when present.
   *
   * @return {@code [start, end)} or {@code null} when there is nothing to select
   */
  public static int[] editorOffsets(Range range, List<SqlScriptStatement> statements) {
    if (range == null || range.isBlank() || statements == null || statements.isEmpty()) {
      return null;
    }
    SqlScriptStatement first = statements.get(0);
    SqlScriptStatement last = statements.get(statements.size() - 1);
    int from = range.start() + Math.max(0, first.getFromIndex());
    int relTo = last.getToIndex();
    String script = range.script();
    if (relTo >= 0 && relTo < script.length() && script.charAt(relTo) == ';') {
      relTo++;
    }
    int to = range.start() + Math.max(0, relTo);
    from = Math.max(range.start(), Math.min(from, range.end()));
    to = Math.max(from, Math.min(to, range.end()));
    if (from == to) {
      return null;
    }
    return new int[] {from, to};
  }

  static Range blankLineBlockRange(String text, int caretOffset) {
    if (text.isEmpty()) {
      return Range.empty();
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
          return Range.empty();
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

    return trimRange(text, blockStart, blockEnd);
  }

  static Range trimRange(String text, int blockStart, int blockEnd) {
    int start = blockStart;
    int end = blockEnd;
    while (start < end && isTrimWhitespace(text.charAt(start))) {
      start++;
    }
    while (end > start && isTrimWhitespace(text.charAt(end - 1))) {
      end--;
    }
    if (start >= end) {
      return Range.empty();
    }
    return new Range(text.substring(start, end), start, end);
  }

  static int indexOfAroundCaret(String text, String selection, int caretOffset) {
    if (text.isEmpty() || selection.isEmpty()) {
      return 0;
    }
    int caret = Math.max(0, Math.min(caretOffset, text.length()));
    int before = caret - selection.length();
    if (before >= 0 && text.startsWith(selection, before)) {
      return before;
    }
    if (caret + selection.length() <= text.length() && text.startsWith(selection, caret)) {
      return caret;
    }
    return text.indexOf(selection);
  }

  private static boolean isTrimWhitespace(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
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
