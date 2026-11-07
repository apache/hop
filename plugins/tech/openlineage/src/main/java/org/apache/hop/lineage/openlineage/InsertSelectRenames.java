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

package org.apache.hop.lineage.openlineage;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Recovers the target columns of an {@code INSERT INTO t (a, b) SELECT …} statement, so column
 * lineage can name the fields of the written table instead of the expressions that produced them.
 *
 * <p>The SQL parser cannot help: it resolves the source columns of each expression correctly but
 * names the output field after the expression (its alias, the column it reads, or a generated
 * {@code _0}, {@code _1}, … for an unaliased expression), and it returns the list sorted by that
 * name, so the position that would tie it to the INSERT's column list is lost. This reads both
 * lists from the statement itself.
 *
 * <p>Deliberately conservative: the mapping is only produced when both lists can be read with
 * certainty and their sizes agree; anything else (no column list, {@code *}, {@code VALUES},
 * several statements) yields an empty map and the parser's names are left alone. A missed rename
 * costs a column name; a wrong one would attach lineage to the wrong column, so ambiguity always
 * gives up.
 */
final class InsertSelectRenames {

  private InsertSelectRenames() {}

  /** Words that end an unaliased expression rather than introduce an alias. */
  private static final List<String> NOT_AN_ALIAS =
      List.of(
          "end",
          "null",
          "true",
          "false",
          "asc",
          "desc",
          "then",
          "else",
          "when",
          "and",
          "or",
          "not",
          "is",
          "in",
          "like",
          "between",
          "case",
          "distinct",
          "all",
          "over",
          "rows",
          "range",
          "current",
          "date",
          "time",
          "timestamp");

  /**
   * The SELECT-expression name the parser will produce for each target column, or an empty map when
   * the statement is not a single {@code INSERT … SELECT} with a readable target column list.
   */
  static Map<String, String> of(String sql) {
    if (sql == null || sql.isBlank()) {
      return Map.of();
    }
    String text = stripComments(sql);
    List<Span> inserts = topLevelWords(text, "insert");
    if (inserts.size() != 1) {
      return Map.of();
    }
    int pos = skipWhitespace(text, inserts.get(0).end());
    if (!wordAt(text, pos, "into")) {
      return Map.of();
    }
    pos = skipWhitespace(text, pos + "into".length());
    int afterTable = skipIdentifierChain(text, pos);
    if (afterTable < 0) {
      return Map.of();
    }
    pos = skipWhitespace(text, afterTable);
    if (pos >= text.length() || text.charAt(pos) != '(') {
      return Map.of();
    }
    int close = matchingParen(text, pos);
    if (close < 0) {
      return Map.of();
    }
    List<String> targets = splitTopLevel(text.substring(pos + 1, close));
    if (targets.isEmpty()) {
      return Map.of();
    }
    for (String target : targets) {
      if (!isIdentifier(target)) {
        return Map.of();
      }
    }
    pos = skipWhitespace(text, close + 1);
    if (!wordAt(text, pos, "select")) {
      return Map.of();
    }
    int selectStart = pos + "select".length();
    List<String> items =
        splitTopLevel(text.substring(selectStart, endOfSelectList(text, selectStart)));
    if (items.size() != targets.size()) {
      return Map.of();
    }
    Map<String, String> rename = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    int unnamed = 0;
    for (int i = 0; i < items.size(); i++) {
      String item = items.get(i);
      if (isStar(item)) {
        return Map.of();
      }
      String name = outputName(item);
      if (name == null) {
        name = "_" + unnamed;
        unnamed++;
      }
      if (rename.containsKey(name)) {
        // Two expressions with one name: the parser's name cannot be tied to a position.
        return Map.of();
      }
      rename.put(name, unquote(targets.get(i)));
    }
    return rename;
  }

  /** The name the SQL parser gives an output column of a SELECT list: alias, column, or nothing. */
  private static String outputName(String item) {
    String expression = item.trim();
    String alias = explicitAlias(expression);
    if (alias != null) {
      return unquote(alias);
    }
    if (isIdentifier(expression)) {
      return unquote(lastPart(expression));
    }
    alias = implicitAlias(expression);
    return alias == null ? null : unquote(alias);
  }

  /** The alias of {@code expr AS alias} when the AS is at the top level; null otherwise. */
  private static String explicitAlias(String expression) {
    List<Span> as = topLevelWords(expression, "as");
    if (as.isEmpty()) {
      return null;
    }
    for (int i = as.size() - 1; i >= 0; i--) {
      String tail = expression.substring(as.get(i).end()).trim();
      if (isIdentifier(tail)) {
        return tail;
      }
    }
    return null;
  }

  /** The alias of {@code expr alias} (no AS), e.g. {@code sum(x) total}; null when unsure. */
  private static String implicitAlias(String expression) {
    int end = expression.length();
    while (end > 0 && Character.isWhitespace(expression.charAt(end - 1))) {
      end--;
    }
    int start = end;
    if (start > 0
        && (expression.charAt(start - 1) == ']'
            || expression.charAt(start - 1) == '"'
            || expression.charAt(start - 1) == '`')) {
      char quote = expression.charAt(start - 1) == ']' ? '[' : expression.charAt(start - 1);
      int open = lastQuote(expression, start - 1, quote);
      if (open < 0) {
        return null;
      }
      start = open;
    } else {
      while (start > 0 && isIdentifierPart(expression.charAt(start - 1))) {
        start--;
      }
    }
    if (start == 0 || start == end) {
      return null;
    }
    String alias = expression.substring(start, end);
    if (!isIdentifier(alias) || NOT_AN_ALIAS.contains(unquote(alias).toLowerCase(Locale.ROOT))) {
      return null;
    }
    char before = expression.charAt(lastNonSpace(expression, start - 1));
    if (before != ')'
        && before != ']'
        && before != '\''
        && before != '"'
        && before != '`'
        && !Character.isLetterOrDigit(before)
        && before != '_'
        && before != '$') {
      return null;
    }
    return alias;
  }

  private static int lastQuote(String text, int close, char open) {
    for (int i = close - 1; i >= 0; i--) {
      if (text.charAt(i) == open) {
        return i;
      }
    }
    return -1;
  }

  private static int lastNonSpace(String text, int from) {
    int i = from;
    while (i > 0 && Character.isWhitespace(text.charAt(i))) {
      i--;
    }
    return i;
  }

  private static boolean isStar(String item) {
    String trimmed = item.trim();
    if ("*".equals(trimmed)) {
      return true;
    }
    return trimmed.endsWith(".*") && isIdentifier(trimmed.substring(0, trimmed.length() - 2));
  }

  private static boolean isIdentifier(String value) {
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return false;
    }
    int i = 0;
    while (true) {
      int next = identifierEnd(trimmed, i);
      if (next < 0) {
        return false;
      }
      if (next == trimmed.length()) {
        return true;
      }
      if (trimmed.charAt(next) != '.') {
        return false;
      }
      i = next + 1;
    }
  }

  /** Reads one identifier (bare or quoted) at {@code from}; -1 when none starts there. */
  private static int identifierEnd(String text, int from) {
    if (from >= text.length()) {
      return -1;
    }
    char c = text.charAt(from);
    if (c == '"' || c == '`' || c == '[') {
      char close = c == '[' ? ']' : c;
      int i = from + 1;
      while (i < text.length()) {
        if (text.charAt(i) == close) {
          if (i + 1 < text.length() && text.charAt(i + 1) == close) {
            i += 2;
            continue;
          }
          return i + 1;
        }
        i++;
      }
      return -1;
    }
    if (!isIdentifierStart(c)) {
      return -1;
    }
    int i = from + 1;
    while (i < text.length() && isIdentifierPart(text.charAt(i))) {
      i++;
    }
    return i;
  }

  private static boolean isIdentifierStart(char c) {
    return Character.isLetter(c) || c == '_';
  }

  private static boolean isIdentifierPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '#';
  }

  private static int skipIdentifierChain(String text, int from) {
    int i = from;
    while (true) {
      int next = identifierEnd(text, i);
      if (next < 0) {
        return i > from ? i : -1;
      }
      i = next;
      if (i < text.length() && text.charAt(i) == '.') {
        i++;
        continue;
      }
      return i;
    }
  }

  private static String lastPart(String identifier) {
    int depth = 0;
    char quote = 0;
    for (int i = 0; i < identifier.length(); i++) {
      char c = identifier.charAt(i);
      if (quote != 0) {
        if (c == quote) {
          quote = 0;
        }
        continue;
      }
      if (c == '"' || c == '`' || c == '[') {
        quote = c == '[' ? ']' : c;
        continue;
      }
      if (c == '.') {
        depth = i;
      }
    }
    return identifier.substring(depth == 0 ? 0 : depth + 1).trim();
  }

  private static String unquote(String identifier) {
    String trimmed = identifier.trim();
    if (trimmed.length() >= 2) {
      char first = trimmed.charAt(0);
      char last = trimmed.charAt(trimmed.length() - 1);
      if ((first == '"' && last == '"') || (first == '`' && last == '`')) {
        char q = first;
        return trimmed
            .substring(1, trimmed.length() - 1)
            .replace(String.valueOf(q) + q, String.valueOf(q));
      }
      if (first == '[' && last == ']') {
        return trimmed.substring(1, trimmed.length() - 1).replace("]]", "]");
      }
    }
    return trimmed;
  }

  private static int skipWhitespace(String text, int from) {
    int i = from;
    while (i < text.length() && Character.isWhitespace(text.charAt(i))) {
      i++;
    }
    return i;
  }

  private static boolean wordAt(String text, int at, String word) {
    if (at < 0 || at + word.length() > text.length()) {
      return false;
    }
    if (!text.regionMatches(true, at, word, 0, word.length())) {
      return false;
    }
    if (at > 0 && isIdentifierPart(text.charAt(at - 1))) {
      return false;
    }
    int after = at + word.length();
    return after >= text.length() || !isIdentifierPart(text.charAt(after));
  }

  /** Start of the first top-level word that ends the SELECT list. */
  private static int endOfSelectList(String text, int from) {
    List<Span> ends = new ArrayList<>();
    for (String word : List.of("from", "union", "intersect", "except", "minus", "into")) {
      ends.addAll(topLevelWords(text.substring(from), word));
    }
    int end = text.length();
    for (Span span : ends) {
      end = Math.min(end, from + span.start());
    }
    int depth = 0;
    char quote = 0;
    for (int i = from; i < end; i++) {
      char c = text.charAt(i);
      if (quote != 0) {
        if (c == quote) {
          quote = 0;
        }
        continue;
      }
      if (c == '\'' || c == '"' || c == '`' || c == '[') {
        quote = c == '[' ? ']' : c;
      } else if (c == ';') {
        return i;
      }
    }
    return end;
  }

  /** Splits on the commas that are not inside parentheses, quotes or comments. */
  private static List<String> splitTopLevel(String text) {
    List<String> parts = new ArrayList<>();
    int depth = 0;
    char quote = 0;
    int start = 0;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (quote != 0) {
        if (c == quote) {
          quote = 0;
        }
        continue;
      }
      if (c == '\'' || c == '"' || c == '`' || c == '[') {
        quote = c == '[' ? ']' : c;
      } else if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (c == ',' && depth == 0) {
        parts.add(text.substring(start, i).trim());
        start = i + 1;
      }
    }
    parts.add(text.substring(start).trim());
    parts.removeIf(String::isEmpty);
    return parts;
  }

  private static int matchingParen(String text, int open) {
    int depth = 0;
    char quote = 0;
    for (int i = open; i < text.length(); i++) {
      char c = text.charAt(i);
      if (quote != 0) {
        if (c == quote) {
          quote = 0;
        }
        continue;
      }
      if (c == '\'' || c == '"' || c == '`' || c == '[') {
        quote = c == '[' ? ']' : c;
      } else if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
        if (depth == 0) {
          return i;
        }
      }
    }
    return -1;
  }

  /** Positions of a keyword at the top level (outside parentheses, quotes and comments). */
  private static List<Span> topLevelWords(String text, String word) {
    List<Span> spans = new ArrayList<>();
    int depth = 0;
    char quote = 0;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (quote != 0) {
        if (c == quote) {
          quote = 0;
        }
        continue;
      }
      if (c == '\'' || c == '"' || c == '`' || c == '[') {
        quote = c == '[' ? ']' : c;
        continue;
      }
      if (c == '(') {
        depth++;
        continue;
      }
      if (c == ')') {
        depth--;
        continue;
      }
      if (depth == 0 && wordAt(text, i, word)) {
        spans.add(new Span(i, i + word.length()));
        i += word.length() - 1;
      }
    }
    return spans;
  }

  private static String stripComments(String sql) {
    StringBuilder out = new StringBuilder(sql.length());
    char quote = 0;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (quote != 0) {
        out.append(c);
        if (c == quote) {
          quote = 0;
        }
        continue;
      }
      if (c == '\'' || c == '"' || c == '`' || c == '[') {
        quote = c == '[' ? ']' : c;
        out.append(c);
        continue;
      }
      if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
        while (i < sql.length() && sql.charAt(i) != '\n') {
          i++;
        }
        out.append('\n');
        continue;
      }
      if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
        i += 2;
        while (i + 1 < sql.length() && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) {
          i++;
        }
        i++;
        out.append(' ');
        continue;
      }
      out.append(c);
    }
    return out.toString();
  }

  private record Span(int start, int end) {}
}
