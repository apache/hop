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

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.hop.core.util.Utils;

/**
 * Decide whether a SQL statement returns a result set (run with {@code executeQuery}) or is DDL/DML
 * ({@code execute}).
 *
 * <p>{@code WITH} common table expressions are queries when the statement after the CTE list is
 * {@code SELECT} (or {@code VALUES} / {@code TABLE} / {@code SHOW} / {@code EXPLAIN}), and DML when
 * it is {@code INSERT}, {@code UPDATE}, {@code DELETE} or {@code MERGE}.
 */
public final class SqlQueryClassifier {

  private static final Set<String> QUERY_STARTERS =
      Set.of("SELECT", "SHOW", "EXPLAIN", "DESCRIBE", "DESC", "VALUES", "TABLE");

  /**
   * First keywords of a complete statement. Leftover clauses after a semicolon ({@code WHERE},
   * {@code AND}, {@code ORDER}, …) are not in this set.
   */
  private static final Set<String> STATEMENT_STARTERS = statementStarters();

  private SqlQueryClassifier() {}

  private static Set<String> statementStarters() {
    Set<String> starters = new HashSet<>(QUERY_STARTERS);
    starters.addAll(
        Set.of(
            "WITH",
            "INSERT",
            "UPDATE",
            "DELETE",
            "MERGE",
            "CREATE",
            "DROP",
            "ALTER",
            "TRUNCATE",
            "GRANT",
            "REVOKE",
            "COMMENT",
            "CALL",
            "EXEC",
            "EXECUTE",
            "SET",
            "USE",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "START",
            "SAVEPOINT",
            "RELEASE",
            "DECLARE",
            "PREPARE",
            "DEALLOCATE",
            "ANALYZE",
            "VACUUM",
            "COPY",
            "LOAD",
            "UNLOAD",
            "REFRESH",
            "OPTIMIZE",
            "REPLACE",
            "UPSERT",
            "LOCK",
            "UNLOCK",
            "RENAME",
            "ATTACH",
            "DETACH",
            "PRAGMA",
            "REINDEX",
            "KILL",
            "DO",
            "IF",
            "WHILE",
            "FOR",
            "RETURN",
            "OPEN",
            "CLOSE",
            "FETCH",
            "MOVE"));
    return Set.copyOf(starters);
  }

  /**
   * @param sql one statement, comments allowed
   * @return {@code true} when the statement should be executed as a query
   */
  public static boolean isQuery(String sql) {
    if (Utils.isEmpty(sql)) {
      return false;
    }
    int i = skipTrivia(sql, 0);
    if (i < sql.length() && sql.charAt(i) == '(') {
      return true;
    }
    String first = keywordAt(sql, i);
    if (first == null) {
      return false;
    }
    int selectAt = i;
    if ("WITH".equals(first)) {
      selectAt = indexAfterCteList(sql, i);
      first = keywordAt(sql, selectAt);
      if (first == null) {
        return false;
      }
    }
    if (!QUERY_STARTERS.contains(first)) {
      return false;
    }
    if ("SELECT".equals(first)) {
      return !hasIntoAtDepthZero(sql, skipKeyword(sql, selectAt));
    }
    return true;
  }

  /**
   * @param sql one statement, comments allowed
   * @return {@code true} when {@code sql} starts with a SQL verb (query, DML or DDL), {@code false}
   *     for leftover clauses such as {@code WHERE} after a semicolon
   */
  public static boolean isExecutableStatement(String sql) {
    if (Utils.isEmpty(sql)) {
      return false;
    }
    int i = skipTrivia(sql, 0);
    if (i < sql.length() && sql.charAt(i) == '(') {
      return true;
    }
    String first = keywordAt(sql, i);
    return first != null && STATEMENT_STARTERS.contains(first);
  }

  /**
   * Skip {@code WITH [RECURSIVE] name AS (...), ...} and return the index of the main statement.
   */
  static int indexAfterCteList(String sql, int withPos) {
    int i = skipKeyword(sql, withPos);
    i = skipTrivia(sql, i);
    if ("RECURSIVE".equals(keywordAt(sql, i))) {
      i = skipKeyword(sql, i);
    }
    while (i < sql.length()) {
      i = skipTrivia(sql, i);
      i = skipIdentifier(sql, i);
      i = skipTrivia(sql, i);
      if (i < sql.length() && sql.charAt(i) == '(') {
        i = skipBalancedParens(sql, i);
        i = skipTrivia(sql, i);
      }
      if ("AS".equals(keywordAt(sql, i))) {
        i = skipKeyword(sql, i);
        i = skipTrivia(sql, i);
      }
      while ("NOT".equals(keywordAt(sql, i)) || "MATERIALIZED".equals(keywordAt(sql, i))) {
        i = skipKeyword(sql, i);
        i = skipTrivia(sql, i);
      }
      if (i < sql.length() && sql.charAt(i) == '(') {
        i = skipBalancedParens(sql, i);
        i = skipTrivia(sql, i);
      }
      if (i < sql.length() && sql.charAt(i) == ',') {
        i++;
        continue;
      }
      break;
    }
    return skipTrivia(sql, i);
  }

  private static boolean hasIntoAtDepthZero(String sql, int from) {
    int i = from;
    int depth = 0;
    while (i < sql.length()) {
      i = skipTrivia(sql, i);
      if (i >= sql.length()) {
        return false;
      }
      char c = sql.charAt(i);
      if (c == '(') {
        depth++;
        i++;
        continue;
      }
      if (c == ')') {
        if (depth > 0) {
          depth--;
        }
        i++;
        continue;
      }
      if (isQuote(c)) {
        i = skipQuoted(sql, i);
        continue;
      }
      if (depth == 0) {
        String keyword = keywordAt(sql, i);
        if ("INTO".equals(keyword)) {
          return true;
        }
        if ("FROM".equals(keyword) || "WHERE".equals(keyword)) {
          return false;
        }
        if (keyword != null) {
          i = skipKeyword(sql, i);
          continue;
        }
      }
      i++;
    }
    return false;
  }

  static int skipTrivia(String sql, int i) {
    int n = sql.length();
    while (i < n) {
      char c = sql.charAt(i);
      if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f') {
        i++;
        continue;
      }
      if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
        i += 2;
        while (i < n && sql.charAt(i) != '\n' && sql.charAt(i) != '\r') {
          i++;
        }
        continue;
      }
      if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
        i += 2;
        while (i + 1 < n && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) {
          i++;
        }
        i = Math.min(n, i + 2);
        continue;
      }
      break;
    }
    return i;
  }

  static String keywordAt(String sql, int i) {
    i = skipTrivia(sql, i);
    if (i >= sql.length() || !isIdentStart(sql.charAt(i))) {
      return null;
    }
    int start = i;
    i++;
    while (i < sql.length() && isIdentPart(sql.charAt(i))) {
      i++;
    }
    return sql.substring(start, i).toUpperCase(Locale.ROOT);
  }

  private static int skipKeyword(String sql, int i) {
    i = skipTrivia(sql, i);
    if (i >= sql.length() || !isIdentStart(sql.charAt(i))) {
      return i;
    }
    i++;
    while (i < sql.length() && isIdentPart(sql.charAt(i))) {
      i++;
    }
    return i;
  }

  private static int skipIdentifier(String sql, int i) {
    i = skipTrivia(sql, i);
    if (i >= sql.length()) {
      return i;
    }
    if (isQuote(sql.charAt(i))) {
      return skipQuoted(sql, i);
    }
    return skipKeyword(sql, i);
  }

  private static int skipBalancedParens(String sql, int i) {
    if (i >= sql.length() || sql.charAt(i) != '(') {
      return i;
    }
    int depth = 0;
    while (i < sql.length()) {
      char c = sql.charAt(i);
      if (isQuote(c)) {
        i = skipQuoted(sql, i);
        continue;
      }
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
        i++;
        if (depth == 0) {
          return i;
        }
        continue;
      } else if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
        i = skipTrivia(sql, i);
        continue;
      } else if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
        i = skipTrivia(sql, i);
        continue;
      }
      i++;
    }
    return i;
  }

  private static int skipQuoted(String sql, int i) {
    char q = sql.charAt(i);
    char close = q == '[' ? ']' : q;
    i++;
    while (i < sql.length()) {
      char c = sql.charAt(i);
      if (c == close) {
        if (close != ']' && i + 1 < sql.length() && sql.charAt(i + 1) == close) {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }
    return i;
  }

  private static boolean isQuote(char c) {
    return c == '\'' || c == '"' || c == '`' || c == '[';
  }

  private static boolean isIdentStart(char c) {
    return Character.isLetter(c) || c == '_';
  }

  private static boolean isIdentPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$';
  }
}
