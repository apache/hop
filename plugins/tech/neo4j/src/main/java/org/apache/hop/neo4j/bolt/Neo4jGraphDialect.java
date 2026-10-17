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

package org.apache.hop.neo4j.bolt;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphIndex;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.IGraphConnection;

/** Neo4j 5: named indexes and constraints, IF [NOT] EXISTS. */
public class Neo4jGraphDialect extends BoltGraphDialect {

  public static final Neo4jGraphDialect INSTANCE = new Neo4jGraphDialect();

  /** A CALL keyword, not a property or a part of a longer name. */
  private static final Pattern CALL =
      Pattern.compile("(?<![\\w.$])CALL\\b", Pattern.CASE_INSENSITIVE);

  /** What follows the subquery of CALL { ... } IN [n] [CONCURRENT] TRANSACTIONS. */
  private static final Pattern IN_TRANSACTIONS =
      Pattern.compile(
          "\\s*IN\\s+(?:[^\\s{}()]+\\s+)?(?:CONCURRENT\\s+)?TRANSACTIONS\\b",
          Pattern.CASE_INSENSITIVE);

  /** USING PERIODIC COMMIT, before LOAD CSV in Neo4j 4. */
  private static final Pattern PERIODIC_COMMIT =
      Pattern.compile("(?<![\\w.$])USING\\s+PERIODIC\\s+COMMIT\\b", Pattern.CASE_INSENSITIVE);

  public Neo4jGraphDialect() {
    super("NEO4J");
  }

  /**
   * Neo4j only runs CALL { ... } IN TRANSACTIONS and USING PERIODIC COMMIT in an implicit
   * transaction: they commit batches of rows themselves.
   */
  @Override
  public boolean isRequiringAutoCommit(String statement) {
    if (statement == null) {
      return false;
    }
    String code = blankLiteralsAndComments(statement);
    return PERIODIC_COMMIT.matcher(code).find() || isCallInTransactions(code);
  }

  /** True if the code has a CALL subquery followed by IN TRANSACTIONS. */
  private static boolean isCallInTransactions(String code) {
    Matcher call = CALL.matcher(code);
    while (call.find()) {
      int index = skipWhitespace(code, call.end());
      // The variables of the subquery: CALL (n, m) { ... } or CALL (*) { ... }
      if (index < code.length() && code.charAt(index) == '(') {
        index = skipWhitespace(code, closingIndex(code, index, '(', ')') + 1);
      }
      if (index >= code.length() || code.charAt(index) != '{') {
        // A procedure call
        continue;
      }
      int end = closingIndex(code, index, '{', '}');
      if (end < code.length()
          && IN_TRANSACTIONS.matcher(code).region(end + 1, code.length()).lookingAt()) {
        return true;
      }
    }
    return false;
  }

  private static int skipWhitespace(String code, int index) {
    while (index < code.length() && Character.isWhitespace(code.charAt(index))) {
      index++;
    }
    return index;
  }

  /** The index of the bracket which closes the one at the index, the length if there is none. */
  private static int closingIndex(String code, int index, char open, char close) {
    int depth = 0;
    for (int i = index; i < code.length(); i++) {
      char c = code.charAt(i);
      if (c == open) {
        depth++;
      } else if (c == close && --depth == 0) {
        return i;
      }
    }
    return code.length();
  }

  /**
   * The statement with the string literals, quoted names and comments replaced by spaces, so that
   * the words and brackets in them don't count.
   */
  static String blankLiteralsAndComments(String statement) {
    StringBuilder code = new StringBuilder(statement.length());
    int i = 0;
    while (i < statement.length()) {
      char c = statement.charAt(i);
      int end;
      if (c == '\'' || c == '"' || c == '`') {
        end = i + 1;
        while (end < statement.length() && statement.charAt(end) != c) {
          // A backslash escapes the next character in a string, not in a quoted name
          end += c != '`' && statement.charAt(end) == '\\' ? 2 : 1;
        }
        end = Math.min(end + 1, statement.length());
      } else if (statement.startsWith("//", i)) {
        int newline = statement.indexOf('\n', i);
        end = newline < 0 ? statement.length() : newline;
      } else if (statement.startsWith("/*", i)) {
        int close = statement.indexOf("*/", i + 2);
        end = close < 0 ? statement.length() : close + 2;
      } else {
        code.append(c);
        i++;
        continue;
      }
      code.append(" ".repeat(end - i));
      i = end;
    }
    return code.toString();
  }

  @Override
  public List<GraphIndex> getIndexes(IGraphConnection connection) throws HopException {
    try {
      return BoltIndexes.fromNeo4j(connection.execute("SHOW INDEXES", Map.of()));
    } catch (HopException e) {
      // Neo4j before 4.2 doesn't know SHOW INDEXES. If the old procedure fails as well, the
      // error of SHOW INDEXES is the one which tells what went wrong.
      try {
        return BoltIndexes.fromNeo4j(connection.execute("CALL db.indexes()", Map.of()));
      } catch (HopException fallbackException) {
        e.addSuppressed(fallbackException);
        throw new HopException("Unable to list the indexes of the database", e);
      }
    }
  }

  /**
   * The schema from the schema procedures, which read all nodes and relationships: the sample size
   * doesn't apply. The labels at the ends of the relationships come from db.schema.visualization(),
   * left out if it fails. On versions or with privileges without the procedures, a sample.
   */
  @Override
  public GraphSchema getSchema(IGraphConnection connection, int sampleSize) throws HopException {
    List<Map<String, Object>> nodes;
    List<Map<String, Object>> relationships;
    try {
      nodes = connection.execute("CALL db.schema.nodeTypeProperties()", Map.of());
      relationships = connection.execute("CALL db.schema.relTypeProperties()", Map.of());
    } catch (HopException e) {
      return super.getSchema(connection, sampleSize);
    }
    List<Map<String, Object>> visualization;
    try {
      visualization = connection.execute("CALL db.schema.visualization()", Map.of());
    } catch (HopException e) {
      visualization = null;
    }
    return BoltSchemas.fromNeo4j(nodes, relationships, visualization);
  }
}
