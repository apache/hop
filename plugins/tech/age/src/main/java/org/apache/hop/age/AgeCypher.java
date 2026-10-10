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

package org.apache.hop.age;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.math.BigInteger;
import java.time.temporal.Temporal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphIndex;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.GraphPathValue;
import org.apache.hop.core.graph.GraphRelationshipValue;

/**
 * Runs Cypher on Apache AGE: the statement goes into the {@code cypher()} function of a SQL query,
 * its parameters into one agtype map, and the results come back as agtype text: JSON with {@code
 * ::vertex}, {@code ::edge} and {@code ::path} suffixes.
 */
public final class AgeCypher {
  /** AGE prints the float values NaN, Infinity and -Infinity as bare tokens, not JSON numbers. */
  private static final ObjectMapper MAPPER =
      JsonMapper.builder().enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS).build();

  private AgeCypher() {}

  /**
   * The SQL query running a Cypher statement.
   *
   * @param graphName The AGE graph
   * @param cypher The Cypher statement
   * @param columns The names of the returned values, empty when nothing is returned
   * @param withParameters True to pass the parameters as the one JDBC parameter of the query
   */
  public static String toSql(
      String graphName, String cypher, List<String> columns, boolean withParameters) {
    // A statement can't end with a semicolon inside cypher()
    String statement = cypher.strip();
    while (statement.endsWith(";")) {
      statement = statement.substring(0, statement.length() - 1).strip();
    }
    cypher = statement;
    StringBuilder sql = new StringBuilder("SELECT * FROM ag_catalog.cypher(");
    sql.append('\'').append(graphName.replace("'", "''")).append("', ");
    String tag = "$hop$";
    for (int i = 1; cypher.contains(tag); i++) {
      tag = "$hop" + i + "$";
    }
    sql.append(tag).append(' ').append(cypher).append(' ').append(tag);
    if (withParameters) {
      sql.append(", ?");
    }
    sql.append(") AS (");
    if (columns.isEmpty()) {
      sql.append("v agtype");
    } else {
      for (int i = 0; i < columns.size(); i++) {
        sql.append(i > 0 ? ", " : "").append('c').append(i + 1).append(" agtype");
      }
    }
    return sql.append(')').toString();
  }

  /** The parameters as one agtype map: JSON. Dates and times become ISO strings. */
  public static String toParameterJson(Map<String, Object> parameters) throws HopException {
    try {
      return MAPPER.writeValueAsString(toJsonValue(parameters));
    } catch (JsonProcessingException e) {
      throw new HopException("Unable to convert the statement parameters to JSON", e);
    }
  }

  private static Object toJsonValue(Object value) {
    if (value == null
        || value instanceof String
        || value instanceof Boolean
        || value instanceof Number) {
      return value;
    }
    if (value instanceof Map<?, ?> map) {
      Map<String, Object> json = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        json.put(String.valueOf(entry.getKey()), toJsonValue(entry.getValue()));
      }
      return json;
    }
    if (value instanceof Iterable<?> iterable) {
      List<Object> json = new ArrayList<>();
      for (Object element : iterable) {
        json.add(toJsonValue(element));
      }
      return json;
    }
    if (value instanceof byte[] bytes) {
      return Base64.getEncoder().encodeToString(bytes);
    }
    if (value instanceof Object[] array) {
      return toJsonValue(List.of(array));
    }
    if (value instanceof Date date) {
      return date.toInstant().toString();
    }
    if (value instanceof Temporal) {
      return value.toString();
    }
    return value.toString();
  }

  /** The keywords which start a clause, to find the last clause of a statement. */
  private static final List<String> CLAUSE_KEYWORDS =
      List.of(
          "CALL",
          "YIELD",
          "MATCH",
          "OPTIONAL",
          "CREATE",
          "MERGE",
          "SET",
          "DELETE",
          "DETACH",
          "REMOVE",
          "WITH",
          "UNWIND",
          "FOREACH",
          "RETURN");

  /**
   * The names of the values the statement returns: the items of its last RETURN clause outside of
   * subqueries. Empty if it doesn't return anything.
   *
   * @throws HopException for RETURN *, or for a statement ending with CALL ... YIELD: AGE needs to
   *     know how many values come back
   */
  public static List<String> getReturnColumns(String cypher) throws HopException {
    List<int[]> tokens = topLevelWords(cypher);
    int returnEnd = -1;
    for (int i = 0; i < tokens.size(); i++) {
      if (word(cypher, tokens.get(i)).equals("RETURN") && !isAlias(cypher, tokens, i)) {
        returnEnd = tokens.get(i)[1];
      }
    }
    List<String> columns = new ArrayList<>();
    if (returnEnd < 0) {
      checkNoProcedureResults(cypher, tokens);
      return columns;
    }
    // The clause ends at ORDER BY, SKIP, LIMIT, UNION or the end of the statement. A keyword
    // directly after AS is an alias.
    int clauseEnd = cypher.length();
    int clauseStart = returnEnd;
    for (int i = 0; i < tokens.size(); i++) {
      int[] token = tokens.get(i);
      if (token[0] < returnEnd) {
        continue;
      }
      String word = word(cypher, token);
      if (token[0] == firstWordAfter(tokens, returnEnd) && word.equals("DISTINCT")) {
        clauseStart = token[1];
      }
      if ((word.equals("ORDER")
              || word.equals("SKIP")
              || word.equals("LIMIT")
              || word.equals("UNION"))
          && !isAlias(cypher, tokens, i)) {
        clauseEnd = token[0];
        break;
      }
    }
    String clause = withoutComments(cypher.substring(clauseStart, clauseEnd)).trim();
    if (clause.endsWith(";")) {
      clause = clause.substring(0, clause.length() - 1).trim();
    }
    for (String item : splitTopLevel(clause)) {
      String expression = item.trim();
      if (expression.equals("*")) {
        throw new HopException(
            "RETURN * isn't supported on Apache AGE: it needs to know which values come back. "
                + "Name the values in the RETURN clause, for example RETURN n, m, and list them "
                + "in the Returns grid if the transform has one.");
      }
      columns.add(columnName(expression));
    }
    return columns;
  }

  /**
   * A statement without RETURN which ends with a procedure call (CALL ... YIELD, or a CALL without
   * YIELD) returns the values of the procedure. AGE needs to know these values up front, so ask for
   * a RETURN clause instead of silently dropping the rows. CALL { ... } subqueries return nothing.
   */
  private static void checkNoProcedureResults(String cypher, List<int[]> tokens)
      throws HopException {
    String lastClause = null;
    for (int i = 0; i < tokens.size(); i++) {
      String word = word(cypher, tokens.get(i));
      if (CLAUSE_KEYWORDS.contains(word) && !isAlias(cypher, tokens, i)) {
        lastClause =
            word.equals("CALL") && nextCharacter(cypher, tokens.get(i)[1]) == '{' ? "CALL {" : word;
      }
    }
    if ("CALL".equals(lastClause) || "YIELD".equals(lastClause)) {
      throw new HopException(
          "A statement ending with CALL ... YIELD isn't supported on Apache AGE: it needs to know "
              + "which values come back. Add a RETURN clause naming them, for example "
              + "CALL ... YIELD x RETURN x.");
    }
  }

  /** True if the word is directly preceded by AS: then it's an alias, not a keyword. */
  private static boolean isAlias(String cypher, List<int[]> tokens, int index) {
    if (index == 0) {
      return false;
    }
    int[] previous = tokens.get(index - 1);
    return word(cypher, previous).equals("AS")
        && withoutComments(cypher.substring(previous[1], tokens.get(index)[0])).isBlank();
  }

  /** The first character from the position on which isn't whitespace or a comment, or 0. */
  private static char nextCharacter(String cypher, int position) {
    String rest = withoutComments(cypher.substring(position)).stripLeading();
    return rest.isEmpty() ? 0 : rest.charAt(0);
  }

  private static int firstWordAfter(List<int[]> tokens, int position) {
    for (int[] token : tokens) {
      if (token[0] >= position) {
        return token[0];
      }
    }
    return -1;
  }

  private static String word(String cypher, int[] token) {
    return cypher.substring(token[0], token[1]).toUpperCase(Locale.ROOT);
  }

  /** The alias of a returned value, or the expression itself. */
  private static String columnName(String expression) {
    List<int[]> tokens = topLevelWords(expression);
    for (int i = tokens.size() - 1; i >= 0; i--) {
      int[] token = tokens.get(i);
      if (word(expression, token).equals("AS")) {
        String alias = expression.substring(token[1]).trim();
        if (alias.startsWith("`") && alias.endsWith("`") && alias.length() > 1) {
          alias = alias.substring(1, alias.length() - 1).replace("``", "`");
        }
        return alias;
      }
    }
    return expression;
  }

  /**
   * The words outside of strings, comments, brackets and braces, as [start, end] positions. These
   * are the keywords of the statement itself, not of subqueries or literals.
   */
  private static List<int[]> topLevelWords(String cypher) {
    List<int[]> words = new ArrayList<>();
    int depth = 0;
    int i = 0;
    int n = cypher.length();
    while (i < n) {
      char c = cypher.charAt(i);
      if (c == '\'' || c == '"' || c == '`') {
        i = skipQuoted(cypher, i);
        continue;
      }
      int commentEnd = commentEnd(cypher, i);
      if (commentEnd > i) {
        i = commentEnd;
        continue;
      }
      if (c == '(' || c == '[' || c == '{') {
        depth++;
      } else if (c == ')' || c == ']' || c == '}') {
        depth--;
      } else if (depth == 0 && Character.isLetter(c)) {
        int start = i;
        while (i < n && (Character.isLetterOrDigit(cypher.charAt(i)) || cypher.charAt(i) == '_')) {
          i++;
        }
        boolean wordStart =
            start == 0
                || !(Character.isLetterOrDigit(cypher.charAt(start - 1))
                    || cypher.charAt(start - 1) == '_'
                    || cypher.charAt(start - 1) == '.'
                    || cypher.charAt(start - 1) == '$');
        if (wordStart) {
          words.add(new int[] {start, i});
        }
        continue;
      }
      i++;
    }
    return words;
  }

  private static int skipQuoted(String cypher, int start) {
    char quote = cypher.charAt(start);
    int i = start + 1;
    while (i < cypher.length()) {
      char c = cypher.charAt(i);
      if (c == '\\' && quote != '`') {
        i += 2;
        continue;
      }
      if (c == quote) {
        if (i + 1 < cypher.length() && cypher.charAt(i + 1) == quote) {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }
    return cypher.length();
  }

  /**
   * The end of the line or block comment starting at the position, or the position itself if no
   * comment starts there. A line comment ends before its line break.
   */
  private static int commentEnd(String text, int i) {
    if (text.charAt(i) == '/' && i + 1 < text.length()) {
      if (text.charAt(i + 1) == '/') {
        int newline = text.indexOf('\n', i);
        return newline < 0 ? text.length() : newline;
      }
      if (text.charAt(i + 1) == '*') {
        int close = text.indexOf("*/", i + 2);
        return close < 0 ? text.length() : close + 2;
      }
    }
    return i;
  }

  /** The text with every comment outside of strings replaced by a space. */
  static String withoutComments(String text) {
    StringBuilder result = new StringBuilder();
    int i = 0;
    while (i < text.length()) {
      char c = text.charAt(i);
      if (c == '\'' || c == '"' || c == '`') {
        int end = skipQuoted(text, i);
        result.append(text, i, end);
        i = end;
        continue;
      }
      int commentEnd = commentEnd(text, i);
      if (commentEnd > i) {
        result.append(' ');
        i = commentEnd;
        continue;
      }
      result.append(c);
      i++;
    }
    return result.toString();
  }

  /** Split on the commas outside of strings, comments, brackets and braces. */
  private static List<String> splitTopLevel(String text) {
    List<String> parts = new ArrayList<>();
    int depth = 0;
    int start = 0;
    int i = 0;
    while (i < text.length()) {
      char c = text.charAt(i);
      if (c == '\'' || c == '"' || c == '`') {
        i = skipQuoted(text, i);
        continue;
      }
      int commentEnd = commentEnd(text, i);
      if (commentEnd > i) {
        i = commentEnd;
        continue;
      }
      if (c == '(' || c == '[' || c == '{') {
        depth++;
      } else if (c == ')' || c == ']' || c == '}') {
        depth--;
      } else if (c == ',' && depth == 0) {
        parts.add(text.substring(start, i));
        start = i + 1;
      }
      i++;
    }
    parts.add(text.substring(start));
    return parts;
  }

  private static final Pattern INDEXED_PROPERTY =
      Pattern.compile("properties, '\"((?:[^\"\\\\]|\\\\.)*)\"'::agtype");

  /**
   * The properties a PostgreSQL index on a label table covers, from its definition: the properties
   * accessed in its expressions, or all properties ({@link GraphIndex#ALL_PROPERTIES}) for an index
   * on the properties column itself, like a GIN index. Empty if it isn't on properties, like the
   * index on the id.
   */
  public static List<String> getIndexedProperties(String indexDefinition) {
    List<String> properties = new ArrayList<>();
    Matcher matcher = INDEXED_PROPERTY.matcher(indexDefinition);
    while (matcher.find()) {
      properties.add(matcher.group(1).replace("\\\"", "\""));
    }
    if (properties.isEmpty() && indexDefinition.matches("(?s).*\\(properties\\)\\s*$")) {
      return GraphIndex.ALL_PROPERTIES;
    }
    return properties;
  }

  /** The key marking a vertex or edge in the JSON of an agtype value, and the path marker. */
  private static final String TYPE_KEY = "\u0000agtype";

  private static final String PATH_MARKER = "\u0000agtype:path";

  /**
   * An agtype value as a Java value: maps, lists, strings, longs, doubles and booleans. Vertices,
   * edges and paths become {@link GraphNodeValue}, {@link GraphRelationshipValue} and {@link
   * GraphPathValue}.
   */
  public static Object toValue(String agtype) throws HopException {
    if (agtype == null) {
      return null;
    }
    try {
      return normalize(MAPPER.readValue(stripTypeSuffixes(agtype), Object.class));
    } catch (JsonProcessingException | RuntimeException e) {
      throw new HopException("Unable to read Apache AGE value " + agtype, e);
    }
  }

  private static Object normalize(Object value) {
    if (value instanceof Integer integer) {
      return integer.longValue();
    }
    if (value instanceof BigInteger bigInteger) {
      return bigInteger.longValue();
    }
    if (value instanceof Map<?, ?> map) {
      Map<String, Object> normalized = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        normalized.put(String.valueOf(entry.getKey()), normalize(entry.getValue()));
      }
      Object type = normalized.remove(TYPE_KEY);
      if ("vertex".equals(type)) {
        return new GraphNodeValue(
            String.valueOf(normalized.get("id")),
            List.of(String.valueOf(normalized.get("label"))),
            toProperties(normalized.get("properties")));
      }
      if ("edge".equals(type)) {
        return new GraphRelationshipValue(
            String.valueOf(normalized.get("id")),
            String.valueOf(normalized.get("label")),
            String.valueOf(normalized.get("start_id")),
            String.valueOf(normalized.get("end_id")),
            toProperties(normalized.get("properties")));
      }
      return normalized;
    }
    if (value instanceof List<?> list) {
      if (!list.isEmpty() && PATH_MARKER.equals(list.get(0))) {
        // A path: vertices and edges alternate
        List<GraphNodeValue> nodes = new ArrayList<>();
        List<GraphRelationshipValue> relationships = new ArrayList<>();
        for (Object element : list.subList(1, list.size())) {
          Object normalizedElement = normalize(element);
          if (normalizedElement instanceof GraphNodeValue node) {
            nodes.add(node);
          } else if (normalizedElement instanceof GraphRelationshipValue relationship) {
            relationships.add(relationship);
          }
        }
        return new GraphPathValue(nodes, relationships);
      }
      List<Object> normalized = new ArrayList<>();
      for (Object element : list) {
        normalized.add(normalize(element));
      }
      return normalized;
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> toProperties(Object properties) {
    return properties instanceof Map<?, ?> map ? (Map<String, Object>) map : new LinkedHashMap<>();
  }

  /**
   * Turn agtype text into JSON: remove the ::numeric suffixes outside of strings and replace the
   * ::vertex, ::edge and ::path suffixes by markers inside the object or array they follow.
   */
  static String stripTypeSuffixes(String agtype) {
    StringBuilder json = new StringBuilder();
    // The positions in the JSON of the objects and arrays which are open
    Deque<Integer> open = new ArrayDeque<>();
    int lastClosed = -1;
    int i = 0;
    while (i < agtype.length()) {
      char c = agtype.charAt(i);
      if (c == '"') {
        int end = i + 1;
        while (end < agtype.length() && agtype.charAt(end) != '"') {
          end += agtype.charAt(end) == '\\' ? 2 : 1;
        }
        end = Math.min(end + 1, agtype.length());
        json.append(agtype, i, end);
        i = end;
        continue;
      }
      if (c == ':' && i + 1 < agtype.length() && agtype.charAt(i + 1) == ':') {
        int start = i + 2;
        i = start;
        while (i < agtype.length() && Character.isLetter(agtype.charAt(i))) {
          i++;
        }
        String type = agtype.substring(start, i);
        if (lastClosed >= 0) {
          if (("vertex".equals(type) || "edge".equals(type)) && json.charAt(lastClosed) == '{') {
            json.insert(lastClosed + 1, "\"\\u0000agtype\":\"" + type + "\",");
          } else if ("path".equals(type) && json.charAt(lastClosed) == '[') {
            json.insert(lastClosed + 1, "\"\\u0000agtype:path\",");
          }
        }
        continue;
      }
      if (c == '{' || c == '[') {
        open.push(json.length());
      } else if ((c == '}' || c == ']') && !open.isEmpty()) {
        lastClosed = open.pop();
      }
      json.append(c);
      i++;
    }
    return json.toString();
  }
}
