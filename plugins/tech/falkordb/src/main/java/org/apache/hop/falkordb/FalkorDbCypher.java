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

package org.apache.hop.falkordb;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.GraphPathValue;
import org.apache.hop.core.graph.GraphRelationshipValue;

/**
 * Parameters and results of FalkorDB queries. FalkorDB takes query parameters as Cypher literals in
 * a {@code CYPHER name=value ...} prefix, and returns results in its compact format: every value
 * with its type, labels, relationship types and property keys as ids.
 */
public final class FalkorDbCypher {

  private FalkorDbCypher() {}

  /** The statement with its parameters in a CYPHER prefix. */
  public static String withParameters(String statement, Map<String, Object> parameters) {
    if (parameters == null || parameters.isEmpty()) {
      return statement;
    }
    StringBuilder query = new StringBuilder("CYPHER");
    for (Map.Entry<String, Object> parameter : parameters.entrySet()) {
      query
          .append(' ')
          .append(parameter.getKey())
          .append('=')
          .append(toLiteral(parameter.getValue()));
    }
    return query.append(' ').append(statement).toString();
  }

  /**
   * A value as a Cypher literal. Dates and times become ISO strings and binary values Base64
   * strings: FalkorDB has no parameter literals for them.
   */
  public static String toLiteral(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof String string) {
      return quote(string);
    }
    if (value instanceof Boolean
        || value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte
        || value instanceof BigInteger) {
      return value.toString();
    }
    if (value instanceof Double || value instanceof Float) {
      double d = ((Number) value).doubleValue();
      return Double.isFinite(d) ? Double.toString(d) : "null";
    }
    if (value instanceof BigDecimal bigDecimal) {
      return bigDecimal.toPlainString();
    }
    if (value instanceof byte[] bytes) {
      return quote(Base64.getEncoder().encodeToString(bytes));
    }
    if (value instanceof Date date) {
      return quote(date.toInstant().toString());
    }
    if (value instanceof Map<?, ?> map) {
      StringJoiner joiner = new StringJoiner(", ", "{", "}");
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        joiner.add(quoteName(String.valueOf(entry.getKey())) + ": " + toLiteral(entry.getValue()));
      }
      return joiner.toString();
    }
    if (value instanceof Collection<?> collection) {
      StringJoiner joiner = new StringJoiner(", ", "[", "]");
      for (Object element : collection) {
        joiner.add(toLiteral(element));
      }
      return joiner.toString();
    }
    if (value.getClass().isArray()) {
      StringJoiner joiner = new StringJoiner(", ", "[", "]");
      for (int i = 0; i < Array.getLength(value); i++) {
        joiner.add(toLiteral(Array.get(value, i)));
      }
      return joiner.toString();
    }
    // java.time values and anything else: the string form
    return quote(value.toString());
  }

  private static String quote(String string) {
    return "'" + string.replace("\\", "\\\\").replace("'", "\\'") + "'";
  }

  private static String quoteName(String name) {
    return "`" + name.replace("`", "``") + "`";
  }

  /** Resolves the ids of labels, relationship types and property keys in compact replies. */
  public interface NameResolver {
    String label(int id);

    String relationshipType(int id);

    String propertyKey(int id);
  }

  /**
   * The rows of a compact GRAPH.QUERY reply: [header, rows, statistics], or only [statistics] for a
   * statement without results. Every value comes with its type.
   */
  public static List<Map<String, Object>> toRows(List<Object> reply, NameResolver names) {
    List<Map<String, Object>> rows = new ArrayList<>();
    if (reply == null || reply.size() < 3) {
      return rows;
    }
    List<String> header = new ArrayList<>();
    for (Object column : (List<?>) reply.get(0)) {
      // [column type, name]
      header.add(String.valueOf(((List<?>) column).get(1)));
    }
    for (Object rowObject : (List<?>) reply.get(1)) {
      List<?> values = (List<?>) rowObject;
      Map<String, Object> row = new LinkedHashMap<>();
      for (int i = 0; i < header.size(); i++) {
        row.put(header.get(i), toValue((List<?>) values.get(i), names));
      }
      rows.add(row);
    }
    return rows;
  }

  /** A typed value of a compact reply: [type, value]. */
  static Object toValue(List<?> typedValue, NameResolver names) {
    return toValue(toInt(typedValue.get(0)), typedValue.get(1), names);
  }

  private static Object toValue(int type, Object value, NameResolver names) {
    switch (type) {
      case 1: // null
        return null;
      case 2: // string
        return value == null ? null : value.toString();
      case 3: // integer
        return value instanceof Number number ? number.longValue() : Long.valueOf(value.toString());
      case 4: // boolean
        return value instanceof Boolean bool ? bool : Boolean.valueOf(value.toString());
      case 5: // double
        return value instanceof Number number
            ? number.doubleValue()
            : Double.valueOf(value.toString());
      case 6: // array
        {
          List<Object> list = new ArrayList<>();
          for (Object element : (List<?>) value) {
            list.add(toValue((List<?>) element, names));
          }
          return list;
        }
      case 7: // edge: [id, type id, source id, destination id, properties]
        {
          List<?> edge = (List<?>) value;
          return new GraphRelationshipValue(
              String.valueOf(toLong(edge.get(0))),
              names.relationshipType(toInt(edge.get(1))),
              String.valueOf(toLong(edge.get(2))),
              String.valueOf(toLong(edge.get(3))),
              toProperties((List<?>) edge.get(4), names));
        }
      case 8: // node: [id, [label ids], properties]
        {
          List<?> node = (List<?>) value;
          List<String> labels = new ArrayList<>();
          for (Object labelId : (List<?>) node.get(1)) {
            labels.add(names.label(toInt(labelId)));
          }
          return new GraphNodeValue(
              String.valueOf(toLong(node.get(0))),
              labels,
              toProperties((List<?>) node.get(2), names));
        }
      case 9: // path: [typed array of nodes, typed array of edges]
        {
          List<?> path = (List<?>) value;
          List<GraphNodeValue> nodes = new ArrayList<>();
          for (Object node : (List<?>) toValue((List<?>) path.get(0), names)) {
            nodes.add((GraphNodeValue) node);
          }
          List<GraphRelationshipValue> relationships = new ArrayList<>();
          for (Object edge : (List<?>) toValue((List<?>) path.get(1), names)) {
            relationships.add((GraphRelationshipValue) edge);
          }
          return new GraphPathValue(nodes, relationships);
        }
      case 10: // map: [key, typed value, key, typed value, ...]
        {
          List<?> entries = (List<?>) value;
          Map<String, Object> map = new LinkedHashMap<>();
          for (int i = 0; i + 1 < entries.size(); i += 2) {
            map.put(String.valueOf(entries.get(i)), toValue((List<?>) entries.get(i + 1), names));
          }
          return map;
        }
      case 11: // point: [latitude, longitude]
        {
          List<?> point = (List<?>) value;
          Map<String, Object> map = new LinkedHashMap<>();
          map.put("latitude", Double.valueOf(point.get(0).toString()));
          map.put("longitude", Double.valueOf(point.get(1).toString()));
          return map;
        }
      case 12: // vector of floats
        {
          List<Object> vector = new ArrayList<>();
          for (Object element : (List<?>) value) {
            vector.add(Double.valueOf(element.toString()));
          }
          return vector;
        }
      case 13: // local date time, seconds since the epoch
        return LocalDateTime.ofEpochSecond(toLong(value), 0, ZoneOffset.UTC);
      case 14: // date, seconds since the epoch
        return LocalDateTime.ofEpochSecond(toLong(value), 0, ZoneOffset.UTC).toLocalDate();
      case 15: // local time, seconds since midnight
        return LocalTime.ofSecondOfDay(toLong(value) % 86400);
      case 16: // duration in seconds
        return Duration.ofSeconds(toLong(value));
      default:
        return value;
    }
  }

  /** Properties: a list of [key id, type, value]. */
  private static Map<String, Object> toProperties(List<?> properties, NameResolver names) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (Object propertyObject : properties) {
      List<?> property = (List<?>) propertyObject;
      map.put(
          names.propertyKey(toInt(property.get(0))),
          toValue(toInt(property.get(1)), property.get(2), names));
    }
    return map;
  }

  private static int toInt(Object value) {
    return (int) toLong(value);
  }

  private static long toLong(Object value) {
    return value instanceof Number number ? number.longValue() : Long.parseLong(value.toString());
  }
}
