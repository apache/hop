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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hop.core.graph.GraphIndex;

/** Reads the indexes and unique constraints which Neo4j and Memgraph list. */
public final class BoltIndexes {

  private BoltIndexes() {}

  /**
   * The indexes from the rows of SHOW INDEXES (Neo4j 4.2 and later) or CALL db.indexes() (Neo4j 3.5
   * to 4.x). Unique indexes are those backing a uniqueness or key constraint.
   */
  public static List<GraphIndex> fromNeo4j(List<Map<String, Object>> rows) {
    List<GraphIndex> indexes = new ArrayList<>();
    for (Map<String, Object> row : rows) {
      List<String> labels = toStrings(row.getOrDefault("labelsOrTypes", row.get("tokenNames")));
      List<String> properties = toStrings(row.get("properties"));
      if (labels.isEmpty() || properties.isEmpty()) {
        // A lookup index on all labels or types: no use for properties
        continue;
      }
      String type = String.valueOf(row.get("type")).toUpperCase(Locale.ROOT);
      boolean unique =
          "UNIQUE".equalsIgnoreCase(String.valueOf(row.get("uniqueness")))
              || row.get("owningConstraint") != null
              || type.contains("UNIQUE");
      boolean relationship =
          "RELATIONSHIP".equalsIgnoreCase(String.valueOf(row.get("entityType")))
              || type.contains("RELATIONSHIP");
      indexes.add(
          new GraphIndex(
              toName(row.getOrDefault("name", row.get("indexName"))),
              relationship,
              labels,
              properties,
              unique));
    }
    return indexes;
  }

  /**
   * The indexes from the rows of SHOW INDEX INFO and the unique constraints from SHOW CONSTRAINT
   * INFO on Memgraph. Memgraph doesn't back unique constraints with an index.
   */
  public static List<GraphIndex> fromMemgraph(
      List<Map<String, Object>> indexRows, List<Map<String, Object>> constraintRows) {
    List<GraphIndex> indexes = new ArrayList<>();
    for (Map<String, Object> row : indexRows) {
      String type = String.valueOf(row.get("index type"));
      List<String> properties = toStrings(row.get("property"));
      if (properties.isEmpty() || !type.endsWith("+property")) {
        continue;
      }
      indexes.add(
          new GraphIndex(
              "", type.startsWith("edge-type"), toStrings(row.get("label")), properties, false));
    }
    for (Map<String, Object> row : constraintRows) {
      if ("unique".equalsIgnoreCase(String.valueOf(row.get("constraint type")))) {
        indexes.add(
            new GraphIndex(
                "", false, toStrings(row.get("label")), toStrings(row.get("properties")), true));
      }
    }
    return indexes;
  }

  private static String toName(Object value) {
    return value == null ? "" : value.toString();
  }

  /** A string or a list of strings as a list, empty for null. */
  static List<String> toStrings(Object value) {
    List<String> strings = new ArrayList<>();
    if (value instanceof Iterable<?> iterable) {
      for (Object element : iterable) {
        if (element != null) {
          strings.add(element.toString());
        }
      }
    } else if (value != null) {
      strings.add(value.toString());
    }
    return strings;
  }
}
