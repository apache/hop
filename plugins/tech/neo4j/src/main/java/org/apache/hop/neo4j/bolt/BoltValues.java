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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.GraphPathValue;
import org.apache.hop.core.graph.GraphRelationshipValue;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

/**
 * Converts the values of the Neo4j driver to the plain values of graph connections: nodes,
 * relationships and paths become graph values, also inside lists and maps.
 */
public final class BoltValues {

  private BoltValues() {}

  public static Object toValue(Object value) {
    if (value instanceof Node node) {
      return toNode(node);
    }
    if (value instanceof Relationship relationship) {
      return toRelationship(relationship);
    }
    if (value instanceof Path path) {
      List<GraphNodeValue> nodes = new ArrayList<>();
      path.nodes().forEach(node -> nodes.add(toNode(node)));
      List<GraphRelationshipValue> relationships = new ArrayList<>();
      path.relationships().forEach(relationship -> relationships.add(toRelationship(relationship)));
      return new GraphPathValue(nodes, relationships);
    }
    if (value instanceof Map<?, ?> map) {
      Map<String, Object> converted = new LinkedHashMap<>();
      map.forEach((key, element) -> converted.put(String.valueOf(key), toValue(element)));
      return converted;
    }
    if (value instanceof List<?> list) {
      List<Object> converted = new ArrayList<>();
      list.forEach(element -> converted.add(toValue(element)));
      return converted;
    }
    return value;
  }

  /** A result row with its values converted. */
  public static Map<String, Object> toRow(Map<String, Object> row) {
    Map<String, Object> converted = new LinkedHashMap<>();
    row.forEach((key, value) -> converted.put(key, toValue(value)));
    return converted;
  }

  private static GraphNodeValue toNode(Node node) {
    List<String> labels = new ArrayList<>();
    node.labels().forEach(labels::add);
    return new GraphNodeValue(node.elementId(), labels, toProperties(node.asMap()));
  }

  private static GraphRelationshipValue toRelationship(Relationship relationship) {
    return new GraphRelationshipValue(
        relationship.elementId(),
        relationship.type(),
        relationship.startNodeElementId(),
        relationship.endNodeElementId(),
        toProperties(relationship.asMap()));
  }

  private static Map<String, Object> toProperties(Map<String, Object> properties) {
    return new LinkedHashMap<>(properties);
  }
}
