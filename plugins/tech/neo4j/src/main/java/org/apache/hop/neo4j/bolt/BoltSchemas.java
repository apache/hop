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

import static org.apache.hop.core.graph.GraphSchemaSampler.toStrings;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.GraphRelationshipValue;
import org.apache.hop.core.graph.GraphSchema;
import org.apache.hop.core.graph.GraphSchemaBuilder;

/** Reads the schema which Neo4j and Memgraph describe. */
public final class BoltSchemas {

  private BoltSchemas() {}

  /**
   * The schema from Neo4j's schema procedures.
   *
   * @param nodeRows The rows of CALL db.schema.nodeTypeProperties(): one per property of a label
   *     combination
   * @param relationshipRows The rows of CALL db.schema.relTypeProperties()
   * @param visualizationRows The rows of CALL db.schema.visualization(), for the labels at the ends
   *     of relationships. Null if unknown.
   */
  public static GraphSchema fromNeo4j(
      List<Map<String, Object>> nodeRows,
      List<Map<String, Object>> relationshipRows,
      List<Map<String, Object>> visualizationRows) {
    GraphSchemaBuilder builder = new GraphSchemaBuilder();
    for (Map<String, Object> row : nodeRows) {
      String group = String.valueOf(row.get("nodeType"));
      Object propertyName = row.get("propertyName");
      for (String label : toStrings(row.get("nodeLabels"))) {
        if (propertyName == null) {
          builder.addGroup(GraphObjectType.NODE, label, group, null, null);
        } else {
          builder.addGroupProperty(
              GraphObjectType.NODE,
              label,
              group,
              propertyName.toString(),
              toStrings(row.get("propertyTypes")),
              toBoolean(row.get("mandatory")));
        }
      }
    }
    for (Map<String, Object> row : relationshipRows) {
      String group = String.valueOf(row.get("relType"));
      String type = unquoteType(group);
      Object propertyName = row.get("propertyName");
      if (propertyName == null) {
        builder.addGroup(GraphObjectType.RELATIONSHIP, type, group, null, null);
      } else {
        builder.addGroupProperty(
            GraphObjectType.RELATIONSHIP,
            type,
            group,
            propertyName.toString(),
            toStrings(row.get("propertyTypes")),
            toBoolean(row.get("mandatory")));
      }
    }
    if (visualizationRows != null) {
      for (Map<String, Object> row : visualizationRows) {
        addVisualization(builder, row);
      }
    }
    return builder.build(null, false);
  }

  /** The labels at the ends of the relationships of db.schema.visualization(). */
  private static void addVisualization(GraphSchemaBuilder builder, Map<String, Object> row) {
    Map<String, List<String>> labelsById = new HashMap<>();
    if (row.get("nodes") instanceof Iterable<?> nodes) {
      for (Object node : nodes) {
        if (node instanceof GraphNodeValue value) {
          labelsById.put(value.id(), value.labels());
        }
      }
    }
    if (row.get("relationships") instanceof Iterable<?> relationships) {
      for (Object relationship : relationships) {
        if (relationship instanceof GraphRelationshipValue value) {
          builder.addRelationshipEnds(
              value.type(),
              labelsById.getOrDefault(value.startNodeId(), List.of()),
              labelsById.getOrDefault(value.endNodeId(), List.of()));
        }
      }
    }
  }

  /** :`KNOWS` to KNOWS */
  static String unquoteType(String relType) {
    String type = relType.startsWith(":") ? relType.substring(1) : relType;
    if (type.length() >= 2 && type.startsWith("`") && type.endsWith("`")) {
      type = type.substring(1, type.length() - 1).replace("``", "`");
    }
    return type;
  }

  private static Boolean toBoolean(Object value) {
    if (value instanceof Boolean b) {
      return b;
    }
    return value == null ? null : Boolean.valueOf(value.toString());
  }

  /**
   * The schema from Memgraph's SHOW SCHEMA INFO: a JSON document with the label combinations of the
   * nodes and the relationship types with their properties and how many have them.
   *
   * @param json The schema column of SHOW SCHEMA INFO
   */
  public static GraphSchema fromMemgraph(String json) throws HopException {
    JsonNode schema;
    try {
      schema = new ObjectMapper().readTree(json);
    } catch (Exception e) {
      throw new HopException("Unable to read the schema information of Memgraph", e);
    }
    GraphSchemaBuilder builder = new GraphSchemaBuilder();
    for (JsonNode node : schema.path("nodes")) {
      List<String> labels = strings(node.path("labels"));
      String group = String.join(":", labels);
      for (String label : labels) {
        builder.addGroup(GraphObjectType.NODE, label, group, null, null);
        addMemgraphProperties(builder, GraphObjectType.NODE, label, group, node);
      }
    }
    for (JsonNode edge : schema.path("edges")) {
      String type = edge.path("type").asText();
      List<String> start = strings(edge.path("start_node_labels"));
      List<String> end = strings(edge.path("end_node_labels"));
      String group = String.join(":", start) + "-" + type + "-" + String.join(":", end);
      builder.addGroup(GraphObjectType.RELATIONSHIP, type, group, start, end);
      addMemgraphProperties(builder, GraphObjectType.RELATIONSHIP, type, group, edge);
    }
    return builder.build(null, false);
  }

  private static void addMemgraphProperties(
      GraphSchemaBuilder builder,
      GraphObjectType elementType,
      String name,
      String group,
      JsonNode element) {
    for (JsonNode property : element.path("properties")) {
      List<String> types = new ArrayList<>();
      for (JsonNode type : property.path("types")) {
        types.add(type.path("type").asText());
      }
      JsonNode fillingFactor = property.path("filling_factor");
      Boolean mandatory = fillingFactor.isNumber() ? fillingFactor.asDouble() >= 100.0 : null;
      builder.addGroupProperty(
          elementType, name, group, property.path("key").asText(), types, mandatory);
    }
  }

  private static List<String> strings(JsonNode array) {
    List<String> strings = new ArrayList<>();
    for (JsonNode element : array) {
      strings.add(element.asText());
    }
    return strings;
  }
}
