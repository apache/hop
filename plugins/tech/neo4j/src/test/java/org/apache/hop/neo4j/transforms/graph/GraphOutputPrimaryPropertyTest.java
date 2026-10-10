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

package org.apache.hop.neo4j.transforms.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.neo4j.model.GraphModel;
import org.apache.hop.neo4j.model.GraphNode;
import org.apache.hop.neo4j.model.GraphProperty;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.junit.jupiter.api.Test;

/**
 * A node is merged on its primary properties: one which is written to without a primary property
 * mapped would match every node with its label.
 */
class GraphOutputPrimaryPropertyTest {

  private static GraphModel model() {
    List<GraphNode> nodes = new ArrayList<>();
    nodes.add(
        new GraphNode(
            "Person",
            null,
            new ArrayList<>(List.of("Person")),
            new ArrayList<>(
                List.of(
                    new GraphProperty("id", null, GraphPropertyType.String, true, true, true, true),
                    new GraphProperty(
                        "name", null, GraphPropertyType.String, false, false, false, false)))));
    nodes.add(
        new GraphNode(
            "Company",
            null,
            new ArrayList<>(List.of("Company")),
            new ArrayList<>(
                List.of(
                    new GraphProperty(
                        "code", null, GraphPropertyType.String, true, true, true, true),
                    new GraphProperty(
                        "city", null, GraphPropertyType.String, false, false, false, false)))));
    return new GraphModel("model", null, nodes, new ArrayList<>());
  }

  private static FieldModelMapping node(String field, String node, String property) {
    return new FieldModelMapping(field, ModelTargetType.Node, node, property, ModelTargetHint.None);
  }

  @Test
  void everyNodeWithAPrimaryPropertyIsAccepted() {
    List<FieldModelMapping> mappings =
        List.of(
            node("id", "Person", "id"),
            node("name", "Person", "name"),
            node("code", "Company", "code"));
    assertEquals(List.of(), GraphOutput.findNodesWithoutMappedPrimaryProperty(model(), mappings));
  }

  @Test
  void nodeWithoutPrimaryPropertyIsReported() {
    List<FieldModelMapping> mappings =
        List.of(
            node("id", "Person", "id"),
            node("city", "Company", "city"),
            node("name", "Person", "name"));
    assertEquals(
        List.of("Company"), GraphOutput.findNodesWithoutMappedPrimaryProperty(model(), mappings));
  }

  @Test
  void primaryPropertyWithoutFieldDoesNotCount() {
    List<FieldModelMapping> mappings =
        List.of(node(null, "Person", "id"), node("name", "Person", "name"));
    assertEquals(
        List.of("Person"), GraphOutput.findNodesWithoutMappedPrimaryProperty(model(), mappings));
  }

  @Test
  void relationshipMappingsAreIgnored() {
    List<FieldModelMapping> mappings =
        List.of(
            node("id", "Person", "id"),
            new FieldModelMapping(
                "since", ModelTargetType.Relationship, "WORKS_AT", "since", ModelTargetHint.None));
    assertEquals(List.of(), GraphOutput.findNodesWithoutMappedPrimaryProperty(model(), mappings));
  }
}
