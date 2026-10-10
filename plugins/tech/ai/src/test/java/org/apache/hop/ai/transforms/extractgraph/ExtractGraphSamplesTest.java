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

package org.apache.hop.ai.transforms.extractgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * The Extract graph samples are files only, so nothing else checks that they still match the
 * transforms, the actions and the graph model they use.
 */
class ExtractGraphSamplesTest {

  private static final String SAMPLES = "src/main/samples/";
  private static final String PIPELINE = SAMPLES + "transforms/extract-graph-to-database.hpl";
  private static final String WORKFLOW = SAMPLES + "transforms/extract-graph-to-database.hwf";
  private static final String MODEL = SAMPLES + "metadata/neo4j-graph-model/knowledge-graph.json";

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void thePipelineIsWiredAsDocumented() throws Exception {
    Node pipeline = XmlHandler.getSubNode(read(PIPELINE), "pipeline");

    Map<String, Node> transforms = new HashMap<>();
    for (Node transform : XmlHandler.getNodes(pipeline, "transform")) {
      transforms.put(XmlHandler.getTagValue(transform, "name"), transform);
    }
    assertEquals(
        Set.of(
            "DataGrid",
            "ExtractGraph",
            "FilterRows",
            "Neo4jGraphOutput",
            "BlockingTransform",
            "BlockUntilTransformsFinish"),
        new HashSet<>(transforms.values().stream().map(t -> type(t)).toList()));

    for (Node hop : XmlHandler.getNodes(XmlHandler.getSubNode(pipeline, "order"), "hop")) {
      assertTrue(transforms.containsKey(XmlHandler.getTagValue(hop, "from")));
      assertTrue(transforms.containsKey(XmlHandler.getTagValue(hop, "to")));
    }

    // The transform reads its own sample back
    ExtractGraphMeta meta = new ExtractGraphMeta();
    meta.loadXml(transforms.get("Extract graph"), null);
    assertEquals("text", meta.getInputField());
    assertEquals("graph_element", meta.getKindField());
    assertFalse(meta.isPassRowsWithoutResults());

    Node filter = transforms.get("Entity?");
    Node condition = XmlHandler.getSubNode(XmlHandler.getSubNode(filter, "compare"), "condition");
    assertEquals("graph_element", XmlHandler.getTagValue(condition, "leftvalue"));
    assertEquals("ENTITY", XmlHandler.getTagValue(condition, "value", "text"));
    assertTrue(transforms.containsKey(XmlHandler.getTagValue(filter, "send_true_to")));
    assertTrue(transforms.containsKey(XmlHandler.getTagValue(filter, "send_false_to")));

    // Relationships wait for the entities
    Node wait = transforms.get("Wait for the entities");
    Node waitFor = XmlHandler.getSubNode(XmlHandler.getSubNode(wait, "transforms"), "transform");
    assertEquals("Write entities", XmlHandler.getTagValue(waitFor, "name"));
  }

  @Test
  void theGraphOutputsOnlyUseTheModel() throws Exception {
    JsonNode model;
    try (InputStream in = HopVfs.getInputStream(MODEL)) {
      model = HopJson.newMapper().readTree(in);
    }
    assertEquals("knowledge-graph", model.get("name").asText());
    Set<String> nodeProperties = new HashSet<>();
    for (JsonNode node : model.get("nodes")) {
      for (JsonNode property : node.get("properties")) {
        nodeProperties.add(node.get("name").asText() + "." + property.get("name").asText());
      }
    }
    Set<String> relationshipProperties = new HashSet<>();
    for (JsonNode relationship : model.get("relationships")) {
      for (JsonNode property : relationship.get("properties")) {
        relationshipProperties.add(
            relationship.get("name").asText() + "." + property.get("name").asText());
      }
    }

    Node pipeline = XmlHandler.getSubNode(read(PIPELINE), "pipeline");
    List<String> hints = new ArrayList<>();
    for (Node transform : XmlHandler.getNodes(pipeline, "transform")) {
      if (!"Neo4jGraphOutput".equals(type(transform))) {
        continue;
      }
      assertEquals("knowledge-graph", XmlHandler.getTagValue(transform, "model"));
      for (Node mapping :
          XmlHandler.getNodes(XmlHandler.getSubNode(transform, "mappings"), "mapping")) {
        String target =
            XmlHandler.getTagValue(mapping, "target_name")
                + "."
                + XmlHandler.getTagValue(mapping, "target_property");
        if ("Node".equals(XmlHandler.getTagValue(mapping, "target_type"))) {
          assertTrue(nodeProperties.contains(target), target);
        } else {
          assertTrue(relationshipProperties.contains(target), target);
        }
        hints.add(XmlHandler.getTagValue(mapping, "target_hint"));
      }
    }
    assertTrue(hints.contains("SelfRelationshipSource"));
    assertTrue(hints.contains("SelfRelationshipTarget"));
  }

  @Test
  void theWorkflowCreatesTheConstraintAndRunsThePipeline() throws Exception {
    Node workflow = XmlHandler.getSubNode(read(WORKFLOW), "workflow");
    List<String> types = new ArrayList<>();
    for (Node action : XmlHandler.getNodes(XmlHandler.getSubNode(workflow, "actions"), "action")) {
      types.add(type(action));
      if ("PIPELINE".equals(type(action))) {
        assertEquals(
            "${PROJECT_HOME}/transforms/extract-graph-to-database.hpl",
            XmlHandler.getTagValue(action, "filename"));
      }
      if ("NEO4J_CONSTRAINT".equals(type(action))) {
        Node update = XmlHandler.getSubNode(XmlHandler.getSubNode(action, "updates"), "update");
        assertEquals("Entity", XmlHandler.getTagValue(update, "object_name"));
        assertEquals("name", XmlHandler.getTagValue(update, "object_properties"));
        assertEquals("UNIQUE", XmlHandler.getTagValue(update, "constraint_type"));
      }
    }
    assertEquals(List.of("SPECIAL", "NEO4J_CONSTRAINT", "PIPELINE"), types);
  }

  private static String type(Node node) {
    return XmlHandler.getTagValue(node, "type");
  }

  private static Document read(String path) throws Exception {
    try (InputStream in = HopVfs.getInputStream(path)) {
      return XmlHandler.loadXmlFile(in);
    }
  }
}
