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

package org.apache.hop.neo4j.actions.propertygraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.neo4j.model.GraphModel;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Checks the property graph sample in src/main/samples: the workflow refers to the Create property
 * graph action, the graph model and the pipelines it ships with, and the load pipeline writes the
 * tables and columns which the action creates for that graph model.
 */
class PropertyGraphSampleTest {
  private static final String SAMPLES = "src/main/samples/";
  private static final String FOLDER = SAMPLES + "property-graph/";
  private static final String CONNECTION = "oracle-23ai";

  private static Node root(String filename, String tag) throws Exception {
    Document document = XmlHandler.loadXmlFile(filename);
    Node root = XmlHandler.getSubNode(document, tag);
    assertNotNull(root, filename + " has no <" + tag + "> element");
    return root;
  }

  private static GraphModel loadGraphModel(String name) throws Exception {
    String filename = SAMPLES + "metadata/neo4j-graph-model/" + name + ".json";
    assertTrue(HopVfs.getFileObject(filename).exists(), "Missing graph model " + filename);
    try (InputStream inputStream = HopVfs.getInputStream(filename)) {
      JsonParser parser = new JsonFactory().createParser(inputStream);
      parser.nextToken();
      return new JsonMetadataParser<>(GraphModel.class, new MemoryMetadataProvider())
          .loadJsonObject(GraphModel.class, parser);
    }
  }

  private static Node action(Node workflow, String type) {
    Node actions = XmlHandler.getSubNode(workflow, "actions");
    for (Node action : XmlHandler.getNodes(actions, "action")) {
      if (type.equals(XmlHandler.getTagValue(action, "type"))) {
        return action;
      }
    }
    return null;
  }

  @Test
  void testWorkflow() throws Exception {
    Node workflow = root(FOLDER + "social-network-property-graph.hwf", "workflow");

    String actionId = ActionCreatePropertyGraph.class.getAnnotation(Action.class).id();
    Node action = action(workflow, actionId);
    assertNotNull(action, "The workflow has no " + actionId + " action");
    assertEquals(CONNECTION, XmlHandler.getTagValue(action, "connection"));
    assertEquals("SOCIAL_NETWORK", XmlHandler.getTagValue(action, "graph_name"));
    assertEquals("social-network", XmlHandler.getTagValue(action, "graph_model"));
    assertNotNull(loadGraphModel(XmlHandler.getTagValue(action, "graph_model")));

    Node actions = XmlHandler.getSubNode(workflow, "actions");
    List<String> pipelines = new ArrayList<>();
    for (Node pipeline : XmlHandler.getNodes(actions, "action")) {
      if ("PIPELINE".equals(XmlHandler.getTagValue(pipeline, "type"))) {
        String filename = XmlHandler.getTagValue(pipeline, "filename");
        assertTrue(filename.startsWith("${PROJECT_HOME}/"), filename);
        String local = SAMPLES + filename.substring("${PROJECT_HOME}/".length());
        assertTrue(HopVfs.getFileObject(local).exists(), "Missing pipeline " + local);
        pipelines.add(local);
      }
    }
    assertEquals(
        List.of(FOLDER + "social-network-load-tables.hpl", FOLDER + "social-network-query.hpl"),
        pipelines);
  }

  /** The Table outputs write the default tables and columns of the generator, no others. */
  @Test
  void testLoadPipelineMatchesGeneratedTables() throws Exception {
    PropertyGraphGenerator generator =
        new PropertyGraphGenerator(loadGraphModel("social-network"), null, null);
    Map<String, List<String>> expected = new LinkedHashMap<>();
    for (PropertyGraphGenerator.NodeTable table : generator.getNodeTables()) {
      expected.put(table.table(), table.columns().stream().map(c -> c.name()).toList());
    }
    for (PropertyGraphGenerator.EdgeTable table : generator.getEdgeTables()) {
      expected.put(table.table(), table.columns().stream().map(c -> c.name()).toList());
    }

    Node pipeline = root(FOLDER + "social-network-load-tables.hpl", "pipeline");
    Map<String, Node> transforms = new HashMap<>();
    for (Node transform : XmlHandler.getNodes(pipeline, "transform")) {
      transforms.put(XmlHandler.getTagValue(transform, "name"), transform);
    }
    Map<String, List<String>> written = new LinkedHashMap<>();
    Node order = XmlHandler.getSubNode(pipeline, "order");
    for (Node hop : XmlHandler.getNodes(order, "hop")) {
      Node from = transforms.get(XmlHandler.getTagValue(hop, "from"));
      Node to = transforms.get(XmlHandler.getTagValue(hop, "to"));
      assertEquals("DataGrid", XmlHandler.getTagValue(from, "type"));
      assertEquals("TableOutput", XmlHandler.getTagValue(to, "type"));
      assertEquals(CONNECTION, XmlHandler.getTagValue(to, "connection"));
      List<String> fields = new ArrayList<>();
      for (Node field : XmlHandler.getNodes(XmlHandler.getSubNode(from, "fields"), "field")) {
        fields.add(XmlHandler.getTagValue(field, "name"));
      }
      for (Node line : XmlHandler.getNodes(XmlHandler.getSubNode(from, "data"), "line")) {
        assertEquals(fields.size(), XmlHandler.getNodes(line, "item").size());
      }
      written.put(XmlHandler.getTagValue(to, "table"), fields);
    }
    assertEquals(expected, written);
  }

  @Test
  void testQueryPipeline() throws Exception {
    Node pipeline = root(FOLDER + "social-network-query.hpl", "pipeline");
    Node tableInput = null;
    for (Node transform : XmlHandler.getNodes(pipeline, "transform")) {
      if ("TableInput".equals(XmlHandler.getTagValue(transform, "type"))) {
        tableInput = transform;
      }
    }
    assertNotNull(tableInput);
    assertEquals(CONNECTION, XmlHandler.getTagValue(tableInput, "connection"));
    String sql = XmlHandler.getTagValue(tableInput, "sql");
    assertTrue(sql.contains("GRAPH_TABLE ( SOCIAL_NETWORK"), sql);
    assertTrue(sql.contains("-[k IS KNOWS]->"), sql);
    assertTrue(sql.contains("-[w IS WORKS_AT]->"), sql);
  }
}
