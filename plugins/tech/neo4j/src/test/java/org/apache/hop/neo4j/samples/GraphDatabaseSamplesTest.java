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

package org.apache.hop.neo4j.samples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.graph.GraphDatabasePlugin;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.neo4j.actions.check.CheckConnections;
import org.apache.hop.neo4j.actions.index.Neo4jIndex;
import org.apache.hop.neo4j.bolt.BoltGraphDatabase;
import org.apache.hop.neo4j.bolt.MemgraphGraphDatabase;
import org.apache.hop.neo4j.bolt.Neo4jGraphDatabase;
import org.apache.hop.neo4j.bolt.NeptuneGraphDatabase;
import org.apache.hop.neo4j.model.GraphModel;
import org.apache.hop.neo4j.model.GraphNode;
import org.apache.hop.neo4j.model.GraphProperty;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.model.GraphRelationship;
import org.apache.hop.neo4j.transforms.cypher.CypherMeta;
import org.apache.hop.neo4j.transforms.cypherbuilder.CypherBuilderMeta;
import org.apache.hop.neo4j.transforms.gencsv.GenerateCsvMeta;
import org.apache.hop.neo4j.transforms.graph.GraphOutputMeta;
import org.apache.hop.neo4j.transforms.loginfo.GetLoggingInfoMeta;
import org.apache.hop.neo4j.transforms.schema.GetGraphSchemaMeta;
import org.apache.hop.neo4j.transforms.split.SplitGraphMeta;
import org.apache.hop.neo4j.transforms.vectorsearch.GraphVectorSearchMeta;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * The graph database samples are files only: this checks that they still match the transforms,
 * actions, graph models and graph database connections they use. It covers the graph-databases
 * folder of the samples of this plugin and of the FalkorDB, Apache AGE and Gremlin plugins, which
 * all end up in the same samples project.
 */
class GraphDatabaseSamplesTest {

  private static final String NEO4J_SAMPLES = "src/main/samples/";
  private static final List<String> SAMPLE_ROOTS =
      List.of(
          NEO4J_SAMPLES,
          "../falkordb/src/main/samples/",
          "../age/src/main/samples/",
          "../gremlin/src/main/samples/");
  private static final String FOLDER = "graph-databases/";
  private static final String CONNECTIONS = "metadata/graph-database-connection/";
  private static final String MODELS = "metadata/neo4j-graph-model/";

  /** The transforms and actions of other plugins which the samples use. */
  private static final Set<String> OTHER_PLUGIN_IDS =
      Set.of("DataGrid", "WriteToLog", "BlockingTransform", "SPECIAL", "PIPELINE", "SET_VARIABLES");

  /** The graph database types of the FalkorDB, Apache AGE and Gremlin plugins. */
  private static final Set<String> OTHER_GRAPH_DATABASE_IDS = Set.of("FALKORDB", "AGE", "GREMLIN");

  private static Set<String> pluginIds() {
    Set<String> ids = new HashSet<>(OTHER_PLUGIN_IDS);
    for (Class<?> c :
        List.of(
            GraphOutputMeta.class,
            CypherMeta.class,
            CypherBuilderMeta.class,
            GenerateCsvMeta.class,
            SplitGraphMeta.class,
            GetLoggingInfoMeta.class,
            GetGraphSchemaMeta.class,
            GraphVectorSearchMeta.class)) {
      ids.add(c.getAnnotation(Transform.class).id());
    }
    for (Class<?> c : List.of(CheckConnections.class, Neo4jIndex.class)) {
      ids.add(c.getAnnotation(Action.class).id());
    }
    return ids;
  }

  private static String boltPluginId(Class<?> c) {
    return c.getAnnotation(GraphDatabasePlugin.class).id();
  }

  /** Every pipeline and workflow in the graph-databases folders, with the samples root of each. */
  private static Map<String, String> sampleFiles() throws Exception {
    Map<String, String> files = new HashMap<>();
    for (String root : SAMPLE_ROOTS) {
      FileObject folder = HopVfs.getFileObject(root + FOLDER);
      if (!folder.exists()) {
        continue;
      }
      for (FileObject child : folder.getChildren()) {
        String name = child.getName().getBaseName();
        if (name.endsWith(".hpl") || name.endsWith(".hwf")) {
          files.put(root + FOLDER + name, root);
        }
      }
    }
    return files;
  }

  private static Document read(String path) throws Exception {
    try (InputStream in = HopVfs.getInputStream(path)) {
      return XmlHandler.loadXmlFile(in);
    }
  }

  private static JsonNode readJson(String path) throws Exception {
    try (InputStream in = HopVfs.getInputStream(path)) {
      return HopJson.newMapper().readTree(in);
    }
  }

  private static boolean exists(String path) throws Exception {
    return HopVfs.getFileObject(path).exists();
  }

  private static String findConnection(String name) throws Exception {
    for (String root : SAMPLE_ROOTS) {
      String path = root + CONNECTIONS + name + ".json";
      if (exists(path)) {
        return path;
      }
    }
    return null;
  }

  private static GraphModel loadGraphModel(String name) throws Exception {
    String filename = NEO4J_SAMPLES + MODELS + name + ".json";
    assertTrue(exists(filename), "Missing graph model " + filename);
    try (InputStream inputStream = HopVfs.getInputStream(filename)) {
      JsonParser parser = new JsonFactory().createParser(inputStream);
      parser.nextToken();
      return new JsonMetadataParser<>(GraphModel.class, new MemoryMetadataProvider())
          .loadJsonObject(GraphModel.class, parser);
    }
  }

  private static String type(Node node) {
    return XmlHandler.getTagValue(node, "type");
  }

  private static String name(Node node) {
    return XmlHandler.getTagValue(node, "name");
  }

  private static List<Node> elements(Node root, boolean pipeline) {
    if (pipeline) {
      return XmlHandler.getNodes(root, "transform");
    }
    return XmlHandler.getNodes(XmlHandler.getSubNode(root, "actions"), "action");
  }

  /** The graph database connection names a transform or action refers to. */
  private static List<String> connectionNames(Node element) {
    List<String> names = new ArrayList<>();
    String connection = XmlHandler.getTagValue(element, "connection");
    if (connection != null) {
      names.add(connection);
    }
    String connectionName = XmlHandler.getTagValue(element, "connectionName");
    if (connectionName != null) {
      names.add(connectionName);
    }
    Node connections = XmlHandler.getSubNode(element, "connections");
    if (connections != null) {
      for (Node c : XmlHandler.getNodes(connections, "connection")) {
        names.add(XmlHandler.getNodeValue(c));
      }
    }
    if ("SET_VARIABLES".equals(type(element))) {
      for (Node field : XmlHandler.getNodes(XmlHandler.getSubNode(element, "fields"), "field")) {
        if ("HOP_GRAPH_LOGGING_CONNECTION".equals(XmlHandler.getTagValue(field, "variable_name"))) {
          names.add(XmlHandler.getTagValue(field, "variable_value"));
        }
      }
    }
    return names;
  }

  @Test
  void theSamplesAreThere() throws Exception {
    Set<String> names = new HashSet<>();
    for (String file : sampleFiles().keySet()) {
      names.add(file.substring(file.lastIndexOf('/') + 1));
    }
    assertTrue(
        names.containsAll(
            Set.of(
                "vector-search.hwf",
                "vector-search-load.hpl",
                "vector-search-query.hpl",
                "social-network-memgraph.hpl",
                "get-graph-schema.hpl",
                "graph-csv-export.hpl",
                "graph-logging.hwf",
                "graph-logging-info.hpl",
                "social-network-falkordb.hpl",
                "social-network-age.hpl",
                "social-network-age.hwf",
                "social-network-gremlin.hpl")),
        names.toString());
  }

  @Test
  void everySampleIsWiredToWhatExists() throws Exception {
    Set<String> pluginIds = pluginIds();
    for (Map.Entry<String, String> entry : sampleFiles().entrySet()) {
      String file = entry.getKey();
      boolean isPipeline = file.endsWith(".hpl");
      Node root = XmlHandler.getSubNode(read(file), isPipeline ? "pipeline" : "workflow");
      assertNotNull(root, file);

      String baseName = file.substring(file.lastIndexOf('/') + 1, file.length() - 4);
      String fileName =
          isPipeline
              ? XmlHandler.getTagValue(XmlHandler.getSubNode(root, "info"), "name")
              : XmlHandler.getTagValue(root, "name");
      assertEquals(baseName, fileName, file);
      String description =
          isPipeline
              ? XmlHandler.getTagValue(XmlHandler.getSubNode(root, "info"), "description")
              : XmlHandler.getTagValue(root, "description");
      assertFalse(description == null || description.isBlank(), file + " has no description");
      assertFalse(
          XmlHandler.getNodes(XmlHandler.getSubNode(root, "notepads"), "notepad").isEmpty(),
          file + " has no notepad");

      Set<String> elementNames = new HashSet<>();
      for (Node element : elements(root, isPipeline)) {
        assertTrue(pluginIds.contains(type(element)), file + ": unknown type " + type(element));
        assertTrue(elementNames.add(name(element)), file + ": duplicate " + name(element));

        for (String connection : connectionNames(element)) {
          if (connection.isEmpty()) {
            continue;
          }
          assertNotNull(findConnection(connection), file + ": missing connection " + connection);
        }

        String filename = XmlHandler.getTagValue(element, "filename");
        if (filename != null) {
          assertTrue(filename.startsWith("${PROJECT_HOME}/"), filename);
          String local = filename.substring("${PROJECT_HOME}/".length());
          assertTrue(
              exists(entry.getValue() + local) || exists(NEO4J_SAMPLES + local),
              file + ": missing " + filename);
        }

        if (GraphOutputMeta.class.getAnnotation(Transform.class).id().equals(type(element))) {
          checkGraphOutput(file, element);
        }
      }

      Node hops = XmlHandler.getSubNode(root, isPipeline ? "order" : "hops");
      for (Node hop : XmlHandler.getNodes(hops, "hop")) {
        assertTrue(elementNames.contains(XmlHandler.getTagValue(hop, "from")), file);
        assertTrue(elementNames.contains(XmlHandler.getTagValue(hop, "to")), file);
      }
    }
  }

  /** The mappings of a Graph output only use properties of its graph model. */
  private void checkGraphOutput(String file, Node transform) throws Exception {
    GraphModel model = loadGraphModel(XmlHandler.getTagValue(transform, "model"));
    for (Node mapping :
        XmlHandler.getNodes(XmlHandler.getSubNode(transform, "mappings"), "mapping")) {
      String targetName = XmlHandler.getTagValue(mapping, "target_name");
      String targetProperty = XmlHandler.getTagValue(mapping, "target_property");
      GraphProperty property;
      if ("Node".equals(XmlHandler.getTagValue(mapping, "target_type"))) {
        GraphNode node = model.findNode(targetName);
        assertNotNull(node, file + ": no node " + targetName);
        property = node.findProperty(targetProperty);
      } else {
        GraphRelationship relationship = model.findRelationship(targetName);
        assertNotNull(relationship, file + ": no relationship " + targetName);
        property = relationship.findProperty(targetProperty);
      }
      assertNotNull(property, file + ": no property " + targetName + "." + targetProperty);
    }
    if (!"Y".equals(XmlHandler.getTagValue(transform, "returning_graph"))) {
      assertFalse(XmlHandler.getTagValue(transform, "connection").isEmpty(), file);
    }
  }

  /** The vectors written, the indexes created and the vectors searched have the same size. */
  @Test
  void theVectorSampleIsConsistent() throws Exception {
    GraphModel model = loadGraphModel("vector-docs");
    assertEquals(
        GraphPropertyType.Vector, model.findNode("Doc").findProperty("embedding").getType());
    assertEquals(
        GraphPropertyType.Vector,
        model.findRelationship("ABOUT").findProperty("embedding").getType());

    Node workflow =
        XmlHandler.getSubNode(read(NEO4J_SAMPLES + FOLDER + "vector-search.hwf"), "workflow");
    Map<String, Integer> dimensions = new HashMap<>();
    Set<String> objectTypes = new HashSet<>();
    for (Node action : elements(workflow, false)) {
      if (!"NEO4J_INDEX".equals(type(action))) {
        continue;
      }
      for (Node update : XmlHandler.getNodes(XmlHandler.getSubNode(action, "updates"), "update")) {
        assertEquals("VECTOR", XmlHandler.getTagValue(update, "index_type"));
        objectTypes.add(XmlHandler.getTagValue(update, "object_type"));
        dimensions.put(
            XmlHandler.getTagValue(update, "index_name"),
            Integer.parseInt(XmlHandler.getTagValue(update, "vector_dimensions")));
      }
    }
    assertEquals(Set.of("NODE", "RELATIONSHIP"), objectTypes);

    for (String pipeline : List.of("vector-search-load.hpl", "vector-search-query.hpl")) {
      Node root = XmlHandler.getSubNode(read(NEO4J_SAMPLES + FOLDER + pipeline), "pipeline");
      for (Node transform : elements(root, true)) {
        if (!"DataGrid".equals(type(transform))) {
          continue;
        }
        List<Integer> vectorColumns = new ArrayList<>();
        List<Node> fields =
            XmlHandler.getNodes(XmlHandler.getSubNode(transform, "fields"), "field");
        for (int i = 0; i < fields.size(); i++) {
          if ("Vector".equals(type(fields.get(i)))) {
            vectorColumns.add(i);
          }
        }
        assertFalse(vectorColumns.isEmpty(), pipeline);
        for (Node line : XmlHandler.getNodes(XmlHandler.getSubNode(transform, "data"), "line")) {
          List<Node> items = XmlHandler.getNodes(line, "item");
          for (int column : vectorColumns) {
            String vector = XmlHandler.getNodeValue(items.get(column));
            int size = vector.replaceAll("[\\[\\]]", "").split(",").length;
            for (int indexDimensions : dimensions.values()) {
              assertEquals(indexDimensions, size, pipeline + ": " + vector);
            }
          }
        }
      }
    }

    Node query =
        XmlHandler.getSubNode(read(NEO4J_SAMPLES + FOLDER + "vector-search-query.hpl"), "pipeline");
    Set<String> searched = new HashSet<>();
    for (Node transform : elements(query, true)) {
      if ("GraphVectorSearch".equals(type(transform))) {
        GraphVectorSearchMeta meta = new GraphVectorSearchMeta();
        meta.loadXml(transform, new MemoryMetadataProvider());
        assertTrue(dimensions.containsKey(meta.getIndexName()), meta.getIndexName());
        searched.add(meta.getElementType().name());
        // FalkorDB finds the index by label and property: they must match the model
        if (meta.isSearchingRelationships()) {
          assertNotNull(model.findRelationship(meta.getLabel()).findProperty("embedding"));
        } else {
          assertNotNull(model.findNode(meta.getLabel()).findProperty(meta.getVectorProperty()));
        }
      }
    }
    assertEquals(Set.of("NODE", "RELATIONSHIP"), searched);
  }

  /** The Bolt connections only hold settings of their graph database type. */
  @Test
  void theBoltConnectionsMatchTheirType() throws Exception {
    Set<String> keys = new HashSet<>();
    for (Class<?> c = BoltGraphDatabase.class; c != null; c = c.getSuperclass()) {
      for (Field field : c.getDeclaredFields()) {
        HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
        if (property != null) {
          keys.add(property.key().isEmpty() ? field.getName() : property.key());
        }
      }
    }
    Map<String, String> expected =
        Map.of(
            "demo-neo4j", boltPluginId(Neo4jGraphDatabase.class),
            "demo-memgraph", boltPluginId(MemgraphGraphDatabase.class),
            "demo-neptune", boltPluginId(NeptuneGraphDatabase.class));
    for (Map.Entry<String, String> entry : expected.entrySet()) {
      JsonNode json = readJson(NEO4J_SAMPLES + CONNECTIONS + entry.getKey() + ".json");
      assertEquals(entry.getKey(), json.get("name").asText());
      JsonNode type = json.get("graphDatabase").get(entry.getValue());
      assertNotNull(type, entry.getKey() + " is not of type " + entry.getValue());
      for (Iterator<String> it = type.fieldNames(); it.hasNext(); ) {
        String key = it.next();
        assertTrue(keys.contains(key), entry.getKey() + ": unknown setting " + key);
      }
      // No secrets in the samples: credentials come from variables or are empty
      String password = type.get("password").asText();
      assertTrue(password.isEmpty() || password.startsWith("${"), entry.getKey());
    }
  }

  /** Every connection in the samples has a type which a graph plugin provides. */
  @Test
  void everyConnectionHasAKnownType() throws Exception {
    Set<String> types = new HashSet<>(OTHER_GRAPH_DATABASE_IDS);
    types.add(boltPluginId(Neo4jGraphDatabase.class));
    types.add(boltPluginId(MemgraphGraphDatabase.class));
    types.add(boltPluginId(NeptuneGraphDatabase.class));
    int count = 0;
    for (String root : SAMPLE_ROOTS) {
      FileObject folder = HopVfs.getFileObject(root + CONNECTIONS);
      if (!folder.exists()) {
        continue;
      }
      for (FileObject child : folder.getChildren()) {
        JsonNode json = readJson(root + CONNECTIONS + child.getName().getBaseName());
        Iterator<String> names = json.get("graphDatabase").fieldNames();
        String type = names.next();
        assertFalse(names.hasNext());
        assertTrue(types.contains(type), child.getName().getBaseName() + ": " + type);
        count++;
      }
    }
    assertEquals(6, count);
  }
}
