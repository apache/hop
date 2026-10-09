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

package org.apache.hop.age;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hop.core.graph.GraphDatabasePlugin;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

/**
 * The Apache AGE samples are files only: the connection holds settings of the Apache AGE type,
 * without secrets, and the sample pipeline uses it. The neo4j plugin checks the transforms of the
 * pipeline.
 */
class AgeSamplesTest {

  private static final String CONNECTION =
      "src/main/samples/metadata/graph-database-connection/demo-age.json";
  private static final String PIPELINE = "src/main/samples/graph-databases/social-network-age.hpl";

  @Test
  void theConnectionMatchesTheType() throws Exception {
    Set<String> keys = new HashSet<>();
    for (Class<?> c = AgeGraphDatabase.class; c != null; c = c.getSuperclass()) {
      for (Field field : c.getDeclaredFields()) {
        HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
        if (property != null) {
          keys.add(property.key().isEmpty() ? field.getName() : property.key());
        }
      }
    }
    JsonNode json;
    try (InputStream in = HopVfs.getInputStream(CONNECTION)) {
      json = HopJson.newMapper().readTree(in);
    }
    assertEquals("demo-age", json.get("name").asText());
    String id = AgeGraphDatabase.class.getAnnotation(GraphDatabasePlugin.class).id();
    JsonNode type = json.get("graphDatabase").get(id);
    assertNotNull(type, "The connection is not of type " + id);
    for (Iterator<String> it = type.fieldNames(); it.hasNext(); ) {
      String key = it.next();
      assertTrue(keys.contains(key), "Unknown setting " + key);
    }
    String password = type.get("password").asText();
    assertTrue(password.isEmpty() || password.startsWith("${"), "No passwords in samples");
  }

  @Test
  void thePipelineUsesTheConnection() throws Exception {
    Node pipeline;
    try (InputStream in = HopVfs.getInputStream(PIPELINE)) {
      pipeline = XmlHandler.getSubNode(XmlHandler.loadXmlFile(in), "pipeline");
    }
    int graphTransforms = 0;
    for (Node transform : XmlHandler.getNodes(pipeline, "transform")) {
      String connection = XmlHandler.getTagValue(transform, "connection");
      if (connection != null) {
        assertEquals("demo-age", connection);
        graphTransforms++;
      }
    }
    assertEquals(2, graphTransforms);
  }
}
