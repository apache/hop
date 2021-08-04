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

package org.apache.hop.neo4j.model.arrows;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.neo4j.model.GraphModel;
import org.apache.hop.neo4j.model.GraphNode;
import org.apache.hop.neo4j.model.GraphPresentation;
import org.apache.hop.neo4j.model.GraphProperty;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.model.GraphRelationship;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ArrowsAppImporter {

  public static GraphModel importFromArrowsJson(String jsonString) throws HopException {
    try {
      GraphModel graphModel = new GraphModel();
      JSONParser parser = new JSONParser();

      JSONObject jModel = (JSONObject) parser.parse(jsonString);

      Map<String, GraphNode> nodesMap = new HashMap<>();

      // Import the nodes
      //
      JSONArray jNodes = (JSONArray) jModel.get("nodes");
      for (int n = 0; n < jNodes.size(); n++) {
        JSONObject jNode = (JSONObject) jNodes.get(n);

        GraphNode graphNode = new GraphNode();

        // ID is ignored. We'll take the caption as the name of the node for now

        // Labels
        String name = (String) jNode.get("caption");
        graphNode.setName(name);

        // Labels
        JSONArray jLabels = (JSONArray) jNode.get("labels");
        for (int l = 0; l < jLabels.size(); l++) {
          String label = (String) jLabels.get(l);
          graphNode.getLabels().add(label);
        }

        // Properties
        //
        graphNode.getProperties().addAll(importProperties(jNode, "properties"));

        // Add the node to the model
        graphModel.getNodes().add(graphNode);

        // Save the node below the ID to use it later in the relationships...
        //
        String key = (String) jNode.get("id");
        nodesMap.put(key, graphNode);
      }

      // Import the relationships...
      //
      JSONArray jRelationships = (JSONArray) jModel.get("relationships");
      for (int r = 0; r < jRelationships.size(); r++) {
        JSONObject jRelationship = (JSONObject) jRelationships.get(r);

        GraphRelationship graphRelationship = new GraphRelationship();

        // The label
        //
        String label = (String) jRelationship.get("type");
        graphRelationship.setLabel(label);

        // The name is then the same as the label...
        //
        if (StringUtils.isNotEmpty(label)) {
          graphRelationship.setName(label);
        } else {
          // Take the natural ID as fall-back
          String id = (String) jRelationship.get("id");
          graphRelationship.setName(id);
        }

        String fromKey = (String) jRelationship.get("fromId");
        GraphNode fromNode = nodesMap.get(fromKey);
        if (fromNode != null) {
          graphRelationship.setNodeSource(fromNode.getName());
        }
        String toKey = (String) jRelationship.get("toId");
        GraphNode toNode = nodesMap.get(toKey);
        if (toNode != null) {
          graphRelationship.setNodeTarget(toNode.getName());
        }

        // Properties
        graphRelationship.getProperties().addAll(importProperties(jRelationship, "properties"));

        // Add the relationship to the model.
        //
        graphModel.getRelationships().add(graphRelationship);
      }

      return graphModel;
    } catch (Exception e) {
      throw new HopException("Error parsing Cypher Workbench model", e);
    }
  }

  private static List<GraphProperty> importProperties(JSONObject j, String propertiesKey) {
    List<GraphProperty> properties = new ArrayList<>();
    JSONObject jNodeProperties = (JSONObject) j.get(propertiesKey);
    if (jNodeProperties != null) {
      for (Object jPropertyKey : jNodeProperties.keySet()) {
        // This is simple: key is the property name and the value is the data type
        //
        String propertyName = (String) jPropertyKey;
        String propertyTypeString = (String) jNodeProperties.get(jPropertyKey);

        GraphPropertyType propertyType = GraphPropertyType.parseCode(propertyTypeString);
        GraphProperty nodeProperty =
            new GraphProperty(propertyName, propertyName, propertyType, false, false, false, false);
        properties.add(nodeProperty);
      }
    }
    return properties;
  }
}
