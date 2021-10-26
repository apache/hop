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

package org.apache.hop.neo4j.model.cw;

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

public class CypherWorkbenchImporter {

  public static GraphModel importFromCwJson(String jsonString) throws HopException {
    try {
      GraphModel graphModel = new GraphModel();
      JSONParser parser = new JSONParser();

      JSONObject jModel = (JSONObject) parser.parse(jsonString);
      JSONObject jMetadata = (JSONObject) jModel.get("metadata");

      String modelName = (String) jMetadata.get("title");
      graphModel.setName(modelName);
      String modelDescription = (String) jMetadata.get("description");
      graphModel.setDescription(modelDescription);

      JSONObject jDataModel = (JSONObject) jModel.get("dataModel");

      // Keep track of which node (key is the name) has secondary node labels
      // These are stored as extra nodes in the model
      // We should remove all the secondary nodes later on.
      //
      Map<String, List<String>> secondaryNodeLabels = new HashMap<>();
      List<GraphNode> secondaryLabelNodes = new ArrayList<>();

      // Import the nodes
      //
      JSONObject jNodeLabels = (JSONObject) jDataModel.get("nodeLabels");
      for (Object jNodeKey : jNodeLabels.keySet()) {
        JSONObject jNodeLabel = (JSONObject) jNodeLabels.get(jNodeKey);
        String classTypeString = (String) jNodeLabel.get("classType");
        if ("NodeLabel".equals(classTypeString)) {
          GraphNode graphNode = new GraphNode();
          String key = (String) jNodeLabel.get("key");
          String label = (String) jNodeLabel.get("label");
          graphNode.setName(key);
          graphNode.getLabels().add(label);

          // Get the properties
          //
          graphNode.getProperties().addAll(importProperties(jNodeLabel, "properties"));

          JSONObject jNodeDisplay = (JSONObject) jNodeLabel.get("display");
          Double nodeLocationX = getDouble(jNodeDisplay.get("x"));
          Double nodeLocationY = getDouble(jNodeDisplay.get("y"));
          if (nodeLocationX != null && nodeLocationY != null) {
            graphNode.setPresentation(
                new GraphPresentation(nodeLocationX.intValue(), nodeLocationY.intValue()));
          }

          boolean isOnlySecondaryNodeLabel = getBoolean(jNodeLabel.get("isOnlySecondaryNodeLabel"));
          if (!isOnlySecondaryNodeLabel) {
            // Get the secondary node label keys:
            //
            JSONArray jSecondaryNodeLabelKeys =
                (JSONArray) jNodeLabel.get("secondaryNodeLabelKeys");
            List<String> keys = new ArrayList<>();
            for (int s = 0; s < jSecondaryNodeLabelKeys.size(); s++) {
              keys.add((String) jSecondaryNodeLabelKeys.get(s));
            }
            secondaryNodeLabels.put(graphNode.getName(), keys);

            // Add to the model...
            //
            graphModel.getNodes().add(graphNode);
          } else {
            secondaryLabelNodes.add(graphNode);
          }
        }
      }

      // Now add the labels from the secondary nodes to the nodes...
      //
      for (String nodeName : secondaryNodeLabels.keySet()) {
        GraphNode primaryNode = graphModel.findNode(nodeName);
        List<String> keys = secondaryNodeLabels.get(nodeName);
        for (String key : keys) {
          for (GraphNode secondaryLabelNode : secondaryLabelNodes) {
            if (secondaryLabelNode.getName().equals(key)) {
              primaryNode.getLabels().addAll(secondaryLabelNode.getLabels());
            }
          }
        }
      }

      // Import the relationships...
      //
      JSONObject jRelationshipTypes = (JSONObject) jDataModel.get("relationshipTypes");
      for (Object jRelationshipTypeKey : jRelationshipTypes.keySet()) {
        JSONObject jRelationshipType = (JSONObject) jRelationshipTypes.get(jRelationshipTypeKey);
        String classTypeString = (String) jRelationshipType.get("classType");
        if ("RelationshipType".equals(classTypeString)) {
          String relationshipKey = (String) jRelationshipType.get("key");
          String relationshipType = (String) jRelationshipType.get("type");
          String relationshipStartKey = (String) jRelationshipType.get("startNodeLabelKey");
          String relationshipEndKey = (String) jRelationshipType.get("endNodeLabelKey");
          List<GraphProperty> relationshipProperties =
              importProperties(jRelationshipType, "properties");
          GraphRelationship graphRelationship =
              new GraphRelationship(
                  relationshipKey,
                  relationshipType,
                  relationshipType,
                  relationshipProperties,
                  relationshipStartKey,
                  relationshipEndKey);
          graphModel.getRelationships().add(graphRelationship);
        }
      }

      return graphModel;
    } catch (Exception e) {
      throw new HopException("Error parsing Cypher Workbench model", e);
    }
  }

  private static boolean getBoolean(Object obj) {
    if (obj == null) {
      return false;
    }
    return (boolean) obj;
  }

  private static Double getDouble(Object obj) {
    if (obj == null) {
      return Double.valueOf(0);
    }
    if (obj instanceof Double) {
      return (Double) obj;
    }
    if (obj instanceof Float) {
      return ((Float) obj).doubleValue();
    }
    if (obj instanceof Long) {
      return ((Long) obj).doubleValue();
    }
    throw new RuntimeException("Unrecognized data type for value " + obj.toString());
  }

  private static List<GraphProperty> importProperties(JSONObject j, String propertiesKey) {
    List<GraphProperty> properties = new ArrayList<>();
    JSONObject jNodeProperties = (JSONObject) j.get(propertiesKey);
    if (jNodeProperties != null) {
      for (Object jPropertyKey : jNodeProperties.keySet()) {
        JSONObject jNodeProperty = (JSONObject) jNodeProperties.get(jPropertyKey);
        String propertyName = (String) jNodeProperty.get("name");
        String propertyKey = (String) jNodeProperty.get("key");
        String propertyTypeString = (String) jNodeProperty.get("datatype");
        Boolean propertyPartOfKey = (Boolean) jNodeProperty.get("isPartOfKey");
        Boolean propertyMustExist = (Boolean) jNodeProperty.get("mustExist");
        Boolean propertyHasUniqueConstraints = (Boolean) jNodeProperty.get("hasUniqueConstraint");
        Boolean propertyIsIndexed = (Boolean) jNodeProperty.get("isIndexed");

        GraphPropertyType propertyType = GraphPropertyType.parseCode(propertyTypeString);
        GraphProperty nodeProperty;
        nodeProperty =
            new GraphProperty(
                propertyKey,
                propertyName,
                propertyType,
                propertyPartOfKey != null && propertyPartOfKey,
                propertyMustExist != null && propertyMustExist,
                propertyHasUniqueConstraints != null && propertyHasUniqueConstraints,
                propertyIsIndexed != null && propertyIsIndexed);
        properties.add(nodeProperty);
      }
    }
    return properties;
  }

  /**
   * Change imported keys to the provided labels. Make sure we don't create any duplicates and so
   * on. This method does sanity checks on the labels and names. This does not change the source
   * graph model.
   *
   * @param sourceModel The source graph model, typically imported with importFromCwJson()
   * @return A modified copy of the source model
   */
  public static final GraphModel changeNamesToLabels(GraphModel sourceModel) throws HopException {
    GraphModel graphModel = new GraphModel(sourceModel);

    // Do sanity check on the labels
    //
    Set<String> nodeLabels = new HashSet<>();
    for (GraphNode graphNode : graphModel.getNodes()) {
      if (graphNode.getLabels().isEmpty()) {
        throw new HopException("No node labels found for node " + graphNode.getName());
      }
      for (String label : graphNode.getLabels()) {
        if (nodeLabels.contains(label)) {
          throw new HopException(
              "Node label '" + label + "' is used more than once in model " + graphModel.getName());
        }
      }
      // We also need to make sure that the label is not an existing node name...
      //
      String label = graphNode.getLabels().get(0);
      if (graphModel.findNode(label) != null) {
        throw new HopException(
            "A node named '"
                + label
                + "' already exists in the model. Renaming nodes might break consistency");
      }

      nodeLabels.add(label);
    }

    // Now we can change the node name to the first label
    // Also change the node name in relationship source or target...
    //
    for (GraphNode graphNode : graphModel.getNodes()) {
      String label = graphNode.getLabels().get(0);
      String oldName = graphNode.getName();
      graphNode.setName(label);

      // Change the node properties : description --> name
      //
      for (GraphProperty property : graphNode.getProperties()) {
        property.setName(property.getDescription());
        property.setDescription(null);
      }

      // Change the relationships to make source/target match the new names
      //
      for (GraphRelationship relationship : graphModel.getRelationships()) {
        if (relationship.getNodeSource().equals(oldName)) {
          relationship.setNodeSource(label);
        }
        if (relationship.getNodeTarget().equals(oldName)) {
          relationship.setNodeTarget(label);
        }
      }
    }

    // Change the relationship names to their labels as well...
    // First do a sanity check on the relationship labels, check for duplicates
    //
    Set<String> relationshipLabels = new HashSet<>();
    for (GraphRelationship relationship : graphModel.getRelationships()) {
      String label = relationship.getLabel();
      if (StringUtils.isEmpty(label)) {
        throw new HopException(
            "No relationship label found for relationship between nodes: "
                + relationship.getNodeSource()
                + " and "
                + relationship.getNodeTarget());
      }
      // We also need to make sure that the label is not an existing node name...
      //
      if (graphModel.findRelationship(label) != null) {
        throw new HopException(
            "A relationship named '"
                + label
                + "' already exists in the model. Renaming relationships might break consistency");
      }

      relationshipLabels.add(label);
    }

    // Now rename the relationships to their labels
    // Also set a description
    //
    for (GraphRelationship relationship : graphModel.getRelationships()) {
      relationship.setName(relationship.getLabel());
      relationship.setDescription(
          relationship.getNodeSource() + " - " + relationship.getNodeTarget());

      // Fix the property names as well
      //
      for (GraphProperty property : relationship.getProperties()) {
        property.setName(property.getDescription());
        property.setDescription(null);
      }
    }

    return graphModel;
  }
}
