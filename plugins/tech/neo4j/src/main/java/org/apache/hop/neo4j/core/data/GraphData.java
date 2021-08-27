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

package org.apache.hop.neo4j.core.data;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

import java.util.ArrayList;
import java.util.List;

public class GraphData {

  protected List<GraphNodeData> nodes;

  protected List<GraphRelationshipData> relationships;

  protected String sourcePipelineName;

  protected String sourceTransformName;

  public GraphData() {
    nodes = new ArrayList<>();
    relationships = new ArrayList<>();
  }

  public GraphData(List<GraphNodeData> nodes, List<GraphRelationshipData> relationships) {
    this();
    this.nodes = nodes;
    this.relationships = relationships;
  }

  @Override
  public boolean equals(Object object) {
    if (object == null) {
      return false;
    }
    if (!(object instanceof GraphData)) {
      return false;
    }
    if (object == this) {
      return true;
    }

    // Same number of nodes, same number of relationships,
    // Same node IDs, same properties
    // TODO:
    throw new RuntimeException("equals() not yet implemented on GraphData");
  }

  public GraphData(GraphData source) {
    this();
    replace(source);
  }

  public void replace(GraphData source) {
    // Copy nodes
    //
    nodes = new ArrayList<>();
    for (GraphNodeData node : source.getNodes()) {
      nodes.add(new GraphNodeData(node));
    }

    // replace relationships
    //
    relationships = new ArrayList<>();
    for (GraphRelationshipData relationship : source.getRelationships()) {
      relationships.add(new GraphRelationshipData(relationship));
    }

    sourcePipelineName = source.sourcePipelineName;
    sourceTransformName = source.sourceTransformName;
  }

  public JSONObject toJson() {
    JSONObject jGraph = new JSONObject();

    JSONArray jNodes = new JSONArray();
    jGraph.put("nodes", jNodes);
    for (GraphNodeData node : nodes) {
      jNodes.add(node.toJson());
    }

    JSONArray jRelationships = new JSONArray();
    jGraph.put("relationships", jRelationships);
    for (GraphRelationshipData relationship : relationships) {
      jRelationships.add(relationship.toJson());
    }

    jGraph.put("source_pipeline", sourcePipelineName);
    jGraph.put("source_transform", sourceTransformName);

    return jGraph;
  }

  /**
   * Convert from JSON to Graph data
   *
   * @param graphJsonString
   */
  public GraphData(String graphJsonString) throws ParseException {
    this((JSONObject) new JSONParser().parse(graphJsonString));
  }

  public GraphData(JSONObject jGraph) {
    this();
    JSONArray jNodes = (JSONArray) jGraph.get("nodes");
    for (int i = 0; i < jNodes.size(); i++) {
      JSONObject jNode = (JSONObject) jNodes.get(i);
      nodes.add(new GraphNodeData(jNode));
    }

    JSONArray jRelationships = (JSONArray) jGraph.get("relationships");
    for (int i = 0; i < jRelationships.size(); i++) {
      JSONObject jRelationship = (JSONObject) jRelationships.get(i);
      relationships.add(new GraphRelationshipData(jRelationship));
    }

    sourcePipelineName = (String) jGraph.get("source_pipeline");
    sourceTransformName = (String) jGraph.get("source_transform");
  }

  public String toJsonString() {
    return toJson().toJSONString();
  }

  @Override
  public GraphData clone() {
    return new GraphData(this);
  }

  public GraphData createEmptyCopy() {
    GraphData copy = new GraphData();
    copy.setSourcePipelineName(getSourcePipelineName());
    copy.setSourceTransformName(getSourceTransformName());
    return copy;
  }

  /**
   * Find a node with the given ID
   *
   * @param nodeId
   * @return The mode with the given ID or null if the node was not found
   */
  public GraphNodeData findNode(String nodeId) {
    for (GraphNodeData node : nodes) {
      if (node.getId() != null && node.getId().equals(nodeId)) {
        return node;
      }
    }
    return null;
  }

  /**
   * Find a relationship with the given ID
   *
   * @param relationshipId
   * @return The relationship with the given name or null if the relationship was not found
   */
  public GraphRelationshipData findRelationship(String relationshipId) {
    for (GraphRelationshipData relationship : relationships) {
      if (relationship.getId() != null && relationship.getId().equals(relationshipId)) {
        return relationship;
      }
    }
    return null;
  }

  /**
   * Find a relationship with source and target
   *
   * @param sourceId
   * @param targetId
   * @return the relationship or null if nothing was found.
   */
  public GraphRelationshipData findRelationship(String sourceId, String targetId) {
    for (GraphRelationshipData relationship : relationships) {
      if (relationship.getSourceNodeId().equals(sourceId)
          && relationship.getTargetNodeId().equals(targetId)) {
        return relationship;
      }
      // Also match on the inverse
      if (relationship.getSourceNodeId().equals(targetId)
          && relationship.getTargetNodeId().equals(sourceId)) {
        return relationship;
      }
    }
    return null;
  }

  public GraphData(Result result) {
    this();

    while (result.hasNext()) {
      Record record = result.next();
      for (String key : record.keys()) {
        Value value = record.get(key);
        if ("NODE".equals(value.type().name())) {
          update(value.asNode());
        } else if ("RELATIONSHIP".equals(value.type().name())) {
          update(value.asRelationship());
        } else if ("PATH".equals(value.type().name())) {
          update(value.asPath());
        }
      }
    }
  }

  public void update(GraphNodeData dataNode) {
    int index = nodes.indexOf(dataNode);
    if (index < 0) {
      nodes.add(dataNode);
    } else {
      nodes.set(index, dataNode);
    }
  }

  public void update(GraphRelationshipData dataRelationship) {
    int index = relationships.indexOf(dataRelationship);
    if (index < 0) {
      relationships.add(dataRelationship);
    } else {
      relationships.set(index, dataRelationship);
    }
  }

  public void update(Node node) {
    update(new GraphNodeData(node));
  }

  public void update(Relationship relationship) {
    update(new GraphRelationshipData(relationship));
  }

  public void update(Path path) {
    for (Node node : path.nodes()) {
      update(node);
    }
    for (Relationship relationship : path.relationships()) {
      update(relationship);
    }
  }

  public GraphNodeData findNodeWithProperty(String propertyId, Object value) {
    for (GraphNodeData node : nodes) {
      GraphPropertyData property = node.findProperty(propertyId);
      if (property != null) {
        if (property.getValue() != null && property.getValue().equals(value)) {
          return node;
        }
      }
    }
    return null;
  }

  public List<GraphRelationshipData> findRelationships(String label) {
    List<GraphRelationshipData> rels = new ArrayList<>();
    for (GraphRelationshipData relationship : relationships) {
      if (relationship.getLabel().equals(label)) {
        rels.add(relationship);
      }
    }
    return rels;
  }

  public GraphNodeData findTopNode(String labelToFollow) {

    // Start from any relationship with given label
    //
    List<GraphRelationshipData> rels = findRelationships(labelToFollow);
    // System.out.println("Found "+rels.size()+" relationships for "+labelToFollow);
    if (rels.size() == 0) {
      return null;
    }
    GraphRelationshipData rel = rels.get(0);
    GraphNodeData node = null;
    while (rel != null) {
      node = findNode(rel.getSourceNodeId());
      rel = findRelationshipsWithTarget(rels, node.getId());
    }
    return node;
  }

  public GraphRelationshipData findRelationshipsWithTarget(
      List<GraphRelationshipData> rels, String targetId) {
    for (GraphRelationshipData relationship : rels) {
      if (relationship.getTargetNodeId().equals(targetId)) {
        return relationship;
      }
    }
    return null;
  }

  public GraphRelationshipData findRelationshipsWithSource(
      List<GraphRelationshipData> rels, String sourceId) {
    for (GraphRelationshipData relationship : rels) {
      if (relationship.getSourceNodeId().equals(sourceId)) {
        return relationship;
      }
    }
    return null;
  }

  private List<GraphNodeData> getPath(GraphNodeData startNode, String mainRelationshipLabel) {
    GraphNodeData currentNode = startNode;
    GraphNodeData previousNode = null;
    List<GraphNodeData> path = new ArrayList<>();
    while (currentNode != null) {
      path.add(currentNode);
      GraphNodeData nextNode = findNextNode(currentNode, mainRelationshipLabel, previousNode);
      previousNode = currentNode;
      currentNode = nextNode;
    }
    return path;
  }

  private GraphNodeData getOtherNode(GraphRelationshipData nextRel, GraphNodeData currentNode) {
    if (nextRel.getSourceNodeId().equals(currentNode.getId())) {
      return findNode(nextRel.getTargetNodeId());
    }
    if (nextRel.getTargetNodeId().equals(currentNode.getId())) {
      return findNode(nextRel.getSourceNodeId());
    }
    return null;
  }

  public int getFirstRelationshipLabelIndex(
      List<GraphRelationshipData> rels, String mainRelationshipLabel) {
    for (int i = 0; i < rels.size(); i++) {
      GraphRelationshipData relationship = rels.get(i);
      if (mainRelationshipLabel.equals(relationship.getLabel())) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Find all next nodes from the given current node. Exclude the main relationship label given
   *
   * @param currentNode
   * @param excludeLabel
   * @return
   */
  private List<GraphNodeData> findNextNodes(GraphNodeData currentNode, String excludeLabel) {

    List<GraphNodeData> nextNodes = new ArrayList<>();
    for (GraphRelationshipData relationship : relationships) {

      if (!relationship.getLabel().equals(excludeLabel)) {
        if (relationship.getSourceNodeId().equals(currentNode.getId())) {
          nextNodes.add(findNode(relationship.getTargetNodeId()));
        }
        if (relationship.getTargetNodeId().equals(currentNode.getId())) {
          nextNodes.add(findNode(relationship.getSourceNodeId()));
        }
      }
    }
    return nextNodes;
  }

  public GraphNodeData findNextNode(
      GraphNodeData currentNode, String mainRelationshipLabel, GraphNodeData excludeNode) {
    List<GraphRelationshipData> rels = findRelationships(currentNode);
    for (GraphRelationshipData rel : rels) {
      if (mainRelationshipLabel.equals(rel.getLabel())) {
        if (excludeNode == null
            || !(rel.getSourceNodeId().equals(excludeNode.getId())
                || rel.getTargetNodeId().equals(excludeNode.getId()))) {
          // Don't return the same node, return the other
          //
          if (rel.getSourceNodeId().equals(currentNode.getId())) {
            return findNode(rel.getTargetNodeId());
          } else {
            return findNode(rel.getSourceNodeId());
          }
        }
      }
    }
    return null;
  }

  /**
   * Get the list of relationships involved in the given node (source or target)
   *
   * @param dataNode
   * @return
   */
  public List<GraphRelationshipData> findRelationships(GraphNodeData dataNode) {

    List<GraphRelationshipData> found = new ArrayList<>();
    for (GraphRelationshipData relationship : relationships) {
      if (relationship.getSourceNodeId().equals(dataNode.getId())
          || relationship.getTargetNodeId().equals(dataNode.getId())) {
        found.add(relationship);
      }
    }
    return found;
  }

  /**
   * Gets nodes
   *
   * @return value of nodes
   */
  public List<GraphNodeData> getNodes() {
    return nodes;
  }

  /** @param nodes The nodes to set */
  public void setNodes(List<GraphNodeData> nodes) {
    this.nodes = nodes;
  }

  /**
   * Gets relationships
   *
   * @return value of relationships
   */
  public List<GraphRelationshipData> getRelationships() {
    return relationships;
  }

  /** @param relationships The relationships to set */
  public void setRelationships(List<GraphRelationshipData> relationships) {
    this.relationships = relationships;
  }

  /**
   * Gets sourcePipelineName
   *
   * @return value of sourcePipelineName
   */
  public String getSourcePipelineName() {
    return sourcePipelineName;
  }

  /** @param sourcePipelineName The sourcePipelineName to set */
  public void setSourcePipelineName(String sourcePipelineName) {
    this.sourcePipelineName = sourcePipelineName;
  }

  /**
   * Gets sourceTransformName
   *
   * @return value of sourceTransformName
   */
  public String getSourceTransformName() {
    return sourceTransformName;
  }

  /** @param sourceTransformName The sourceTransformName to set */
  public void setSourceTransformName(String sourceTransformName) {
    this.sourceTransformName = sourceTransformName;
  }
}
