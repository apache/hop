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

import org.apache.hop.core.Const;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;

import java.util.ArrayList;
import java.util.List;

public class GraphNodeData {

  protected String id;

  protected List<String> labels;

  protected List<GraphPropertyData> properties;

  protected String propertySetId;

  public GraphNodeData() {
    labels = new ArrayList<>();
    properties = new ArrayList<>();
  }

  public GraphNodeData(String id) {
    this();
    this.id = id;
  }

  public GraphNodeData(String id, List<String> labels, List<GraphPropertyData> properties) {
    this.id = id;
    this.labels = labels;
    this.properties = properties;
  }

  public GraphNodeData(Node node) {
    this();
    this.id = Long.toString(node.id());
    StringBuilder propertySet = new StringBuilder();
    for (String label : node.labels()) {
      labels.add(label);
      if (propertySet.length() > 0) {
        propertySet.append(",");
      }
      propertySet.append(label);
    }
    for (String propertyKey : node.keys()) {
      Value propertyValue = node.get(propertyKey);
      Object propertyObject = propertyValue.asObject();
      GraphPropertyDataType propertyType =
          GraphPropertyDataType.getTypeFromNeo4jValue(propertyObject);
      properties.add(new GraphPropertyData(propertyKey, propertyObject, propertyType, false));
    }
    this.propertySetId = propertySet.toString();
  }

  public GraphNodeData(GraphNodeData graphNode) {
    this();
    setId(graphNode.getId());

    // Copy labels
    setLabels(new ArrayList<>(graphNode.getLabels()));

    // Copy properties
    List<GraphPropertyData> propertiesCopy = new ArrayList<>();
    for (GraphPropertyData property : graphNode.getProperties()) {
      GraphPropertyData propertyCopy =
          new GraphPropertyData(
              property.getId(), property.getValue(), property.getType(), property.isPrimary());
      propertiesCopy.add(propertyCopy);
    }
    setProperties(propertiesCopy);

    setPropertySetId(graphNode.getPropertySetId());
  }

  public JSONObject toJson() {
    JSONObject jNode = new JSONObject();
    jNode.put("id", id);

    jNode.put("labels", labels);

    JSONArray jProperties = new JSONArray();
    jNode.put("properties", jProperties);
    for (GraphPropertyData property : properties) {
      jProperties.add(property.toJson());
    }

    jNode.put("property_set", propertySetId);

    return jNode;
  }

  public GraphNodeData(JSONObject jNode) {

    id = (String) jNode.get("id");
    JSONArray jLabels = (JSONArray) jNode.get("labels");
    for (int i = 0; i < jLabels.size(); i++) {
      labels.add((String) jLabels.get(i));
    }

    JSONArray jProperties = (JSONArray) jNode.get("properties");
    for (int i = 0; i < jProperties.size(); i++) {
      properties.add(new GraphPropertyData((JSONObject) jProperties.get(i)));
    }

    propertySetId = (String) jNode.get("property_set");
  }

  @Override
  public GraphNodeData clone() {
    return new GraphNodeData(this);
  }

  @Override
  public String toString() {
    return id == null ? super.toString() : id;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof GraphNodeData)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    return ((GraphNodeData) o).getId().equals(id);
  }

  /**
   * Search for the property with the given ID, case insensitive
   *
   * @param id the name of the property to look for
   * @return the property or null if nothing could be found.
   */
  public GraphPropertyData findProperty(String id) {
    for (GraphPropertyData property : properties) {
      if (property.getId().equalsIgnoreCase(id)) {
        return property;
      }
    }
    return null;
  }

  /**
   * Find a String property called name
   *
   * @return the name property string or if not available, the ID
   */
  public String getName() {
    GraphPropertyData nameProperty = findProperty("name");
    if (nameProperty == null || nameProperty.getValue() == null) {
      return id;
    }
    return nameProperty.getValue().toString();
  }

  public String getNodeText() {
    String nodeText = getName() + Const.CR;
    GraphPropertyData typeProperty = findProperty("type");
    if (typeProperty != null && typeProperty.getValue() != null) {
      nodeText += typeProperty.getValue().toString();
    }
    if (labels.size() > 0) {
      nodeText += " (:" + labels.get(0) + ")";
    }

    return nodeText;
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /** @param id The id to set */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Gets labels
   *
   * @return value of labels
   */
  public List<String> getLabels() {
    return labels;
  }

  /** @param labels The labels to set */
  public void setLabels(List<String> labels) {
    this.labels = labels;
  }

  /**
   * Gets properties
   *
   * @return value of properties
   */
  public List<GraphPropertyData> getProperties() {
    return properties;
  }

  /** @param properties The properties to set */
  public void setProperties(List<GraphPropertyData> properties) {
    this.properties = properties;
  }

  /**
   * Gets propertySetId
   *
   * @return value of propertySetId
   */
  public String getPropertySetId() {
    return propertySetId;
  }

  /** @param propertySetId The propertySetId to set */
  public void setPropertySetId(String propertySetId) {
    this.propertySetId = propertySetId;
  }
}
