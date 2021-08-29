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
 *
 */

package org.apache.hop.neo4j.core.data;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Relationship;

import java.util.ArrayList;
import java.util.List;

public class GraphRelationshipData {

  protected String id;

  protected String label;

  protected List<GraphPropertyData> properties;

  protected String sourceNodeId;

  protected String targetNodeId;

  protected String propertySetId;

  public GraphRelationshipData() {
    properties = new ArrayList<>();
  }

  public GraphRelationshipData(
      String id,
      String label,
      List<GraphPropertyData> properties,
      String nodeSource,
      String nodeTarget) {
    this.id = id;
    this.label = label;
    this.properties = properties;
    this.sourceNodeId = nodeSource;
    this.targetNodeId = nodeTarget;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof GraphRelationshipData)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    return ((GraphRelationshipData) o).getId().equalsIgnoreCase(id);
  }

  @Override
  public String toString() {
    return id == null ? super.toString() : id;
  }

  public GraphRelationshipData(GraphRelationshipData graphRelationship) {
    this();

    setId(graphRelationship.getId());
    setLabel(graphRelationship.getLabel());
    setSourceNodeId(graphRelationship.getSourceNodeId());
    setTargetNodeId(graphRelationship.getTargetNodeId());

    List<GraphPropertyData> properties = new ArrayList<>();
    for (GraphPropertyData property : graphRelationship.getProperties()) {
      properties.add(
          new GraphPropertyData(
              property.getId(), property.getValue(), property.getType(), property.isPrimary()));
    }
    setProperties(properties);
    setPropertySetId(graphRelationship.getPropertySetId());
  }

  public GraphRelationshipData(Relationship relationship) {
    this();
    setId(Long.toString(relationship.id()));
    setSourceNodeId(Long.toString(relationship.startNodeId()));
    setTargetNodeId(Long.toString(relationship.endNodeId()));
    setLabel(relationship.type());
    for (String propertyKey : relationship.keys()) {
      Value propertyValue = relationship.get(propertyKey);
      Object propertyObject = propertyValue.asObject();
      GraphPropertyDataType propertyType =
          GraphPropertyDataType.getTypeFromNeo4jValue(propertyObject);
      properties.add(new GraphPropertyData(propertyKey, propertyObject, propertyType, false));
    }
  }

  public JSONObject toJson() {
    JSONObject jRelationship = new JSONObject();

    jRelationship.put("id", id);
    jRelationship.put("label", label);
    jRelationship.put("sourceNodeId", sourceNodeId);
    jRelationship.put("targetNodeId", targetNodeId);

    if (!properties.isEmpty()) {
      JSONArray jProperties = new JSONArray();
      jRelationship.put("properties", jProperties);
      for (GraphPropertyData property : properties) {
        jProperties.add(property.toJson());
      }
    }
    jRelationship.put("property_set", propertySetId);

    return jRelationship;
  }

  public GraphRelationshipData(JSONObject jRelationship) {
    this();
    id = (String) jRelationship.get("id");
    label = (String) jRelationship.get("label");
    sourceNodeId = (String) jRelationship.get("sourceNodeId");
    targetNodeId = (String) jRelationship.get("targetNodeId");

    JSONArray jProperties = (JSONArray) jRelationship.get("properties");
    if (jProperties != null) {
      for (int i = 0; i < jProperties.size(); i++) {
        JSONObject jProperty = (JSONObject) jProperties.get(i);
        properties.add(new GraphPropertyData(jProperty));
      }
    }
    propertySetId = (String) jRelationship.get("property_set");
  }

  @Override
  public GraphRelationshipData clone() {
    return new GraphRelationshipData(this);
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
   * Gets label
   *
   * @return value of label
   */
  public String getLabel() {
    return label;
  }

  /** @param label The label to set */
  public void setLabel(String label) {
    this.label = label;
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
   * Gets sourceNodeId
   *
   * @return value of sourceNodeId
   */
  public String getSourceNodeId() {
    return sourceNodeId;
  }

  /** @param sourceNodeId The sourceNodeId to set */
  public void setSourceNodeId(String sourceNodeId) {
    this.sourceNodeId = sourceNodeId;
  }

  /**
   * Gets targetNodeId
   *
   * @return value of targetNodeId
   */
  public String getTargetNodeId() {
    return targetNodeId;
  }

  /** @param targetNodeId The targetNodeId to set */
  public void setTargetNodeId(String targetNodeId) {
    this.targetNodeId = targetNodeId;
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
