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

package org.apache.hop.neo4j.model;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class GraphRelationship {

  @HopMetadataProperty protected String name;

  @HopMetadataProperty protected String description;

  @HopMetadataProperty protected String label;

  @HopMetadataProperty protected List<GraphProperty> properties;

  @HopMetadataProperty protected String nodeSource;

  @HopMetadataProperty protected String nodeTarget;

  public GraphRelationship() {
    properties = new ArrayList<>();
  }

  public GraphRelationship(
      String name,
      String description,
      String label,
      List<GraphProperty> properties,
      String nodeSource,
      String nodeTarget) {
    this.name = name;
    this.description = description;
    this.label = label;
    this.properties = properties;
    this.nodeSource = nodeSource;
    this.nodeTarget = nodeTarget;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof GraphRelationship)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    return ((GraphRelationship) o).getName().equalsIgnoreCase(name);
  }

  @Override
  public String toString() {
    return name == null ? super.toString() : name;
  }

  public GraphRelationship(GraphRelationship graphRelationship) {
    this();
    List<GraphProperty> properties = new ArrayList<>();
    for (GraphProperty property : graphRelationship.getProperties()) {
      properties.add(new GraphProperty(property));
    }

    setName(graphRelationship.getName());
    setDescription(graphRelationship.getDescription());
    setLabel(graphRelationship.getLabel());
    setProperties(properties);
    setNodeSource(graphRelationship.getNodeSource());
    setNodeTarget(graphRelationship.getNodeTarget());
  }

  /**
   * Search for the property with the given name, case insensitive
   *
   * @param name the name of the property to look for
   * @return the property or null if nothing could be found.
   */
  public GraphProperty findProperty(String name) {
    for (GraphProperty property : properties) {
      if (property.getName().equalsIgnoreCase(name)) {
        return property;
      }
    }
    return null;
  }

  /**
   * Validate the integrity of this relationship: make sure that there is a name and a label and
   * that the referenced nodes can be found in the provided list.
   *
   * @param nodes the list of nodes to validate the source and target nodes with.
   */
  public void validateIntegrity(List<GraphNode> nodes) throws HopException {
    if (StringUtils.isEmpty(name)) {
      throw new HopException("A relationship in a graph model needs to have a name");
    }
    if (StringUtils.isEmpty(label)) {
      throw new HopException("A graph relationship needs to have a label");
    }
    boolean sourceFound = false;
    boolean targetFound = false;
    for (GraphNode node : nodes) {
      if (node.getName().equals(getNodeSource())) {
        sourceFound = true;
      }
      if (node.getName().equals(getNodeTarget())) {
        targetFound = true;
      }
    }
    if (!sourceFound) {
      throw new HopException(
          "Source node '"
              + getNodeSource()
              + "' of relationship '"
              + name
              + "' can not be found in the model");
    }
    if (!targetFound) {
      throw new HopException(
          "Target node '"
              + getNodeTarget()
              + "' of relationship '"
              + name
              + "' can not be found in the model");
    }
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public List<GraphProperty> getProperties() {
    return properties;
  }

  public void setProperties(List<GraphProperty> properties) {
    this.properties = properties;
  }

  public String getNodeSource() {
    return nodeSource;
  }

  public void setNodeSource(String nodeSource) {
    this.nodeSource = nodeSource;
  }

  public String getNodeTarget() {
    return nodeTarget;
  }

  public void setNodeTarget(String nodeTarget) {
    this.nodeTarget = nodeTarget;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }
}
