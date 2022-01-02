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

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.ArrayList;
import java.util.List;

public class GraphNode {

  @HopMetadataProperty protected String name;

  @HopMetadataProperty protected String description;

  @HopMetadataProperty protected List<String> labels;

  @HopMetadataProperty protected List<GraphProperty> properties;

  @HopMetadataProperty protected GraphPresentation presentation;

  public GraphNode() {
    labels = new ArrayList<>();
    properties = new ArrayList<>();
    presentation = new GraphPresentation(0, 0);
  }

  public GraphNode(
      String name, String description, List<String> labels, List<GraphProperty> properties) {
    this.name = name;
    this.description = description;
    this.labels = labels;
    this.properties = properties;
    this.presentation = new GraphPresentation(0, 0);
  }

  public GraphNode(GraphNode graphNode) {
    this();
    setName(graphNode.getName());
    setDescription(graphNode.getDescription());

    // Copy labels
    setLabels(new ArrayList<>(graphNode.getLabels()));

    // Copy properties
    List<GraphProperty> propertiesCopy = new ArrayList<>();
    for (GraphProperty property : graphNode.getProperties()) {
      GraphProperty propertyCopy = new GraphProperty(property);
      propertiesCopy.add(propertyCopy);
    }
    setProperties(propertiesCopy);
    setPresentation(graphNode.getPresentation().clone());
  }

  @Override
  public String toString() {
    return "GraphNode{" + "name='" + name + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof GraphNode)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    return ((GraphNode) o).getName().equalsIgnoreCase(name);
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
   * Validate the integrity of this node: make sure that there is a primary key property and that
   * there is at least one label.
   */
  public void validateIntegrity() throws HopException {
    if (StringUtils.isEmpty(name)) {
      throw new HopException("A node in a graph model needs to have a name");
    }
    if (labels == null || labels.isEmpty()) {
      throw new HopException("A graph node needs to have at least one label");
    }
    boolean hasPk = false;
    for (GraphProperty property : properties) {
      if (property.isPrimary()) {
        hasPk = true;
        break;
      }
    }
    if (!hasPk) {
      throw new HopException(
          "Node '"
              + name
              + "' has no primary key field. This makes it impossible to update or create relationships with.");
    }
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /** @param name The name to set */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /** @param description The description to set */
  public void setDescription(String description) {
    this.description = description;
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
  public List<GraphProperty> getProperties() {
    return properties;
  }

  /** @param properties The properties to set */
  public void setProperties(List<GraphProperty> properties) {
    this.properties = properties;
  }

  /**
   * Gets presentation
   *
   * @return value of presentation
   */
  public GraphPresentation getPresentation() {
    return presentation;
  }

  /** @param presentation The presentation to set */
  public void setPresentation(GraphPresentation presentation) {
    this.presentation = presentation;
  }
}
