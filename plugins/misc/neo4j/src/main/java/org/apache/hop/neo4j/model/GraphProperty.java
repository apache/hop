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

package org.apache.hop.neo4j.model;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class GraphProperty {

  @HopMetadataProperty protected String name;

  @HopMetadataProperty protected String description;

  @HopMetadataProperty protected GraphPropertyType type;

  @HopMetadataProperty protected boolean primary;

  @HopMetadataProperty protected boolean mandatory;

  @HopMetadataProperty protected boolean unique;

  @HopMetadataProperty protected boolean indexed;

  public GraphProperty() {}

  public GraphProperty(
      String name,
      String description,
      GraphPropertyType type,
      boolean primary,
      boolean mandatory,
      boolean unique,
      boolean indexed) {
    this.name = name;
    this.description = description;
    this.type = type;
    this.primary = primary;
    this.mandatory = mandatory;
    this.unique = unique;
    this.indexed = indexed;
  }

  public GraphProperty(GraphProperty property) {
    this(
        property.name,
        property.description,
        property.type,
        property.primary,
        property.mandatory,
        property.unique,
        property.indexed);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GraphProperty that = (GraphProperty) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
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
   * Gets type
   *
   * @return value of type
   */
  public GraphPropertyType getType() {
    return type;
  }

  /** @param type The type to set */
  public void setType(GraphPropertyType type) {
    this.type = type;
  }

  /**
   * Gets primary
   *
   * @return value of primary
   */
  public boolean isPrimary() {
    return primary;
  }

  /** @param primary The primary to set */
  public void setPrimary(boolean primary) {
    this.primary = primary;
  }

  /**
   * Gets mandatory
   *
   * @return value of mandatory
   */
  public boolean isMandatory() {
    return mandatory;
  }

  /** @param mandatory The mandatory to set */
  public void setMandatory(boolean mandatory) {
    this.mandatory = mandatory;
  }

  /**
   * Gets unique
   *
   * @return value of unique
   */
  public boolean isUnique() {
    return unique;
  }

  /** @param unique The unique to set */
  public void setUnique(boolean unique) {
    this.unique = unique;
  }

  /**
   * Gets indexed
   *
   * @return value of indexed
   */
  public boolean isIndexed() {
    return indexed;
  }

  /** @param indexed The indexed to set */
  public void setIndexed(boolean indexed) {
    this.indexed = indexed;
  }
}
