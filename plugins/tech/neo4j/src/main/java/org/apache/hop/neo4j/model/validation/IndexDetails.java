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

package org.apache.hop.neo4j.model.validation;

import org.neo4j.driver.Record;

import java.util.List;

public class IndexDetails {
  private int id;
  private String name;
  private String state;
  private float populationPercent;
  private String uniqueness;
  private String type;
  private String entityType;
  private List<String> labelsOrTypes;
  private List<String> properties;
  private String provider;

  public IndexDetails() {}

  public IndexDetails(Record record) {
    this.id = record.get("id").asInt();
    this.name = record.get("name").asString();
    this.state = record.get("state").asString();
    this.populationPercent = record.get("populationPercent").asFloat();
    this.uniqueness = record.get("uniqueness").asString();
    this.type = record.get("type").asString();
    this.entityType = record.get("entityType").asString();
    this.labelsOrTypes = record.get("labelsOrTypes").asList(value -> value.asString());
    this.properties = record.get("properties").asList(value -> value.asString());
    this.provider = record.get("provider").asString();
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public int getId() {
    return id;
  }

  /** @param id The id to set */
  public void setId(int id) {
    this.id = id;
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
   * Gets state
   *
   * @return value of state
   */
  public String getState() {
    return state;
  }

  /** @param state The state to set */
  public void setState(String state) {
    this.state = state;
  }

  /**
   * Gets populationPercent
   *
   * @return value of populationPercent
   */
  public float getPopulationPercent() {
    return populationPercent;
  }

  /** @param populationPercent The populationPercent to set */
  public void setPopulationPercent(float populationPercent) {
    this.populationPercent = populationPercent;
  }

  /**
   * Gets uniqueness
   *
   * @return value of uniqueness
   */
  public String getUniqueness() {
    return uniqueness;
  }

  /** @param uniqueness The uniqueness to set */
  public void setUniqueness(String uniqueness) {
    this.uniqueness = uniqueness;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public String getType() {
    return type;
  }

  /** @param type The type to set */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * Gets entityType
   *
   * @return value of entityType
   */
  public String getEntityType() {
    return entityType;
  }

  /** @param entityType The entityType to set */
  public void setEntityType(String entityType) {
    this.entityType = entityType;
  }

  /**
   * Gets labelsOrTypes
   *
   * @return value of labelsOrTypes
   */
  public List<String> getLabelsOrTypes() {
    return labelsOrTypes;
  }

  /** @param labelsOrTypes The labelsOrTypes to set */
  public void setLabelsOrTypes(List<String> labelsOrTypes) {
    this.labelsOrTypes = labelsOrTypes;
  }

  /**
   * Gets properties
   *
   * @return value of properties
   */
  public List<String> getProperties() {
    return properties;
  }

  /** @param properties The properties to set */
  public void setProperties(List<String> properties) {
    this.properties = properties;
  }

  /**
   * Gets provider
   *
   * @return value of provider
   */
  public String getProvider() {
    return provider;
  }

  /** @param provider The provider to set */
  public void setProvider(String provider) {
    this.provider = provider;
  }
}
