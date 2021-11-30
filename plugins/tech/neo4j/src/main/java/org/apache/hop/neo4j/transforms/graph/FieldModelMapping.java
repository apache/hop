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

package org.apache.hop.neo4j.transforms.graph;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class FieldModelMapping {

  /** The Hop input field where the data is coming from */
  @HopMetadataProperty(
      key = "source_field",
      injectionKey = "MAPPING_SOURCE_FIELD",
      injectionKeyDescription = "GraphOutput.Injection.MAPPING_SOURCE_FIELD")
  private String field;

  /** Write to a node or a relationship */
  @HopMetadataProperty(
      key = "target_type",
      injectionKey = "MAPPING_TARGET_TYPE",
      injectionKeyDescription = "GraphOutput.Injection.MAPPING_TARGET_TYPE")
  private ModelTargetType targetType;

  /** Name of the node or relationship to write to */
  @HopMetadataProperty(
      key = "target_name",
      injectionKey = "MAPPING_TARGET_NAME",
      injectionKeyDescription = "GraphOutput.Injection.MAPPING_TARGET_NAME")
  private String targetName;

  /** Name of the property to write to */
  @HopMetadataProperty(
      key = "target_property",
      injectionKey = "MAPPING_TARGET_PROPERTY",
      injectionKeyDescription = "GraphOutput.Injection.MAPPING_TARGET_PROPERTY")
  private String targetProperty;

  /** Write to a node or a relationship */
  @HopMetadataProperty(
      key = "target_hint",
      injectionKey = "MAPPING_TARGET_HINT",
      injectionKeyDescription = "GraphOutput.Injection.MAPPING_TARGET_HINT")
  private ModelTargetHint targetHint;

  public FieldModelMapping() {
    targetType = ModelTargetType.Node;
    targetHint = ModelTargetHint.None;
  }

  public FieldModelMapping(
      String field,
      ModelTargetType targetType,
      String targetName,
      String targetProperty,
      ModelTargetHint targetHint) {
    this.field = field;
    this.targetType = targetType;
    this.targetName = targetName;
    this.targetProperty = targetProperty;
    this.targetHint = targetHint;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldModelMapping that = (FieldModelMapping) o;
    return Objects.equals(field, that.field)
        && targetType == that.targetType
        && Objects.equals(targetName, that.targetName)
        && Objects.equals(targetProperty, that.targetProperty)
        && targetHint == that.targetHint;
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, targetType, targetName, targetProperty, targetHint);
  }

  /**
   * Gets field
   *
   * @return value of field
   */
  public String getField() {
    return field;
  }

  /** @param field The field to set */
  public void setField(String field) {
    this.field = field;
  }

  /**
   * Gets targetType
   *
   * @return value of targetType
   */
  public ModelTargetType getTargetType() {
    return targetType;
  }

  /** @param targetType The targetType to set */
  public void setTargetType(ModelTargetType targetType) {
    this.targetType = targetType;
  }

  /**
   * Gets targetName
   *
   * @return value of targetName
   */
  public String getTargetName() {
    return targetName;
  }

  /** @param targetName The targetName to set */
  public void setTargetName(String targetName) {
    this.targetName = targetName;
  }

  /**
   * Gets targetProperty
   *
   * @return value of targetProperty
   */
  public String getTargetProperty() {
    return targetProperty;
  }

  /** @param targetProperty The targetProperty to set */
  public void setTargetProperty(String targetProperty) {
    this.targetProperty = targetProperty;
  }

  /**
   * Gets targetHint
   *
   * @return value of targetHint
   */
  public ModelTargetHint getTargetHint() {
    return targetHint;
  }

  /** @param targetHint The targetHint to set */
  public void setTargetHint(ModelTargetHint targetHint) {
    this.targetHint = targetHint;
  }
}
