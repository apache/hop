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

public class RelationshipMapping {

  /** The name (in the model) of the relationship to merge */
  @HopMetadataProperty(
      key = "relationship_mapping_type",
      injectionKey = "relationship_mapping_type",
      injectionGroupDescription = "GraphOutput.Injection.RELATIONSHIP_MAPPING_TYPE",
      storeWithCode = true)
  private RelationshipMappingType type;

  /** The name (in the model) of the relationship to merge */
  @HopMetadataProperty(
      key = "relationship_name",
      injectionKey = "relationship_name",
      injectionGroupDescription = "GraphOutput.Injection.TARGET_RELATIONSHIP_NAME")
  private String targetRelationship;

  /** The Hop input field where the data is coming from */
  @HopMetadataProperty(
      key = "field_name",
      injectionKey = "field_name",
      injectionGroupDescription = "GraphOutput.Injection.FIELD_NAME")
  private String fieldName;

  /** The Hop input field where the data is coming from */
  @HopMetadataProperty(
      key = "field_value",
      injectionKey = "field_value",
      injectionGroupDescription = "GraphOutput.Injection.FIELD_VALUE")
  private String fieldValue;

  /** The source node when for types None, All first */
  @HopMetadataProperty(
      key = "source_node",
      injectionKey = "source_node",
      injectionGroupDescription = "GraphOutput.Injection.RELATIONSHIP_MAPPING_SOURCE_NODE")
  private String sourceNode;

  /** The target node when for types None, All first */
  @HopMetadataProperty(
      key = "target_node",
      injectionKey = "target_node",
      injectionGroupDescription = "GraphOutput.Injection.RELATIONSHIP_MAPPING_TARGET_NODE")
  private String targetNode;

  public RelationshipMapping() {
    type = RelationshipMappingType.NoRelationship;
  }

  public RelationshipMapping(
      RelationshipMappingType type,
      String fieldName,
      String fieldValue,
      String targetRelationship,
      String sourceNode,
      String targetNode) {
    this.type = type;
    this.fieldName = fieldName;
    this.fieldValue = fieldValue;
    this.targetRelationship = targetRelationship;
    this.sourceNode = sourceNode;
    this.targetNode = targetNode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RelationshipMapping that = (RelationshipMapping) o;
    return type == that.type
        && Objects.equals(targetRelationship, that.targetRelationship)
        && Objects.equals(fieldName, that.fieldName)
        && Objects.equals(fieldValue, that.fieldValue)
        && Objects.equals(sourceNode, that.sourceNode)
        && Objects.equals(targetNode, that.targetNode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, targetRelationship, fieldName, fieldValue, sourceNode, targetNode);
  }

  @Override
  public String toString() {
    return "RelationshipMapping{"
        + "type="
        + type
        + ", targetRelationship='"
        + targetRelationship
        + '\''
        + ", fieldName='"
        + fieldName
        + '\''
        + ", fieldValue='"
        + fieldValue
        + '\''
        + ", sourceNode='"
        + sourceNode
        + '\''
        + ", targetNode='"
        + targetNode
        + '\''
        + '}';
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public RelationshipMappingType getType() {
    return type;
  }

  /** @param type The type to set */
  public void setType(RelationshipMappingType type) {
    this.type = type;
  }

  /**
   * Gets fieldName
   *
   * @return value of fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  /** @param fieldName The fieldName to set */
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * Gets fieldValue
   *
   * @return value of fieldValue
   */
  public String getFieldValue() {
    return fieldValue;
  }

  /** @param fieldValue The fieldValue to set */
  public void setFieldValue(String fieldValue) {
    this.fieldValue = fieldValue;
  }

  /**
   * Gets targetRelationship
   *
   * @return value of targetRelationship
   */
  public String getTargetRelationship() {
    return targetRelationship;
  }

  /** @param targetRelationship The targetRelationship to set */
  public void setTargetRelationship(String targetRelationship) {
    this.targetRelationship = targetRelationship;
  }

  /**
   * Gets sourceNode
   *
   * @return value of sourceNode
   */
  public String getSourceNode() {
    return sourceNode;
  }

  /** @param sourceNode The sourceNode to set */
  public void setSourceNode(String sourceNode) {
    this.sourceNode = sourceNode;
  }

  /**
   * Gets targetNode
   *
   * @return value of targetNode
   */
  public String getTargetNode() {
    return targetNode;
  }

  /** @param targetNode The targetNode to set */
  public void setTargetNode(String targetNode) {
    this.targetNode = targetNode;
  }
}
