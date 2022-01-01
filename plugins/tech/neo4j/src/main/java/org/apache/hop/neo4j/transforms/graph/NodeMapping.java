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

public class NodeMapping {

  /** The node mapping type */
  @HopMetadataProperty(
      key = "node_mapping_type",
      injectionKey = "node_mapping_type",
      injectionGroupDescription = "GraphOutput.Injection.NODE_MAPPING_TYPE",
      storeWithCode = true)
  private NodeMappingType type;

  /** The name (in the model) of the node to merge */
  @HopMetadataProperty(
      key = "node_name",
      injectionKey = "node_name",
      injectionGroupDescription = "GraphOutput.Injection.TARGET_NODE_NAME")
  private String targetNode;

  /** The Hop input field where the data is coming from */
  @HopMetadataProperty(
      key = "field_name",
      injectionKey = "field_name",
      injectionGroupDescription = "GraphOutput.Injection.FIELD_NAME")
  private String fieldName;

  /** The applicable input field value */
  @HopMetadataProperty(
      key = "field_value",
      injectionKey = "field_value",
      injectionGroupDescription = "GraphOutput.Injection.FIELD_VALUE")
  private String fieldValue;

  /** The label to select if the field has the specified value */
  @HopMetadataProperty(
      key = "target_label",
      injectionKey = "target_label",
      injectionGroupDescription = "GraphOutput.Injection.TARGET_NODE_LABEL")
  private String targetLabel;

  public NodeMapping() {
    type = NodeMappingType.All;
  }

  public NodeMapping(
      NodeMappingType type,
      String targetNode,
      String fieldName,
      String fieldValue,
      String targetLabel) {
    this.type = type;
    this.targetNode = targetNode;
    this.fieldName = fieldName;
    this.fieldValue = fieldValue;
    this.targetLabel = targetLabel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeMapping that = (NodeMapping) o;
    return type == that.type
        && Objects.equals(targetNode, that.targetNode)
        && Objects.equals(fieldName, that.fieldName)
        && Objects.equals(fieldValue, that.fieldValue)
        && Objects.equals(targetLabel, that.targetLabel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, targetNode, fieldName, fieldValue, targetLabel);
  }

  @Override
  public String toString() {
    return "NodeMapping{"
        + "type="
        + type
        + ", targetNode='"
        + targetNode
        + '\''
        + ", fieldName='"
        + fieldName
        + '\''
        + ", fieldValue='"
        + fieldValue
        + '\''
        + ", targetLabel='"
        + targetLabel
        + '\''
        + '}';
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public NodeMappingType getType() {
    return type;
  }

  /** @param type The type to set */
  public void setType(NodeMappingType type) {
    this.type = type;
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
   * Gets targetLabel
   *
   * @return value of targetLabel
   */
  public String getTargetLabel() {
    return targetLabel;
  }

  /** @param targetLabel The targetLabel to set */
  public void setTargetLabel(String targetLabel) {
    this.targetLabel = targetLabel;
  }
}
