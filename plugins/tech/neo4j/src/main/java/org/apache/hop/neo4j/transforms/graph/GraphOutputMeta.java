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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.neo4j.core.value.ValueMetaGraph;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "Neo4jGraphOutput",
    name = "i18n::GraphOutput.Name",
    description = "i18n::GraphOutput.Description",
    image = "neo4j_graph_output.svg",
    categoryDescription = "Neo4j",
    keywords = "i18n::GraphOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/neo4j-graphoutput.html")
public class GraphOutputMeta extends BaseTransformMeta<GraphOutput, GraphOutputData> {

  @HopMetadataProperty(
      key = "connection",
      injectionKey = "connection",
      injectionKeyDescription = "GraphOutput.Injection.CONNECTION",
      hopMetadataPropertyType = HopMetadataPropertyType.GRAPH_CONNECTION)
  private String connectionName;

  @HopMetadataProperty(
      key = "model",
      injectionKey = "model",
      injectionKeyDescription = "GraphOutput.Injection.MODEL")
  private String model;

  @HopMetadataProperty(
      key = "batch_size",
      injectionKey = "batch_size",
      injectionKeyDescription = "GraphOutput.Injection.BATCH_SIZE")
  private String batchSize;

  @HopMetadataProperty(
      key = "create_indexes",
      injectionKey = "create_indexes",
      injectionKeyDescription = "GraphOutput.Injection.CREATE_INDEXES")
  private boolean creatingIndexes;

  @HopMetadataProperty(
      key = "returning_graph",
      injectionKey = "returning_graph",
      injectionKeyDescription = "GraphOutput.Injection.RETURNING_GRAPH")
  private boolean returningGraph;

  @HopMetadataProperty(
      key = "return_graph_field",
      injectionKey = "return_graph_field",
      injectionKeyDescription = "GraphOutput.Injection.RETURNING_GRAPH_FIELD")
  private String returnGraphField;

  @HopMetadataProperty(
      key = "validate_against_model",
      injectionKey = "validate_against_model",
      injectionKeyDescription = "GraphOutput.Injection.VALIDATE_AGAINST_MODEL")
  private boolean validatingAgainstModel;

  @HopMetadataProperty(
      key = "out_of_order_allowed",
      injectionKey = "out_of_order_allowed",
      injectionKeyDescription = "GraphOutput.Injection.OUT_OF_ORDER_ALLOWED")
  private boolean outOfOrderAllowed;

  @HopMetadataProperty(
      groupKey = "mappings",
      key = "mapping",
      injectionGroupKey = "mappings",
      injectionKey = "mapping",
      injectionGroupDescription = "GraphOutput.Injection.MAPPINGS",
      injectionKeyDescription = "GraphOutput.Injection.MAPPING")
  private List<FieldModelMapping> fieldModelMappings;

  @HopMetadataProperty(
      groupKey = "relationship_mappings",
      key = "relationship_mapping",
      injectionGroupKey = "relationship_mappings",
      injectionGroupDescription = "GraphOutput.Injection.RELATIONSHIP_MAPPINGS",
      injectionKeyDescription = "GraphOutput.Injection.RELATIONSHIP_MAPPING")
  private List<RelationshipMapping> relationshipMappings;

  @HopMetadataProperty(
      groupKey = "node_mappings",
      key = "node_mapping",
      injectionGroupKey = "node_mappings",
      injectionGroupDescription = "GraphOutput.Injection.NODE_MAPPINGS",
      injectionKeyDescription = "GraphOutput.Injection.NODE_MAPPING")
  private List<NodeMapping> nodeMappings;

  public GraphOutputMeta() {
    super();
    fieldModelMappings = new ArrayList<>();
    relationshipMappings = new ArrayList<>();
    nodeMappings = new ArrayList<>();
    creatingIndexes = false;
    outOfOrderAllowed = true;
  }

  @Override
  public String getDialogClassName() {
    return GraphOutputDialog.class.getName();
  }

  @Override
  public void setDefault() {
    batchSize = "1000";
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextStep,
      IVariables space,
      IHopMetadataProvider metadataProvider) {

    if (returningGraph) {
      IValueMeta valueMetaGraph = new ValueMetaGraph(Const.NVL(returnGraphField, "graph"));
      valueMetaGraph.setOrigin(name);
      rowMeta.addValueMeta(valueMetaGraph);
    }
  }

  /**
   * Gets connectionName
   *
   * @return value of connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /**
   * @param connectionName The connectionName to set
   */
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  /**
   * Gets model
   *
   * @return value of model
   */
  public String getModel() {
    return model;
  }

  /**
   * @param model The model to set
   */
  public void setModel(String model) {
    this.model = model;
  }

  /**
   * Gets batchSize
   *
   * @return value of batchSize
   */
  public String getBatchSize() {
    return batchSize;
  }

  /**
   * @param batchSize The batchSize to set
   */
  public void setBatchSize(String batchSize) {
    this.batchSize = batchSize;
  }

  /**
   * Gets creatingIndexes
   *
   * @return value of creatingIndexes
   */
  public boolean isCreatingIndexes() {
    return creatingIndexes;
  }

  /**
   * @param creatingIndexes The creatingIndexes to set
   */
  public void setCreatingIndexes(boolean creatingIndexes) {
    this.creatingIndexes = creatingIndexes;
  }

  /**
   * Gets fieldModelMappings
   *
   * @return value of fieldModelMappings
   */
  public List<FieldModelMapping> getFieldModelMappings() {
    return fieldModelMappings;
  }

  /**
   * @param fieldModelMappings The fieldModelMappings to set
   */
  public void setFieldModelMappings(List<FieldModelMapping> fieldModelMappings) {
    this.fieldModelMappings = fieldModelMappings;
  }

  /**
   * Gets returningGraph
   *
   * @return value of returningGraph
   */
  public boolean isReturningGraph() {
    return returningGraph;
  }

  /**
   * @param returningGraph The returningGraph to set
   */
  public void setReturningGraph(boolean returningGraph) {
    this.returningGraph = returningGraph;
  }

  /**
   * Gets returnGraphField
   *
   * @return value of returnGraphField
   */
  public String getReturnGraphField() {
    return returnGraphField;
  }

  /**
   * @param returnGraphField The returnGraphField to set
   */
  public void setReturnGraphField(String returnGraphField) {
    this.returnGraphField = returnGraphField;
  }

  /**
   * Gets validatingAgainstModel
   *
   * @return value of validatingAgainstModel
   */
  public boolean isValidatingAgainstModel() {
    return validatingAgainstModel;
  }

  /**
   * @param validatingAgainstModel The validatingAgainstModel to set
   */
  public void setValidatingAgainstModel(boolean validatingAgainstModel) {
    this.validatingAgainstModel = validatingAgainstModel;
  }

  /**
   * Gets outOfOrderAllowed
   *
   * @return value of outOfOrderAllowed
   */
  public boolean isOutOfOrderAllowed() {
    return outOfOrderAllowed;
  }

  /**
   * @param outOfOrderAllowed The outOfOrderAllowed to set
   */
  public void setOutOfOrderAllowed(boolean outOfOrderAllowed) {
    this.outOfOrderAllowed = outOfOrderAllowed;
  }

  /**
   * Gets relationshipMappings
   *
   * @return value of relationshipMappings
   */
  public List<RelationshipMapping> getRelationshipMappings() {
    return relationshipMappings;
  }

  /**
   * @param relationshipMappings The relationshipMappings to set
   */
  public void setRelationshipMappings(List<RelationshipMapping> relationshipMappings) {
    this.relationshipMappings = relationshipMappings;
  }

  /**
   * Gets nodeMappings
   *
   * @return value of nodeMappings
   */
  public List<NodeMapping> getNodeMappings() {
    return nodeMappings;
  }

  /**
   * @param nodeMappings The nodeMappings to set
   */
  public void setNodeMappings(List<NodeMapping> nodeMappings) {
    this.nodeMappings = nodeMappings;
  }
}
