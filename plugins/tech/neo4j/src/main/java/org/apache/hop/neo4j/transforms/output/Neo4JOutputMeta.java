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

package org.apache.hop.neo4j.transforms.output;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.neo4j.core.value.ValueMetaGraph;
import org.apache.hop.neo4j.transforms.output.fields.LabelField;
import org.apache.hop.neo4j.transforms.output.fields.NodeFromField;
import org.apache.hop.neo4j.transforms.output.fields.NodeToField;
import org.apache.hop.neo4j.transforms.output.fields.PropertyField;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "Neo4JOutput",
    image = "neo4j_output.svg",
    name = "i18n::Neo4JOutput.Transform.Name",
    description = "i18n::Neo4JOutput.Transform.Description",
    categoryDescription = "i18n::Neo4JOutput.Transform.Category",
    keywords = "i18n::Neo4JOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/neo4j-output.html",
    actionTransformTypes = {ActionTransformType.OUTPUT, ActionTransformType.GRAPH})
@Getter
@Setter
public class Neo4JOutputMeta extends BaseTransformMeta<Neo4JOutput, Neo4JOutputData> {

  @HopMetadataProperty(key = "connection", injectionKey = "CONNECTION")
  private String connection;

  @HopMetadataProperty(key = "batch_size", injectionKey = "BATCH_SIZE")
  private String batchSize;

  @HopMetadataProperty(key = "create_indexes", injectionKey = "CREATE_INDEXES")
  private boolean creatingIndexes;

  @HopMetadataProperty(key = "use_create", injectionKey = "USE_CREATE")
  private boolean usingCreate;

  @HopMetadataProperty(
      key = "only_create_relationships",
      injectionKey = "ONLY_CREATE_RELATIONSHIPS")
  private boolean onlyCreatingRelationships;

  @HopMetadataProperty(
      key = "relprop",
      injectionKey = "",
      groupKey = "relprops",
      injectionGroupKey = "REL_PROPS")
  private List<PropertyField> relProps;

  @HopMetadataProperty(key = "returning_graph", injectionKey = "RETURNING_GRAPH")
  private boolean returningGraph;

  @HopMetadataProperty(key = "return_graph_field", injectionKey = "RETURNING_GRAPH_FIELD")
  private String returnGraphField;

  @HopMetadataProperty(key = "from", injectionKey = "")
  private NodeFromField nodeFromField;

  @HopMetadataProperty(key = "to", injectionKey = "")
  private NodeToField nodeToField;

  @HopMetadataProperty(key = "relationship", injectionKey = "RELATIONSHIP")
  private String relationship;

  @HopMetadataProperty(key = "relationship_value", injectionKey = "RELATIONSHIP_VALUE")
  private String relationshipValue;

  @Override
  public void setDefault() {
    connection = "";
    batchSize = "1000";
    creatingIndexes = true;
    usingCreate = false;
    onlyCreatingRelationships = false;

    returnGraphField = "graph";
    nodeFromField = new NodeFromField();
    nodeToField = new NodeToField();
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables space,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;
    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              "Not receiving any fields from previous transform!",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "Transform is connected to previous one, receiving " + prev.size() + " fields",
              transformMeta);
      remarks.add(cr);
    }

    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "Transform is receiving info from other transform.",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "No input received from other transform!",
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextStep,
      IVariables space,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (returningGraph) {

      IValueMeta valueMetaGraph = new ValueMetaGraph(Const.NVL(returnGraphField, "graph"));
      valueMetaGraph.setOrigin(name);
      inputRowMeta.addValueMeta(valueMetaGraph);
    }
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  public boolean dynamicFromLabels() {
    return dynamicLabels(nodeFromField.getLabels());
  }

  public boolean dynamicToLabels() {
    return dynamicLabels(nodeToField.getLabels());
  }

  protected boolean dynamicLabels(List<LabelField> fieldList) {
    for (LabelField labelField : fieldList) {
      if (StringUtils.isNotEmpty(labelField.getLabel())) {
        return true;
      }
    }
    return false;
  }

  public boolean isCreatingRelationships() {
    return StringUtils.isNotEmpty(relationshipValue) || StringUtils.isNotEmpty(relationship);
  }
}
