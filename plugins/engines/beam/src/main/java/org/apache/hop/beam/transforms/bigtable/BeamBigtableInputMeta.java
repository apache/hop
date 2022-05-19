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

package org.apache.hop.beam.transforms.bigtable;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.Dummy;
import org.apache.hop.pipeline.transforms.dummy.DummyData;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.*;

@Transform(
    id = "BeamBigtableInput",
    name = "i18n::BeamBigtableInput.Name",
    description = "i18n::BeamBigtableInput.Description",
    image = "beam-gcp-bigtable-input.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamBigtableInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/beambigtableinput.html")
public class BeamBigtableInputMeta extends BaseTransformMeta<Dummy, DummyData>
    implements IBeamPipelineTransformHandler {

  @HopMetadataProperty(key = "project_id")
  private String projectId;

  @HopMetadataProperty(key = "instance_id")
  private String instanceId;

  @HopMetadataProperty(key = "table_id")
  private String tableId;

  @HopMetadataProperty(key = "key_field")
  private String keyField;

  @HopMetadataProperty(groupKey = "columns", key = "column")
  private List<BigtableSourceColumn> sourceColumns;

  public BeamBigtableInputMeta() {
    keyField = "key";
    sourceColumns = new ArrayList<>();
  }

  @Override
  public String getDialogClassName() {
    return BeamBigtableInputDialog.class.getName();
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    String keyFieldName = variables.resolve(keyField);
    if (StringUtils.isNotEmpty(keyFieldName)) {
      inputRowMeta.addValueMeta(new ValueMetaString(variables.resolve(keyField)));
    }

    for (BigtableSourceColumn sourceColumn : sourceColumns) {
      try {
        inputRowMeta.addValueMeta(sourceColumn.getValueMeta());
      } catch (Exception e) {
        throw new HopTransformException(
            "Error creating value metadata for Bigtable source column "
                + sourceColumn.getQualifier(),
            e);
      }
    }
  }

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      IBeamPipelineEngineRunConfiguration runConfiguration,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      List<String> transformPluginClasses,
      List<String> xpPluginClasses,
      TransformMeta transformMeta,
      Map<String, PCollection<HopRow>> transformCollectionMap,
      Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input)
      throws HopException {

    JSONArray j = new JSONArray();
    for (BigtableSourceColumn column : sourceColumns) {
      JSONObject jc = new JSONObject();
      jc.put("qualifier", variables.resolve(column.getQualifier()));
      jc.put("target_type", variables.resolve(column.getTargetType()));
      jc.put("target_field_name", variables.resolve(column.getTargetFieldName()));
      j.add(jc);
    }

    PCollection<com.google.bigtable.v2.Row> rowPCollection =
        pipeline
            .begin()
            .apply(
                transformMeta.getName(),
                BigtableIO.read()
                    .withProjectId(variables.resolve(projectId))
                    .withInstanceId(variables.resolve(instanceId))
                    .withTableId(variables.resolve(tableId)));

    BigtableRowToHopRowFn fn =
        new BigtableRowToHopRowFn(
            transformMeta.getName(),
            JsonRowMeta.toJson(rowMeta),
            variables.resolve(keyField),
            j.toJSONString(),
            transformPluginClasses,
            xpPluginClasses);

    PCollection<HopRow> output = rowPCollection.apply(ParDo.of(fn));
    transformCollectionMap.put(transformMeta.getName(), output);
    log.logBasic("Handled transform (Bigtable INPUT) : " + transformMeta.getName());
  }

  @Override
  public boolean isInput() {
    return true;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  /**
   * Gets projectId
   *
   * @return value of projectId
   */
  public String getProjectId() {
    return projectId;
  }

  /** @param projectId The projectId to set */
  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  /**
   * Gets datasetId
   *
   * @return value of datasetId
   */
  public String getInstanceId() {
    return instanceId;
  }

  /** @param instanceId The datasetId to set */
  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  /**
   * Gets tableId
   *
   * @return value of tableId
   */
  public String getTableId() {
    return tableId;
  }

  /** @param tableId The tableId to set */
  public void setTableId(String tableId) {
    this.tableId = tableId;
  }

  public String getKeyField() {
    return keyField;
  }

  public void setKeyField(String keyField) {
    this.keyField = keyField;
  }

  public List<BigtableSourceColumn> getSourceColumns() {
    return sourceColumns;
  }

  public void setSourceColumns(List<BigtableSourceColumn> sourceColumns) {
    this.sourceColumns = sourceColumns;
  }
}
