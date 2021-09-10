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

package org.apache.hop.beam.transforms.bq;

import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamBQInputTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Transform(
    id = "BeamBQInput",
    name = "Beam BigQuery Input",
    description = "Reads from a BigQuery table in Beam",
    image = "beam-bq-input.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/beambigqueryinput.html")
public class BeamBQInputMeta extends BaseTransformMeta<BeamBQInput, BeamBQInputData>
    implements IBeamPipelineTransformHandler {

  @HopMetadataProperty(key = "project_id")
  private String projectId;

  @HopMetadataProperty(key = "dataset_id")
  private String datasetId;

  @HopMetadataProperty(key = "table_id")
  private String tableId;

  @HopMetadataProperty(key = "query")
  private String query;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<BQField> fields;

  public BeamBQInputMeta() {
    super();
    fields = new ArrayList<>();
  }

  @Override
  public String getDialogClassName() {
    return BeamBQInputDialog.class.getName();
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

    try {
      for (BQField field : fields) {
        int type = ValueMetaFactory.getIdForValueMeta(field.getHopType());
        IValueMeta valueMeta =
            ValueMetaFactory.createValueMeta(field.getNewNameOrName(), type, -1, -1);
        valueMeta.setOrigin(name);
        inputRowMeta.addValueMeta(valueMeta);
      }
    } catch (Exception e) {
      throw new HopTransformException("Error getting Beam BQ Input transform output", e);
    }
  }

  @Override
  public boolean isInput() {
    return true;
  }

  @Override
  public boolean isOutput() {
    return false;
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
      org.apache.beam.sdk.Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input)
      throws HopException {

    // Output rows (fields selection)
    //
    IRowMeta outputRowMeta = new RowMeta();
    getFields(outputRowMeta, transformMeta.getName(), null, null, variables, null);

    BeamBQInputTransform beamInputTransform =
        new BeamBQInputTransform(
            transformMeta.getName(),
            transformMeta.getName(),
            variables.resolve(projectId),
            variables.resolve(datasetId),
            variables.resolve(tableId),
            variables.resolve(query),
            JsonRowMeta.toJson(outputRowMeta),
            transformPluginClasses,
            xpPluginClasses);
    PCollection<HopRow> afterInput = pipeline.apply(beamInputTransform);
    transformCollectionMap.put(transformMeta.getName(), afterInput);
    log.logBasic("Handled transform (BQ INPUT) : " + transformMeta.getName());
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
  public String getDatasetId() {
    return datasetId;
  }

  /** @param datasetId The datasetId to set */
  public void setDatasetId(String datasetId) {
    this.datasetId = datasetId;
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

  /**
   * Gets query
   *
   * @return value of query
   */
  public String getQuery() {
    return query;
  }

  /** @param query The query to set */
  public void setQuery(String query) {
    this.query = query;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<BQField> getFields() {
    return fields;
  }

  /** @param fields The fields to set */
  public void setFields(List<BQField> fields) {
    this.fields = fields;
  }
}
