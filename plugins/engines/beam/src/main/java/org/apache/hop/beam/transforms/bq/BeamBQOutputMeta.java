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
import org.apache.hop.beam.core.transform.BeamBQOutputTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.Dummy;
import org.apache.hop.pipeline.transforms.dummy.DummyData;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;

import java.util.List;
import java.util.Map;

@Transform(
    id = "BeamBQOutput",
    image = "beam-bq-output.svg",
    name = "Beam BigQuery Output",
    description = "Writes to a BigQuery table in Beam",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.BigData",
    documentationUrl = "/pipeline/transforms/beambigqueryoutput.html",
    keywords = "i18n::BeamBQOutputDialog.keyword")
public class BeamBQOutputMeta extends BaseTransformMeta
    implements ITransformMeta<Dummy, DummyData>, IBeamPipelineTransformHandler {

  @HopMetadataProperty(key = "project_id")
  private String projectId;

  @HopMetadataProperty(key = "dataset_id")
  private String datasetId;

  @HopMetadataProperty(key = "table_id")
  private String tableId;

  @HopMetadataProperty(key = "create_if_needed")
  private boolean creatingIfNeeded;

  @HopMetadataProperty(key = "truncate_table")
  private boolean truncatingTable;

  @HopMetadataProperty(key = "fail_if_not_empty")
  private boolean failingIfNotEmpty;

  @Override
  public void setDefault() {
    creatingIfNeeded = true;
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

    // This is an endpoint in Beam, produces no further output
    //
    inputRowMeta.clear();
  }

  @Override
  public Dummy createTransform(
      TransformMeta transformMeta,
      DummyData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new Dummy(transformMeta, new DummyMeta(), data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public DummyData getTransformData() {
    return new DummyData();
  }

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public boolean isOutput() {
    return true;
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

    BeamBQOutputTransform beamOutputTransform =
        new BeamBQOutputTransform(
            transformMeta.getName(),
            variables.resolve(projectId),
            variables.resolve(datasetId),
            variables.resolve(tableId),
            creatingIfNeeded,
            truncatingTable,
            failingIfNotEmpty,
            JsonRowMeta.toJson(rowMeta),
            transformPluginClasses,
            xpPluginClasses);

    // Which transform do we apply this transform to?
    // Ignore info hops until we figure that out.
    //
    if (previousTransforms.size() > 1) {
      throw new HopException("Combining data from multiple transforms is not supported yet!");
    }
    TransformMeta previousTransform = previousTransforms.get(0);

    // No need to store this, it's PDone.
    //
    input.apply(beamOutputTransform);
    log.logBasic(
        "Handled transform (BQ OUTPUT) : "
            + transformMeta.getName()
            + ", gets data from "
            + previousTransform.getName());
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
   * Gets creatingIfNeeded
   *
   * @return value of creatingIfNeeded
   */
  public boolean isCreatingIfNeeded() {
    return creatingIfNeeded;
  }

  /** @param creatingIfNeeded The creatingIfNeeded to set */
  public void setCreatingIfNeeded(boolean creatingIfNeeded) {
    this.creatingIfNeeded = creatingIfNeeded;
  }

  /**
   * Gets truncatingTable
   *
   * @return value of truncatingTable
   */
  public boolean isTruncatingTable() {
    return truncatingTable;
  }

  /** @param truncatingTable The truncatingTable to set */
  public void setTruncatingTable(boolean truncatingTable) {
    this.truncatingTable = truncatingTable;
  }

  /**
   * Gets failingIfNotEmpty
   *
   * @return value of failingIfNotEmpty
   */
  public boolean isFailingIfNotEmpty() {
    return failingIfNotEmpty;
  }

  /** @param failingIfNotEmpty The failingIfNotEmpty to set */
  public void setFailingIfNotEmpty(boolean failingIfNotEmpty) {
    this.failingIfNotEmpty = failingIfNotEmpty;
  }
}
