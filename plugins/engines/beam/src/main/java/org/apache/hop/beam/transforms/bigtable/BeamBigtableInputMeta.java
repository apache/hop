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

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
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

@Transform(
    id = "BeamBigtableInput",
    name = "i18n::BeamBigtableInput.Name",
    description = "i18n::BeamBigtableInput.Description",
    image = "beam-gcp-bigtable-input.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
        keywords = "i18n::BeamBigtableInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/beambigtableinput.html")
public class BeamBigtableInputMeta extends BaseTransformMeta
    implements ITransformMeta<Dummy, DummyData> {

  @HopMetadataProperty(key = "project_id")
  private String projectId;

  @HopMetadataProperty(key = "instance_id")
  private String instanceId;

  @HopMetadataProperty(key = "table_id")
  private String tableId;

  public BeamBigtableInputMeta() {
    super();
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
      throws HopTransformException {}

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
}
