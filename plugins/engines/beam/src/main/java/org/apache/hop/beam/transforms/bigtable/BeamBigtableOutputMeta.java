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

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
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

@Transform(
    id = "BeamBigtableOutput",
    name = "i18n::BeamBigtableOutput.Name",
    description = "i18n::BeamBigtableOutput.Description",
    image = "beam-gcp-bigtable-output.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamBigtableOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/beambigtableoutput.html")
public class BeamBigtableOutputMeta extends BaseTransformMeta<Dummy, DummyData>
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
  private List<BigtableColumn> columns;

  public BeamBigtableOutputMeta() {
    columns = new ArrayList<>();
  }

  public BeamBigtableOutputMeta(BeamBigtableOutputMeta m) {
    this();
    this.projectId = m.projectId;
    this.instanceId = m.instanceId;
    this.tableId = m.tableId;
    this.keyField = m.keyField;
    for (BigtableColumn column : this.columns) {
      this.columns.add(new BigtableColumn(column));
    }
  }

  @Override
  public BeamBigtableOutputMeta clone() {
    return new BeamBigtableOutputMeta(this);
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
      String runConfigurationName,
      IBeamPipelineEngineRunConfiguration runConfiguration,
      String dataSamplersJson,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, PCollection<HopRow>> transformCollectionMap,
      org.apache.beam.sdk.Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input,
      String parentLogChannelId)
      throws HopException {

    // Which transform do we apply this transform to?
    // Ignore info hops until we figure that out.
    //
    if (previousTransforms.size() > 1) {
      throw new HopException("Combining data from multiple transforms is not supported yet!");
    }
    TransformMeta previousTransform = previousTransforms.get(0);

    // Verify that each column has qualifier, a family value and a source:
    //
    for (BigtableColumn column : columns) {
      if (StringUtils.isEmpty(column.getName())) {
        throw new HopException("Every column needs to have a name");
      }
      if (StringUtils.isEmpty(column.getFamily())) {
        throw new HopException("Every column needs to have a family value: " + column.getName());
      }
      if (StringUtils.isEmpty(column.getSourceField())) {
        throw new HopException("Every column needs to have a source field: " + column.getName());
      }
    }

    BigtableIO.Write write =
        BigtableIO.write()
            .withProjectId(variables.resolve(projectId))
            .withInstanceId(variables.resolve(instanceId))
            .withTableId(variables.resolve(tableId));

    String realKeyField = variables.resolve(keyField);
    int keyIndex = rowMeta.indexOfValue(realKeyField);
    if (keyIndex < 0) {
      throw new HopException("Key field " + realKeyField + " could not be found in the input");
    }

    // Encode the columns to JSON...
    //
    JSONArray j = new JSONArray();
    for (BigtableColumn column : columns) {
      JSONObject jc = new JSONObject();
      jc.put("qualifier", variables.resolve(column.getName()));
      jc.put("family", variables.resolve(column.getFamily()));
      jc.put("field", variables.resolve(column.getSourceField()));
      j.add(jc);
    }

    HopToBigtableFn function =
        new HopToBigtableFn(
            keyIndex, j.toJSONString(), transformMeta.getName(), JsonRowMeta.toJson(rowMeta));

    PCollection<KV<ByteString, Iterable<Mutation>>> bigtableInput =
        input.apply(transformMeta.getName(), ParDo.of(function));
    write.expand(bigtableInput);

    log.logBasic(
        "Handled transform (Bigtable OUTPUT) : "
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

  /**
   * @param projectId The projectId to set
   */
  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  /**
   * Gets instanceId
   *
   * @return value of instanceId
   */
  public String getInstanceId() {
    return instanceId;
  }

  /**
   * @param instanceId The instanceId to set
   */
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

  /**
   * @param tableId The tableId to set
   */
  public void setTableId(String tableId) {
    this.tableId = tableId;
  }

  /**
   * Gets keyField
   *
   * @return value of keyField
   */
  public String getKeyField() {
    return keyField;
  }

  /**
   * @param keyField The keyField to set
   */
  public void setKeyField(String keyField) {
    this.keyField = keyField;
  }

  /**
   * Gets columns
   *
   * @return value of columns
   */
  public List<BigtableColumn> getColumns() {
    return columns;
  }

  /**
   * @param columns The columns to set
   */
  public void setColumns(List<BigtableColumn> columns) {
    this.columns = columns;
  }
}
