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

package org.apache.hop.beam.transforms.kinesis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyData;

@Transform(
    id = "BeamKinesisProduce",
    name = "i18n::BeamKinesisProduceMeta.name",
    description = "i18n::BeamKinesisProduceMeta.description",
    image = "beam-kinesis-produce.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamKinesisProduceMeta.keyword",
    documentationUrl = "/pipeline/transforms/beamkinesisproduce.html")
public class BeamKinesisProduceMeta extends BaseTransformMeta<BeamKinesisProduce, DummyData>
    implements IBeamPipelineTransformHandler {

  @HopMetadataProperty(key = "access_key")
  private String accessKey;

  @HopMetadataProperty(key = "secret_key", password = true)
  private String secretKey;

  @HopMetadataProperty(key = "stream_name", password = true)
  private String streamName;

  @HopMetadataProperty(key = "data_field")
  private String dataField;

  @HopMetadataProperty(key = "message_type")
  private String dataType;

  @HopMetadataProperty(key = "partition_key_field")
  private String partitionKey;

  @HopMetadataProperty(groupKey = "config_options", key = "config_option")
  private List<KinesisConfigOption> configOptions;

  public BeamKinesisProduceMeta() {
    streamName = "stream-name";
    configOptions = new ArrayList<>();
  }

  @Override
  public String getDialogClassName() {
    return BeamKinesisProduceDialog.class.getName();
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

    // No output
    //
    inputRowMeta.clear();
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

    String[] parameters = new String[getConfigOptions().size()];
    String[] values = new String[getConfigOptions().size()];
    for (int i = 0; i < parameters.length; i++) {
      KinesisConfigOption option = getConfigOptions().get(i);
      parameters[i] = variables.resolve(option.getParameter());
      values[i] = variables.resolve(option.getValue());
    }

    String dataFieldName = variables.resolve(getDataField());
    IValueMeta messageValueMeta = rowMeta.searchValueMeta(dataFieldName);
    if (messageValueMeta == null) {
      throw new HopException(
          "Error finding message/data field " + dataFieldName + " in the input rows");
    }

    BeamKinesisProduceTransform beamProduceTransform =
        new BeamKinesisProduceTransform(
            transformMeta.getName(),
            JsonRowMeta.toJson(rowMeta),
            variables.resolve(accessKey),
            variables.resolve(secretKey),
            "us-east-1",
            variables.resolve(streamName),
            dataFieldName,
            variables.resolve(dataType),
            variables.resolve(partitionKey),
            parameters,
            values);

    // Which transform do we apply this transform to?
    // Ignore info hops until we figure that out.
    //
    if (previousTransforms.size() > 1) {
      throw new HopException("Combining data from multiple transforms is not supported yet!");
    }
    TransformMeta previousTransform = previousTransforms.get(0);

    // No need to store this, it's PDone.
    //
    input.apply(beamProduceTransform);
    log.logBasic(
        "Handled transform (KINESIS OUTPUT) : "
            + transformMeta.getName()
            + ", gets data from "
            + previousTransform.getName());
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  public String getStreamName() {
    return streamName;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public String getDataField() {
    return dataField;
  }

  public void setDataField(String dataField) {
    this.dataField = dataField;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public String getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(String partitionKey) {
    this.partitionKey = partitionKey;
  }

  public List<KinesisConfigOption> getConfigOptions() {
    return configOptions;
  }

  public void setConfigOptions(List<KinesisConfigOption> configOptions) {
    this.configOptions = configOptions;
  }
}
