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

package org.apache.hop.beam.transforms.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamKafkaOutputTransform;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyData;

@Transform(
    id = "BeamKafkaProduce",
    name = "Beam Kafka Produce",
    description = "Send messages to a Kafka Topic (Producer)",
    image = "beam-kafka-output.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamProduceMeta.keyword",
    documentationUrl = "/pipeline/transforms/beamkafkaproduce.html")
public class BeamProduceMeta extends BaseTransformMeta<BeamProduce, DummyData>
    implements IBeamPipelineTransformHandler {

  @HopMetadataProperty(key = "bootstrap_servers")
  private String bootstrapServers;

  @HopMetadataProperty private String topic;

  @HopMetadataProperty(key = "key_field")
  private String keyField;

  @HopMetadataProperty(key = "message_field")
  private String messageField;

  @HopMetadataProperty(groupKey = "config_options", key = "config_option")
  private List<ConfigOption> configOptions;

  public BeamProduceMeta() {
    bootstrapServers = "bootstrapServer1:9001,bootstrapServer2:9001";
    topic = "Topic1";
    keyField = "";
    messageField = "";
    configOptions = new ArrayList<>();
  }

  @Override
  public String getDialogClassName() {
    return BeamProduceDialog.class.getName();
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
    String[] types = new String[getConfigOptions().size()];
    for (int i = 0; i < parameters.length; i++) {
      ConfigOption option = getConfigOptions().get(i);
      parameters[i] = variables.resolve(option.getParameter());
      values[i] = variables.resolve(option.getValue());
      types[i] =
          option.getType() == null ? ConfigOption.Type.String.name() : option.getType().name();
    }

    String messageFieldName = variables.resolve(getMessageField());
    IValueMeta messageValueMeta = rowMeta.searchValueMeta(messageFieldName);
    if (messageValueMeta == null) {
      throw new HopException(
          "Error finding message/value field " + messageFieldName + " in the input rows");
    }

    // Register a coder for the short time that KV<HopRow, GenericRecord> exists in Beam
    //
    if (messageValueMeta.getType() == IValueMeta.TYPE_AVRO) {
      ValueMetaAvroRecord valueMetaAvroRecord = (ValueMetaAvroRecord) messageValueMeta;
      Schema schema = valueMetaAvroRecord.getSchema();
      AvroCoder<GenericRecord> coder = AvroCoder.of(schema);
      pipeline.getCoderRegistry().registerCoderForClass(GenericRecord.class, coder);
    }

    BeamKafkaOutputTransform beamOutputTransform =
        new BeamKafkaOutputTransform(
            transformMeta.getName(),
            variables.resolve(getBootstrapServers()),
            variables.resolve(getTopic()),
            variables.resolve(getKeyField()),
            messageFieldName,
            parameters,
            values,
            types,
            JsonRowMeta.toJson(rowMeta));

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
        "Handled transform (KAFKA OUTPUT) : "
            + transformMeta.getName()
            + ", gets data from "
            + previousTransform.getName());
  }

  /**
   * Gets topic
   *
   * @return value of topic
   */
  public String getTopic() {
    return topic;
  }

  /**
   * @param topic The topic to set
   */
  public void setTopic(String topic) {
    this.topic = topic;
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
   * Gets messageField
   *
   * @return value of messageField
   */
  public String getMessageField() {
    return messageField;
  }

  /**
   * @param messageField The messageField to set
   */
  public void setMessageField(String messageField) {
    this.messageField = messageField;
  }

  /**
   * Gets bootstrapServers
   *
   * @return value of bootstrapServers
   */
  public String getBootstrapServers() {
    return bootstrapServers;
  }

  /**
   * @param bootstrapServers The bootstrapServers to set
   */
  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public List<ConfigOption> getConfigOptions() {
    return configOptions;
  }

  public void setConfigOptions(List<ConfigOption> configOptions) {
    this.configOptions = configOptions;
  }
}
