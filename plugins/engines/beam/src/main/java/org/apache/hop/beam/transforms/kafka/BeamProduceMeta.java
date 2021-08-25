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

import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamKafkaOutputTransform;
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
import org.apache.hop.pipeline.transforms.dummy.DummyData;

import java.util.List;
import java.util.Map;

@Transform(
    id = "BeamKafkaProduce",
    name = "Beam Kafka Produce",
    description = "Send messages to a Kafka Topic (Producer)",
    image = "beam-kafka-output.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/beamkafkaproduce.html")
public class BeamProduceMeta extends BaseTransformMeta
    implements ITransformMeta<BeamProduce, DummyData>, IBeamPipelineTransformHandler {

  @HopMetadataProperty(key = "bootstrap_servers")
  private String bootstrapServers;

  @HopMetadataProperty private String topic;

  @HopMetadataProperty(key = "key_field")
  private String keyField;

  @HopMetadataProperty(key = "message_field")
  private String messageField;

  public BeamProduceMeta() {
    bootstrapServers = "bootstrapServer1:9001,bootstrapServer2:9001";
    topic = "Topic1";
    keyField = "";
    messageField = "";
  }

  @Override
  public BeamProduce createTransform(
      TransformMeta transformMeta,
      DummyData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new BeamProduce(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public DummyData getTransformData() {
    return new DummyData();
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

    BeamKafkaOutputTransform beamOutputTransform =
        new BeamKafkaOutputTransform(
            transformMeta.getName(),
            variables.resolve(getBootstrapServers()),
            variables.resolve(getTopic()),
            variables.resolve(getKeyField()),
            variables.resolve(getMessageField()),
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

  /** @param topic The topic to set */
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

  /** @param keyField The keyField to set */
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

  /** @param messageField The messageField to set */
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

  /** @param bootstrapServers The bootstrapServers to set */
  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }
}
