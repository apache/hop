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
import org.apache.hop.beam.core.transform.BeamKafkaInputTransform;
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
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Transform(
    id = "BeamKafkaConsume",
    name = "Beam Kafka Consume",
    description = "Get messages from Kafka topics (Kafka Consumer)",
    image = "beam-kafka-input.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/beamkafkaconsume.html")
public class BeamConsumeMeta extends BaseTransformMeta
    implements ITransformMeta<BeamConsume, DummyData>, IBeamPipelineTransformHandler {

  @HopMetadataProperty private String topics;

  @HopMetadataProperty(key = "bootstrap_servers")
  private String bootstrapServers;

  @HopMetadataProperty(key = "key_field")
  private String keyField;

  @HopMetadataProperty(key = "message_field")
  private String messageField;

  @HopMetadataProperty(key = "group_id")
  private String groupId;

  @HopMetadataProperty(key = "use_processing_time")
  private boolean usingProcessingTime; // default

  @HopMetadataProperty(key = "use_log_append_time")
  private boolean usingLogAppendTime;

  @HopMetadataProperty(key = "use_create_time")
  private boolean usingCreateTime;

  @HopMetadataProperty(key = "restrict_to_committed")
  private boolean restrictedToCommitted;

  @HopMetadataProperty(key = "allow_commit_on_consumed")
  private boolean allowingCommitOnConsumedOffset;

  @HopMetadataProperty(groupKey = "config_options", key = "config_option")
  private List<ConfigOption> configOptions;

  public BeamConsumeMeta() {
    super();
    configOptions = new ArrayList<>();
  }

  @Override
  public void setDefault() {
    bootstrapServers = "bootstrapServer1:9001,bootstrapServer2:9001";
    topics = "Topic1,Topic2";
    keyField = "key";
    messageField = "message";
    groupId = "GroupID";
    usingProcessingTime = true;
    usingLogAppendTime = false;
    usingCreateTime = false;
    restrictedToCommitted = false;
    allowingCommitOnConsumedOffset = true;
    configOptions = new ArrayList<>();
  }

  @Override
  public BeamConsume createTransform(
      TransformMeta transformMeta,
      DummyData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new BeamConsume(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public DummyData getTransformData() {
    return new DummyData();
  }

  @Override
  public String getDialogClassName() {
    return BeamConsumeDialog.class.getName();
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

    IValueMeta keyValueMeta = new ValueMetaString(variables.resolve(keyField));
    keyValueMeta.setOrigin(name);
    inputRowMeta.addValueMeta(keyValueMeta);

    IValueMeta messageValueMeta = new ValueMetaString(variables.resolve(messageField));
    messageValueMeta.setOrigin(name);
    inputRowMeta.addValueMeta(messageValueMeta);
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

    BeamKafkaInputTransform beamInputTransform =
        new BeamKafkaInputTransform(
            transformMeta.getName(),
            transformMeta.getName(),
            variables.resolve(getBootstrapServers()),
            variables.resolve(getTopics()),
            variables.resolve(getGroupId()),
            isUsingProcessingTime(),
            isUsingLogAppendTime(),
            isUsingCreateTime(),
            isRestrictedToCommitted(),
            isAllowingCommitOnConsumedOffset(),
            parameters,
            values,
            types,
            JsonRowMeta.toJson(outputRowMeta),
            transformPluginClasses,
            xpPluginClasses);
    PCollection<HopRow> afterInput = pipeline.apply(beamInputTransform);
    transformCollectionMap.put(transformMeta.getName(), afterInput);
    log.logBasic("Handled transform (KAFKA INPUT) : " + transformMeta.getName());
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

  /**
   * Gets topics
   *
   * @return value of topics
   */
  public String getTopics() {
    return topics;
  }

  /** @param topics The topics to set */
  public void setTopics(String topics) {
    this.topics = topics;
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
   * Gets groupId
   *
   * @return value of groupId
   */
  public String getGroupId() {
    return groupId;
  }

  /** @param groupId The groupId to set */
  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  /**
   * Gets usingProcessingTime
   *
   * @return value of usingProcessingTime
   */
  public boolean isUsingProcessingTime() {
    return usingProcessingTime;
  }

  /** @param usingProcessingTime The usingProcessingTime to set */
  public void setUsingProcessingTime(boolean usingProcessingTime) {
    this.usingProcessingTime = usingProcessingTime;
  }

  /**
   * Gets usingLogAppendTime
   *
   * @return value of usingLogAppendTime
   */
  public boolean isUsingLogAppendTime() {
    return usingLogAppendTime;
  }

  /** @param usingLogAppendTime The usingLogAppendTime to set */
  public void setUsingLogAppendTime(boolean usingLogAppendTime) {
    this.usingLogAppendTime = usingLogAppendTime;
  }

  /**
   * Gets usingCreateTime
   *
   * @return value of usingCreateTime
   */
  public boolean isUsingCreateTime() {
    return usingCreateTime;
  }

  /** @param usingCreateTime The usingCreateTime to set */
  public void setUsingCreateTime(boolean usingCreateTime) {
    this.usingCreateTime = usingCreateTime;
  }

  /**
   * Gets restrictedToCommitted
   *
   * @return value of restrictedToCommitted
   */
  public boolean isRestrictedToCommitted() {
    return restrictedToCommitted;
  }

  /** @param restrictedToCommitted The restrictedToCommitted to set */
  public void setRestrictedToCommitted(boolean restrictedToCommitted) {
    this.restrictedToCommitted = restrictedToCommitted;
  }

  /**
   * Gets allowingCommitOnConsumedOffset
   *
   * @return value of allowingCommitOnConsumedOffset
   */
  public boolean isAllowingCommitOnConsumedOffset() {
    return allowingCommitOnConsumedOffset;
  }

  /** @param allowingCommitOnConsumedOffset The allowingCommitOnConsumedOffset to set */
  public void setAllowingCommitOnConsumedOffset(boolean allowingCommitOnConsumedOffset) {
    this.allowingCommitOnConsumedOffset = allowingCommitOnConsumedOffset;
  }

  /**
   * Gets configOptions
   *
   * @return value of configOptions
   */
  public List<ConfigOption> getConfigOptions() {
    return configOptions;
  }

  /** @param configOptions The configOptions to set */
  public void setConfigOptions(List<ConfigOption> configOptions) {
    this.configOptions = configOptions;
  }
}
