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

package org.apache.hop.beam.transforms.pubsub;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamPublishTransform;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "BeamPublish",
    name = "i18n::BeamPublishDialog.DialogTitle",
    description = "i18n::BeamPublishDialog.Description",
    image = "beam-gcp-pubsub-publish.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamPublishMeta.keyword",
    documentationUrl = "/pipeline/transforms/beamgcppublisher.html")
public class BeamPublishMeta extends BaseTransformMeta<BeamPublish, BeamPublishData>
    implements IBeamPipelineTransformHandler {

  public static final String TOPIC = "topic";
  public static final String MESSAGE_TYPE = "message_type";
  public static final String MESSAGE_FIELD = "message_field";

  @HopMetadataProperty private String topic;

  @HopMetadataProperty(key = "message_type")
  private String messageType;

  @HopMetadataProperty(key = "message_field")
  private String messageField;

  public BeamPublishMeta() {
    topic = "Topic";
    messageType = "String";
    messageField = "message";
  }

  @Override
  public String getDialogClassName() {
    return BeamPublishDialog.class.getName();
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

    // some validation
    //
    if (StringUtils.isEmpty(topic)) {
      throw new HopException(
          "Please specify a topic to publish to in Beam Pub/Sub Publish transform '"
              + transformMeta.getName()
              + "'");
    }

    BeamPublishTransform beamOutputTransform =
        new BeamPublishTransform(
            transformMeta.getName(),
            variables.resolve(topic),
            messageType,
            messageField,
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
        "Handled transform (PUBLISH) : "
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
   * Gets messageType
   *
   * @return value of messageType
   */
  public String getMessageType() {
    return messageType;
  }

  /**
   * @param messageType The messageType to set
   */
  public void setMessageType(String messageType) {
    this.messageType = messageType;
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
}
