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

import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamDefaults;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamSubscribeTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaSerializable;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

@Transform(
    id = "BeamSubscribe",
    name = "Beam GCP Pub/Sub : Subscribe",
    description = "Subscribe to data from a Pub/Sub topic",
    image = "beam-gcp-pubsub-subscribe.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/beamgcpsubscriber.html")
public class BeamSubscribeMeta extends BaseTransformMeta<BeamSubscribe, BeamSubscribeData>
    implements IBeamPipelineTransformHandler {

  @HopMetadataProperty private String topic;
  @HopMetadataProperty private String subscription;

  @HopMetadataProperty(key = "message_type")
  private String messageType;

  @HopMetadataProperty(key = "message_field")
  private String messageField;

  public BeamSubscribeMeta() {
    subscription = "Subscription";
    topic = "Topic";
    messageType = "String";
    messageField = "message";
  }

  @Override
  public String getDialogClassName() {
    return BeamSubscribeDialog.class.getName();
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

    if (StringUtils.isEmpty(messageType)) {
      throw new HopTransformException("You need to specify the type of message to read");
    }
    if (StringUtils.isEmpty(messageField)) {
      throw new HopTransformException("You need to specify the field name of the message to read");
    }

    String type = variables.resolve(messageType);
    String fieldName = variables.resolve(messageField);

    IValueMeta valueMeta;
    if (BeamDefaults.PUBSUB_MESSAGE_TYPE_STRING.equalsIgnoreCase(type)) {
      valueMeta = new ValueMetaString(fieldName);
    } else {
      valueMeta = new ValueMetaSerializable(fieldName);
    }

    valueMeta.setOrigin(name);
    inputRowMeta.addValueMeta(valueMeta);
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
    IRowMeta outputRowMeta = pipelineMeta.getTransformFields(variables, transformMeta);
    String rowMetaJson = JsonRowMeta.toJson(outputRowMeta);

    // Verify some things:
    //
    if (StringUtils.isEmpty(topic)) {
      throw new HopException(
          "Please specify a topic to read from in Beam Pub/Sub Subscribe transform '"
              + transformMeta.getName()
              + "'");
    }

    BeamSubscribeTransform subscribeTransform =
        new BeamSubscribeTransform(
            transformMeta.getName(),
            transformMeta.getName(),
            variables.resolve(subscription),
            variables.resolve(topic),
            messageType,
            rowMetaJson,
            transformPluginClasses,
            xpPluginClasses);

    PCollection<HopRow> afterInput = pipeline.apply(subscribeTransform);
    transformCollectionMap.put(transformMeta.getName(), afterInput);

    log.logBasic("Handled transform (SUBSCRIBE) : " + transformMeta.getName());
  }

  /**
   * Gets subscription
   *
   * @return value of subscription
   */
  public String getSubscription() {
    return subscription;
  }

  /** @param subscription The subscription to set */
  public void setSubscription(String subscription) {
    this.subscription = subscription;
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
   * Gets messageType
   *
   * @return value of messageType
   */
  public String getMessageType() {
    return messageType;
  }

  /** @param messageType The messageType to set */
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

  /** @param messageField The messageField to set */
  public void setMessageField(String messageField) {
    this.messageField = messageField;
  }
}
