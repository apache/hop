/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.jms.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Reads messages from a JMS queue or topic and turns each into a row.
 *
 * <p>Consumption stops on whichever comes first: the configured message limit, a receive timeout
 * with no message waiting, or the pipeline being stopped.
 */
@Transform(
    id = "JmsConsumer",
    name = "i18n::JmsConsumer.Name",
    description = "i18n::JmsConsumer.Description",
    image = "jms-consumer.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Streaming",
    keywords = "i18n::JmsConsumer.Keyword",
    documentationUrl = "/pipeline/transforms/jms-consumer.html")
@Getter
@Setter
public class JmsConsumerMeta extends BaseTransformMeta<JmsConsumer, JmsConsumerData> {

  /** Name of the JMS connection metadata object. */
  @HopMetadataProperty(key = "connection", injectionKey = "CONNECTION")
  private String connectionName;

  @HopMetadataProperty(key = "destination", injectionKey = "DESTINATION")
  private String destination;

  /** QUEUE or TOPIC. */
  @HopMetadataProperty(key = "destination_type", injectionKey = "DESTINATION_TYPE")
  private String destinationType = JmsDestinationType.QUEUE.name();

  /** Optional JMS message selector, e.g. {@code priority > 5}. */
  @HopMetadataProperty(key = "message_selector", injectionKey = "MESSAGE_SELECTOR")
  private String messageSelector;

  /** Durable subscription name. Topics only; requires a client id on the connection. */
  @HopMetadataProperty(key = "durable_subscription", injectionKey = "DURABLE_SUBSCRIPTION")
  private String durableSubscription;

  /** Commit each message in a JMS transaction rather than acknowledging it individually. */
  @HopMetadataProperty(key = "transacted", injectionKey = "TRANSACTED")
  private boolean transacted;

  /** Stop after this many messages. 0 means no limit. Supports variables. */
  @HopMetadataProperty(key = "max_messages", injectionKey = "MAX_MESSAGES")
  private String maxMessages = "0";

  /** How long to wait for a message before concluding the destination is drained, in ms. */
  @HopMetadataProperty(key = "receive_timeout", injectionKey = "RECEIVE_TIMEOUT")
  private String receiveTimeout = "5000";

  // Output field names. An empty name leaves that field out of the row.
  @HopMetadataProperty(key = "body_field", injectionKey = "BODY_FIELD")
  private String bodyField = "message";

  @HopMetadataProperty(key = "key_field", injectionKey = "KEY_FIELD")
  private String keyField = "";

  @HopMetadataProperty(key = "destination_field", injectionKey = "DESTINATION_FIELD")
  private String destinationField = "";

  @HopMetadataProperty(key = "message_id_field", injectionKey = "MESSAGE_ID_FIELD")
  private String messageIdField = "";

  @HopMetadataProperty(key = "timestamp_field", injectionKey = "TIMESTAMP_FIELD")
  private String timestampField = "";

  public JmsConsumerMeta() {
    super();
  }

  @Override
  public String getDialogClassName() {
    return JmsConsumerDialog.class.getName();
  }

  public boolean isTopic() {
    return JmsDestinationType.TOPIC.name().equalsIgnoreCase(destinationType);
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
    inputRowMeta.clear();
    addStringField(inputRowMeta, variables, bodyField, name);
    addStringField(inputRowMeta, variables, keyField, name);
    addStringField(inputRowMeta, variables, destinationField, name);
    addStringField(inputRowMeta, variables, messageIdField, name);

    String resolvedTimestamp = variables.resolve(timestampField);
    if (StringUtils.isNotEmpty(resolvedTimestamp)) {
      ValueMetaDate valueMeta = new ValueMetaDate(resolvedTimestamp);
      valueMeta.setOrigin(name);
      inputRowMeta.addValueMeta(valueMeta);
    }
  }

  private void addStringField(
      IRowMeta rowMeta, IVariables variables, String fieldName, String origin) {
    String resolved = variables.resolve(fieldName);
    if (StringUtils.isEmpty(resolved)) {
      return;
    }
    ValueMetaString valueMeta = new ValueMetaString(resolved);
    valueMeta.setOrigin(origin);
    rowMeta.addValueMeta(valueMeta);
  }
}
