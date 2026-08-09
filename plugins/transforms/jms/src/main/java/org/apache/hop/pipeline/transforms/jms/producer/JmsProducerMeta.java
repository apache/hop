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

package org.apache.hop.pipeline.transforms.jms.producer;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transforms.jms.consumer.JmsDestinationType;

/** Sends each incoming row to a JMS queue or topic as a text message. */
@Transform(
    id = "JmsProducer",
    name = "i18n::JmsProducer.Name",
    description = "i18n::JmsProducer.Description",
    image = "jms-producer.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Streaming",
    keywords = "i18n::JmsProducer.Keyword",
    documentationUrl = "/pipeline/transforms/jms-producer.html")
@Getter
@Setter
public class JmsProducerMeta extends BaseTransformMeta<JmsProducer, JmsProducerData> {

  /** Name of the JMS connection metadata object. */
  @HopMetadataProperty(key = "connection", injectionKey = "CONNECTION")
  private String connectionName;

  @HopMetadataProperty(key = "destination", injectionKey = "DESTINATION")
  private String destination;

  /** QUEUE or TOPIC. */
  @HopMetadataProperty(key = "destination_type", injectionKey = "DESTINATION_TYPE")
  private String destinationType = JmsDestinationType.QUEUE.name();

  /** Commit each message in a JMS transaction instead of sending it outside one. */
  @HopMetadataProperty(key = "transacted", injectionKey = "TRANSACTED")
  private boolean transacted;

  /** Incoming field holding the message body. Required. */
  @HopMetadataProperty(key = "body_field", injectionKey = "BODY_FIELD")
  private String bodyField;

  /** Optional incoming field holding the correlation id. */
  @HopMetadataProperty(key = "key_field", injectionKey = "KEY_FIELD")
  private String keyField;

  public JmsProducerMeta() {
    super();
  }

  @Override
  public String getDialogClassName() {
    return JmsProducerDialog.class.getName();
  }

  public boolean isTopic() {
    return JmsDestinationType.TOPIC.name().equalsIgnoreCase(destinationType);
  }
}
