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

package org.apache.hop.pipeline.transforms.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaOption;
import org.w3c.dom.Node;

@Transform(
    id = "KafkaProducerOutput",
    image = "KafkaProducerOutput.svg",
    name = "i18n::KafkaProducer.TypeLongDesc",
    description = "i18n::KafkaProducer.TypeTooltipDesc",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Streaming",
    keywords = "i18n::KafkaProducerOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/kafkaproducer.html")
@Getter
@Setter
public class KafkaProducerOutputMeta
    extends BaseTransformMeta<KafkaProducerOutput, KafkaProducerOutputData> {

  public static final String ADVANCED_CONFIG = "advancedConfig";
  public static final String CONFIG_OPTION = "option";
  public static final String OPTION_PROPERTY = "property";
  public static final String OPTION_VALUE = "value";

  @HopMetadataProperty(
      key = "directBootstrapServers",
      injectionKey = "DIRECT_BOOTSTRAP_SERVERS",
      injectionKeyDescription = "KafkaProducerOutputMeta.Injection.DIRECT_BOOTSTRAP_SERVERS")
  private String directBootstrapServers;

  @HopMetadataProperty(
      key = "clientId",
      injectionKey = "CLIENT_ID",
      injectionKeyDescription = "KafkaProducerOutputMeta.Injection.CLIENT_ID")
  private String clientId;

  @HopMetadataProperty(
      key = "topic",
      injectionKey = "TOPIC",
      injectionKeyDescription = "KafkaProducerOutputMeta.Injection.TOPIC")
  private String topic;

  @HopMetadataProperty(
      key = "keyField",
      injectionKey = "KEY_FIELD",
      injectionKeyDescription = "KafkaProducerOutputMeta.Injection.KEY_FIELD")
  private String keyField;

  @HopMetadataProperty(
      key = "messageField",
      injectionKey = "MESSAGE_FIELD",
      injectionKeyDescription = "KafkaProducerOutputMeta.Injection.MESSAGE_FIELD")
  private String messageField;

  @HopMetadataProperty(
      groupKey = "options",
      key = "option",
      injectionGroupKey = "CONFIGURATION_PROPERTIES",
      injectionGroupDescription = "KafkaConsumerInputMeta.Injection.CONFIGURATION_PROPERTIES")
  private List<KafkaOption> options;

  public KafkaProducerOutputMeta() {
    super();
    this.options = new ArrayList<>();
  }

  public KafkaProducerOutputMeta(KafkaProducerOutputMeta m) {
    this();
    this.directBootstrapServers = m.directBootstrapServers;
    this.clientId = m.clientId;
    this.topic = m.topic;
    this.keyField = m.keyField;
    this.messageField = m.messageField;
    m.options.forEach(option -> this.options.add(new KafkaOption(option)));
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    // Default: nothing changes to rowMeta
  }

  @Override
  public void convertLegacyXml(Node transformNode) {
    // Read the old options format:
    //
    Node advancedNode = XmlHandler.getSubNode(transformNode, ADVANCED_CONFIG);
    if (advancedNode != null) {
      List<Node> configNodes = XmlHandler.getNodes(advancedNode, CONFIG_OPTION);
      for (Node configNode : configNodes) {
        String property = XmlHandler.getTagAttribute(configNode, OPTION_PROPERTY);
        String value = Const.NVL(XmlHandler.getTagAttribute(configNode, OPTION_VALUE), null);
        options.add(new KafkaOption(property, value));
      }
    }
  }
}
