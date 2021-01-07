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

import com.google.common.base.Preconditions;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Transform(
    id = "KafkaProducerOutput",
    image = "KafkaProducerOutput.svg",
    name = "i18n::KafkaProducer.TypeLongDesc",
    description = "i18n::KafkaProducer.TypeTooltipDesc",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Streaming",
    keywords = "kafka,producer,output")
@InjectionSupported(
    localizationPrefix = "KafkaProducerOutputMeta.Injection.",
    groups = {"CONFIGURATION_PROPERTIES"})
public class KafkaProducerOutputMeta extends BaseTransformMeta
    implements ITransformMeta<KafkaProducerOutput, KafkaProducerOutputData> {

  public static final String DIRECT_BOOTSTRAP_SERVERS = "directBootstrapServers";
  public static final String CLIENT_ID = "clientId";
  public static final String TOPIC = "topic";
  public static final String KEY_FIELD = "keyField";
  public static final String MESSAGE_FIELD = "messageField";
  public static final String ADVANCED_CONFIG = "advancedConfig";
  public static final String CONFIG_OPTION = "option";
  public static final String OPTION_PROPERTY = "property";
  public static final String OPTION_VALUE = "value";

  @Injection(name = "DIRECT_BOOTSTRAP_SERVERS")
  private String directBootstrapServers;

  @Injection(name = "CLIENT_ID")
  private String clientId;

  @Injection(name = "TOPIC")
  private String topicVal;

  @Injection(name = "KEY_FIELD")
  private String keyField;

  @Injection(name = "MESSAGE_FIELD")
  private String messageField;

  @Injection(name = "NAMES", group = "CONFIGURATION_PROPERTIES")
  protected List<String> injectedConfigNames;

  @Injection(name = "VALUES", group = "CONFIGURATION_PROPERTIES")
  protected List<String> injectedConfigValues;

  private Map<String, String> config = new LinkedHashMap<>();

  public KafkaProducerOutputMeta() {
    super();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider) {
    setDirectBootstrapServers(XmlHandler.getTagValue(transformNode, DIRECT_BOOTSTRAP_SERVERS));
    setClientId(XmlHandler.getTagValue(transformNode, CLIENT_ID));
    setTopic(XmlHandler.getTagValue(transformNode, TOPIC));
    setKeyField(XmlHandler.getTagValue(transformNode, KEY_FIELD));
    setMessageField(XmlHandler.getTagValue(transformNode, MESSAGE_FIELD));

    config = new LinkedHashMap<>();

    Optional.ofNullable(XmlHandler.getSubNode(transformNode, ADVANCED_CONFIG))
        .map(Node::getChildNodes)
        .ifPresent(
            nodes ->
                IntStream.range(0, nodes.getLength())
                    .mapToObj(nodes::item)
                    .filter(node -> node.getNodeType() == Node.ELEMENT_NODE)
                    .forEach(
                        node -> {
                          if (CONFIG_OPTION.equals(node.getNodeName())) {
                            config.put(
                                node.getAttributes().getNamedItem(OPTION_PROPERTY).getTextContent(),
                                node.getAttributes().getNamedItem(OPTION_VALUE).getTextContent());
                          } else {
                            config.put(node.getNodeName(), node.getTextContent());
                          }
                        }));
  }

  @Override
  public void setDefault() {
    // no defaults
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
  public KafkaProducerOutput createTransform(
      TransformMeta transformMeta,
      KafkaProducerOutputData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new KafkaProducerOutput(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public KafkaProducerOutputData getTransformData() {
    return new KafkaProducerOutputData();
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public String getTopic() {
    return topicVal;
  }

  public void setTopic(String topic) {
    this.topicVal = topic;
  }

  public String getKeyField() {
    return keyField;
  }

  public void setKeyField(String keyField) {
    this.keyField = keyField;
  }

  public String getMessageField() {
    return messageField;
  }

  public void setMessageField(String messageField) {
    this.messageField = messageField;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval
        .append("    ")
        .append(XmlHandler.addTagValue(DIRECT_BOOTSTRAP_SERVERS, directBootstrapServers));
    retval.append("    ").append(XmlHandler.addTagValue(TOPIC, topicVal));
    retval.append("    ").append(XmlHandler.addTagValue(CLIENT_ID, clientId));
    retval.append("    ").append(XmlHandler.addTagValue(KEY_FIELD, keyField));
    retval.append("    ").append(XmlHandler.addTagValue(MESSAGE_FIELD, messageField));
    retval.append("    ").append(XmlHandler.openTag(ADVANCED_CONFIG)).append(Const.CR);
    getConfig()
        .forEach(
            (key, value) ->
                retval
                    .append("        ")
                    .append(
                        XmlHandler.addTagValue(
                            CONFIG_OPTION,
                            "",
                            true,
                            OPTION_PROPERTY,
                            (String) key,
                            OPTION_VALUE,
                            (String) value)));
    retval.append("    ").append(XmlHandler.closeTag(ADVANCED_CONFIG)).append(Const.CR);

    return retval.toString();
  }

  public void setDirectBootstrapServers(final String directBootstrapServers) {
    this.directBootstrapServers = directBootstrapServers;
  }

  public String getDirectBootstrapServers() {
    return directBootstrapServers;
  }

  public void setConfig(Map<String, String> config) {
    this.config = config;
  }

  public Map<String, String> getConfig() {
    applyInjectedProperties();
    return config;
  }

  protected void applyInjectedProperties() {
    if (injectedConfigNames != null || injectedConfigValues != null) {
      Preconditions.checkState(injectedConfigNames != null, "Options names were not injected");
      Preconditions.checkState(injectedConfigValues != null, "Options values were not injected");
      Preconditions.checkState(
          injectedConfigNames.size() == injectedConfigValues.size(),
          "Injected different number of options names and value");

      setConfig(
          IntStream.range(0, injectedConfigNames.size())
              .boxed()
              .collect(
                  Collectors.toMap(
                      injectedConfigNames::get,
                      injectedConfigValues::get,
                      (v1, v2) -> v1,
                      LinkedHashMap::new)));

      injectedConfigNames = null;
      injectedConfigValues = null;
    }
  }
}
