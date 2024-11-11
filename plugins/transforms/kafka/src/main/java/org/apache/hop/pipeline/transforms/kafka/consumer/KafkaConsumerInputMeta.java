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

package org.apache.hop.pipeline.transforms.kafka.consumer;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.pipelineexecutor.PipelineExecutorMeta;
import org.w3c.dom.Node;

@Transform(
    id = "KafkaConsumer",
    image = "KafkaConsumerInput.svg",
    name = "i18n::KafkaConsumer.TypeLongDesc",
    description = "i18n::KafkaConsumer.TypeTooltipDesc",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Streaming",
    keywords = "i18n::KafkaConsumerInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/kafkaconsumer.html",
    actionTransformTypes = {ActionTransformType.HOP_FILE, ActionTransformType.HOP_PIPELINE})
@InjectionSupported(
    localizationPrefix = "KafkaConsumerInputMeta.Injection.",
    groups = {"CONFIGURATION_PROPERTIES"})
public class KafkaConsumerInputMeta
    extends TransformWithMappingMeta<KafkaConsumerInput, KafkaConsumerInputData>
    implements Cloneable {

  private static final Class<?> PKG = KafkaConsumerInputMeta.class;

  public static final String NUM_MESSAGES = "numMessages";
  public static final String DURATION = "duration";
  public static final String SUB_TRANSFORM = "subTransform";
  public static final String TOPIC = "topic";
  public static final String CONSUMER_GROUP = "consumerGroup";
  public static final String PIPELINE_PATH = "pipelinePath";
  public static final String BATCH_SIZE = "batchSize";
  public static final String BATCH_DURATION = "batchDuration";
  public static final String DIRECT_BOOTSTRAP_SERVERS = "directBootstrapServers";
  public static final String ADVANCED_CONFIG = "advancedConfig";
  public static final String CONFIG_OPTION = "option";
  public static final String OPTION_PROPERTY = "property";
  public static final String OPTION_VALUE = "value";
  public static final String TOPIC_FIELD_NAME = TOPIC;
  public static final String OFFSET_FIELD_NAME = "offset";
  public static final String PARTITION_FIELD_NAME = "partition";
  public static final String TIMESTAMP_FIELD_NAME = "timestamp";
  public static final String OUTPUT_FIELD_TAG_NAME = "OutputField";
  public static final String KAFKA_NAME_ATTRIBUTE = "kafkaName";
  public static final String TYPE_ATTRIBUTE = "type";
  public static final String AUTO_COMMIT = "AUTO_COMMIT";

  @Injection(name = PIPELINE_PATH)
  protected String filename = "";

  @Injection(name = NUM_MESSAGES)
  protected String batchSize = "1000";

  @Injection(name = DURATION)
  protected String batchDuration = "1000";

  @Injection(name = SUB_TRANSFORM)
  protected String subTransform = "";

  @Injection(name = "DIRECT_BOOTSTRAP_SERVERS")
  private String directBootstrapServers;

  @Injection(name = "TOPICS")
  private List<String> topics;

  @Injection(name = "CONSUMER_GROUP")
  private String consumerGroup;

  @InjectionDeep(prefix = "KEY")
  private KafkaConsumerField keyField;

  @InjectionDeep(prefix = "MESSAGE")
  private KafkaConsumerField messageField;

  @Injection(name = "NAMES", group = "CONFIGURATION_PROPERTIES")
  @SuppressWarnings("java:S2065") // disable sonar warning on transient
  protected transient List<String> injectedConfigNames;

  @Injection(name = "VALUES", group = "CONFIGURATION_PROPERTIES")
  @SuppressWarnings("java:S2065") // disable sonar warning on transient
  protected transient List<String> injectedConfigValues;

  @Injection(name = AUTO_COMMIT)
  private boolean autoCommit = true;

  private Map<String, String> config = new LinkedHashMap<>();

  private KafkaConsumerField topicField;

  private KafkaConsumerField offsetField;

  private KafkaConsumerField partitionField;

  private KafkaConsumerField timestampField;

  // Describe the standard way of retrieving mappings
  //
  @FunctionalInterface
  interface MappingMetaRetriever {
    PipelineMeta get(
        TransformWithMappingMeta mappingMeta,
        IHopMetadataProvider metadataProvider,
        IVariables variables)
        throws HopException;
  }

  MappingMetaRetriever mappingMetaRetriever = PipelineExecutorMeta::loadMappingMeta;

  public KafkaConsumerInputMeta() {
    super(); // allocate BaseTransformMeta

    topics = new ArrayList<>();

    keyField =
        new KafkaConsumerField(
            KafkaConsumerField.Name.KEY,
            BaseMessages.getString(PKG, "KafkaConsumerInputDialog.KeyField"));

    messageField =
        new KafkaConsumerField(
            KafkaConsumerField.Name.MESSAGE,
            BaseMessages.getString(PKG, "KafkaConsumerInputDialog.MessageField"));

    topicField =
        new KafkaConsumerField(
            KafkaConsumerField.Name.TOPIC,
            BaseMessages.getString(PKG, "KafkaConsumerInputDialog.TopicField"));

    partitionField =
        new KafkaConsumerField(
            KafkaConsumerField.Name.PARTITION,
            BaseMessages.getString(PKG, "KafkaConsumerInputDialog.PartitionField"),
            KafkaConsumerField.Type.Integer);

    offsetField =
        new KafkaConsumerField(
            KafkaConsumerField.Name.OFFSET,
            BaseMessages.getString(PKG, "KafkaConsumerInputDialog.OffsetField"),
            KafkaConsumerField.Type.Integer);

    timestampField =
        new KafkaConsumerField(
            KafkaConsumerField.Name.TIMESTAMP,
            BaseMessages.getString(PKG, "KafkaConsumerInputDialog.TimestampField"),
            KafkaConsumerField.Type.Integer);
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider) {

    setFilename(XmlHandler.getTagValue(transformNode, PIPELINE_PATH));

    topics = new ArrayList<>();
    List<Node> topicsNode = XmlHandler.getNodes(transformNode, TOPIC);
    topicsNode.forEach(
        node -> {
          String displayName = XmlHandler.getNodeValue(node);
          topics.add(displayName);
        });

    setConsumerGroup(XmlHandler.getTagValue(transformNode, CONSUMER_GROUP));
    String subTransformTag = XmlHandler.getTagValue(transformNode, SUB_TRANSFORM);
    if (!StringUtils.isEmpty(subTransformTag)) {
      setSubTransform(subTransformTag);
    }
    setBatchSize(XmlHandler.getTagValue(transformNode, BATCH_SIZE));
    setBatchDuration(XmlHandler.getTagValue(transformNode, BATCH_DURATION));
    setDirectBootstrapServers(XmlHandler.getTagValue(transformNode, DIRECT_BOOTSTRAP_SERVERS));

    String autoCommitValue = XmlHandler.getTagValue(transformNode, AUTO_COMMIT);
    setAutoCommit("Y".equals(autoCommitValue) || StringUtils.isEmpty(autoCommitValue));

    List<Node> ofNode = XmlHandler.getNodes(transformNode, OUTPUT_FIELD_TAG_NAME);

    ofNode.forEach(
        node -> {
          String displayName = XmlHandler.getNodeValue(node);
          String kafkaName = XmlHandler.getTagAttribute(node, KAFKA_NAME_ATTRIBUTE);
          String type = XmlHandler.getTagAttribute(node, TYPE_ATTRIBUTE);
          KafkaConsumerField field =
              new KafkaConsumerField(
                  KafkaConsumerField.Name.valueOf(kafkaName.toUpperCase()),
                  displayName,
                  KafkaConsumerField.Type.valueOf(type));

          setField(field);
        });

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

  protected void setField(KafkaConsumerField field) {
    field.getKafkaName().setFieldOnMeta(this, field);
  }

  @Override
  public void setDefault() {
    batchSize = "1000";
    batchDuration = "1000";
  }

  public RowMeta getRowMeta(String origin, IVariables variables) throws HopTransformException {
    RowMeta rowMeta = new RowMeta();
    putFieldOnRowMeta(getKeyField(), rowMeta, origin, variables);
    putFieldOnRowMeta(getMessageField(), rowMeta, origin, variables);
    putFieldOnRowMeta(getTopicField(), rowMeta, origin, variables);
    putFieldOnRowMeta(getPartitionField(), rowMeta, origin, variables);
    putFieldOnRowMeta(getOffsetField(), rowMeta, origin, variables);
    putFieldOnRowMeta(getTimestampField(), rowMeta, origin, variables);
    return rowMeta;
  }

  private void putFieldOnRowMeta(
      KafkaConsumerField field, IRowMeta rowMeta, String origin, IVariables variables)
      throws HopTransformException {
    if (field != null && !StringUtils.isEmpty(field.getOutputName())) {
      try {
        String value = variables.resolve(field.getOutputName());
        IValueMeta v =
            ValueMetaFactory.createValueMeta(value, field.getOutputType().getIValueMetaType());
        v.setOrigin(origin);
        rowMeta.addValueMeta(v);
      } catch (Exception e) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "KafkaConsumerInputMeta.UnableToCreateValueType", field.getOutputName()),
            e);
      }
    }
  }

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder();

    getTopics().forEach(topic -> xml.append("    ").append(XmlHandler.addTagValue(TOPIC, topic)));

    xml.append("    ").append(XmlHandler.addTagValue(CONSUMER_GROUP, consumerGroup));
    xml.append("    ").append(XmlHandler.addTagValue(PIPELINE_PATH, filename));
    xml.append("    ").append(XmlHandler.addTagValue(SUB_TRANSFORM, getSubTransform()));
    xml.append("    ").append(XmlHandler.addTagValue(BATCH_SIZE, batchSize));
    xml.append("    ").append(XmlHandler.addTagValue(BATCH_DURATION, batchDuration));
    xml.append("    ")
        .append(XmlHandler.addTagValue(DIRECT_BOOTSTRAP_SERVERS, directBootstrapServers));
    xml.append("    ").append(XmlHandler.addTagValue(AUTO_COMMIT, autoCommit));

    getFieldDefinitions()
        .forEach(
            field ->
                xml.append("    ")
                    .append(
                        XmlHandler.addTagValue(
                            OUTPUT_FIELD_TAG_NAME,
                            field.getOutputName(),
                            true,
                            KAFKA_NAME_ATTRIBUTE,
                            field.getKafkaName().toString(),
                            TYPE_ATTRIBUTE,
                            field.getOutputType().toString())));

    xml.append("    ").append(XmlHandler.openTag(ADVANCED_CONFIG)).append(Const.CR);
    getConfig()
        .forEach(
            (key, value) ->
                xml.append("        ")
                    .append(
                        XmlHandler.addTagValue(
                            CONFIG_OPTION, "", true, OPTION_PROPERTY, key, OPTION_VALUE, value)));
    xml.append("    ").append(XmlHandler.closeTag(ADVANCED_CONFIG)).append(Const.CR);

    return xml.toString();
  }

  public List<KafkaConsumerField> getFieldDefinitions() {
    return new ArrayList<>(
        Arrays.asList(
            getKeyField(),
            getMessageField(),
            getTopicField(),
            getPartitionField(),
            getOffsetField(),
            getTimestampField()));
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

  @Override
  public KafkaConsumerInputMeta clone() {
    return copyObject();
  }

  public KafkaConsumerInputMeta copyObject() {
    KafkaConsumerInputMeta newClone = (KafkaConsumerInputMeta) super.clone();
    newClone.topics = new ArrayList<>(this.topics);
    newClone.keyField = new KafkaConsumerField(this.keyField);
    newClone.messageField = new KafkaConsumerField(this.messageField);
    if (null != this.injectedConfigNames) {
      newClone.injectedConfigNames = new ArrayList<>(this.injectedConfigNames);
    }
    if (null != this.injectedConfigValues) {
      newClone.injectedConfigValues = new ArrayList<>(this.injectedConfigValues);
    }
    newClone.config = new LinkedHashMap<>(this.config);
    newClone.topicField = new KafkaConsumerField(this.topicField);
    newClone.offsetField = new KafkaConsumerField(this.offsetField);
    newClone.partitionField = new KafkaConsumerField(this.partitionField);
    newClone.timestampField = new KafkaConsumerField(this.timestampField);
    return newClone;
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      PipelineMeta pipelineMeta = mappingMetaRetriever.get(this, metadataProvider, variables);
      if (!StringUtils.isEmpty(getSubTransform())) {
        String realSubTransformName = variables.resolve(getSubTransform());
        rowMeta.addRowMeta(pipelineMeta.getPrevTransformFields(variables, realSubTransformName));
        pipelineMeta.getTransforms().stream()
            .filter(transformMeta -> transformMeta.getName().equals(realSubTransformName))
            .findFirst()
            .ifPresent(
                transformMeta -> {
                  try {
                    transformMeta
                        .getTransform()
                        .getFields(
                            rowMeta, origin, info, nextTransform, variables, metadataProvider);
                  } catch (HopTransformException e) {
                    throw new RuntimeException(e);
                  }
                });
      }

      // Check if we get called from error path and only in that case, show fields that will dump
      // the
      // record coming from the kafka queue.
      TransformErrorMeta transformErrorMeta = getParentTransformMeta().getTransformErrorMeta();
      if (transformErrorMeta != null
          && transformErrorMeta.getTargetTransform().getName().equals(nextTransform.getName())) {
        rowMeta.addValueMeta(createValueMetaString(getKeyField().getOutputName()));
        rowMeta.addValueMeta(createValueMetaString(getMessageField().getOutputName()));
        rowMeta.addValueMeta(createValueMetaString(getTopicField().getOutputName()));
        rowMeta.addValueMeta(createValueMetaInteger(getPartitionField().getOutputName()));
        rowMeta.addValueMeta(createValueMetaInteger(getOffsetField().getOutputName()));
        rowMeta.addValueMeta(createValueMetaInteger(getTimestampField().getOutputName()));
      }
    } catch (HopException e) {
      getLog().logDebug("could not get fields, probable AEL");
      rowMeta.addRowMeta(getRowMeta(origin, variables));
    }
  }

  private IValueMeta createValueMetaString(String name) {
    IValueMeta vm = new ValueMetaString(name);
    vm.setOrigin(getParentTransformMeta().getName());

    return vm;
  }

  private IValueMeta createValueMetaInteger(String name) {
    IValueMeta vm = new ValueMetaInteger(name);
    vm.setOrigin(getParentTransformMeta().getName());

    return vm;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    long duration = Long.MIN_VALUE;
    try {
      duration = Long.parseLong(variables.resolve(getBatchDuration()));
    } catch (NumberFormatException e) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "KafkaConsumerInputMeta.CheckResult.NaN", "Duration"),
              transformMeta));
    }

    long size = Long.MIN_VALUE;
    try {
      size = Long.parseLong(variables.resolve(getBatchSize()));
    } catch (NumberFormatException e) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "KafkaConsumerInputMeta.CheckResult.NaN", "Number of records"),
              transformMeta));
    }

    if (duration == 0 && size == 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "KafkaConsumerInputMeta.CheckResult.NoBatchDefined"),
              transformMeta));
    }
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] {StringUtils.isNotEmpty(filename)};
  }

  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] {"Kafka Pipeline"};
  }

  @Override
  public String getActiveReferencedObjectDescription() {
    return "Running Kafka Pipeline";
  }

  @Override
  public IHasFilename loadReferencedObject(
      int index, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    return TransformWithMappingMeta.loadMappingMeta(this, metadataProvider, variables);
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  @Override
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename The filename to set
   */
  @Override
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /**
   * Gets batchSize
   *
   * @return value of batchSize
   */
  public String getBatchSize() {
    return batchSize;
  }

  /**
   * @param batchSize The batchSize to set
   */
  public void setBatchSize(String batchSize) {
    this.batchSize = batchSize;
  }

  /**
   * Gets batchDuration
   *
   * @return value of batchDuration
   */
  public String getBatchDuration() {
    return batchDuration;
  }

  /**
   * @param batchDuration The batchDuration to set
   */
  public void setBatchDuration(String batchDuration) {
    this.batchDuration = batchDuration;
  }

  /**
   * Gets the name of the transform in the kafka pipeline to retrieve data from
   *
   * @return name of the transform
   */
  public String getSubTransform() {
    return subTransform;
  }

  /**
   * @param subTransform the name of the transform in the kafka pipeline to retrieve data from to
   *     set
   */
  public void setSubTransform(String subTransform) {
    this.subTransform = subTransform;
  }

  /**
   * Gets directBootstrapServers
   *
   * @return value of directBootstrapServers
   */
  public String getDirectBootstrapServers() {
    return directBootstrapServers;
  }

  /**
   * @param directBootstrapServers The directBootstrapServers to set
   */
  public void setDirectBootstrapServers(String directBootstrapServers) {
    this.directBootstrapServers = directBootstrapServers;
  }

  /**
   * Gets topics
   *
   * @return value of topics
   */
  public List<String> getTopics() {
    return topics;
  }

  /**
   * @param topics The topics to set
   */
  public void setTopics(List<String> topics) {
    this.topics = topics;
  }

  /**
   * Gets consumerGroup
   *
   * @return value of consumerGroup
   */
  public String getConsumerGroup() {
    return consumerGroup;
  }

  /**
   * @param consumerGroup The consumerGroup to set
   */
  public void setConsumerGroup(String consumerGroup) {
    this.consumerGroup = consumerGroup;
  }

  /**
   * Gets keyField
   *
   * @return value of keyField
   */
  public KafkaConsumerField getKeyField() {
    return keyField;
  }

  /**
   * @param keyField The keyField to set
   */
  public void setKeyField(KafkaConsumerField keyField) {
    this.keyField = keyField;
  }

  /**
   * Gets messageField
   *
   * @return value of messageField
   */
  public KafkaConsumerField getMessageField() {
    return messageField;
  }

  /**
   * @param messageField The messageField to set
   */
  public void setMessageField(KafkaConsumerField messageField) {
    this.messageField = messageField;
  }

  /**
   * Gets injectedConfigNames
   *
   * @return value of injectedConfigNames
   */
  public List<String> getInjectedConfigNames() {
    return injectedConfigNames;
  }

  /**
   * @param injectedConfigNames The injectedConfigNames to set
   */
  public void setInjectedConfigNames(List<String> injectedConfigNames) {
    this.injectedConfigNames = injectedConfigNames;
  }

  /**
   * Gets injectedConfigValues
   *
   * @return value of injectedConfigValues
   */
  public List<String> getInjectedConfigValues() {
    return injectedConfigValues;
  }

  /**
   * @param injectedConfigValues The injectedConfigValues to set
   */
  public void setInjectedConfigValues(List<String> injectedConfigValues) {
    this.injectedConfigValues = injectedConfigValues;
  }

  /**
   * Gets topicField
   *
   * @return value of topicField
   */
  public KafkaConsumerField getTopicField() {
    return topicField;
  }

  /**
   * @param topicField The topicField to set
   */
  public void setTopicField(KafkaConsumerField topicField) {
    this.topicField = topicField;
  }

  /**
   * Gets offsetField
   *
   * @return value of offsetField
   */
  public KafkaConsumerField getOffsetField() {
    return offsetField;
  }

  /**
   * @param offsetField The offsetField to set
   */
  public void setOffsetField(KafkaConsumerField offsetField) {
    this.offsetField = offsetField;
  }

  /**
   * Gets partitionField
   *
   * @return value of partitionField
   */
  public KafkaConsumerField getPartitionField() {
    return partitionField;
  }

  /**
   * @param partitionField The partitionField to set
   */
  public void setPartitionField(KafkaConsumerField partitionField) {
    this.partitionField = partitionField;
  }

  /**
   * Gets timestampField
   *
   * @return value of timestampField
   */
  public KafkaConsumerField getTimestampField() {
    return timestampField;
  }

  /**
   * @param timestampField The timestampField to set
   */
  public void setTimestampField(KafkaConsumerField timestampField) {
    this.timestampField = timestampField;
  }

  /**
   * Gets autoCommit
   *
   * @return value of autoCommit
   */
  public boolean isAutoCommit() {
    return autoCommit;
  }

  /**
   * @param autoCommit The autoCommit to set
   */
  public void setAutoCommit(boolean autoCommit) {
    this.autoCommit = autoCommit;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
