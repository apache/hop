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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaOption;
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
@Getter
@Setter
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
  public static final String EXECUTION_INFORMATION_LOCATION = "executionInformationLocation";
  public static final String EXECUTION_DATA_PROFILE = "executionDataProfile";
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

  @HopMetadataProperty private KeyConsumerField keyField;

  @HopMetadataProperty private MessageConsumerField messageField;

  @HopMetadataProperty private TopicConsumerField topicField;

  @HopMetadataProperty private OffsetConsumerField offsetField;

  @HopMetadataProperty private PartitionConsumerField partitionField;

  @HopMetadataProperty private TimestampConsumerField timestampField;

  @HopMetadataProperty(
      key = "pipelinePath",
      injectionKey = "pipelinePath",
      injectionKeyDescription = "KafkaConsumerInputMeta.Injection.PIPELINE_PATH")
  protected String filename;

  @HopMetadataProperty(
      key = "executionInformationLocation",
      injectionKey = "executionInformationLocation",
      injectionKeyDescription = "KafkaConsumerInputMeta.Injection.EXECUTION_INFORMATION_LOCATION")
  protected String executionInformationLocation;

  @HopMetadataProperty(
      key = "executionDataProfile",
      injectionKey = "executionDataProfile",
      injectionKeyDescription = "KafkaConsumerInputMeta.Injection.EXECUTION_DATA_PROFILE")
  protected String executionDataProfile;

  @HopMetadataProperty(
      key = "batchSize",
      injectionKey = "numMessages",
      injectionKeyDescription = "KafkaConsumerInputMeta.Injection.NUM_MESSAGES")
  protected String batchSize;

  @HopMetadataProperty(
      key = "batchDuration",
      injectionKey = "duration",
      injectionKeyDescription = "KafkaConsumerInputMeta.Injection.BATCH_DURATION")
  protected String batchDuration;

  @HopMetadataProperty(
      key = "subTransform",
      injectionKey = "subTransform",
      injectionKeyDescription = "KafkaConsumerInputMeta.Injection.SUB_TRANSFORM")
  protected String subTransform;

  @HopMetadataProperty(
      key = "directBootstrapServers",
      injectionKey = "DIRECT_BOOTSTRAP_SERVERS",
      injectionKeyDescription = "KafkaConsumerInputMeta.Injection.DIRECT_BOOTSTRAP_SERVERS")
  private String directBootstrapServers;

  @HopMetadataProperty(
      key = "topic",
      injectionGroupKey = "TOPICS_LIST",
      injectionGroupDescription = "KafkaConsumerInputMeta.Injection.TOPICS_LIST",
      injectionKey = "TOPICS",
      injectionKeyDescription = "KafkaConsumerInputMeta.Injection.TOPICS")
  private List<String> topics;

  @HopMetadataProperty(
      key = "consumerGroup",
      injectionKey = CONSUMER_GROUP,
      injectionKeyDescription = "KafkaConsumerInputMeta.Injection.CONSUMER_GROUP")
  private String consumerGroup;

  @HopMetadataProperty(
      key = "AUTO_COMMIT",
      injectionKey = "AUTO_COMMIT",
      injectionKeyDescription = "KafkaConsumerInputMeta.Injection.AUTO_COMMIT")
  private boolean autoCommit = true;

  @HopMetadataProperty(
      groupKey = "options",
      key = "option",
      injectionGroupKey = "CONFIGURATION_PROPERTIES",
      injectionGroupDescription = "KafkaConsumerInputMeta.Injection.CONFIGURATION_PROPERTIES")
  private List<KafkaOption> options;

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
    super();
    filename = "";
    executionInformationLocation = "";
    executionDataProfile = "";
    batchSize = "1000";
    batchDuration = "1000";
    subTransform = "";
    topics = new ArrayList<>();
    options = new ArrayList<>();

    keyField = new KeyConsumerField();
    keyField.setKafkaName(KafkaConsumerField.Name.KEY);
    keyField.setOutputType(KafkaConsumerField.Type.String);
    keyField.setOutputName(BaseMessages.getString(PKG, "KafkaConsumerInputDialog.KeyField"));

    messageField = new MessageConsumerField();
    messageField.setKafkaName(KafkaConsumerField.Name.MESSAGE);
    messageField.setOutputType(KafkaConsumerField.Type.String);
    messageField.setOutputName(
        BaseMessages.getString(PKG, "KafkaConsumerInputDialog.MessageField"));

    topicField =
        new TopicConsumerField(BaseMessages.getString(PKG, "KafkaConsumerInputDialog.TopicField"));

    partitionField =
        new PartitionConsumerField(
            BaseMessages.getString(PKG, "KafkaConsumerInputDialog.PartitionField"));

    offsetField =
        new OffsetConsumerField(
            BaseMessages.getString(PKG, "KafkaConsumerInputDialog.OffsetField"));

    timestampField =
        new TimestampConsumerField(
            BaseMessages.getString(PKG, "KafkaConsumerInputDialog.TimestampField"));
  }

  public KafkaConsumerInputMeta(KafkaConsumerInputMeta m) {
    super(m);
    this.keyField = new KeyConsumerField(m.keyField);
    this.messageField = new MessageConsumerField(m.messageField);
    this.topicField = new TopicConsumerField(m.topicField);
    this.offsetField = new OffsetConsumerField(m.offsetField);
    this.partitionField = new PartitionConsumerField(m.partitionField);
    this.timestampField = new TimestampConsumerField(m.timestampField);
    this.filename = m.filename;
    this.executionInformationLocation = m.executionInformationLocation;
    this.executionDataProfile = m.executionDataProfile;
    this.batchSize = m.batchSize;
    this.batchDuration = m.batchDuration;
    this.subTransform = m.subTransform;
    this.directBootstrapServers = m.directBootstrapServers;
    this.topics = new ArrayList<>(m.topics);
    this.consumerGroup = m.consumerGroup;
    this.autoCommit = m.autoCommit;
    this.mappingMetaRetriever = m.mappingMetaRetriever;
    this.options = new ArrayList<>();
    m.options.forEach(o -> this.options.add(new KafkaOption(o)));
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
            ValueMetaFactory.createValueMeta(value, field.getOutputType().getValueMetaType());
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

  @Override
  public KafkaConsumerInputMeta clone() {
    return new KafkaConsumerInputMeta(this);
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
                    throw new HopRuntimeException(e);
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

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public boolean supportsDrillDown() {
    return true;
  }

  @Override
  public void convertLegacyXml(Node transformNode) throws HopException {
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

    // Read the old fields
    //
    List<Node> outputNodes = XmlHandler.getNodes(transformNode, OUTPUT_FIELD_TAG_NAME);
    for (Node outputNode : outputNodes) {
      String nameString = XmlHandler.getTagAttribute(outputNode, KAFKA_NAME_ATTRIBUTE);
      KafkaConsumerField.Name name =
          IEnumHasCode.lookupCode(KafkaConsumerField.Name.class, nameString, null);
      String typeString = XmlHandler.getTagAttribute(outputNode, TYPE_ATTRIBUTE);
      KafkaConsumerField.Type type =
          IEnumHasCode.lookupCode(
              KafkaConsumerField.Type.class, typeString, KafkaConsumerField.Type.String);
      String outputName = XmlHandler.getNodeValue(outputNode);
      KafkaConsumerField field =
          switch (name) {
            case KEY -> keyField;
            case MESSAGE -> messageField;
            case TOPIC -> topicField;
            case PARTITION -> partitionField;
            case OFFSET -> offsetField;
            case TIMESTAMP -> timestampField;
          };
      field.setKafkaName(name);
      field.setOutputType(type);
      field.setOutputName(outputName);
    }
  }

  @Getter
  @Setter
  public static class KeyConsumerField extends KafkaConsumerField {
    @HopMetadataProperty(
        key = "outputName",
        injectionKey = "KEY.OUTPUT_NAME",
        injectionKeyDescription = "KafkaConsumerInputMeta.Injection.KEY.OUTPUT_NAME")
    protected String outputName;

    @HopMetadataProperty(
        key = "type",
        storeWithCode = true,
        injectionKey = "KEY.TYPE",
        injectionKeyDescription = "KafkaConsumerInputMeta.Injection.KEY.TYPE")
    protected Type outputType;

    public KeyConsumerField() {
      super();
      this.outputName = BaseMessages.getString(PKG, "KafkaConsumerInputDialog.KeyField");
      super.kafkaName = Name.KEY;
      this.outputType = Type.String;
    }

    public KeyConsumerField(KeyConsumerField f) {
      this.outputName = f.outputName;
      this.outputType = f.outputType;
      this.kafkaName = f.kafkaName;
    }
  }

  @Getter
  @Setter
  public static class MessageConsumerField extends KafkaConsumerField {
    @HopMetadataProperty(
        key = "outputName",
        injectionKey = "MESSAGE.OUTPUT_NAME",
        injectionKeyDescription = "KafkaConsumerInputMeta.Injection.MESSAGE.OUTPUT_NAME")
    protected String outputName;

    @HopMetadataProperty(
        key = "type",
        storeWithCode = true,
        injectionKey = "MESSAGE.TYPE",
        injectionKeyDescription = "KafkaConsumerInputMeta.Injection.MESSAGE.TYPE")
    protected Type outputType;

    public MessageConsumerField() {
      super();
      this.outputName = BaseMessages.getString(PKG, "KafkaConsumerInputDialog.MessageField");
      super.kafkaName = Name.MESSAGE;
      this.outputType = Type.String;
    }

    public MessageConsumerField(MessageConsumerField f) {
      this.outputName = f.outputName;
      this.outputType = f.outputType;
      this.kafkaName = f.kafkaName;
    }
  }

  @Getter
  @Setter
  public static class TopicConsumerField extends KafkaConsumerField {
    @HopMetadataProperty(
        key = "outputName",
        injectionKey = "TOPIC.OUTPUT_NAME",
        injectionKeyDescription = "KafkaConsumerInputMeta.Injection.TOPIC.OUTPUT_NAME")
    protected String outputName;

    public TopicConsumerField() {
      super();
      this.outputName = BaseMessages.getString(PKG, "KafkaConsumerInputDialog.TopicField");
      super.outputType = Type.String;
      super.kafkaName = Name.TOPIC;
    }

    public TopicConsumerField(TopicConsumerField f) {
      super(f.kafkaName, f.outputName, f.outputType);
      this.outputName = f.outputName;
    }

    public TopicConsumerField(String outputName) {
      this();
      this.outputName = outputName;
    }
  }

  @Getter
  @Setter
  public static class OffsetConsumerField extends KafkaConsumerField {
    @HopMetadataProperty(
        injectionKey = "OFFSET.OUTPUT_NAME",
        injectionKeyDescription = "KafkaConsumerInputMeta.Injection.OFFSET.OUTPUT_NAME")
    protected String outputName;

    public OffsetConsumerField() {
      super();
      this.outputName = BaseMessages.getString(PKG, "KafkaConsumerInputDialog.OffsetField");
      super.outputType = Type.Integer;
      super.kafkaName = Name.OFFSET;
    }

    public OffsetConsumerField(OffsetConsumerField f) {
      super(f.kafkaName, f.outputName, f.outputType);
      this.outputName = f.outputName;
    }

    public OffsetConsumerField(String outputName) {
      this();
      this.outputName = outputName;
    }
  }

  @Getter
  @Setter
  public static class PartitionConsumerField extends KafkaConsumerField {
    @HopMetadataProperty(
        injectionKey = "PARTITION.OUTPUT_NAME",
        injectionKeyDescription = "KafkaConsumerInputMeta.Injection.PARTITION.OUTPUT_NAME")
    protected String outputName;

    public PartitionConsumerField() {
      super();
      this.outputName = BaseMessages.getString(PKG, "KafkaConsumerInputDialog.PartitionField");
      super.outputType = Type.Integer;
      super.kafkaName = Name.PARTITION;
    }

    public PartitionConsumerField(PartitionConsumerField f) {
      this.outputName = f.outputName;
      this.outputType = f.outputType;
      this.kafkaName = f.kafkaName;
    }

    public PartitionConsumerField(String outputName) {
      this();
      this.outputName = outputName;
    }
  }

  @Getter
  @Setter
  public static class TimestampConsumerField extends KafkaConsumerField {
    @HopMetadataProperty(
        injectionKey = "TIMESTAMP.OUTPUT_NAME",
        injectionKeyDescription = "KafkaConsumerInputMeta.Injection.TIMESTAMP.OUTPUT_NAME")
    protected String outputName;

    public TimestampConsumerField() {
      super();
      this.outputName = BaseMessages.getString(PKG, "KafkaConsumerInputDialog.TimestampField");
      super.outputType = Type.Integer;
      super.kafkaName = Name.TIMESTAMP;
    }

    public TimestampConsumerField(TimestampConsumerField f) {
      this.outputName = f.outputName;
      this.outputType = f.outputType;
      this.kafkaName = f.kafkaName;
    }

    public TimestampConsumerField(String outputName) {
      this();
      this.outputName = outputName;
    }
  }
}
