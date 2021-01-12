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

package org.apache.hop.beam.transforms.kafka;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyData;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Transform(
        id = "BeamKafkaConsume",
        name = "Beam Kafka Consume",
        description = "Get messages from Kafka topics (Kafka Consumer)",
        image = "beam-kafka-input.svg",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
        documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/beamconsume.html"
)
public class BeamConsumeMeta extends BaseTransformMeta implements ITransformMeta<BeamConsume, DummyData> {

  public static final String BOOTSTRAP_SERVERS = "bootstrap_servers";
  public static final String TOPICS = "topics";
  public static final String KEY_FIELD = "key_field";
  public static final String MESSAGE_FIELD = "message_field";
  public static final String GROUP_ID = "group_id";
  public static final String USE_PROCESSING_TIME = "use_processing_time";
  public static final String USE_LOG_APPEND_TIME = "use_log_append_time";
  public static final String USE_CREATE_TIME = "use_create_time";
  public static final String RESTRICT_TO_COMMITTED = "restrict_to_committed";
  public static final String ALLOW_COMMIT_ON_CONSUMED = "allow_commit_on_consumed";
  public static final String CONFIG_OPTIONS = "config_options";
  public static final String CONFIG_OPTION = "config_option";
  public static final String CONFIG_OPTION_PARAMETER = "parameter";
  public static final String CONFIG_OPTION_VALUE = "value";
  public static final String CONFIG_OPTION_TYPE = "type";

  private String topics;
  private String bootstrapServers;
  private String keyField;
  private String messageField;
  private String groupId;
  private boolean usingProcessingTime; // default
  private boolean usingLogAppendTime;
  private boolean usingCreateTime;
  private boolean restrictedToCommitted;
  private boolean allowingCommitOnConsumedOffset;
  private List<ConfigOption> configOptions;

  public BeamConsumeMeta() {
    super();
    configOptions = new ArrayList<>();
  }

  @Override public void setDefault() {
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

  @Override public BeamConsume createTransform( TransformMeta transformMeta, DummyData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new BeamConsume( transformMeta, this, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public DummyData getTransformData() {
    return new DummyData();
  }

  @Override public String getDialogClassName() {
    return BeamConsumeDialog.class.getName();
  }

  @Override public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform, IVariables variables, IHopMetadataProvider metadataProvider )
    throws HopTransformException {

    IValueMeta keyValueMeta = new ValueMetaString( variables.resolve( keyField ) );
    keyValueMeta.setOrigin( name );
    inputRowMeta.addValueMeta( keyValueMeta );

    IValueMeta messageValueMeta = new ValueMetaString( variables.resolve( messageField ) );
    messageValueMeta.setOrigin( name );
    inputRowMeta.addValueMeta( messageValueMeta );
  }

  @Override public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer();
    xml.append( XmlHandler.addTagValue( BOOTSTRAP_SERVERS, bootstrapServers ) );
    xml.append( XmlHandler.addTagValue( TOPICS, topics ) );
    xml.append( XmlHandler.addTagValue( KEY_FIELD, keyField ) );
    xml.append( XmlHandler.addTagValue( MESSAGE_FIELD, messageField ) );
    xml.append( XmlHandler.addTagValue( GROUP_ID, groupId ) );
    xml.append( XmlHandler.addTagValue( USE_PROCESSING_TIME, usingProcessingTime ) );
    xml.append( XmlHandler.addTagValue( USE_LOG_APPEND_TIME, usingLogAppendTime ) );
    xml.append( XmlHandler.addTagValue( USE_CREATE_TIME, usingCreateTime ) );
    xml.append( XmlHandler.addTagValue( RESTRICT_TO_COMMITTED, restrictedToCommitted ) );
    xml.append( XmlHandler.addTagValue( ALLOW_COMMIT_ON_CONSUMED, allowingCommitOnConsumedOffset ) );
    xml.append( XmlHandler.openTag( CONFIG_OPTIONS ) );
    for ( ConfigOption option : configOptions ) {
      xml.append( XmlHandler.openTag( CONFIG_OPTION ) );
      xml.append( XmlHandler.addTagValue( CONFIG_OPTION_PARAMETER, option.getParameter() ) );
      xml.append( XmlHandler.addTagValue( CONFIG_OPTION_VALUE, option.getValue() ) );
      xml.append( XmlHandler.addTagValue( CONFIG_OPTION_TYPE, option.getType() == null ? "" : option.getType().name() ) );
      xml.append( XmlHandler.closeTag( CONFIG_OPTION ) );
    }
    xml.append( XmlHandler.closeTag( CONFIG_OPTIONS ) );
    return xml.toString();
  }

  @Override public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    bootstrapServers = XmlHandler.getTagValue( transformNode, BOOTSTRAP_SERVERS );
    topics = XmlHandler.getTagValue( transformNode, TOPICS );
    keyField = XmlHandler.getTagValue( transformNode, KEY_FIELD );
    messageField = XmlHandler.getTagValue( transformNode, MESSAGE_FIELD );
    groupId = XmlHandler.getTagValue( transformNode, GROUP_ID );
    usingProcessingTime = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, USE_PROCESSING_TIME ) );
    usingLogAppendTime = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, USE_LOG_APPEND_TIME ) );
    usingCreateTime = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, USE_CREATE_TIME ) );
    restrictedToCommitted = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, RESTRICT_TO_COMMITTED ) );
    allowingCommitOnConsumedOffset = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, ALLOW_COMMIT_ON_CONSUMED ) );
    configOptions = new ArrayList<>();
    Node optionsNode = XmlHandler.getSubNode( transformNode, CONFIG_OPTIONS );
    List<Node> optionNodes = XmlHandler.getNodes( optionsNode, CONFIG_OPTION );
    for ( Node optionNode : optionNodes ) {
      String parameter = XmlHandler.getTagValue( optionNode, CONFIG_OPTION_PARAMETER );
      String value = XmlHandler.getTagValue( optionNode, CONFIG_OPTION_VALUE );
      ConfigOption.Type type = ConfigOption.Type.getTypeFromName( XmlHandler.getTagValue( optionNode, CONFIG_OPTION_TYPE ) );
      configOptions.add( new ConfigOption( parameter, value, type ) );
    }
  }


  /**
   * Gets bootstrapServers
   *
   * @return value of bootstrapServers
   */
  public String getBootstrapServers() {
    return bootstrapServers;
  }

  /**
   * @param bootstrapServers The bootstrapServers to set
   */
  public void setBootstrapServers( String bootstrapServers ) {
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

  /**
   * @param topics The topics to set
   */
  public void setTopics( String topics ) {
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

  /**
   * @param keyField The keyField to set
   */
  public void setKeyField( String keyField ) {
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

  /**
   * @param messageField The messageField to set
   */
  public void setMessageField( String messageField ) {
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

  /**
   * @param groupId The groupId to set
   */
  public void setGroupId( String groupId ) {
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

  /**
   * @param usingProcessingTime The usingProcessingTime to set
   */
  public void setUsingProcessingTime( boolean usingProcessingTime ) {
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

  /**
   * @param usingLogAppendTime The usingLogAppendTime to set
   */
  public void setUsingLogAppendTime( boolean usingLogAppendTime ) {
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

  /**
   * @param usingCreateTime The usingCreateTime to set
   */
  public void setUsingCreateTime( boolean usingCreateTime ) {
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

  /**
   * @param restrictedToCommitted The restrictedToCommitted to set
   */
  public void setRestrictedToCommitted( boolean restrictedToCommitted ) {
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

  /**
   * @param allowingCommitOnConsumedOffset The allowingCommitOnConsumedOffset to set
   */
  public void setAllowingCommitOnConsumedOffset( boolean allowingCommitOnConsumedOffset ) {
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

  /**
   * @param configOptions The configOptions to set
   */
  public void setConfigOptions( List<ConfigOption> configOptions ) {
    this.configOptions = configOptions;
  }
}
