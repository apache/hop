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

package org.apache.hop.beam.transforms.pubsub;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
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

@Transform(
        id = "BeamPublish",
        name = "Beam GCP Pub/Sub : Publish",
        description = "Publish to a Pub/Sub topic",
        image = "beam-gcp-pubsub-publish.svg",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
        documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/beampublisher.html"
)
public class BeamPublishMeta extends BaseTransformMeta implements ITransformMeta<BeamPublish, BeamPublishData> {

  public static final String TOPIC = "topic";
  public static final String MESSAGE_TYPE = "message_type";
  public static final String MESSAGE_FIELD = "message_field";

  private String topic;
  private String messageType;
  private String messageField;

  public BeamPublishMeta() {
    super();
  }

  @Override public void setDefault() {
    topic = "Topic";
    messageType = "String";
    messageField = "message";
  }

  @Override public BeamPublish createTransform( TransformMeta transformMeta, BeamPublishData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new BeamPublish( transformMeta, this, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public BeamPublishData getTransformData() {
    return new BeamPublishData();
  }

  @Override public String getDialogClassName() {
    return BeamPublishDialog.class.getName();
  }

  @Override public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform, IVariables variables, IHopMetadataProvider metadataProvider )
    throws HopTransformException {

    // No output
    //
    inputRowMeta.clear();
  }

  @Override public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer();
    xml.append( XmlHandler.addTagValue( TOPIC, topic ) );
    xml.append( XmlHandler.addTagValue( MESSAGE_TYPE, messageType ) );
    xml.append( XmlHandler.addTagValue( MESSAGE_FIELD, messageField ) );
    return xml.toString();
  }

  @Override public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    topic = XmlHandler.getTagValue( transformNode, TOPIC );
    messageType = XmlHandler.getTagValue( transformNode, MESSAGE_TYPE );
    messageField = XmlHandler.getTagValue( transformNode, MESSAGE_FIELD );
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
  public void setTopic( String topic ) {
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
  public void setMessageType( String messageType ) {
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
  public void setMessageField( String messageField ) {
    this.messageField = messageField;
  }
}
