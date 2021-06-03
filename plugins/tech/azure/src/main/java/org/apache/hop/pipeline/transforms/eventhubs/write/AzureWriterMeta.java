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
 *
 */

package org.apache.hop.pipeline.transforms.eventhubs.write;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "AzureWriter",
    name = "Azure Event Hubs Writer",
    description = "Write data to a Microsoft Azure Event Hub",
    image = "event-hubs-writer.svg",
    categoryDescription = "Streaming",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/azure-event-hubs-writer.html")
public class AzureWriterMeta extends BaseTransformMeta
    implements ITransformMeta<AzureWrite, AzureWriterData> {

  public static final String NAMESPACE = "namespace";
  public static final String EVENT_HUB_NAME = "event_hub_name";
  public static final String SAS_KEY_NAME = "sas_key_name";
  public static final String SAS_KEY = "sas_key";
  public static final String BATCH_SIZE = "batch_size";
  public static final String MESSAGE_FIELD = "message_field";

  private String namespace;
  private String eventHubName;
  private String sasKeyName;
  private String sasKey;

  private String batchSize;

  private String messageField;

  public AzureWriterMeta() {
    super();
  }

  @Override
  public void setDefault() {}

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      AzureWriterData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new AzureWrite(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public AzureWriterData getTransformData() {
    return new AzureWriterData();
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
    // No extra or fewer output fields for now
  }

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder();
    xml.append(XmlHandler.addTagValue(NAMESPACE, namespace));
    xml.append(XmlHandler.addTagValue(EVENT_HUB_NAME, eventHubName));
    xml.append(XmlHandler.addTagValue(SAS_KEY_NAME, sasKeyName));
    xml.append(XmlHandler.addTagValue(SAS_KEY, Encr.encryptPasswordIfNotUsingVariables(sasKey)));
    xml.append(XmlHandler.addTagValue(BATCH_SIZE, batchSize));
    xml.append(XmlHandler.addTagValue(MESSAGE_FIELD, messageField));
    return xml.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    namespace = XmlHandler.getTagValue(transformNode, NAMESPACE);
    eventHubName = XmlHandler.getTagValue(transformNode, EVENT_HUB_NAME);
    sasKeyName = XmlHandler.getTagValue(transformNode, SAS_KEY_NAME);
    sasKey =
        Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(transformNode, SAS_KEY));
    batchSize = XmlHandler.getTagValue(transformNode, BATCH_SIZE);
    messageField = XmlHandler.getTagValue(transformNode, MESSAGE_FIELD);

    super.loadXml(transformNode, metadataProvider);
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getEventHubName() {
    return eventHubName;
  }

  public void setEventHubName(String eventHubName) {
    this.eventHubName = eventHubName;
  }

  public String getSasKeyName() {
    return sasKeyName;
  }

  public void setSasKeyName(String sasKeyName) {
    this.sasKeyName = sasKeyName;
  }

  public String getSasKey() {
    return sasKey;
  }

  public void setSasKey(String sasKey) {
    this.sasKey = sasKey;
  }

  public String getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(String batchSize) {
    this.batchSize = batchSize;
  }

  public String getMessageField() {
    return messageField;
  }

  public void setMessageField(String messageField) {
    this.messageField = messageField;
  }
}
