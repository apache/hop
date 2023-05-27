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
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "AzureWriter",
    name = "i18n::AzureWriterMeta.name",
    description = "i18n::AzureWriterMeta.description",
    image = "event-hubs-writer.svg",
    categoryDescription = "i18n::AzureWriterMeta.categoryDescription",
    keywords = "i18n::AzureWriterMeta.keyword",
    documentationUrl = "/pipeline/transforms/azure-event-hubs-writer.html")
public class AzureWriterMeta extends BaseTransformMeta<AzureWrite, AzureWriterData> {
  @HopMetadataProperty(key = "namespace")
  private String namespace;

  @HopMetadataProperty(key = "event_hub_name")
  private String eventHubName;

  @HopMetadataProperty(key = "sas_key_name")
  private String sasKeyName;

  @HopMetadataProperty(key = "sas_key", password = true)
  private String sasKey;

  @HopMetadataProperty(key = "batch_size")
  private String batchSize;

  @HopMetadataProperty(key = "message_field")
  private String messageField;

  public AzureWriterMeta() {
    super();
  }

  public AzureWriterMeta(AzureWriterMeta m) {
    this.namespace = m.namespace;
    this.eventHubName = m.eventHubName;
    this.sasKeyName = m.sasKeyName;
    this.sasKey = m.sasKey;
    this.batchSize = m.batchSize;
    this.messageField = m.messageField;
  }

  @Override
  public AzureWriterMeta clone() {
    return new AzureWriterMeta(this);
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
