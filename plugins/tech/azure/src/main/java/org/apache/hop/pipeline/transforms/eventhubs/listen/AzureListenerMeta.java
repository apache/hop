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
 */

package org.apache.hop.pipeline.transforms.eventhubs.listen;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "AzureListener",
    name = "i18n::AzureListenerMeta.name",
    description = "i18n::AzureListenerMeta.description",
    image = "event-hubs-listener.svg",
    categoryDescription = "i18n::AzureListenerMeta.categoryDescription",
    keywords = "i18n::AzureListenerMeta.keyword",
    documentationUrl = "/pipeline/transforms/azure-event-hubs-listener.html")
public class AzureListenerMeta extends BaseTransformMeta<AzureListener, AzureListenerData> {
  @HopMetadataProperty(key = "namespace")
  private String namespace;

  @HopMetadataProperty(key = "event_hub_name")
  private String eventHubName;

  @HopMetadataProperty(key = "sas_key_name")
  private String sasKeyName;

  @HopMetadataProperty(key = "sas_key", password = true)
  private String sasKey;

  @HopMetadataProperty(key = "consumer_group_name")
  private String consumerGroupName;

  @HopMetadataProperty(key = "storage_connection_string")
  private String storageConnectionString;

  @HopMetadataProperty(key = "storage_container_name")
  private String storageContainerName;

  @HopMetadataProperty(key = "prefetch_size")
  private String prefetchSize;

  @HopMetadataProperty(key = "batch_size")
  private String batchSize;

  @HopMetadataProperty(key = "output_field")
  private String outputField;

  @HopMetadataProperty(key = "partition_id_field")
  private String partitionIdField;

  @HopMetadataProperty(key = "offset_field")
  private String offsetField;

  @HopMetadataProperty(key = "sequence_number_field")
  private String sequenceNumberField;

  @HopMetadataProperty(key = "host_field")
  private String hostField;

  @HopMetadataProperty(key = "enqueued_time_field")
  private String enqueuedTimeField;

  @HopMetadataProperty(key = "batch_transformation")
  private String batchPipeline;

  @HopMetadataProperty(key = "batch_input_Transform")
  private String batchInputTransform;

  @HopMetadataProperty(key = "batch_output_Transform")
  private String batchOutputTransform;

  @HopMetadataProperty(key = "batch_max_wait_time")
  private String batchMaxWaitTime;

  public AzureListenerMeta() {
    super();
  }

  public AzureListenerMeta(AzureListenerMeta m) {
    this();
    this.namespace = m.namespace;
    this.eventHubName = m.eventHubName;
    this.sasKeyName = m.sasKeyName;
    this.sasKey = m.sasKey;
    this.consumerGroupName = m.consumerGroupName;
    this.storageConnectionString = m.storageConnectionString;
    this.storageContainerName = m.storageContainerName;
    this.prefetchSize = m.prefetchSize;
    this.batchSize = m.batchSize;
    this.outputField = m.outputField;
    this.partitionIdField = m.partitionIdField;
    this.offsetField = m.offsetField;
    this.sequenceNumberField = m.sequenceNumberField;
    this.hostField = m.hostField;
    this.enqueuedTimeField = m.enqueuedTimeField;
    this.batchPipeline = m.batchPipeline;
    this.batchInputTransform = m.batchInputTransform;
    this.batchOutputTransform = m.batchOutputTransform;
    this.batchMaxWaitTime = m.batchMaxWaitTime;
  }

  @Override
  public AzureListenerMeta clone() {
    return new AzureListenerMeta(this);
  }

  @Override
  public void setDefault() {
    consumerGroupName = "$Default";
    outputField = "message";
    partitionIdField = "partitionId";
    offsetField = "offset";
    sequenceNumberField = "sequenceNumber";
    hostField = "host";
    enqueuedTimeField = "enqueuedTime";
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (StringUtils.isNotEmpty(batchPipeline) && StringUtils.isNotEmpty(batchInputTransform)) {
      // Load the transformation, get the Transform output fields...
      //
      try {
        PipelineMeta batchTransMeta = loadBatchPipelineMeta(this, metadataProvider, variables);
        IRowMeta transformFields =
            batchTransMeta.getTransformFields(variables, variables.resolve(batchOutputTransform));
        rowMeta.clear();
        rowMeta.addRowMeta(transformFields);
        return;
      } catch (Exception e) {
        throw new HopTransformException(
            "Unable to get fields from batch pipeline Transform " + batchOutputTransform, e);
      }
    }

    getRegularRowMeta(rowMeta, variables);
  }

  public void getRegularRowMeta(IRowMeta rowMeta, IVariables variables) {
    // Output message field name
    //
    String outputFieldName = variables.resolve(outputField);
    if (StringUtils.isNotEmpty(outputFieldName)) {
      IValueMeta outputValueMeta = new ValueMetaString(outputFieldName);
      rowMeta.addValueMeta(outputValueMeta);
    }

    // The partition ID field name
    //
    String partitionIdFieldName = variables.resolve(partitionIdField);
    if (StringUtils.isNotEmpty(partitionIdFieldName)) {
      IValueMeta outputValueMeta = new ValueMetaString(partitionIdFieldName);
      rowMeta.addValueMeta(outputValueMeta);
    }

    // The offset field name
    //
    String offsetFieldName = variables.resolve(offsetField);
    if (StringUtils.isNotEmpty(offsetFieldName)) {
      IValueMeta outputValueMeta = new ValueMetaString(offsetFieldName);
      rowMeta.addValueMeta(outputValueMeta);
    }

    // The sequence number field name
    //
    String sequenceNumberFieldName = variables.resolve(sequenceNumberField);
    if (StringUtils.isNotEmpty(sequenceNumberFieldName)) {
      IValueMeta outputValueMeta = new ValueMetaInteger(sequenceNumberFieldName);
      rowMeta.addValueMeta(outputValueMeta);
    }

    // The host field name
    //
    String hostFieldName = variables.resolve(hostField);
    if (StringUtils.isNotEmpty(hostFieldName)) {
      IValueMeta outputValueMeta = new ValueMetaString(hostFieldName);
      rowMeta.addValueMeta(outputValueMeta);
    }

    // The enqueued time field name
    //
    String enqueuedTimeFieldName = variables.resolve(enqueuedTimeField);
    if (StringUtils.isNotEmpty(enqueuedTimeFieldName)) {
      IValueMeta outputValueMeta = new ValueMetaTimestamp(enqueuedTimeFieldName);
      rowMeta.addValueMeta(outputValueMeta);
    }
  }

  public static final synchronized PipelineMeta loadBatchPipelineMeta(
      AzureListenerMeta azureListenerMeta,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    PipelineMeta batchPipelineMeta;

    String realFilename = variables.resolve(azureListenerMeta.getBatchPipeline());
    try {
      // OK, load the meta-data from file...
      //
      // Don't set internal variables: they belong to the parent thread!
      //
      batchPipelineMeta = new PipelineMeta(realFilename, metadataProvider, variables);
    } catch (Exception e) {
      throw new HopException("Unable to load batch pipeline", e);
    }

    return batchPipelineMeta;
  }

  /**
   * @return The objects referenced in the Transform, like a mapping, a transformation, a job, ...
   */
  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] {
      "Batch pipeline",
    };
  }

  private boolean isPipelineDefined() {
    return StringUtils.isNotEmpty(batchPipeline);
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] {
      isPipelineDefined(),
    };
  }

  /**
   * Load the referenced object
   *
   * @param index the object index to load
   * @param variables the variable variables to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  @Override
  public IHasFilename loadReferencedObject(
      int index, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    return loadBatchPipelineMeta(this, metadataProvider, variables);
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

  public String getOutputField() {
    return outputField;
  }

  public void setOutputField(String outputField) {
    this.outputField = outputField;
  }

  public String getConsumerGroupName() {
    return consumerGroupName;
  }

  public void setConsumerGroupName(String consumerGroupName) {
    this.consumerGroupName = consumerGroupName;
  }

  public String getStorageConnectionString() {
    return storageConnectionString;
  }

  public void setStorageConnectionString(String storageConnectionString) {
    this.storageConnectionString = storageConnectionString;
  }

  public String getStorageContainerName() {
    return storageContainerName;
  }

  public void setStorageContainerName(String storageContainerName) {
    this.storageContainerName = storageContainerName;
  }

  public String getPrefetchSize() {
    return prefetchSize;
  }

  public void setPrefetchSize(String prefetchSize) {
    this.prefetchSize = prefetchSize;
  }

  public String getPartitionIdField() {
    return partitionIdField;
  }

  public void setPartitionIdField(String partitionIdField) {
    this.partitionIdField = partitionIdField;
  }

  public String getOffsetField() {
    return offsetField;
  }

  public void setOffsetField(String offsetField) {
    this.offsetField = offsetField;
  }

  public String getSequenceNumberField() {
    return sequenceNumberField;
  }

  public void setSequenceNumberField(String sequenceNumberField) {
    this.sequenceNumberField = sequenceNumberField;
  }

  public String getHostField() {
    return hostField;
  }

  public void setHostField(String hostField) {
    this.hostField = hostField;
  }

  public String getEnqueuedTimeField() {
    return enqueuedTimeField;
  }

  public void setEnqueuedTimeField(String enqueuedTimeField) {
    this.enqueuedTimeField = enqueuedTimeField;
  }

  public String getBatchPipeline() {
    return batchPipeline;
  }

  public void setBatchPipeline(String batchPipeline) {
    this.batchPipeline = batchPipeline;
  }

  public String getBatchInputTransform() {
    return batchInputTransform;
  }

  public void setBatchInputTransform(String batchInputTransform) {
    this.batchInputTransform = batchInputTransform;
  }

  public String getBatchOutputTransform() {
    return batchOutputTransform;
  }

  public void setBatchOutputTransform(String batchOutputTransform) {
    this.batchOutputTransform = batchOutputTransform;
  }

  public String getBatchMaxWaitTime() {
    return batchMaxWaitTime;
  }

  public void setBatchMaxWaitTime(String batchMaxWaitTime) {
    this.batchMaxWaitTime = batchMaxWaitTime;
  }
}
