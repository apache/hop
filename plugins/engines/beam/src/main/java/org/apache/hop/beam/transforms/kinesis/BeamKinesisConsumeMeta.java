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
 *
 */

package org.apache.hop.beam.transforms.kinesis;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyData;

@Transform(
    id = "BeamKinesisConsume",
    name = "i18n::BeamKinesisConsumeMeta.name",
    description = "i18n::BeamKinesisConsumeMeta.description",
    image = "beam-kinesis-consume.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
    keywords = "i18n::BeamKinesisConsumeMeta.keyword",
    documentationUrl = "/pipeline/transforms/beamkinesisconsume.html")
public class BeamKinesisConsumeMeta extends BaseTransformMeta<BeamKinesisConsume, DummyData>
    implements IBeamPipelineTransformHandler {

  @HopMetadataProperty(key = "access_key")
  private String accessKey;

  @HopMetadataProperty(key = "secret_key", password = true)
  private String secretKey;

  @HopMetadataProperty(key = "stream_name", password = true)
  private String streamName;

  @HopMetadataProperty(key = "unique_id_field")
  private String uniqueIdField;

  @HopMetadataProperty(key = "data_field")
  private String dataField;

  @HopMetadataProperty(key = "message_type")
  private String dataType;

  @HopMetadataProperty(key = "partition_key_field")
  private String partitionKeyField;

  @HopMetadataProperty(key = "sequence_number_field")
  private String sequenceNumberField;

  @HopMetadataProperty(key = "sub_sequence_number_field")
  private String subSequenceNumberField;

  @HopMetadataProperty(key = "shard_id_field")
  private String shardIdField;

  @HopMetadataProperty(key = "stream_name_field")
  private String streamNameField;

  // Detailed options

  @HopMetadataProperty(key = "max_num_records")
  private String maxNumRecords;

  @HopMetadataProperty(key = "max_read_time_ms")
  private String maxReadTimeMs;

  @HopMetadataProperty(key = "up_to_date_threshold_ms")
  private String upToDateThresholdMs;

  @HopMetadataProperty(key = "request_records_limit")
  private String requestRecordsLimit;

  @HopMetadataProperty(key = "arrival_time_watermark_polity")
  private boolean arrivalTimeWatermarkPolicy;

  @HopMetadataProperty(key = "arrival_time_watermark_polity_ms")
  private String arrivalTimeWatermarkPolicyMs;

  @HopMetadataProperty(key = "processing_time_watermark_polity")
  private boolean processingTimeWatermarkPolicy;

  @HopMetadataProperty(key = "fixed_delay_rate_polity")
  private boolean fixedDelayRatePolicy;

  @HopMetadataProperty(key = "fixed_delay_rate_polity_ms")
  private String fixedDelayRatePolicyMs;

  @HopMetadataProperty(key = "max_capacity_per_shard")
  private String maxCapacityPerShard;

  public BeamKinesisConsumeMeta() {
    super();
    streamName = "stream";
    uniqueIdField = "id";
    dataField = "data";
    dataType = "String";
    partitionKeyField = "partitionKey";
    sequenceNumberField = "sequenceNumber";
    subSequenceNumberField = "SubSequenceNumber";
    shardIdField = "shardId";
    streamNameField = "streamName";
  }

  @Override
  public String getDialogClassName() {
    return BeamKinesisConsumeDialog.class.getName();
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      // The unique ID field as a String
      //
      String uniqueId = variables.resolve(uniqueIdField);
      if (StringUtils.isNotEmpty(uniqueId)) {
        IValueMeta valueMeta = new ValueMetaString(uniqueId);
        valueMeta.setOrigin(origin);
        inputRowMeta.addValueMeta(valueMeta);
      }

      // The default data type is String
      //
      String typeString = Const.NVL(variables.resolve(dataType), "String");
      int type = ValueMetaFactory.getIdForValueMeta(typeString);
      IValueMeta messageValueMeta =
          ValueMetaFactory.createValueMeta(variables.resolve(dataField), type);
      messageValueMeta.setOrigin(origin);
      inputRowMeta.addValueMeta(messageValueMeta);

      // Partition key
      //
      String partitionKey = variables.resolve(partitionKeyField);
      if (StringUtils.isNotEmpty(partitionKey)) {
        IValueMeta valueMeta = new ValueMetaString(partitionKey);
        valueMeta.setOrigin(origin);
        inputRowMeta.addValueMeta(valueMeta);
      }

      // Sequence Number field
      //
      String sequenceNumber = variables.resolve(sequenceNumberField);
      if (StringUtils.isNotEmpty(sequenceNumber)) {
        IValueMeta valueMeta = new ValueMetaString(sequenceNumber);
        valueMeta.setOrigin(origin);
        inputRowMeta.addValueMeta(valueMeta);
      }

      // Sub-Sequence Number field
      //
      String subSequenceNumber = variables.resolve(subSequenceNumberField);
      if (StringUtils.isNotEmpty(subSequenceNumber)) {
        IValueMeta valueMeta = new ValueMetaInteger(subSequenceNumber);
        valueMeta.setOrigin(origin);
        inputRowMeta.addValueMeta(valueMeta);
      }

      // Shard ID field
      //
      String shardId = variables.resolve(shardIdField);
      if (StringUtils.isNotEmpty(shardId)) {
        IValueMeta valueMeta = new ValueMetaString(shardId);
        valueMeta.setOrigin(origin);
        inputRowMeta.addValueMeta(valueMeta);
      }

      // Stream name field
      //
      String fieldName = variables.resolve(streamNameField);
      if (StringUtils.isNotEmpty(fieldName)) {
        IValueMeta valueMeta = new ValueMetaString(fieldName);
        valueMeta.setOrigin(origin);
        inputRowMeta.addValueMeta(valueMeta);
      }

    } catch (Exception e) {
      throw new HopTransformException("Error calculating transform output field layout", e);
    }
  }

  @Override
  public boolean isInput() {
    return true;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      IBeamPipelineEngineRunConfiguration runConfiguration,
      String dataSamplersJson,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, PCollection<HopRow>> transformCollectionMap,
      org.apache.beam.sdk.Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input,
      String parentLogChannelId)
      throws HopException {

    // Output rows (fields selection)
    //
    IRowMeta outputRowMeta = new RowMeta();
    getFields(outputRowMeta, transformMeta.getName(), null, null, variables, null);

    BeamKinesisConsumeTransform beamInputTransform =
        new BeamKinesisConsumeTransform(
            transformMeta.getName(),
            variables.resolve(accessKey),
            variables.resolve(secretKey),
            "us-east-1", // TODO : make configurable
            JsonRowMeta.toJson(outputRowMeta),
            variables.resolve(streamName),
            variables.resolve(uniqueIdField),
            variables.resolve(dataField),
            variables.resolve(dataType),
            variables.resolve(partitionKeyField),
            variables.resolve(sequenceNumberField),
            variables.resolve(subSequenceNumberField),
            variables.resolve(shardIdField),
            variables.resolve(streamNameField),
            variables.resolve(maxNumRecords),
            variables.resolve(maxReadTimeMs),
            variables.resolve(upToDateThresholdMs),
            variables.resolve(requestRecordsLimit),
            arrivalTimeWatermarkPolicy,
            variables.resolve(arrivalTimeWatermarkPolicyMs),
            processingTimeWatermarkPolicy,
            fixedDelayRatePolicy,
            variables.resolve(fixedDelayRatePolicyMs),
            variables.resolve(maxCapacityPerShard));
    PCollection<HopRow> afterInput = pipeline.apply(beamInputTransform);
    transformCollectionMap.put(transformMeta.getName(), afterInput);
    log.logBasic("Handled transform (KINESIS CONSUME) : " + transformMeta.getName());
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  public String getStreamName() {
    return streamName;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public String getUniqueIdField() {
    return uniqueIdField;
  }

  public void setUniqueIdField(String uniqueIdField) {
    this.uniqueIdField = uniqueIdField;
  }

  public String getDataField() {
    return dataField;
  }

  public void setDataField(String dataField) {
    this.dataField = dataField;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public String getPartitionKeyField() {
    return partitionKeyField;
  }

  public void setPartitionKeyField(String partitionKeyField) {
    this.partitionKeyField = partitionKeyField;
  }

  public String getSequenceNumberField() {
    return sequenceNumberField;
  }

  public void setSequenceNumberField(String sequenceNumberField) {
    this.sequenceNumberField = sequenceNumberField;
  }

  public String getSubSequenceNumberField() {
    return subSequenceNumberField;
  }

  public void setSubSequenceNumberField(String subSequenceNumberField) {
    this.subSequenceNumberField = subSequenceNumberField;
  }

  public String getShardIdField() {
    return shardIdField;
  }

  public void setShardIdField(String shardIdField) {
    this.shardIdField = shardIdField;
  }

  public String getStreamNameField() {
    return streamNameField;
  }

  public void setStreamNameField(String streamNameField) {
    this.streamNameField = streamNameField;
  }

  public String getMaxNumRecords() {
    return maxNumRecords;
  }

  public void setMaxNumRecords(String maxNumRecords) {
    this.maxNumRecords = maxNumRecords;
  }

  public String getMaxReadTimeMs() {
    return maxReadTimeMs;
  }

  public void setMaxReadTimeMs(String maxReadTimeMs) {
    this.maxReadTimeMs = maxReadTimeMs;
  }

  public String getUpToDateThresholdMs() {
    return upToDateThresholdMs;
  }

  public void setUpToDateThresholdMs(String upToDateThresholdMs) {
    this.upToDateThresholdMs = upToDateThresholdMs;
  }

  public String getRequestRecordsLimit() {
    return requestRecordsLimit;
  }

  public void setRequestRecordsLimit(String requestRecordsLimit) {
    this.requestRecordsLimit = requestRecordsLimit;
  }

  public boolean isArrivalTimeWatermarkPolicy() {
    return arrivalTimeWatermarkPolicy;
  }

  public void setArrivalTimeWatermarkPolicy(boolean arrivalTimeWatermarkPolicy) {
    this.arrivalTimeWatermarkPolicy = arrivalTimeWatermarkPolicy;
  }

  public String getArrivalTimeWatermarkPolicyMs() {
    return arrivalTimeWatermarkPolicyMs;
  }

  public void setArrivalTimeWatermarkPolicyMs(String arrivalTimeWatermarkPolicyMs) {
    this.arrivalTimeWatermarkPolicyMs = arrivalTimeWatermarkPolicyMs;
  }

  public boolean isProcessingTimeWatermarkPolicy() {
    return processingTimeWatermarkPolicy;
  }

  public void setProcessingTimeWatermarkPolicy(boolean processingTimeWatermarkPolicy) {
    this.processingTimeWatermarkPolicy = processingTimeWatermarkPolicy;
  }

  public boolean isFixedDelayRatePolicy() {
    return fixedDelayRatePolicy;
  }

  public void setFixedDelayRatePolicy(boolean fixedDelayRatePolicy) {
    this.fixedDelayRatePolicy = fixedDelayRatePolicy;
  }

  public String getFixedDelayRatePolicyMs() {
    return fixedDelayRatePolicyMs;
  }

  public void setFixedDelayRatePolicyMs(String fixedDelayRatePolicyMs) {
    this.fixedDelayRatePolicyMs = fixedDelayRatePolicyMs;
  }

  public String getMaxCapacityPerShard() {
    return maxCapacityPerShard;
  }

  public void setMaxCapacityPerShard(String maxCapacityPerShard) {
    this.maxCapacityPerShard = maxCapacityPerShard;
  }
}
