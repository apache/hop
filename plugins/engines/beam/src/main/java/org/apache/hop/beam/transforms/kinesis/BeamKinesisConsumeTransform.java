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

package org.apache.hop.beam.transforms.kinesis;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.common.InitialPositionInStream;

public class BeamKinesisConsumeTransform extends PTransform<PBegin, PCollection<HopRow>> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String transformName;
  private String rowMetaJson;

  private String accessKey;
  private String secretKey;
  private String region;
  private String streamName;
  private String uniqueIdField;
  private String dataField;
  private String dataType;
  private String partitionKeyField;
  private String sequenceNumberField;
  private String subSequenceNumberField;
  private String shardIdField;
  private String streamNameField;
  private String maxNumRecords;
  private String maxReadTimeMs;
  private String upToDateThresholdMs;
  private String requestRecordsLimit;
  private boolean arrivalTimeWatermarkPolicy;
  private String arrivalTimeWatermarkPolicyMs;
  private boolean processingTimeWatermarkPolicy;
  private boolean fixedDelayRatePolicy;
  private String fixedDelayRatePolicyMs;
  private String maxCapacityPerShard;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger(BeamKinesisConsumeTransform.class);
  private static final Counter numErrors = Metrics.counter("main", "BeamKinesisConsumerError");

  public BeamKinesisConsumeTransform() {}

  public BeamKinesisConsumeTransform(
      String transformName,
      String accessKey,
      String secretKey,
      String region,
      String rowMetaJson,
      String streamName,
      String uniqueIdField,
      String dataField,
      String dataType,
      String partitionKeyField,
      String sequenceNumberField,
      String subSequenceNumberField,
      String shardIdField,
      String streamNameField,
      String maxNumRecords,
      String maxReadTimeMs,
      String upToDateThresholdMs,
      String requestRecordsLimit,
      boolean arrivalTimeWatermarkPolicy,
      String arrivalTimeWatermarkPolicyMs,
      boolean processingTimeWatermarkPolicy,
      boolean fixedDelayRatePolicy,
      String fixedDelayRatePolicyMs,
      String maxCapacityPerShard) {
    super(transformName);
    this.transformName = transformName;
    this.rowMetaJson = rowMetaJson;
    this.streamName = streamName;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.region = region;
    this.uniqueIdField = uniqueIdField;
    this.dataField = dataField;
    this.dataType = dataType;
    this.partitionKeyField = partitionKeyField;
    this.sequenceNumberField = sequenceNumberField;
    this.subSequenceNumberField = subSequenceNumberField;
    this.shardIdField = shardIdField;
    this.streamNameField = streamNameField;

    this.maxNumRecords = maxNumRecords;
    this.maxReadTimeMs = maxReadTimeMs;
    this.upToDateThresholdMs = upToDateThresholdMs;
    this.requestRecordsLimit = requestRecordsLimit;
    this.arrivalTimeWatermarkPolicy = arrivalTimeWatermarkPolicy;
    this.arrivalTimeWatermarkPolicyMs = arrivalTimeWatermarkPolicyMs;
    this.processingTimeWatermarkPolicy = processingTimeWatermarkPolicy;
    this.fixedDelayRatePolicy = fixedDelayRatePolicy;
    this.fixedDelayRatePolicyMs = fixedDelayRatePolicyMs;
    this.maxCapacityPerShard = maxCapacityPerShard;

    // TODO: The Avro Record use case.
    // Figure out how to use AWS Glue Schema Registry
    // For now only String data type is supported
    //
    if (!"String".equalsIgnoreCase(dataType)) {
      throw new RuntimeException("Only String messages are supported at this time");
    }
  }

  @Override
  public PCollection<HopRow> expand(PBegin input) {
    try {
      // Only initialize once on this node/vm
      //
      BeamHop.init();

      PCollection<HopRow> output;

      StaticCredentialsProvider credentialsProvider =
          StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
      Region awsRegion = Region.of(region);

      ClientConfiguration clientConfig =
          ClientConfiguration.builder()
              .credentialsProvider(credentialsProvider)
              .region(awsRegion)
              .build();

      KinesisIO.Read kinesisRecordRead =
          KinesisIO.read()
              .withClientConfiguration(clientConfig)
              .withStreamName(streamName)
              .withInitialPositionInStream(InitialPositionInStream.LATEST) // TODO make configurable
          ;

      if (StringUtils.isNotEmpty(maxNumRecords)) {
        kinesisRecordRead = kinesisRecordRead.withMaxNumRecords(Long.parseLong(maxNumRecords));
      }

      if (StringUtils.isNotEmpty(maxReadTimeMs)) {
        kinesisRecordRead =
            kinesisRecordRead.withMaxReadTime(Duration.millis(Long.parseLong(maxReadTimeMs)));
      }

      if (StringUtils.isNotEmpty(upToDateThresholdMs)) {
        kinesisRecordRead =
            kinesisRecordRead.withUpToDateThreshold(
                Duration.millis(Long.parseLong(upToDateThresholdMs)));
      }

      if (StringUtils.isNotEmpty(requestRecordsLimit)) {
        kinesisRecordRead =
            kinesisRecordRead.withRequestRecordsLimit(Integer.parseInt(requestRecordsLimit));
      }

      if (arrivalTimeWatermarkPolicy) {
        kinesisRecordRead = kinesisRecordRead.withArrivalTimeWatermarkPolicy();
        if (StringUtils.isNotEmpty(arrivalTimeWatermarkPolicyMs)) {
          kinesisRecordRead =
              kinesisRecordRead.withArrivalTimeWatermarkPolicy(
                  Duration.millis(Long.parseLong(arrivalTimeWatermarkPolicyMs)));
        }
      }

      if (processingTimeWatermarkPolicy) {
        kinesisRecordRead = kinesisRecordRead.withProcessingTimeWatermarkPolicy();
      }

      if (fixedDelayRatePolicy) {
        kinesisRecordRead = kinesisRecordRead.withFixedDelayRateLimitPolicy();
        if (StringUtils.isNotEmpty(fixedDelayRatePolicyMs)) {
          kinesisRecordRead =
              kinesisRecordRead.withFixedDelayRateLimitPolicy(
                  Duration.millis(Long.parseLong(fixedDelayRatePolicyMs)));
        }
      }

      if (StringUtils.isNotEmpty(maxCapacityPerShard)) {
        kinesisRecordRead =
            kinesisRecordRead.withMaxCapacityPerShard(Integer.parseInt(maxCapacityPerShard));
      }

      PCollection<KinesisRecord> kinesisRecordPCollection = input.apply(kinesisRecordRead);

      KinesisRecordToHopRowFn recordToRowFn =
          new KinesisRecordToHopRowFn(
              transformName,
              rowMetaJson,
              uniqueIdField,
              partitionKeyField,
              sequenceNumberField,
              subSequenceNumberField,
              shardIdField,
              streamNameField);
      output = kinesisRecordPCollection.apply(ParDo.of(recordToRowFn));

      return output;

    } catch (Exception e) {
      numErrors.inc();
      LOG.error("Error in Kafka input transform", e);
      throw new RuntimeException("Error in Kafka input transform", e);
    }
  }

  public static final class KVStringStringToHopRowFn extends DoFn<KV<String, String>, HopRow> {

    private final String rowMetaJson;
    private final String transformName;

    private final Logger LOG = LoggerFactory.getLogger(KVStringStringToHopRowFn.class);
    private final Counter numErrors = Metrics.counter("main", "BeamSubscribeTransformErrors");

    private IRowMeta rowMeta;
    private transient Counter inputCounter;
    private transient Counter writtenCounter;

    public KVStringStringToHopRowFn(String transformName, String rowMetaJson) {
      this.transformName = transformName;
      this.rowMetaJson = rowMetaJson;
    }

    @Setup
    public void setUp() {
      try {
        inputCounter = Metrics.counter(Pipeline.METRIC_NAME_INPUT, transformName);
        writtenCounter = Metrics.counter(Pipeline.METRIC_NAME_WRITTEN, transformName);

        // Initialize Hop Beam
        //
        BeamHop.init();
        rowMeta = JsonRowMeta.fromJson(rowMetaJson);

        Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
      } catch (Exception e) {
        numErrors.inc();
        LOG.error("Error in setup of KV<String,String> to Hop Row conversion function", e);
        throw new RuntimeException(
            "Error in setup of KV<String,String> to Hop Row conversion function", e);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {
      try {
        KV<String, String> kv = processContext.element();
        inputCounter.inc();

        Object[] outputRow = new Object[rowMeta.size()];
        outputRow[0] = kv.getKey(); // String
        outputRow[1] = kv.getValue(); // String

        processContext.output(new HopRow(outputRow));
        writtenCounter.inc();
      } catch (Exception e) {
        numErrors.inc();
        LOG.error("Error in KV<String,String> to Hop Row conversion function", e);
        throw new RuntimeException("Error in KV<String,String> to Hop Row conversion function", e);
      }
    }
  }

  public static final class KVStringGenericRecordToHopRowFn
      extends DoFn<KV<String, GenericRecord>, HopRow> {

    private final String rowMetaJson;
    private final String transformName;

    private final Logger LOG = LoggerFactory.getLogger(KVStringGenericRecordToHopRowFn.class);
    private final Counter numErrors = Metrics.counter("main", "BeamSubscribeTransformErrors");

    private IRowMeta rowMeta;
    private transient Counter inputCounter;
    private transient Counter writtenCounter;

    public KVStringGenericRecordToHopRowFn(String transformName, String rowMetaJson) {
      this.transformName = transformName;
      this.rowMetaJson = rowMetaJson;
    }

    @Setup
    public void setUp() {
      try {
        inputCounter = Metrics.counter(Pipeline.METRIC_NAME_INPUT, transformName);
        writtenCounter = Metrics.counter(Pipeline.METRIC_NAME_WRITTEN, transformName);

        // Initialize Hop Beam
        //
        BeamHop.init();
        rowMeta = JsonRowMeta.fromJson(rowMetaJson);

        Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
      } catch (Exception e) {
        numErrors.inc();
        LOG.error("Error in setup of KV<String,GenericRecord> to Hop Row conversion function", e);
        throw new RuntimeException(
            "Error in setup of KV<String,GenericRecord> to Hop Row conversion function", e);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {
      try {
        KV<String, GenericRecord> kv = processContext.element();
        inputCounter.inc();

        Object[] outputRow = new Object[rowMeta.size()];
        outputRow[0] = kv.getKey(); // String
        outputRow[1] = kv.getValue(); // Avro Record (GenericRecord)

        processContext.output(new HopRow(outputRow));
        writtenCounter.inc();
      } catch (Exception e) {
        numErrors.inc();
        LOG.error("Error in KV<String,GenericRecord> to Hop Row conversion function", e);
        throw new RuntimeException(
            "Error in KV<String,GenericRecord> to Hop Row conversion function", e);
      }
    }
  }
}
