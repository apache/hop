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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

public class BeamKinesisProduceTransform extends PTransform<PCollection<HopRow>, PDone> {

  private String transformName;
  private String rowMetaJson;

  private String accessKey;
  private String secretKey;
  private String region;
  private String streamName;
  private String dataField;
  private String dataType;
  private String partitionKey;
  private List<KinesisConfigOption> configOptions;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger(BeamKinesisProduceTransform.class);
  private static final Counter numErrors = Metrics.counter("main", "BeamKafkaOutputError");

  public BeamKinesisProduceTransform() {
    configOptions = new ArrayList<>();
  }

  public BeamKinesisProduceTransform(
      String transformName,
      String rowMetaJson,
      String accessKey,
      String secretKey,
      String region,
      String streamName,
      String dataField,
      String dataType,
      String partitionKeyField,
      String[] configOptionParameters,
      String[] configOptionValues) {
    super(transformName);
    // These non-transient privates get serialized to spread across nodes
    //
    this.transformName = transformName;
    this.rowMetaJson = rowMetaJson;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.region = region;
    this.streamName = streamName;
    this.dataField = dataField;
    this.dataType = dataType;
    this.partitionKey = partitionKeyField;
    this.configOptions = new ArrayList<>();
    for (int i = 0; i < configOptionParameters.length; i++) {
      this.configOptions.add(
          new KinesisConfigOption(configOptionParameters[i], configOptionValues[i]));
    }
  }

  @Override
  public PDone expand(PCollection<HopRow> input) {

    try {
      // Only initialize once on this node/vm
      //
      BeamHop.init();

      // Inflate the metadata on the node where this is running...
      //
      IRowMeta rowMeta = JsonRowMeta.fromJson(rowMetaJson);

      int messageIndex = rowMeta.indexOfValue(dataField);
      if (messageIndex < 0) {
        throw new HopException(
            "Unable to find message field " + dataField + " in input row: " + rowMeta.toString());
      }

      if (!"String".equals(dataType)) {
        throw new HopException("For now, only Strings are supported as Kinesis data messages");
      }

      // Note: Producer properties are not directly supported in the new aws2 API
      // Custom configuration options would need to be set through ClientConfiguration
      // if needed in the future

      // Convert to PCollection of KV<String, byte[]> where key is partition key
      //
      int partitionKeyIndex = -1;
      if (StringUtils.isNotEmpty(partitionKey)) {
        partitionKeyIndex = rowMeta.indexOfValue(partitionKey);
        if (partitionKeyIndex < 0) {
          throw new HopException(
              "Unable to find partition key field "
                  + partitionKey
                  + " in input row: "
                  + rowMeta.toString());
        }
      }

      PCollection<KV<String, byte[]>> messages =
          input.apply(
              ParDo.of(
                  new HopRowToKVMessage(
                      transformName, rowMetaJson, messageIndex, partitionKeyIndex)));

      // Write to Kinesis stream with KV<String, byte[]>
      //
      StaticCredentialsProvider credentialsProvider =
          StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
      Region awsRegion = Region.of(region);

      ClientConfiguration clientConfig =
          ClientConfiguration.builder()
              .credentialsProvider(credentialsProvider)
              .region(awsRegion)
              .build();

      KinesisIO.Write<KV<String, byte[]>> write =
          KinesisIO.<KV<String, byte[]>>write()
              .withClientConfiguration(clientConfig)
              .withStreamName(streamName);

      messages.apply(write);
      return PDone.in(messages.getPipeline());
    } catch (Exception e) {
      numErrors.inc();
      LOG.error("Error in Beam Kinesis Produce transform", e);
      throw new RuntimeException("Error in Beam Kinesis Produce transform", e);
    }
  }

  // Convert HopRow to KV<String, byte[]> where key is partition key
  //
  private static class HopRowToKVMessage extends DoFn<HopRow, KV<String, byte[]>> {
    private final int messageIndex;
    private final int partitionKeyIndex;
    private final String transformName;
    private final String rowMetaJson;

    private transient IValueMeta valueMeta;
    private transient IValueMeta partitionKeyValueMeta;
    private transient Counter outputCounter;
    private transient Counter readCounter;

    public HopRowToKVMessage(
        String transformName, String rowMetaJson, int messageIndex, int partitionKeyIndex) {
      this.transformName = transformName;
      this.rowMetaJson = rowMetaJson;
      this.messageIndex = messageIndex;
      this.partitionKeyIndex = partitionKeyIndex;
    }

    @Setup
    public void setUp() {
      try {
        outputCounter = Metrics.counter(Pipeline.METRIC_NAME_OUTPUT, transformName);
        readCounter = Metrics.counter(Pipeline.METRIC_NAME_READ, transformName);

        // Initialize Hop Beam
        //
        BeamHop.init();
        IRowMeta rowMeta = JsonRowMeta.fromJson(rowMetaJson);
        valueMeta = rowMeta.getValueMeta(messageIndex);
        if (partitionKeyIndex >= 0) {
          partitionKeyValueMeta = rowMeta.getValueMeta(partitionKeyIndex);
        }

        Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
      } catch (Exception e) {
        LOG.error("Error in setup of HopRow to kinesis message conversion function", e);
        throw new RuntimeException(
            "Error in setup of HopRow to kinesis message conversion function", e);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {
      HopRow hopRow = processContext.element();
      readCounter.inc();
      assert hopRow != null;

      // Convert the underlying data type to a binary form.
      // Usually this means String to byte[] in UTF-8
      //
      try {
        byte[] message = valueMeta.getBinary(hopRow.getRow()[messageIndex]);

        // Extract partition key
        String partitionKeyValue = "";
        if (partitionKeyIndex >= 0) {
          partitionKeyValue = partitionKeyValueMeta.getString(hopRow.getRow()[partitionKeyIndex]);
          if (partitionKeyValue == null) {
            partitionKeyValue = "";
          }
        }

        processContext.output(KV.of(partitionKeyValue, message));
        outputCounter.inc();
      } catch (Exception e) {
        throw new RuntimeException(
            "Error converting message to a binary form, value nr "
                + messageIndex
                + " ("
                + valueMeta.getName()
                + ") in the input row",
            e);
      }
    }
  }
}
