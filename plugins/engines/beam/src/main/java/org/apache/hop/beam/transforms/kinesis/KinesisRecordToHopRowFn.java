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

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.StringToHopRowFn;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisRecordToHopRowFn extends DoFn<KinesisRecord, HopRow> {

  private final String rowMetaJson;
  private final String transformName;

  private final String uniqueIdField;
  private final String partitionKeyField;
  private final String sequenceNumberField;
  private final String subSequenceNumberField;
  private final String shardIdField;
  private final String streamNameField;

  private static final Logger LOG = LoggerFactory.getLogger(StringToHopRowFn.class);
  private final Counter numErrors = Metrics.counter("main", "BeamKinesisRecordToHopRowFnErrors");

  private transient IRowMeta rowMeta;
  private transient Counter inputCounter;
  private transient Counter writtenCounter;

  public KinesisRecordToHopRowFn(
      String transformName,
      String rowMetaJson,
      String uniqueIdField,
      String partitionKeyField,
      String sequenceNumberField,
      String subSequenceNumberField,
      String shardIdField,
      String streamNameField) {
    this.transformName = transformName;
    this.rowMetaJson = rowMetaJson;
    this.uniqueIdField = uniqueIdField;
    this.partitionKeyField = partitionKeyField;
    this.sequenceNumberField = sequenceNumberField;
    this.subSequenceNumberField = subSequenceNumberField;
    this.shardIdField = shardIdField;
    this.streamNameField = streamNameField;
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
      LOG.error("Error in setup of KinesisRecord to HopRow conversion function", e);
      throw new RuntimeException(
          "Error in setup of KinesisRecord to HopRow conversion function", e);
    }
  }

  @ProcessElement
  public void processElement(ProcessContext processContext) {
    try {
      KinesisRecord kinesisRecord = processContext.element();
      assert kinesisRecord != null;
      inputCounter.inc();

      Object[] outputRow = new Object[rowMeta.size()];
      int index = 0;
      if (StringUtils.isNotEmpty(uniqueIdField)) {
        String uniqueId = new String(kinesisRecord.getUniqueId(), StandardCharsets.UTF_8);
        outputRow[index++] = uniqueId;
      }

      // Data
      //
      String data = new String(kinesisRecord.getDataAsBytes(), StandardCharsets.UTF_8);
      outputRow[index++] = data;

      // Partition key
      //
      if (StringUtils.isNotEmpty(partitionKeyField)) {
        outputRow[index++] = kinesisRecord.getPartitionKey();
      }

      // Sequence Number field
      //
      if (StringUtils.isNotEmpty(sequenceNumberField)) {
        outputRow[index++] = kinesisRecord.getSequenceNumber();
      }

      // Sub-Sequence Number field (Integer)
      //
      if (StringUtils.isNotEmpty(subSequenceNumberField)) {
        outputRow[index++] = kinesisRecord.getSubSequenceNumber();
      }

      // Shard ID field
      //
      if (StringUtils.isNotEmpty(shardIdField)) {
        outputRow[index++] = kinesisRecord.getShardId();
      }

      // Stream name field
      //
      if (StringUtils.isNotEmpty(streamNameField)) {
        outputRow[index] = kinesisRecord.getStreamName();
      }

      processContext.output(new HopRow(outputRow));
      writtenCounter.inc();

    } catch (Exception e) {
      numErrors.inc();
      LOG.error("Error in KinesisRecord to HopRow conversion function", e);
      throw new RuntimeException("Error in KinesisRecord to HopRow conversion function", e);
    }
  }
}
