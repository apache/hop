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

package org.apache.hop.beam.core.transform;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.HopRowToKVStringStringFn;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.transforms.kafka.ConfigOption;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BeamKafkaOutputTransform extends PTransform<PCollection<HopRow>, PDone> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String transformName;
  private String bootstrapServers;
  private String topic;
  private String keyField;
  private String messageField;
  private List<ConfigOption> configOptions;
  private String rowMetaJson;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger(BeamKafkaOutputTransform.class);
  private static final Counter numErrors = Metrics.counter("main", "BeamKafkaOutputError");

  public BeamKafkaOutputTransform() {}

  public BeamKafkaOutputTransform(
      String transformName,
      String bootstrapServers,
      String topic,
      String keyField,
      String messageField,
      String[] configOptionParameters,
      String[] configOptionValues,
      String[] configOptionTypes,
      String rowMetaJson,
      List<String> transformPluginClasses,
      List<String> xpPluginClasses) {
    super(transformName);
    this.transformName = transformName;
    this.bootstrapServers = bootstrapServers;
    this.topic = topic;
    this.keyField = keyField;
    this.messageField = messageField;
    this.rowMetaJson = rowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
    this.configOptions = new ArrayList<>();
    for (int i = 0; i < configOptionParameters.length; i++) {
      this.configOptions.add(
          new ConfigOption(
              configOptionParameters[i],
              configOptionValues[i],
              ConfigOption.Type.getTypeFromName(configOptionTypes[i])));
    }
  }

  @Override
  public PDone expand(PCollection<HopRow> input) {

    try {
      // Only initialize once on this node/vm
      //
      BeamHop.init(transformPluginClasses, xpPluginClasses);

      // Inflate the metadata on the node where this is running...
      //
      IRowMeta rowMeta = JsonRowMeta.fromJson(rowMetaJson);

      int keyIndex = rowMeta.indexOfValue(keyField);
      if (keyIndex < 0) {
        throw new HopException(
            "Unable to find key field " + keyField + " in input row: " + rowMeta.toString());
      }
      int messageIndex = rowMeta.indexOfValue(messageField);
      if (messageIndex < 0) {
        throw new HopException(
            "Unable to find message field "
                + messageField
                + " in input row: "
                + rowMeta.toString());
      }

      // Add custom configuration options to this map:
      Map<String, Object> producerConfigUpdates = new HashMap<>();
      for (ConfigOption configOption : configOptions) {
        Object value;
        String optionValue = configOption.getValue();
        switch (configOption.getType()) {
          case String:
            value = optionValue;
            break;
          case Short:
            value = Short.valueOf(optionValue);
            break;
          case Int:
            value = Integer.valueOf(optionValue);
            break;
          case Long:
            value = Long.valueOf(optionValue);
            break;
          case Double:
            value = Double.valueOf(optionValue);
            break;
          case Boolean:
            value = Boolean.valueOf(optionValue);
            break;
          default:
            throw new RuntimeException(
                "Config option parameter "
                    + configOption.getParameter()
                    + " uses unsupported type "
                    + configOption.getType().name());
        }
        producerConfigUpdates.put(configOption.getParameter(), value);
      }

      // Write to Kafka topic with <String, String> or <String, Object>
      //
      IValueMeta messageValueMeta = rowMeta.getValueMeta(messageIndex);
      if (messageValueMeta.getType()==IValueMeta.TYPE_STRING) {
        // Convert the input stream of HopRows to KV<String,String> for the keys and messages
        //
        HopRowToKVStringStringFn hopRowToKVStringStringFn =
                new HopRowToKVStringStringFn(
                        transformName,
                        keyIndex,
                        messageIndex,
                        rowMetaJson,
                        transformPluginClasses,
                        xpPluginClasses);

        // Then write to Kafka topic
        //
        KafkaIO.Write<String, String> stringsToKafka =
                KafkaIO.<String, String>write()
                        .withBootstrapServers(bootstrapServers)
                        .withTopic(topic)
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class)
                        .withProducerConfigUpdates(producerConfigUpdates);

        PCollection<KV<String, String>> kvpCollection =
                input.apply(ParDo.of(hopRowToKVStringStringFn));
        return kvpCollection.apply(stringsToKafka);
      } else if (messageValueMeta.getType()==IValueMeta.TYPE_AVRO) {
        // Convert the input stream of HopRows to KV<String,GenericRecord> for the keys and messages.
        //
        HopRowToKVStringGenericRecordFn hopRowToKVStringGenericRecordFn =
                new HopRowToKVStringGenericRecordFn(
                        transformName,
                        keyIndex,
                        messageIndex,
                        rowMetaJson,
                        transformPluginClasses,
                        xpPluginClasses);

        KafkaIO.Write<String, GenericRecord> stringsToKafka =
                KafkaIO.<String, GenericRecord>write()
                        .withBootstrapServers(bootstrapServers)
                        .withTopic(topic)
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer((Class)KafkaAvroSerializer.class)
                        .withProducerConfigUpdates(producerConfigUpdates);
        PCollection<KV<String, GenericRecord>> kvpCollection =
                input.apply(ParDo.of(hopRowToKVStringGenericRecordFn));
        return kvpCollection.apply(stringsToKafka);
      } else {
        throw new HopException("Hop only supports sending String or Avro Record values as Kafka messages");
      }
    } catch (Exception e) {
      numErrors.inc();
      LOG.error("Error in Beam Kafka output transform", e);
      throw new RuntimeException("Error in Beam Kafka output transform", e);
    }
  }


  private static final class GenericRecordCoder extends AtomicCoder<GenericRecord> {
    public static GenericRecordCoder of() {
      return new GenericRecordCoder();
    }

    @Override
    public void encode(GenericRecord value, OutputStream outStream) throws IOException {
      String schemaString = value.getSchema().toString();
      StringUtf8Coder.of().encode(schemaString, outStream);
      AvroCoder<GenericRecord> coder = AvroCoder.of(value.getSchema());
      coder.encode(value, outStream);
    }

    @Override
    public GenericRecord decode(InputStream inStream) throws IOException {
      String schemaString = StringUtf8Coder.of().decode(inStream);
      AvroCoder<GenericRecord> coder = AvroCoder.of(new Schema.Parser().parse(schemaString));
      return coder.decode(inStream);
    }
  }

    private static final class HopRowToKVStringGenericRecordFn extends DoFn<HopRow, KV<String, GenericRecord>> {

    private String rowMetaJson;
    private String transformName;
    private int keyIndex;
    private int valueIndex;
    private List<String> transformPluginClasses;
    private List<String> xpPluginClasses;

    private static final Logger LOG = LoggerFactory.getLogger(HopRowToKVStringGenericRecordFn.class);
    private final Counter numErrors = Metrics.counter("main", "BeamKafkaProducerTransformErrors");

    private IRowMeta rowMeta;
    private transient Counter inputCounter;
    private transient Counter writtenCounter;

    public HopRowToKVStringGenericRecordFn(
            String transformName,
            int keyIndex,
            int valueIndex,
            String rowMetaJson,
            List<String> transformPluginClasses,
            List<String> xpPluginClasses) {
      this.transformName = transformName;
      this.keyIndex = keyIndex;
      this.valueIndex = valueIndex;
      this.rowMetaJson = rowMetaJson;
      this.transformPluginClasses = transformPluginClasses;
      this.xpPluginClasses = xpPluginClasses;
    }

    @Setup
    public void setUp() {
      try {
        inputCounter = Metrics.counter(Pipeline.METRIC_NAME_INPUT, transformName);
        writtenCounter = Metrics.counter(Pipeline.METRIC_NAME_WRITTEN, transformName);

        // Initialize Hop Beam
        //
        BeamHop.init(transformPluginClasses, xpPluginClasses);
        rowMeta = JsonRowMeta.fromJson(rowMetaJson);

        Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
      } catch (Exception e) {
        numErrors.inc();
        LOG.error("Error in setup of HopRow to KV<String,GenericRecord> function", e);
        throw new RuntimeException("Error in setup of HopRow to KV<String,GenericRecord> function", e);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {
      try {
        HopRow hopRow = processContext.element();
        inputCounter.inc();

        String key = rowMeta.getString(hopRow.getRow(), keyIndex);
        GenericRecord value = (GenericRecord) hopRow.getRow()[valueIndex];

        processContext.output(KV.of(key, value));
        writtenCounter.inc();

      } catch (Exception e) {
        numErrors.inc();
        LOG.error("Error in HopRow to KV<String,GenericRecord> function", e);
        throw new RuntimeException("Error in HopRow to KV<String,GenericRecord> function", e);
      }
    }
  }
}
