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

package org.apache.hop.beam.transforms.debezium;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.joda.time.Duration;
import org.junit.jupiter.api.Test;

public class BeamDebeziumInputTransformTest {
  @Test
  void actualDebeziumIoReadsJsonRowsWithRecordLimitAndHopMetrics() throws Exception {
    BeamDebeziumInputMeta meta = new BeamDebeziumInputMeta();
    meta.setUsername("fixture");
    meta.setConnector("Custom");
    meta.setConnectorClass(FixtureConnector.class.getName());
    meta.setMaxRecords("2");
    meta.setMaxTimeMs("3000");
    meta.setPollingTimeoutMs("10");
    StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
    assertFalse(options.isStreaming());
    Pipeline pipeline = Pipeline.create(options);
    Map<String, PCollection<HopRow>> collections = new HashMap<>();
    meta.handleTransform(
        LogChannel.GENERAL,
        new Variables(),
        "direct",
        null,
        null,
        null,
        new PipelineMeta(),
        new TransformMeta("BeamDebeziumInput", "cdc", meta),
        collections,
        pipeline,
        new RowMeta(),
        List.of(),
        null,
        null);
    PCollection<HopRow> rows = collections.get("cdc");
    assertNotNull(rows);
    assertTrue(
        options.isStreaming(), "Debezium SDF must enable streaming even with termination limits");
    assertInstanceOf(HopRowCoder.class, rows.getCoder());
    // Beam's Debezium SDF remains unbounded even when record/time termination limits are set.
    assertEquals(PCollection.IsBounded.UNBOUNDED, rows.isBounded());
    PAssert.that(rows)
        .satisfies(
            actual -> {
              int count = 0;
              for (HopRow row : actual) {
                assertEquals(1, row.length());
                String json = (String) row.getRow()[0];
                assertTrue(json.contains("\"connector\":\"postgresql\""), json);
                assertTrue(json.contains("\"after\":{\"fields\":{\"id\":"), json);
                count++;
              }
              assertEquals(2, count);
              return null;
            });
    PipelineResult result = pipeline.run();
    try {
      assertEquals(PipelineResult.State.DONE, result.waitUntilFinish(Duration.standardSeconds(30)));
      assertEquals(2, counter(result, org.apache.hop.pipeline.Pipeline.METRIC_NAME_INPUT));
      assertEquals(2, counter(result, org.apache.hop.pipeline.Pipeline.METRIC_NAME_WRITTEN));
    } finally {
      if (!result.getState().isTerminal()) {
        result.cancel();
      }
    }
  }

  @Test
  void incomingRowsAreRejectedInsteadOfSilentlyIgnored() {
    BeamDebeziumInputMeta meta = new BeamDebeziumInputMeta();
    meta.setUsername("fixture");
    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> incoming =
        pipeline.apply(
            org.apache.beam.sdk.transforms.Create.of(new HopRow(new Object[] {"incoming"}))
                .withCoder(new HopRowCoder()));
    var collections = new HashMap<String, PCollection<HopRow>>();
    var error =
        assertThrows(
            org.apache.hop.core.exception.HopException.class,
            () ->
                meta.handleTransform(
                    LogChannel.GENERAL,
                    new Variables(),
                    "direct",
                    null,
                    null,
                    null,
                    new PipelineMeta(),
                    new TransformMeta("BeamDebeziumInput", "cdc", meta),
                    collections,
                    pipeline,
                    new RowMeta(),
                    List.of(new TransformMeta()),
                    incoming,
                    null));
    assertTrue(error.getMessage().contains("incoming"));
    assertTrue(collections.isEmpty());
    assertFalse(pipeline.getOptions().as(StreamingOptions.class).isStreaming());
  }

  @Test
  void sparkIsRejectedBeforeSubmissionBecauseItsStreamingRunnerDoesNotSupportSplittableDoFn() {
    BeamDebeziumInputMeta meta = new BeamDebeziumInputMeta();
    meta.setUsername("cdc");
    var options = PipelineOptionsFactory.create();
    options.setRunner(org.apache.beam.runners.spark.SparkRunner.class);
    Pipeline pipeline = Pipeline.create(options);
    var error =
        assertThrows(
            org.apache.hop.core.exception.HopException.class,
            () ->
                meta.handleTransform(
                    LogChannel.GENERAL,
                    new Variables(),
                    "spark",
                    null,
                    null,
                    null,
                    new PipelineMeta(),
                    new TransformMeta("BeamDebeziumInput", "cdc", meta),
                    new HashMap<>(),
                    pipeline,
                    new RowMeta(),
                    List.of(),
                    null,
                    null));
    assertTrue(error.getMessage().contains("Spark"));
  }

  @Test
  void timeLimitTerminatesAnIdleSourceWithoutARecordLimit() throws Exception {
    BeamDebeziumInputMeta meta = new BeamDebeziumInputMeta();
    meta.setUsername("fixture");
    meta.setConnector("Custom");
    meta.setConnectorClass(IdleConnector.class.getName());
    meta.setMaxTimeMs("100");
    meta.setPollingTimeoutMs("10");
    Pipeline pipeline = Pipeline.create();
    Map<String, PCollection<HopRow>> collections = new HashMap<>();
    meta.handleTransform(
        LogChannel.GENERAL,
        new Variables(),
        "direct",
        null,
        null,
        null,
        new PipelineMeta(),
        new TransformMeta("BeamDebeziumInput", "idle", meta),
        collections,
        pipeline,
        new RowMeta(),
        List.of(),
        null,
        null);
    PAssert.that(collections.get("idle")).empty();
    PipelineResult result = pipeline.run();
    try {
      assertEquals(PipelineResult.State.DONE, result.waitUntilFinish(Duration.standardSeconds(30)));
    } finally {
      if (!result.getState().isTerminal()) {
        result.cancel();
      }
    }
  }

  @Test
  void realMapperPreservesUpdateDeleteAndTombstoneJsonThroughHopRowCoder() throws Exception {
    SourceRecord insert = record(7);
    Struct value = (Struct) insert.value();
    Struct before = new Struct(value.schema().field("before").schema()).put("id", 6);
    value.put("before", before);
    var mapper = new org.apache.beam.io.debezium.SourceRecordJson.SourceRecordJsonMapper();
    String update = mapper.mapSourceRecord(insert);
    assertTrue(update.contains("\"before\":{\"fields\":{\"id\":6}}"));
    value.put("after", null);
    String delete = mapper.mapSourceRecord(insert);
    assertTrue(delete.contains("\"after\":null"));
    String tombstone =
        mapper.mapSourceRecord(
            new SourceRecord(
                Map.of("server", "test"), Map.of("ts_usec", 1000L), "test", null, null));
    assertEquals("{\"metadata\":null,\"before\":null,\"after\":null}", tombstone);
    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> rows =
        pipeline
            .apply(org.apache.beam.sdk.transforms.Create.of(update, delete, tombstone))
            .apply(
                org.apache.beam.sdk.transforms.ParDo.of(
                    new org.apache.hop.beam.core.transform.BeamDebeziumInputTransform
                        .JsonToHopRowFn("conversion")))
            .setCoder(new HopRowCoder());
    PAssert.that(rows)
        .containsInAnyOrder(
            new HopRow(new Object[] {update}),
            new HopRow(new Object[] {delete}),
            new HopRow(new Object[] {tombstone}));
    assertEquals(
        PipelineResult.State.DONE, pipeline.run().waitUntilFinish(Duration.standardSeconds(30)));
    var coder = new HopRowCoder();
    java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
    coder.encode(new HopRow(new Object[] {update}), bytes);
    assertArrayEquals(
        new Object[] {update},
        coder.decode(new java.io.ByteArrayInputStream(bytes.toByteArray())).getRow());
  }

  public static class IdleConnector extends FixtureConnector {
    @Override
    public Class<? extends Task> taskClass() {
      return IdleTask.class;
    }
  }

  public static class IdleTask extends FixtureTask {
    @Override
    public List<SourceRecord> poll() {
      return List.of();
    }
  }

  private static long counter(PipelineResult result, String namespace) {
    var filter =
        org.apache.beam.sdk.metrics.MetricsFilter.builder()
            .addNameFilter(org.apache.beam.sdk.metrics.MetricNameFilter.named(namespace, "cdc"))
            .build();
    long count = 0;
    for (var metric : result.metrics().queryMetrics(filter).getCounters()) {
      count += metric.getAttempted();
    }
    return count;
  }

  /** A deterministic Kafka Connect source, not a database or a mocked Beam read. */
  public static class FixtureConnector extends SourceConnector {
    private Map<String, String> properties;

    @Override
    public void start(Map<String, String> properties) {
      this.properties = properties;
    }

    @Override
    public Class<? extends Task> taskClass() {
      return FixtureTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
      return List.of(properties);
    }

    @Override
    public void stop() {}

    @Override
    public ConfigDef config() {
      return new ConfigDef();
    }

    @Override
    public String version() {
      return "test";
    }
  }

  public static class FixtureTask extends SourceTask {
    private boolean sent;

    @Override
    public void start(Map<String, String> properties) {
      sent = false;
    }

    @Override
    public List<SourceRecord> poll() {
      if (sent) {
        return List.of();
      }
      sent = true;
      return List.of(record(1), record(2), record(3));
    }

    @Override
    public void stop() {}

    @Override
    public String version() {
      return "test";
    }
  }

  static SourceRecord record(int id) {
    Schema data = SchemaBuilder.struct().optional().field("id", Schema.INT32_SCHEMA).build();
    Schema source =
        SchemaBuilder.struct()
            .field("connector", Schema.STRING_SCHEMA)
            .field("version", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("db", Schema.STRING_SCHEMA)
            .field("schema", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .build();
    Schema value =
        SchemaBuilder.struct()
            .field("before", data)
            .field("after", data)
            .field("source", source)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .build();
    Struct event =
        new Struct(value)
            .put("before", null)
            .put("after", new Struct(data).put("id", id))
            .put(
                "source",
                new Struct(source)
                    .put("connector", "postgresql")
                    .put("version", "3.1.3")
                    .put("name", "test")
                    .put("db", "inventory")
                    .put("schema", "public")
                    .put("table", "orders"))
            .put("ts_ms", 1000L);
    return new SourceRecord(
        Map.of("server", "test"), Map.of("position", id), "test", null, value, event);
  }
}
