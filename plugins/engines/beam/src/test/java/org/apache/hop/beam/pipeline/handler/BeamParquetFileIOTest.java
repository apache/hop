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

package org.apache.hop.beam.pipeline.handler;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.pipeline.HopPipelineMetaToBeamPipelineConverter;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.parquet.transforms.input.ParquetField;
import org.apache.hop.parquet.transforms.input.ParquetInputMeta;
import org.apache.hop.parquet.transforms.output.ParquetOutput;
import org.apache.hop.parquet.transforms.output.ParquetOutputMeta;
import org.apache.hop.parquet.transforms.output.ParquetPartitionField;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.joda.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Issue #2357: Parquet file input and output on the Beam Direct runner. */
class BeamParquetFileIOTest {

  private static final List<String> SEEN = Collections.synchronizedList(new ArrayList<>());
  private static final List<Object[]> SEEN_ROWS = Collections.synchronizedList(new ArrayList<>());
  private static PipelineResult lastRead;

  private static final Date WHEN = new Date(1_700_000_000_000L);
  private static final byte[] PAYLOAD = new byte[] {9, 8, 7};
  private static final BigDecimal AMOUNT = new BigDecimal("-12.34");

  @TempDir Path tempDir;

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void parquetPluginIdsAreExplicitBeamHandlers() {
    assertTrue(
        HopPipelineMetaToBeamPipelineConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            BeamConst.STRING_PARQUET_FILE_OUTPUT_PLUGIN_ID));
    assertTrue(
        HopPipelineMetaToBeamPipelineConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            BeamConst.STRING_PARQUET_FILE_INPUT_PLUGIN_ID));
  }

  @Test
  void avroSchemaIsTheHopRecord() throws Exception {
    ParquetOutputMeta meta = fileMeta(tempDir.resolve("schema.parquet"));
    List<org.apache.hop.parquet.transforms.output.ParquetField> fields = meta.getFields();
    List<IValueMeta> types = List.of(new ValueMetaString("name"), new ValueMetaInteger("id"));
    Schema schema = ParquetOutput.avroSchema(fields, types);
    assertEquals("ApacheHopParquetSchema", schema.getName());
    assertNotNull(schema.getField("name"));
    assertNotNull(schema.getField("id"));
    assertEquals(Schema.Type.UNION, schema.getField("id").schema().getType());
  }

  @Test
  void writeThenReadKeepsTheIncomingRow() throws Exception {
    Path file = tempDir.resolve("rows.parquet");
    writeTwoRows(file);
    List<String> seen = new ArrayList<>(readBack(file.toAbsolutePath().toString(), stringFields()));
    Collections.sort(seen);
    assertEquals(List.of("keep-me|alice|1", "keep-me|bob|2"), seen);
  }

  @Test
  void remoteOutputDirectoryKeepsTheScheme() {
    BeamParquetOutputTransformHandler.OutputLocation gs =
        BeamParquetOutputTransformHandler.outputLocation("gs://bucket/out/data.parquet");
    assertEquals("gs://bucket/out", gs.directory);
    assertEquals("data.parquet", gs.filename);

    BeamParquetOutputTransformHandler.OutputLocation s3 =
        BeamParquetOutputTransformHandler.outputLocation("s3://bkt/x/y.parquet");
    assertEquals("s3://bkt/x", s3.directory);
    assertEquals("y.parquet", s3.filename);

    BeamParquetOutputTransformHandler.OutputLocation file =
        BeamParquetOutputTransformHandler.outputLocation("file:///tmp/out/data.parquet");
    assertEquals("/tmp/out", file.directory);
    assertEquals("data.parquet", file.filename);

    assertTrue(BeamParquetOutputTransformHandler.isLocalPath("/tmp/out/data.parquet"));
    assertTrue(BeamParquetOutputTransformHandler.isLocalPath("out/data.parquet"));
    assertTrue(BeamParquetOutputTransformHandler.isLocalPath("file:///tmp/out/data.parquet"));
    assertFalse(BeamParquetOutputTransformHandler.isLocalPath("gs://bucket/out/data.parquet"));
    assertFalse(BeamParquetOutputTransformHandler.isLocalPath("s3://bkt/x/y.parquet"));
  }

  @Test
  void fileUriIsReadBack() throws Exception {
    Path file = tempDir.resolve("uri.parquet");
    writeTwoRows(file);
    String uri = file.toUri().toString();
    assertTrue(uri.startsWith("file:"), uri);
    List<String> seen = new ArrayList<>(readBack(uri, stringFields()));
    Collections.sort(seen);
    assertEquals(List.of("keep-me|alice|1", "keep-me|bob|2"), seen);
    assertEquals(2, counter(lastRead, org.apache.hop.pipeline.Pipeline.METRIC_NAME_READ, "read"));
    assertEquals(
        2, counter(lastRead, org.apache.hop.pipeline.Pipeline.METRIC_NAME_WRITTEN, "read"));
    assertEquals(0, counter(lastRead, org.apache.hop.pipeline.Pipeline.METRIC_NAME_OUTPUT, "read"));
  }

  @Test
  void fileUriIsWrittenAndReadBack() throws Exception {
    Path file = tempDir.resolve("from-uri.parquet");
    String uri = file.toUri().toString();
    assertTrue(uri.startsWith("file:"), uri);
    ParquetOutputMeta meta = fileMeta(file);
    String base =
        uri.endsWith(".parquet") ? uri.substring(0, uri.length() - ".parquet".length()) : uri;
    meta.setFilenameBase(base);

    Pipeline pipeline = Pipeline.create();
    writeGraph(meta, pipeline);
    pipeline.run().waitUntilFinish();

    assertTrue(Files.exists(file), uri);
    List<String> seen = new ArrayList<>(readBack(uri, stringFields()));
    Collections.sort(seen);
    assertEquals(List.of("keep-me|alice|1", "keep-me|bob|2"), seen);
  }

  @Test
  void dateTimestampDecimalAndBinaryRoundTrip() throws Exception {
    Path file = tempDir.resolve("typed.parquet");
    Timestamp after = new Timestamp(1_000L);
    after.setNanos(123_456_789);
    Timestamp before = new Timestamp(-1_000L);
    before.setNanos(999_999_000);
    Pipeline pipeline = Pipeline.create();
    writeGraph(
        typedMeta(file),
        pipeline,
        typedRow(),
        List.of(
            new HopRow(new Object[] {"alice", 1L, WHEN, after, AMOUNT, PAYLOAD}),
            new HopRow(
                new Object[] {
                  "bob", 2L, new Date(0L), before, new BigDecimal("0.01"), new byte[] {1}
                })));
    pipeline.run().waitUntilFinish();

    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(new LocalInputFile(file))
            .withConf(new Configuration(false))
            .build()) {
      GenericRecord first = reader.read();
      GenericRecord second = reader.read();
      assertNotNull(first);
      assertNotNull(second);
      GenericRecord alice = "alice".equals(String.valueOf(first.get("name"))) ? first : second;
      GenericRecord bob = alice == first ? second : first;
      assertEquals(1_123_456L, storedEpoch(alice.get("ts"), true));
      assertEquals(-1L, storedEpoch(bob.get("ts"), true));
      assertEquals(WHEN.getTime(), storedEpoch(alice.get("when"), false));
      assertEquals(0, AMOUNT.compareTo(storedDecimal(alice.get("amount"), 2)));
      assertArrayEquals(PAYLOAD, storedBytes(alice.get("payload")));
    }

    readRows(file.toAbsolutePath().toString(), typedInputFields(), typedFileRow());
    assertEquals(2, SEEN_ROWS.size());
    Object[] alice = rowNamed("alice");
    Object[] bob = rowNamed("bob");
    assertEquals("keep-me", alice[1]);
    assertEquals(1L, alice[3]);
    assertInstanceOf(Long.class, alice[3]);
    assertInstanceOf(Date.class, alice[4]);
    assertFalse(alice[4] instanceof Timestamp);
    assertEquals(WHEN, alice[4]);
    assertEquals(Timestamp.from(Instant.ofEpochSecond(1, 123_456_000)), alice[5]);
    assertEquals(AMOUNT, alice[6]);
    assertArrayEquals(PAYLOAD, (byte[]) alice[7]);
    assertEquals(Timestamp.from(Instant.ofEpochSecond(-1, 999_999_000)), bob[5]);
    assertEquals(new BigDecimal("0.01"), bob[6]);
    assertArrayEquals(new byte[] {1}, (byte[]) bob[7]);
  }

  @Test
  void partitionFieldIsRefused() {
    ParquetOutputMeta meta = fileMeta(tempDir.resolve("part.parquet"));
    meta.setPartitionFields(List.of(new ParquetPartitionField("region")));
    HopException refused =
        assertThrows(HopException.class, () -> writeGraph(meta, Pipeline.create()));
    assertTrue(refused.getMessage().contains("partition"));
  }

  @Test
  void explicitParquetTypeIsRefused() {
    ParquetOutputMeta meta = fileMeta(tempDir.resolve("typed-refused.parquet"));
    meta.getFields().get(0).setParquetType("JSON");
    HopException refused =
        assertThrows(HopException.class, () -> writeGraph(meta, Pipeline.create()));
    assertTrue(refused.getMessage().contains("JSON"));
  }

  @Test
  void jsonAndUuidFieldsAreRefused() {
    ParquetOutputMeta jsonMeta = fileMeta(tempDir.resolve("json.parquet"));
    jsonMeta.setFields(
        List.of(new org.apache.hop.parquet.transforms.output.ParquetField("doc", "doc")));
    RowMeta jsonRow = new RowMeta();
    jsonRow.addValueMeta(new ValueMetaJson("doc"));
    HopException json =
        assertThrows(
            HopException.class,
            () ->
                writeGraph(
                    jsonMeta,
                    Pipeline.create(),
                    jsonRow,
                    List.of(new HopRow(new Object[] {"{}"}))));
    assertTrue(json.getMessage().contains("JSON"), json.getMessage());

    ParquetOutputMeta uuidMeta = fileMeta(tempDir.resolve("uuid.parquet"));
    uuidMeta.setFields(
        List.of(new org.apache.hop.parquet.transforms.output.ParquetField("id", "id")));
    RowMeta uuidRow = new RowMeta();
    uuidRow.addValueMeta(new UuidValueMeta("id"));
    HopException uuid =
        assertThrows(
            HopException.class,
            () ->
                writeGraph(
                    uuidMeta,
                    Pipeline.create(),
                    uuidRow,
                    List.of(new HopRow(new Object[] {"00112233-4455-6677-8899-aabbccddeeff"}))));
    assertTrue(uuid.getMessage().contains("UUID"), uuid.getMessage());
  }

  @Test
  void inputWithoutFieldsOrMetadataFileIsRefused() {
    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setFilenameField("file");
    HopException missing =
        assertThrows(
            HopException.class,
            () -> {
              Pipeline pipeline = Pipeline.create();
              PCollection<HopRow> rows =
                  pipeline.apply(
                      Create.of(new HopRow(new Object[] {"x"})).withCoder(new HopRowCoder()));
              new BeamParquetInputTransformHandler()
                  .handleTransform(
                      LogChannel.GENERAL,
                      new Variables(),
                      "direct",
                      null,
                      null,
                      new MemoryMetadataProvider(),
                      new PipelineMeta(),
                      new TransformMeta("ParquetFileInput", "read", meta),
                      new HashMap<>(),
                      pipeline,
                      fileRow(),
                      List.of(upstream()),
                      rows,
                      null);
            });
    assertTrue(missing.getMessage().contains("metadata"));
  }

  @Test
  void nullRowWhenEmptyIsAcceptedInTheGlobalWindow() throws Exception {
    ParquetInputMeta meta = inputMeta(stringFields());
    meta.setSendingNullsRowWhenEmpty(true);
    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> rows =
        pipeline.apply(
            Create.of(new HopRow(new Object[] {"x", "keep"})).withCoder(new HopRowCoder()));
    new BeamParquetInputTransformHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "direct",
            null,
            null,
            new MemoryMetadataProvider(),
            new PipelineMeta(),
            new TransformMeta("ParquetFileInput", "read", meta),
            new HashMap<>(),
            pipeline,
            fileRow(),
            List.of(upstream()),
            rows,
            null);
  }

  @Test
  void nullRowWhenEmptyRefusesAWindowedInput() {
    ParquetInputMeta meta = inputMeta(stringFields());
    meta.setSendingNullsRowWhenEmpty(true);
    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> rows =
        pipeline
            .apply(Create.of(new HopRow(new Object[] {"x", "keep"})).withCoder(new HopRowCoder()))
            .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));
    HopException refused =
        assertThrows(
            HopException.class,
            () ->
                new BeamParquetInputTransformHandler()
                    .handleTransform(
                        LogChannel.GENERAL,
                        new Variables(),
                        "direct",
                        null,
                        null,
                        new MemoryMetadataProvider(),
                        new PipelineMeta(),
                        new TransformMeta("ParquetFileInput", "read", meta),
                        new HashMap<>(),
                        pipeline,
                        fileRow(),
                        List.of(upstream()),
                        rows,
                        null));
    assertTrue(refused.getMessage().contains("global window"), refused.getMessage());
  }

  private void writeTwoRows(Path file) throws Exception {
    Pipeline pipeline = Pipeline.create();
    writeGraph(fileMeta(file), pipeline);
    pipeline.run().waitUntilFinish();
  }

  private static void writeGraph(ParquetOutputMeta meta, Pipeline pipeline) throws Exception {
    writeGraph(
        meta,
        pipeline,
        valueRow(),
        List.of(new HopRow(new Object[] {"alice", 1L}), new HopRow(new Object[] {"bob", 2L})));
  }

  private static void writeGraph(
      ParquetOutputMeta meta, Pipeline pipeline, IRowMeta rowMeta, List<HopRow> rows)
      throws Exception {
    new BeamParquetOutputTransformHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "direct",
            null,
            null,
            new MemoryMetadataProvider(),
            new PipelineMeta(),
            new TransformMeta("ParquetFileOutput", "write", meta),
            new HashMap<>(),
            pipeline,
            rowMeta,
            List.of(upstream()),
            pipeline.apply(Create.of(rows).withCoder(new HopRowCoder())),
            null);
  }

  private static List<String> readBack(String filename, List<ParquetField> fields)
      throws Exception {
    SEEN.clear();
    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> rows =
        pipeline.apply(
            Create.of(new HopRow(new Object[] {filename, "keep-me"})).withCoder(new HopRowCoder()));
    HashMap<String, PCollection<HopRow>> map = new HashMap<>();
    new BeamParquetInputTransformHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "direct",
            null,
            null,
            new MemoryMetadataProvider(),
            new PipelineMeta(),
            new TransformMeta("ParquetFileInput", "read", inputMeta(fields)),
            map,
            pipeline,
            fileRow(),
            List.of(upstream()),
            rows,
            null);
    map.get("read").apply(ParDo.of(new CollectRecord()));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    lastRead = result;
    return new ArrayList<>(SEEN);
  }

  private static void readRows(String filename, List<ParquetField> fields, IRowMeta rowMeta)
      throws Exception {
    SEEN_ROWS.clear();
    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> rows =
        pipeline.apply(
            Create.of(new HopRow(new Object[] {filename, "keep-me"})).withCoder(new HopRowCoder()));
    HashMap<String, PCollection<HopRow>> map = new HashMap<>();
    new BeamParquetInputTransformHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "direct",
            null,
            null,
            new MemoryMetadataProvider(),
            new PipelineMeta(),
            new TransformMeta("ParquetFileInput", "read", inputMeta(fields)),
            map,
            pipeline,
            rowMeta,
            List.of(upstream()),
            rows,
            null);
    map.get("read").apply(ParDo.of(new CollectRow()));
    pipeline.run().waitUntilFinish();
  }

  private static Object[] rowNamed(String name) {
    for (Object[] row : SEEN_ROWS) {
      if (name.equals(row[2])) {
        return row;
      }
    }
    throw new AssertionError("missing row " + name);
  }

  private static long storedEpoch(Object value, boolean micros) {
    if (value instanceof Instant instant) {
      if (micros) {
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
      }
      return instant.toEpochMilli();
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    throw new AssertionError(value.getClass().getName());
  }

  private static BigDecimal storedDecimal(Object value, int scale) {
    if (value instanceof BigDecimal decimal) {
      return decimal;
    }
    return new BigDecimal(new BigInteger(storedBytes(value)), scale);
  }

  private static byte[] storedBytes(Object value) {
    if (value instanceof byte[] bytes) {
      return bytes;
    }
    if (value instanceof ByteBuffer buffer) {
      ByteBuffer duplicate = buffer.duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      return bytes;
    }
    throw new AssertionError(value.getClass().getName());
  }

  private static long counter(PipelineResult result, String namespace, String name) {
    MetricQueryResults results =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(namespace, name))
                    .build());
    long count = 0;
    for (MetricResult<Long> metric : results.getCounters()) {
      count += metric.getAttempted();
    }
    return count;
  }

  static class CollectRecord extends DoFn<HopRow, Void> {
    @ProcessElement
    public void process(@Element HopRow row) {
      Object[] values = row.getRow();
      SEEN.add(values[1] + "|" + values[2] + "|" + values[3]);
    }
  }

  static class CollectRow extends DoFn<HopRow, Void> {
    @ProcessElement
    public void process(@Element HopRow row) {
      SEEN_ROWS.add(row.getRow().clone());
    }
  }

  /** Stands in for the UUID value type plugin, which is not on this test classpath. */
  private static final class UuidValueMeta extends ValueMetaBase {
    private UuidValueMeta(String name) {
      super(name, IValueMeta.TYPE_UUID);
    }
  }

  private static ParquetOutputMeta fileMeta(Path file) {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    applyFile(meta, file);
    meta.setFields(
        List.of(
            new org.apache.hop.parquet.transforms.output.ParquetField("name", "name"),
            new org.apache.hop.parquet.transforms.output.ParquetField("id", "id")));
    return meta;
  }

  private static ParquetOutputMeta typedMeta(Path file) {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    applyFile(meta, file);
    meta.setFields(
        List.of(
            new org.apache.hop.parquet.transforms.output.ParquetField("name", "name"),
            new org.apache.hop.parquet.transforms.output.ParquetField("id", "id"),
            new org.apache.hop.parquet.transforms.output.ParquetField("when", "when"),
            new org.apache.hop.parquet.transforms.output.ParquetField("ts", "ts"),
            new org.apache.hop.parquet.transforms.output.ParquetField("amount", "amount"),
            new org.apache.hop.parquet.transforms.output.ParquetField("payload", "payload")));
    return meta;
  }

  private static void applyFile(ParquetOutputMeta meta, Path file) {
    String path = file.toAbsolutePath().toString();
    String base =
        path.endsWith(".parquet") ? path.substring(0, path.length() - ".parquet".length()) : path;
    meta.setFilenameBase(base);
    meta.setFilenameExtension("parquet");
    meta.setFilenameIncludingCopyNr(false);
    meta.setFilenameIncludingSplitNr(false);
    meta.setFilenameIncludingDate(false);
    meta.setFilenameIncludingTime(false);
    meta.setFilenameIncludingDateTime(false);
    meta.setFilenameCreatingParentFolders(true);
  }

  private static List<ParquetField> stringFields() {
    return List.of(
        new ParquetField("name", "name", "String", null, null, null),
        new ParquetField("id", "id", "Integer", null, null, null));
  }

  private static List<ParquetField> typedInputFields() {
    return List.of(
        new ParquetField("name", "name", "String", null, null, null),
        new ParquetField("id", "id", "Integer", null, null, null),
        new ParquetField("when", "when", "Date", null, null, null),
        new ParquetField("ts", "ts", "Timestamp", null, null, null),
        new ParquetField("amount", "amount", "BigNumber", null, "10", "2"),
        new ParquetField("payload", "payload", "Binary", null, null, null));
  }

  private static ParquetInputMeta inputMeta(List<ParquetField> fields) {
    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setFilenameField("file");
    meta.setFields(fields);
    return meta;
  }

  private static TransformMeta upstream() {
    TransformMeta upstream = new TransformMeta();
    upstream.setName("upstream");
    return upstream;
  }

  private static IRowMeta valueRow() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    return rowMeta;
  }

  private static IRowMeta typedRow() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaDate("when"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("ts"));
    ValueMetaBigNumber amount = new ValueMetaBigNumber("amount");
    amount.setLength(10);
    amount.setPrecision(2);
    rowMeta.addValueMeta(amount);
    rowMeta.addValueMeta(new ValueMetaBinary("payload"));
    return rowMeta;
  }

  private static IRowMeta fileRow() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("file"));
    rowMeta.addValueMeta(new ValueMetaString("tag"));
    return rowMeta;
  }

  private static IRowMeta typedFileRow() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("file"));
    rowMeta.addValueMeta(new ValueMetaString("tag"));
    return rowMeta;
  }
}
