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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.avro.transforms.avroinput.AvroFileInputMeta;
import org.apache.hop.avro.transforms.avrooutput.AvroOutput;
import org.apache.hop.avro.transforms.avrooutput.AvroOutputField;
import org.apache.hop.avro.transforms.avrooutput.AvroOutputMeta;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.pipeline.HopPipelineMetaToBeamPipelineConverter;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Issue #2358: Avro file input and output on the Beam Direct runner. */
class BeamAvroFileIOTest {

  private static final List<String> SEEN = Collections.synchronizedList(new ArrayList<>());

  @TempDir Path tempDir;

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void staticSchemaUsesTheRecordNameAndNullableFields() throws Exception {
    AvroOutputMeta meta = fileMeta(tempDir.resolve("schema.avro"));
    Schema schema =
        AvroOutput.createAvroSchema(
            new ArrayList<>(meta.getOutputFields()),
            "",
            meta.getDoc(),
            meta.getRecordName(),
            meta.getNamespace());

    assertEquals("row", schema.getName());
    assertEquals("org.apache.hop.test", schema.getNamespace());
    assertNotNull(schema.getField("name"));
    assertNotNull(schema.getField("id"));
    assertEquals(Schema.Type.UNION, schema.getField("name").schema().getType());
    assertEquals(Schema.Type.STRING, schema.getField("name").schema().getTypes().get(1).getType());
  }

  @Test
  void avroPluginIdsAreExplicitBeamHandlers() {
    assertTrue(
        HopPipelineMetaToBeamPipelineConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            BeamConst.STRING_AVRO_OUTPUT_PLUGIN_ID));
    assertTrue(
        HopPipelineMetaToBeamPipelineConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            BeamConst.STRING_AVRO_FILE_INPUT_PLUGIN_ID));
  }

  @Test
  void writeThenReadKeepsTheIncomingRow() throws Exception {
    Path file = tempDir.resolve("rows.avro");
    writeTwoRows(file);

    try (DataFileStream<GenericRecord> in =
        new DataFileStream<>(Files.newInputStream(file), new GenericDatumReader<>())) {
      GenericRecord first = in.next();
      assertEquals("alice", first.get("name").toString());
      assertEquals(1L, first.get("id"));
      GenericRecord second = in.next();
      assertEquals("bob", second.get("name").toString());
      assertEquals(2L, second.get("id"));
      assertFalse(in.hasNext());
    }

    assertEquals(List.of("keep-me|alice|1", "keep-me|bob|2"), readBack(file, null));
  }

  @Test
  void rowLimitCapsWhatOneWorkerEmits() throws Exception {
    Path file = tempDir.resolve("limited.avro");
    writeTwoRows(file);
    assertEquals(List.of("keep-me|alice|1"), readBack(file, "1"));
  }

  @Test
  void missingFilenameFieldFailsWhileTheGraphIsBuilt() {
    AvroFileInputMeta meta = new AvroFileInputMeta();
    meta.setDataFilenameField("missing");
    meta.setOutputFieldName("avro");
    HopException missing =
        assertThrows(HopException.class, () -> readGraph(meta, stringRow(), "x"));
    assertTrue(missing.getMessage().contains("missing"));
  }

  @Test
  void copyNumberInTheFilenameIsRefused() throws Exception {
    AvroOutputMeta meta = fileMeta(tempDir.resolve("refused.avro"));
    meta.setTransformNrInFilename(true);
    HopException refused =
        assertThrows(
            HopException.class,
            () ->
                new BeamAvroOutputTransformHandler()
                    .handleTransform(
                        LogChannel.GENERAL,
                        new Variables(),
                        "direct",
                        null,
                        null,
                        new MemoryMetadataProvider(),
                        new PipelineMeta(),
                        new TransformMeta("AvroOutput", "write", meta),
                        new HashMap<>(),
                        Pipeline.create(),
                        valueRow(),
                        List.of(upstream()),
                        twoRows(Pipeline.create()),
                        null));
    assertTrue(refused.getMessage().contains("partition"));
  }

  private void writeTwoRows(Path file) throws Exception {
    AvroOutputMeta meta = fileMeta(file);
    Pipeline pipeline = Pipeline.create();
    new BeamAvroOutputTransformHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "direct",
            null,
            null,
            new MemoryMetadataProvider(),
            new PipelineMeta(),
            new TransformMeta("AvroOutput", "write", meta),
            new HashMap<>(),
            pipeline,
            valueRow(),
            List.of(upstream()),
            twoRows(pipeline),
            null);
    pipeline.run().waitUntilFinish();
  }

  private static List<String> readBack(Path file, String rowsLimit) throws Exception {
    SEEN.clear();
    AvroFileInputMeta meta = new AvroFileInputMeta();
    meta.setDataFilenameField("file");
    meta.setOutputFieldName("avro");
    meta.setRowsLimit(rowsLimit);
    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> rows =
        pipeline.apply(
            Create.of(new HopRow(new Object[] {file.toAbsolutePath().toString(), "keep-me"}))
                .withCoder(new HopRowCoder()));
    HashMap<String, PCollection<HopRow>> map = new HashMap<>();
    new BeamAvroInputTransformHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "direct",
            null,
            null,
            new MemoryMetadataProvider(),
            new PipelineMeta(),
            new TransformMeta("AvroFileInput", "read", meta),
            map,
            pipeline,
            fileRow(),
            List.of(upstream()),
            rows,
            null);
    map.get("read").apply(ParDo.of(new CollectRecord()));
    pipeline.run().waitUntilFinish();
    return new ArrayList<>(SEEN);
  }

  private static void readGraph(AvroFileInputMeta meta, IRowMeta rowMeta, String value)
      throws Exception {
    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> rows =
        pipeline.apply(Create.of(new HopRow(new Object[] {value})).withCoder(new HopRowCoder()));
    new BeamAvroInputTransformHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "direct",
            null,
            null,
            new MemoryMetadataProvider(),
            new PipelineMeta(),
            new TransformMeta("AvroFileInput", "read", meta),
            new HashMap<>(),
            pipeline,
            rowMeta,
            List.of(upstream()),
            rows,
            null);
  }

  /** DirectRunner serializes this function. Results go to {@link #SEEN}. */
  static class CollectRecord extends DoFn<HopRow, Void> {
    @ProcessElement
    public void process(@Element HopRow row) {
      Object[] values = row.getRow();
      GenericRecord record = (GenericRecord) values[2];
      SEEN.add(values[1] + "|" + record.get("name") + "|" + record.get("id"));
    }
  }

  private static AvroOutputMeta fileMeta(Path file) {
    AvroOutputMeta meta = new AvroOutputMeta();
    meta.setDefault();
    meta.setFileName(file.toAbsolutePath().toString());
    meta.setOutputType(AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_BINARY_FILE]);
    meta.setCreateSchemaFile(true);
    meta.setWriteSchemaFile(false);
    meta.setRecordName("row");
    meta.setNamespace("org.apache.hop.test");
    meta.setDoc("test");
    meta.setCompressionType("none");
    meta.setOutputFields(
        List.of(
            new AvroOutputField("name", "name", AvroOutputField.AVRO_TYPE_STRING, true),
            new AvroOutputField("id", "id", AvroOutputField.AVRO_TYPE_LONG, true)));
    return meta;
  }

  private static TransformMeta upstream() {
    TransformMeta upstream = new TransformMeta();
    upstream.setName("upstream");
    return upstream;
  }

  private static PCollection<HopRow> twoRows(Pipeline pipeline) {
    return pipeline.apply(
        Create.of(new HopRow(new Object[] {"alice", 1L}), new HopRow(new Object[] {"bob", 2L}))
            .withCoder(new HopRowCoder()));
  }

  private static IRowMeta valueRow() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    return rowMeta;
  }

  private static IRowMeta fileRow() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("file"));
    rowMeta.addValueMeta(new ValueMetaString("tag"));
    return rowMeta;
  }

  private static IRowMeta stringRow() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("file"));
    return rowMeta;
  }
}
