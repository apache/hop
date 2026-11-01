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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
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
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.parquet.transforms.input.ParquetField;
import org.apache.hop.parquet.transforms.input.ParquetInputMeta;
import org.apache.hop.parquet.transforms.output.ParquetOutput;
import org.apache.hop.parquet.transforms.output.ParquetOutputMeta;
import org.apache.hop.parquet.transforms.output.ParquetPartitionField;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Issue #2357: Parquet file input and output on the Beam Direct runner. */
class BeamParquetFileIOTest {

  private static final List<String> SEEN = Collections.synchronizedList(new ArrayList<>());

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
    List<String> seen = new ArrayList<>(readBack(file));
    Collections.sort(seen);
    assertEquals(List.of("keep-me|alice|1", "keep-me|bob|2"), seen);
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
    ParquetOutputMeta meta = fileMeta(tempDir.resolve("typed.parquet"));
    meta.getFields().get(0).setParquetType("JSON");
    HopException refused =
        assertThrows(HopException.class, () -> writeGraph(meta, Pipeline.create()));
    assertTrue(refused.getMessage().contains("JSON"));
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

  private void writeTwoRows(Path file) throws Exception {
    Pipeline pipeline = Pipeline.create();
    writeGraph(fileMeta(file), pipeline);
    pipeline.run().waitUntilFinish();
  }

  private static void writeGraph(ParquetOutputMeta meta, Pipeline pipeline) throws Exception {
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
            valueRow(),
            List.of(upstream()),
            pipeline.apply(
                Create.of(
                        new HopRow(new Object[] {"alice", 1L}),
                        new HopRow(new Object[] {"bob", 2L}))
                    .withCoder(new HopRowCoder())),
            null);
  }

  private static List<String> readBack(Path file) throws Exception {
    SEEN.clear();
    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setFilenameField("file");
    meta.setFields(
        List.of(
            new ParquetField("name", "name", "String", null, null, null),
            new ParquetField("id", "id", "Integer", null, null, null)));
    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> rows =
        pipeline.apply(
            Create.of(new HopRow(new Object[] {file.toAbsolutePath().toString(), "keep-me"}))
                .withCoder(new HopRowCoder()));
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
            new TransformMeta("ParquetFileInput", "read", meta),
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

  static class CollectRecord extends DoFn<HopRow, Void> {
    @ProcessElement
    public void process(@Element HopRow row) {
      Object[] values = row.getRow();
      SEEN.add(values[1] + "|" + values[2] + "|" + values[3]);
    }
  }

  private static ParquetOutputMeta fileMeta(Path file) {
    ParquetOutputMeta meta = new ParquetOutputMeta();
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
    meta.setFields(
        List.of(
            new org.apache.hop.parquet.transforms.output.ParquetField("name", "name"),
            new org.apache.hop.parquet.transforms.output.ParquetField("id", "id")));
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

  private static IRowMeta fileRow() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("file"));
    rowMeta.addValueMeta(new ValueMetaString("tag"));
    return rowMeta;
  }
}
