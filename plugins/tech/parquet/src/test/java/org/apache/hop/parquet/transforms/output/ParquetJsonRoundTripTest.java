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

package org.apache.hop.parquet.transforms.output;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.parquet.transforms.input.ParquetField;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Avro, which the output schema is built with, has no JSON logical type. These tests pin that a Hop
 * JSON field still comes back out of the file as a JSON field rather than as a String.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ParquetJsonRoundTripTest {

  private static final String JSON = "{\"id\":1,\"tags\":[\"a\",\"b\"]}";

  @TempDir private Path tempDir;

  private TransformMockHelper<ParquetOutputMeta, ParquetOutputData> mockHelper;

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>(
            "Parquet Output", ParquetOutputMeta.class, ParquetOutputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  /** A Hop JSON field has to be annotated as JSON in the schema of the file we write. */
  @Test
  void testJsonFieldIsReadBackAsJson() throws Exception {
    Path file = writeOneRow();

    IRowMeta rowMeta = ParquetTestUtil.readSchema(file.toString());

    assertEquals(IValueMeta.TYPE_JSON, rowMeta.getValueMeta(rowMeta.indexOfValue("doc")).getType());
    // A plain string field has to stay a String, so the annotation isn't applied to everything.
    assertEquals(
        IValueMeta.TYPE_STRING, rowMeta.getValueMeta(rowMeta.indexOfValue("name")).getType());
  }

  /** And the value itself has to survive the round trip. */
  @Test
  void testJsonValueSurvivesTheRoundTrip() throws Exception {
    Path file = writeOneRow();

    List<ParquetField> fields =
        List.of(
            new ParquetField("doc", "doc", "JSON", null, "-1", "-1"),
            new ParquetField("name", "name", "String", null, "-1", "-1"));
    List<RowMetaAndData> rows = ParquetTestUtil.readAllRows(file.toString(), fields);

    assertEquals(1, rows.size());
    RowMetaAndData row = rows.get(0);
    assertEquals(IValueMeta.TYPE_JSON, row.getValueMeta(0).getType());
    assertEquals(
        new ObjectMapper().readTree(JSON), new ObjectMapper().readTree(row.getString(0, "")));
    assertEquals("hop", row.getString(1, ""));
  }

  private Path writeOneRow() throws Exception {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    meta.setCompressionCodec(CompressionCodecName.UNCOMPRESSED);
    meta.setFilenameIncludingSplitNr(false);
    meta.setFilenameBase(tempDir.resolve("docs").toString());
    meta.setRowGroupSize("4096");
    meta.setDataPageSize("1024");
    meta.setDictionaryPageSize("512");

    ParquetOutputData data = new ParquetOutputData();
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("Parquet Output", meta);
    pipelineMeta.addTransform(transformMeta);
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    ParquetOutput output =
        spy(new ParquetOutput(transformMeta, meta, data, 0, pipelineMeta, pipeline));

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaJson("doc"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    output.setInputRowMeta(rowMeta);
    assertTrue(output.init());

    // A single row, spelled out rather than List.of() so it stays a list of one Object[].
    List<Object[]> remaining = new ArrayList<>();
    remaining.add(new Object[] {new ObjectMapper().readTree(JSON), "hop"});

    doNothing().when(output).putRow(any(), any());
    doAnswer(invocation -> remaining.isEmpty() ? null : remaining.remove(0)).when(output).getRow();

    while (output.processRow()) {
      // keep going until the null row closes the file
    }

    return onlyFile(tempDir);
  }

  private static Path onlyFile(Path folder) throws IOException {
    try (Stream<Path> stream = Files.list(folder)) {
      List<Path> files =
          stream.filter(Files::isRegularFile).sorted(Comparator.naturalOrder()).toList();
      assertEquals(1, files.size(), () -> "Expected one file in " + folder + " but got " + files);
      return files.get(0);
    }
  }
}
