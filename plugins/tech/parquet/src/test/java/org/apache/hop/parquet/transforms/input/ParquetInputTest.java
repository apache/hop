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

package org.apache.hop.parquet.transforms.input;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/** Runs {@link ParquetInput} against real files. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ParquetInputTest {

  private static final MessageType SCHEMA =
      Types.buildMessage()
          .required(PrimitiveTypeName.INT64)
          .named("id")
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.stringType())
          .named("name")
          .named("row");

  @TempDir private Path tempDir;

  private TransformMockHelper<ParquetInputMeta, ParquetInputData> mockHelper;

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("Parquet Input", ParquetInputMeta.class, ParquetInputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  private String writeTempFile(String name, long... ids) throws Exception {
    String filename = tempDir.resolve(name).toString();
    writeFile(filename, ids);
    return filename;
  }

  @SuppressWarnings("unchecked")
  private static void writeFile(String filename, long... ids) throws Exception {
    List<java.util.function.Consumer<org.apache.parquet.example.data.Group>> rows =
        new ArrayList<>();
    for (long id : ids) {
      rows.add(g -> g.append("id", id).append("name", Binary.fromString("name-" + id)));
    }
    ParquetTestFiles.write(filename, SCHEMA, rows.toArray(new java.util.function.Consumer[0]));
  }

  @Test
  void readsEveryFileNamedInTheInputAndAppendsToTheInputRow() throws Exception {
    String one = writeTempFile("one.parquet", 1L, 2L);
    String two = writeTempFile("two.parquet", 3L);

    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setFilenameField("filename");

    List<RowMetaAndData> output =
        run(meta, new Object[] {"a", one}, new Object[] {"b", null}, new Object[] {"c", two});

    assertEquals(3, output.size());
    // Input fields first, then every column of the file since no fields were configured.
    assertEquals(
        List.of("tag", "filename", "id", "name"),
        List.of(output.get(0).getRowMeta().getFieldNames()));
    assertEquals("a", output.get(0).getString("tag", null));
    assertEquals(1L, output.get(0).getInteger("id", -1L));
    assertEquals("name-1", output.get(0).getString("name", null));
    assertEquals(2L, output.get(1).getInteger("id", -1L));
    assertEquals("c", output.get(2).getString("tag", null));
    assertEquals(3L, output.get(2).getInteger("id", -1L));
  }

  @Test
  void configuredFieldsSelectRenameAndConvert() throws Exception {
    String file = writeTempFile("one.parquet", 7L);

    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setFilenameField("filename");
    meta.getFields().add(new ParquetField("id", "id_as_text", "String", null, "-1", "-1"));

    List<RowMetaAndData> output = run(meta, new Object[] {"a", file});

    assertEquals(1, output.size());
    assertEquals(3, output.get(0).size());
    assertEquals("7", output.get(0).getString("id_as_text", null));
  }

  @Test
  void readsFromAnyVfsScheme() throws Exception {
    // ram:// takes the non-local path through ParquetStream's seekable VFS stream.
    String filename = "ram:///parquet-input-one.parquet";
    writeFile(filename, 11L, 12L);

    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setFilenameField("filename");

    List<RowMetaAndData> output = run(meta, new Object[] {"a", filename});

    assertEquals(2, output.size());
    assertEquals(11L, output.get(0).getInteger("id", -1L));
    assertEquals(12L, output.get(1).getInteger("id", -1L));
  }

  @Test
  void sendsARowOfNullsWhenNothingWasRead() throws Exception {
    String empty = writeTempFile("empty.parquet");

    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setFilenameField("filename");
    meta.setSendingNullsRowWhenEmpty(true);

    List<RowMetaAndData> output = run(meta, new Object[] {"a", empty});

    assertEquals(1, output.size());
    assertEquals(4, output.get(0).getRowMeta().size());
    for (int i = 0; i < 4; i++) {
      assertNull(output.get(0).getData()[i]);
    }
  }

  @Test
  void sendsARowOfNullsWhenNoFileNameEverArrived() throws Exception {
    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setFilenameField("filename");
    meta.setSendingNullsRowWhenEmpty(true);
    meta.getFields().add(new ParquetField("id", "id", "Integer", null, "-1", "-1"));

    List<RowMetaAndData> output = run(meta);

    assertEquals(1, output.size());
    assertEquals(1, output.get(0).getRowMeta().size());
    assertNull(output.get(0).getData()[0]);
  }

  @Test
  void staysSilentWhenNothingWasReadAndNullsAreNotWanted() throws Exception {
    String empty = writeTempFile("empty.parquet");
    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setFilenameField("filename");

    assertTrue(run(meta, new Object[] {"a", empty}).isEmpty());
  }

  @Test
  void failsOnAMissingFilenameField() {
    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setFilenameField("nope");

    HopException e =
        assertThrows(HopException.class, () -> run(meta, new Object[] {"a", "x.parquet"}));
    assertTrue(e.getMessage().contains("nope"));
  }

  @Test
  void failsOnAnUnreadableFile() {
    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setFilenameField("filename");
    String missing = tempDir.resolve("missing.parquet").toString();

    HopException e = assertThrows(HopException.class, () -> run(meta, new Object[] {"a", missing}));
    assertTrue(e.getMessage().contains("missing.parquet"));
  }

  @Test
  void failsOnAnUnknownSourceField() throws Exception {
    String file = writeTempFile("one.parquet", 1L);
    ParquetInputMeta meta = new ParquetInputMeta();
    meta.setFilenameField("filename");
    meta.getFields().add(new ParquetField("nope", "nope", "String", null, "-1", "-1"));

    assertThrows(HopException.class, () -> run(meta, new Object[] {"a", file}));
  }

  private static IRowMeta inputRowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("tag"));
    rowMeta.addValueMeta(new ValueMetaString("filename"));
    return rowMeta;
  }

  /** Feeds the rows through a fresh transform and collects what it puts out. */
  private List<RowMetaAndData> run(ParquetInputMeta meta, Object[]... rows) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("Parquet Input", meta);
    pipelineMeta.addTransform(transformMeta);
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    ParquetInputData data = new ParquetInputData();
    ParquetInput input =
        spy(new ParquetInput(transformMeta, meta, data, 0, pipelineMeta, pipeline));
    if (rows.length > 0) {
      input.setInputRowMeta(inputRowMeta());
    }
    assertTrue(input.init());

    List<Object[]> remaining = new ArrayList<>(List.of(rows));
    doAnswer(invocation -> remaining.isEmpty() ? null : remaining.remove(0)).when(input).getRow();
    List<RowMetaAndData> output = new ArrayList<>();
    doAnswer(
            invocation ->
                output.add(
                    new RowMetaAndData(
                        invocation.getArgument(0, IRowMeta.class),
                        invocation.getArgument(1, Object[].class))))
        .when(input)
        .putRow(any(), any());

    while (input.processRow()) {
      // until the null row
    }
    assertNull(data.reader, "reader must be released after the last file");
    input.dispose();
    return output;
  }
}
