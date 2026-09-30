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

package org.apache.hop.pipeline.transforms.jsonoutputenhanced;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class JsonEOutputBehaviorTest {
  private final String base = "ram:///enhanced-json-" + UUID.randomUUID();

  @BeforeAll
  static void initialize() throws HopException {
    HopClientEnvironment.init();
  }

  @AfterEach
  void removeFiles() throws Exception {
    try (FileObject file = HopVfs.getFileObject(base)) {
      if (file.exists()) {
        file.deleteAll();
      }
    }
  }

  private JsonEOutputMeta meta(JsonEOutputMeta.OperationType operation) {
    JsonEOutputMeta meta = new JsonEOutputMeta();
    meta.setOperationType(operation);
    meta.setOutputValue("rows");
    meta.setJsonBloc("");
    meta.setEncoding("UTF-8");
    JsonEOutputField field = new JsonEOutputField();
    field.setFieldName("payload");
    field.setElementName("payload");
    meta.getOutputFields().add(field);
    meta.getFileSettings().setFileName(base + "/out");
    meta.getFileSettings().setExtension("json");
    meta.getFileSettings().setCreateParentFolder(true);
    meta.getFileSettings().setDoNotOpenNewFileInit(true);
    return meta;
  }

  private static String read(String path) throws Exception {
    try (FileObject file = HopVfs.getFileObject(path);
        var input = HopVfs.getInputStream(file)) {
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static JsonNode parse(String json) throws Exception {
    return HopJson.newMapper()
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .readTree(json);
  }

  private static String payload(String value) {
    return HopJson.newMapper().createObjectNode().put("payload", value).toString();
  }

  private static IRowMeta rowMeta(String... names) {
    IRowMeta row = new RowMeta();
    for (String name : names) {
      row.addValueMeta(new ValueMetaString(name));
    }
    return row;
  }

  private static final class Harness implements AutoCloseable {
    final TransformMockHelper<JsonEOutputMeta, JsonEOutputData> helper;
    final List<RowMetaAndData> written = new ArrayList<>();
    final JsonEOutputData data;
    final JsonEOutput transform;

    Harness(JsonEOutputMeta meta, IRowMeta inputMeta, Object[]... rows) {
      helper =
          new TransformMockHelper<>("enhanced JSON", JsonEOutputMeta.class, JsonEOutputData.class);
      when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
          .thenReturn(helper.iLogChannel);
      when(helper.pipeline.isRunning()).thenReturn(true);
      when(helper.transformMeta.getTransform()).thenReturn(meta);
      var iterator = Arrays.asList(rows).iterator();
      data = new JsonEOutputData();
      transform =
          new JsonEOutput(
              helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline) {
            @Override
            public Object[] getRow() {
              return iterator.hasNext() ? iterator.next() : null;
            }

            @Override
            public void putRow(IRowMeta rowMeta, Object[] row) {
              written.add(new RowMetaAndData(rowMeta.clone(), row.clone()));
            }
          };
      transform.setInputRowMeta(inputMeta);
    }

    void run() throws HopException {
      assertTrue(transform.init());
      while (transform.processRow()) {
        // Drain the real transform through EOF.
      }
    }

    @Override
    public void close() {
      try {
        transform.dispose();
      } finally {
        helper.cleanUp();
      }
    }
  }

  @Test
  void missingRuntimeGroupKeyIsAnActionableError() throws Exception {
    JsonEOutputMeta meta = meta(JsonEOutputMeta.OperationType.OUTPUT_VALUE);
    meta.getKeyFields().add(new JsonEOutputKeyField("gone"));
    try (Harness h = new Harness(meta, rowMeta("payload"), new Object[] {"x"})) {
      assertTrue(h.transform.init());
      HopException error = assertThrows(HopException.class, h.transform::processRow);
      assertTrue(error.getMessage().contains("gone"), error.getMessage());
    }
  }

  @Test
  void missingKeyIsNamedDuringSchemaDiscovery() {
    JsonEOutputMeta meta = meta(JsonEOutputMeta.OperationType.OUTPUT_VALUE);
    meta.getKeyFields().add(new JsonEOutputKeyField("gone"));
    IRowMeta input = rowMeta("payload");
    HopTransformException error =
        assertThrows(
            HopTransformException.class,
            () -> meta.getFields(input, "json", null, null, new Variables(), null));
    assertTrue(error.getMessage().contains("gone"), error.getMessage());
    assertEquals("payload", input.getValueMeta(0).getName());
  }

  @Test
  void bothKeepsRowShapeAndWritesEveryGroup() throws Exception {
    JsonEOutputMeta meta = meta(JsonEOutputMeta.OperationType.BOTH);
    JsonEOutputKeyField key = new JsonEOutputKeyField("grp");
    key.setElementName("group");
    meta.getKeyFields().add(key);
    try (Harness h =
        new Harness(
            meta,
            rowMeta("grp", "payload"),
            new Object[] {"a", "x"},
            new Object[] {"a", "y"},
            new Object[] {"b", "z"})) {
      h.run();
      assertEquals(2, h.written.size());
      for (RowMetaAndData row : h.written) {
        assertEquals(row.getRowMeta().size(), row.getData().length);
        assertTrue(row.getRowMeta().indexOfValue("grp") >= 0);
        assertEquals(-1, row.getRowMeta().indexOfValue("group"));
      }
    }
    JsonNode file = parse(read(base + "/out.json"));
    assertEquals(2, file.size());
    assertEquals("a", file.get(0).get("group").asText());
    assertEquals(2, file.get(0).get("rows").size());
    assertEquals("z", file.get(1).get("rows").get("payload").asText());
  }

  @Test
  void keySuggestionsExcludeLiveOutputAndExistingKeys() {
    assertEquals(
        List.of("grp"),
        JsonEOutputDialog.suggestKeyFieldNames(
            rowMeta("payload", "grp", "already"), List.of("payload"), List.of("already")));
    assertEquals(
        List.of(),
        JsonEOutputDialog.suggestKeyFieldNames(
            rowMeta("payload", "grp"), List.of("payload"), List.of("grp")));
  }

  @Test
  void disposalClosesAnActiveFileGenerator() throws Exception {
    JsonEOutputMeta meta = meta(JsonEOutputMeta.OperationType.WRITE_TO_FILE);
    try (Harness h =
        new Harness(
            meta, rowMeta("payload"), new Object[] {"a"}, new Object[] {"b"}, new Object[] {"c"})) {
      assertTrue(h.transform.init());
      assertTrue(h.transform.processRow());
      assertTrue(h.transform.processRow());
      var generator = h.data.fileGenerator;
      assertNotNull(generator);
      h.transform.dispose();
      assertTrue(generator.isClosed());
      assertNull(h.data.fileGenerator);
      assertNull(h.data.writer);
    }
  }

  /**
   * The pipeline attached to issue #2569: pretty-printed file output, one group key with an element
   * name, a JSON block around the file, and a mix of one-row and many-row groups. Parsed structure
   * is what matters; pretty-print whitespace is not.
   */
  @Test
  void issue2569SampleGroupsPrettyFileByKeyAlias() throws Exception {
    JsonEOutputMeta meta = meta(JsonEOutputMeta.OperationType.WRITE_TO_FILE);
    meta.setOutputValue("lvl1Details");
    meta.setJsonBloc("result");
    meta.setJsonPrettified(true);
    meta.getOutputFields().clear();
    JsonEOutputField field2 = new JsonEOutputField();
    field2.setFieldName("Field2");
    field2.setElementName("campo2");
    JsonEOutputField field3 = new JsonEOutputField();
    field3.setFieldName("Field3");
    field3.setElementName("campo3");
    meta.getOutputFields().add(field2);
    meta.getOutputFields().add(field3);
    JsonEOutputKeyField key = new JsonEOutputKeyField("Field1");
    key.setElementName("recordKey");
    meta.getKeyFields().add(key);

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("Field1"));
    input.addValueMeta(new ValueMetaString("Field2"));
    input.addValueMeta(new ValueMetaInteger("Field3"));
    try (Harness h =
        new Harness(
            meta,
            input,
            new Object[] {"A", "B", 2L},
            new Object[] {"B", "C", 1L},
            new Object[] {"B", "C", 2L},
            new Object[] {"B", "D", 4L},
            new Object[] {"C", "F", 5L},
            new Object[] {"C", "F", 6L},
            new Object[] {"C", "V", 6L},
            new Object[] {"C", "B", 7L})) {
      h.run();
    }
    String content = read(base + "/out.json");
    assertTrue(content.contains("\n"), content);
    JsonNode file = parse(content);
    JsonNode result = file.get("result");
    assertEquals(3, result.size());
    assertEquals("A", result.get(0).get("recordKey").asText());
    assertEquals("B", result.get(0).get("lvl1Details").get("campo2").asText());
    assertEquals(2, result.get(0).get("lvl1Details").get("campo3").asInt());
    assertFalse(result.get(0).get("lvl1Details").isArray());
    assertEquals(3, result.get(1).get("lvl1Details").size());
    assertEquals(4, result.get(1).get("lvl1Details").get(2).get("campo3").asInt());
    assertEquals(4, result.get(2).get("lvl1Details").size());
    assertEquals("B", result.get(2).get("lvl1Details").get(3).get("campo2").asText());
    assertEquals(7, result.get(2).get("lvl1Details").get(3).get("campo3").asInt());
  }

  @Test
  void ndjsonAppendEscapesEmbeddedNewlines() throws Exception {
    JsonEOutputMeta meta = meta(JsonEOutputMeta.OperationType.WRITE_TO_FILE);
    meta.setNewlineDelimited(true);
    meta.getFileSettings().setFileAppended(true);
    try (Harness h = new Harness(meta, rowMeta("payload"), new Object[] {"a\nb"})) {
      h.run();
    }
    try (Harness h = new Harness(meta, rowMeta("payload"), new Object[] {"c\rd"})) {
      h.run();
    }
    String content = read(base + "/out.json");
    assertEquals(payload("a\nb") + "\n" + payload("c\rd") + "\n", content);
    List<String> lines = content.lines().toList();
    assertEquals(2, lines.size());
    assertEquals("a\nb", parse(lines.get(0)).get("payload").asText());
    assertEquals("c\rd", parse(lines.get(1)).get("payload").asText());
  }

  @Test
  void ndjsonSplitsRecordsWithoutOuterArrays() throws Exception {
    JsonEOutputMeta meta = meta(JsonEOutputMeta.OperationType.WRITE_TO_FILE);
    meta.setNewlineDelimited(true);
    meta.setUseArrayWithSingleInstance(true);
    meta.getFileSettings().setSplitOutputAfter(2);
    try (Harness h =
        new Harness(
            meta, rowMeta("payload"), new Object[] {"x"}, new Object[] {"y"}, new Object[] {"z"})) {
      h.run();
      assertEquals(3, h.written.size());
    }
    assertEquals(payload("x") + "\n" + payload("y") + "\n", read(base + "/out_0.json"));
    assertEquals(payload("z") + "\n", read(base + "/out_1.json"));
    try (FileObject extra = HopVfs.getFileObject(base + "/out_2.json")) {
      assertFalse(extra.exists());
    }
  }

  @Test
  void ndjsonWrapsEachCompleteGroup() throws Exception {
    JsonEOutputMeta meta = meta(JsonEOutputMeta.OperationType.BOTH);
    meta.setNewlineDelimited(true);
    meta.setJsonBloc("data");
    JsonEOutputKeyField key = new JsonEOutputKeyField("grp");
    key.setElementName("group");
    meta.getKeyFields().add(key);
    try (Harness h =
        new Harness(
            meta,
            rowMeta("grp", "payload"),
            new Object[] {"a", "x"},
            new Object[] {"a", "y"},
            new Object[] {"b", "z"})) {
      h.run();
      assertEquals(2, h.written.size());
    }
    List<String> lines = read(base + "/out.json").lines().toList();
    assertEquals(2, lines.size());
    JsonNode first = parse(lines.get(0)).get("data");
    JsonNode last = parse(lines.get(1)).get("data");
    assertEquals("a", first.get("group").asText());
    assertEquals(2, first.get("rows").size());
    assertEquals("b", last.get("group").asText());
    assertEquals("z", last.get("rows").get("payload").asText());
  }

  @Test
  void ndjsonRejectsPrettyPrintingBeforeOpeningFile() throws Exception {
    JsonEOutputMeta meta = meta(JsonEOutputMeta.OperationType.WRITE_TO_FILE);
    meta.setNewlineDelimited(true);
    meta.setJsonPrettified(true);
    meta.getFileSettings().setDoNotOpenNewFileInit(false);
    try (Harness h = new Harness(meta, rowMeta("payload"), new Object[] {"x"})) {
      assertFalse(h.transform.init());
    }
    try (FileObject file = HopVfs.getFileObject(base + "/out.json")) {
      assertFalse(file.exists());
    }
  }

  @Test
  void ndjsonRejectsNonUtf8BeforeOpeningFile() throws Exception {
    JsonEOutputMeta meta = meta(JsonEOutputMeta.OperationType.WRITE_TO_FILE);
    meta.setNewlineDelimited(true);
    meta.setEncoding("UTF-16");
    meta.getFileSettings().setDoNotOpenNewFileInit(false);
    try (Harness h = new Harness(meta, rowMeta("payload"), new Object[] {"x"})) {
      assertFalse(h.transform.init());
    }
    try (FileObject file = HopVfs.getFileObject(base + "/out.json")) {
      assertFalse(file.exists());
    }
  }
}
