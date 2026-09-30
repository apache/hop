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
}
