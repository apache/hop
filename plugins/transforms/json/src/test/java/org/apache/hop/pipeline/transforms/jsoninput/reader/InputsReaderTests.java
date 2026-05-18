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

package org.apache.hop.pipeline.transforms.jsoninput.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInput;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputData;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputField;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

/**
 * Unit tests for {@link InputsReader}. Placed in this package to exercise protected {@link
 * InputsReader#getFieldIterator()} and nested iterators.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class InputsReaderTests {

  private static final String BASE_RAM_DIR = "ram:///inputsReaderTest/";
  private static final String BASIC_JSON =
      "{\"store\":{\"book\":[{\"price\":1.0},{\"price\":2.0}]}}";

  private TransformMockHelper<JsonInputMeta, JsonInputData> helper;

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>("inputs reader test", JsonInputMeta.class, JsonInputData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() throws Exception {
    helper.cleanUp();
    try (FileObject baseDir = HopVfs.getFileObject(BASE_RAM_DIR)) {
      baseDir.deleteAll();
    }
  }

  private static final class RecordingErrorHandler implements InputsReader.ErrorHandler {
    final List<Exception> errors = new ArrayList<>();
    final List<FileSystemException> fileOpenErrors = new ArrayList<>();
    final List<FileSystemException> fileCloseErrors = new ArrayList<>();

    @Override
    public void error(Exception thrown) {
      errors.add(thrown);
    }

    @Override
    public void fileOpenError(FileObject file, FileSystemException exception) {
      fileOpenErrors.add(exception);
    }

    @Override
    public void fileCloseError(FileObject file, FileSystemException exception) {
      fileCloseErrors.add(exception);
    }
  }

  /** JsonInput.init() requires at least one field so FastJsonReader can be built. */
  private static void ensureMinimalInputField(JsonInputMeta meta) {
    if (meta.getInputFields().isEmpty()) {
      JsonInputField f = new JsonInputField();
      f.setName("_");
      f.setPath("$");
      f.setType(IValueMeta.TYPE_STRING);
      meta.getInputFields().add(f);
    }
  }

  private JsonInputMeta createFileListMeta(final List<FileObject> files) {
    JsonInputMeta meta =
        new JsonInputMeta() {
          @Override
          public FileInputList getFileInputList(IVariables variables) {
            return new FileInputList() {
              @Override
              public List<FileObject> getFiles() {
                return files;
              }

              @Override
              public int nrOfFiles() {
                return files.size();
              }
            };
          }
        };
    meta.setDefault();
    meta.setInFields(false);
    ensureMinimalInputField(meta);
    return meta;
  }

  private JsonInput newJsonInputWithoutInputRows(JsonInputMeta meta, JsonInputData data) {
    ensureMinimalInputField(meta);
    JsonInput jsonInput =
        new JsonInput(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    jsonInput.init();
    return jsonInput;
  }

  private JsonInput newJsonInputWithStringRows(
      JsonInputMeta meta, JsonInputData data, String columnName, Object[]... rows)
      throws HopPluginException {
    ensureMinimalInputField(meta);
    JsonInput jsonInput =
        new JsonInput(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(ValueMetaFactory.createValueMeta(columnName, IValueMeta.TYPE_STRING));
    jsonInput.setInputRowMeta(rowMeta);
    jsonInput.addRowSetToInputRowSets(helper.getMockInputRowSet(rows));
    jsonInput.init();
    return jsonInput;
  }

  private JsonInput newJsonInputWithJsonRows(
      JsonInputMeta meta, JsonInputData data, Object[]... rows) throws HopPluginException {
    ensureMinimalInputField(meta);
    JsonInput jsonInput =
        new JsonInput(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(ValueMetaFactory.createValueMeta("j", IValueMeta.TYPE_JSON));
    jsonInput.setInputRowMeta(rowMeta);
    jsonInput.addRowSetToInputRowSets(helper.getMockInputRowSet(rows));
    jsonInput.init();
    return jsonInput;
  }

  @Test
  void iterator_staticFileList_returnsStreamsForEachFile() throws Exception {
    try (FileObject f1 = HopVfs.getFileObject(BASE_RAM_DIR + "a.json")) {
      f1.createFile();
      try (OutputStream os = f1.getContent().getOutputStream()) {
        os.write(BASIC_JSON.getBytes(StandardCharsets.UTF_8));
      }
      JsonInputMeta meta = createFileListMeta(List.of(f1));
      JsonInputData data = new JsonInputData();
      JsonInput jsonInput = newJsonInputWithoutInputRows(meta, data);
      InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());

      var it = reader.iterator();
      assertTrue(it.hasNext());
      try (InputStream in = it.next()) {
        assertNotNull(in);
        String body = IOUtils.toString(in, StandardCharsets.UTF_8);
        assertTrue(body.contains("store"));
      }
      assertFalse(it.hasNext());
    }
  }

  @Test
  void iterator_cachesFileInputListOnData() throws Exception {
    try (FileObject f1 = HopVfs.getFileObject(BASE_RAM_DIR + "cached.json")) {
      f1.createFile();
      try (OutputStream os = f1.getContent().getOutputStream()) {
        os.write(BASIC_JSON.getBytes(StandardCharsets.UTF_8));
      }
      AtomicInteger listBuilds = new AtomicInteger();
      JsonInputMeta meta =
          new JsonInputMeta() {
            @Override
            public FileInputList getFileInputList(IVariables variables) {
              listBuilds.incrementAndGet();
              return new FileInputList() {
                @Override
                public List<FileObject> getFiles() {
                  return List.of(f1);
                }

                @Override
                public int nrOfFiles() {
                  return 1;
                }
              };
            }
          };
      meta.setDefault();
      meta.setInFields(false);
      ensureMinimalInputField(meta);
      JsonInputData data = new JsonInputData();
      JsonInput jsonInput = newJsonInputWithoutInputRows(meta, data);

      new InputsReader(jsonInput, meta, data, new RecordingErrorHandler()).iterator();
      assertEquals(1, listBuilds.get());
      assertNotNull(data.files);

      new InputsReader(jsonInput, meta, data, new RecordingErrorHandler()).iterator();
      assertEquals(1, listBuilds.get(), "Second iterator() must reuse data.files");
    }
  }

  @Test
  void iterator_acceptingFilenames_resolvesPathsFromField() throws Exception {
    try (FileObject f1 = HopVfs.getFileObject(BASE_RAM_DIR + "fromfield.json")) {
      f1.createFile();
      try (OutputStream os = f1.getContent().getOutputStream()) {
        os.write(BASIC_JSON.getBytes(StandardCharsets.UTF_8));
      }
      JsonInputMeta meta = new JsonInputMeta();
      meta.setDefault();
      ensureMinimalInputField(meta);
      meta.setInFields(true);
      meta.setFieldValue("pathcol");
      meta.setIsAFile(true);
      meta.getFileInput().setAcceptingFilenames(true);
      meta.getFileInput().setAcceptingField("pathcol");

      JsonInputData data = new JsonInputData();
      data.indexSourceField = 0;
      JsonInput jsonInput =
          newJsonInputWithStringRows(meta, data, "pathcol", new Object[] {f1.getName().getURI()});

      InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());
      var it = reader.iterator();
      assertTrue(it.hasNext());
      try (InputStream in = it.next()) {
        assertNotNull(in);
        assertTrue(IOUtils.toString(in, StandardCharsets.UTF_8).contains("store"));
      }
      assertFalse(it.hasNext());
    }
  }

  @Test
  void iterator_directFieldContent_wrapsStringAsUtf8Stream() throws Exception {
    JsonInputMeta meta = new JsonInputMeta();
    meta.setDefault();
    meta.setInFields(true);
    meta.setFieldValue("js");
    meta.setSourceAFile(false);
    meta.setReadUrl(false);

    JsonInputData data = new JsonInputData();
    data.indexSourceField = 0;
    JsonInput jsonInput = newJsonInputWithStringRows(meta, data, "js", new Object[] {BASIC_JSON});

    InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());
    var it = reader.iterator();
    assertTrue(it.hasNext());
    try (InputStream in = it.next()) {
      assertEquals(BASIC_JSON, IOUtils.toString(in, StandardCharsets.UTF_8));
    }
    assertFalse(it.hasNext());
  }

  @Test
  void iterator_readUrl_opensUrlStream() throws Exception {
    File tmp = File.createTempFile("inputsreader", ".json");
    tmp.deleteOnExit();
    Files.writeString(tmp.toPath(), BASIC_JSON, StandardCharsets.UTF_8);
    String url = tmp.toURI().toURL().toString();

    JsonInputMeta meta = new JsonInputMeta();
    meta.setDefault();
    ensureMinimalInputField(meta);
    meta.setInFields(true);
    meta.setFieldValue("loc");
    meta.setSourceAFile(false);
    meta.setReadUrl(true);

    JsonInputData data = new JsonInputData();
    data.indexSourceField = 0;
    JsonInput jsonInput = newJsonInputWithStringRows(meta, data, "loc", new Object[] {url});

    InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());
    var it = reader.iterator();
    assertTrue(it.hasNext());
    try (InputStream in = it.next()) {
      assertTrue(IOUtils.toString(in, StandardCharsets.UTF_8).contains("store"));
    }
    assertFalse(it.hasNext());
  }

  @Test
  void iterator_readUrl_badUrl_reportsErrorAndReturnsNullStream() throws Exception {
    RecordingErrorHandler handler = new RecordingErrorHandler();
    JsonInputMeta meta = new JsonInputMeta();
    meta.setDefault();
    ensureMinimalInputField(meta);
    meta.setInFields(true);
    meta.setFieldValue("loc");
    meta.setSourceAFile(false);
    meta.setReadUrl(true);

    JsonInputData data = new JsonInputData();
    data.indexSourceField = 0;
    JsonInput jsonInput = newJsonInputWithStringRows(meta, data, "loc", new Object[] {"bogus"});

    InputsReader reader = new InputsReader(jsonInput, meta, data, handler);
    var it = reader.iterator();
    assertTrue(it.hasNext());
    assertNull(it.next());
    assertEquals(1, handler.errors.size());
  }

  @Test
  void iterator_skipsEmptyFileWhenConfigured_thenReturnsNextStream() throws Exception {
    try (FileObject empty = HopVfs.getFileObject(BASE_RAM_DIR + "empty.json");
        FileObject good = HopVfs.getFileObject(BASE_RAM_DIR + "good.json")) {
      empty.createFile();
      good.createFile();
      try (OutputStream os = good.getContent().getOutputStream()) {
        os.write(BASIC_JSON.getBytes(StandardCharsets.UTF_8));
      }
      JsonInputMeta meta = createFileListMeta(List.of(empty, good));
      meta.setIgnoringEmptyFile(true);

      JsonInputData data = new JsonInputData();
      JsonInput jsonInput = newJsonInputWithoutInputRows(meta, data);
      InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());

      var it = reader.iterator();
      assertTrue(it.hasNext());
      try (InputStream in = it.next()) {
        assertNotNull(in);
        assertTrue(IOUtils.toString(in, StandardCharsets.UTF_8).contains("store"));
      }
      assertFalse(it.hasNext());
      assertFalse(data.skipEmptyFile);
    }
  }

  @Test
  void iterator_onNewFileRejectsWithoutSkip_returnsNull() throws Exception {
    try (FileObject f1 = HopVfs.getFileObject(BASE_RAM_DIR + "reject.json")) {
      f1.createFile();
      try (OutputStream os = f1.getContent().getOutputStream()) {
        os.write(BASIC_JSON.getBytes(StandardCharsets.UTF_8));
      }
      JsonInputMeta meta = createFileListMeta(List.of(f1));
      JsonInputData data = new JsonInputData();
      JsonInput jsonInput = newJsonInputWithoutInputRows(meta, data);
      JsonInput spyInput = spy(jsonInput);
      Mockito.doReturn(false).when(spyInput).onNewFile(org.mockito.ArgumentMatchers.any());

      InputsReader reader = new InputsReader(spyInput, meta, data, new RecordingErrorHandler());
      var it = reader.iterator();
      assertTrue(it.hasNext());
      assertNull(it.next());
      assertFalse(it.hasNext());
    }
  }

  @Test
  void getFieldIterator_yieldsFieldStringsAndEnds() throws Exception {
    JsonInputMeta meta = new JsonInputMeta();
    meta.setDefault();
    ensureMinimalInputField(meta);
    meta.setInFields(true);
    meta.setFieldValue("c");
    JsonInputData data = new JsonInputData();
    data.indexSourceField = 0;
    JsonInput jsonInput =
        newJsonInputWithStringRows(meta, data, "c", new Object[] {"a"}, new Object[] {"b"});

    InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());
    Iterator<String> fields = reader.getFieldIterator();
    assertTrue(fields.hasNext());
    assertEquals("a", fields.next());
    assertTrue(fields.hasNext());
    assertEquals("b", fields.next());
    assertFalse(fields.hasNext());
  }

  @Test
  void getFieldIterator_rowShorterThanIndex_returnsNull() throws Exception {
    JsonInputMeta meta = new JsonInputMeta();
    meta.setDefault();
    ensureMinimalInputField(meta);
    meta.setInFields(true);
    meta.setFieldValue("c");
    JsonInputData data = new JsonInputData();
    data.indexSourceField = 1;
    JsonInput jsonInput =
        newJsonInputWithStringRows(meta, data, "c", new Object[] {"only-one-col"});

    InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());
    Iterator<String> fields = reader.getFieldIterator();
    assertTrue(fields.hasNext());
    assertNull(fields.next());
    assertFalse(fields.hasNext());
  }

  @Test
  void getFieldIterator_nullCell_returnsNullString() throws Exception {
    JsonInputMeta meta = new JsonInputMeta();
    meta.setDefault();
    ensureMinimalInputField(meta);
    meta.setInFields(true);
    meta.setFieldValue("c");
    JsonInputData data = new JsonInputData();
    data.indexSourceField = 0;
    JsonInput jsonInput = newJsonInputWithStringRows(meta, data, "c", new Object[] {null});

    InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());
    Iterator<String> fields = reader.getFieldIterator();
    assertTrue(fields.hasNext());
    assertNull(fields.next());
    assertFalse(fields.hasNext());
  }

  @Test
  void getFieldIterator_nonStringCell_throwsClassCastException() throws Exception {
    JsonInputMeta meta = new JsonInputMeta();
    meta.setDefault();
    ensureMinimalInputField(meta);
    meta.setInFields(true);
    meta.setFieldValue("c");
    JsonInputData data = new JsonInputData();
    data.indexSourceField = 0;
    JsonInput jsonInput = newJsonInputWithStringRows(meta, data, "c", new Object[] {42});

    InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());
    Iterator<String> fields = reader.getFieldIterator();
    assertTrue(fields.hasNext());
    assertThrows(ClassCastException.class, fields::next);
  }

  @Test
  void jsonFieldIterator_yieldsJsonNodes() throws Exception {
    ObjectMapper mapper = HopJson.newMapper();
    JsonNode node = mapper.readTree(BASIC_JSON);

    JsonInputMeta meta = new JsonInputMeta();
    meta.setDefault();
    ensureMinimalInputField(meta);
    meta.setInFields(true);
    meta.setFieldValue("j");
    JsonInputData data = new JsonInputData();
    data.indexSourceField = 0;
    JsonInput jsonInput = newJsonInputWithJsonRows(meta, data, new Object[] {node});

    InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());
    Iterator<JsonNode> it = reader.jsonFieldIterator();
    assertTrue(it.hasNext());
    assertEquals(node, it.next());
    assertFalse(it.hasNext());
  }

  @Test
  void jsonFieldIterator_nonJsonCell_throwsClassCastException() throws Exception {
    JsonInputMeta meta = new JsonInputMeta();
    meta.setDefault();
    ensureMinimalInputField(meta);
    meta.setInFields(true);
    meta.setFieldValue("j");
    JsonInputData data = new JsonInputData();
    data.indexSourceField = 0;
    JsonInput jsonInput =
        newJsonInputWithStringRows(meta, data, "j", new Object[] {"not-json-node"});

    InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());
    Iterator<JsonNode> it = reader.jsonFieldIterator();
    assertTrue(it.hasNext());
    assertThrows(ClassCastException.class, it::next);
  }

  @Test
  void iterators_removeUnsupported() throws Exception {
    JsonInputMeta meta = new JsonInputMeta();
    meta.setDefault();
    meta.setInFields(true);
    meta.setFieldValue("js");
    meta.setSourceAFile(false);
    meta.setReadUrl(false);
    JsonInputData data = new JsonInputData();
    data.indexSourceField = 0;
    JsonInput jsonInput = newJsonInputWithStringRows(meta, data, "js", new Object[] {"{}"});

    InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());
    Iterator<InputStream> iterator = reader.iterator();
    assertThrows(UnsupportedOperationException.class, iterator::remove);

    InputsReader.StringFieldIterator fieldIterator = reader.getFieldIterator();
    assertThrows(UnsupportedOperationException.class, fieldIterator::remove);

    Iterator<JsonNode> jsonNodeIterator = reader.jsonFieldIterator();
    assertThrows(UnsupportedOperationException.class, jsonNodeIterator::remove);
  }

  @Test
  void iterator_inFieldsAndSourceIsFile_usesFileBranch() throws Exception {
    final String path = BASE_RAM_DIR + "isFile.json";
    try (FileObject writeTo = HopVfs.getFileObject(path)) {
      writeTo.createFile();
      try (OutputStream os = writeTo.getContent().getOutputStream()) {
        os.write(BASIC_JSON.getBytes(StandardCharsets.UTF_8));
      }
    }

    // New FileObject for the list so size/exists match what onNewFile + getInputStream see (RAM
    // VFS).
    try (FileObject f1 = HopVfs.getFileObject(path)) {
      assertTrue(f1.exists(), "file must exist before iterator");
      assertTrue(f1.getContent().getSize() > 0, "file must be non-empty before iterator");

      JsonInputMeta meta = createFileListMeta(List.of(f1));
      meta.setInFields(true);
      // Use setIsAFile (not only Lombok setSourceAFile) so sourceAFile is set on anonymous
      // JsonInputMeta subclasses.
      meta.setIsAFile(true);
      meta.getFileInput().setAcceptingFilenames(false);
      meta.setFieldValue("_");

      JsonInputData data = new JsonInputData();
      JsonInput jsonInput = newJsonInputWithoutInputRows(meta, data);
      InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());
      var it = reader.iterator();
      assertTrue(it.hasNext(), "!inFields || getIsAFile must be true to use file list iterator");
      try (InputStream in = it.next()) {
        assertNotNull(in, "expected stream after onNewFile accepted the file");
        assertTrue(IOUtils.toString(in, StandardCharsets.UTF_8).contains("store"));
      }
      assertFalse(it.hasNext());
    }
  }

  @Test
  void iterator_whenNoFiles_hasNextFalse() {
    JsonInputMeta meta = createFileListMeta(Collections.emptyList());
    JsonInputData data = new JsonInputData();
    JsonInput jsonInput = newJsonInputWithoutInputRows(meta, data);
    InputsReader reader = new InputsReader(jsonInput, meta, data, new RecordingErrorHandler());
    assertFalse(reader.iterator().hasNext());
  }
}
