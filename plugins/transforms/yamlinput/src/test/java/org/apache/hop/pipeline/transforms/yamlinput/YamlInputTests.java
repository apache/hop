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

package org.apache.hop.pipeline.transforms.yamlinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.FileWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;

/** Unit test for {@link YamlInput} */
class YamlInputTests {
  private TransformMockHelper<YamlInputMeta, YamlInputData> helper;
  @TempDir private Path tempDir;
  @Mock private IVariables variables;

  @BeforeEach
  void setup() throws Exception {
    HopEnvironment.init();

    helper = new TransformMockHelper<>("Yaml_test", YamlInputMeta.class, YamlInputData.class);
    Mockito.doReturn(helper.iLogChannel)
        .when(helper.logChannelFactory)
        .create(any(), any(ILoggingObject.class));

    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @Test
  void testInit_shouldBuildRowMeta() {
    YamlInputField field = new YamlInputField();
    field.setPath("user.name");
    field.setType(IValueMeta.TYPE_STRING);
    field.setTrimType(YamlInputField.TYPE_TRIM_BOTH);

    YamlInputMeta meta = helper.iTransformMeta;
    YamlInputData data = helper.iTransformData;
    PipelineMeta plMeta = helper.pipelineMeta;
    Pipeline pipeline = helper.pipeline;
    YamlInput input = new YamlInput(helper.transformMeta, meta, data, 0, plMeta, pipeline);

    when(meta.getInputFields()).thenReturn(List.of(field));

    boolean result = input.init();
    assertTrue(result);

    assertNotNull(input.getData().rowMeta);
    assertEquals(1, input.getData().nrInputFields);
  }

  @Test
  void testHandleMissingFiles_shouldThrowException() {
    FileObject mockFile = mock(FileObject.class);
    FileInputList fileList = mock(FileInputList.class);

    when(fileList.getNonExistentFiles()).thenReturn(List.of(mockFile));

    YamlInputMeta meta = helper.iTransformMeta;
    YamlInputData data = helper.iTransformData;
    PipelineMeta plMeta = helper.pipelineMeta;
    Pipeline pipeline = helper.pipeline;
    data.files = fileList;
    YamlInput transform = new YamlInput(helper.transformMeta, meta, data, 0, plMeta, pipeline);

    InvocationTargetException ex =
        assertThrows(
            InvocationTargetException.class,
            () -> MethodUtils.invokeMethod(transform, true, "handleMissingFiles"));

    Throwable exception = ex.getTargetException();
    assertInstanceOf(HopException.class, exception);
  }

  @Test
  void testGetRowData_shouldMergeYamlAndInputRow() throws Exception {
    YamlInputMeta meta = helper.iTransformMeta;
    YamlInputData data = helper.iTransformData;
    PipelineMeta plMeta = helper.pipelineMeta;
    Pipeline pipeline = helper.pipeline;
    YamlInput transform = new YamlInput(helper.transformMeta, meta, data, 0, plMeta, pipeline);

    // mock yaml reader
    YamlReader yamlReader = mock(YamlReader.class);
    Object[] yamlRow = new Object[] {"alice", 18};

    data.yaml = yamlReader;
    data.rowMeta = new RowMeta();
    data.totalPreviousFields = 1;
    data.totalOutStreamFields = 3;
    data.totalOutFields = 1;

    data.readRow = new Object[] {"file1.yml"};
    when(yamlReader.getRow(any())).thenReturn(yamlRow);

    Object[] result = (Object[]) MethodUtils.invokeMethod(transform, true, "getRowData");

    assertNotNull(result);
    assertEquals("file1.yml", result[0]);
    assertEquals("alice", result[1]);
    assertEquals(18, result[2]);
  }

  @Test
  void testOpenNextFile_shouldAdvanceIndex() throws Exception {
    YamlInputMeta meta = helper.iTransformMeta;
    YamlInputData data = helper.iTransformData;
    PipelineMeta plMeta = helper.pipelineMeta;
    Pipeline pipeline = helper.pipeline;
    YamlInput transform = new YamlInput(helper.transformMeta, meta, data, 0, plMeta, pipeline);

    Path tempFile = tempDir.resolve("test.yaml");
    try (FileWriter writer = new FileWriter(tempFile.toFile())) {
      writer.write("""
					name: Alice
					age: 30
					""");
    }

    data.fileIndex = 0;

    data.files = mock(FileInputList.class);
    when(data.files.nrOfFiles()).thenReturn(1);
    when(data.files.getFile(0)).thenReturn(HopVfs.getFileObject(tempFile.toString()));

    boolean result = (boolean) MethodUtils.invokeMethod(transform, true, "openNextFile");
    assertTrue(result);
    assertEquals(1, data.fileIndex);
    assertNotNull(data.yaml);
  }

  /**
   * {@link YamlInput#processRow()} in file mode: reads a YAML map, emits one row via {@code
   * putRow}, then returns false when no more rows.
   */
  @Test
  void testProcessRow_fileMode_emitsOneRowThenFinishes() throws Exception {
    Path tempFile = tempDir.resolve("processRow-one.yaml");
    Files.writeString(tempFile, """
						name: Alice
						age: 30
						""");

    YamlInputMeta meta = new YamlInputMeta();
    meta.setInFields(false);
    meta.setDoNotFailIfNoFile(true);
    YamlInputField fName = new YamlInputField();
    fName.setName("name");
    fName.setPath("name");
    fName.setType(IValueMeta.TYPE_STRING);
    YamlInputField fAge = new YamlInputField();
    fAge.setName("age");
    fAge.setPath("age");
    fAge.setType(IValueMeta.TYPE_INTEGER);
    meta.getInputFields().add(fName);
    meta.getInputFields().add(fAge);
    YamlInputMeta.YamlFile yf = new YamlInputMeta.YamlFile();
    yf.setFilename(tempFile.toAbsolutePath().toString());
    yf.setFileMask(".*\\.ya?ml");
    yf.setFileRequired(false);
    meta.getYamlFiles().add(yf);

    YamlInputData data = new YamlInputData();
    YamlInput transform =
        new YamlInput(helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    transform = spy(transform);

    List<Object[]> written = new ArrayList<>();
    doAnswer(
            invocation -> {
              Object[] row = invocation.getArgument(1);
              written.add(Arrays.copyOf(row, row.length));
              return null;
            })
        .when(transform)
        .putRow(any(IRowMeta.class), any());

    assertTrue(transform.init());
    assertTrue(transform.processRow(), "first processRow should emit a row");
    assertFalse(transform.processRow(), "second processRow should end (no more rows)");
    assertEquals(1, written.size());
    assertEquals("Alice", written.getFirst()[0]);
    assertEquals(30L, ((Number) written.getFirst()[1]).longValue());
  }

  /**
   * Row limit stops {@link YamlInput#processRow()} after the configured number of output rows
   * (single-document map still yields only one row when limit is 1).
   */
  @Test
  void testProcessRow_rowLimit_stopsAfterOneRow() throws Exception {
    Path tempFile = tempDir.resolve("processRow-limit.yaml");
    Files.writeString(tempFile, """
						name: Bob
						age: 40
						""");

    YamlInputMeta meta = new YamlInputMeta();
    meta.setInFields(false);
    meta.setDoNotFailIfNoFile(true);
    meta.setRowLimit(1);
    YamlInputField fName = new YamlInputField();
    fName.setName("name");
    fName.setPath("name");
    fName.setType(IValueMeta.TYPE_STRING);
    YamlInputField fAge = new YamlInputField();
    fAge.setName("age");
    fAge.setPath("age");
    fAge.setType(IValueMeta.TYPE_INTEGER);
    meta.getInputFields().add(fName);
    meta.getInputFields().add(fAge);
    YamlInputMeta.YamlFile yf = new YamlInputMeta.YamlFile();
    yf.setFilename(tempFile.toAbsolutePath().toString());
    yf.setFileMask(".*\\.ya?ml");
    yf.setFileRequired(false);
    meta.getYamlFiles().add(yf);

    YamlInputData data = new YamlInputData();
    YamlInput transform =
        spy(
            new YamlInput(
                helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline));

    List<Object[]> written = new ArrayList<>();
    doAnswer(
            invocation -> {
              Object[] row = invocation.getArgument(1);
              written.add(Arrays.copyOf(row, row.length));
              return null;
            })
        .when(transform)
        .putRow(any(IRowMeta.class), any());

    assertTrue(transform.init());
    assertFalse(
        transform.processRow(),
        "one row is emitted then processRow returns false when row limit is exceeded");
    assertFalse(transform.processRow(), "no further rows after end of YAML");
    assertEquals(1, written.size());
    assertEquals("Bob", written.getFirst()[0]);
    assertEquals(40L, ((Number) written.getFirst()[1]).longValue());
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }
}
