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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Node;

/** Unit test for {@link YamlInputMeta} */
class YamlInputMetaTest {

  @BeforeEach
  void beforeEach() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    String[] classNames = {
      ValueMetaString.class.getName(),
      ValueMetaInteger.class.getName(),
      ValueMetaDate.class.getName(),
      ValueMetaNumber.class.getName(),
      ValueMetaJson.class.getName()
    };
    for (String className : classNames) {
      registry.registerPluginClass(className, ValueMetaPluginType.class, ValueMetaPlugin.class);
    }
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    YamlInputMeta meta =
        TransformSerializationTestUtil.testSerialization("/yaml-input.xml", YamlInputMeta.class);

    assertTrue(meta.isIncludeFilename());
    assertEquals("includeField", meta.getFilenameField());
    assertTrue(meta.isIncludeRowNumber());
    assertEquals("rowNumField", meta.getRowNumberField());
    assertTrue(meta.isValidating());
    assertTrue(meta.isAddingResultFile());
    assertTrue(meta.isIgnoringEmptyFile());
    assertTrue(meta.isDoNotFailIfNoFile());
    assertEquals("rowNumField", meta.getRowNumberField());
    assertEquals("UTF-8", meta.getEncoding());
    assertEquals(999L, meta.getRowLimit());
    assertTrue(meta.isSourceFile());
    assertTrue(meta.isInFields());
    assertEquals("filenameField", meta.getYamlField());

    assertEquals(2, meta.getYamlFiles().size());
    YamlInputMeta.YamlFile file = meta.getYamlFiles().getFirst();
    assertEquals("folder1/", file.getFilename());
    assertEquals(".*\\.yaml", file.getFileMask());
    assertTrue(file.isFileRequired());
    assertTrue(file.isIncludingSubFolders());
    file = meta.getYamlFiles().get(1);
    assertEquals("folder1/", file.getFilename());
    assertEquals(".*\\.yml", file.getFileMask());
    assertTrue(file.isFileRequired());
    assertTrue(file.isIncludingSubFolders());

    testSerializationRoundTrip_1(meta);
  }

  private void testSerializationRoundTrip_1(YamlInputMeta meta) {
    assertEquals(3, meta.getInputFields().size());
    YamlInputField field = meta.getInputFields().getFirst();
    assertEquals("field1", field.getName());
    assertEquals("key1", field.getPath());
    assertEquals(IValueMeta.TYPE_STRING, field.getType());
    assertEquals(100, field.getLength());
    assertEquals(-1, field.getPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_LEFT, field.getTrimType());
    field = meta.getInputFields().get(1);
    assertEquals("field2", field.getName());
    assertEquals("key2", field.getPath());
    assertEquals(IValueMeta.TYPE_INTEGER, field.getType());
    assertEquals(7, field.getLength());
    assertEquals(0, field.getPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_RIGHT, field.getTrimType());
    field = meta.getInputFields().getLast();
    assertEquals("field3", field.getName());
    assertEquals("key3", field.getPath());
    assertEquals(IValueMeta.TYPE_NUMBER, field.getType());
    assertEquals(9, field.getLength());
    assertEquals(2, field.getPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_BOTH, field.getTrimType());
  }

  @Test
  void testDefaultConstructor() {
    YamlInputMeta meta = new YamlInputMeta();

    assertNotNull(meta.getYamlFiles());
    assertNotNull(meta.getInputFields());

    assertTrue(meta.getYamlFiles().isEmpty());
    assertTrue(meta.getInputFields().isEmpty());

    assertEquals("", meta.getFilenameField());
    assertEquals("", meta.getRowNumberField());
    assertEquals("", meta.getYamlField());
    assertEquals(0, meta.getRowLimit());

    assertTrue(meta.isDoNotFailIfNoFile());
  }

  @Test
  void testCloneDeepCopy() {
    YamlInputMeta meta = new YamlInputMeta();

    YamlInputMeta.YamlFile file = new YamlInputMeta.YamlFile();
    file.setFilename("test.yaml");
    meta.getYamlFiles().add(file);

    YamlInputField field = new YamlInputField("name");
    meta.getInputFields().add(field);

    YamlInputMeta clone = meta.clone();

    assertNotSame(meta, clone);

    assertNotSame(meta.getYamlFiles(), clone.getYamlFiles());
    assertNotSame(meta.getInputFields(), clone.getInputFields());

    assertNotSame(meta.getYamlFiles().getFirst(), clone.getYamlFiles().getFirst());
    assertNotSame(meta.getInputFields().getFirst(), clone.getInputFields().getFirst());
  }

  @Test
  void testGetFields_basic() throws Exception {
    YamlInputMeta meta = new YamlInputMeta();

    YamlInputField field = new YamlInputField("id");
    field.setType(IValueMeta.TYPE_INTEGER);
    field.setLength(10);
    field.setPrecision(0);

    meta.getInputFields().add(field);

    IRowMeta rowMeta = new RowMeta();
    Variables vars = new Variables();

    meta.getFields(rowMeta, "testTransform", null, null, vars, null);

    assertEquals(1, rowMeta.size());

    IValueMeta v = rowMeta.getValueMeta(0);
    assertEquals("id", v.getName());
    assertEquals(IValueMeta.TYPE_INTEGER, v.getType());
    assertEquals(10, v.getLength());
  }

  @Test
  void testGetFields_withExtraFields() throws Exception {
    YamlInputMeta meta = new YamlInputMeta();

    meta.setIncludeFilename(true);
    meta.setFilenameField("filename");

    meta.setIncludeRowNumber(true);
    meta.setRowNumberField("rowNum");

    IRowMeta rowMeta = new RowMeta();
    Variables vars = new Variables();

    meta.getFields(rowMeta, "t", null, null, vars, null);

    assertEquals(2, rowMeta.size());
    assertEquals("filename", rowMeta.getValueMeta(0).getName());
    assertEquals("rowNum", rowMeta.getValueMeta(1).getName());
  }

  @Test
  void testGetFiles(@TempDir Path tempDir) throws IOException {
    YamlInputMeta meta = new YamlInputMeta();

    Path tempFile = tempDir.resolve("test.yaml");
    Files.createFile(tempFile);

    YamlInputMeta.YamlFile file = new YamlInputMeta.YamlFile();
    file.setFilename(tempFile.toString());
    file.setFileMask(".*yaml");

    meta.getYamlFiles().add(file);

    Variables vars = new Variables();
    FileInputList list = meta.getFiles(vars);

    assertNotNull(list);
    assertFalse(list.getFiles().isEmpty());
  }

  @Test
  void testCheck_noInput() {
    YamlInputMeta meta = new YamlInputMeta();
    List<ICheckResult> remarks = new ArrayList<>();

    meta.check(
        remarks,
        null,
        new TransformMeta(),
        null,
        new String[] {},
        null,
        null,
        new Variables(),
        null);

    assertFalse(remarks.isEmpty());
  }

  @Test
  void testCheck_noFields() {
    YamlInputMeta meta = new YamlInputMeta();
    List<ICheckResult> remarks = new ArrayList<>();

    meta.check(
        remarks,
        null,
        new TransformMeta(),
        null,
        new String[] {"input"},
        null,
        null,
        new Variables(),
        null);

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void testCheck_inFieldsMode_ok() {
    YamlInputMeta meta = new YamlInputMeta();
    meta.setInFields(true);
    meta.setYamlField("yaml");

    meta.getInputFields().add(new YamlInputField("name"));

    List<ICheckResult> remarks = new ArrayList<>();

    meta.check(
        remarks,
        null,
        new TransformMeta(),
        null,
        new String[] {"input"},
        null,
        null,
        new Variables(),
        null);

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK));
  }

  @Test
  void testConvertLegacyXml(@TempDir Path tempDir) throws Exception {
    Path tempFile = tempDir.resolve("test.yaml");
    Files.createFile(tempFile);

    String xml =
        String.format(
            """
				<file>
				      <name>%s</name>
				      <filemask>.*\\.yaml</filemask>
				      <file_required>Y</file_required>
				      <include_subfolders>Y</include_subfolders>
				  </file>
				""",
            tempFile);
    Node node = XmlHandler.loadXmlString(xml);

    YamlInputMeta meta = new YamlInputMeta();
    meta.convertLegacyXml(node);

    assertEquals(1, meta.getYamlFiles().size());

    YamlInputMeta.YamlFile file = meta.getYamlFiles().getFirst();
    assertEquals(tempFile.toString(), file.getFilename());
    assertTrue(file.isFileRequired());
    assertTrue(file.isIncludingSubFolders());
  }
}
