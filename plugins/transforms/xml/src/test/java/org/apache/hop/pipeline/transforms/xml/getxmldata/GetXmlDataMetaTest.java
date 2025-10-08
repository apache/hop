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

package org.apache.hop.pipeline.transforms.xml.getxmldata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class GetXmlDataMetaTest {

  private GetXmlDataMeta meta;

  @BeforeEach
  void setUp() {
    meta = new GetXmlDataMeta();
  }

  @Test
  void testSetDefault() {
    meta.setDefault();

    assertFalse(meta.isIncludeFilename());
    assertFalse(meta.isIncludeRowNumber());
    assertEquals(0L, meta.getRowLimit());
    assertFalse(meta.isInFields());
    assertFalse(meta.isAFile());
    assertFalse(meta.isAddResultFile());
    assertFalse(meta.isValidating());
    assertFalse(meta.isReadUrl());
    assertFalse(meta.isIgnoreEmptyFile());
    assertTrue(meta.isDoNotFailIfNoFile()); // Defaults to true
    assertFalse(meta.isIgnoreComments());
    assertFalse(meta.isUseToken());
    assertFalse(meta.isNameSpaceAware());
  }

  @Test
  void testFileConfiguration() {
    // Test file-related properties using new GetXmlFileItem API

    GetXmlFileItem file1 = new GetXmlFileItem("/path/file1.xml", "*.xml", "*test*.xml", "Y", "Y");

    GetXmlFileItem file2 = new GetXmlFileItem("/path/file2.xml", "*.XML", "*tmp*.xml", "N", "Y");

    java.util.List<GetXmlFileItem> filesList = new java.util.ArrayList<>();
    filesList.add(file1);
    filesList.add(file2);

    meta.setFilesList(filesList);

    assertNotNull(meta.getFilesList());
    assertEquals(2, meta.getFilesList().size());
    assertEquals("/path/file1.xml", meta.getFilesList().get(0).getFileName());
    assertEquals("*.xml", meta.getFilesList().get(0).getFileMask());
    assertEquals("*test*.xml", meta.getFilesList().get(0).getExcludeFileMask());
    assertEquals("Y", meta.getFilesList().get(0).getFileRequired());
    assertEquals("Y", meta.getFilesList().get(0).getIncludeSubFolders());
  }

  @Test
  void testOutputFieldConfiguration() {
    // Test filename field
    meta.setIncludeFilename(true);
    assertTrue(meta.isIncludeFilename());

    meta.setFilenameField("xml_filename");
    assertEquals("xml_filename", meta.getFilenameField());

    // Test row number field
    meta.setIncludeRowNumber(true);
    assertTrue(meta.isIncludeRowNumber());

    meta.setRowNumberField("row_nr");
    assertEquals("row_nr", meta.getRowNumberField());

    meta.setRowLimit(1000L);
    assertEquals(1000L, meta.getRowLimit());

    // Test additional fields
    meta.setShortFileFieldName("short_name");
    assertEquals("short_name", meta.getShortFileFieldName());

    meta.setPathFieldName("path");
    assertEquals("path", meta.getPathFieldName());

    meta.setHiddenFieldName("hidden");
    assertEquals("hidden", meta.getHiddenFieldName());

    meta.setLastModificationTimeFieldName("mod_time");
    assertEquals("mod_time", meta.getLastModificationTimeFieldName());

    meta.setUriNameFieldName("uri");
    assertEquals("uri", meta.getUriNameFieldName());

    meta.setRootUriNameFieldName("root_uri");
    assertEquals("root_uri", meta.getRootUriNameFieldName());

    meta.setExtensionFieldName("extension");
    assertEquals("extension", meta.getExtensionFieldName());

    meta.setSizeFieldName("size");
    assertEquals("size", meta.getSizeFieldName());
  }

  @Test
  void testXmlConfiguration() {
    // Test XML-related settings
    meta.setXmlField("xml_content");
    assertEquals("xml_content", meta.getXmlField());

    meta.setInFields(true);
    assertTrue(meta.isInFields());

    meta.setAFile(true);
    assertTrue(meta.isAFile());

    meta.setLoopXPath("/root/item");
    assertEquals("/root/item", meta.getLoopXPath());

    meta.setEncoding("ISO-8859-1");
    assertEquals("ISO-8859-1", meta.getEncoding());

    meta.setPrunePath("/root/prune");
    assertEquals("/root/prune", meta.getPrunePath());
  }

  @Test
  void testParsingFlags() {
    // Test various parsing flags
    meta.setValidating(true);
    assertTrue(meta.isValidating());

    meta.setNameSpaceAware(true);
    assertTrue(meta.isNameSpaceAware());

    meta.setIgnoreComments(true);
    assertTrue(meta.isIgnoreComments());

    meta.setReadUrl(true);
    assertTrue(meta.isReadUrl());

    meta.setUseToken(true);
    assertTrue(meta.isUseToken());

    meta.setAddResultFile(true);
    assertTrue(meta.isAddResultFile());

    meta.setIgnoreEmptyFile(true);
    assertTrue(meta.isIgnoreEmptyFile());

    meta.setDoNotFailIfNoFile(true);
    assertTrue(meta.isDoNotFailIfNoFile());
  }

  @Test
  void testInputFieldsConfiguration() {
    java.util.List<GetXmlDataField> fields = new java.util.ArrayList<>();
    for (int i = 0; i < 3; i++) {
      GetXmlDataField field = new GetXmlDataField("field" + i);
      field.setXPath("/path" + i);
      field.setType(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
      fields.add(field);
    }

    meta.setInputFields(fields);
    assertNotNull(meta.getInputFields());
    assertEquals(3, meta.getInputFields().size());
    assertEquals("field0", meta.getInputFields().get(0).getName());
    assertEquals("/path0", meta.getInputFields().get(0).getXPath());
  }

  @Test
  void testInputFieldsList() {
    // Test that input fields is a list
    java.util.List<GetXmlDataField> fields = new java.util.ArrayList<>();
    fields.add(new GetXmlDataField("field1"));
    fields.add(new GetXmlDataField("field2"));
    fields.add(new GetXmlDataField("field3"));

    meta.setInputFields(fields);

    assertNotNull(meta.getInputFields());
    assertEquals(3, meta.getInputFields().size());

    // Files list is managed separately
    if (meta.getFilesList() == null) {
      meta.setFilesList(new java.util.ArrayList<>());
    }
    assertNotNull(meta.getFilesList());
  }

  @Test
  void testClone() {
    // Set up original meta
    java.util.List<GetXmlFileItem> filesList = new java.util.ArrayList<>();
    filesList.add(new GetXmlFileItem("file1.xml", "*.xml", "", "N", "N"));
    filesList.add(new GetXmlFileItem("file2.xml", "*.XML", "", "N", "N"));
    meta.setFilesList(filesList);

    java.util.List<GetXmlDataField> inputFields = new java.util.ArrayList<>();
    inputFields.add(new GetXmlDataField("field1"));
    inputFields.add(new GetXmlDataField("field2"));
    meta.setInputFields(inputFields);

    meta.setLoopXPath("/root/item");
    meta.setEncoding("UTF-8");
    meta.setInFields(true);

    // Clone
    GetXmlDataMeta cloned = (GetXmlDataMeta) meta.clone();

    // Verify
    assertNotNull(cloned);
    assertNotNull(cloned.getFilesList());
    assertEquals(2, cloned.getFilesList().size());
    assertEquals("file1.xml", cloned.getFilesList().get(0).getFileName());
    assertEquals("file2.xml", cloned.getFilesList().get(1).getFileName());
    assertEquals(2, cloned.getInputFields().size());
    assertEquals("field1", cloned.getInputFields().get(0).getName());
    assertEquals("/root/item", cloned.getLoopXPath());
    assertEquals("UTF-8", cloned.getEncoding());
    assertTrue(cloned.isInFields());
  }

  @Test
  void testGetFields() throws Exception {
    // Set up meta with fields using List
    java.util.List<GetXmlDataField> fields = new java.util.ArrayList<>();

    GetXmlDataField field1 = new GetXmlDataField("xml_field1");
    field1.setType(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
    fields.add(field1);

    GetXmlDataField field2 = new GetXmlDataField("xml_field2");
    field2.setType(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_INTEGER));
    fields.add(field2);

    meta.setInputFields(fields);

    meta.setIncludeFilename(true);
    meta.setFilenameField("filename");
    meta.setIncludeRowNumber(true);
    meta.setRowNumberField("rownr");

    RowMeta rowMeta = new RowMeta();
    Variables variables = new Variables();

    meta.getFields(rowMeta, "GetXMLData", null, null, variables, null);

    // Should have 4 fields: 2 XML fields + filename + row number
    assertEquals(4, rowMeta.size());
    assertNotNull(rowMeta.searchValueMeta("xml_field1"));
    assertNotNull(rowMeta.searchValueMeta("xml_field2"));
    assertNotNull(rowMeta.searchValueMeta("filename"));
    assertNotNull(rowMeta.searchValueMeta("rownr"));
  }

  @Test
  void testGetXml() throws Exception {
    java.util.List<GetXmlDataField> fields = new java.util.ArrayList<>();
    GetXmlDataField field = new GetXmlDataField("test_field");
    field.setXPath("/root/item");
    field.setType(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING));
    fields.add(field);
    meta.setInputFields(fields);

    meta.setLoopXPath("/root/data");
    meta.setEncoding("UTF-8");
    meta.setInFields(true);
    meta.setXmlField("xml_content");

    String xml = meta.getXml();

    assertNotNull(xml);
    // Files are serialized via HopMetadataProperty annotations, not in getXml()
    // Check for what's actually in the XML output from getXml()
    assertTrue(xml.contains("/root/data"));
    assertTrue(xml.contains("UTF-8"));
    assertTrue(xml.contains("xml_content"));
    assertTrue(xml.contains("test_field"));
  }

  @Test
  void testConstants() {
    assertEquals("@", GetXmlDataMeta.AT);
    assertEquals("/", GetXmlDataMeta.N0DE_SEPARATOR);
    assertEquals("      ", GetXmlDataMeta.CONST_SPACES);
    assertEquals("field", GetXmlDataMeta.CONST_FIELD);
    assertEquals("N", GetXmlDataMeta.RequiredFilesCode[0]);
    assertEquals("Y", GetXmlDataMeta.RequiredFilesCode[1]);
  }

  @Test
  void testGetFieldsWithAdditionalFields() throws Exception {
    // Initialize with empty input fields list
    meta.setInputFields(new java.util.ArrayList<>());

    meta.setShortFileFieldName("short_filename");
    meta.setPathFieldName("path");
    meta.setHiddenFieldName("is_hidden");
    meta.setLastModificationTimeFieldName("last_modified");
    meta.setUriNameFieldName("uri");
    meta.setRootUriNameFieldName("root_uri");
    meta.setExtensionFieldName("ext");
    meta.setSizeFieldName("file_size");

    RowMeta rowMeta = new RowMeta();
    Variables variables = new Variables();

    meta.getFields(rowMeta, "GetXMLData", null, null, variables, null);

    assertNotNull(rowMeta.searchValueMeta("short_filename"));
    assertNotNull(rowMeta.searchValueMeta("path"));
    assertNotNull(rowMeta.searchValueMeta("is_hidden"));
    assertNotNull(rowMeta.searchValueMeta("last_modified"));
    assertNotNull(rowMeta.searchValueMeta("uri"));
    assertNotNull(rowMeta.searchValueMeta("root_uri"));
    assertNotNull(rowMeta.searchValueMeta("ext"));
    assertNotNull(rowMeta.searchValueMeta("file_size"));
  }
}
