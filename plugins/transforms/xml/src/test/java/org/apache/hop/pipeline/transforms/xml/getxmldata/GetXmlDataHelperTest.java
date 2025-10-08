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

import static org.apache.hop.pipeline.transforms.xml.getxmldata.GetXmlDataField.getElementTypeDesc;
import static org.apache.hop.pipeline.transforms.xml.getxmldata.GetXmlDataField.getResultTypeCode;
import static org.apache.hop.pipeline.transforms.xml.getxmldata.GetXmlDataField.getTrimTypeCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Additional unit tests for GetXmlData components focusing on configuration scenarios. */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class GetXmlDataHelperTest {

  private GetXmlDataMeta meta;
  private GetXmlDataData data;

  @BeforeEach
  void setUp() {
    meta = new GetXmlDataMeta();
    data = new GetXmlDataData();
    meta.setDefault();
  }

  @Test
  void testConstants() {
    assertEquals(
        "GetXMLData.Log.UnableCreateDocument",
        GetXmlData.CONST_GET_XMLDATA_LOG_UNABLE_CREATE_DOCUMENT);
    assertEquals(
        "GetXMLData.Log.LoopFileOccurences", GetXmlData.CONST_GET_XMLDATA_LOG_LOOP_FILE_OCCURENCES);
    assertEquals(
        "GetXMLData.Log.UnableApplyXPath", GetXmlData.CONST_GET_XMLDATA_LOG_UNABLE_APPLY_XPATH);
  }

  @Test
  void testFieldConfigurationWithAttributes() {
    java.util.List<GetXmlDataField> fields = new java.util.ArrayList<>();

    // Test attribute field
    GetXmlDataField field0 = new GetXmlDataField("id_attribute");
    field0.setXPath("/root/item/@id");
    field0.setElementType(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_ATTRIBUTE));
    field0.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING));
    fields.add(field0);

    // Test node field
    GetXmlDataField field1 = new GetXmlDataField("name_node");
    field1.setXPath("/root/item/name");
    field1.setElementType(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_NODE));
    field1.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING));
    fields.add(field1);

    // Test single node result
    GetXmlDataField field2 = new GetXmlDataField("xml_node");
    field2.setXPath("/root/item");
    field2.setElementType(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_NODE));
    field2.setResultType(getResultTypeCode(GetXmlDataField.RESULT_TYPE_TYPE_SINGLE_NODE));
    field2.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING));
    fields.add(field2);

    meta.setInputFields(fields);

    assertEquals(
        getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_ATTRIBUTE), fields.get(0).getElementType());
    assertEquals(
        getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_NODE), fields.get(1).getElementType());
    assertEquals(
        getResultTypeCode(GetXmlDataField.RESULT_TYPE_TYPE_SINGLE_NODE),
        fields.get(2).getResultType());
  }

  @Test
  void testComplexXPathExpressions() {
    java.util.List<GetXmlDataField> fields = new java.util.ArrayList<>();

    fields.add(createField("simple", "/root/item"));
    fields.add(createField("with_attribute", "/root/item/@id"));
    fields.add(createField("nested", "/root/parent/child/grandchild"));
    fields.add(createField("predicate", "/root/item[1]/name"));
    fields.add(createField("namespace", "//ns:element"));

    meta.setInputFields(fields);

    assertEquals("/root/item", fields.get(0).getXPath());
    assertEquals("/root/item/@id", fields.get(1).getXPath());
    assertEquals("/root/parent/child/grandchild", fields.get(2).getXPath());
    assertEquals("/root/item[1]/name", fields.get(3).getXPath());
    assertEquals("//ns:element", fields.get(4).getXPath());
  }

  @Test
  void testMultipleFormatTypes() {
    java.util.List<GetXmlDataField> fields = new java.util.ArrayList<>();

    // String with trim
    GetXmlDataField field0 = new GetXmlDataField("trimmed_string");
    field0.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING));
    field0.setTrimType(getTrimTypeCode(GetXmlDataField.TYPE_TRIM_BOTH));
    field0.setLength(100);
    fields.add(field0);

    // Number with formatting
    GetXmlDataField field1 = new GetXmlDataField("formatted_number");
    field1.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_NUMBER));
    field1.setFormat("#,##0.00");
    field1.setDecimalSymbol(".");
    field1.setGroupSymbol(",");
    field1.setPrecision(2);
    fields.add(field1);

    // Date with format
    GetXmlDataField field2 = new GetXmlDataField("formatted_date");
    field2.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_DATE));
    field2.setFormat("yyyy-MM-dd HH:mm:ss");
    fields.add(field2);

    // Integer
    GetXmlDataField field3 = new GetXmlDataField("integer_value");
    field3.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_INTEGER));
    field3.setLength(10);
    fields.add(field3);

    meta.setInputFields(fields);

    assertEquals(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING), fields.get(0).getType());
    assertEquals(getTrimTypeCode(GetXmlDataField.TYPE_TRIM_BOTH), fields.get(0).getTrimType());
    assertEquals(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_NUMBER), fields.get(1).getType());
    assertEquals("#,##0.00", fields.get(1).getFormat());
    assertEquals(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_DATE), fields.get(2).getType());
    assertEquals("yyyy-MM-dd HH:mm:ss", fields.get(2).getFormat());
    assertEquals(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_INTEGER), fields.get(3).getType());
  }

  @Test
  void testRepeatedFieldsConfiguration() {
    java.util.List<GetXmlDataField> fields = new java.util.ArrayList<>();

    GetXmlDataField field0 = new GetXmlDataField("normal");
    field0.setRepeat(false);
    fields.add(field0);

    GetXmlDataField field1 = new GetXmlDataField("repeated_when_null");
    field1.setRepeat(true);
    fields.add(field1);

    GetXmlDataField field2 = new GetXmlDataField("also_repeated");
    field2.setRepeat(true);
    fields.add(field2);

    meta.setInputFields(fields);

    assertFalse(fields.get(0).isRepeat());
    assertTrue(fields.get(1).isRepeat());
    assertTrue(fields.get(2).isRepeat());
  }

  @Test
  void testNamespaceConfiguration() {
    meta.setNameSpaceAware(true);
    assertTrue(meta.isNameSpaceAware());

    data.NAMESPACE.put("ns", "http://example.com/namespace");
    data.NAMESPACE.put("soap", "http://schemas.xmlsoap.org/soap/envelope/");

    assertEquals(2, data.NAMESPACE.size());
    assertEquals("http://example.com/namespace", data.NAMESPACE.get("ns"));
    assertEquals("http://schemas.xmlsoap.org/soap/envelope/", data.NAMESPACE.get("soap"));

    data.NSPath.add("/ns:root/ns:item");
    data.NSPath.add("/soap:Envelope/soap:Body");

    assertEquals(2, data.NSPath.size());
  }

  @Test
  void testTokenConfiguration() {
    meta.setUseToken(true);
    assertTrue(meta.isUseToken());

    data.tokenStart = "@_";
    data.tokenEnd = "-";

    assertEquals("@_", data.tokenStart);
    assertEquals("-", data.tokenEnd);

    // Custom tokens
    data.tokenStart = "{{";
    data.tokenEnd = "}}";

    assertEquals("{{", data.tokenStart);
    assertEquals("}}", data.tokenEnd);
  }

  @Test
  void testPruningForLargeFiles() {
    meta.setPrunePath("/root/large-collection/item");
    assertEquals("/root/large-collection/item", meta.getPrunePath());

    data.prunePath = "/root/large-collection/item";
    data.stopPruning = false;

    assertEquals("/root/large-collection/item", data.prunePath);
    assertFalse(data.stopPruning);

    data.stopPruning = true;
    assertTrue(data.stopPruning);
  }

  @Test
  void testRowLimitConfiguration() {
    meta.setRowLimit(0L); // No limit
    assertEquals(0L, meta.getRowLimit());

    meta.setRowLimit(100L);
    assertEquals(100L, meta.getRowLimit());

    meta.setRowLimit(1000000L);
    assertEquals(1000000L, meta.getRowLimit());
  }

  @Test
  void testFilePatterns() {
    // Initialize empty input fields
    meta.setInputFields(new java.util.ArrayList<>());

    // Create files list with different patterns
    java.util.List<GetXmlFileItem> filesList = new java.util.ArrayList<>();

    filesList.add(new GetXmlFileItem("/data/xml/*.xml", ".*\\.xml$", ".*test.*", "Y", "N"));

    filesList.add(new GetXmlFileItem("/data/backup/", "backup_.*\\.xml", "", "N", "Y"));

    filesList.add(new GetXmlFileItem("/data/archive/", ".*", ".*\\.tmp", "Y", "Y"));

    meta.setFilesList(filesList);

    assertEquals(3, meta.getFilesList().size());
    assertEquals(".*\\.xml$", meta.getFilesList().get(0).getFileMask());
    assertEquals("Y", meta.getFilesList().get(0).getFileRequired());
    assertEquals("Y", meta.getFilesList().get(1).getIncludeSubFolders());
    assertEquals(".*\\.tmp", meta.getFilesList().get(2).getExcludeFileMask());
  }

  @Test
  void testInFieldsVsFileMode() {
    // Test in-fields mode
    meta.setInFields(true);
    meta.setXmlField("xml_column");
    assertTrue(meta.isInFields());
    assertEquals("xml_column", meta.getXmlField());

    // Test file mode
    meta.setInFields(false);
    meta.setAFile(true);
    assertFalse(meta.isInFields());
    assertTrue(meta.isAFile());
  }

  @Test
  void testUrlMode() {
    meta.setReadUrl(true);
    assertTrue(meta.isReadUrl());

    meta.setReadUrl(false);
    assertFalse(meta.isReadUrl());
  }

  @Test
  void testValidationOptions() {
    meta.setValidating(true);
    meta.setIgnoreComments(true);
    meta.setIgnoreEmptyFile(true);
    meta.setDoNotFailIfNoFile(false);

    assertTrue(meta.isValidating());
    assertTrue(meta.isIgnoreComments());
    assertTrue(meta.isIgnoreEmptyFile());
    assertFalse(meta.isDoNotFailIfNoFile());
  }

  @Test
  void testFieldWithAllProperties() {
    GetXmlDataField field = new GetXmlDataField("complete_field");
    field.setXPath("/root/item/value");
    field.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_NUMBER));
    field.setLength(15);
    field.setPrecision(2);
    field.setFormat("#,##0.00");
    field.setTrimType(getTrimTypeCode(GetXmlDataField.TYPE_TRIM_BOTH));
    field.setElementType(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_NODE));
    field.setResultType(getResultTypeCode(GetXmlDataField.RESULT_TYPE_VALUE_OF));
    field.setCurrencySymbol("$");
    field.setDecimalSymbol(".");
    field.setGroupSymbol(",");
    field.setRepeat(true);

    assertEquals("complete_field", field.getName());
    assertEquals("/root/item/value", field.getXPath());
    assertEquals(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_NUMBER), field.getType());
    assertEquals(15, field.getLength());
    assertEquals(2, field.getPrecision());
    assertEquals("#,##0.00", field.getFormat());
    assertEquals(getTrimTypeCode(GetXmlDataField.TYPE_TRIM_BOTH), field.getTrimType());
    assertEquals(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_NODE), field.getElementType());
    assertEquals(getResultTypeCode(GetXmlDataField.RESULT_TYPE_VALUE_OF), field.getResultType());
    assertEquals("$", field.getCurrencySymbol());
    assertEquals(".", field.getDecimalSymbol());
    assertEquals(",", field.getGroupSymbol());
    assertTrue(field.isRepeat());
  }

  private GetXmlDataField createField(String name, String xpath) {
    GetXmlDataField field = new GetXmlDataField(name);
    field.setXPath(xpath);
    field.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING));
    return field;
  }
}
