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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Test class for GetXmlDataMeta XML serialization and deserialization. */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class GetXmlDataMetaSerializationTest {

  @Test
  void testRoundTripSerialization() throws Exception {
    // Create a meta with known configuration
    GetXmlDataMeta originalMeta = new GetXmlDataMeta();
    originalMeta.setDefault();

    // Configure basic settings
    originalMeta.setEncoding("UTF-8");
    originalMeta.setLoopXPath("/root");
    originalMeta.setInFields(false);
    originalMeta.setAFile(false);
    originalMeta.setRowLimit(0L);
    originalMeta.setNameSpaceAware(false);
    originalMeta.setValidating(false);
    originalMeta.setUseToken(false);
    originalMeta.setIgnoreEmptyFile(false);
    originalMeta.setDoNotFailIfNoFile(true);

    // Configure file using new API
    java.util.List<GetXmlFileItem> filesList = new java.util.ArrayList<>();
    filesList.add(new GetXmlFileItem("${PROJECT_HOME}/files/wellFormed1.xml", "", "", "N", "N"));
    originalMeta.setFilesList(filesList);

    // Configure fields using List
    java.util.List<GetXmlDataField> inputFields = new java.util.ArrayList<>();

    GetXmlDataField field1 = new GetXmlDataField("tag1");
    field1.setXPath("tag1");
    field1.setElementType(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_NODE));
    field1.setResultType(getResultTypeCode(GetXmlDataField.RESULT_TYPE_VALUE_OF));
    field1.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING));
    field1.setTrimType(getTrimTypeCode(GetXmlDataField.TYPE_TRIM_NONE));
    field1.setRepeat(false);
    inputFields.add(field1);

    GetXmlDataField field2 = new GetXmlDataField("tag2");
    field2.setXPath("tag2");
    field2.setElementType(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_NODE));
    field2.setResultType(getResultTypeCode(GetXmlDataField.RESULT_TYPE_VALUE_OF));
    field2.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING));
    field2.setTrimType(getTrimTypeCode(GetXmlDataField.TYPE_TRIM_NONE));
    field2.setRepeat(false);
    inputFields.add(field2);

    originalMeta.setInputFields(inputFields);

    // Get XML and verify it can be generated
    String xml = originalMeta.getXml();
    assertNotNull(xml);
    assertTrue(xml.contains("UTF-8"));
    assertTrue(xml.contains("/root"));
    assertTrue(xml.contains("tag1"));
    assertTrue(xml.contains("tag2"));

    // Test that clone preserves configuration
    GetXmlDataMeta cloned = (GetXmlDataMeta) originalMeta.clone();
    assertEquals(originalMeta.getEncoding(), cloned.getEncoding());
    assertEquals(originalMeta.getLoopXPath(), cloned.getLoopXPath());
    assertEquals(originalMeta.getInputFields().size(), cloned.getInputFields().size());
  }

  @Test
  void testSupportsErrorHandling() {
    assertTrue(new GetXmlDataMeta().supportsErrorHandling());
  }

  @Test
  void testDefaults() {
    GetXmlDataMeta meta = new GetXmlDataMeta();
    meta.setDefault();

    // Verify default flags
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
  void testGetFields() throws HopTransformException {
    GetXmlDataMeta meta = new GetXmlDataMeta();
    meta.setDefault();

    // Configure input fields using List
    java.util.List<GetXmlDataField> inputFields = new java.util.ArrayList<>();

    GetXmlDataField field1 = new GetXmlDataField("xml_field_1");
    field1.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING));
    field1.setXPath("/root/field1");
    inputFields.add(field1);

    GetXmlDataField field2 = new GetXmlDataField("xml_field_2");
    field2.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_INTEGER));
    field2.setXPath("/root/field2");
    inputFields.add(field2);

    GetXmlDataField field3 = new GetXmlDataField("xml_field_3");
    field3.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_DATE));
    field3.setXPath("/root/field3");
    inputFields.add(field3);

    meta.setInputFields(inputFields);

    // Configure additional output fields
    meta.setIncludeFilename(true);
    meta.setFilenameField("source_file");
    meta.setIncludeRowNumber(true);
    meta.setRowNumberField("row_nr");
    meta.setShortFileFieldName("short_name");

    RowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "GetXMLData Transform", null, null, new Variables(), null);

    // Should have 3 XML fields + filename + row number + short filename = 6 fields
    assertEquals(6, rowMeta.size());

    // Verify XML field 1
    assertEquals("xml_field_1", rowMeta.getValueMeta(0).getName());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(0).getType());
    assertEquals("GetXMLData Transform", rowMeta.getValueMeta(0).getOrigin());

    // Verify XML field 2
    assertEquals("xml_field_2", rowMeta.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(1).getType());
    assertEquals("GetXMLData Transform", rowMeta.getValueMeta(1).getOrigin());

    // Verify XML field 3
    assertEquals("xml_field_3", rowMeta.getValueMeta(2).getName());
    assertEquals(IValueMeta.TYPE_DATE, rowMeta.getValueMeta(2).getType());
    assertEquals("GetXMLData Transform", rowMeta.getValueMeta(2).getOrigin());

    // Verify filename field
    assertEquals("source_file", rowMeta.getValueMeta(3).getName());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(3).getType());

    // Verify row number field
    assertEquals("row_nr", rowMeta.getValueMeta(4).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(4).getType());

    // Verify short filename field
    assertEquals("short_name", rowMeta.getValueMeta(5).getName());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(5).getType());
  }

  @Test
  void testClone() {
    GetXmlDataMeta meta = new GetXmlDataMeta();
    meta.setDefault();

    // Configure file using new API
    java.util.List<GetXmlFileItem> filesList = new java.util.ArrayList<>();
    filesList.add(new GetXmlFileItem("/path/to/file.xml", "*.xml", "", "Y", "N"));
    meta.setFilesList(filesList);

    // Configure fields using List
    java.util.List<GetXmlDataField> inputFields = new java.util.ArrayList<>();

    GetXmlDataField field1 = new GetXmlDataField("field1");
    field1.setXPath("/root/field1");
    field1.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING));
    inputFields.add(field1);

    GetXmlDataField field2 = new GetXmlDataField("field2");
    field2.setXPath("/root/field2");
    field2.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_INTEGER));
    inputFields.add(field2);

    meta.setInputFields(inputFields);

    meta.setLoopXPath("/root/item");
    meta.setEncoding("UTF-8");
    meta.setRowLimit(1000L);

    // Clone the meta
    GetXmlDataMeta cloned = (GetXmlDataMeta) meta.clone();

    // Verify clone
    assertNotNull(cloned);
    assertNotNull(cloned.getFilesList());
    assertEquals(1, cloned.getFilesList().size());
    assertEquals("/path/to/file.xml", cloned.getFilesList().get(0).getFileName());
    assertEquals("*.xml", cloned.getFilesList().get(0).getFileMask());
    assertEquals("Y", cloned.getFilesList().get(0).getFileRequired());
    assertEquals(2, cloned.getInputFields().size());
    assertEquals("field1", cloned.getInputFields().get(0).getName());
    assertEquals("field2", cloned.getInputFields().get(1).getName());
    assertEquals("/root/item", cloned.getLoopXPath());
    assertEquals("UTF-8", cloned.getEncoding());
    assertEquals(1000L, cloned.getRowLimit());
  }

  @Test
  void testGetXml() throws Exception {
    GetXmlDataMeta meta = new GetXmlDataMeta();
    meta.setDefault();

    // Configure file using new API
    java.util.List<GetXmlFileItem> filesList = new java.util.ArrayList<>();
    filesList.add(new GetXmlFileItem("/data/test.xml", "*.xml", "*test*", "Y", "N"));
    meta.setFilesList(filesList);

    // Configure input fields using List
    java.util.List<GetXmlDataField> inputFields = new java.util.ArrayList<>();

    GetXmlDataField field1 = new GetXmlDataField("field1");
    field1.setXPath("/root/field1");
    field1.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING));
    inputFields.add(field1);

    GetXmlDataField field2 = new GetXmlDataField("field2");
    field2.setXPath("/root/field2");
    field2.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_INTEGER));
    inputFields.add(field2);

    meta.setInputFields(inputFields);

    meta.setLoopXPath("/root/items/item");
    meta.setEncoding("UTF-8");
    meta.setInFields(true);
    meta.setXmlField("xml_content");
    meta.setRowLimit(500L);
    meta.setIncludeFilename(true);
    meta.setFilenameField("filename");
    meta.setIncludeRowNumber(true);
    meta.setRowNumberField("rownr");

    String xml = meta.getXml();

    // Verify XML contains key configuration (files are serialized via HopMetadataProperty, not
    // getXml())
    assertNotNull(xml);
    assertTrue(xml.contains("UTF-8"));
    assertTrue(xml.contains("/root/items/item"));
    assertTrue(xml.contains("xml_content"));
    assertTrue(xml.contains("filename"));
    assertTrue(xml.contains("rownr"));
    assertTrue(xml.contains("<limit>500</limit>"));
  }

  @Test
  void testFieldTypes() throws HopTransformException {
    GetXmlDataMeta meta = new GetXmlDataMeta();

    // Test all major field types using List
    java.util.List<GetXmlDataField> inputFields = new java.util.ArrayList<>();
    inputFields.add(createField("string_field", IValueMeta.TYPE_STRING));
    inputFields.add(createField("integer_field", IValueMeta.TYPE_INTEGER));
    inputFields.add(createField("number_field", IValueMeta.TYPE_NUMBER));
    inputFields.add(createField("date_field", IValueMeta.TYPE_DATE));
    inputFields.add(createField("boolean_field", IValueMeta.TYPE_BOOLEAN));
    inputFields.add(createField("bignumber_field", IValueMeta.TYPE_BIGNUMBER));
    meta.setInputFields(inputFields);

    RowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "test", null, null, new Variables(), null);

    assertEquals(6, rowMeta.size());
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(0).getType());
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(1).getType());
    assertEquals(IValueMeta.TYPE_NUMBER, rowMeta.getValueMeta(2).getType());
    assertEquals(IValueMeta.TYPE_DATE, rowMeta.getValueMeta(3).getType());
    assertEquals(IValueMeta.TYPE_BOOLEAN, rowMeta.getValueMeta(4).getType());
    assertEquals(IValueMeta.TYPE_BIGNUMBER, rowMeta.getValueMeta(5).getType());
  }

  @Test
  void testElementAndResultTypes() {
    GetXmlDataMeta meta = new GetXmlDataMeta();

    // Configure input fields using List
    java.util.List<GetXmlDataField> inputFields = new java.util.ArrayList<>();

    // Node with valueOf
    GetXmlDataField field1 = new GetXmlDataField("node_valueof");
    field1.setElementType(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_NODE));
    field1.setResultType(getResultTypeCode(GetXmlDataField.RESULT_TYPE_VALUE_OF));
    inputFields.add(field1);

    // Node with singleNode
    GetXmlDataField field2 = new GetXmlDataField("node_singlenode");
    field2.setElementType(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_NODE));
    field2.setResultType(getResultTypeCode(GetXmlDataField.RESULT_TYPE_TYPE_SINGLE_NODE));
    inputFields.add(field2);

    // Attribute with valueOf
    GetXmlDataField field3 = new GetXmlDataField("attr_valueof");
    field3.setElementType(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_ATTRIBUTE));
    field3.setResultType(getResultTypeCode(GetXmlDataField.RESULT_TYPE_VALUE_OF));
    inputFields.add(field3);

    // Attribute with singleNode
    GetXmlDataField field4 = new GetXmlDataField("attr_singlenode");
    field4.setElementType(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_ATTRIBUTE));
    field4.setResultType(getResultTypeCode(GetXmlDataField.RESULT_TYPE_TYPE_SINGLE_NODE));
    inputFields.add(field4);

    meta.setInputFields(inputFields);

    assertEquals(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_NODE), field1.getElementType());
    assertEquals(getResultTypeCode(GetXmlDataField.RESULT_TYPE_VALUE_OF), field1.getResultType());
    assertEquals(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_NODE), field2.getElementType());
    assertEquals(
        getResultTypeCode(GetXmlDataField.RESULT_TYPE_TYPE_SINGLE_NODE), field2.getResultType());
    assertEquals(
        getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_ATTRIBUTE), field3.getElementType());
    assertEquals(getResultTypeCode(GetXmlDataField.RESULT_TYPE_VALUE_OF), field3.getResultType());
    assertEquals(
        getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_ATTRIBUTE), field4.getElementType());
    assertEquals(
        getResultTypeCode(GetXmlDataField.RESULT_TYPE_TYPE_SINGLE_NODE), field4.getResultType());
  }

  private GetXmlDataField createField(String name, int type) {
    GetXmlDataField field = new GetXmlDataField(name);
    field.setType(ValueMetaBase.getTypeDesc(type));
    field.setXPath("/root/" + name);
    return field;
  }
}
