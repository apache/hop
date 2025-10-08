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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class GetXmlDataFieldTest {

  private GetXmlDataField field;

  @BeforeEach
  void setUp() {
    field = new GetXmlDataField("testField");
  }

  @Test
  void testDefaultConstructor() {
    assertNotNull(field);
    assertEquals("testField", field.getName());
    assertEquals("", field.getXPath());
    assertEquals(-1, field.getLength());
    assertEquals(-1, field.getPrecision());
    assertEquals(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING), field.getType());
    assertEquals(
        GetXmlDataField.getTrimTypeCode(GetXmlDataField.TYPE_TRIM_NONE), field.getTrimTypeCode());
    assertEquals(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_NODE), field.getElementType());
    assertEquals(getResultTypeCode(GetXmlDataField.RESULT_TYPE_VALUE_OF), field.getResultType());
    assertFalse(field.isRepeat());
  }

  @Test
  void testEmptyConstructor() {
    GetXmlDataField emptyField = new GetXmlDataField();
    assertNotNull(emptyField);
  }

  @Test
  void testFieldConfiguration() {
    field.setName("customName");
    assertEquals("customName", field.getName());

    field.setXPath("/root/element/@attribute");
    assertEquals("/root/element/@attribute", field.getXPath());

    field.setType(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_INTEGER));
    assertEquals(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_INTEGER), field.getTypeDesc());

    field.setLength(100);
    assertEquals(100, field.getLength());

    field.setPrecision(2);
    assertEquals(2, field.getPrecision());

    field.setFormat("###,##0.00");
    assertEquals("###,##0.00", field.getFormat());
  }

  @Test
  void testTrimTypes() {
    field.setTrimType(GetXmlDataField.getTrimTypeCode(GetXmlDataField.TYPE_TRIM_NONE));
    assertEquals(
        GetXmlDataField.getTrimTypeCode(GetXmlDataField.TYPE_TRIM_NONE), field.getTrimTypeCode());

    field.setTrimType(GetXmlDataField.getTrimTypeCode(GetXmlDataField.TYPE_TRIM_LEFT));
    assertEquals(
        GetXmlDataField.getTrimTypeCode(GetXmlDataField.TYPE_TRIM_LEFT), field.getTrimTypeCode());

    field.setTrimType(GetXmlDataField.getTrimTypeCode(GetXmlDataField.TYPE_TRIM_RIGHT));
    assertEquals(
        GetXmlDataField.getTrimTypeCode(GetXmlDataField.TYPE_TRIM_RIGHT), field.getTrimTypeCode());

    field.setTrimType(GetXmlDataField.getTrimTypeCode(GetXmlDataField.TYPE_TRIM_BOTH));
    assertEquals(
        GetXmlDataField.getTrimTypeCode(GetXmlDataField.TYPE_TRIM_BOTH), field.getTrimTypeCode());
  }

  @Test
  void testElementTypes() {
    field.setElementType(GetXmlDataField.getElementTypeCode(GetXmlDataField.ELEMENT_TYPE_NODE));
    assertEquals(
        GetXmlDataField.getElementTypeCode(GetXmlDataField.ELEMENT_TYPE_NODE),
        field.getElementTypeCode());

    field.setElementType(
        GetXmlDataField.getElementTypeCode(GetXmlDataField.ELEMENT_TYPE_ATTRIBUTE));
    assertEquals(
        GetXmlDataField.getElementTypeCode(GetXmlDataField.ELEMENT_TYPE_ATTRIBUTE),
        field.getElementTypeCode());
  }

  @Test
  void testFormatSymbols() {
    field.setCurrencySymbol("€");
    assertEquals("€", field.getCurrencySymbol());

    field.setDecimalSymbol(",");
    assertEquals(",", field.getDecimalSymbol());

    field.setGroupSymbol(".");
    assertEquals(".", field.getGroupSymbol());
  }

  @Test
  void testRepeatedFlag() {
    assertFalse(field.isRepeat());

    field.setRepeat(true);
    assertTrue(field.isRepeat());

    field.setRepeat(false);
    assertFalse(field.isRepeat());
  }

  @Test
  void testClone() {
    field.setName("original");
    field.setXPath("/root/item");
    field.setType(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_NUMBER));
    field.setLength(50);
    field.setPrecision(3);
    field.setTrimType(GetXmlDataField.getTrimTypeCode(GetXmlDataField.TYPE_TRIM_BOTH));
    field.setRepeat(true);

    GetXmlDataField cloned = (GetXmlDataField) field.clone();

    assertNotNull(cloned);
    assertEquals("original", cloned.getName());
    assertEquals("/root/item", cloned.getXPath());
    assertEquals(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_NUMBER), cloned.getTypeDesc());
    assertEquals(50, cloned.getLength());
    assertEquals(3, cloned.getPrecision());
    assertEquals(
        GetXmlDataField.getTrimTypeCode(GetXmlDataField.TYPE_TRIM_BOTH), cloned.getTrimTypeCode());
    assertTrue(cloned.isRepeat());
  }

  @Test
  void testConstants() {
    assertEquals(0, GetXmlDataField.TYPE_TRIM_NONE);
    assertEquals(1, GetXmlDataField.TYPE_TRIM_LEFT);
    assertEquals(2, GetXmlDataField.TYPE_TRIM_RIGHT);
    assertEquals(3, GetXmlDataField.TYPE_TRIM_BOTH);

    assertEquals(0, GetXmlDataField.ELEMENT_TYPE_NODE);
    assertEquals(1, GetXmlDataField.ELEMENT_TYPE_ATTRIBUTE);

    assertEquals(0, GetXmlDataField.RESULT_TYPE_VALUE_OF);
    assertEquals(1, GetXmlDataField.RESULT_TYPE_TYPE_SINGLE_NODE);
  }

  @Test
  void testTrimTypeCode() {
    assertEquals("none", GetXmlDataField.trimTypeCode[GetXmlDataField.TYPE_TRIM_NONE]);
    assertEquals("left", GetXmlDataField.trimTypeCode[GetXmlDataField.TYPE_TRIM_LEFT]);
    assertEquals("right", GetXmlDataField.trimTypeCode[GetXmlDataField.TYPE_TRIM_RIGHT]);
    assertEquals("both", GetXmlDataField.trimTypeCode[GetXmlDataField.TYPE_TRIM_BOTH]);
  }

  @Test
  void testElementTypeCode() {
    assertEquals("node", GetXmlDataField.ElementTypeCode[GetXmlDataField.ELEMENT_TYPE_NODE]);
    assertEquals(
        "attribute", GetXmlDataField.ElementTypeCode[GetXmlDataField.ELEMENT_TYPE_ATTRIBUTE]);
  }

  @Test
  void testResultTypeCode() {
    assertEquals("valueof", GetXmlDataField.ResultTypeCode[GetXmlDataField.RESULT_TYPE_VALUE_OF]);
    assertEquals(
        "singlenode", GetXmlDataField.ResultTypeCode[GetXmlDataField.RESULT_TYPE_TYPE_SINGLE_NODE]);
  }

  @Test
  void testResolvedXPath() {
    field.setResolvedXPath("/resolved/path");
    assertEquals("/resolved/path", field.getResolvedXPath());
  }

  @Test
  void testNumericFieldConfiguration() {
    field.setType(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_NUMBER));
    field.setLength(15);
    field.setPrecision(2);
    field.setFormat("#,##0.00");
    field.setCurrencySymbol("$");
    field.setDecimalSymbol(".");
    field.setGroupSymbol(",");

    assertEquals(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_NUMBER), field.getTypeDesc());
    assertEquals(15, field.getLength());
    assertEquals(2, field.getPrecision());
    assertEquals("#,##0.00", field.getFormat());
    assertEquals("$", field.getCurrencySymbol());
    assertEquals(".", field.getDecimalSymbol());
    assertEquals(",", field.getGroupSymbol());
  }

  @Test
  void testDateFieldConfiguration() {
    field.setType(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_DATE));
    field.setFormat("yyyy-MM-dd HH:mm:ss");

    assertEquals(ValueMetaBase.getTypeDesc(IValueMeta.TYPE_DATE), field.getType());
    assertEquals("yyyy-MM-dd HH:mm:ss", field.getFormat());
  }

  @Test
  void testAttributeXPathConfiguration() {
    field.setXPath("/root/element/@id");
    field.setElementType(getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_ATTRIBUTE));

    assertEquals("/root/element/@id", field.getXPath());
    assertEquals(
        getElementTypeDesc(GetXmlDataField.ELEMENT_TYPE_ATTRIBUTE), field.getElementType());
  }
}
