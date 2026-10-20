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

package org.apache.hop.pipeline.transforms.calculator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

class CalculatorMetaFunctionTest {

  @Test
  void testEquals() {
    CalculatorMetaFunction meta1 = new CalculatorMetaFunction();
    CalculatorMetaFunction meta2 = (CalculatorMetaFunction) meta1.clone();
    assertNotSame(meta1, meta2);

    assertNotEquals(null, meta1);
    assertNotEquals(new Object(), meta1);
    assertEquals(meta1, meta2);

    meta2.setCalcType(CalculationType.ADD_DAYS);
    assertNotEquals(meta1, meta2);
  }

  @Test
  void testXmlSerialization() throws Exception {
    CalculatorMetaFunction function =
        new CalculatorMetaFunction(
            "copyA",
            CalculationType.COPY_OF_FIELD,
            "A",
            null,
            null,
            "String",
            100,
            -1,
            null,
            null,
            null,
            null,
            false);
    String xml = XmlMetadataUtil.serializeObjectToXml(function);
    Node node = XmlHandler.loadXmlString(XmlHandler.aroundTag("hop", xml), "hop");
    assertEquals("COPY_FIELD", XmlHandler.getTagValue(node, "calc_type"));
    assertEquals("A", XmlHandler.getTagValue(node, "field_a"));
    assertEquals("copyA", XmlHandler.getTagValue(node, "field_name"));
    assertEquals("N", XmlHandler.getTagValue(node, "remove"));
    assertEquals("100", XmlHandler.getTagValue(node, "value_length"));
    assertEquals("-1", XmlHandler.getTagValue(node, "value_precision"));
    assertEquals("String", XmlHandler.getTagValue(node, "value_type"));
  }

  @Test
  void testRestoreLegacyXmlWithoutLengthPrecisionDefaultsToMinusOne() throws Exception {
    // A legacy pipeline (Hop <= 2.x) that did NOT write value_length/value_precision.
    // Before HOP-2849 the -1 default survived this restore; after the switch to
    // HopMetadataProperty-based serialization the fields were left at their Java default 0
    // until a fix re-initialised them to -1.
    String legacy =
        ""
            + "<hop>"
            + "  <field_name>sum</field_name>"
            + "  <calc_type>ADD</calc_type>"
            + "  <field_a>A</field_a>"
            + "  <field_b>B</field_b>"
            + "  <value_type>Number</value_type>"
            + "  <remove>N</remove>"
            + "</hop>";

    Node node = XmlHandler.loadXmlString(legacy, "hop");

    CalculatorMetaFunction function =
        XmlMetadataUtil.deSerializeFromXml(node, CalculatorMetaFunction.class, null);

    assertEquals("sum", function.getFieldName());
    // Regression HOP-2849: the -1 sentinel for an un-specified length/precision must survive a
    // legacy restore instead of collapsing to 0.
    assertEquals(-1, function.getValueLength());
    assertEquals(-1, function.getValuePrecision());
  }

  @Test
  void testRestoreXmlWithExplicitLengthPrecisionPreservesValues() throws Exception {
    // A regular file where the tags ARE present: the values must round-trip untouched and must
    // not be clobbered by the -1 default used for absent tags.
    String xml =
        ""
            + "<hop>"
            + "  <field_name>rounded</field_name>"
            + "  <calc_type>ROUND_2</calc_type>"
            + "  <field_a>A</field_a>"
            + "  <value_type>Number</value_type>"
            + "  <value_length>15</value_length>"
            + "  <value_precision>4</value_precision>"
            + "  <remove>N</remove>"
            + "</hop>";

    Node node = XmlHandler.loadXmlString(xml, "hop");

    CalculatorMetaFunction function =
        XmlMetadataUtil.deSerializeFromXml(node, CalculatorMetaFunction.class, null);

    assertEquals("rounded", function.getFieldName());
    assertEquals(15, function.getValueLength());
    assertEquals(4, function.getValuePrecision());
  }
}
