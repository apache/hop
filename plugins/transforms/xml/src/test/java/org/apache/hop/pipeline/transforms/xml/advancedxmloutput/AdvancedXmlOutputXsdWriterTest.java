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

package org.apache.hop.pipeline.transforms.xml.advancedxmloutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link AdvancedXmlOutputXsdWriter}: derives XSD type for various Hop value types
 * and renders a minimal but well-formed schema document for a representative tree.
 */
class AdvancedXmlOutputXsdWriterTest {

  @BeforeAll
  static void setup() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void testTypeMapping() {
    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("s"));
    rm.addValueMeta(new ValueMetaInteger("i"));
    rm.addValueMeta(new ValueMetaNumber("n"));
    rm.addValueMeta(new ValueMetaDate("d"));
    rm.addValueMeta(new ValueMetaBoolean("b"));

    assertEquals("string", typeFor("s", rm));
    assertEquals("long", typeFor("i", rm));
    assertEquals("decimal", typeFor("n", rm));
    assertEquals("dateTime", typeFor("d", rm));
    assertEquals("boolean", typeFor("b", rm));
    assertEquals("string", typeFor("missing", rm));
    assertEquals("string", typeFor(null, rm));
  }

  private static String typeFor(String mappedField, IRowMeta rm) {
    XmlNode n = new XmlNode("x", XmlNode.NodeKind.Element);
    if (mappedField != null) {
      n.setMappedField(mappedField);
    }
    return AdvancedXmlOutputXsdWriter.xsdSimpleTypeFor(n, rm);
  }

  @Test
  void testGeneratesSchemaForFlatTree(@TempDir Path tempDir) throws Exception {
    XmlNode root = new XmlNode("Rows", XmlNode.NodeKind.Element);
    XmlNode row = new XmlNode("Row", XmlNode.NodeKind.Element);
    row.setLoop(true);
    XmlNode name = new XmlNode("name", XmlNode.NodeKind.Element);
    name.setMappedField("name");
    XmlNode age = new XmlNode("age", XmlNode.NodeKind.Element);
    age.setMappedField("age");
    XmlNode active = new XmlNode("active", XmlNode.NodeKind.Attribute);
    active.setMappedField("active");
    row.addChild(active);
    row.addChild(name);
    row.addChild(age);
    root.addChild(row);

    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("name"));
    rm.addValueMeta(new ValueMetaInteger("age"));
    rm.addValueMeta(new ValueMetaBoolean("active"));

    Path xsdPath = tempDir.resolve("flat.xsd");
    AdvancedXmlOutputXsdWriter.write(xsdPath.toString(), new Variables(), "UTF-8", root, rm);
    String xsd = Files.readString(xsdPath);

    assertTrue(xsd.contains("<xs:schema"), "schema root missing: " + xsd);
    assertTrue(xsd.contains("<xs:element name=\"Rows\""), "Rows element missing");
    assertTrue(xsd.contains("<xs:element name=\"Row\""), "Row element missing");
    // Loop -> unbounded
    assertTrue(xsd.contains("maxOccurs=\"unbounded\""), "loop should be unbounded");
    // Type mappings on simple elements
    assertTrue(xsd.contains("type=\"xs:string\""), "expected xs:string for name");
    assertTrue(xsd.contains("type=\"xs:long\""), "expected xs:long for age");
    // Attribute on Row
    assertTrue(xsd.contains("<xs:attribute name=\"active\""), "active attribute missing");
    assertTrue(xsd.contains("type=\"xs:boolean\""), "expected xs:boolean for active");
    // Root element shouldn't be unbounded
    int rowsIdx = xsd.indexOf("<xs:element name=\"Rows\"");
    int rowIdx = xsd.indexOf("<xs:element name=\"Row\"");
    String rowsHeader = xsd.substring(rowsIdx, xsd.indexOf('>', rowsIdx));
    assertFalse(rowsHeader.contains("maxOccurs"), "root element should not have maxOccurs");
    assertTrue(rowIdx > rowsIdx);
  }

  @Test
  void testGeneratesSchemaWithTargetNamespace(@TempDir Path tempDir) throws Exception {
    XmlNode root = new XmlNode("Catalog", XmlNode.NodeKind.Element);
    root.setNamespace("http://example.com/catalog");
    XmlNode item = new XmlNode("Item", XmlNode.NodeKind.Element);
    item.setLoop(true);
    XmlNode title = new XmlNode("Title", XmlNode.NodeKind.Element);
    title.setMappedField("title");
    item.addChild(title);
    root.addChild(item);

    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("title"));

    Path xsdPath = tempDir.resolve("ns.xsd");
    AdvancedXmlOutputXsdWriter.write(xsdPath.toString(), new Variables(), "UTF-8", root, rm);
    String xsd = Files.readString(xsdPath);

    assertTrue(
        xsd.contains("targetNamespace=\"http://example.com/catalog\""), "targetNamespace missing");
    assertTrue(xsd.contains("elementFormDefault=\"qualified\""), "qualified default missing");
  }

  @Test
  void testGroupByElementIsUnbounded(@TempDir Path tempDir) throws Exception {
    XmlNode orders = new XmlNode("orders", XmlNode.NodeKind.Element);
    XmlNode order = new XmlNode("order", XmlNode.NodeKind.Element);
    order.setGroupBy(true);
    order.setMappedField("orderId");
    XmlNode item = new XmlNode("item", XmlNode.NodeKind.Element);
    item.setLoop(true);
    XmlNode name = new XmlNode("name", XmlNode.NodeKind.Element);
    name.setMappedField("itemName");
    item.addChild(name);
    order.addChild(item);
    orders.addChild(order);

    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaInteger("orderId"));
    rm.addValueMeta(new ValueMetaString("itemName"));

    Path xsdPath = tempDir.resolve("orders.xsd");
    AdvancedXmlOutputXsdWriter.write(xsdPath.toString(), new Variables(), "UTF-8", orders, rm);
    String xsd = Files.readString(xsdPath);

    // Both order (group-by) and item (loop) should be unbounded
    int unbounded = 0;
    int idx = 0;
    while ((idx = xsd.indexOf("maxOccurs=\"unbounded\"", idx)) >= 0) {
      unbounded++;
      idx++;
    }
    assertTrue(unbounded >= 2, "expected at least two unbounded children, got " + unbounded);
  }
}
