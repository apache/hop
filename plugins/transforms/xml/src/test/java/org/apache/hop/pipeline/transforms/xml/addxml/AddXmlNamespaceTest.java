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

package org.apache.hop.pipeline.transforms.xml.addxml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.xml.PipelineTestFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Runtime tests for the namespace handling of the Add XML transform (apache/hop#3320). Namespaces
 * are declared through regular output fields, so each test runs an actual pipeline and asserts on
 * the generated XML.
 */
class AddXmlNamespaceTest {

  private static final String XSI = "http://www.w3.org/2001/XMLSchema-instance";
  private static final String TRANSFORM = "addXml";
  private static final String XML_FIELD = "xml";

  @BeforeAll
  static void setup() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void defaultNamespaceIsTakenFromTheDeclaringField() throws Exception {
    AddXmlMeta meta = meta("catalog", field("ns", "xmlns", true), field("id", "id", false));

    List<RowMetaAndData> out = run(meta, row("ns", "https://hop.apache.org", "id", "1"));
    Element root = firstRoot(out);

    assertEquals("catalog", root.getTagName());
    assertEquals("https://hop.apache.org", root.getNamespaceURI());
    // the field is consumed as the declaration, so it is written once and not also as an attribute
    assertEquals(1, occurrences(xml(out, 0), "xmlns="));
  }

  @Test
  void unprefixedChildrenInheritTheDefaultNamespace() throws Exception {
    AddXmlMeta meta = meta("catalog", field("ns", "xmlns", true), field("id", "id", false));

    Element root = firstRoot(run(meta, row("ns", "https://hop.apache.org", "id", "1")));
    Element child = (Element) root.getElementsByTagName("id").item(0);

    assertNotNull(child);
    assertEquals("https://hop.apache.org", child.getNamespaceURI());
    assertEquals("1", child.getTextContent());
  }

  @Test
  void namespaceCanDifferPerRow() throws Exception {
    AddXmlMeta meta = meta("catalog", field("ns", "xmlns", true), field("id", "id", false));

    List<RowMetaAndData> out =
        run(
            meta,
            row("ns", "https://hop.apache.org/one", "id", "1"),
            row("ns", "https://hop.apache.org/two", "id", "2"));

    assertEquals(2, out.size());
    assertEquals("https://hop.apache.org/one", root(out, 0).getNamespaceURI());
    assertEquals("https://hop.apache.org/two", root(out, 1).getNamespaceURI());
  }

  @Test
  void prefixedRootNodeBindsItsPrefix() throws Exception {
    AddXmlMeta meta = meta("it:catalog", field("ns", "xmlns:it", true), field("id", "id", false));

    Element root = firstRoot(run(meta, row("ns", "https://hop.apache.org", "id", "1")));

    assertEquals("it:catalog", root.getTagName());
    assertEquals("https://hop.apache.org", root.getNamespaceURI());
    assertEquals("it", root.getPrefix());
  }

  @Test
  void unprefixedChildrenAreNotInTheNamespaceOfAPrefixedRootNode() throws Exception {
    AddXmlMeta meta = meta("it:catalog", field("ns", "xmlns:it", true), field("id", "id", false));

    Element root = firstRoot(run(meta, row("ns", "https://hop.apache.org", "id", "1")));
    Element child = (Element) root.getElementsByTagName("id").item(0);

    // A prefixed root node declares no default namespace, so an unprefixed child is in none.
    assertNotNull(child);
    assertNull(child.getNamespaceURI());
  }

  @Test
  void prefixedRootNodeWithoutADeclaringFieldFails() {
    AddXmlMeta meta = meta("it:catalog", field("id", "id", false));

    assertThrows(HopException.class, () -> run(meta, row("id", "1")));
  }

  @Test
  void secondaryDeclarationAndAPrefixedAttributeAreKept() throws Exception {
    AddXmlMeta meta =
        meta(
            "catalog",
            field("ns", "xmlns", true),
            field("xsins", "xmlns:xsi", true),
            field("loc", "xsi:schemaLocation", true));

    Element root =
        firstRoot(
            run(
                meta,
                row(
                    "ns", "https://hop.apache.org",
                    "xsins", XSI,
                    "loc", "https://hop.apache.org catalog.xsd")));

    assertEquals("https://hop.apache.org", root.getNamespaceURI());
    // the prefixed attribute has to resolve through the xsi declaration, not the default namespace
    assertEquals("https://hop.apache.org catalog.xsd", root.getAttributeNS(XSI, "schemaLocation"));
  }

  @Test
  void withoutADeclaringFieldTheXmlHasNoNamespace() throws Exception {
    AddXmlMeta meta = meta("catalog", field("id", "id", false));

    Element root = firstRoot(run(meta, row("id", "1")));

    assertEquals("catalog", root.getTagName());
    assertNull(root.getNamespaceURI());
  }

  @Test
  void anEmptyNamespaceValueLeavesTheXmlWithoutANamespace() throws Exception {
    AddXmlMeta meta = meta("catalog", field("ns", "xmlns", true), field("id", "id", false));

    Element root = firstRoot(run(meta, row("ns", "", "id", "1")));

    assertNull(root.getNamespaceURI());
  }

  @Test
  void aRegularAttributeIsStillWrittenAsAPlainAttribute() throws Exception {
    AddXmlMeta meta =
        meta("catalog", field("ns", "xmlns", true), field("other", "not_xmlns", true));

    Element root =
        firstRoot(run(meta, row("ns", "https://hop.apache.org", "other", "https://example.com")));

    assertEquals("https://hop.apache.org", root.getNamespaceURI());
    assertEquals("https://example.com", root.getAttribute("not_xmlns"));
  }

  // ---------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------

  private static XmlField field(String fieldName, String elementName, boolean attribute) {
    XmlField field = new XmlField();
    field.setFieldName(fieldName);
    field.setElementName(elementName);
    field.setType(IValueMeta.TYPE_STRING);
    field.setLength(-1);
    field.setPrecision(-1);
    field.setAttribute(attribute);
    return field;
  }

  private static AddXmlMeta meta(String rootNode, XmlField... fields) {
    AddXmlMeta meta = new AddXmlMeta();
    meta.setRootNode(rootNode);
    meta.setValueName(XML_FIELD);
    meta.getOmitDetails().setOmittingXmlHeader(true);
    meta.setOutputFields(new ArrayList<>(List.of(fields)));
    return meta;
  }

  /** Builds one input row from alternating field name / value pairs. */
  private static RowMetaAndData row(String... nameValuePairs) {
    RowMeta rowMeta = new RowMeta();
    Object[] data = new Object[nameValuePairs.length / 2];
    for (int i = 0; i < nameValuePairs.length; i += 2) {
      rowMeta.addValueMeta(new ValueMetaString(nameValuePairs[i]));
      data[i / 2] = nameValuePairs[i + 1];
    }
    return new RowMetaAndData(rowMeta, data);
  }

  private static List<RowMetaAndData> run(AddXmlMeta meta, RowMetaAndData... rows)
      throws HopException {
    PipelineMeta pipelineMeta =
        PipelineTestFactory.generateTestTransformation(new Variables(), meta, TRANSFORM);
    return PipelineTestFactory.executeTestTransformation(
        pipelineMeta,
        PipelineTestFactory.INJECTOR_TRANSFORMNAME,
        TRANSFORM,
        PipelineTestFactory.DUMMY_TRANSFORMNAME,
        List.of(rows));
  }

  private static int occurrences(String haystack, String needle) {
    int count = 0;
    for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + 1)) {
      count++;
    }
    return count;
  }

  private static String xml(List<RowMetaAndData> rows, int index) throws HopException {
    String xml = rows.get(index).getString(XML_FIELD, null);
    assertNotNull(xml, "no XML was added to the row");
    return xml;
  }

  private static Element firstRoot(List<RowMetaAndData> rows) throws Exception {
    assertTrue(!rows.isEmpty(), "the transform produced no rows");
    return root(rows, 0);
  }

  /** Parses the generated XML of one output row, namespace aware, and returns its root element. */
  private static Element root(List<RowMetaAndData> rows, int index) throws Exception {
    DocumentBuilderFactory factory = XmlParserFactoryProducer.createSecureDocBuilderFactory();
    factory.setNamespaceAware(true);
    Document document =
        factory
            .newDocumentBuilder()
            .parse(new ByteArrayInputStream(xml(rows, index).getBytes(StandardCharsets.UTF_8)));
    return document.getDocumentElement();
  }
}
