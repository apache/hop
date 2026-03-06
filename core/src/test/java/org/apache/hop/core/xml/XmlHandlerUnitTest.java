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

package org.apache.hop.core.xml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.GregorianCalendar;
import javax.xml.parsers.DocumentBuilder;
import org.apache.hop.core.Const;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/** Unit test for {@link XmlHandler} */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class XmlHandlerUnitTest {
  private static final String CR = Const.CR;

  @Test
  void openTagWithNotNull() {
    assertEquals("<qwerty>", XmlHandler.openTag("qwerty"));
  }

  @Test
  void openTagWithNull() {
    assertEquals("<null>", XmlHandler.openTag(null));
  }

  @Test
  void openTagWithExternalBuilder() {
    StringBuilder builder = new StringBuilder("qwe");
    XmlHandler.openTag(builder, "rty");
    assertEquals("qwe<rty>", builder.toString());
  }

  @Test
  void closeTagWithNotNull() {
    assertEquals("</qwerty>", XmlHandler.closeTag("qwerty"));
  }

  @Test
  void closeTagWithNull() {
    assertEquals("</null>", XmlHandler.closeTag(null));
  }

  @Test
  void closeTagWithExternalBuilder() {
    StringBuilder builder = new StringBuilder("qwe");
    XmlHandler.closeTag(builder, "rty");
    assertEquals("qwe</rty>", builder.toString());
  }

  @Test
  void buildCdataWithNotNull() {
    assertEquals("<![CDATA[qwerty]]>", XmlHandler.buildCDATA("qwerty"));
  }

  @Test
  void buildCdataWithNull() {
    assertEquals("<![CDATA[]]>", XmlHandler.buildCDATA(null));
  }

  @Test
  void buildCdataWithExternalBuilder() {
    StringBuilder builder = new StringBuilder("qwe");
    XmlHandler.buildCDATA(builder, "rty");
    assertEquals("qwe<![CDATA[rty]]>", builder.toString());
  }

  @Test
  void timestamp2stringTest() {
    String actual = XmlHandler.timestamp2string(null);
    assertNull(actual);
  }

  @Test
  void date2stringTest() {
    String actual = XmlHandler.date2string(null);
    assertNull(actual);
  }

  @Test
  void addTagValueBigDecimal() {
    BigDecimal input = new BigDecimal("1234567890123456789.01");
    assertEquals(
        "<bigdec>1234567890123456789.01</bigdec>" + CR, XmlHandler.addTagValue("bigdec", input));
    assertEquals(
        "<bigdec>1234567890123456789.01</bigdec>" + CR,
        XmlHandler.addTagValue("bigdec", input, true));
    assertEquals(
        "<bigdec>1234567890123456789.01</bigdec>", XmlHandler.addTagValue("bigdec", input, false));
  }

  @Test
  void addTagValueBoolean() {
    assertEquals("<abool>Y</abool>" + CR, XmlHandler.addTagValue("abool", true));
    assertEquals("<abool>Y</abool>" + CR, XmlHandler.addTagValue("abool", true, true));
    assertEquals("<abool>Y</abool>", XmlHandler.addTagValue("abool", true, false));
    assertEquals("<abool>N</abool>" + CR, XmlHandler.addTagValue("abool", false));
    assertEquals("<abool>N</abool>" + CR, XmlHandler.addTagValue("abool", false, true));
    assertEquals("<abool>N</abool>", XmlHandler.addTagValue("abool", false, false));
  }

  @Test
  void addTagValueDate() {
    String result = "2014/12/29 15:59:45.789";
    Calendar aDate = new GregorianCalendar();
    aDate.set(2014, (Calendar.DECEMBER), 29, 15, 59, 45);
    aDate.set(Calendar.MILLISECOND, 789);

    assertEquals(
        "<adate>" + result + "</adate>" + CR, XmlHandler.addTagValue("adate", aDate.getTime()));
    assertEquals(
        "<adate>" + result + "</adate>" + CR,
        XmlHandler.addTagValue("adate", aDate.getTime(), true));
    assertEquals(
        "<adate>" + result + "</adate>", XmlHandler.addTagValue("adate", aDate.getTime(), false));
  }

  @Test
  void addTagValueLong() {
    long input = 123;
    assertEquals("<along>123</along>" + CR, XmlHandler.addTagValue("along", input));
    assertEquals("<along>123</along>" + CR, XmlHandler.addTagValue("along", input, true));
    assertEquals("<along>123</along>", XmlHandler.addTagValue("along", input, false));

    assertEquals(
        "<along>" + Long.MAX_VALUE + "</along>",
        XmlHandler.addTagValue("along", Long.MAX_VALUE, false));
    assertEquals(
        "<along>" + Long.MIN_VALUE + "</along>",
        XmlHandler.addTagValue("along", Long.MIN_VALUE, false));
  }

  @Test
  void addTagValueInt() {
    int input = 456;
    assertEquals("<anint>456</anint>" + CR, XmlHandler.addTagValue("anint", input));
    assertEquals("<anint>456</anint>" + CR, XmlHandler.addTagValue("anint", input, true));
    assertEquals("<anint>456</anint>", XmlHandler.addTagValue("anint", input, false));

    assertEquals(
        "<anint>" + Integer.MAX_VALUE + "</anint>",
        XmlHandler.addTagValue("anint", Integer.MAX_VALUE, false));
    assertEquals(
        "<anint>" + Integer.MIN_VALUE + "</anint>",
        XmlHandler.addTagValue("anint", Integer.MIN_VALUE, false));
  }

  @Test
  void addTagValueDouble() {
    double input = 123.45;
    assertEquals("<adouble>123.45</adouble>" + CR, XmlHandler.addTagValue("adouble", input));
    assertEquals("<adouble>123.45</adouble>" + CR, XmlHandler.addTagValue("adouble", input, true));
    assertEquals("<adouble>123.45</adouble>", XmlHandler.addTagValue("adouble", input, false));

    assertEquals(
        "<adouble>" + Double.MAX_VALUE + "</adouble>",
        XmlHandler.addTagValue("adouble", Double.MAX_VALUE, false));
    assertEquals(
        "<adouble>" + Double.MIN_VALUE + "</adouble>",
        XmlHandler.addTagValue("adouble", Double.MIN_VALUE, false));
    assertEquals(
        "<adouble>" + Double.MIN_NORMAL + "</adouble>",
        XmlHandler.addTagValue("adouble", Double.MIN_NORMAL, false));
  }

  @Test
  @Disabled("This test needs to be reviewed")
  void addTagValueBinary() throws IOException {
    byte[] input = "Test Data".getBytes();
    String result = "H4sIAAAAAAAAAAtJLS5RcEksSQQAL4PL8QkAAAA=";

    assertEquals(
        "<bytedata>" + result + "</bytedata>" + CR, XmlHandler.addTagValue("bytedata", input));
    assertEquals(
        "<bytedata>" + result + "</bytedata>" + CR,
        XmlHandler.addTagValue("bytedata", input, true));
    assertEquals(
        "<bytedata>" + result + "</bytedata>", XmlHandler.addTagValue("bytedata", input, false));
  }

  @Test
  void addTagValueWithSurrogateCharacters() throws Exception {
    String expected =
        "<testTag attributeTest=\"test attribute value \uD842\uDFB7\" >a\uD800\uDC01\uD842\uDFB7ﻉＤtest \uD802\uDF44&lt;</testTag>";
    String tagValueWithSurrogates = "a\uD800\uDC01\uD842\uDFB7ﻉＤtest \uD802\uDF44<";
    String attributeValueWithSurrogates = "test attribute value \uD842\uDFB7";
    String result =
        XmlHandler.addTagValue(
            "testTag",
            tagValueWithSurrogates,
            false,
            "attributeTest",
            attributeValueWithSurrogates);
    assertEquals(expected, result);
    DocumentBuilder builder = XmlHandler.createDocumentBuilder(false, false);
    builder.parse(new ByteArrayInputStream(result.getBytes()));
  }

  @Test
  void testEscapingXmlBagCharacters() {
    String testString = "[value_start (\"'<&>) value_end]";
    String expectedStrAfterConversion =
        "<[value_start (&#34;&#39;&lt;&amp;&gt;) value_end] "
            + "[value_start (&#34;&#39;&lt;&amp;&gt;) value_end]=\""
            + "[value_start (&#34;&#39;&lt;&amp;>) value_end]\" >"
            + "[value_start (&#34;&#39;&lt;&amp;&gt;) value_end]"
            + "</[value_start (&#34;&#39;&lt;&amp;&gt;) value_end]>";
    String result = XmlHandler.addTagValue(testString, testString, false, testString, testString);
    assertEquals(expectedStrAfterConversion, result);
  }

  @Test
  void testGetSubNode() throws Exception {
    String testXML =
        """
				<?xml version="1.0"?>
				<root>
				    <xpto>A</xpto>
				    <xpto>B</xpto>
				    <xpto>C</xpto>
				    <xpto>D</xpto>
				</root>
				""";
    DocumentBuilder builder = XmlHandler.createDocumentBuilder(false, false);

    Document parse = builder.parse(new ByteArrayInputStream(testXML.getBytes()));
    Node rootNode = parse.getFirstChild();
    Node lastSubNode = XmlHandler.getSubNode(rootNode, "xpto");
    assertNotNull(lastSubNode);
    assertEquals("A", lastSubNode.getTextContent());
  }

  @Test
  void testGetLastSubNode() throws Exception {
    String testXML =
        """
				<?xml version="1.0"?>
				<root>
				    <xpto>A</xpto>
				    <xpto>B</xpto>
				    <xpto>C</xpto>
				    <xpto>D</xpto>
				</root>
				""";

    DocumentBuilder builder = XmlHandler.createDocumentBuilder(false, false);

    Document parse = builder.parse(new ByteArrayInputStream(testXML.getBytes()));
    Node rootNode = parse.getFirstChild();
    Node lastSubNode = XmlHandler.getLastSubNode(rootNode, "xpto");
    assertNotNull(lastSubNode);
    assertEquals("D", lastSubNode.getTextContent());
  }

  @Test
  void testGetSubNodeByNr_WithCache() throws Exception {
    String testXML =
        """
				<?xml version="1.0"?>
				<root>
				    <xpto>0</xpto>
				    <xpto>1</xpto>
				    <xpto>2</xpto>
				    <xpto>3</xpto>
				</root>
				""";

    DocumentBuilder builder = XmlHandler.createDocumentBuilder(false, false);

    Document parse = builder.parse(new ByteArrayInputStream(testXML.getBytes()));
    Node rootNode = parse.getFirstChild();

    Node subNode = XmlHandler.getSubNodeByNr(rootNode, "xpto", 0);
    assertNotNull(subNode);
    assertEquals("0", subNode.getTextContent());
    subNode = XmlHandler.getSubNodeByNr(rootNode, "xpto", 1);
    assertNotNull(subNode);
    assertEquals("1", subNode.getTextContent());
    subNode = XmlHandler.getSubNodeByNr(rootNode, "xpto", 2);
    assertNotNull(subNode);
    assertEquals("2", subNode.getTextContent());
    subNode = XmlHandler.getSubNodeByNr(rootNode, "xpto", 3);
    assertNotNull(subNode);
    assertEquals("3", subNode.getTextContent());
  }

  @Test
  void testGetSubNodeByNr_WithoutCache() throws Exception {
    String testXML =
        """
				<?xml version="1.0"?>
				<root>
				    <xpto>0</xpto>
				    <xpto>1</xpto>
				    <xpto>2</xpto>
				    <xpto>3</xpto>
				</root>
				""";
    DocumentBuilder builder = XmlHandler.createDocumentBuilder(false, false);

    Document parse = builder.parse(new ByteArrayInputStream(testXML.getBytes()));
    Node rootNode = parse.getFirstChild();

    Node subNode = XmlHandler.getSubNodeByNr(rootNode, "xpto", 0, false);
    assertNotNull(subNode);
    assertEquals("0", subNode.getTextContent());
    subNode = XmlHandler.getSubNodeByNr(rootNode, "xpto", 1, false);
    assertNotNull(subNode);
    assertEquals("1", subNode.getTextContent());
    subNode = XmlHandler.getSubNodeByNr(rootNode, "xpto", 2, false);
    assertNotNull(subNode);
    assertEquals("2", subNode.getTextContent());
    subNode = XmlHandler.getSubNodeByNr(rootNode, "xpto", 3, false);
    assertNotNull(subNode);
    assertEquals("3", subNode.getTextContent());
  }
}
