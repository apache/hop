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

package org.apache.hop.core.svg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.batik.anim.dom.SAXSVGDocumentFactory;
import org.apache.batik.util.XMLResourceDescriptor;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

class SvgSizingTest {

  private static Document parse(String xml) throws Exception {
    SAXSVGDocumentFactory factory =
        new SAXSVGDocumentFactory(XMLResourceDescriptor.getXMLParserClassName());
    return factory.createDocument(
        "test.svg", new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }

  private static Element rootOf(String xml) throws Exception {
    return parse(xml).getDocumentElement();
  }

  @Test
  void addsViewBoxFromIntrinsicSizeWhenMissing() throws Exception {
    Document icon =
        parse(
            "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"24\" height=\"24\">"
                + "<path fill=\"#0e3a5a\" d=\"M2 2h20v20H2z\"/></svg>");

    Element root = rootOf(SvgSizing.toXml(icon, 24, 24, 16, 16));

    assertEquals("0 0 24 24", root.getAttribute("viewBox"));
    assertEquals("16", root.getAttribute("width"));
    assertEquals("16", root.getAttribute("height"));
    assertEquals("none", root.getAttribute("preserveAspectRatio"));
    assertEquals(1, root.getElementsByTagName("path").getLength(), "content is kept");
  }

  @Test
  void keepsAnExistingViewBox() throws Exception {
    Document icon =
        parse(
            "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"32\" height=\"32\""
                + " viewBox=\"4 4 24 24\"><rect width=\"1\" height=\"1\"/></svg>");

    Element root = rootOf(SvgSizing.toXml(icon, 32, 32, 20, 20));

    assertEquals("4 4 24 24", root.getAttribute("viewBox"));
    assertEquals("20", root.getAttribute("width"));
  }

  @Test
  void fractionalIntrinsicSizeIsWrittenPlainly() throws Exception {
    Document icon = parse("<svg xmlns=\"http://www.w3.org/2000/svg\"><rect/></svg>");

    Element root = rootOf(SvgSizing.toXml(icon, 23.5, 1000000, 16, 16));

    assertEquals("0 0 23.5 1000000", root.getAttribute("viewBox"));
  }

  @Test
  void nothingExecutableSurvivesTheCopy() throws Exception {
    Document icon =
        parse(
            "<svg xmlns=\"http://www.w3.org/2000/svg\" xmlns:xlink=\"http://www.w3.org/1999/xlink\""
                + " width=\"24\" height=\"24\" onload=\"alert(1)\">"
                + "<script>alert(2)</script>"
                + "<g onclick=\"alert(3)\"><foreignObject><div xmlns=\"http://www.w3.org/1999/xhtml\">x</div></foreignObject>"
                + "<a xlink:href=\"javascript:alert(4)\"><rect width=\"1\" height=\"1\"/></a>"
                + "<a href=\"data:text/html,x\"/>"
                + "<use xlink:href=\"#shape\"/>"
                + "<image href=\"data:image/png;base64,AA==\"/>"
                + "</g></svg>");

    String xml = SvgSizing.toXml(icon, 24, 24, 16, 16);
    Element root = rootOf(xml);

    assertFalse(xml.toLowerCase().contains("alert("), xml);
    assertEquals(0, root.getElementsByTagName("script").getLength());
    assertEquals(0, root.getElementsByTagName("foreignObject").getLength());
    assertFalse(root.hasAttribute("onload"));
    Element g = (Element) root.getElementsByTagName("g").item(0);
    assertFalse(g.hasAttribute("onclick"));
    assertEquals(1, root.getElementsByTagName("rect").getLength(), "content is kept");
    assertEquals(
        "#shape",
        ((Element) root.getElementsByTagName("use").item(0))
            .getAttributeNS("http://www.w3.org/1999/xlink", "href"),
        "fragment links are fine");
    assertTrue(
        ((Element) root.getElementsByTagName("image").item(0))
            .getAttribute("href")
            .startsWith("data:image/"),
        "embedded images are fine");
  }

  @Test
  void sourceDocumentIsNotModified() throws Exception {
    Document icon = parse("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"24\" height=\"24\"/>");

    String xml = SvgSizing.toXml(icon, 24, 24, 16, 16);

    assertTrue(xml.startsWith("<?xml"), "declaration kept for the resource file");
    Element original = icon.getDocumentElement();
    assertEquals("24", original.getAttribute("width"));
    assertFalse(original.hasAttribute("viewBox"));
  }
}
