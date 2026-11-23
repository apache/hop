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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.StringReader;
import java.util.List;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.pipeline.transforms.xml.xmljoin.TargetXPathBuilder.KeyCandidate;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

/**
 * The XPath for a picked node must lead the transform back to that node. Each test evaluates the
 * statement the way {@link XmlJoin} does: the same parser, '?' replaced by the key value, the first
 * node found.
 */
class TargetXPathBuilderTest {

  private static final String ORDERS =
      """
      <OrderList>
        <OrderHeader orderNumber="1001"><customer>A</customer><OrderLines/></OrderHeader>
        <OrderHeader orderNumber="1002"><customer>B</customer><OrderLines/></OrderHeader>
      </OrderList>""";

  @Test
  void oneNodeLeadsToExactlyThePickedNode() throws Exception {
    Document doc = parse(ORDERS);
    Element picked = element(doc, "(//OrderLines)[2]");

    String xpath = TargetXPathBuilder.xpath(picked);

    assertEquals("/OrderList/OrderHeader[2]/OrderLines", xpath);
    assertSame(picked, evaluate(doc, xpath));
  }

  @Test
  void aUniqueNameGetsNoPosition() throws Exception {
    Document doc = parse("<a><b><c/></b></a>");

    assertEquals("/a/b/c", TargetXPathBuilder.xpath(element(doc, "//c")));
    assertEquals("/a", TargetXPathBuilder.xpath(doc.getDocumentElement()));
  }

  @Test
  void anAttributeOfAParentLeadsToTheNodeOfEachKey() throws Exception {
    Document doc = parse(ORDERS);
    Element picked = element(doc, "(//OrderLines)[2]");
    KeyCandidate key = candidate(picked, "orderNumber");

    String xpath = TargetXPathBuilder.xpath(picked, key);

    assertEquals("/OrderList/OrderHeader[@orderNumber='?']/OrderLines", xpath);
    assertSame(
        element(doc, "(//OrderLines)[1]"),
        evaluate(doc, TargetXPathBuilder.forKeyValue(xpath, "1001")));
    assertSame(picked, evaluate(doc, TargetXPathBuilder.forKeyValue(xpath, "1002")));
    assertNull(evaluate(doc, TargetXPathBuilder.forKeyValue(xpath, "1003")));
  }

  @Test
  void aChildElementWithTextCanBeTheKey() throws Exception {
    Document doc = parse(ORDERS);
    Element picked = element(doc, "(//OrderLines)[1]");
    KeyCandidate key = candidate(picked, "customer");

    String xpath = TargetXPathBuilder.xpath(picked, key);

    assertEquals("/OrderList/OrderHeader[customer='?']/OrderLines", xpath);
    assertSame(
        element(doc, "(//OrderLines)[2]"),
        evaluate(doc, TargetXPathBuilder.forKeyValue(xpath, "B")));
  }

  @Test
  void theKeyCanBeOnThePickedNodeItself() throws Exception {
    Document doc = parse(ORDERS);
    Element picked = element(doc, "(//OrderHeader)[1]");

    String xpath = TargetXPathBuilder.xpath(picked, candidate(picked, "orderNumber"));

    assertEquals("/OrderList/OrderHeader[@orderNumber='?']", xpath);
    assertSame(picked, evaluate(doc, TargetXPathBuilder.forKeyValue(xpath, "1001")));
  }

  @Test
  void stepsBelowTheKeyKeepTheirPosition() throws Exception {
    Document doc =
        parse(
            """
            <OrderList>
              <OrderHeader orderNumber="1001"><Lines/><Lines/></OrderHeader>
              <OrderHeader orderNumber="1002"><Lines/><Lines/></OrderHeader>
            </OrderList>""");
    Element picked = element(doc, "//OrderHeader[@orderNumber='1002']/Lines[2]");

    String xpath = TargetXPathBuilder.xpath(picked, candidate(picked, "orderNumber"));

    assertEquals("/OrderList/OrderHeader[@orderNumber='?']/Lines[2]", xpath);
    assertSame(picked, evaluate(doc, TargetXPathBuilder.forKeyValue(xpath, "1002")));
  }

  @Test
  void stepsAboveTheKeyGetNoPosition() throws Exception {
    Document doc =
        parse(
            """
            <Regions>
              <Region><Order id="1"><Lines/></Order></Region>
              <Region><Order id="2"><Lines/></Order></Region>
            </Regions>""");
    Element picked = element(doc, "//Order[@id='2']/Lines");

    String xpath = TargetXPathBuilder.xpath(picked, candidate(picked, "id"));

    assertEquals("/Regions/Region/Order[@id='?']/Lines", xpath);
    assertSame(
        element(doc, "//Order[@id='1']/Lines"),
        evaluate(doc, TargetXPathBuilder.forKeyValue(xpath, "1")));
    assertSame(picked, evaluate(doc, TargetXPathBuilder.forKeyValue(xpath, "2")));
  }

  @Test
  void prefixedNamesAreMatchedOnTheirFullName() throws Exception {
    Document doc =
        parse(
            """
            <ns:list xmlns:ns="urn:x">
              <ns:item ns:id="1"><ns:lines/></ns:item>
              <ns:item ns:id="2"><ns:lines/></ns:item>
            </ns:list>""");
    Element picked = (Element) doc.getElementsByTagName("ns:lines").item(1);

    String oneNode = TargetXPathBuilder.xpath(picked);
    String byKey = TargetXPathBuilder.xpath(picked, candidate(picked, "ns:id"));

    assertEquals("/*[name()='ns:list']/*[name()='ns:item'][2]/*[name()='ns:lines']", oneNode);
    assertSame(picked, evaluate(doc, oneNode));
    assertEquals(
        "/*[name()='ns:list']/*[name()='ns:item'][@*[name()='ns:id']='?']/*[name()='ns:lines']",
        byKey);
    assertSame(picked, evaluate(doc, TargetXPathBuilder.forKeyValue(byKey, "2")));
  }

  @Test
  void aDefaultNamespaceNeedsNoPrefix() throws Exception {
    Document doc = parse("<list xmlns=\"urn:x\"><item/><item/></list>");
    Element picked = (Element) doc.getElementsByTagName("item").item(1);

    String xpath = TargetXPathBuilder.xpath(picked);

    assertEquals("/list/item[2]", xpath);
    assertSame(picked, evaluate(doc, xpath));
  }

  @Test
  void theCandidatesStartAtThePickedNode() throws Exception {
    Document doc =
        parse(
            """
            <root xmlns="urn:x" xmlns:p="urn:p" version="1">
              <order id="7"><ref>R7</ref><empty/><lines kind="main"><line>x</line></lines></order>
            </root>""");
    Element picked = element(doc, "//lines");

    List<String> names =
        TargetXPathBuilder.keyCandidates(picked).stream()
            .map(c -> c.holder().getNodeName() + "/" + c.name() + "=" + c.value())
            .toList();

    // The picked node's own attribute and text child first, then its parent's attribute and text
    // children (not the picked node, not an empty element), then the root's attribute without the
    // xmlns ones.
    assertEquals(
        List.of("lines/kind=main", "lines/line=x", "order/id=7", "order/ref=R7", "root/version=1"),
        names);
  }

  @Test
  void theCandidateNamedLikeTheKeyFieldIsFound() throws Exception {
    Document doc = parse("<a><b p:Order_ID=\"1\" xmlns:p=\"urn:p\" other=\"2\"><c/></b></a>");
    List<KeyCandidate> candidates = TargetXPathBuilder.keyCandidates(element(doc, "//c"));

    assertEquals("p:Order_ID", TargetXPathBuilder.findByName(candidates, "order_id ").name());
    assertNull(TargetXPathBuilder.findByName(candidates, "missing"));
    assertNull(TargetXPathBuilder.findByName(candidates, ""));
    assertNull(TargetXPathBuilder.findByName(candidates, null));
  }

  // ---------------------------------------------------------------------------------------------

  private static KeyCandidate candidate(Element picked, String name) {
    for (KeyCandidate candidate : TargetXPathBuilder.keyCandidates(picked)) {
      if (candidate.name().equals(name)) {
        return candidate;
      }
    }
    throw new AssertionError("no key candidate '" + name + "'");
  }

  /** Parses like the transform parses its target document. */
  private static Document parse(String xml) throws Exception {
    return XmlParserFactoryProducer.createSecureDocBuilderFactory()
        .newDocumentBuilder()
        .parse(new InputSource(new StringReader(xml)));
  }

  /** Finds a node like the transform finds the node to add the elements to. */
  private static Node evaluate(Document doc, String xpath) throws Exception {
    return (Node) XPathFactory.newInstance().newXPath().evaluate(xpath, doc, XPathConstants.NODE);
  }

  private static Element element(Document doc, String xpath) throws Exception {
    Node node = evaluate(doc, xpath);
    assertNotNull(node, "test document has no " + xpath);
    return (Element) node;
  }
}
