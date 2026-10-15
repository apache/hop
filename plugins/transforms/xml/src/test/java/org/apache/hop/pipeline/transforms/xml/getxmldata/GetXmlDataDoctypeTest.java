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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.xml.Dom4JUtil;
import org.apache.hop.pipeline.transforms.xml.RowTransformCollector;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 * Issue #8936: Get data from XML refused every document with a DOCTYPE, whatever {@link
 * Const#XML_ALLOW_DOCTYPE_DECL} said. External DTDs and entities must stay unloaded once it is
 * allowed.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class GetXmlDataDoctypeTest {

  private static final String SECRET = "do-not-leak-this";

  private static final String XML_WITH_DOCTYPE =
      "<?xml version=\"1.0\"?>\n"
          + "<!DOCTYPE orders [\n"
          + "  <!ELEMENT orders (order)+>\n"
          + "  <!ELEMENT order (#PCDATA)>\n"
          + "]>\n"
          + "<orders><order>first</order><order>second</order></orders>";

  private String previousValue;

  @BeforeEach
  void init() throws Exception {
    HopEnvironment.init();
    previousValue = System.getProperty(Const.XML_ALLOW_DOCTYPE_DECL);
  }

  @AfterEach
  void restore() {
    if (previousValue == null) {
      System.clearProperty(Const.XML_ALLOW_DOCTYPE_DECL);
    } else {
      System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, previousValue);
    }
  }

  /** Get data from XML reads XML_ALLOW_DOCTYPE_DECL as Hop's other XML parsers do. */
  @ParameterizedTest
  @ValueSource(strings = {"N", "Y", "y", "true", "", "<unset>"})
  void doctypeFollowsTheCoreSetting(String value) throws Exception {
    if ("<unset>".equals(value)) {
      System.clearProperty(Const.XML_ALLOW_DOCTYPE_DECL);
    } else {
      System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, value);
    }
    SAXReader reader = Dom4JUtil.getSAXReader();
    if (XmlParserFactoryProducer.isDoctypeAllowed()) {
      assertEquals(
          2, reader.read(new StringReader(XML_WITH_DOCTYPE)).getRootElement().elements().size());
    } else {
      assertThrows(DocumentException.class, () -> reader.read(new StringReader(XML_WITH_DOCTYPE)));
    }
  }

  /** A parser that cannot keep external entities out must not read a DOCTYPE at all. */
  @Test
  void doctypeIsRefusedWhenAParserFeatureIsMissing(@TempDir Path dir) throws Exception {
    System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, "Y");
    Path secret = Files.writeString(dir.resolve("secret.txt"), SECRET);
    String xml =
        "<!DOCTYPE orders [<!ENTITY leak SYSTEM \""
            + secret.toUri()
            + "\">]><orders><order>&leak;</order></orders>";
    String previousFactory = System.getProperty(SAX_PARSER_FACTORY);
    System.setProperty(SAX_PARSER_FACTORY, MissingFeatureSaxParserFactory.class.getName());
    try {
      SAXReader reader = Dom4JUtil.getSAXReader();
      assertThrows(DocumentException.class, () -> reader.read(new StringReader(xml)));
    } finally {
      if (previousFactory == null) {
        System.clearProperty(SAX_PARSER_FACTORY);
      } else {
        System.setProperty(SAX_PARSER_FACTORY, previousFactory);
      }
    }
  }

  private static final String SAX_PARSER_FACTORY = "javax.xml.parsers.SAXParserFactory";

  /** Xerces, except that it cannot switch off external general entities. */
  public static class MissingFeatureSaxParserFactory extends SAXParserFactory {
    private static final String MISSING = "http://xml.org/sax/features/external-general-entities";

    private final SAXParserFactory xerces =
        SAXParserFactory.newInstance("org.apache.xerces.jaxp.SAXParserFactoryImpl", null);

    @Override
    public SAXParser newSAXParser() throws ParserConfigurationException, SAXException {
      xerces.setNamespaceAware(isNamespaceAware());
      xerces.setValidating(isValidating());
      SAXParser parser = xerces.newSAXParser();
      XMLReader reader =
          new XMLFilterImpl(parser.getXMLReader()) {
            @Override
            public void setFeature(String name, boolean value)
                throws SAXNotRecognizedException, SAXNotSupportedException {
              if (MISSING.equals(name)) {
                throw new SAXNotRecognizedException(name);
              }
              super.setFeature(name, value);
            }
          };
      return new SAXParser() {
        @Override
        @SuppressWarnings("deprecation")
        public org.xml.sax.Parser getParser() throws SAXException {
          throw new SAXException("Not supported");
        }

        @Override
        public XMLReader getXMLReader() {
          return reader;
        }

        @Override
        public boolean isNamespaceAware() {
          return parser.isNamespaceAware();
        }

        @Override
        public boolean isValidating() {
          return parser.isValidating();
        }

        @Override
        public void setProperty(String name, Object value)
            throws SAXNotRecognizedException, SAXNotSupportedException {
          reader.setProperty(name, value);
        }

        @Override
        public Object getProperty(String name)
            throws SAXNotRecognizedException, SAXNotSupportedException {
          return reader.getProperty(name);
        }
      };
    }

    @Override
    public void setFeature(String name, boolean value)
        throws ParserConfigurationException, SAXNotRecognizedException, SAXNotSupportedException {
      xerces.setFeature(name, value);
    }

    @Override
    public boolean getFeature(String name)
        throws ParserConfigurationException, SAXNotRecognizedException, SAXNotSupportedException {
      return xerces.getFeature(name);
    }
  }

  @Test
  void doctypeIsRefusedWhenNotAllowed() {
    System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, "N");
    SAXReader reader = Dom4JUtil.getSAXReader();
    assertThrows(DocumentException.class, () -> reader.read(new StringReader(XML_WITH_DOCTYPE)));
  }

  @Test
  void doctypeIsReadWhenAllowed() throws Exception {
    System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, "Y");
    Document document = Dom4JUtil.getSAXReader().read(new StringReader(XML_WITH_DOCTYPE));
    assertEquals(2, document.getRootElement().elements("order").size());
  }

  @Test
  void externalEntityIsNotLoadedWhenDoctypeAllowed(@TempDir Path dir) throws Exception {
    System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, "Y");
    Path secret = Files.writeString(dir.resolve("secret.txt"), SECRET);
    String xml =
        "<!DOCTYPE orders [<!ENTITY leak SYSTEM \""
            + secret.toUri()
            + "\">]><orders><order>&leak;</order></orders>";

    Document document = Dom4JUtil.getSAXReader().read(new StringReader(xml));

    assertFalse(document.asXML().contains(SECRET), document.asXML());
  }

  @Test
  void externalDtdIsNotLoadedWhenDoctypeAllowed(@TempDir Path dir) throws Exception {
    System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, "Y");
    Path dtd = Files.writeString(dir.resolve("orders.dtd"), "<!ENTITY leak \"" + SECRET + "\">");
    String xml =
        "<!DOCTYPE orders SYSTEM \"" + dtd.toUri() + "\"><orders><order>&leak;</order></orders>";

    Document document = Dom4JUtil.getSAXReader().read(new StringReader(xml));

    assertFalse(document.asXML().contains(SECRET), document.asXML());
  }

  @Test
  void entityExpansionIsLimitedWhenDoctypeAllowed() {
    System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, "Y");
    StringBuilder xml = new StringBuilder("<!DOCTYPE lolz [<!ENTITY lol0 \"lol\">");
    for (int i = 1; i <= 10; i++) {
      xml.append("<!ENTITY lol").append(i).append(" \"");
      xml.append(("&lol" + (i - 1) + ";").repeat(10));
      xml.append("\">");
    }
    xml.append("]><lolz>&lol10;</lolz>");

    SAXReader reader = Dom4JUtil.getSAXReader();
    assertThrows(DocumentException.class, () -> reader.read(new StringReader(xml.toString())));
  }

  /** One large entity referenced many times: few expansions, but a lot of text. */
  @Test
  void quadraticBlowupIsRefusedWhenDoctypeAllowed() {
    System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, "Y");
    String xml =
        "<!DOCTYPE r [<!ENTITY a \""
            + "x".repeat(5000)
            + "\">]><r>"
            + "&a;".repeat(12_000)
            + "</r>";

    SAXReader reader = Dom4JUtil.getSAXReader();
    assertThrows(DocumentException.class, () -> reader.read(new StringReader(xml)));
  }

  @Test
  void quadraticBlowupInAnAttributeIsRefusedWhenDoctypeAllowed() {
    System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, "Y");
    String xml =
        "<!DOCTYPE r [<!ENTITY a \""
            + "x".repeat(5000)
            + "\">]><r a=\""
            + "&a;".repeat(12_000)
            + "\"/>";

    SAXReader reader = Dom4JUtil.getSAXReader();
    assertThrows(DocumentException.class, () -> reader.read(new StringReader(xml)));
  }

  @Test
  void getXmlDataReadsAFileWithADoctypeWhenAllowed(@TempDir Path dir) throws Exception {
    System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, "Y");
    Files.writeString(dir.resolve("orders.xml"), XML_WITH_DOCTYPE);

    Pipeline pipeline = readOrders(dir, false);

    assertEquals(0, pipeline.getResult().getNrErrors(), "pipeline errors");
    assertEquals(List.of("first", "second"), values());
  }

  /** A validating parser loads the external DTD whatever the parser features say. */
  @Test
  void getXmlDataRefusesADoctypeWhenValidating(@TempDir Path dir) throws Exception {
    System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, "Y");
    Path dtd =
        Files.writeString(
            dir.resolve("orders.dtd"),
            "<!ELEMENT orders (order)+><!ELEMENT order (#PCDATA)><!ENTITY leak \""
                + SECRET
                + "\">");
    Files.writeString(
        dir.resolve("orders.xml"),
        "<!DOCTYPE orders SYSTEM \"" + dtd.toUri() + "\"><orders><order>&leak;</order></orders>");

    Pipeline pipeline = readOrders(dir, true);

    assertEquals(1, pipeline.getResult().getNrErrors(), "the DOCTYPE must be refused");
    assertFalse(values().contains(SECRET));
  }

  private RowTransformCollector collector;

  private Pipeline readOrders(Path dir, boolean validating) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("getxmldata-doctype");
    PluginRegistry registry = PluginRegistry.getInstance();

    GetXmlDataField order = new GetXmlDataField("order");
    order.setXPath(".");
    GetXmlDataMeta meta = new GetXmlDataMeta();
    meta.setEncoding(Const.UTF_8);
    meta.setLoopXPath("/orders/order");
    meta.setValidating(validating);
    meta.setInputFields(Collections.singletonList(order));
    meta.setFilesList(
        Collections.singletonList(new GetXmlFileItem(dir.toString(), ".*\\.xml$", "", "N", "N")));
    TransformMeta getXmlData =
        new TransformMeta(
            registry.getPluginId(TransformPluginType.class, meta), "get xml data", meta);
    pipelineMeta.addTransform(getXmlData);

    DummyMeta dummyMeta = new DummyMeta();
    TransformMeta dummy =
        new TransformMeta(
            registry.getPluginId(TransformPluginType.class, dummyMeta), "dummy", dummyMeta);
    pipelineMeta.addTransform(dummy);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(getXmlData, dummy));

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.prepareExecution();
    collector = new RowTransformCollector();
    pipeline.getTransform("dummy", 0).addRowListener(collector);
    pipeline.startThreads();
    pipeline.waitUntilFinished();
    return pipeline;
  }

  private List<Object> values() {
    return collector.getRowsWritten().stream().map(r -> r.getData()[0]).toList();
  }
}
