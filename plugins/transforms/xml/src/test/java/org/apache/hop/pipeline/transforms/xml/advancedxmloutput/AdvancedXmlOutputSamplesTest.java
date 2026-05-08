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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Loads each shipped sample pipeline (in {@code src/main/samples/transforms/}) and verifies that
 * its {@code AdvancedXMLOutput} transform deserializes to a sensible {@link AdvancedXmlOutputMeta}
 * (typed-tree shape, mandatory loop element, recognized properties).
 *
 * <p>This is a structural smoke test, not a full pipeline run: it catches typos and schema drift in
 * the hand-written sample files before they ever reach the user.
 */
class AdvancedXmlOutputSamplesTest {

  private static final Path SAMPLES_DIR =
      Path.of("src", "main", "samples", "transforms").toAbsolutePath();

  @BeforeAll
  static void setup() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void basicSampleParsesAndHasExpectedShape() throws Exception {
    AdvancedXmlOutputMeta meta = loadAdvancedXmlOutput("advanced-xml-output-basic.hpl");

    assertEquals(
        "${java.io.tmpdir}/advanced-xml-output-basic", meta.getFileSupport().getFileName());
    assertEquals("xml", meta.getFileSupport().getExtension());
    assertTrue(meta.isGenerateXsd(), "basic sample is meant to demonstrate XSD generation");

    XmlNode root = meta.getRootNode();
    assertNotNull(root);
    assertEquals("customers", root.getName());
    assertEquals(1, root.getChildren().size());
    XmlNode customer = root.getChildren().get(0);
    assertEquals("customer", customer.getName());
    assertTrue(customer.isLoop(), "the <customer> element must be the row loop");
    // 1 attribute (id) + 4 element fields
    assertEquals(5, customer.getChildren().size());
    assertEquals(XmlNode.NodeKind.Attribute, customer.getChildren().get(0).getKind());
    assertEquals("id", customer.getChildren().get(0).getName());
  }

  @Test
  void groupedSampleParsesAndHasExpectedShape() throws Exception {
    AdvancedXmlOutputMeta meta = loadAdvancedXmlOutput("advanced-xml-output-grouped.hpl");

    assertTrue(meta.isGenerateXsd());
    assertEquals("orders", meta.getDoctypeRootElement());
    assertEquals("orders.dtd", meta.getDoctypeSystemId());
    assertEquals("orders.xsl", meta.getXslStylesheetHref());

    XmlNode root = meta.getRootNode();
    assertNotNull(root);
    assertEquals("orders", root.getName());
    assertEquals("http://example.com/orders", root.getNamespace());

    XmlNode order = root.getChildren().get(0);
    assertEquals("order", order.getName());
    assertTrue(order.isGroupBy(), "<order> must be flagged group-by");
    assertEquals("orderId", order.getMappedField());

    // order > [id @, customer, lines]
    assertEquals(3, order.getChildren().size());
    XmlNode lines = order.getChildren().get(2);
    assertEquals("lines", lines.getName());
    assertTrue(lines.isForceCreate(), "<lines> wraps the loop and is marked force-create");

    XmlNode line = lines.getChildren().get(0);
    assertEquals("line", line.getName());
    assertTrue(line.isLoop(), "<line> must be the row loop");

    // Validator clean run: assert exactly one loop, lines/line/sku/name/qty/price etc.
    List<String> structural = AdvancedXmlOutputValidator.validate(root, null);
    // With no input row meta we expect no field-existence errors, only structural checks pass.
    assertTrue(
        structural.isEmpty(),
        "structural validation should be clean for the grouped sample, got: " + structural);
  }

  @Test
  void allSamplesAreWellFormedXml() throws Exception {
    List<Path> samples = listSamples();
    assertTrue(samples.size() >= 2, "expected at least two sample pipelines");
    for (Path p : samples) {
      Document doc = XmlHandler.loadXmlString(Files.readString(p));
      assertNotNull(doc, "could not parse " + p.getFileName());
      assertEquals("pipeline", doc.getDocumentElement().getNodeName(), "wrong root in " + p);
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static AdvancedXmlOutputMeta loadAdvancedXmlOutput(String pipelineFilename)
      throws Exception {
    Path path = SAMPLES_DIR.resolve(pipelineFilename);
    Document doc = XmlHandler.loadXmlString(Files.readString(path));
    Node transformNode = findAdvancedXmlOutputTransformNode(doc);
    assertNotNull(transformNode, "no AdvancedXMLOutput transform found in " + pipelineFilename);

    return XmlMetadataUtil.deSerializeFromXml(
        transformNode, AdvancedXmlOutputMeta.class, new MemoryMetadataProvider());
  }

  private static Node findAdvancedXmlOutputTransformNode(Document doc) {
    NodeList transforms = doc.getElementsByTagName("transform");
    for (int i = 0; i < transforms.getLength(); i++) {
      Node t = transforms.item(i);
      String type = XmlHandler.getTagValue(t, "type");
      if ("AdvancedXMLOutput".equals(type)) {
        return t;
      }
    }
    return null;
  }

  private static List<Path> listSamples() throws Exception {
    List<Path> out = new ArrayList<>();
    try (var stream = Files.list(SAMPLES_DIR)) {
      stream
          .filter(p -> p.getFileName().toString().endsWith(".hpl"))
          .filter(p -> p.getFileName().toString().startsWith("advanced-xml-output-"))
          .sorted()
          .forEach(out::add);
    }
    return out;
  }
}
