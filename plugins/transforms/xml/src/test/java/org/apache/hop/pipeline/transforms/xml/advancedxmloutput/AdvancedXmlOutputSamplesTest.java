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

  /** All sample pipelines that demonstrate the XML Output (Advanced) transform. */
  private static final String SAMPLE_PREFIX = "xml-output-advanced-";

  private static final Path SAMPLES_DIR =
      Path.of("src", "main", "samples", "transforms").toAbsolutePath();

  @BeforeAll
  static void setup() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void basicSampleParsesAndHasExpectedShape() throws Exception {
    AdvancedXmlOutputMeta meta = loadAdvancedXmlOutput("xml-output-advanced-basic.hpl");

    assertEquals(
        "${java.io.tmpdir}/xml-output-advanced-basic", meta.getFileSupport().getFileName());
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
    AdvancedXmlOutputMeta meta = loadAdvancedXmlOutput("xml-output-advanced-grouped.hpl");

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
  void multiGroupBySampleHasTwoNestedGroupAncestors() throws Exception {
    AdvancedXmlOutputMeta meta = loadAdvancedXmlOutput("xml-output-advanced-multi-group-by.hpl");

    XmlNode root = meta.getRootNode();
    assertEquals("regions", root.getName());

    XmlNode region = root.getChildren().get(0);
    assertEquals("region", region.getName());
    assertTrue(region.isGroupBy(), "<region> must be flagged group-by (outer)");
    assertEquals("region", region.getMappedField());

    // region > [code @, order]
    XmlNode order =
        region.getChildren().stream()
            .filter(c -> "order".equals(c.getName()))
            .findFirst()
            .orElseThrow();
    assertTrue(order.isGroupBy(), "<order> must be flagged group-by (inner)");
    assertEquals("orderId", order.getMappedField());

    // Find the loop element down the tree.
    XmlNode loop = findLoop(root);
    assertNotNull(loop, "the multi-group-by sample must declare exactly one loop element");
    assertEquals("line", loop.getName());
  }

  @Test
  void documentFragmentSampleEmbedsFragmentNode() throws Exception {
    AdvancedXmlOutputMeta meta = loadAdvancedXmlOutput("xml-output-advanced-document-fragment.hpl");

    XmlNode root = meta.getRootNode();
    assertEquals("products", root.getName());
    XmlNode product = root.getChildren().get(0);
    assertEquals("product", product.getName());
    assertTrue(product.isLoop());

    boolean foundFragment = false;
    for (XmlNode c : product.getChildren()) {
      if (c.getKind() == XmlNode.NodeKind.DocumentFragment) {
        assertEquals("extras", c.getMappedField());
        foundFragment = true;
      }
    }
    assertTrue(
        foundFragment, "document-fragment sample must include at least one DocumentFragment node");
  }

  @Test
  void personAddressesSampleUsesDocumentFragmentUnderAddresses() throws Exception {
    AdvancedXmlOutputMeta meta =
        loadAdvancedXmlOutput(
            "xml-output-advanced-person-addresses.hpl", "people xml file and field");

    XmlNode root = meta.getRootNode();
    assertEquals("people", root.getName());
    XmlNode person = root.getChildren().get(0);
    assertEquals("person", person.getName());
    assertTrue(person.isLoop());

    XmlNode addresses =
        person.getChildren().stream()
            .filter(c -> "addresses".equals(c.getName()))
            .findFirst()
            .orElseThrow();
    assertEquals(XmlNode.NodeKind.Element, addresses.getKind());
    assertEquals(1, addresses.getChildren().size());
    XmlNode frag = addresses.getChildren().get(0);
    assertEquals(XmlNode.NodeKind.DocumentFragment, frag.getKind());
    assertEquals("addressesXml", frag.getMappedField());
    assertTrue(
        frag.isStripOuterFragmentElement(),
        "sample uses a wrapped &lt;addresses&gt; field; strip outer avoids double wrapper");

    assertTrue(meta.writesXmlFile());
    assertTrue(meta.writesXmlField());
    assertEquals("both", meta.resolvedOperationType());
    assertEquals("peopleXml", meta.getOutputXmlField());

    XmlNode name =
        person.getChildren().stream()
            .filter(c -> "name".equals(c.getName()))
            .findFirst()
            .orElseThrow();
    assertEquals("person_name", name.getMappedField());
  }

  @Test
  void chainedSampleMatchesPersonAddressesTransforms() throws Exception {
    AdvancedXmlOutputMeta people =
        loadAdvancedXmlOutput("xml-output-advanced-chained.hpl", "people xml file and field");
    assertEquals("people", people.getRootNode().getName());
    XmlNode frag =
        people.getRootNode().getChildren().get(0).getChildren().stream()
            .filter(c -> "addresses".equals(c.getName()))
            .findFirst()
            .orElseThrow()
            .getChildren()
            .get(0);
    assertEquals(XmlNode.NodeKind.DocumentFragment, frag.getKind());
    assertEquals("addressesXml", frag.getMappedField());
    assertTrue(frag.isStripOuterFragmentElement());

    AdvancedXmlOutputMeta addresses =
        loadAdvancedXmlOutput("xml-output-advanced-chained.hpl", "addresses to xml field");
    assertEquals(
        AdvancedXmlOutputMeta.XmlOutputOperation.OUTPUT_VALUE, addresses.getOperationType());
    assertEquals("addressesXml", addresses.getOutputXmlField());
    assertEquals(
        "${java.io.tmpdir}/xml-output-advanced-chained-segment-unused",
        addresses.getFileSupport().getFileName());
  }

  @Test
  void personAddressesSampleFirstTransformOutputsAddressesSegments() throws Exception {
    AdvancedXmlOutputMeta meta =
        loadAdvancedXmlOutput("xml-output-advanced-person-addresses.hpl", "addresses to xml field");
    assertEquals(AdvancedXmlOutputMeta.OPERATION_TYPE_OUTPUT_VALUE, meta.resolvedOperationType());
    assertEquals(AdvancedXmlOutputMeta.XmlOutputOperation.OUTPUT_VALUE, meta.getOperationType());
    assertEquals("addressesXml", meta.getOutputXmlField());
    assertEquals(4, meta.getFileSupport().getSplitEvery());
    XmlNode root = meta.getRootNode();
    assertEquals("addresses", root.getName());
    assertTrue(root.isGroupBy());
    assertEquals("person_name", root.getMappedField());
    XmlNode loop = root.getChildren().stream().filter(XmlNode::isLoop).findFirst().orElseThrow();
    assertEquals("address", loop.getName());
  }

  @Test
  void splitSampleEnablesSplitEveryAndTransformNr() throws Exception {
    AdvancedXmlOutputMeta meta = loadAdvancedXmlOutput("xml-output-advanced-split.hpl");

    XmlFileOutputSupport file = meta.getFileSupport();
    assertEquals(5, file.getSplitEvery(), "<splitevery> drives split rollover");
    assertTrue(file.isTransformNrInFilename(), "the copy number must be added to the filename");
    assertFalse(file.isZipped());
    assertFalse(meta.isGenerateXsd(), "the split sample intentionally turns XSD off");
  }

  @Test
  void zippedSampleUsesCustomDateTimePatternAndZip() throws Exception {
    AdvancedXmlOutputMeta meta = loadAdvancedXmlOutput("xml-output-advanced-zipped.hpl");

    XmlFileOutputSupport file = meta.getFileSupport();
    assertTrue(file.isZipped(), "<zipped> must be on");
    assertTrue(file.isSpecifyFormat(), "<SpecifyFormat> must be on");
    assertEquals("yyyy-MM-dd_HH-mm-ss", file.getDateTimeFormat());
    assertTrue(meta.isGenerateXsd(), "the XSD lives next to the .zip, not inside it");
  }

  @Test
  void compactSampleExercisesNullHandlingFlags() throws Exception {
    AdvancedXmlOutputMeta meta = loadAdvancedXmlOutput("xml-output-advanced-compact.hpl");

    assertTrue(meta.isCompactFile(), "<compact_file> must be on");
    assertFalse(meta.isBlankLineAfterXmlDeclaration());
    assertTrue(meta.isCreateAttributeIfNull());
    assertTrue(meta.isCreateAttributeIfUnmapped());

    XmlNode product = meta.getRootNode().getChildren().get(0);
    long forceCreated = product.getChildren().stream().filter(XmlNode::isForceCreate).count();
    assertTrue(forceCreated >= 2, "compact sample must have at least two force-create children");

    long withDefaults =
        product.getChildren().stream()
            .filter(c -> c.getDefaultValue() != null && !c.getDefaultValue().isEmpty())
            .count();
    assertTrue(withDefaults >= 3, "compact sample must define at least three default values");

    boolean foundUnmappedAttr =
        product.getChildren().stream()
            .anyMatch(
                c ->
                    c.getKind() == XmlNode.NodeKind.Attribute
                        && (c.getMappedField() == null || c.getMappedField().isEmpty())
                        && c.getDefaultValue() != null
                        && !c.getDefaultValue().isEmpty());
    assertTrue(
        foundUnmappedAttr,
        "compact sample must have at least one unmapped attribute with a default value");
  }

  @Test
  void allSamplesAreWellFormedXmlAndDeserializeCleanly() throws Exception {
    List<Path> samples = listSamples();
    assertTrue(samples.size() >= 9, "expected all shipped xml-output-advanced sample pipelines");
    for (Path p : samples) {
      Document doc = XmlHandler.loadXmlString(Files.readString(p));
      assertNotNull(doc, "could not parse " + p.getFileName());
      assertEquals("pipeline", doc.getDocumentElement().getNodeName(), "wrong root in " + p);

      NodeList transforms = doc.getElementsByTagName("transform");
      int axoCount = 0;
      for (int i = 0; i < transforms.getLength(); i++) {
        Node t = transforms.item(i);
        if (!"AdvancedXMLOutput".equals(XmlHandler.getTagValue(t, "type"))) {
          continue;
        }
        axoCount++;
        AdvancedXmlOutputMeta meta =
            XmlMetadataUtil.deSerializeFromXml(
                t, AdvancedXmlOutputMeta.class, new MemoryMetadataProvider());
        assertNotNull(meta.getRootNode(), "no root tree in " + p.getFileName());
        assertNotNull(findLoop(meta.getRootNode()), "no loop element in " + p.getFileName());
      }
      assertTrue(axoCount > 0, "no AdvancedXMLOutput in " + p.getFileName());
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static AdvancedXmlOutputMeta loadAdvancedXmlOutput(String pipelineFilename)
      throws Exception {
    Path path = SAMPLES_DIR.resolve(pipelineFilename);
    Document doc = XmlHandler.loadXmlString(Files.readString(path));
    Node transformNode = findAdvancedXmlOutputTransformNode(doc, null);
    assertNotNull(transformNode, "no AdvancedXMLOutput transform found in " + pipelineFilename);

    return XmlMetadataUtil.deSerializeFromXml(
        transformNode, AdvancedXmlOutputMeta.class, new MemoryMetadataProvider());
  }

  private static AdvancedXmlOutputMeta loadAdvancedXmlOutput(
      String pipelineFilename, String transformName) throws Exception {
    Path path = SAMPLES_DIR.resolve(pipelineFilename);
    Document doc = XmlHandler.loadXmlString(Files.readString(path));
    Node transformNode = findAdvancedXmlOutputTransformNode(doc, transformName);
    assertNotNull(transformNode, "no AdvancedXMLOutput named " + transformName);

    return XmlMetadataUtil.deSerializeFromXml(
        transformNode, AdvancedXmlOutputMeta.class, new MemoryMetadataProvider());
  }

  private static Node findAdvancedXmlOutputTransformNode(Document doc, String transformName) {
    NodeList transforms = doc.getElementsByTagName("transform");
    for (int i = 0; i < transforms.getLength(); i++) {
      Node t = transforms.item(i);
      String type = XmlHandler.getTagValue(t, "type");
      if (!"AdvancedXMLOutput".equals(type)) {
        continue;
      }
      if (transformName == null || transformName.equals(XmlHandler.getTagValue(t, "name"))) {
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
          .filter(p -> p.getFileName().toString().startsWith(SAMPLE_PREFIX))
          .sorted()
          .forEach(out::add);
    }
    return out;
  }

  private static XmlNode findLoop(XmlNode node) {
    if (node == null) {
      return null;
    }
    if (node.isLoop()) {
      return node;
    }
    if (node.getChildren() == null) {
      return null;
    }
    for (XmlNode c : node.getChildren()) {
      XmlNode hit = findLoop(c);
      if (hit != null) {
        return hit;
      }
    }
    return null;
  }
}
