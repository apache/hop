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
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.xml.PipelineTestFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * End-to-end runtime tests for the XML Output (Advanced) transform. Each test runs an actual
 * pipeline via the local engine, writes to a temp file and asserts on the produced XML.
 */
class AdvancedXmlOutputTest {

  @BeforeAll
  static void setup() throws Exception {
    HopEnvironment.init();
  }

  // ---------------------------------------------------------------------------
  // Simple flat case (no group-by)
  // ---------------------------------------------------------------------------

  @Test
  void testFlatTreeProducesOneRowPerInputLine(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("flat");
    AdvancedXmlOutputMeta meta = buildFlatMeta(output.toString());

    runPipeline(meta, buildFlatRows("Alice", 30, "Bob", 25));

    String xml = readWrittenFile(output);
    // Expect <Rows><Row><name>Alice</name><age>30</age></Row><Row>...</Row></Rows>
    assertTrue(xml.contains("<Rows>"));
    assertTrue(xml.contains("</Rows>"));
    assertEquals(2, count(xml, "<Row>"));
    assertTrue(xml.contains("<name>Alice</name>"));
    assertTrue(xml.contains("<name>Bob</name>"));
    assertTrue(xml.contains("<age>30</age>"));
    assertTrue(xml.contains("<age>25</age>"));
  }

  // ---------------------------------------------------------------------------
  // Group-by case
  // ---------------------------------------------------------------------------

  @Test
  void testGroupByCollapsesConsecutiveRowsWithSameKey(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("orders");
    AdvancedXmlOutputMeta meta = buildOrderMeta(output.toString());

    // 3 rows: order=1 (foo, bar), order=2 (baz)
    List<RowMetaAndData> rows = new ArrayList<>();
    rows.add(orderRow(1L, "foo", 1.50));
    rows.add(orderRow(1L, "bar", 2.00));
    rows.add(orderRow(2L, "baz", 3.25));

    runPipeline(meta, rows);

    String xml = readWrittenFile(output);
    // Two </order> elements (one per group), three <item> elements (one per row).
    assertEquals(2, count(xml, "</order>"));
    assertEquals(3, count(xml, "<item>"));
    // Group attribute on order element
    assertTrue(xml.contains("id=\"1\""));
    assertTrue(xml.contains("id=\"2\""));
    // Items appear under the right group (order=1 contains foo and bar; order=2 contains baz)
    int firstOrderEnd = xml.indexOf("</order>");
    int secondOrderStart = xml.indexOf("<order ", firstOrderEnd);
    String firstOrderXml = xml.substring(0, firstOrderEnd);
    String secondOrderXml = xml.substring(secondOrderStart);
    assertTrue(firstOrderXml.contains("<name>foo</name>"));
    assertTrue(firstOrderXml.contains("<name>bar</name>"));
    assertFalse(firstOrderXml.contains("<name>baz</name>"));
    assertTrue(secondOrderXml.contains("<name>baz</name>"));
  }

  // ---------------------------------------------------------------------------
  // Don't-create-empty-file
  // ---------------------------------------------------------------------------

  @Test
  void testDontCreateEmptyFileSkipsFileWhenNoRows(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("empty");
    AdvancedXmlOutputMeta meta = buildFlatMeta(output.toString());
    meta.getFileSupport().setDoNotCreateEmptyFile(true);

    runPipeline(meta, new ArrayList<>());

    Path expected = Path.of(output.toString() + ".xml");
    assertFalse(Files.exists(expected), "Expected no file to be created when input is empty");
  }

  // ---------------------------------------------------------------------------
  // Compact mode
  // ---------------------------------------------------------------------------

  @Test
  void testCompactFileHasNoNewlinesBetweenElements(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("compact");
    AdvancedXmlOutputMeta meta = buildFlatMeta(output.toString());
    meta.setCompactFile(true);
    meta.setBlankLineAfterXmlDeclaration(false);

    runPipeline(meta, buildFlatRows("X", 1, "Y", 2));

    String xml = readWrittenFile(output);
    // After the XML declaration, no newlines between row elements should remain.
    int afterDecl = xml.indexOf("?>") + 2;
    String body = xml.substring(afterDecl);
    assertFalse(body.contains("\n"), "Compact mode should not contain newlines: " + body);
  }

  // ---------------------------------------------------------------------------
  // Namespace inheritance: only the root declares xmlns; children inherit it.
  // ---------------------------------------------------------------------------

  @Test
  void testRootDefaultNamespaceIsInheritedByChildren(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("ns");
    AdvancedXmlOutputMeta meta = buildFlatMeta(output.toString());
    meta.getRootNode().setNamespace("http://example.com/customers");

    runPipeline(meta, buildFlatRows("Alice", 30));

    String xml = readWrittenFile(output);
    // Root declares the default namespace exactly once
    assertTrue(
        xml.contains("xmlns=\"http://example.com/customers\""),
        "expected root xmlns declaration, got: " + xml);
    assertEquals(
        1,
        count(xml, "xmlns=\"http://example.com/customers\""),
        "the namespace should only be declared on the root: " + xml);
    // Child elements still appear (the writer didn't fail on an "unbound" URI)
    assertTrue(xml.contains("<Row>"), "row element missing: " + xml);
    assertTrue(xml.contains("<name>Alice</name>"));
  }

  // ---------------------------------------------------------------------------
  // DOCTYPE + XSL stylesheet PI
  // ---------------------------------------------------------------------------

  @Test
  void testDoctypeAndXslPiAreEmitted(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("doc");
    AdvancedXmlOutputMeta meta = buildFlatMeta(output.toString());
    meta.setDoctypeRootElement("Rows");
    meta.setDoctypeSystemId("rows.dtd");
    meta.setXslStylesheetHref("rows.xsl");

    runPipeline(meta, buildFlatRows("a", 1));
    String xml = readWrittenFile(output);
    assertTrue(xml.contains("<!DOCTYPE Rows SYSTEM \"rows.dtd\""), "DOCTYPE missing: " + xml);
    assertTrue(
        xml.contains("<?xml-stylesheet type=\"text/xsl\" href=\"rows.xsl\"?>"),
        "xml-stylesheet PI missing: " + xml);
  }

  // ---------------------------------------------------------------------------
  // Force create / create-attribute-if-null / create-empty-element
  // ---------------------------------------------------------------------------

  @Test
  void testForceCreateEmitsStaticElementWithDefaultValue(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("force");
    AdvancedXmlOutputMeta meta = buildFlatMeta(output.toString());
    XmlNode loop = meta.getRootNode().getChildren().get(0);
    XmlNode note = new XmlNode("note", XmlNode.NodeKind.Element);
    note.setForceCreate(true);
    note.setDefaultValue("(none)");
    loop.addChild(note);

    runPipeline(meta, buildFlatRows("Alice", 30));

    String xml = readWrittenFile(output);
    assertTrue(xml.contains("<note>(none)</note>"), "expected force-created element: " + xml);
  }

  /**
   * A mapped element whose row value is null and that has both {@code force_create} and a {@code
   * default_value} should fall back to the default value, not to an empty tag.
   */
  @Test
  void testMappedElementFallsBackToDefaultWhenRowValueIsNull(@TempDir Path tempDir)
      throws Exception {
    Path output = tempDir.resolve("nullfallback");
    AdvancedXmlOutputMeta meta = buildFlatMeta(output.toString());
    // <Row> already has <name> mapped to "name". Mark it force-create + default and feed a null.
    XmlNode loop = meta.getRootNode().getChildren().get(0);
    XmlNode name = loop.getChildren().get(0);
    name.setForceCreate(true);
    name.setDefaultValue("(unknown)");

    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("name"));
    rm.addValueMeta(new ValueMetaInteger("age"));
    List<RowMetaAndData> rows = new ArrayList<>();
    rows.add(new RowMetaAndData(rm, null, 30L));

    runPipeline(meta, rows);

    String xml = readWrittenFile(output);
    assertTrue(
        xml.contains("<name>(unknown)</name>"),
        "mapped element with null row value must fall back to its default value: " + xml);
  }

  /**
   * An attribute with no mapped field but a non-empty default value must be emitted exactly once,
   * even when {@code Create attribute if no field is mapped} is on. (Regression: an earlier
   * implementation walked attribute children twice and produced a duplicated attribute.)
   */
  @Test
  void testUnmappedAttributeWithDefaultIsEmittedExactlyOnce(@TempDir Path tempDir)
      throws Exception {
    Path output = tempDir.resolve("unmappedonce");
    AdvancedXmlOutputMeta meta = buildFlatMeta(output.toString());
    meta.setCreateAttributeIfUnmapped(true);

    XmlNode loop = meta.getRootNode().getChildren().get(0);
    XmlNode currency = new XmlNode("currency", XmlNode.NodeKind.Attribute);
    currency.setDefaultValue("USD");
    loop.addChild(currency);

    runPipeline(meta, buildFlatRows("Alice", 30));

    String xml = readWrittenFile(output);
    int matches = count(xml, "currency=\"USD\"");
    assertEquals(
        1, matches, "unmapped attribute with default value must be emitted exactly once: " + xml);
  }

  // ---------------------------------------------------------------------------
  // Split-every produces multiple files
  // ---------------------------------------------------------------------------

  @Test
  void testSplitEveryProducesMultipleFiles(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("split");
    AdvancedXmlOutputMeta meta = buildFlatMeta(output.toString());
    meta.getFileSupport().setSplitEvery(2);

    runPipeline(meta, buildFlatRows("A", 1, "B", 2, "C", 3, "D", 4, "E", 5));

    // Expect 3 files: 2 rows, 2 rows, 1 row
    Path f1 = Path.of(output + "_00001.xml");
    Path f2 = Path.of(output + "_00002.xml");
    Path f3 = Path.of(output + "_00003.xml");
    assertTrue(Files.exists(f1), "first split file missing: " + f1);
    assertTrue(Files.exists(f2), "second split file missing: " + f2);
    assertTrue(Files.exists(f3), "third split file missing: " + f3);
    assertEquals(2, count(Files.readString(f1), "<Row>"));
    assertEquals(2, count(Files.readString(f2), "<Row>"));
    assertEquals(1, count(Files.readString(f3), "<Row>"));
  }

  // ---------------------------------------------------------------------------
  // Zipped output
  // ---------------------------------------------------------------------------

  @Test
  void testStripOuterFragmentRemovesDuplicateWrapper(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("frag-strip");
    AdvancedXmlOutputMeta meta = new AdvancedXmlOutputMeta();
    meta.getFileSupport().setFileName(output.toString());
    meta.getFileSupport().setExtension("xml");
    meta.getFileSupport().setDoNotOpenNewFileInit(true);
    meta.setGenerateXsd(false);

    XmlNode people = new XmlNode("people", XmlNode.NodeKind.Element);
    XmlNode person = new XmlNode("person", XmlNode.NodeKind.Element);
    person.setLoop(true);
    XmlNode name = new XmlNode("name", XmlNode.NodeKind.Element);
    name.setMappedField("person_name");
    XmlNode addresses = new XmlNode("addresses", XmlNode.NodeKind.Element);
    XmlNode frag = new XmlNode("f", XmlNode.NodeKind.DocumentFragment);
    frag.setMappedField("addresses_wrapped");
    frag.setStripOuterFragmentElement(true);
    addresses.addChild(frag);
    person.addChild(name);
    person.addChild(addresses);
    people.addChild(person);
    meta.setRootNode(people);

    String wrapped = "<addresses><address><street>s</street></address></addresses>";
    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("person_name"));
    rm.addValueMeta(new ValueMetaString("addresses_wrapped"));
    List<RowMetaAndData> rows = new ArrayList<>();
    rows.add(new RowMetaAndData(rm, "Alice", wrapped));

    runPipeline(meta, rows);
    String xml = readWrittenFile(output);
    assertFalse(xml.contains("<addresses><addresses>"), xml);
    assertTrue(xml.contains("<addresses><address>"), xml);
  }

  @Test
  void testStripOuterWorksWhenFragmentIncludesXmlDeclaration(@TempDir Path tempDir)
      throws Exception {
    Path output = tempDir.resolve("frag-decl");
    AdvancedXmlOutputMeta meta = new AdvancedXmlOutputMeta();
    meta.getFileSupport().setFileName(output.toString());
    meta.getFileSupport().setExtension("xml");
    meta.getFileSupport().setDoNotOpenNewFileInit(true);
    meta.setGenerateXsd(false);

    XmlNode people = new XmlNode("people", XmlNode.NodeKind.Element);
    XmlNode person = new XmlNode("person", XmlNode.NodeKind.Element);
    person.setLoop(true);
    XmlNode name = new XmlNode("name", XmlNode.NodeKind.Element);
    name.setMappedField("person_name");
    XmlNode addresses = new XmlNode("addresses", XmlNode.NodeKind.Element);
    XmlNode frag = new XmlNode("f", XmlNode.NodeKind.DocumentFragment);
    frag.setMappedField("addressesXml");
    frag.setStripOuterFragmentElement(true);
    addresses.addChild(frag);
    person.addChild(name);
    person.addChild(addresses);
    people.addChild(person);
    meta.setRootNode(people);

    String wrapped =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<addresses><address><street>s</street></address></addresses>";
    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("person_name"));
    rm.addValueMeta(new ValueMetaString("addressesXml"));
    List<RowMetaAndData> rows = new ArrayList<>();
    rows.add(new RowMetaAndData(rm, "Alice", wrapped));

    runPipeline(meta, rows);
    String xml = readWrittenFile(output);
    assertFalse(xml.contains("<addresses><addresses>"), xml);
    assertTrue(xml.contains("<addresses><address>"), xml);
  }

  @Test
  void testOutputValueWithSplitEmitsOneRowPerSegment(@TempDir Path tempDir) throws Exception {
    AdvancedXmlOutputMeta meta = new AdvancedXmlOutputMeta();
    meta.setOperationType(AdvancedXmlOutputMeta.XmlOutputOperation.OUTPUT_VALUE);
    meta.setOutputXmlField("docXml");
    meta.getFileSupport().setSplitEvery(4);
    meta.getFileSupport().setFileName(tempDir.resolve("unused").toString());
    meta.getFileSupport().setDoNotOpenNewFileInit(true);
    meta.setGenerateXsd(false);

    XmlNode root = new XmlNode("addresses", XmlNode.NodeKind.Element);
    root.setGroupBy(true);
    root.setMappedField("person_name");
    XmlNode address = new XmlNode("address", XmlNode.NodeKind.Element);
    address.setLoop(true);
    XmlNode st = new XmlNode("street", XmlNode.NodeKind.Element);
    st.setMappedField("street");
    address.addChild(st);
    root.addChild(address);
    meta.setRootNode(root);

    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("person_name"));
    rm.addValueMeta(new ValueMetaString("street"));
    List<RowMetaAndData> rows = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      rows.add(new RowMetaAndData(rm, "Alice", "A" + i));
    }
    for (int i = 0; i < 4; i++) {
      rows.add(new RowMetaAndData(rm, "Bob", "B" + i));
    }

    PipelineMeta pipelineMeta = PipelineTestFactory.generateTestTransformation(null, meta, "axo");
    List<RowMetaAndData> out =
        PipelineTestFactory.executeTestTransformation(
            pipelineMeta,
            PipelineTestFactory.INJECTOR_TRANSFORMNAME,
            "axo",
            PipelineTestFactory.DUMMY_TRANSFORMNAME,
            rows);
    assertEquals(2, out.size());
    String xml0 = out.get(0).getString("docXml", "");
    String xml1 = out.get(1).getString("docXml", "");
    assertTrue(xml0.contains("<street>A0</street>") && xml0.contains("<street>A3</street>"), xml0);
    assertTrue(xml1.contains("<street>B0</street>") && xml1.contains("<street>B3</street>"), xml1);
  }

  @Test
  void testOutputValueXmlOnlyOmitsInputFields(@TempDir Path tempDir) throws Exception {
    AdvancedXmlOutputMeta meta = new AdvancedXmlOutputMeta();
    meta.setOperationType(AdvancedXmlOutputMeta.XmlOutputOperation.OUTPUT_VALUE);
    meta.setOutputXmlField("docXml");
    meta.setIncludeInputFieldsInOutput(false);
    meta.getFileSupport().setSplitEvery(4);
    meta.getFileSupport().setFileName(tempDir.resolve("unused").toString());
    meta.getFileSupport().setDoNotOpenNewFileInit(true);
    meta.setGenerateXsd(false);

    XmlNode root = new XmlNode("addresses", XmlNode.NodeKind.Element);
    root.setGroupBy(true);
    root.setMappedField("person_name");
    XmlNode address = new XmlNode("address", XmlNode.NodeKind.Element);
    address.setLoop(true);
    XmlNode st = new XmlNode("street", XmlNode.NodeKind.Element);
    st.setMappedField("street");
    address.addChild(st);
    root.addChild(address);
    meta.setRootNode(root);

    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("person_name"));
    rm.addValueMeta(new ValueMetaString("street"));
    List<RowMetaAndData> rows = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      rows.add(new RowMetaAndData(rm, "Alice", "A" + i));
    }
    for (int i = 0; i < 4; i++) {
      rows.add(new RowMetaAndData(rm, "Bob", "B" + i));
    }

    PipelineMeta pipelineMeta = PipelineTestFactory.generateTestTransformation(null, meta, "axo");
    List<RowMetaAndData> out =
        PipelineTestFactory.executeTestTransformation(
            pipelineMeta,
            PipelineTestFactory.INJECTOR_TRANSFORMNAME,
            "axo",
            PipelineTestFactory.DUMMY_TRANSFORMNAME,
            rows);
    assertEquals(2, out.size());
    assertEquals(1, out.get(0).getRowMeta().size());
    assertEquals("docXml", out.get(0).getRowMeta().getValueMeta(0).getName());
    String xml0 = out.get(0).getString("docXml", "");
    assertTrue(xml0.contains("<street>A0</street>") && xml0.contains("<street>A3</street>"), xml0);
  }

  @Test
  void deserializedPersonAddressesSampleStripsFragmentAtRuntime(@TempDir Path tempDir)
      throws Exception {
    AdvancedXmlOutputMeta meta =
        loadMetaFromSamplePipeline(
            "xml-output-advanced-person-addresses.hpl", "people xml file and field");
    XmlNode frag =
        meta.getRootNode().getChildren().get(0).getChildren().stream()
            .filter(c -> "addresses".equals(c.getName()))
            .findFirst()
            .orElseThrow()
            .getChildren()
            .get(0);
    assertTrue(
        frag.isStripOuterFragmentElement(),
        "sample fragment node must deserialize strip_outer_fragment_element");
    assertEquals("addressesXml", frag.getMappedField());

    meta.getFileSupport().setFileName(tempDir.resolve("person-addrs-from-sample").toString());
    meta.getFileSupport().setDoNotOpenNewFileInit(true);
    meta.setGenerateXsd(false);
    meta.setOperationType(AdvancedXmlOutputMeta.XmlOutputOperation.WRITE_TO_FILE);

    String wrapped =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<addresses><address><street>x</street><zip>1</zip><country>y</country></address></addresses>";
    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("person_name"));
    rm.addValueMeta(new ValueMetaString("street"));
    rm.addValueMeta(new ValueMetaString("zip"));
    rm.addValueMeta(new ValueMetaString("country"));
    rm.addValueMeta(new ValueMetaString("addressesXml"));
    List<RowMetaAndData> rows = new ArrayList<>();
    rows.add(new RowMetaAndData(rm, "Alice", "s", "1", "BE", wrapped));

    runPipeline(meta, rows);
    String xml = readWrittenFile(tempDir.resolve("person-addrs-from-sample"));
    assertFalse(xml.contains("<addresses><addresses>"), xml);
  }

  private static AdvancedXmlOutputMeta loadMetaFromSamplePipeline(
      String filename, String transformName) throws Exception {
    Path path = Path.of("src/main/samples/transforms").toAbsolutePath().resolve(filename);
    Document doc = XmlHandler.loadXmlString(Files.readString(path));
    NodeList transforms = doc.getElementsByTagName("transform");
    for (int i = 0; i < transforms.getLength(); i++) {
      Node t = transforms.item(i);
      if ("AdvancedXMLOutput".equals(XmlHandler.getTagValue(t, "type"))
          && transformName.equals(XmlHandler.getTagValue(t, "name"))) {
        return XmlMetadataUtil.deSerializeFromXml(
            t, AdvancedXmlOutputMeta.class, new MemoryMetadataProvider());
      }
    }
    throw new IllegalStateException(
        "no AdvancedXMLOutput named " + transformName + " in " + filename);
  }

  @Test
  void testZippedOutputContainsValidXml(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("zipped");
    AdvancedXmlOutputMeta meta = buildFlatMeta(output.toString());
    meta.getFileSupport().setZipped(true);

    runPipeline(meta, buildFlatRows("a", 1, "b", 2));

    Path zip = Path.of(output + ".zip");
    assertTrue(Files.exists(zip), "zip archive missing: " + zip);
    try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zip))) {
      ZipEntry e = zis.getNextEntry();
      assertTrue(e != null && e.getName().endsWith(".xml"), "first entry is not .xml: " + e);
      String content = new String(zis.readAllBytes());
      assertTrue(content.contains("<Row>"));
      assertEquals(2, count(content, "<Row>"));
    }
  }

  // ---------------------------------------------------------------------------
  // XSD generation
  // ---------------------------------------------------------------------------

  @Test
  void testGenerateXsdProducesSiblingSchema(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("schema");
    AdvancedXmlOutputMeta meta = buildFlatMeta(output.toString());
    meta.setGenerateXsd(true);

    runPipeline(meta, buildFlatRows("a", 1, "b", 2));

    Path xsd = Path.of(output + ".xsd");
    assertTrue(Files.exists(xsd), "Sibling XSD should have been written: " + xsd);
    String content = Files.readString(xsd);
    assertTrue(content.contains("<xs:schema"));
    assertTrue(content.contains("<xs:element name=\"Rows\""));
    assertTrue(content.contains("<xs:element name=\"Row\""));
    assertTrue(content.contains("type=\"xs:string\"") || content.contains("type=\"xs:long\""));
  }

  @Test
  void testGenerateXsdSkippedForEmptyInput(@TempDir Path tempDir) throws Exception {
    Path output = tempDir.resolve("emptyschema");
    AdvancedXmlOutputMeta meta = buildFlatMeta(output.toString());
    meta.setGenerateXsd(true);
    meta.getFileSupport().setDoNotCreateEmptyFile(true);

    runPipeline(meta, new ArrayList<>());

    Path xsd = Path.of(output + ".xsd");
    assertFalse(Files.exists(xsd), "Empty input must not produce an XSD: " + xsd);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private AdvancedXmlOutputMeta buildFlatMeta(String filenameWithoutExt) {
    AdvancedXmlOutputMeta meta = new AdvancedXmlOutputMeta();
    meta.getFileSupport().setFileName(filenameWithoutExt);
    meta.getFileSupport().setExtension("xml");
    meta.getFileSupport().setDoNotOpenNewFileInit(true);

    XmlNode root = new XmlNode("Rows", XmlNode.NodeKind.Element);
    XmlNode row = new XmlNode("Row", XmlNode.NodeKind.Element);
    row.setLoop(true);
    XmlNode name = new XmlNode("name", XmlNode.NodeKind.Element);
    name.setMappedField("name");
    XmlNode age = new XmlNode("age", XmlNode.NodeKind.Element);
    age.setMappedField("age");
    row.addChild(name);
    row.addChild(age);
    root.addChild(row);
    meta.setRootNode(root);
    return meta;
  }

  private List<RowMetaAndData> buildFlatRows(Object... pairs) {
    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("name"));
    rm.addValueMeta(new ValueMetaInteger("age"));
    List<RowMetaAndData> rows = new ArrayList<>();
    for (int i = 0; i < pairs.length; i += 2) {
      rows.add(new RowMetaAndData(rm, pairs[i], Long.valueOf(((Number) pairs[i + 1]).longValue())));
    }
    return rows;
  }

  private AdvancedXmlOutputMeta buildOrderMeta(String filenameWithoutExt) {
    AdvancedXmlOutputMeta meta = new AdvancedXmlOutputMeta();
    meta.getFileSupport().setFileName(filenameWithoutExt);
    meta.getFileSupport().setExtension("xml");
    meta.getFileSupport().setDoNotOpenNewFileInit(true);

    // <orders>
    //   <order group_by id={orderId}>
    //     <item loop>
    //       <name>{itemName}</name>
    //       <price>{price}</price>
    //     </item>
    //   </order>
    // </orders>
    XmlNode orders = new XmlNode("orders", XmlNode.NodeKind.Element);

    XmlNode order = new XmlNode("order", XmlNode.NodeKind.Element);
    order.setGroupBy(true);
    order.setMappedField("orderId");

    XmlNode id = new XmlNode("id", XmlNode.NodeKind.Attribute);
    id.setMappedField("orderId");
    order.addChild(id);

    XmlNode item = new XmlNode("item", XmlNode.NodeKind.Element);
    item.setLoop(true);
    XmlNode itemName = new XmlNode("name", XmlNode.NodeKind.Element);
    itemName.setMappedField("itemName");
    XmlNode price = new XmlNode("price", XmlNode.NodeKind.Element);
    price.setMappedField("price");
    price.setFormat("0.00");
    item.addChild(itemName);
    item.addChild(price);
    order.addChild(item);

    orders.addChild(order);
    meta.setRootNode(orders);
    return meta;
  }

  private RowMetaAndData orderRow(long orderId, String itemName, double price) {
    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaInteger("orderId"));
    rm.addValueMeta(new ValueMetaString("itemName"));
    rm.addValueMeta(new ValueMetaNumber("price"));
    return new RowMetaAndData(rm, orderId, itemName, price);
  }

  private void runPipeline(AdvancedXmlOutputMeta meta, List<RowMetaAndData> input)
      throws Exception {
    PipelineMeta pipelineMeta = PipelineTestFactory.generateTestTransformation(null, meta, "axo");
    PipelineTestFactory.executeTestTransformation(
        pipelineMeta,
        PipelineTestFactory.INJECTOR_TRANSFORMNAME,
        "axo",
        PipelineTestFactory.DUMMY_TRANSFORMNAME,
        input);
  }

  private String readWrittenFile(Path withoutExt) throws Exception {
    Path withExt = Path.of(withoutExt.toString() + ".xml");
    return Files.readString(withExt);
  }

  private int count(String haystack, String needle) {
    int c = 0;
    int idx = 0;
    while ((idx = haystack.indexOf(needle, idx)) >= 0) {
      c++;
      idx += needle.length();
    }
    return c;
  }
}
