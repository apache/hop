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

package org.apache.hop.pipeline.transforms.xml;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.xml.advancedxmloutput.AdvancedXmlOutputMeta;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Opt-in golden refresh ({@code -Dhop.generate.chained.golden=true}): runs injector + two XML
 * Output (Advanced) steps with the same row set as {@code 0018-xml-output-advanced-chained.hpl},
 * then copies XML/XSD into {@code integration-tests/xml/files/expected/}. Post-processing aligns
 * decl quoting, XSD self-closing tags, and trailing bytes with the existing integration goldens.
 */
class XmlOutputAdvanced0018GoldenGeneratorTest {

  private static final PluginRegistry REGISTRY = PluginRegistry.getInstance();

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
  }

  @Test
  void writeExpected0018Files() throws Exception {
    Assumptions.assumeTrue(Boolean.getBoolean("hop.generate.chained.golden"));

    Path moduleDir = Path.of("").toAbsolutePath();
    Path integrationHome = moduleDir.resolve("../../../integration-tests/xml").normalize();
    Path hpl = integrationHome.resolve("0018-xml-output-advanced-chained.hpl");
    Path expectedDir = integrationHome.resolve("files/expected");
    Files.createDirectories(expectedDir);

    AdvancedXmlOutputMeta meta1 = loadAdvancedXmlOutput(hpl, "addresses to xml field");
    AdvancedXmlOutputMeta meta2 = loadAdvancedXmlOutput(hpl, "people xml file and field");

    PipelineMeta pm = new PipelineMeta();
    TransformMeta inj = PipelineTestFactory.getInjectorTransformMeta();
    TransformMeta tm1 =
        new TransformMeta(
            REGISTRY.getPluginId(TransformPluginType.class, meta1),
            "addresses to xml field",
            meta1);
    tm1.setLocation(150, 50);
    TransformMeta tm2 =
        new TransformMeta(
            REGISTRY.getPluginId(TransformPluginType.class, meta2),
            "people xml file and field",
            meta2);
    tm2.setLocation(250, 50);
    TransformMeta dum = PipelineTestFactory.getReadTransformMeta();
    dum.setLocation(350, 50);

    pm.addTransform(inj);
    pm.addTransform(tm1);
    pm.addTransform(tm2);
    pm.addTransform(dum);
    pm.addPipelineHop(new PipelineHopMeta(inj, tm1));
    pm.addPipelineHop(new PipelineHopMeta(tm1, tm2));
    pm.addPipelineHop(new PipelineHopMeta(tm2, dum));

    Path outBase =
        Path.of(System.getProperty("java.io.tmpdir")).resolve("hop-xml-integration-output");
    Files.createDirectories(outBase);

    Variables variables = new Variables();
    Pipeline pipeline = new LocalPipelineEngine(pm, variables, null);
    pipeline.prepareExecution();

    RowTransformCollector collector = new RowTransformCollector();
    pipeline.getTransform(PipelineTestFactory.DUMMY_TRANSFORMNAME, 0).addRowListener(collector);

    RowProducer rp = pipeline.addRowProducer(PipelineTestFactory.INJECTOR_TRANSFORMNAME, 0);
    pipeline.startThreads();

    List<RowMetaAndData> rows = peopleAddressRows();
    Iterator<RowMetaAndData> it = rows.iterator();
    while (it.hasNext()) {
      RowMetaAndData rm = it.next();
      rp.putRow(rm.getRowMeta(), rm.getData());
    }
    rp.finished();

    pipeline.waitUntilFinished();
    Assertions.assertEquals(0L, pipeline.getResult().getNrErrors(), "pipeline must succeed");
    Assertions.assertFalse(collector.getRowsRead().isEmpty(), "dummy should receive rows");

    Files.copy(
        outBase.resolve("0018-chained.xml"),
        expectedDir.resolve("0018-chained.xml"),
        StandardCopyOption.REPLACE_EXISTING);
    Files.copy(
        outBase.resolve("0018-chained.xsd"),
        expectedDir.resolve("0018-chained.xsd"),
        StandardCopyOption.REPLACE_EXISTING);
    normalizeHopIntegrationXmlDecl(expectedDir.resolve("0018-chained.xml"));
    normalizeHopIntegrationXmlDecl(expectedDir.resolve("0018-chained.xsd"));
    normalizeIntegrationXsdSelfClosingTags(expectedDir.resolve("0018-chained.xsd"));
    trimTrailingContentAfterXsdSchema(expectedDir.resolve("0018-chained.xsd"));
  }

  /** Drop any bytes after the closing {@code </xs:schema>} tag. */
  private static void trimTrailingContentAfterXsdSchema(Path xsd) throws Exception {
    String s = Files.readString(xsd);
    String marker = "</xs:schema>";
    int idx = s.lastIndexOf(marker);
    if (idx < 0) {
      return;
    }
    String head = s.substring(0, idx + marker.length());
    if (!s.equals(head)) {
      Files.writeString(xsd, head);
    }
  }

  /** Match integration goldens: single-quoted {@code <?xml ?>} declaration. */
  private static void normalizeHopIntegrationXmlDecl(Path file) throws Exception {
    String s = Files.readString(file);
    String n =
        s.replace(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>", "<?xml version='1.0' encoding='UTF-8'?>");
    if (!s.equals(n)) {
      Files.writeString(file, n);
    }
  }

  private static void normalizeIntegrationXsdSelfClosingTags(Path xsd) throws Exception {
    String s = Files.readString(xsd);
    String n =
        Pattern.compile("<xs:element name=\"([^\"]+)\" type=\"xs:([^\"]+)\"></xs:element>")
            .matcher(s)
            .replaceAll("<xs:element name=\"$1\" type=\"xs:$2\"/>");
    n = Pattern.compile("<xs:any ([^>]*)></xs:any>").matcher(n).replaceAll("<xs:any $1/>");
    if (!s.equals(n)) {
      Files.writeString(xsd, n);
    }
  }

  private static AdvancedXmlOutputMeta loadAdvancedXmlOutput(
      Path pipelineFile, String transformName) throws Exception {
    Document doc = XmlHandler.loadXmlString(Files.readString(pipelineFile));
    NodeList transforms = doc.getElementsByTagName("transform");
    for (int i = 0; i < transforms.getLength(); i++) {
      Node t = transforms.item(i);
      if (!"AdvancedXMLOutput".equals(XmlHandler.getTagValue(t, "type"))) {
        continue;
      }
      if (transformName.equals(XmlHandler.getTagValue(t, "name"))) {
        return XmlMetadataUtil.deSerializeFromXml(
            t, AdvancedXmlOutputMeta.class, new MemoryMetadataProvider());
      }
    }
    throw new IllegalStateException(
        "no AdvancedXMLOutput named " + transformName + " in " + pipelineFile);
  }

  /** Same rows as the Data Grid in {@code 0018-xml-output-advanced-chained.hpl}. */
  private static List<RowMetaAndData> peopleAddressRows() throws HopException {
    IRowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString("person_name"));
    rm.addValueMeta(new ValueMetaString("street"));
    rm.addValueMeta(new ValueMetaString("zip"));
    rm.addValueMeta(new ValueMetaString("country"));
    List<RowMetaAndData> rows = new ArrayList<>();
    rows.add(new RowMetaAndData(rm, "Alice", "Oak Avenue 1", "1000", "BE"));
    rows.add(new RowMetaAndData(rm, "Alice", "Elm Road 12", "2000", "NL"));
    rows.add(new RowMetaAndData(rm, "Alice", "Cedar Plaza 5", "3000", "DE"));
    rows.add(new RowMetaAndData(rm, "Alice", "Willow Court 9", "4000", "LU"));
    rows.add(new RowMetaAndData(rm, "Bob", "Pine Lane 3", "5000", "FR"));
    rows.add(new RowMetaAndData(rm, "Bob", "Maple Square 7", "6000", "ES"));
    rows.add(new RowMetaAndData(rm, "Bob", "Birch Walk 2", "7000", "IT"));
    rows.add(new RowMetaAndData(rm, "Bob", "Spruce Ring 8", "8000", "PT"));
    rows.add(new RowMetaAndData(rm, "Carol", "Aspen Row 4", "9000", "AT"));
    rows.add(new RowMetaAndData(rm, "Carol", "Hickory Path 6", "9100", "CH"));
    rows.add(new RowMetaAndData(rm, "Carol", "Linden Alley 11", "9200", "SE"));
    rows.add(new RowMetaAndData(rm, "Carol", "Redwood Drive 22", "9300", "NO"));
    return rows;
  }
}
