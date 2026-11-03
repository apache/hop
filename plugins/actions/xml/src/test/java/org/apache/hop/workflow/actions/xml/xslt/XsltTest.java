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

package org.apache.hop.workflow.actions.xml.xslt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Node;

/** Unit tests for {@link Xslt}, covering issue #9172 fixes. */
class XsltTest {

  private static final String XSL =
      """
      <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
        <xsl:output method="text" encoding="UTF-8"/>
        <xsl:template match="/orders">
          <xsl:text>id,customer&#10;</xsl:text>
          <xsl:for-each select="order">
            <xsl:value-of select="@id"/>,<xsl:value-of select="customer"/>
            <xsl:text>&#10;</xsl:text>
          </xsl:for-each>
        </xsl:template>
      </xsl:stylesheet>
      """;

  private static final String XML =
      """
      <orders>
        <order id="1"><customer>Apache Hop</customer></order>
      </orders>
      """;

  @TempDir Path tempDir;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    Xslt action = ActionSerializationTestUtil.testSerialization("/xslt-action.xml", Xslt.class);

    assertEquals("${Internal.Entry.Current.Folder}/files/in.xml", action.getXmlFilename());
    assertEquals("${Internal.Entry.Current.Folder}/files/style.xsl", action.getXslFilename());
    assertEquals("${Internal.Entry.Current.Folder}/files/out.csv", action.getOutputFilename());
    assertEquals(0, action.getIfFileExists());
    assertTrue(action.isAddFileToResult());
    assertTrue(action.isFilenamesFromPrevious());
    assertEquals(Xslt.FACTORY_SAXON, action.getTransformerFactory());
    assertEquals(1, action.getParameters().size());
    assertEquals("paramName", action.getParameters().getFirst().getName());
    assertEquals("paramField", action.getParameters().getFirst().getField());
    assertEquals(1, action.getOutputPropertyList().size());
    assertEquals("encoding", action.getOutputPropertyList().getFirst().getName());
    assertEquals("UTF-8", action.getOutputPropertyList().getFirst().getValue());
  }

  @Test
  void testCloneCopiesNestedSettings() throws Exception {
    Xslt action = ActionSerializationTestUtil.testSerialization("/xslt-action.xml", Xslt.class);

    Xslt clone = (Xslt) action.clone();

    assertEquals(action.isAddFileToResult(), clone.isAddFileToResult());
    assertEquals(action.isFilenamesFromPrevious(), clone.isFilenamesFromPrevious());
    assertEquals(action.getTransformerFactory(), clone.getTransformerFactory());
    assertEquals(action.getParameters().size(), clone.getParameters().size());
    assertEquals(
        action.getParameters().getFirst().getName(), clone.getParameters().getFirst().getName());
    assertEquals(
        action.getOutputPropertyList().getFirst().getValue(),
        clone.getOutputPropertyList().getFirst().getValue());
  }

  /**
   * Workflows saved before the XML cleanup still use {@code <iffileexists>} instead of {@code
   * <ifFileExists>}.
   */
  @Test
  void convertLegacyXmlReadsIffileexistsTag() throws Exception {
    String xml =
        "<xmlfilename>in.xml</xmlfilename>"
            + "<xslfilename>style.xsl</xslfilename>"
            + "<outputfilename>out.csv</outputfilename>"
            + "<iffileexists>2</iffileexists>";
    String actionXml =
        XmlHandler.openTag(ActionMeta.XML_TAG) + xml + XmlHandler.closeTag(ActionMeta.XML_TAG);
    Node node = XmlHandler.loadXmlString(actionXml, ActionMeta.XML_TAG);

    Xslt action = XmlMetadataUtil.deSerializeFromXml(node, Xslt.class, new Xslt(), null);

    assertEquals(2, action.getIfFileExists());
  }

  @Test
  void createNewWithUniqueNameKeepsExistingFile() throws Exception {
    Path xml = Files.writeString(tempDir.resolve("in.xml"), XML);
    Path xsl = Files.writeString(tempDir.resolve("style.xsl"), XSL);
    Path output = Files.writeString(tempDir.resolve("out.csv"), "old");

    Xslt action = action(xml, xsl, output);
    action.setIfFileExists(0);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.isResult());
    assertEquals("old", Files.readString(output));
    List<Path> uniqueOutputs;
    try (Stream<Path> paths = Files.list(tempDir)) {
      uniqueOutputs =
          paths
              .filter(path -> path.getFileName().toString().startsWith("out_"))
              .filter(path -> path.getFileName().toString().endsWith(".csv"))
              .toList();
    }
    assertEquals(1, uniqueOutputs.size());
    assertTrue(Files.readString(uniqueOutputs.getFirst()).contains("Apache Hop"));
  }

  @Test
  void missingXslLogsTheXslFilename() throws Exception {
    Path xml = Files.writeString(tempDir.resolve("in.xml"), XML);
    Path xsl = tempDir.resolve("missing.xsl");
    Path output = tempDir.resolve("out.csv");

    Xslt action = action(xml, xsl, output);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    String log =
        HopLogStore.getAppender()
            .getBuffer(action.getLogChannel().getLogChannelId(), true)
            .toString();
    assertTrue(log.contains(xsl.toString()), log);
    assertFalse(log.contains("File [" + xml + "] does not exist!"), log);
  }

  private Xslt action(Path xml, Path xsl, Path output) {
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(new WorkflowMeta());
    Xslt action = new Xslt("xslt");
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    action.setParentWorkflowMeta(mock(WorkflowMeta.class));
    action.setLog(new LogChannel(action));
    action.setXmlFilename(xml.toString());
    action.setXslFilename(xsl.toString());
    action.setOutputFilename(output.toString());
    return action;
  }
}
