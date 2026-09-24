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
package org.apache.hop.imports.kettle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Kettle mapping and metadata injection steps saved in a repository refer to their
 * sub-transformation by name and folder instead of by filename (issue #4119).
 */
class KettleImportMappingTest {

  private static final String ENTRY_TYPE = "org.apache.hop.imports.kettle.KettleImport$EntryType";

  private KettleImport kettleImport;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    kettleImport = new KettleImport();
    kettleImport.setDefaultPipelineRunConfiguration("local");
  }

  @ParameterizedTest
  @ValueSource(strings = {"Mapping", "SimpleMapping"})
  void testRepositoryReferenceBecomesFilename(String type) throws Exception {
    Node transform =
        importStep(type, "REPOSITORY_BY_NAME", "child", "", "/public/sub", "runConfiguration");

    assertEquals("SimpleMapping", XmlHandler.getTagValue(transform, "type"));
    assertEquals(
        "${PROJECT_HOME}/public/sub/child.hpl", XmlHandler.getTagValue(transform, "filename"));
    assertEquals("local", XmlHandler.getTagValue(transform, "runConfiguration"));
    assertRepositoryElementsRemoved(transform);
  }

  @Test
  void testMetaInjectGetsItsOwnRunConfigurationElement() throws Exception {
    Node transform =
        importStep("MetaInject", "REPOSITORY_BY_NAME", "child", "", "/public/sub", null);

    assertEquals(
        "${PROJECT_HOME}/public/sub/child.hpl", XmlHandler.getTagValue(transform, "filename"));
    assertEquals("local", XmlHandler.getTagValue(transform, "run_configuration"));
    assertNull(XmlHandler.getSubNode(transform, "runConfiguration"));
    assertRepositoryElementsRemoved(transform);
  }

  @Test
  void testRootFolderHasNoDoubleSlash() throws Exception {
    Node transform = importStep("SimpleMapping", "REPOSITORY_BY_NAME", "child", "", "/", null);

    assertEquals("${PROJECT_HOME}/child.hpl", XmlHandler.getTagValue(transform, "filename"));
  }

  /** Kettle keeps the old repository name after the step is switched to a filename. */
  @Test
  void testFilenameWinsOverStaleRepositoryName() throws Exception {
    Node transform =
        importStep(
            "SimpleMapping",
            "FILENAME",
            "old-child",
            "${Internal.Entry.Current.Directory}/child.ktr",
            "/public/old",
            null);

    assertEquals(
        "${Internal.Entry.Current.Folder}/child.hpl",
        XmlHandler.getTagValue(transform, "filename"));
    assertRepositoryElementsRemoved(transform);
  }

  @Test
  void testMissingFilenameElementIsAdded() throws Exception {
    Document doc =
        parse(
            "<transformation><step><name>map</name><type>SimpleMapping</type>"
                + "<trans_name>child</trans_name><directory_path>/sub</directory_path>"
                + "</step></transformation>");
    processNode(doc);

    Node transform = XmlHandler.getSubNode(XmlHandler.getSubNode(doc, "pipeline"), "transform");
    assertEquals("${PROJECT_HOME}/sub/child.hpl", XmlHandler.getTagValue(transform, "filename"));
  }

  private static void assertRepositoryElementsRemoved(Node transform) {
    for (String tag :
        new String[] {"specification_method", "trans_object_id", "trans_name", "directory_path"}) {
      assertNull(XmlHandler.getSubNode(transform, tag), tag + " should have been removed");
    }
  }

  /** A step laid out the way Kettle writes it, whitespace included. */
  private Node importStep(
      String type,
      String specificationMethod,
      String transName,
      String filename,
      String directoryPath,
      String existingRunConfigurationTag)
      throws Exception {
    Document doc =
        parse(
            "<transformation>\n"
                + "  <step>\n"
                + "    <name>map</name>\n"
                + "    <type>"
                + type
                + "</type>\n"
                + "    <specification_method>"
                + specificationMethod
                + "</specification_method>\n"
                + "    <trans_object_id/>\n"
                + "    <trans_name>"
                + transName
                + "</trans_name>\n"
                + "    <filename>"
                + filename
                + "</filename>\n"
                + "    <directory_path>"
                + directoryPath
                + "</directory_path>\n"
                + "    <mappings>\n"
                + "      <input><mapping><input_step/></mapping></input>\n"
                + "    </mappings>\n"
                + "  </step>\n"
                + "</transformation>");
    processNode(doc);
    Node transform = XmlHandler.getSubNode(XmlHandler.getSubNode(doc, "pipeline"), "transform");
    if (existingRunConfigurationTag != null) {
      // The run configuration is only added once.
      assertEquals(1, XmlHandler.countNodes(transform, existingRunConfigurationTag));
    }
    return transform;
  }

  private void processNode(Document doc) throws Exception {
    Class<?> entryTypeClass = Class.forName(ENTRY_TYPE);
    Object other = null;
    for (Object constant : entryTypeClass.getEnumConstants()) {
      if ("OTHER".equals(constant.toString())) {
        other = constant;
      }
    }
    Method method =
        KettleImport.class.getDeclaredMethod(
            "processNode", Document.class, Node.class, entryTypeClass, int.class);
    method.setAccessible(true);
    method.invoke(kettleImport, doc, doc, other, 0);
  }

  private static Document parse(String xml) throws Exception {
    return XmlParserFactoryProducer.createSecureDocBuilderFactory()
        .newDocumentBuilder()
        .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }
}
