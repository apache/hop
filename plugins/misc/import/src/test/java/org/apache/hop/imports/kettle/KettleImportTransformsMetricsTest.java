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
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import org.w3c.dom.Document;
import org.w3c.dom.Node;

class KettleImportTransformsMetricsTest {

  private KettleImport kettleImport;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    kettleImport = new KettleImport();
  }

  @Test
  void testStepsMetricsBecomesTransformsMetrics() throws Exception {
    Document doc = parse(kettleSnippet());
    processNode(doc);

    Node pipeline = XmlHandler.getSubNode(doc, "pipeline");
    assertNotNull(pipeline);
    Node transform = XmlHandler.getSubNode(pipeline, "transform");
    assertNotNull(transform);
    assertEquals("TransformsMetrics", XmlHandler.getTagValue(transform, "type"));

    Node list = XmlHandler.getSubNode(transform, "transforms");
    assertNotNull(list, "nested steps list should become transforms");
    Node watched = XmlHandler.getSubNode(list, "transform");
    assertNotNull(watched);
    assertEquals("A", XmlHandler.getTagValue(watched, "name"));
    assertEquals("0", XmlHandler.getTagValue(watched, "copyNr"));
    assertEquals("Y", XmlHandler.getTagValue(watched, "transformRequired"));
    assertNull(XmlHandler.getTagValue(watched, "stepRequired"));

    assertEquals("Step name", XmlHandler.getTagValue(transform, "transformnamefield"));
    assertNull(XmlHandler.getTagValue(transform, "stepnamefield"));
    assertEquals("Lines written", XmlHandler.getTagValue(transform, "transformlineswrittenfield"));
    assertNull(XmlHandler.getTagValue(transform, "steplineswrittentfield"));
    assertEquals("Lines rejected", XmlHandler.getTagValue(transform, "transformlineserrorsfield"));
    assertNull(XmlHandler.getTagValue(transform, "steplineserrorsfield"));
  }

  private String kettleSnippet() {
    return "<transformation>"
        + "<step>"
        + "<name>metrics</name>"
        + "<type>StepsMetrics</type>"
        + "<steps>"
        + "<step>"
        + "<name>A</name>"
        + "<copyNr>0</copyNr>"
        + "<stepRequired>Y</stepRequired>"
        + "</step>"
        + "</steps>"
        + "<stepnamefield>Step name</stepnamefield>"
        + "<steplineswrittentfield>Lines written</steplineswrittentfield>"
        + "<steplineserrorsfield>Lines rejected</steplineserrorsfield>"
        + "</step>"
        + "</transformation>";
  }

  private void processNode(Document doc) throws Exception {
    Method method =
        KettleImport.class.getDeclaredMethod(
            "processNode",
            Document.class,
            Node.class,
            Class.forName(entryTypeClassName()),
            int.class);
    method.setAccessible(true);
    method.invoke(kettleImport, doc, doc, otherEntryType(), 0);
  }

  private static String entryTypeClassName() {
    return "org.apache.hop.imports.kettle.KettleImport$EntryType";
  }

  private Object otherEntryType() throws Exception {
    Class<?> entryTypeClass = Class.forName(entryTypeClassName());
    for (Object constant : entryTypeClass.getEnumConstants()) {
      if ("OTHER".equals(constant.toString())) {
        return constant;
      }
    }
    throw new IllegalStateException("No OTHER entry type");
  }

  private static Document parse(String xml) throws Exception {
    return XmlParserFactoryProducer.createSecureDocBuilderFactory()
        .newDocumentBuilder()
        .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }
}
