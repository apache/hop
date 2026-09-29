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
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * A Kettle Kafka consumer stores its sub-transformation in {@code transformationPath}. Repository
 * references omit the {@code .ktr} extension, and Hop cannot open the imported pipeline until that
 * path ends with {@code .hpl} (issue #4107).
 */
class KettleImportKafkaConsumerTest {

  private static final String ENTRY_TYPE = "org.apache.hop.imports.kettle.KettleImport$EntryType";

  @ParameterizedTest
  @CsvSource({"KafkaConsumerInput,KafkaConsumerInput", "KettleKafkaConsumerInput,KafkaConsumer"})
  void testPathWithoutExtensionGainsHpl(String kettleType, String hopType) throws Exception {
    Node transform =
        importKafkaStep(
            kettleType, "${Internal.Entry.Current.Directory}/yyy/yyPhase1KafkaSub", true);

    assertEquals(hopType, XmlHandler.getTagValue(transform, "type"));
    assertEquals(
        "${Internal.Entry.Current.Folder}/yyy/yyPhase1KafkaSub.hpl",
        XmlHandler.getTagValue(transform, "pipelinePath"));
    assertNull(XmlHandler.getSubNode(transform, "transformationPath"));
    assertNull(XmlHandler.getSubNode(transform, "remotesteps"));
  }

  @Test
  void testKtrExtensionIsRewrittenToHpl() throws Exception {
    Node transform =
        importKafkaStep(
            "KafkaConsumerInput", "${Internal.Entry.Current.Directory}/abortSub.ktr", false);

    assertEquals(
        "${Internal.Entry.Current.Folder}/abortSub.hpl",
        XmlHandler.getTagValue(transform, "pipelinePath"));
  }

  @Test
  void testUpperCaseKtrExtensionIsRewrittenToHpl() throws Exception {
    Node transform =
        importKafkaStep("KafkaConsumerInput", "C:/wfpl/yyy/yyPhase1KafkaSub.KTR", false);

    assertEquals(
        "C:/wfpl/yyy/yyPhase1KafkaSub.hpl", XmlHandler.getTagValue(transform, "pipelinePath"));
  }

  @Test
  void testWindowsPathWithoutExtensionGainsHpl() throws Exception {
    Node transform =
        importKafkaStep("KafkaConsumerInput", "C:\\wfpl\\yyy\\yyPhase1KafkaSub", false);

    assertEquals(
        "C:\\wfpl\\yyy\\yyPhase1KafkaSub.hpl", XmlHandler.getTagValue(transform, "pipelinePath"));
  }

  @Test
  void testEmptyPathStaysEmpty() throws Exception {
    Node transform = importKafkaStep("KafkaConsumerInput", "", false);

    // An empty element has no text child, so the tag value is null rather than ".hpl".
    assertNotNull(XmlHandler.getSubNode(transform, "pipelinePath"));
    assertNull(XmlHandler.getTagValue(transform, "pipelinePath"));
  }

  @Test
  void testExistingExtensionIsNotDoubled() throws Exception {
    Node transform =
        importKafkaStep(
            "KafkaConsumerInput", "${Internal.Entry.Current.Directory}/child.hpl", false);

    assertEquals(
        "${Internal.Entry.Current.Folder}/child.hpl",
        XmlHandler.getTagValue(transform, "pipelinePath"));
  }

  /** A trailing slash is a folder, not a pipeline name missing its extension. */
  @Test
  void testDirectoryPathIsNotGivenHpl() throws Exception {
    Node transform =
        importKafkaStep("KafkaConsumerInput", "${Internal.Entry.Current.Directory}/yyy/", false);

    assertEquals(
        "${Internal.Entry.Current.Folder}/yyy/", XmlHandler.getTagValue(transform, "pipelinePath"));
  }

  /** Another step can carry the same element name. Only the Kafka consumer gains {@code .hpl}. */
  @Test
  void testOtherStepPathIsLeftWithoutExtension() throws Exception {
    Document doc =
        parse(
            "<transformation><step><name>other</name><type>Dummy</type>"
                + "<transformationPath>${Internal.Entry.Current.Directory}/child</transformationPath>"
                + "</step></transformation>");
    processNode(doc);

    Node transform = XmlHandler.getSubNode(XmlHandler.getSubNode(doc, "pipeline"), "transform");
    assertEquals(
        "${Internal.Entry.Current.Folder}/child",
        XmlHandler.getTagValue(transform, "pipelinePath"));
  }

  private Node importKafkaStep(String type, String transformationPath, boolean withRemoteSteps)
      throws Exception {
    String remoteSteps =
        withRemoteSteps ? "<remotesteps><input></input><output></output></remotesteps>" : "";
    Document doc =
        parse(
            "<transformation><step><name>Kafka Consumer</name><type>"
                + type
                + "</type><transformationPath>"
                + transformationPath
                + "</transformationPath>"
                + remoteSteps
                + "</step></transformation>");
    processNode(doc);
    return XmlHandler.getSubNode(XmlHandler.getSubNode(doc, "pipeline"), "transform");
  }

  private static void processNode(Document doc) throws Exception {
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
    method.invoke(new KettleImport(), doc, doc, other, 0);
  }

  private static Document parse(String xml) throws Exception {
    return XmlParserFactoryProducer.createSecureDocBuilderFactory()
        .newDocumentBuilder()
        .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }
}
