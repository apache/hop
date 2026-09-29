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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * A Kettle Kafka consumer stores its sub-transformation in {@code transformationPath}. Repository
 * references omit the {@code .ktr} extension, and Hop cannot open the imported pipeline until that
 * path ends with {@code .hpl} (issue #4107).
 *
 * <p>Pentaho writes the Kafka Consumer step as {@code KafkaConsumerInput}. Hop's transform id is
 * {@code KafkaConsumer}. Leaving the Kettle id in place makes the imported transform unopenable
 * (issue #4106).
 */
class KettleImportKafkaConsumerTest {

  private static final String ENTRY_TYPE = "org.apache.hop.imports.kettle.KettleImport$EntryType";

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @ParameterizedTest
  @CsvSource({"KafkaConsumerInput,KafkaConsumer", "KettleKafkaConsumerInput,KafkaConsumer"})
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

  @ParameterizedTest
  @ValueSource(strings = {"KafkaConsumerInput", "KettleKafkaConsumerInput"})
  void testKafkaConsumerTypeBecomesKafkaConsumer(String kettleType) throws Exception {
    Document doc = parse(kettleKafkaConsumer(kettleType));
    processNode(doc);

    Node pipeline = XmlHandler.getSubNode(doc, "pipeline");
    assertNotNull(pipeline);
    Node transform = XmlHandler.getSubNode(pipeline, "transform");
    assertNotNull(transform);
    assertEquals("kafkaCnsmr:zsp1", XmlHandler.getTagValue(transform, "name"));
    assertEquals("KafkaConsumer", XmlHandler.getTagValue(transform, "type"));
    assertNull(XmlHandler.getSubNode(transform, "transformationPath"));
    assertEquals(
        "${Internal.Entry.Current.Folder}/child.hpl",
        XmlHandler.getTagValue(transform, "pipelinePath"));
    assertNull(XmlHandler.getSubNode(transform, "SUB_STEP"));
    assertEquals("Output", XmlHandler.getTagValue(transform, "subTransform"));
    assertEquals("orders", XmlHandler.getTagValue(transform, "topic"));
    assertEquals("group-a", XmlHandler.getTagValue(transform, "consumerGroup"));
    assertEquals("100", XmlHandler.getTagValue(transform, "batchSize"));
    assertEquals("1000", XmlHandler.getTagValue(transform, "batchDuration"));
    assertEquals("localhost:9092", XmlHandler.getTagValue(transform, "directBootstrapServers"));
    assertEquals("Y", XmlHandler.getTagValue(transform, "AUTO_COMMIT"));

    Node keyField = XmlHandler.getSubNodeByNr(transform, "OutputField", 0);
    assertEquals("key", XmlHandler.getTagAttribute(keyField, "kafkaName"));
    assertEquals("String", XmlHandler.getTagAttribute(keyField, "type"));
    assertEquals("Key", XmlHandler.getNodeValue(keyField));

    Node option =
        XmlHandler.getSubNode(XmlHandler.getSubNode(transform, "advancedConfig"), "option");
    assertEquals("auto.offset.reset", XmlHandler.getTagAttribute(option, "property"));
    assertEquals("earliest", XmlHandler.getTagAttribute(option, "value"));
  }

  @Test
  void testOtherStepTypesAreLeftAlone() throws Exception {
    Document doc =
        parse(
            "<transformation><step><name>read</name><type>TableInput</type></step></transformation>");
    processNode(doc);

    Node transform = XmlHandler.getSubNode(XmlHandler.getSubNode(doc, "pipeline"), "transform");
    assertEquals("TableInput", XmlHandler.getTagValue(transform, "type"));
  }

  private static String kettleKafkaConsumer(String type) {
    return "<transformation>"
        + "<step>"
        + "<name>kafkaCnsmr:zsp1</name>"
        + "<type>"
        + type
        + "</type>"
        + "<topic>orders</topic>"
        + "<consumerGroup>group-a</consumerGroup>"
        + "<transformationPath>${Internal.Entry.Current.Directory}/child.ktr</transformationPath>"
        + "<SUB_STEP>Output</SUB_STEP>"
        + "<batchSize>100</batchSize>"
        + "<batchDuration>1000</batchDuration>"
        + "<connectionType>DIRECT</connectionType>"
        + "<directBootstrapServers>localhost:9092</directBootstrapServers>"
        + "<AUTO_COMMIT>Y</AUTO_COMMIT>"
        + "<OutputField kafkaName=\"key\" type=\"String\">Key</OutputField>"
        + "<OutputField kafkaName=\"message\" type=\"String\">Message</OutputField>"
        + "<advancedConfig>"
        + "<option property=\"auto.offset.reset\" value=\"earliest\"/>"
        + "</advancedConfig>"
        + "</step>"
        + "</transformation>";
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
