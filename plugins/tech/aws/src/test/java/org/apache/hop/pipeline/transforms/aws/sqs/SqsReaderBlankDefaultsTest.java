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

package org.apache.hop.pipeline.transforms.aws.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/**
 * Blank SQS Reader settings must fall back to their defaults. Up to 2.17 a saved empty tag loaded
 * as null; since 2.18 it loads as an empty string. A blank "max messages" must still mean 0
 * (unlimited) and a blank "use AWS credentials chain" must still mean "N" (use the configured
 * keys). No AWS calls are made: building an SDK client is offline.
 */
class SqsReaderBlankDefaultsTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void blankSettingsFallBackToDefaults() throws Exception {
    SqsReaderMeta meta = loadMeta("<AwsCredChain/>", "<field_max_messages/>");

    assertEquals("N", meta.getAwsCredChain());
    assertEquals("0", meta.getTFldMaxMessages());
  }

  @Test
  void blankMaxMessagesInitializesAsUnlimited() throws Exception {
    SqsReaderMeta meta = loadMeta("<AwsCredChain>N</AwsCredChain>", "<field_max_messages/>");
    SqsReaderData data = new SqsReaderData();

    assertTrue(newSqsReader(meta, data).init(), "init() must accept a blank max messages");
    assertEquals(0, data.realMaxMessages);
  }

  @Test
  void blankCredChainUsesConfiguredKeys() throws Exception {
    SqsReaderMeta meta = loadMeta("<AwsCredChain/>", "<field_max_messages>0</field_max_messages>");
    BaseTransform transform = resolvingTransform();

    assertTrue(new AwsSqsReader(meta, new PipelineMeta(), transform).getAWSConnection());
    verify(transform, never()).logBasic(contains("Credentials Chain"));
  }

  @Test
  void specifiedValuesAreUsed() throws Exception {
    SqsReaderMeta meta =
        loadMeta("<AwsCredChain>Y</AwsCredChain>", "<field_max_messages>5</field_max_messages>");
    assertEquals("Y", meta.getAwsCredChain());
    assertEquals("5", meta.getTFldMaxMessages());

    meta.setAwsCredChain("N");
    SqsReaderData data = new SqsReaderData();
    assertTrue(newSqsReader(meta, data).init());
    assertEquals(5, data.realMaxMessages);
  }

  private static BaseTransform resolvingTransform() {
    BaseTransform transform = mock(BaseTransform.class);
    when(transform.resolve(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
    return transform;
  }

  private static SqsReader newSqsReader(SqsReaderMeta meta, SqsReaderData data) {
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("sqs", meta);
    pipelineMeta.addTransform(transformMeta);
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    return new SqsReader(transformMeta, meta, data, 0, pipelineMeta, pipeline);
  }

  private static SqsReaderMeta loadMeta(String credChainTag, String maxMessagesTag)
      throws Exception {
    String xml =
        "<transform>"
            + credChainTag
            + "<aws_key>key</aws_key>"
            + "<aws_key_secret>secret</aws_key_secret>"
            + "<aws_region>eu-west-1</aws_region>"
            + "<sqs_queue>https://sqs.eu-west-1.amazonaws.com/123456789012/queue</sqs_queue>"
            + "<field_message_id>id</field_message_id>"
            + "<field_message_body>body</field_message_body>"
            + "<field_message_delete>N</field_message_delete>"
            + maxMessagesTag
            + "</transform>";
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    SqsReaderMeta meta = new SqsReaderMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());
    return meta;
  }
}
