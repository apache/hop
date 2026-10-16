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

package org.apache.hop.pipeline.transforms.aws.sns;

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
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/**
 * A blank "use AWS credentials chain" setting must still mean "N" (use the configured keys). Up to
 * 2.17 a saved {@code <AwsCredChain/>} loaded as null; since 2.18 it loads as an empty string. No
 * AWS calls are made: building an SDK client is offline.
 */
class SnsNotifyBlankCredChainTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void blankCredChainUsesConfiguredKeys() throws Exception {
    SnsNotifyMeta meta = loadMeta("<AwsCredChain/>");
    assertEquals("N", meta.getAwsCredChain());

    BaseTransform transform = resolvingTransform();
    assertTrue(new AwsSns(meta, new PipelineMeta(), transform).getAWSConnection());
    verify(transform, never()).logBasic(contains("Credentials Chain"));
    verify(transform).logBasic(contains("with API-Key"));
  }

  @Test
  void specifiedCredChainIsUsed() throws Exception {
    assertEquals("Y", loadMeta("<AwsCredChain>Y</AwsCredChain>").getAwsCredChain());

    SnsNotifyMeta meta = loadMeta("<AwsCredChain>N</AwsCredChain>");
    BaseTransform transform = resolvingTransform();
    assertTrue(new AwsSns(meta, new PipelineMeta(), transform).getAWSConnection());
    verify(transform, never()).logBasic(contains("Credentials Chain"));
    verify(transform).logBasic(contains("with API-Key"));
  }

  private static BaseTransform resolvingTransform() {
    BaseTransform transform = mock(BaseTransform.class);
    when(transform.resolve(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
    return transform;
  }

  private static SnsNotifyMeta loadMeta(String credChainTag) throws Exception {
    String xml =
        "<transform>"
            + credChainTag
            + "<aws_key>key</aws_key>"
            + "<aws_key_secret>secret</aws_key_secret>"
            + "<aws_region>eu-west-1</aws_region>"
            + "<notify_point>Send once with first row</notify_point>"
            + "<chooser_topic_arn>N</chooser_topic_arn>"
            + "<chooser_subject>N</chooser_subject>"
            + "<chooser_message>N</chooser_message>"
            + "<value_topic_arn>arn:aws:sns:eu-west-1:123456789012:topic</value_topic_arn>"
            + "<value_subject>subject</value_subject>"
            + "<value_message>message</value_message>"
            + "<field_message_id>id</field_message_id>"
            + "</transform>";
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), "transform");
    SnsNotifyMeta meta = new SnsNotifyMeta();
    meta.loadXml(transformNode, new MemoryMetadataProvider());
    return meta;
  }
}
