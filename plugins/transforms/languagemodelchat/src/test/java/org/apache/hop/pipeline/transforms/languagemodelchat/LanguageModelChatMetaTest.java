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

package org.apache.hop.pipeline.transforms.languagemodelchat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ModelType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

class LanguageModelChatMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void theGeneralSettingsSurviveASave() throws Exception {
    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setInputField("question");
    meta.setInputChatJson(true);
    meta.setOutputChatJson(true);
    meta.setMock(true);
    meta.setMockOutputValue("mocked answer");
    meta.setOutputFieldNamePrefix("chat_");
    meta.setIdentifierValue("run-42");
    meta.setModelType(ModelType.OLLAMA.code());
    meta.setParallelism(4);
    meta.setOpenAiTemperature(0.2);
    meta.setOpenAiMaxTokens(512);

    LanguageModelChatMeta copy = fromXml(meta.getXml());

    assertEquals("question", copy.getInputField());
    assertTrue(copy.isInputChatJson());
    assertTrue(copy.isOutputChatJson());
    assertTrue(copy.isMock());
    assertEquals("mocked answer", copy.getMockOutputValue());
    assertEquals("chat_", copy.getOutputFieldNamePrefix());
    assertEquals("run-42", copy.getIdentifierValue());
    assertEquals(ModelType.OLLAMA.code(), copy.getModelType());
    assertEquals(4, copy.getParallelism());
    assertEquals(0.2, copy.getOpenAiTemperature());
    assertEquals(512, copy.getOpenAiMaxTokens());
  }

  private LanguageModelChatMeta fromXml(String xml) throws Exception {
    Node node =
        XmlHandler.loadXmlString(
            XmlHandler.openTag("transform") + xml + XmlHandler.closeTag("transform"), "transform");
    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.loadXml(node, new MemoryMetadataProvider());
    return meta;
  }
}
