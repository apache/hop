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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/** Api keys and access tokens are stored the way Hop stores every other secret. */
class LanguageModelChatMetaSecretsTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void keysAreNotWrittenInTheClear() throws Exception {
    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setOpenAiApiKey("sk-open-ai-secret");
    meta.setAnthropicApiKey("sk-anthropic-secret");
    meta.setMistralApiKey("sk-mistral-secret");
    meta.setHuggingFaceAccessToken("hf-secret");

    String xml = meta.getXml();

    assertFalse(xml.contains("sk-open-ai-secret"), xml);
    assertFalse(xml.contains("sk-anthropic-secret"), xml);
    assertFalse(xml.contains("sk-mistral-secret"), xml);
    assertFalse(xml.contains("hf-secret"), xml);
    assertTrue(xml.contains("<openAiApiKey>Encrypted "), xml);

    LanguageModelChatMeta back = fromXml(xml);
    assertEquals("sk-open-ai-secret", back.getOpenAiApiKey());
    assertEquals("sk-anthropic-secret", back.getAnthropicApiKey());
    assertEquals("sk-mistral-secret", back.getMistralApiKey());
    assertEquals("hf-secret", back.getHuggingFaceAccessToken());
  }

  @Test
  void variablesStayVariables() throws Exception {
    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setOpenAiApiKey("${OPENAI_API_KEY}");
    meta.setAnthropicApiKey("${ANTHROPIC_API_KEY}");
    meta.setMistralApiKey("${MISTRAL_API_KEY}");
    meta.setHuggingFaceAccessToken("${HF_ACCESS_TOKEN}");

    String xml = meta.getXml();
    assertTrue(xml.contains("<openAiApiKey>${OPENAI_API_KEY}</openAiApiKey>"), xml);

    LanguageModelChatMeta back = fromXml(xml);
    assertEquals("${OPENAI_API_KEY}", back.getOpenAiApiKey());
    assertEquals("${ANTHROPIC_API_KEY}", back.getAnthropicApiKey());
    assertEquals("${MISTRAL_API_KEY}", back.getMistralApiKey());
    assertEquals("${HF_ACCESS_TOKEN}", back.getHuggingFaceAccessToken());
  }

  /** Pipelines written before the fields became passwords hold their value in the clear. */
  @Test
  void plainTextKeysFromOlderPipelinesStillLoad() throws Exception {
    LanguageModelChatMeta back =
        fromXml(
            "<openAiApiKey>sk-written-by-an-older-hop</openAiApiKey>"
                + "<mistralApiKey>${MISTRAL_API_KEY}</mistralApiKey>"
                + "<huggingFaceAccessToken>HF_ACCESS_TOKEN</huggingFaceAccessToken>");

    assertEquals("sk-written-by-an-older-hop", back.getOpenAiApiKey());
    assertEquals("${MISTRAL_API_KEY}", back.getMistralApiKey());
    assertEquals("HF_ACCESS_TOKEN", back.getHuggingFaceAccessToken());
  }

  /** The field default disagreed with setDefault() and shipped a stray brace, see issue #8336. */
  @Test
  void openAiApiKeyDefaultHasNoStrayBrace() {
    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    assertEquals("OPENAI_API_KEY", meta.getOpenAiApiKey());

    meta.setDefault();
    assertEquals("OPENAI_API_KEY", meta.getOpenAiApiKey());
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
