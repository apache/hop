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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.OllamaThink;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.w3c.dom.Node;

/** The Ollama Thinking option is saved with the transform, and an older transform has none. */
class LanguageModelChatMetaThinkTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void thinkIsLeftToTheModelByDefault() {
    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setDefault();
    assertEquals(OllamaThink.DEFAULT, meta.getOllamaThink());
    assertNull(OllamaThink.think(meta.getOllamaThink()));
  }

  @Test
  void thinkSurvivesASave() throws Exception {
    for (OllamaThink think : OllamaThink.values()) {
      LanguageModelChatMeta meta = new LanguageModelChatMeta();
      meta.setOllamaThink(think);
      String xml = meta.getXml();
      assertTrue(xml.contains("<ollamaThink>" + think.name() + "</ollamaThink>"), xml);
      assertEquals(think, fromXml(xml).getOllamaThink());
    }
  }

  @Test
  void aTransformSavedBeforeTheOptionLeavesItToTheModel() throws Exception {
    String xml =
        new LanguageModelChatMeta().getXml().replaceAll("<ollamaThink>[^<]*</ollamaThink>", "");
    assertFalse(xml.contains("ollamaThink"), xml);
    // Not Off: a missing value must not switch thinking off.
    assertNull(OllamaThink.think(fromXml(xml).getOllamaThink()));
  }

  @Test
  void eachSettingMapsOntoThink() {
    assertNull(OllamaThink.think(null));
    assertNull(OllamaThink.DEFAULT.think());
    assertEquals(Boolean.FALSE, OllamaThink.OFF.think());
    assertEquals(Boolean.TRUE, OllamaThink.ON.think());
    assertEquals(OllamaThink.DEFAULT, OllamaThink.of(null));
    assertEquals(OllamaThink.OFF, OllamaThink.of(false));
    assertEquals(OllamaThink.ON, OllamaThink.of(true));
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
