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

package org.apache.hop.ai.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class HopAiConfigTest {

  @Test
  void defaultsArePrivacySafe() {
    HopAiConfig config = new HopAiConfig();
    assertFalse(config.isAiEnabled());
    assertEquals("", config.getDefaultProviderName());
    assertFalse(config.isAllowSendFullXml());
    // The plugin's ai-context.md holds the standing notes; the user's notes start empty.
    assertEquals("", config.getExtraContext());
    assertEquals(HopAiConfig.DEFAULT_EXTRA_CONTEXT_FILES, config.getExtraContextFiles());
  }

  @Test
  void theNotesEarlierVersionsSetByDefaultAreReadAsNone() {
    HopAiConfig config = new HopAiConfig();
    config.setExtraContext(HopAiConfig.LEGACY_DEFAULT_EXTRA_CONTEXT);
    assertEquals("", config.getExtraContext());
    // As hop-config.json may hold them, with other line ends.
    config.setExtraContext(HopAiConfig.LEGACY_DEFAULT_EXTRA_CONTEXT.replace("\n", "\r\n") + "\n");
    assertEquals("", config.getExtraContext());
    config.setExtraContext(
        HopAiConfig.LEGACY_DEFAULT_EXTRA_CONTEXT + "Our tables are in the dwh schema.");
    assertTrue(config.getExtraContext().contains("dwh schema"), "edited notes are kept");
  }

  @Test
  void blankNotesAreNone() {
    HopAiConfig config = new HopAiConfig();
    config.setExtraContext("\n");
    assertEquals("", config.getExtraContext());
    config.setExtraContext(null);
    assertEquals("", config.getExtraContext());
  }

  @Test
  void copyConstructor() {
    HopAiConfig original = new HopAiConfig();
    original.setAiEnabled(true);
    original.setDefaultProviderName("prod-openai");
    original.setAllowSendFullXml(true);
    original.setExtraContext("Metadata names are case-sensitive.");
    original.setExtraContextFiles("${PROJECT_HOME}/AGENTS.md\n${PROJECT_HOME}/HOP.md");
    HopAiConfig copy = new HopAiConfig(original);
    assertTrue(copy.isAiEnabled());
    assertEquals("prod-openai", copy.getDefaultProviderName());
    assertTrue(copy.isAllowSendFullXml());
    assertEquals("Metadata names are case-sensitive.", copy.getExtraContext());
    assertEquals("${PROJECT_HOME}/AGENTS.md\n${PROJECT_HOME}/HOP.md", copy.getExtraContextFiles());
  }
}
