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
    assertEquals("", config.getExtraContext());
    assertEquals(HopAiConfig.DEFAULT_EXTRA_CONTEXT_FILES, config.getExtraContextFiles());
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
