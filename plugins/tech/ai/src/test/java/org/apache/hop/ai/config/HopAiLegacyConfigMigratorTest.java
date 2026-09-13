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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.Test;

class HopAiLegacyConfigMigratorTest {

  @Test
  void grokPresetCreatesProviderAndSetsDefaultName() throws Exception {
    Map<String, Object> legacy = new LinkedHashMap<>();
    legacy.put("aiEnabled", true);
    legacy.put("aiProviderPreset", "GROK");
    legacy.put("aiApiKey", "xai-secret");
    legacy.put("aiBaseUrl", "https://api.x.ai/v1");
    legacy.put("aiModelName", "grok-4");
    legacy.put("aiTemperature", "0.3");
    HopAiConfig config = new HopAiConfig();
    MemoryMetadataProvider metadata = new MemoryMetadataProvider();

    assertTrue(HopAiLegacyConfigMigrator.apply(legacy, config, metadata));
    assertTrue(config.isAiEnabled());
    assertEquals("Grok (xAI)", config.getDefaultProviderName());
    AiProvider saved = metadata.getSerializer(AiProvider.class).load("Grok (xAI)");
    assertNotNull(saved);
    assertEquals("xai-secret", saved.getApiKey());
    assertEquals("grok-4", saved.getModelName());
    assertEquals("https://api.x.ai/v1", saved.getBaseUrl());
    assertEquals("grok", saved.getPluginId());
  }

  @Test
  void alreadyNewConfigIsNoOp() throws Exception {
    HopAiConfig config = new HopAiConfig();
    config.setDefaultProviderName("prod");
    MemoryMetadataProvider metadata = new MemoryMetadataProvider();
    assertFalse(HopAiLegacyConfigMigrator.apply(Map.of("aiEnabled", true), config, metadata));
    assertEquals("prod", config.getDefaultProviderName());
    assertTrue(metadata.getSerializer(AiProvider.class).listObjectNames().isEmpty());
  }

  @Test
  void existingProviderIsNotDuplicated() throws Exception {
    MemoryMetadataProvider metadata = new MemoryMetadataProvider();
    AiProvider existing = new AiProvider();
    existing.setName("Grok (xAI)");
    existing.setApiKey("already");
    metadata.getSerializer(AiProvider.class).save(existing);

    Map<String, Object> legacy = new LinkedHashMap<>();
    legacy.put("aiProviderPreset", "GROK");
    legacy.put("aiApiKey", "new-secret");
    HopAiConfig config = new HopAiConfig();
    assertTrue(HopAiLegacyConfigMigrator.apply(legacy, config, metadata));
    AiProvider loaded = metadata.getSerializer(AiProvider.class).load("Grok (xAI)");
    assertEquals("already", loaded.getApiKey());
    assertEquals("Grok (xAI)", config.getDefaultProviderName());
  }

  @Test
  void mergedLegacyMapKeepsSecretsUntilMigrate() {
    Map<String, Object> legacy = new LinkedHashMap<>();
    legacy.put("aiApiKey", "secret");
    legacy.put("aiProviderPreset", "GROK");
    HopAiConfig config = new HopAiConfig();
    config.setAiEnabled(true);
    Map<String, Object> merged = HopAiConfigSingleton.mergedLegacyMap(legacy, config);
    assertEquals("secret", merged.get("aiApiKey"));
    assertEquals("GROK", merged.get("aiProviderPreset"));
    assertEquals(true, merged.get("aiEnabled"));
  }

  @Test
  void presetMapping() {
    assertEquals("grok", HopAiLegacyConfigMigrator.pluginIdForPreset("GROK"));
    assertEquals("google-gemini", HopAiLegacyConfigMigrator.pluginIdForPreset("GOOGLE_GEMINI"));
    assertEquals("hugging-face", HopAiLegacyConfigMigrator.pluginIdForPreset("HUGGING_FACE"));
    assertEquals("openai-custom", HopAiLegacyConfigMigrator.pluginIdForPreset("CUSTOM"));
    assertEquals("openai", HopAiLegacyConfigMigrator.pluginIdForPreset("OPENAI"));
  }
}
