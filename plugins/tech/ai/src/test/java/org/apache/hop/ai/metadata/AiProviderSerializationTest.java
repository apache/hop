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

package org.apache.hop.ai.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.hop.ai.provider.AiProviderPlugin;
import org.apache.hop.ai.provider.AiProviderPluginType;
import org.apache.hop.ai.providers.OllamaProvider;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** An AI provider read from disk must save with its provider type. */
class AiProviderSerializationTest {

  private static JsonMetadataParser<AiProvider> parser;

  @BeforeAll
  static void setUp() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry.addPluginType(AiProviderPluginType.getInstance());
    PluginRegistry.getInstance()
        .registerPluginClass(
            OllamaProvider.class.getName(), AiProviderPluginType.class, AiProviderPlugin.class);
    JsonMetadataProvider metadataProvider =
        new JsonMetadataProvider(
            new HopTwoWayPasswordEncoder(),
            "/tmp/test-metadata",
            Variables.getADefaultVariableSpace());
    parser = new JsonMetadataParser<>(AiProvider.class, metadataProvider);
  }

  @Test
  void aLoadedProviderSavesWithItsType() throws Exception {
    AiProvider provider =
        load(
            "{\"name\":\"local\",\"provider\":{\"ollama\":{}},"
                + "\"baseUrl\":\"http://localhost:11434\",\"modelName\":\"llama3.2\"}");

    assertTrue(provider.hasProviderType());
    assertEquals("ollama", provider.getPluginId());
    assertEquals("Ollama", provider.getPluginName());

    JSONObject saved = parser.getJsonObject(provider);
    JSONObject block = (JSONObject) saved.get("provider");
    assertTrue(block.containsKey("ollama"), saved.toJSONString());
    assertFalse(block.containsKey("null"), saved.toJSONString());
  }

  @Test
  void aProviderSavedWithoutTypeLoadsAndCanBeRepaired() throws Exception {
    AiProvider provider =
        load(
            "{\"name\":\"broken\",\"provider\":{\"null\":{}},"
                + "\"baseUrl\":\"http://localhost:11434\",\"modelName\":\"llama3.2\"}");

    assertFalse(provider.hasProviderType());
    assertEquals("http://localhost:11434", provider.getBaseUrl());
    assertEquals("llama3.2", provider.getModelName());
    assertThrows(HopException.class, () -> parser.getJsonObject(provider));

    provider.setProviderType("Ollama");

    JSONObject block = (JSONObject) parser.getJsonObject(provider).get("provider");
    assertTrue(block.containsKey("ollama"));
    assertEquals("http://localhost:11434", provider.getBaseUrl());
    assertEquals("llama3.2", provider.getModelName());
  }

  @Test
  void thinkingSurvivesASaveAndAnOldProviderLoadsWithout() throws Exception {
    AiProvider old =
        load(
            "{\"name\":\"local\",\"provider\":{\"ollama\":{}},"
                + "\"baseUrl\":\"http://localhost:11434\",\"modelName\":\"qwen3\"}");
    assertEquals("", old.getThinking());

    old.setThinking("Off");
    JSONObject saved = parser.getJsonObject(old);
    assertEquals("Off", saved.get("thinking"), saved.toJSONString());
    assertEquals("Off", load(saved.toJSONString()).getThinking());
  }

  private static AiProvider load(String json) throws Exception {
    try (JsonParser jsonParser = new JsonFactory().createParser(json)) {
      jsonParser.nextToken();
      return parser.loadJsonObject(AiProvider.class, jsonParser);
    }
  }
}
