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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.provider.IAiProvider;
import org.apache.hop.ai.providers.AnthropicProvider;
import org.apache.hop.ai.providers.CustomOpenAiProvider;
import org.apache.hop.ai.providers.GeminiProvider;
import org.apache.hop.ai.providers.GrokProvider;
import org.apache.hop.ai.providers.HuggingFaceProvider;
import org.apache.hop.ai.providers.MistralProvider;
import org.apache.hop.ai.providers.OllamaProvider;
import org.apache.hop.ai.providers.OpenAiProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;

/**
 * Imports hopper-edw / older {@code hopAiConfig} API keys into an {@link AiProvider} metadata
 * object and rewrites hop-config without those secrets.
 */
public final class HopAiLegacyConfigMigrator {

  private static final AtomicBoolean migrated = new AtomicBoolean();

  private HopAiLegacyConfigMigrator() {}

  public static void migrate(IHopMetadataProvider metadataProvider) {
    if (metadataProvider == null || migrated.get()) {
      return;
    }
    HopAiConfig config = HopAiConfigSingleton.getConfig();
    Map<String, Object> legacy = HopAiConfigSingleton.peekLegacySnapshot();
    if (!hasLegacyKeys(legacy)) {
      migrated.set(true);
      return;
    }
    try {
      apply(legacy, config, metadataProvider);
      HopAiConfigSingleton.clearLegacySnapshot();
      HopAiConfigSingleton.saveConfig();
      migrated.set(true);
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Could not migrate hopAiConfig into an AI Provider", e);
    }
  }

  static boolean apply(
      Map<String, Object> legacy, HopAiConfig config, IHopMetadataProvider metadataProvider)
      throws HopException {
    if (!hasLegacyKeys(legacy) || config == null || metadataProvider == null) {
      return false;
    }
    if (legacy.containsKey("aiEnabled")) {
      config.setAiEnabled(booleanValue(legacy.get("aiEnabled")));
    }
    String pluginId = pluginIdForPreset(stringValue(legacy.get("aiProviderPreset")));
    String name = displayName(pluginId);
    IHopMetadataSerializer<AiProvider> serializer =
        metadataProvider.getSerializer(AiProvider.class);
    AiProvider existing = serializer.exists(name) ? serializer.load(name) : null;
    if (existing == null) {
      AiProvider provider = new AiProvider();
      provider.setName(name);
      IAiProvider backend = newBackend(pluginId);
      backend.setPluginId(pluginId);
      backend.setPluginName(name);
      provider.setProvider(backend);
      String apiKey = stringValue(legacy.get("aiApiKey"));
      String baseUrl = stringValue(legacy.get("aiBaseUrl"));
      String modelName = stringValue(legacy.get("aiModelName"));
      String temperature = stringValue(legacy.get("aiTemperature"));
      if (!Utils.isEmpty(apiKey)) {
        provider.setApiKey(apiKey);
      }
      if (!Utils.isEmpty(baseUrl)) {
        provider.setBaseUrl(baseUrl);
      }
      if (!Utils.isEmpty(modelName)) {
        provider.setModelName(modelName);
      }
      if (!Utils.isEmpty(temperature)) {
        provider.setTemperature(temperature);
      }
      provider.applyProviderDefaults();
      serializer.save(provider);
    }
    if (Utils.isEmpty(config.getDefaultProviderName())) {
      config.setDefaultProviderName(name);
    }
    return true;
  }

  static boolean hasLegacyKeys(Map<String, Object> map) {
    if (map == null || map.isEmpty()) {
      return false;
    }
    return map.containsKey("aiApiKey") || map.containsKey("aiProviderPreset");
  }

  static String pluginIdForPreset(String preset) {
    if (Utils.isEmpty(preset)) {
      return "openai-custom";
    }
    return switch (preset.trim().toUpperCase()) {
      case "GROK" -> "grok";
      case "OPENAI" -> "openai";
      case "GOOGLE_GEMINI", "GEMINI" -> "google-gemini";
      case "ANTHROPIC" -> "anthropic";
      case "OLLAMA" -> "ollama";
      case "HUGGING_FACE", "HUGGINGFACE" -> "hugging-face";
      case "MISTRAL" -> "mistral";
      default -> "openai-custom";
    };
  }

  static String displayName(String pluginId) {
    return switch (pluginId) {
      case "grok" -> "Grok (xAI)";
      case "openai" -> "OpenAI";
      case "google-gemini" -> "Google (Gemini)";
      case "anthropic" -> "Anthropic";
      case "ollama" -> "Ollama";
      case "hugging-face" -> "Hugging Face";
      case "mistral" -> "Mistral";
      default -> "Custom (OpenAI compatible)";
    };
  }

  static IAiProvider newBackend(String pluginId) {
    return switch (pluginId) {
      case "grok" -> new GrokProvider();
      case "openai" -> new OpenAiProvider();
      case "google-gemini" -> new GeminiProvider();
      case "anthropic" -> new AnthropicProvider();
      case "ollama" -> new OllamaProvider();
      case "hugging-face" -> new HuggingFaceProvider();
      case "mistral" -> new MistralProvider();
      default -> new CustomOpenAiProvider();
    };
  }

  static Map<String, Object> asStringObjectMap(Object raw) {
    if (!(raw instanceof Map<?, ?> map)) {
      return null;
    }
    Map<String, Object> copy = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() != null) {
        copy.put(String.valueOf(entry.getKey()), entry.getValue());
      }
    }
    return copy;
  }

  private static String stringValue(Object value) {
    return value == null ? "" : String.valueOf(value);
  }

  private static boolean booleanValue(Object value) {
    if (value instanceof Boolean bool) {
      return bool;
    }
    return "true".equalsIgnoreCase(stringValue(value));
  }
}
