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

package org.apache.hop.ai.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.model.anthropic.AnthropicModelCatalog;
import dev.langchain4j.model.catalog.ModelCatalog;
import dev.langchain4j.model.catalog.ModelDescription;
import dev.langchain4j.model.catalog.ModelType;
import dev.langchain4j.model.mistralai.MistralAiModelCatalog;
import dev.langchain4j.model.openai.OpenAiModelCatalog;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.provider.IAiProvider;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

/**
 * Live model-id lists for {@link AiProvider}. Uses langchain4j {@code ModelCatalog} where it exists
 * (OpenAI, Anthropic, Mistral), Ollama {@code /api/tags} otherwise, and OpenAI {@code /models} as a
 * fallback for compatible endpoints.
 */
public final class AiModelCatalog {

  public static final int MAX_MODELS = 500;

  private AiModelCatalog() {}

  public static List<String> listModelNames(AiProvider provider, IVariables variables)
      throws HopException {
    if (provider == null || provider.getProvider() == null) {
      throw new HopException("Please select an AI provider type.");
    }
    IAiProvider backend = provider.getProvider();
    String hopType = Const.NVL(backend.getHopModelType(), "OPEN_AI");
    if ("HUGGING_FACE".equals(hopType)) {
      throw new HopException(
          "Hugging Face does not expose a model catalog. Enter a router model id or a dedicated endpoint URL.");
    }
    String baseUrl = resolve(variables, provider.getBaseUrl());
    if (Utils.isEmpty(baseUrl)) {
      baseUrl = Const.NVL(backend.getDefaultBaseUrl(), "");
    }
    String apiKey = resolve(variables, provider.getApiKey());
    if (backend.requiresApiKey() && Utils.isEmpty(apiKey)) {
      throw new HopException("Set an API key (a variable is fine) before listing models.");
    }
    Duration timeout = timeoutOf(provider.getTimeoutSeconds());
    try {
      List<String> names =
          switch (hopType) {
            case "OLLAMA" -> listOllama(baseUrl, timeout);
            case "ANTHROPIC" ->
                namesFromCatalog(
                    AnthropicModelCatalog.builder()
                        .apiKey(apiKey)
                        .baseUrl(emptyToNull(baseUrl))
                        .timeout(timeout)
                        .build());
            case "MISTRAL" ->
                namesFromCatalog(
                    MistralAiModelCatalog.builder()
                        .apiKey(apiKey)
                        .baseUrl(emptyToNull(baseUrl))
                        .timeout(timeout)
                        .build());
            default -> listOpenAiFamily(baseUrl, apiKey, timeout);
          };
      if (names.isEmpty()) {
        throw new HopException("The provider returned no models.");
      }
      return names;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Could not list models: "
              + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()),
          e);
    }
  }

  static List<String> listOpenAiFamily(String baseUrl, String apiKey, Duration timeout)
      throws Exception {
    try {
      OpenAiModelCatalog.Builder builder =
          OpenAiModelCatalog.builder()
              .apiKey(Const.NVL(apiKey, "none"))
              .connectTimeout(timeout)
              .readTimeout(timeout);
      if (!Utils.isEmpty(baseUrl)) {
        builder.baseUrl(baseUrl);
      }
      List<String> names = namesFromCatalog(builder.build());
      if (!names.isEmpty()) {
        return names;
      }
    } catch (Exception ignored) {
      // Compatible endpoints that do not implement langchain4j's catalog still often serve /models.
    }
    return listOpenAiHttp(baseUrl, apiKey, timeout);
  }

  static List<String> namesFromCatalog(ModelCatalog catalog) {
    List<String> names = new ArrayList<>();
    if (catalog == null) {
      return names;
    }
    for (ModelDescription description : catalog.listModels()) {
      if (description == null || Utils.isEmpty(description.name())) {
        continue;
      }
      if (skipModelType(description.type())) {
        continue;
      }
      names.add(description.name().trim());
    }
    return uniqueSorted(names);
  }

  static boolean skipModelType(ModelType type) {
    if (type == null || type == ModelType.CHAT || type == ModelType.OTHER) {
      return false;
    }
    return true;
  }

  static List<String> listOllama(String baseUrl, Duration timeout) throws Exception {
    String root = stripKnownSuffix(Const.NVL(baseUrl, "http://localhost:11434"), "/v1", "/api");
    String body = httpGet(root + "/api/tags", null, timeout);
    return parseOllamaTags(body);
  }

  static List<String> listOpenAiHttp(String baseUrl, String apiKey, Duration timeout)
      throws Exception {
    if (Utils.isEmpty(baseUrl)) {
      throw new HopException("Base URL is required to list models for this provider.");
    }
    String root = stripTrailingSlash(baseUrl);
    String body = httpGet(root + "/models", apiKey, timeout);
    return parseOpenAiModels(body);
  }

  static List<String> parseOllamaTags(String json) throws Exception {
    ObjectMapper mapper = HopJson.newMapper();
    JsonNode root = mapper.readTree(json);
    JsonNode models = root != null ? root.get("models") : null;
    List<String> names = new ArrayList<>();
    if (models != null && models.isArray()) {
      for (JsonNode model : models) {
        String name = text(model, "name", "model");
        if (!Utils.isEmpty(name)) {
          names.add(name);
        }
      }
    }
    return uniqueSorted(names);
  }

  static List<String> parseOpenAiModels(String json) throws Exception {
    ObjectMapper mapper = HopJson.newMapper();
    JsonNode root = mapper.readTree(json);
    JsonNode data = root != null ? root.get("data") : null;
    List<String> names = new ArrayList<>();
    if (data != null && data.isArray()) {
      for (JsonNode model : data) {
        String id = text(model, "id", "name");
        if (!Utils.isEmpty(id)) {
          names.add(id);
        }
      }
    }
    return uniqueSorted(names);
  }

  static String httpGet(String url, String bearerToken, Duration timeout) throws Exception {
    HttpClient client = HttpClient.newBuilder().connectTimeout(timeout).build();
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create(url))
            .timeout(timeout)
            .GET()
            .header("Accept", "application/json");
    if (!Utils.isEmpty(bearerToken)) {
      builder.header("Authorization", "Bearer " + bearerToken);
    }
    HttpResponse<String> response =
        client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    int status = response.statusCode();
    if (status < 200 || status >= 300) {
      throw new HopException("HTTP " + status + " listing models from " + url);
    }
    return Const.NVL(response.body(), "");
  }

  static List<String> uniqueSorted(List<String> names) {
    Set<String> unique = new LinkedHashSet<>();
    for (String name : names) {
      if (!Utils.isEmpty(name)) {
        unique.add(name.trim());
      }
    }
    List<String> sorted = new ArrayList<>(unique);
    sorted.sort(String.CASE_INSENSITIVE_ORDER);
    if (sorted.size() > MAX_MODELS) {
      return new ArrayList<>(sorted.subList(0, MAX_MODELS));
    }
    return sorted;
  }

  static String text(JsonNode node, String... fields) {
    if (node == null || fields == null) {
      return "";
    }
    for (String field : fields) {
      JsonNode value = node.get(field);
      if (value != null && value.isValueNode() && !value.isNull()) {
        String text = value.asText("");
        if (!Utils.isEmpty(text)) {
          return text.trim();
        }
      }
    }
    return "";
  }

  static String stripKnownSuffix(String url, String... suffixes) {
    String root = stripTrailingSlash(url);
    if (Utils.isEmpty(root) || suffixes == null) {
      return root;
    }
    String lower = root.toLowerCase(Locale.ROOT);
    for (String suffix : suffixes) {
      if (lower.endsWith(suffix)) {
        return stripTrailingSlash(root.substring(0, root.length() - suffix.length()));
      }
    }
    return root;
  }

  static String stripTrailingSlash(String url) {
    if (Utils.isEmpty(url)) {
      return "";
    }
    String trimmed = url.trim();
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  static String emptyToNull(String value) {
    return Utils.isEmpty(value) ? null : value;
  }

  static Duration timeoutOf(String timeoutSeconds) {
    int seconds = 30;
    if (!Utils.isEmpty(timeoutSeconds)) {
      try {
        seconds = Math.max(1, Integer.parseInt(timeoutSeconds.trim()));
      } catch (NumberFormatException ignored) {
        seconds = 30;
      }
    }
    return Duration.ofSeconds(seconds);
  }

  static String resolve(IVariables variables, String value) {
    if (value == null) {
      return "";
    }
    return variables != null ? variables.resolve(value) : value;
  }
}
