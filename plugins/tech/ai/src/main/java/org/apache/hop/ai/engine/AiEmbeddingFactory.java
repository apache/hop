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

import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.ollama.OllamaEmbeddingModel;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import org.apache.hop.ai.metadata.AiModelRole;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * Builds an embedding model from an {@link AiProvider}, the counterpart of {@link AiChatFactory}
 * for the chat model.
 *
 * <p>The model comes from the provider's {@link AiModelRole#EMBEDDING} entry, so one provider can
 * serve a chat transform and an embedding transform at the same time. A caller that needs a
 * different model for one pipeline passes it explicitly and it wins.
 */
public final class AiEmbeddingFactory {

  private AiEmbeddingFactory() {}

  /**
   * Loads the named provider once and builds its embedding model, returning the model together with
   * the model name that was actually used.
   *
   * <p>Both come from one load: the name is wanted for the optional output field and for error
   * messages, and reading the metadata twice to get it risks the two disagreeing.
   *
   * @param providerName the {@code AiProvider} to use
   * @param modelName an embedding model that overrides the provider's, or empty to use its {@link
   *     AiModelRole#EMBEDDING} entry
   * @param variables used to resolve the provider's fields
   * @param metadataProvider where the provider is loaded from
   * @throws HopException when the provider is missing, incomplete, or serves no embedding model
   */
  public static ResolvedEmbeddingModel resolveEmbeddingModel(
      String providerName,
      String modelName,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    return resolveEmbeddingModel(providerName, modelName, variables, metadataProvider, null);
  }

  /**
   * As {@link #resolveEmbeddingModel(String, String, IVariables, IHopMetadataProvider)}, with the
   * number of times the model retries a failed call itself.
   *
   * <p>langchain4j logs a full stack trace at WARN for every retry, outside of Hop's log level. A
   * caller that retries on its own, so it can say what failed in one line, passes 0.
   *
   * @param maxRetries the model's own retries, or null for langchain4j's default
   */
  public static ResolvedEmbeddingModel resolveEmbeddingModel(
      String providerName,
      String modelName,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      Integer maxRetries)
      throws HopException {
    AiProvider provider = AiProviderLoader.load(providerName, metadataProvider);
    String resolvedName =
        Utils.isEmpty(modelName)
            ? variables.resolve(provider.resolveModelName(AiModelRole.EMBEDDING))
            : variables.resolve(modelName);
    // of() rejects a null or half configured provider, so there is one guard, not two.
    AiProviderSettings settings = AiProviderSettings.of(provider, variables);
    return new ResolvedEmbeddingModel(
        createEmbeddingModel(provider, settings, resolvedName, variables, maxRetries),
        resolvedName,
        provider.getName(),
        effectiveBaseUrl(settings));
  }

  /**
   * An embedding model, the name of the model it talks to, and where it lives: the provider name
   * and the resolved base URL, which a failed call needs to say which endpoint did not answer.
   */
  public record ResolvedEmbeddingModel(
      EmbeddingModel model, String modelName, String providerName, String baseUrl) {}

  /**
   * The endpoint langchain4j uses when an OpenAI compatible provider has no base URL, as its
   * OpenAiEmbeddingModel falls back to it.
   */
  static final String OPEN_AI_DEFAULT_BASE_URL = "https://api.openai.com/v1";

  /** The URL the model actually talks to, so a failed call can name it even when none is set. */
  static String effectiveBaseUrl(AiProviderSettings settings) {
    if (Utils.isEmpty(settings.baseUrl()) && "OPEN_AI".equals(settings.type())) {
      return OPEN_AI_DEFAULT_BASE_URL;
    }
    return settings.baseUrl();
  }

  /** Builds the provider's embedding model, with langchain4j's default retries. */
  public static EmbeddingModel createEmbeddingModel(
      AiProvider provider, String modelName, IVariables variables) throws HopException {
    return createEmbeddingModel(
        provider, AiProviderSettings.of(provider, variables), modelName, variables, null);
  }

  private static EmbeddingModel createEmbeddingModel(
      AiProvider provider,
      AiProviderSettings settings,
      String modelName,
      IVariables variables,
      Integer maxRetries)
      throws HopException {

    // Resolution order: the transform's override, then the provider's EMBEDDING row. The
    // provider's plain modelName is the chat model and is deliberately not a fallback here.
    String model =
        Utils.isEmpty(modelName)
            ? variables.resolve(provider.resolveModelName(AiModelRole.EMBEDDING))
            : modelName;
    if (Utils.isEmpty(model)) {
      throw new HopException(
          "No embedding model is configured. Set one on this transform, or add an EMBEDDING model"
              + " to AI provider '"
              + provider.getName()
              + "'.");
    }

    return switch (settings.type()) {
      case "OLLAMA" -> ollamaModel(settings, model, maxRetries);
      case "OPEN_AI" -> openAiModel(settings, model, maxRetries);
      default ->
          throw new HopException(
              "Provider type '"
                  + settings.type()
                  + "' does not serve embedding models yet. Use an Ollama or OpenAI compatible"
                  + " provider.");
    };
  }

  private static EmbeddingModel ollamaModel(
      AiProviderSettings settings, String model, Integer maxRetries) {
    OllamaEmbeddingModel.OllamaEmbeddingModelBuilder builder =
        OllamaEmbeddingModel.builder().baseUrl(settings.baseUrl()).modelName(model);
    if (maxRetries != null) {
      builder.maxRetries(maxRetries);
    }
    if (settings.timeout() != null) {
      builder.timeout(settings.timeout());
    }
    return builder.build();
  }

  private static EmbeddingModel openAiModel(
      AiProviderSettings settings, String model, Integer maxRetries) {
    OpenAiEmbeddingModel.OpenAiEmbeddingModelBuilder builder =
        OpenAiEmbeddingModel.builder().modelName(model);
    if (maxRetries != null) {
      builder.maxRetries(maxRetries);
    }
    if (!Utils.isEmpty(settings.baseUrl())) {
      builder.baseUrl(settings.baseUrl());
    }
    if (!Utils.isEmpty(settings.apiKey())) {
      builder.apiKey(settings.apiKey());
    }
    if (settings.timeout() != null) {
      builder.timeout(settings.timeout());
    }
    return builder.build();
  }
}
