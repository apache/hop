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
    AiProvider provider = AiProviderLoader.load(providerName, metadataProvider);
    String resolvedName =
        Utils.isEmpty(modelName)
            ? variables.resolve(provider.resolveModelName(AiModelRole.EMBEDDING))
            : variables.resolve(modelName);
    return new ResolvedEmbeddingModel(
        createEmbeddingModel(provider, resolvedName, variables), resolvedName);
  }

  /** An embedding model and the name of the model it talks to. */
  public record ResolvedEmbeddingModel(EmbeddingModel model, String modelName) {}

  public static EmbeddingModel createEmbeddingModel(
      AiProvider provider, String modelName, IVariables variables) throws HopException {
    // of() rejects a null or half configured provider, so there is one guard, not two.
    AiProviderSettings settings = AiProviderSettings.of(provider, variables);

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
      case "OLLAMA" -> ollamaModel(settings, model);
      case "OPEN_AI" -> openAiModel(settings, model);
      default ->
          throw new HopException(
              "Provider type '"
                  + settings.type()
                  + "' does not serve embedding models yet. Use an Ollama or OpenAI compatible"
                  + " provider.");
    };
  }

  private static EmbeddingModel ollamaModel(AiProviderSettings settings, String model) {
    OllamaEmbeddingModel.OllamaEmbeddingModelBuilder builder =
        OllamaEmbeddingModel.builder().baseUrl(settings.baseUrl()).modelName(model);
    if (settings.timeout() != null) {
      builder.timeout(settings.timeout());
    }
    return builder.build();
  }

  private static EmbeddingModel openAiModel(AiProviderSettings settings, String model) {
    OpenAiEmbeddingModel.OpenAiEmbeddingModelBuilder builder =
        OpenAiEmbeddingModel.builder().modelName(model);
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
