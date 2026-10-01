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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.langchain4j.model.ModelProvider;
import dev.langchain4j.model.catalog.ModelCatalog;
import dev.langchain4j.model.catalog.ModelDescription;
import dev.langchain4j.model.catalog.ModelType;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.providers.HuggingFaceProvider;
import org.apache.hop.ai.providers.OpenAiProvider;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class AiModelCatalogTest {

  @Test
  void parseOllamaTags() throws Exception {
    String json =
        """
        {"models":[{"name":"llama3.2:latest"},{"name":"mistral"},{"model":"phi4"}]}
        """;
    List<String> names = AiModelCatalog.parseOllamaTags(json);
    assertEquals(List.of("llama3.2:latest", "mistral", "phi4"), names);
  }

  @Test
  void parseOpenAiModels() throws Exception {
    String json =
        """
        {"data":[{"id":"gpt-4o-mini"},{"id":"gpt-4o"},{"name":"grok-4"}]}
        """;
    List<String> names = AiModelCatalog.parseOpenAiModels(json);
    assertEquals(List.of("gpt-4o", "gpt-4o-mini", "grok-4"), names);
  }

  @Test
  void huggingFaceHasNoCatalog() {
    AiProvider provider = new AiProvider();
    HuggingFaceProvider backend = new HuggingFaceProvider();
    backend.setPluginId("hugging-face");
    provider.setProvider(backend);
    provider.setApiKey("hf-token");
    HopException error =
        assertThrows(HopException.class, () -> AiModelCatalog.listModelNames(provider, null));
    assertTrue(error.getMessage().toLowerCase().contains("hugging face"));
  }

  @Test
  void stripOllamaV1Suffix() {
    assertEquals(
        "http://localhost:11434",
        AiModelCatalog.stripKnownSuffix("http://localhost:11434/v1/", "/v1", "/api"));
  }

  @Test
  void skipNonChatModelTypes() {
    assertFalse(AiModelCatalog.skipModelType(null));
    assertFalse(AiModelCatalog.skipModelType(ModelType.CHAT));
    assertFalse(AiModelCatalog.skipModelType(ModelType.OTHER));
    assertTrue(AiModelCatalog.skipModelType(ModelType.EMBEDDING));
    assertTrue(AiModelCatalog.skipModelType(ModelType.IMAGE_GENERATION));
  }

  @Test
  void namesFromCatalogSkipsEmbeddings() {
    ModelCatalog catalog =
        new ModelCatalog() {
          @Override
          public List<ModelDescription> listModels() {
            return List.of(
                ModelDescription.builder()
                    .name("gpt-4o")
                    .provider(ModelProvider.OPEN_AI)
                    .type(ModelType.CHAT)
                    .build(),
                ModelDescription.builder()
                    .name("text-embedding-3-small")
                    .provider(ModelProvider.OPEN_AI)
                    .type(ModelType.EMBEDDING)
                    .build(),
                ModelDescription.builder()
                    .name("custom")
                    .provider(ModelProvider.OPEN_AI)
                    .type(ModelType.OTHER)
                    .build());
          }

          @Override
          public ModelProvider provider() {
            return ModelProvider.OPEN_AI;
          }
        };
    assertEquals(List.of("custom", "gpt-4o"), AiModelCatalog.namesFromCatalog(catalog));
  }

  @Test
  void uniqueSortedCapsAndDedupes() {
    List<String> input = new ArrayList<>();
    for (int i = 0; i < AiModelCatalog.MAX_MODELS + 5; i++) {
      input.add("model-" + i);
      input.add("model-" + i);
    }
    List<String> names = AiModelCatalog.uniqueSorted(input);
    assertEquals(AiModelCatalog.MAX_MODELS, names.size());
    assertEquals("model-0", names.get(0));
  }

  @Test
  void parseEmptyCatalogs() throws Exception {
    assertTrue(AiModelCatalog.parseOllamaTags("{}").isEmpty());
    assertTrue(AiModelCatalog.parseOpenAiModels("{}").isEmpty());
  }

  @Test
  void openAiRequiresApiKey() {
    AiProvider provider = new AiProvider();
    OpenAiProvider backend = new OpenAiProvider();
    backend.setPluginId("openai");
    provider.setProvider(backend);
    HopException error =
        assertThrows(HopException.class, () -> AiModelCatalog.listModelNames(provider, null));
    assertTrue(error.getMessage().toLowerCase().contains("api key"));
  }

  @Test
  void missingProviderType() {
    HopException error =
        assertThrows(
            HopException.class, () -> AiModelCatalog.listModelNames(new AiProvider(), null));
    assertTrue(error.getMessage().toLowerCase().contains("provider type"));
  }
}
