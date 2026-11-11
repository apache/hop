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

import org.apache.hop.ai.provider.IAiProvider;
import org.apache.hop.ai.providers.CustomOpenAiProvider;
import org.apache.hop.ai.providers.OllamaProvider;
import org.junit.jupiter.api.Test;

class AiEmbeddingFactoryTest {

  @Test
  void namesLangchain4jsEndpointForAnOpenAiCompatibleProviderWithoutABaseUrl() {
    // OpenAiEmbeddingModel talks to api.openai.com when it gets no base URL, so a failure there
    // should say so rather than "at ?".
    assertEquals(
        AiEmbeddingFactory.OPEN_AI_DEFAULT_BASE_URL,
        AiEmbeddingFactory.effectiveBaseUrl(settings("", new CustomOpenAiProvider())));
  }

  @Test
  void keepsAConfiguredBaseUrl() {
    assertEquals(
        "http://llm:8000/v1",
        AiEmbeddingFactory.effectiveBaseUrl(
            settings("http://llm:8000/v1", new CustomOpenAiProvider())));
    assertEquals(
        "http://localhost:11434",
        AiEmbeddingFactory.effectiveBaseUrl(
            settings("http://localhost:11434", new OllamaProvider())));
  }

  private static AiProviderSettings settings(String baseUrl, IAiProvider backend) {
    return new AiProviderSettings(baseUrl, "", null, null, null, null, null, backend);
  }
}
