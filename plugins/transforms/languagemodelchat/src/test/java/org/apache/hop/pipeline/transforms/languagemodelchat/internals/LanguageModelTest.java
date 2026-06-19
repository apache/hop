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

package org.apache.hop.pipeline.transforms.languagemodelchat.internals;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class LanguageModelTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void openAiModel() {
    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.OPEN_AI.code());
    meta.setOpenAiModelName("gpt-test");
    LanguageModel lm = new LanguageModel(meta);
    assertEquals(ModelType.OPEN_AI, lm.getType());
    assertEquals("gpt-test", lm.getName());
  }

  @Test
  void huggingFaceModel() {
    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setModelType(ModelType.HUGGING_FACE.code());
    meta.setHuggingFaceModelId("org/llama");
    LanguageModel lm = new LanguageModel(meta);
    assertEquals(ModelType.HUGGING_FACE, lm.getType());
    assertEquals("org/llama", lm.getName());
  }

  @Test
  void equalsAndHashCode() {
    LanguageModelChatMeta m1 = new LanguageModelChatMeta();
    m1.setModelType(ModelType.OLLAMA.code());
    m1.setOllamaModelName("phi3");
    LanguageModelChatMeta m2 = new LanguageModelChatMeta();
    m2.setModelType(ModelType.OLLAMA.code());
    m2.setOllamaModelName("phi3");
    LanguageModel a = new LanguageModel(m1);
    LanguageModel b = new LanguageModel(m2);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void notEqualsWhenNameDiffers() {
    LanguageModelChatMeta m1 = new LanguageModelChatMeta();
    m1.setModelType(ModelType.MISTRAL.code());
    m1.setMistralModelName("a");
    LanguageModelChatMeta m2 = new LanguageModelChatMeta();
    m2.setModelType(ModelType.MISTRAL.code());
    m2.setMistralModelName("b");
    assertNotEquals(new LanguageModel(m1), new LanguageModel(m2));
  }
}
