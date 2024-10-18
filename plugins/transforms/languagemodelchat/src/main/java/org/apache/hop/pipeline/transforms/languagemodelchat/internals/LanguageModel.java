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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;

public class LanguageModel {

  private final ModelType type;
  private final String name;

  public LanguageModel(LanguageModelChatMeta meta) {
    this.type = ModelType.valueOf(meta.getModelType());
    switch (this.type) {
      case OPEN_AI -> name = meta.getOpenAiModelName();
      case ANTHROPIC -> name = meta.getAnthropicModelName();
      case OLLAMA -> name = meta.getOllamaModelName();
      case MISTRAL -> name = meta.getMistralModelName();
      case HUGGING_FACE -> name = meta.getHuggingFaceModelId();
      default -> throw new IllegalArgumentException("Invalid model type");
    }
  }

  public ModelType getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (!(o instanceof LanguageModel)) return false;

    LanguageModel model = (LanguageModel) o;

    return new EqualsBuilder().append(type, model.type).append(name, model.name).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(type).append(name).toHashCode();
  }
}
