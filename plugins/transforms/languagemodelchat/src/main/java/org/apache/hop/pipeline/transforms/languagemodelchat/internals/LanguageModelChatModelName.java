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

public enum LanguageModelChatModelName {

  // https://platform.openai.com/docs/models/continuous-model-upgrades
  OPENAI_GPT_4O("gpt-4o"),
  OPENAI_GPT_4O_MINI("gpt-4o-mini"),
  OPENAI_GPT_O1_MINI("o1-mini"),
  OPENAI_GPT_O1_PREVIEW("o1-preview"),
  OPENAI_GPT_4_TURBO("gpt-4-turbo"),

  // https://docs.mistral.ai/getting-started/models/
  OPEN_MISTRAL_7B("open-mistral-7b"),
  OPEN_MIXTRAL_8X7B("open-mixtral-8x7b"),
  MISTRAL_SMALL_LATEST("mistral-small-latest"),
  MISTRAL_MEDIUM_LATEST("mistral-medium-latest"),
  MISTRAL_LARGE_LATEST("mistral-large-latest"),

  // https://ollama.com/library
  OLLAMA_LLAMA3_8B("llama3"),
  OLLAMA_LLAMA3_70B("llama3:70b"),
  OLLAMA_PHI3_3_8B("phi3"),
  OLLAMA_PHI3_14B("phi3:medium"),

  // https://ui.endpoints.huggingface.co/catalog
  HUGGING_FACE_LLAMA3_70B_INSTRUCT("meta-llama/Meta-Llama-3-70B-Instruct"),
  HUGGING_FACE_MISTRAL_7B_INSTRUCT("mistralai/Mistral-7B-Instruct-v0.3"),

  // https://docs.anthropic.com/en/docs/models-overview
  ANTHROPIC_CLAUDE_3_OPUS_20240229("claude-3-opus-20240229"),
  ANTHROPIC_CLAUDE_3_SONNET_20240229("claude-3-sonnet-20240229"),
  ANTHROPIC_CLAUDE_3_HAIKU_20240307("claude-3-haiku-20240307");

  private final String stringValue;

  LanguageModelChatModelName(String stringValue) {
    this.stringValue = stringValue;
  }

  @Override
  public String toString() {
    return stringValue;
  }
}
