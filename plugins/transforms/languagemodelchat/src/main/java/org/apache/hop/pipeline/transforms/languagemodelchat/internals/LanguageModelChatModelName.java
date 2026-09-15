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

  // https://platform.openai.com/docs/models
  OPENAI_GPT_6_ASTRA("gpt-6-astra"),
  OPENAI_GPT_5_6_TERRA("gpt-5.6-terra"),
  OPENAI_GPT_5_6_LUNA("gpt-5.6-luna"),
  OPENAI_GPT_5_6_SOL("gpt-5.6-sol"),

  // https://docs.mistral.ai/getting-started/models/
  MISTRAL_LARGE_LATEST("mistral-large-latest"),
  MISTRAL_MEDIUM_LATEST("mistral-medium-latest"),
  MISTRAL_SMALL_LATEST("mistral-small-latest"),
  MAGISTRAL_MEDIUM_2509("magistral-medium-2509"),

  // https://ollama.com/library
  OLLAMA_LLAMA3_3("llama3.3"),
  OLLAMA_QWEN3("qwen3"),
  OLLAMA_PHI4("phi4"),
  OLLAMA_GEMMA3("gemma3"),
  OLLAMA_DEEPSEEK_R1("deepseek-r1"),

  // https://huggingface.co/models
  HUGGING_FACE_QWEN3_8B("Qwen/Qwen3-8B"),
  HUGGING_FACE_LLAMA3_3_70B_INSTRUCT("meta-llama/Llama-3.3-70B-Instruct"),
  HUGGING_FACE_MISTRAL_7B_INSTRUCT("mistralai/Mistral-7B-Instruct-v0.3"),
  HUGGING_FACE_GPT_OSS_20B("openai/gpt-oss-20b"),

  // https://docs.claude.com/en/docs/about-claude/models/overview
  ANTHROPIC_CLAUDE_OPUS_5("claude-opus-5"),
  ANTHROPIC_CLAUDE_SONNET_5("claude-sonnet-5"),
  ANTHROPIC_CLAUDE_HAIKU_4_5("claude-haiku-4-5");

  private final String stringValue;

  LanguageModelChatModelName(String stringValue) {
    this.stringValue = stringValue;
  }

  @Override
  public String toString() {
    return stringValue;
  }
}
