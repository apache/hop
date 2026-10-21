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

import java.time.Duration;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.metadata.AiThinking;
import org.apache.hop.ai.provider.IAiProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;

/**
 * The connection settings shared by every model a provider serves.
 *
 * <p>Endpoint, credentials and timeout mean the same thing whether a chat model, an embedding model
 * or a reranker is being built, so they are resolved once here rather than in each factory.
 *
 * @param baseUrl the endpoint, falling back to the provider type's default
 * @param apiKey the resolved key, empty when the provider type needs none
 * @param timeout the request timeout, or null to leave the client's own default
 * @param temperature the sampling temperature, or null when not set
 * @param contextSize the context window in tokens, or null when unknown
 * @param maxOutputTokens the answer length limit in tokens, or null for the provider's default
 * @param think whether a thinking model reasons before it answers, or null to leave it to the model
 * @param backend the provider type, which says how to talk to the endpoint
 */
public record AiProviderSettings(
    String baseUrl,
    String apiKey,
    Duration timeout,
    Double temperature,
    Integer contextSize,
    Integer maxOutputTokens,
    Boolean think,
    IAiProvider backend) {

  /**
   * Ollama's own default window (2048 or 4096 depending on the version) does not hold a typical
   * advisor prompt, and it truncates silently. 16k holds one with room for history and the answer.
   */
  public static final int DEFAULT_OLLAMA_CONTEXT_SIZE = 16_384;

  /** Anthropic requires a limit. Its old default here, 1024, cut answers and proposals short. */
  public static final int DEFAULT_ANTHROPIC_MAX_OUTPUT_TOKENS = 4_096;

  /** The {@code hopModelType} of the backend, never null, so it can be switched on directly. */
  public String type() {
    return backend.getHopModelType() == null ? "" : backend.getHopModelType();
  }

  public static AiProviderSettings of(AiProvider provider, IVariables variables)
      throws HopException {
    if (provider == null) {
      throw new AiUserException(
          BaseMessages.getString(AiProviderSettings.class, "AiChatFactory.NoProvider"));
    }
    if (!provider.hasProviderType()) {
      throw new AiUserException(AiChatFactory.missingTypeMessage(provider));
    }
    IAiProvider backend = provider.getProvider();
    String baseUrl = variables.resolve(provider.getBaseUrl());
    if (Utils.isEmpty(baseUrl)) {
      baseUrl = backend.getDefaultBaseUrl();
    }
    return new AiProviderSettings(
        baseUrl,
        variables.resolve(provider.getApiKey()),
        parseTimeout(variables.resolve(provider.getTimeoutSeconds())),
        parseDouble(variables.resolve(provider.getTemperature())),
        contextSize(provider, variables),
        maxOutputTokens(provider, variables),
        think(provider, variables),
        backend);
  }

  /** The configured context size, or the Ollama default, or null when nothing is known. */
  public static Integer contextSize(AiProvider provider, IVariables variables) {
    Integer configured = parsePositiveInt(resolve(variables, provider.getContextSize()));
    if (configured != null) {
      return configured;
    }
    return "OLLAMA".equals(provider.getHopModelType()) ? DEFAULT_OLLAMA_CONTEXT_SIZE : null;
  }

  /**
   * The window a question is checked against before it is sent: the context size, or when that is
   * not known (a hosted provider without the field set), a generous figure for the provider type.
   * It is only a budget for the check; it is not sent to the provider.
   */
  public static int contextBudget(AiProvider provider, IVariables variables) {
    Integer known = contextSize(provider, variables);
    if (known != null) {
      return known;
    }
    return "ANTHROPIC".equals(provider.getHopModelType())
        ? DEFAULT_ANTHROPIC_CONTEXT_BUDGET
        : DEFAULT_HOSTED_CONTEXT_BUDGET;
  }

  /** The context window of current Anthropic models. */
  public static final int DEFAULT_ANTHROPIC_CONTEXT_BUDGET = 200_000;

  /**
   * The context window of current hosted models of OpenAI, Mistral, Gemini and most OpenAI
   * compatible servers. Older or smaller models have less; set Context size for those.
   */
  public static final int DEFAULT_HOSTED_CONTEXT_BUDGET = 128_000;

  /** The configured answer limit, or the Anthropic default, or null for the provider's own. */
  public static Integer maxOutputTokens(AiProvider provider, IVariables variables) {
    Integer configured = parsePositiveInt(resolve(variables, provider.getMaxOutputTokens()));
    if (configured != null) {
      return configured;
    }
    return "ANTHROPIC".equals(provider.getHopModelType())
        ? DEFAULT_ANTHROPIC_MAX_OUTPUT_TOKENS
        : null;
  }

  /**
   * The Thinking setting as the model's {@code think} option: false for Off, true for On, and null
   * for Default or a value that is not a setting, so the model decides as it did before.
   */
  public static Boolean think(AiProvider provider, IVariables variables) {
    AiThinking thinking = AiThinking.lookup(resolve(variables, provider.getThinking()));
    return thinking == null ? null : thinking.think();
  }

  static Integer parsePositiveInt(String text) {
    if (Utils.isEmpty(text)) {
      return null;
    }
    try {
      int value = Integer.parseInt(text.trim());
      return value > 0 ? value : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String resolve(IVariables variables, String value) {
    if (value == null) {
      return "";
    }
    return variables != null ? variables.resolve(value) : value;
  }

  /**
   * Whole seconds, as the merged embedding factory has always read it. Anything else, including a
   * fractional value, leaves the timeout unset so the client's own default applies.
   */
  private static Duration parseTimeout(String seconds) {
    if (Utils.isEmpty(seconds)) {
      return null;
    }
    try {
      long value = Long.parseLong(seconds.trim());
      return value > 0 ? Duration.ofSeconds(value) : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Double parseDouble(String text) {
    if (Utils.isEmpty(text)) {
      return null;
    }
    try {
      return Double.valueOf(text.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
