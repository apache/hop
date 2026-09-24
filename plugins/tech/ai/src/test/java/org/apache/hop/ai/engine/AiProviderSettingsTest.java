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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.providers.OllamaProvider;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class AiProviderSettingsTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void fallsBackToTheProviderTypesDefaultEndpoint() throws Exception {
    AiProviderSettings settings = AiProviderSettings.of(provider(p -> p.setBaseUrl("")), vars());

    assertEquals("http://localhost:11434", settings.baseUrl());
  }

  @Test
  void anExplicitEndpointWins() throws Exception {
    AiProviderSettings settings =
        AiProviderSettings.of(provider(p -> p.setBaseUrl("http://ollama:11434")), vars());

    assertEquals("http://ollama:11434", settings.baseUrl());
  }

  @Test
  void resolvesVariablesInEveryField() throws Exception {
    IVariables variables = vars();
    variables.setVariable("AI_URL", "http://from-variable:11434");
    variables.setVariable("AI_KEY", "secret");
    variables.setVariable("AI_TIMEOUT", "30");

    AiProviderSettings settings =
        AiProviderSettings.of(
            provider(
                p -> {
                  p.setBaseUrl("${AI_URL}");
                  p.setApiKey("${AI_KEY}");
                  p.setTimeoutSeconds("${AI_TIMEOUT}");
                }),
            variables);

    assertEquals("http://from-variable:11434", settings.baseUrl());
    assertEquals("secret", settings.apiKey());
    assertEquals(Duration.ofSeconds(30), settings.timeout());
  }

  @Test
  void aTimeoutIsWholeSeconds() throws Exception {
    assertEquals(
        Duration.ofSeconds(60),
        AiProviderSettings.of(provider(p -> p.setTimeoutSeconds("60")), vars()).timeout());
  }

  @Test
  void anUnreadableTimeoutLeavesTheClientDefault() throws Exception {
    // Including a fractional value: this has always meant "unset" rather than a rounded timeout,
    // and a shared helper must not quietly change that for the transforms already using it.
    for (String bad : new String[] {"", "abc", "60.7", "0", "-5"}) {
      assertNull(
          AiProviderSettings.of(provider(p -> p.setTimeoutSeconds(bad)), vars()).timeout(),
          "timeout '" + bad + "' should leave the client default");
    }
  }

  @Test
  void anUnreadableTemperatureIsSimplyUnset() throws Exception {
    assertNull(
        AiProviderSettings.of(provider(p -> p.setTemperature("warm")), vars()).temperature());
  }

  @Test
  void theTypeIsNeverNullSoItCanBeSwitchedOn() throws Exception {
    AiProviderSettings settings = AiProviderSettings.of(provider(p -> {}), vars());

    assertEquals("OLLAMA", settings.type());
  }

  @Test
  void rejectsANullProvider() {
    HopException e = assertThrows(HopException.class, () -> AiProviderSettings.of(null, vars()));

    assertTrue(e.getMessage().contains("required"), e.getMessage());
  }

  @Test
  void rejectsAProviderWithNoType() {
    AiProvider provider = new AiProvider();
    provider.setName("half-configured");

    HopException e =
        assertThrows(HopException.class, () -> AiProviderSettings.of(provider, vars()));

    assertTrue(e.getMessage().contains("half-configured"), e.getMessage());
  }

  private static IVariables vars() {
    return new Variables();
  }

  private static AiProvider provider(java.util.function.Consumer<AiProvider> tweak) {
    AiProvider provider = new AiProvider();
    provider.setName("ollama-test");
    OllamaProvider backend = new OllamaProvider();
    backend.setPluginId("ollama");
    backend.setPluginName("Ollama");
    provider.setProvider(backend);
    provider.setTimeoutSeconds("");
    provider.setTemperature("");
    tweak.accept(provider);
    return provider;
  }
}
