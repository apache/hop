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
import org.apache.hop.ai.provider.IAiProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

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
 * @param backend the provider type, which says how to talk to the endpoint
 */
public record AiProviderSettings(
    String baseUrl, String apiKey, Duration timeout, Double temperature, IAiProvider backend) {

  /** The {@code hopModelType} of the backend, never null, so it can be switched on directly. */
  public String type() {
    return backend.getHopModelType() == null ? "" : backend.getHopModelType();
  }

  public static AiProviderSettings of(AiProvider provider, IVariables variables)
      throws HopException {
    if (provider == null) {
      throw new HopException("An AI provider is required");
    }
    IAiProvider backend = provider.getProvider();
    if (backend == null) {
      throw new HopException("AI provider type is not set on '" + provider.getName() + "'");
    }
    String baseUrl = variables.resolve(provider.getBaseUrl());
    if (Utils.isEmpty(baseUrl)) {
      baseUrl = backend.getDefaultBaseUrl();
    }
    return new AiProviderSettings(
        baseUrl,
        variables.resolve(provider.getApiKey()),
        parseTimeout(variables.resolve(provider.getTimeoutSeconds())),
        parseDouble(variables.resolve(provider.getTemperature())),
        backend);
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
