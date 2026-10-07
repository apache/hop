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

import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * Reaches an {@link AiProvider} by name, which is how a transform refers to one.
 *
 * <p>Separate from {@link AiProviderSettings} because loading metadata is not a property of the
 * settings a provider carries, and both model factories need it.
 */
public final class AiProviderLoader {

  private AiProviderLoader() {}

  /**
   * @throws HopException when the provider cannot be read, or there is no provider by that name
   */
  public static AiProvider load(String providerName, IHopMetadataProvider metadataProvider)
      throws HopException {
    AiProvider provider;
    try {
      provider = metadataProvider.getSerializer(AiProvider.class).load(providerName);
    } catch (Exception e) {
      throw new HopException("Error loading AI provider '" + providerName + "'", e);
    }
    if (provider == null) {
      throw new HopException("AI provider not found: " + providerName);
    }
    return provider;
  }
}
