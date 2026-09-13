/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ai.provider;

import org.apache.hop.ai.AiAuthKind;
import org.apache.hop.metadata.api.HopMetadataObject;

/**
 * A pluggable AI backend (OpenAI-compatible, Anthropic, Ollama, …). Implementations are discovered
 * via {@link AiProviderPlugin} the same way {@code IDatabase} plugins are discovered.
 *
 * <p>Keep this interface free of langchain4j and SWT. Named credentials and model settings live on
 * the {@code AiProvider} metadata object in the tech plugin; this type only identifies the backend
 * and its defaults.
 */
@HopMetadataObject(objectFactory = AiProviderObjectFactory.class)
public interface IAiProvider extends Cloneable {

  /**
   * Parent id for {@code @GuiWidgetElement} fields on an implementation. The metadata editor
   * creates widgets for the selected provider under this id.
   */
  String GUI_PLUGIN_ELEMENT_PARENT_ID = "AiProvider-PluginSpecific-Options";

  String getPluginId();

  void setPluginId(String pluginId);

  String getPluginName();

  void setPluginName(String pluginName);

  AiAuthKind getAuthKind();

  /** Default API base URL, or empty when the backend has no URL (or uses a built-in default). */
  String getDefaultBaseUrl();

  String getDefaultModelName();

  /**
   * Language Model Chat {@code ModelType} code this backend maps to ({@code OPEN_AI}, {@code
   * ANTHROPIC}, {@code OLLAMA}, {@code MISTRAL}, {@code HUGGING_FACE}).
   */
  String getHopModelType();

  /** Whether a non-empty API key is required before a call can be made. */
  boolean requiresApiKey();

  IAiProvider clone();
}
