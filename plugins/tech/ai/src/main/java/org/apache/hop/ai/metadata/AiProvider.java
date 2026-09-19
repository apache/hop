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

package org.apache.hop.ai.metadata;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ai.engine.AiChatFactory;
import org.apache.hop.ai.provider.IAiProvider;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/** Named AI provider (model + credentials) stored as project metadata. */
@Getter
@Setter
@GuiPlugin(classLoaderGroup = "hop-ai")
@SuppressWarnings("java:S2160")
@HopMetadata(
    key = "ai-provider",
    name = "i18n::AiProvider.name",
    description = "i18n::AiProvider.description",
    image = "ai-provider.svg",
    category = HopMetadataCategory.CONNECTIONS,
    documentationUrl = "/metadata-types/ai-provider.html",
    hopMetadataPropertyType = HopMetadataPropertyType.AI_PROVIDER,
    classLoaderGroup = "hop-ai")
public class AiProvider extends HopMetadataBase implements IHopMetadata {

  public static final String GUI_WIDGETS_PARENT_ID = "AiProviderEditor.Widgets";
  public static final String WIDGET_BASE_URL = "0100-base-url";
  public static final String WIDGET_API_KEY = "0200-api-key";
  public static final String WIDGET_TIMEOUT = "0300-timeout";
  public static final String WIDGET_MODEL_NAME = "0400-model-name";
  public static final String WIDGET_TEMPERATURE = "0500-temperature";

  @HopMetadataProperty(key = "provider")
  private IAiProvider provider;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_BASE_URL,
      order = "0100",
      type = GuiElementType.TEXT,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "Connection",
      groupOrder = "10",
      label = "i18n::AiProvider.BaseUrl.Label",
      toolTip = "i18n::AiProvider.BaseUrl.Tooltip")
  private String baseUrl = "";

  @HopMetadataProperty(password = true)
  @GuiWidgetElement(
      id = WIDGET_API_KEY,
      order = "0200",
      type = GuiElementType.TEXT,
      password = true,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "Connection",
      groupOrder = "10",
      label = "i18n::AiProvider.ApiKey.Label",
      toolTip = "i18n::AiProvider.ApiKey.Tooltip")
  private String apiKey = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_TIMEOUT,
      order = "0300",
      type = GuiElementType.TEXT,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "Connection",
      groupOrder = "10",
      label = "i18n::AiProvider.Timeout.Label",
      toolTip = "i18n::AiProvider.Timeout.Tooltip")
  private String timeoutSeconds = "60";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_MODEL_NAME,
      order = "0400",
      type = GuiElementType.COMBO,
      comboValuesMethod = "getModelNameChoices",
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "Model",
      groupOrder = "20",
      label = "i18n::AiProvider.ModelName.Label",
      toolTip = "i18n::AiProvider.ModelName.Tooltip")
  private String modelName = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_TEMPERATURE,
      order = "0500",
      type = GuiElementType.TEXT,
      parentId = GUI_WIDGETS_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "Model",
      groupOrder = "20",
      label = "i18n::AiProvider.Temperature.Label",
      toolTip = "i18n::AiProvider.Temperature.Tooltip")
  private String temperature = "0.3";

  /**
   * Models this provider serves, one entry per {@link AiModelRole}. A transform resolves the role
   * it needs, so one provider can back a chat transform, an embedding transform and a reranker at
   * once.
   *
   * <p>Empty on a provider created before roles existed, in which case {@link AiModelRole#CHAT}
   * falls back to {@link #modelName} and nothing changes.
   */
  @HopMetadataProperty(key = "models", injectionGroupKey = "MODELS")
  private List<AiProviderModel> models = new ArrayList<>();

  public AiProvider() {}

  public AiProvider(AiProvider other) {
    super(other.name);
    if (other.provider != null) {
      this.provider = other.provider.clone();
    }
    this.baseUrl = other.baseUrl;
    this.apiKey = other.apiKey;
    this.timeoutSeconds = other.timeoutSeconds;
    this.modelName = other.modelName;
    this.temperature = other.temperature;
    for (AiProviderModel model : other.models) {
      this.models.add(new AiProviderModel(model));
    }
  }

  /**
   * The model name to use for a role: the entry for that role when there is one, otherwise {@link
   * #modelName} for {@link AiModelRole#CHAT}.
   *
   * <p>{@code modelName} is the chat model, so it is deliberately not a fallback for the other
   * roles: embedding a text with a chat model fails at the provider with a confusing error, and an
   * empty result here produces a clear one instead.
   *
   * @param role the role the caller needs a model for
   * @return the model name, or an empty string when none is configured for that role
   */
  public String resolveModelName(AiModelRole role) {
    for (AiProviderModel model : models) {
      if (model != null && model.getRole() == role && !Utils.isEmpty(model.getModelName())) {
        return model.getModelName();
      }
    }
    return role == AiModelRole.CHAT ? Const.NVL(modelName, "") : "";
  }

  public String getPluginId() {
    return provider != null ? provider.getPluginId() : null;
  }

  public String getPluginName() {
    return provider != null ? provider.getPluginName() : null;
  }

  public String getHopModelType() {
    return provider != null ? provider.getHopModelType() : "OPEN_AI";
  }

  /**
   * Replace the backend plugin. Empty URL/model fields pick up the new plugin defaults; filled
   * fields are kept.
   */
  public void setProviderType(String pluginNameOrId) throws HopException {
    IAiProvider loaded = AiProviderPlugins.load(pluginNameOrId);
    if (provider != null) {
      if (Utils.isEmpty(baseUrl) || baseUrl.equals(provider.getDefaultBaseUrl())) {
        baseUrl = loaded.getDefaultBaseUrl();
      }
      if (Utils.isEmpty(modelName) || modelName.equals(provider.getDefaultModelName())) {
        modelName = loaded.getDefaultModelName();
      }
    } else {
      if (Utils.isEmpty(baseUrl)) {
        baseUrl = loaded.getDefaultBaseUrl();
      }
      if (Utils.isEmpty(modelName)) {
        modelName = loaded.getDefaultModelName();
      }
    }
    this.provider = loaded;
  }

  public void applyProviderDefaults() {
    if (provider == null) {
      return;
    }
    if (Utils.isEmpty(baseUrl)) {
      baseUrl = provider.getDefaultBaseUrl();
    }
    if (Utils.isEmpty(modelName)) {
      modelName = provider.getDefaultModelName();
    }
  }

  public String test(IVariables variables) throws HopException {
    return AiChatFactory.healthCheck(this, variables);
  }

  /**
   * Initial combo items only (current value and plugin default). Live ids are loaded with Refresh
   * models in the editor.
   */
  public List<String> getModelNameChoices(ILogChannel log, IHopMetadataProvider metadataProvider) {
    List<String> items = new ArrayList<>();
    if (!Utils.isEmpty(modelName)) {
      items.add(modelName);
    }
    String fallback = provider != null ? provider.getDefaultModelName() : "";
    if (!Utils.isEmpty(fallback) && items.stream().noneMatch(fallback::equals)) {
      items.add(fallback);
    }
    return items;
  }
}
