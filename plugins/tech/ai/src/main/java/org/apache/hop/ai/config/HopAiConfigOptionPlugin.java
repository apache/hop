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

package org.apache.hop.ai.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigPluginOptionsTab;
import org.eclipse.swt.widgets.Control;
import picocli.CommandLine;

@Getter
@Setter
@ConfigPlugin(
    id = "HopAiConfigOptionPlugin",
    description = "Configuration options for Hop AI advisory",
    category = ConfigPlugin.CATEGORY_CONFIG,
    classLoaderGroup = "hop-ai")
@GuiPlugin(description = "i18n::HopAiConfig.Tab.Name", classLoaderGroup = "hop-ai")
public class HopAiConfigOptionPlugin implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final Class<?> PKG = HopAiConfigOptionPlugin.class;
  private static final String PARENT = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID;

  @GuiWidgetElement(
      id = "0100-ai-enabled",
      order = "0100",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::HopAiConfigOptionPlugin.AiEnabled.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.AiEnabled.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "AI Advisory")
  @CommandLine.Option(
      names = {"--hop-ai-enabled"},
      description = "Enable AI advisory in Hop GUI",
      negatable = true)
  private Boolean aiEnabled;

  @GuiWidgetElement(
      id = "0200-ai-default-provider",
      order = "0200",
      parentId = PARENT,
      type = GuiElementType.METADATA,
      metadata = AiProvider.class,
      label = "i18n::HopAiConfigOptionPlugin.DefaultProvider.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.DefaultProvider.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "AI Advisory")
  @CommandLine.Option(
      names = {"--hop-ai-default-provider"},
      description = "Default AI provider metadata name")
  private String defaultProviderName;

  @GuiWidgetElement(
      id = "0300-ai-allow-full-xml",
      order = "0300",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::HopAiConfigOptionPlugin.AllowFullXml.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.AllowFullXml.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "AI Advisory")
  @CommandLine.Option(
      names = {"--hop-ai-allow-full-xml"},
      description = "Allow advisors to send full pipeline or workflow XML",
      negatable = true)
  private Boolean allowSendFullXml;

  @GuiWidgetElement(
      id = "0400-ai-extra-context",
      order = "0400",
      parentId = PARENT,
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 6,
      variables = true,
      label = "i18n::HopAiConfigOptionPlugin.ExtraContext.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.ExtraContext.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "AI Advisory")
  private String extraContext;

  @GuiWidgetElement(
      id = "0500-ai-extra-context-files",
      order = "0500",
      parentId = PARENT,
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 4,
      variables = true,
      label = "i18n::HopAiConfigOptionPlugin.ExtraContextFiles.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.ExtraContextFiles.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "AI Advisory")
  private String extraContextFiles;

  public static HopAiConfigOptionPlugin getInstance() {
    HopAiConfigOptionPlugin instance = new HopAiConfigOptionPlugin();
    try {
      HopGui hopGui = HopGui.getInstance();
      if (hopGui != null) {
        HopAiLegacyConfigMigrator.migrate(hopGui.getMetadataProvider());
      }
    } catch (Throwable ignored) {
      // CLI / tests may have no GUI.
    }
    HopAiConfig config = HopAiConfigSingleton.getConfig();
    instance.aiEnabled = config.isAiEnabled();
    instance.defaultProviderName = config.getDefaultProviderName();
    instance.allowSendFullXml = config.isAllowSendFullXml();
    instance.extraContext = config.getExtraContext();
    instance.extraContextFiles = config.getExtraContextFiles();
    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    try {
      if (hasHopMetadataProvider != null && hasHopMetadataProvider.getMetadataProvider() != null) {
        HopAiLegacyConfigMigrator.migrate(hasHopMetadataProvider.getMetadataProvider());
      }
      HopAiConfig config = HopAiConfigSingleton.getConfig();
      boolean changed = false;
      if (aiEnabled != null && config.isAiEnabled() != aiEnabled) {
        config.setAiEnabled(aiEnabled);
        log.logBasic(aiEnabled ? "Enabled Hop AI advisory" : "Disabled Hop AI advisory");
        changed = true;
      }
      if (defaultProviderName != null
          && !defaultProviderName.equals(config.getDefaultProviderName())) {
        config.setDefaultProviderName(defaultProviderName);
        changed = true;
      }
      if (allowSendFullXml != null && config.isAllowSendFullXml() != allowSendFullXml) {
        config.setAllowSendFullXml(allowSendFullXml);
        changed = true;
      }
      if (changed) {
        HopAiConfigSingleton.saveConfig();
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling Hop AI configuration options", e);
    }
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    // Do nothing
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    // Do nothing
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    persistContents(compositeWidgets);
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    compositeWidgets.getWidgetsContents(this, PARENT);
    HopAiConfig config = HopAiConfigSingleton.getConfig();
    if (aiEnabled != null) {
      config.setAiEnabled(aiEnabled);
    }
    if (defaultProviderName != null) {
      config.setDefaultProviderName(defaultProviderName);
    }
    if (allowSendFullXml != null) {
      config.setAllowSendFullXml(allowSendFullXml);
    }
    if (extraContext != null) {
      config.setExtraContext(extraContext);
    }
    if (extraContextFiles != null) {
      config.setExtraContextFiles(extraContextFiles);
    }
    try {
      HopAiConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "HopAiConfigOptionPlugin.Save.Error.Title"),
          BaseMessages.getString(PKG, "HopAiConfigOptionPlugin.Save.Error.Message"),
          e);
    }
  }
}
