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
package org.apache.hop.ui.hopgui.file.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigPluginOptionsTab;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import picocli.CommandLine;

/**
 * Options that run when a pipeline or workflow file is saved. Surfaced in the Configuration
 * perspective Plugins tab.
 */
@ConfigPlugin(
    id = "file-validation-config",
    description = "Validate referenced database connections when saving a file",
    category = ConfigPlugin.CATEGORY_CONFIG)
@GuiPlugin(description = "i18n::FileValidationConfigPlugin.Description")
public class FileValidationConfigPlugin
    implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  public static final String KEY_VALIDATE_DB_CONNECTIONS_ON_SAVE = "ValidateDbConnectionsOnSave";

  private static final String WIDGET_VALIDATE_DB_CONNECTIONS_ON_SAVE =
      "file-validation-validate-db-connections-on-save";

  @GuiWidgetElement(
      id = WIDGET_VALIDATE_DB_CONNECTIONS_ON_SAVE,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::FileValidationConfigPlugin.ValidateDbConnectionsOnSave.Label",
      toolTip = "i18n::FileValidationConfigPlugin.ValidateDbConnectionsOnSave.ToolTip")
  @CommandLine.Option(
      names = {"--validate-db-connections-on-save"},
      description =
          "Warn when saving a pipeline or workflow if a referenced database connection does not exist (default: true)")
  private Boolean validateDbConnectionsOnSave;

  public FileValidationConfigPlugin() {
    loadFromHopConfig();
  }

  public static FileValidationConfigPlugin getInstance() {
    return new FileValidationConfigPlugin();
  }

  private void loadFromHopConfig() {
    try {
      validateDbConnectionsOnSave =
          HopConfig.readOptionBoolean(KEY_VALIDATE_DB_CONNECTIONS_ON_SAVE, true);
    } catch (Exception e) {
      validateDbConnectionsOnSave = true;
    }
  }

  /**
   * JavaBeans getter used by {@code GuiCompositeWidgets}. Null (never persisted) is treated as
   * enabled, which is the default.
   */
  public Boolean getValidateDbConnectionsOnSave() {
    return isValidateDbConnectionsOnSave();
  }

  /** JavaBeans setter used by {@code GuiCompositeWidgets}. */
  public void setValidateDbConnectionsOnSave(Boolean validateDbConnectionsOnSave) {
    this.validateDbConnectionsOnSave = validateDbConnectionsOnSave;
  }

  public boolean isValidateDbConnectionsOnSave() {
    return validateDbConnectionsOnSave == null || validateDbConnectionsOnSave;
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    // Widgets are filled from this instance, loaded in the constructor.
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    // Nothing to do.
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    persistContents(compositeWidgets);
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(WIDGET_VALIDATE_DB_CONNECTIONS_ON_SAVE);
    if (control instanceof Button button) {
      validateDbConnectionsOnSave = button.getSelection();
    }
    saveToHopConfig();
  }

  public Map<String, Object> saveToHopConfig() {
    Map<String, Object> options = new HashMap<>();
    if (validateDbConnectionsOnSave != null) {
      options.put(KEY_VALIDATE_DB_CONNECTIONS_ON_SAVE, validateDbConnectionsOnSave);
      HopConfig.saveOptions(options);
    }
    return options;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    if (validateDbConnectionsOnSave == null) {
      return false;
    }
    saveToHopConfig();
    log.logBasic(
        "Validate database connections on save is now "
            + (isValidateDbConnectionsOnSave() ? "enabled" : "disabled"));
    return true;
  }
}
