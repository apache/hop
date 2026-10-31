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

package org.apache.hop.ui.hopgui.file.pipeline.debug.config;

import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.pipeline.debug.DebugSessionStore;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigPluginOptionsTab;
import org.eclipse.swt.widgets.Control;
import picocli.CommandLine;

@ConfigPlugin(
    id = "DebugSessionsConfigPlugin",
    description = "i18n::DebugSessionsConfig.ConfigPlugin.Description",
    category = ConfigPlugin.CATEGORY_CONFIG,
    configKey = DebugSessionsConfig.HOP_CONFIG_KEY,
    configClass = DebugSessionsConfig.class)
@GuiPlugin(description = "i18n::DebugSessionsConfig.GuiPlugin.Description")
public class DebugSessionsConfigPlugin
    implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final String WIDGET_ID_FOLDER = "10000-debug-sessions-folder";

  @GuiWidgetElement(
      id = WIDGET_ID_FOLDER,
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FOLDER,
      variables = true,
      label = "i18n::DebugSessionsConfig.Folder.Label",
      toolTip = "i18n::DebugSessionsConfig.Folder.Tooltip")
  @CommandLine.Option(
      names = {"--debug-sessions-folder"},
      description = "Folder where Hop GUI stores pipeline debug sessions")
  private String folder;

  public static DebugSessionsConfigPlugin getInstance() {
    DebugSessionsConfigPlugin instance = new DebugSessionsConfigPlugin();
    instance.folder = DebugSessionsConfigSingleton.getConfig().getFolder();
    return instance;
  }

  /** Configured folder, or the default under the home directory when the field is empty. */
  public static String resolveRoot(IVariables variables) {
    String configured = DebugSessionsConfigSingleton.getConfig().getFolder();
    if (configured != null && !configured.isBlank()) {
      String resolved = variables == null ? configured : variables.resolve(configured);
      if (resolved != null && !resolved.isBlank()) {
        return resolved;
      }
    }
    return DebugSessionStore.defaultRoot();
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    if (folder == null) {
      return false;
    }
    DebugSessionsConfigSingleton.getConfig().setFolder(folder);
    DebugSessionsConfigSingleton.saveConfig();
    log.logBasic("Debug sessions folder set to '" + folder + "'");
    return true;
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    // Nothing to enable or hide.
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    // Nothing to enable or hide.
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    persistContents(compositeWidgets);
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(WIDGET_ID_FOLDER);
    if (control instanceof TextVar text) {
      folder = text.getText();
      DebugSessionsConfigSingleton.getConfig().setFolder(folder);
    }
    try {
      DebugSessionsConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), "Error", "Error saving the debug sessions folder", e);
    }
  }
}
