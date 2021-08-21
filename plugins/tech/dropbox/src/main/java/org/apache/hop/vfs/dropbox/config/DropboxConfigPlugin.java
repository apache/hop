/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.vfs.dropbox.config;

import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.ui.core.dialog.EnterOptionsDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.widgets.Control;
import picocli.CommandLine;

@ConfigPlugin(
    id = "DropboxConfigPlugin",
    description = "Configuration options for Dropbox",
    category = ConfigPlugin.CATEGORY_CONFIG)
@GuiPlugin(
    description = "Dropbox" // Tab label in options dialog
    )
public class DropboxConfigPlugin implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final String WIDGET_ID_DROPBOX_ACCESS_TOKEN = "10000-dropbox-access-token";

  @GuiWidgetElement(
      id = WIDGET_ID_DROPBOX_ACCESS_TOKEN,
      parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "Access token")
  @CommandLine.Option(
      names = {"-dbxt", "--dropbox-access-token"},
      description = "The Dropbox access token to use for VFS")
  private String accessToken;

  /* Gets instance
   *
   * @return value of instance
   */
  public static DropboxConfigPlugin getInstance() {
    DropboxConfigPlugin instance = new DropboxConfigPlugin();

    DropboxConfig config = DropboxConfigSingleton.getConfig();
    instance.accessToken = config.getAccessToken();   

    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    DropboxConfig config = DropboxConfigSingleton.getConfig();
    try {
      boolean changed = false;

      if (accessToken != null) {
        config.setAccessToken(accessToken);
        log.logBasic("The Dropbox access token is set to '" + accessToken + "'");
        changed = true;
      }

      // Save to file if anything changed
      //
      if (changed) {
        DropboxConfigSingleton.saveConfig();
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling Dropbox configuration options", e);
    }
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {}

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {}

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {}

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    for (String widgetId : compositeWidgets.getWidgetsMap().keySet()) {
      Control control = compositeWidgets.getWidgetsMap().get(widgetId);
      switch (widgetId) {
        case WIDGET_ID_DROPBOX_ACCESS_TOKEN:
          accessToken = ((TextVar) control).getText();
          DropboxConfigSingleton.getConfig().setAccessToken(accessToken);
          break;       
      }
    }
    // Save the project...
    //
    try {
      DropboxConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(HopGui.getInstance().getShell(), "Error", "Error saving option", e);
    }
  }

  /**
   * Gets access token
   *
   * @return value of access token
   */
  public String getAccessToken() {
    return accessToken;
  }

  /** @param accessToken The access token to set */
  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }
}
