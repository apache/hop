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

package org.apache.hop.vfs.googledrive.config;

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
    id = "GoogleDriveConfigPlugin",
    description = "Configuration options for the Google Drive VFS plugin",
    category = ConfigPlugin.CATEGORY_CONFIG)
@GuiPlugin(
    description = "Google Drive VFS" // Tab label in options dialog
    )
public class GoogleDriveConfigPlugin implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final String WIDGET_ID_GOOGLE_DRIVE_VFS_CREDENTIALS_PATH =
      "10000-google-drive-vfs-credentials-path";
  private static final String WIDGET_ID_GOOGLE_DRIVE_VFS_TOKENS_FOLDER =
      "10100-google-drive-vfs-tokens-folder";

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_DRIVE_VFS_CREDENTIALS_PATH,
      parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "Path to a Google Drive credentials JSON file",
      toolTip =
          "Please note that this is a global configuration option and while you can specify variables that these will be not include project specific settings.")
  @CommandLine.Option(
      names = {"-gdc", "--google-drive-credentials-file"},
      description = "Configure the path to a Google Drive credentials JSON file")
  private String credentialsFile;

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_DRIVE_VFS_TOKENS_FOLDER,
      parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "Path to a tokens folder for credential caching",
      toolTip = "Pick a safe folder to store these tokens")
  @CommandLine.Option(
      names = {"-gdt", "--google-drive-tokens-folder"},
      description = "Configure the path to a Google Drive tokens folder")
  private String tokensFolder;

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static GoogleDriveConfigPlugin getInstance() {
    GoogleDriveConfigPlugin instance = new GoogleDriveConfigPlugin();

    GoogleDriveConfig config = GoogleDriveConfigSingleton.getConfig();
    instance.credentialsFile = config.getCredentialsFile();
    instance.tokensFolder = config.getTokensFolder();

    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    GoogleDriveConfig config = GoogleDriveConfigSingleton.getConfig();
    try {
      boolean changed = false;

      if (credentialsFile != null) {
        config.setCredentialsFile(credentialsFile);
        log.logBasic("The Google Drive credentials file is set to '" + credentialsFile + "'");
        changed = true;
      }
      if (tokensFolder != null) {
        config.setTokensFolder(tokensFolder);
        log.logBasic("The Google Drive tokens folder is set to '" + tokensFolder + "'");
        changed = true;
      }

      // Save to file if anything changed
      //
      if (changed) {
        GoogleDriveConfigSingleton.saveConfig();
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling Google Drive plugin configuration options", e);
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
        case WIDGET_ID_GOOGLE_DRIVE_VFS_CREDENTIALS_PATH:
          credentialsFile = ((TextVar) control).getText();
          GoogleDriveConfigSingleton.getConfig().setCredentialsFile(credentialsFile);
          break;
        case WIDGET_ID_GOOGLE_DRIVE_VFS_TOKENS_FOLDER:
          tokensFolder = ((TextVar) control).getText();
          GoogleDriveConfigSingleton.getConfig().setTokensFolder(tokensFolder);
          break;
      }
    }
    // Save the project...
    //
    try {
      GoogleDriveConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(HopGui.getInstance().getShell(), "Error", "Error saving option", e);
    }
  }

  /**
   * Gets credentialsFile
   *
   * @return value of credentialsFile
   */
  public String getCredentialsFile() {
    return credentialsFile;
  }

  /** @param credentialsFile The credentialsFile to set */
  public void setCredentialsFile(String credentialsFile) {
    this.credentialsFile = credentialsFile;
  }

  /**
   * Gets tokensFolder
   *
   * @return value of tokensFolder
   */
  public String getTokensFolder() {
    return tokensFolder;
  }

  /** @param tokensFolder The tokensFolder to set */
  public void setTokensFolder(String tokensFolder) {
    this.tokensFolder = tokensFolder;
  }
}
