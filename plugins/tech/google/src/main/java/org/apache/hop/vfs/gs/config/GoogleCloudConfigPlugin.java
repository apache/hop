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

package org.apache.hop.vfs.gs.config;

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
    id = "GoogleCloudStorageConfigPlugin",
    description = "Configuration options for Google Cloud",
    category = ConfigPlugin.CATEGORY_CONFIG)
@GuiPlugin(
    description = "Google Cloud" // Tab label in options dialog
    )
public class GoogleCloudConfigPlugin implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final String WIDGET_ID_GOOGLE_CLOUD_SERVICE_ACCOUNT_KEY_FILE =
      "10000-google-cloud-service-account-key-file";

  @GuiWidgetElement(
      id = WIDGET_ID_GOOGLE_CLOUD_SERVICE_ACCOUNT_KEY_FILE,
      parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "Path to a Google Cloud service account JSON key file",
      toolTip =
          "Go to the Google Cloud console to create a key file for the service account you want to use")
  @CommandLine.Option(
      names = {"-gck", "--google-cloud-service-account-key-file"},
      description = "Configure the path to a Google Cloud service account JSON key file")
  private String serviceAccountKeyFile;


  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static GoogleCloudConfigPlugin getInstance() {
    GoogleCloudConfigPlugin instance = new GoogleCloudConfigPlugin();

    GoogleCloudConfig config = GoogleCloudConfigSingleton.getConfig();
    instance.serviceAccountKeyFile = config.getServiceAccountKeyFile();

    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    GoogleCloudConfig config = GoogleCloudConfigSingleton.getConfig();
    try {
      boolean changed = false;

      if (serviceAccountKeyFile != null) {
        config.setServiceAccountKeyFile(serviceAccountKeyFile);
        log.logBasic("The Google Cloud service account JSON jey file is set to '" + serviceAccountKeyFile + "'");
        changed = true;
      }

      // Save to file if anything changed
      //
      if (changed) {
        GoogleCloudConfigSingleton.saveConfig();
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling Google Cloud configuration options", e);
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
        case WIDGET_ID_GOOGLE_CLOUD_SERVICE_ACCOUNT_KEY_FILE:
          serviceAccountKeyFile = ((TextVar) control).getText();
          GoogleCloudConfigSingleton.getConfig().setServiceAccountKeyFile(serviceAccountKeyFile);
          break;
      }
    }
    // Save the project...
    //
    try {
      GoogleCloudConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(HopGui.getInstance().getShell(), "Error", "Error saving option", e);
    }
  }

  /**
   * Gets serviceAccountKeyFile
   *
   * @return value of serviceAccountKeyFile
   */
  public String getServiceAccountKeyFile() {
    return serviceAccountKeyFile;
  }

  /**
   * @param serviceAccountKeyFile The serviceAccountKeyFile to set
   */
  public void setServiceAccountKeyFile( String serviceAccountKeyFile ) {
    this.serviceAccountKeyFile = serviceAccountKeyFile;
  }
}
