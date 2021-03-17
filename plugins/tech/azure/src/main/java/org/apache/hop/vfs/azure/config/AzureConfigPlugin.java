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

package org.apache.hop.vfs.azure.config;

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
import org.apache.hop.vfs.azure.config.AzureConfigSingleton;
import org.eclipse.swt.widgets.Control;
import picocli.CommandLine;

@ConfigPlugin(
    id = "AzureConfigPlugin",
    description = "Configuration options for Azure",
    category = ConfigPlugin.CATEGORY_CONFIG)
@GuiPlugin(
    description = "Azure" // Tab label in options dialog
    )
public class AzureConfigPlugin implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final String WIDGET_ID_AZURE_ACCOUNT = "10000-azure-account";
  private static final String WIDGET_ID_AZURE_KEY = "10000-azure-key";

  @GuiWidgetElement(
      id = WIDGET_ID_AZURE_ACCOUNT,
      parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = true,
      label = "Your Azure account")
  @CommandLine.Option(
      names = {"-aza", "--azure-account"},
      description = "The account to use for the Azure VFS")
  private String account;

  @GuiWidgetElement(
    id = WIDGET_ID_AZURE_KEY,
    parentId = EnterOptionsDialog.GUI_WIDGETS_PARENT_ID,
    type = GuiElementType.TEXT,
    variables = true,
    label = "Your Azure key")
  @CommandLine.Option(
    names = {"-azk", "--azure-key"},
    description = "The key to use for the Azure VFS")
  private String key;

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static AzureConfigPlugin getInstance() {
    AzureConfigPlugin instance = new AzureConfigPlugin();

    AzureConfig config = AzureConfigSingleton.getConfig();
    instance.account = config.getAccount();
    instance.key = config.getKey();

    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    AzureConfig config = AzureConfigSingleton.getConfig();
    try {
      boolean changed = false;

      if ( account != null) {
        config.setAccount( account );
        log.logBasic(
            "The Azure account is set to '"
                + account
                + "'");
        changed = true;
      }

      if ( key != null) {
        config.setKey( key );
        log.logBasic(
          "The Azure key is set to '"
            + key
            + "'");
        changed = true;
      }

      // Save to file if anything changed
      //
      if (changed) {
        AzureConfigSingleton.saveConfig();
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling Azure configuration options", e);
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
        case WIDGET_ID_AZURE_ACCOUNT:
          account = ((TextVar) control).getText();
          AzureConfigSingleton.getConfig().setAccount( account );
          break;
        case WIDGET_ID_AZURE_KEY:
          key = ((TextVar) control).getText();
          AzureConfigSingleton.getConfig().setKey( key );
          break;
      }
    }
    // Save the project...
    //
    try {
      AzureConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(HopGui.getInstance().getShell(), "Error", "Error saving option", e);
    }
  }

  /**
   * Gets account
   *
   * @return value of account
   */
  public String getAccount() {
    return account;
  }

  /**
   * @param account The account to set
   */
  public void setAccount( String account ) {
    this.account = account;
  }

  /**
   * Gets key
   *
   * @return value of key
   */
  public String getKey() {
    return key;
  }

  /**
   * @param key The key to set
   */
  public void setKey( String key ) {
    this.key = key;
  }
}
