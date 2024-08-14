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
 *
 */

package org.apache.hop.ui.hopgui.welcome;

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

@ConfigPlugin(id = "WelcomeDialogOption", description = "Enable or disable the welcome dialog")
@GuiPlugin(description = "i18n::WelcomeDialog.Description")
public class WelcomeDialogOptions implements IConfigOptions, IGuiPluginCompositeWidgetsListener {
  protected static final Class<?> PKG = WelcomeDialogOptions.class;

  @CommandLine.Option(
      names = {"-wd", "--welcome-dialog-disabled"},
      description = "Disable the welcome dialog at startup of the Hop GUI")
  private boolean welcomeDialogDisabled;

  @CommandLine.Option(
      names = {"-we", "--welcome-dialog-enabled"},
      description = "Show the welcome dialog at startup of the Hop GUI")
  private boolean welcomeDialogEnabled;

  @GuiWidgetElement(
      id = "NoWelcomeDialog",
      parentId = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::WelcomeDialog.Show.Label")
  private boolean welcomeDialogShowAtStartup;

  public WelcomeDialogOptions() {
    welcomeDialogShowAtStartup =
        HopConfig.readOptionBoolean(WelcomeDialog.HOP_CONFIG_NO_SHOW_OPTION, false);
  }

  private static WelcomeDialogOptions instance;

  public static WelcomeDialogOptions getInstance() {
    if (instance == null) {
      instance = new WelcomeDialogOptions();
    }
    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    try {
      if (welcomeDialogDisabled || welcomeDialogEnabled) {
        boolean disabled = welcomeDialogDisabled && !welcomeDialogEnabled;
        HopConfig.getInstance().saveOption(WelcomeDialog.HOP_CONFIG_NO_SHOW_OPTION, disabled);
        log.logBasic("The welcome dialog is now " + (disabled ? "disabled" : "enabled"));
        log.logBasic(
            "Note: you can also set variable "
                + WelcomeDialog.VARIABLE_HOP_NO_WELCOME_DIALOG
                + " to prevent the welcome dialog of showing itself at the start of Hop GUI.");
        return true;
      } else {
        return false;
      }
    } catch (Exception e) {
      throw new HopException("Error handling welcome dialog option", e);
    }
  }

  /**
   * Gets welcomeDialogDisabled
   *
   * @return value of welcomeDialogDisabled
   */
  public boolean isWelcomeDialogDisabled() {
    return welcomeDialogDisabled;
  }

  /**
   * Sets welcomeDialogDisabled
   *
   * @param welcomeDialogDisabled value of welcomeDialogDisabled
   */
  public void setWelcomeDialogDisabled(boolean welcomeDialogDisabled) {
    this.welcomeDialogDisabled = welcomeDialogDisabled;
  }

  /**
   * Gets welcomeDialogEnabled
   *
   * @return value of welcomeDialogEnabled
   */
  public boolean isWelcomeDialogEnabled() {
    return welcomeDialogEnabled;
  }

  /**
   * Sets welcomeDialogEnabled
   *
   * @param welcomeDialogEnabled value of welcomeDialogEnabled
   */
  public void setWelcomeDialogEnabled(boolean welcomeDialogEnabled) {
    this.welcomeDialogEnabled = welcomeDialogEnabled;
  }

  /**
   * Gets welcomeDialogShowAtStartup
   *
   * @return value of welcomeDialogShowAtStartup
   */
  public boolean isWelcomeDialogShowAtStartup() {
    return welcomeDialogShowAtStartup;
  }

  /**
   * Sets welcomeDialogShowAtStartup
   *
   * @param welcomeDialogShowAtStartup value of welcomeDialogShowAtStartup
   */
  public void setWelcomeDialogShowAtStartup(boolean welcomeDialogShowAtStartup) {
    this.welcomeDialogShowAtStartup = welcomeDialogShowAtStartup;
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
    this.welcomeDialogShowAtStartup = ((Button) changedWidget).getSelection();
    persistContents(compositeWidgets);
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    HopConfig.getInstance()
        .saveOption(WelcomeDialog.HOP_CONFIG_NO_SHOW_OPTION, welcomeDialogShowAtStartup);
  }
}
