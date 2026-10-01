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
 */

package org.apache.hop.setup.gui;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;

/** Tools menu entry for the Hop environment configurator. */
@GuiPlugin
public class SetupGuiPlugin {

  public static final Class<?> PKG = SetupGuiPlugin.class;

  public static final String ID_MAIN_MENU_TOOLS_SETUP = "40150-menu-tools-setup";

  private static SetupGuiPlugin instance;

  public SetupGuiPlugin() {
    // Instantiated by the GUI plugin system
  }

  public static SetupGuiPlugin getInstance() {
    if (instance == null) {
      instance = new SetupGuiPlugin();
    }
    return instance;
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_SETUP,
      label = "i18n::SetupGuiPlugin.Menu.Text",
      toolTip = "i18n::SetupGuiPlugin.Menu.Tooltip",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      image = "setup.svg",
      separator = true)
  public void menuToolsSetup() {
    HopGui hopGui = HopGui.getInstance();
    try {
      new SetupDialog(hopGui.getShell(), hopGui.getVariables()).open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "SetupDialog.Error.Header"),
          BaseMessages.getString(PKG, "SetupDialog.Error.Header"),
          e);
    }
  }
}
