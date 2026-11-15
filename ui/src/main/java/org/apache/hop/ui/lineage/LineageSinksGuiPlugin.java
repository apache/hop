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

package org.apache.hop.ui.lineage;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.ui.hopgui.HopGui;

/** Tools menu entry that shows how the lineage sinks are doing. */
@GuiPlugin
public class LineageSinksGuiPlugin {

  public static final String ID_MAIN_MENU_TOOLS_LINEAGE_SINKS = "40170-menu-tools-lineage-sinks";

  private static LineageSinksGuiPlugin instance;

  public LineageSinksGuiPlugin() {
    // Instantiated by the GUI plugin system
  }

  public static LineageSinksGuiPlugin getInstance() {
    if (instance == null) {
      instance = new LineageSinksGuiPlugin();
    }
    return instance;
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_LINEAGE_SINKS,
      label = "i18n::LineageSinksGuiPlugin.Menu.Text",
      toolTip = "i18n::LineageSinksGuiPlugin.Menu.Tooltip",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID)
  public void menuToolsLineageSinks() {
    HopGui hopGui = HopGui.getInstance();
    new LineageSinksDialog(hopGui.getShell(), hopGui.getVariables(), LineageHub.getInstance())
        .open();
  }
}
