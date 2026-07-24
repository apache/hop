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

package org.apache.hop.marketplace.gui;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;

/** GUI entry points for the Hop plugin marketplace. */
@GuiPlugin
public class MarketplaceGuiPlugin {

  public static final Class<?> PKG = MarketplaceGuiPlugin.class;

  public static final String ID_MAIN_MENU_TOOLS_MARKETPLACE = "40200-menu-tools-marketplace";

  /** After Save As ({@code toolbar-10050-save-as}) on the main toolbar. */
  public static final String ID_MAIN_TOOLBAR_MARKETPLACE = "toolbar-10060-marketplace";

  private static MarketplaceGuiPlugin instance;

  public MarketplaceGuiPlugin() {
    // Instantiated by the GUI plugin system
  }

  public static MarketplaceGuiPlugin getInstance() {
    if (instance == null) {
      instance = new MarketplaceGuiPlugin();
    }
    return instance;
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_MARKETPLACE,
      label = "i18n::MarketplaceGuiPlugin.Menu.Marketplace.Text",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      image = "ui/images/plugin.svg",
      separator = true)
  @GuiToolbarElement(
      root = HopGui.ID_MAIN_TOOLBAR,
      id = ID_MAIN_TOOLBAR_MARKETPLACE,
      image = "ui/images/plugin.svg",
      toolTip = "i18n::MarketplaceGuiPlugin.Toolbar.Marketplace.Tooltip",
      separator = true)
  public void menuToolsMarketplace() {
    HopGui hopGui = HopGui.getInstance();
    try {
      new MarketplaceDialog(hopGui.getShell()).open();
    } catch (Exception e) {
      new ErrorDialog(hopGui.getShell(), "Marketplace", "Unable to open the marketplace dialog", e);
    }
  }
}
