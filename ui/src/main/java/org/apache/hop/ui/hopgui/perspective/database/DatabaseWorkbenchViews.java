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

package org.apache.hop.ui.hopgui.perspective.database;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.terminal.HopGuiBottomDock;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

/**
 * Opens a {@link DatabaseWorkbench} as a floating window or as a tab in the bottom dock, the same
 * way Search uses {@code SearchEverywhereDialog} and {@code HopGuiSearchResultsPanel}.
 */
@GuiPlugin
public class DatabaseWorkbenchViews {

  public static final Class<?> PKG = DatabasePerspective.class;

  public static final String DOCK_TOOL_ID = "database-workbench";

  public static final String ID_MAIN_MENU_TOOLS_DATABASE_WINDOW =
      "40025-menu-tools-database-window";

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_DATABASE_WINDOW,
      label = "i18n::DatabasePerspective.Menu.Tools.Window",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      image = "ui/images/database.svg")
  public void menuToolsDatabaseWindow() {
    HopGui hopGui;
    try {
      hopGui = HopGui.peekInstance();
    } catch (Throwable e) {
      return;
    }
    if (hopGui == null) {
      return;
    }
    DatabaseWorkbenchDialog.open(hopGui);
  }

  /** Open or focus the floating Database window. */
  public static void openDialog(HopGui hopGui) {
    DatabaseWorkbenchDialog.open(hopGui);
  }

  /** Open or focus the Database tab in the bottom dock. */
  public static void openDock(HopGui hopGui) {
    if (hopGui == null) {
      return;
    }
    HopGuiBottomDock dock = hopGui.getTerminalPanel();
    if (dock == null || dock.isDisposed()) {
      return;
    }
    String title = BaseMessages.getString(PKG, "DatabasePerspective.Name");
    dock.focusOrOpenToolTab(
        DOCK_TOOL_ID,
        title,
        GuiResource.getInstance().getImageDatabase(),
        true,
        container -> createDockedWorkbench(container, hopGui));
  }

  private static Control createDockedWorkbench(Composite container, HopGui hopGui) {
    HopGuiDatabaseWorkbenchHost host =
        new HopGuiDatabaseWorkbenchHost(
            hopGui, () -> !container.isDisposed(), () -> openDock(hopGui));
    DatabaseWorkbench workbench = new DatabaseWorkbench(container, host);
    workbench.setLayoutData(new FormDataBuilder().fullSize().result());
    return workbench;
  }

  /** True when this dock tab is already open. */
  public static boolean isDockOpen(HopGui hopGui) {
    if (hopGui == null) {
      return false;
    }
    HopGuiBottomDock dock = hopGui.getTerminalPanel();
    if (dock == null || dock.isDisposed()) {
      return false;
    }
    CTabItem item = dock.findToolTab(DOCK_TOOL_ID);
    return item != null && !item.isDisposed();
  }
}
