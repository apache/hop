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

package org.apache.hop.ui.hopgui.context.menu;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.gui.plugin.menu.GuiMenuItem;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;

public class MenuContextHandler implements IGuiContextHandler {

  private static final Class<?> PKG = MenuContextHandler.class;

  public static final String CONTEXT_ID = "HopGuiMenuContext";

  private final String rootMenuId;
  private final GuiMenuWidgets widgets;

  public MenuContextHandler(String rootMenuId, GuiMenuWidgets widgets) {
    this.rootMenuId = rootMenuId;
    this.widgets = widgets;
  }

  @Override
  public String getContextId() {
    return CONTEXT_ID;
  }

  @Override
  public List<GuiAction> getSupportedActions() {

    List<GuiAction> actions = new ArrayList<>();

    GuiRegistry registry = GuiRegistry.getInstance();
    Map<String, Map<String, GuiMenuItem>> guiMenuMap = registry.getGuiMenuMap();
    Map<String, GuiMenuItem> menuMap = guiMenuMap.get(rootMenuId);

    List<GuiMenuItem> items = new ArrayList<>();

    for (String key : menuMap.keySet()) {
      GuiMenuItem item = menuMap.get(key);
      // See if this item is enabled
      Boolean enabled = widgets.getMenuEnabledMap().get(item.getId());
      // Enabled by default
      if (enabled != null && !enabled) {
        continue;
      }

      String parentId = item.getParentId();
      if (parentId != null) {
        GuiMenuItem parentMenuItem = registry.findGuiMenuItem(rootMenuId, parentId);
        if (parentMenuItem != null) {
          items.add(item);
        }
      }
    }

    Collections.sort(items);

    for (GuiMenuItem item : items) {
      String parentId = item.getParentId();
      if (parentId != null) {
        GuiMenuItem parentMenuItem = registry.findGuiMenuItem(rootMenuId, parentId);
        if (parentMenuItem != null) {
          // Build a new action item using the menu item.
          //
          GuiAction action =
              new GuiAction(
                  item.getId(),
                  GuiActionType.Custom,
                  item.getLabel().replace("&", ""),
                  item.getToolTip(),
                  Const.NVL(item.getImage(), "ui/images/logo_bw.svg"),
                  (shiftClicked, controlClicked, parameters) -> {
                    // Execute the method behind the menu item
                    //
                    try {
                      GuiMenuWidgets.executeMenuItem(item, widgets.getInstanceId());
                    } catch (Exception e) {
                      new ErrorDialog(
                          HopGui.getInstance().getShell(),
                          "Error",
                          "There was an error executing menu item " + item.getId(),
                          e);
                    }
                  });
          action.setClassLoader(item.getClassLoader());
          action.setCategory(parentMenuItem.getLabel().replace("&", ""));
          action.setCategoryOrder(parentMenuItem.getId());
          actions.add(action);
        }
      }
    }

    return actions;
  }

  /**
   * Gets rootMenuId
   *
   * @return value of rootMenuId
   */
  public String getRootMenuId() {
    return rootMenuId;
  }

  /**
   * Gets widgets
   *
   * @return value of widgets
   */
  public GuiMenuWidgets getWidgets() {
    return widgets;
  }
}
