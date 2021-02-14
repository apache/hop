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

package org.apache.hop.ui.core.gui;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.KeyboardShortcut;
import org.apache.hop.core.gui.plugin.menu.GuiMenuItem;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** This class contains the widgets for Menu Bars */
public class GuiMenuWidgets extends BaseGuiWidgets {

  private Map<String, MenuItem> menuItemMap;
  private Map<String, KeyboardShortcut> shortcutMap;

  public GuiMenuWidgets() {
    super(UUID.randomUUID().toString());
    this.menuItemMap = new HashMap<>();
    this.shortcutMap = new HashMap<>();
  }

  public void createMenuWidgets(String root, Shell shell, Menu parent) {
    // Find the GUI Elements for the given class...
    //
    GuiRegistry registry = GuiRegistry.getInstance();

    // Loop over the GUI elements and create menus all the way down...
    // We used the same ID for root and top level menu
    //
    List<GuiMenuItem> guiMenuItems = registry.findChildGuiMenuItems(root, root);
    if (guiMenuItems.isEmpty()) {
      System.err.println("Create menu widgets: no GUI menu items found for root: " + root);
      return;
    }

    // Sort by ID to get a stable UI
    //
    Collections.sort(guiMenuItems);

    for (GuiMenuItem guiMenuItem : guiMenuItems) {
      addMenuWidgets(root, shell, parent, guiMenuItem);
    }
  }

  private void addMenuWidgets(String root, Shell shell, Menu parentMenu, GuiMenuItem guiMenuItem) {

    if (guiMenuItem.isIgnored()) {
      return;
    }

    MenuItem menuItem;

    // With children mean: drop-down menu item
    // Without children:
    //
    List<GuiMenuItem> children =
        GuiRegistry.getInstance().findChildGuiMenuItems(root, guiMenuItem.getId());

    if (children.isEmpty()) {

      if (guiMenuItem.isAddingSeparator()) {
        new MenuItem(parentMenu, SWT.SEPARATOR);
      }

      menuItem = new MenuItem(parentMenu, SWT.PUSH);
      menuItem.setText(guiMenuItem.getLabel());
      if (StringUtils.isNotEmpty(guiMenuItem.getImage())) {
        menuItem.setImage(
            GuiResource.getInstance()
                .getImage(
                    guiMenuItem.getImage(), ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
      }

      setMenuItemKeyboardShortcut(menuItem, guiMenuItem);
      if (StringUtils.isNotEmpty(guiMenuItem.getToolTip()) && !EnvironmentUtils.getInstance().isWeb()) {
        menuItem.setToolTipText(guiMenuItem.getToolTip());
      }

      // Call the method to which the GuiWidgetElement annotation belongs.
      //
      menuItem.addListener(
          SWT.Selection,
          e -> {
            try {

              Object singleton =
                  findGuiPluginInstance(
                      guiMenuItem.getClassLoader(), guiMenuItem.getListenerClassName());

              Method menuMethod = singleton.getClass().getMethod(guiMenuItem.getListenerMethod());
              if (menuMethod == null) {
                throw new HopException(
                    "Unable to find method "
                        + guiMenuItem.getListenerMethod()
                        + " in singleton "
                        + guiMenuItem.getListenerClassName());
              }
              menuMethod.invoke(singleton);
            } catch (Exception ex) {
              System.err.println(
                  "Unable to call method "
                      + guiMenuItem.getListenerMethod()
                      + " in singleton "
                      + guiMenuItem.getListenerClassName()
                      + " : "
                      + ex.getMessage());
              ex.printStackTrace(System.err);
            }
          });

      menuItemMap.put(guiMenuItem.getId(), menuItem);

    } else {
      // We have a bunch of children so we want to create a new drop-down menu in the parent menu
      //
      Menu menu = parentMenu;
      if (guiMenuItem.getId() != null) {
        menuItem = new MenuItem(parentMenu, SWT.CASCADE);
        menuItem.setText(Const.NVL(guiMenuItem.getLabel(), ""));
        setMenuItemKeyboardShortcut(menuItem, guiMenuItem);
        if (StringUtils.isNotEmpty(guiMenuItem.getToolTip()) && !EnvironmentUtils.getInstance().isWeb()) {
          menuItem.setToolTipText(guiMenuItem.getToolTip());
        }
        menu = new Menu(shell, SWT.DROP_DOWN);
        menuItem.setMenu(menu);
        menuItemMap.put(guiMenuItem.getId(), menuItem);
      }

      // Add the children to this menu...
      //

      // Sort the children as well.  It gets chaotic otherwise
      //
      Collections.sort(children);

      for (GuiMenuItem child : children) {
        addMenuWidgets(root, shell, menu, child);
      }
    }
  }

  private void setMenuItemKeyboardShortcut(MenuItem menuItem, GuiMenuItem guiMenuItem) {
    // See if there's a shortcut worth mentioning...
    //
    KeyboardShortcut shortcut =
        GuiRegistry.getInstance()
            .findKeyboardShortcut(
                guiMenuItem.getListenerClassName(), guiMenuItem.getListenerMethod(), Const.isOSX());
    if (shortcut != null) {
      appendShortCut(menuItem, shortcut);
      menuItem.setAccelerator(getAccelerator(shortcut));
      shortcutMap.put(guiMenuItem.getId(), shortcut);
    }
  }

  public static void appendShortCut(MenuItem menuItem, KeyboardShortcut shortcut) {
    menuItem.setText(menuItem.getText() + "\t" + getShortcutString(shortcut));
  }

  public static int getAccelerator(KeyboardShortcut shortcut) {
    int a = 0;
    a += shortcut.getKeyCode();
    if (shortcut.isControl()) {
      a += SWT.CONTROL;
    }
    if (shortcut.isShift()) {
      a += SWT.SHIFT;
    }
    if (shortcut.isAlt()) {
      a += SWT.ALT;
    }
    if (shortcut.isCommand()) {
      a += SWT.COMMAND;
    }
    return a;
  }

  public static String getShortcutString(KeyboardShortcut shortcut) {
    String s = shortcut.toString();
    if (StringUtils.isEmpty(s) || s.endsWith("+")) {
      // Unknown characters from the SWT library
      // We'll handle the special cases here.
      //
      int keyCode = shortcut.getKeyCode();
      if (keyCode == SWT.BS) {
        return s + "Backspace";
      }
      if (keyCode == SWT.ESC) {
        return s + "Esc";
      }
      if (keyCode == SWT.DEL) {
        return s + "Delete";
      }
      if (keyCode == SWT.ARROW_LEFT) {
        return s + "Left";
      }
      if (keyCode == SWT.ARROW_RIGHT) {
        return s + "Right";
      }
      if (keyCode == SWT.ARROW_UP) {
        return s + "Up";
      }
      if (keyCode == SWT.ARROW_DOWN) {
        return s + "Down";
      }
      if (keyCode == SWT.HOME) {
        return s + "Home";
      }
      if (keyCode == SWT.F1) {
        return s + "F1";
      }
      if (keyCode == SWT.F2) {
        return s + "F2";
      }
      if (keyCode == SWT.F3) {
        return s + "F3";
      }
      if (keyCode == SWT.F4) {
        return s + "F4";
      }
      if (keyCode == SWT.F5) {
        return s + "F5";
      }
      if (keyCode == SWT.F6) {
        return s + "F6";
      }
      if (keyCode == SWT.F7) {
        return s + "F7";
      }
      if (keyCode == SWT.F8) {
        return s + "F8";
      }
      if (keyCode == SWT.F9) {
        return s + "F9";
      }
      if (keyCode == SWT.F10) {
        return s + "F10";
      }
      if (keyCode == SWT.F11) {
        return s + "F11";
      }
      if (keyCode == SWT.F12) {
        return s + "F12";
      }
    }
    return s;
  }

  /**
   * Find the menu item with the given ID
   *
   * @param id The ID to look for
   * @return The menu item or null if nothing is found
   */
  public MenuItem findMenuItem(String id) {
    return menuItemMap.get(id);
  }

  public KeyboardShortcut findKeyboardShortcut(String id) {
    return shortcutMap.get(id);
  }

  /**
   * Find the menu item with the given ID. If we find it we enable or disable it.
   *
   * @param id The ID to look for
   * @param enabled true if the item needs to be enabled.
   * @return The menu item or null if nothing is found
   */
  public MenuItem enableMenuItem(String id, boolean enabled) {
    MenuItem menuItem = menuItemMap.get(id);
    if (menuItem == null || menuItem.isDisposed()) {
      return null;
    }
    menuItem.setEnabled(enabled);
    return menuItem;
  }

  /**
   * Find the menu item with the given ID. Check the capability in the given file type Enable or
   * disable accordingly.
   *
   * @param fileType
   * @param id The ID of the widget to look for
   * @param permission
   * @return The menu item or null if nothing is found
   */
  public MenuItem enableMenuItem(IHopFileType fileType, String id, String permission) {
    return enableMenuItem(fileType, id, permission, true);
  }

  /**
   * Find the menu item with the given ID. Check the capability in the given file type Enable or
   * disable accordingly.
   *
   * @param fileType
   * @param id The ID of the widget to look for
   * @param permission
   * @param active The state if the permission is available
   * @return The menu item or null if nothing is found
   */
  public MenuItem enableMenuItem(
      IHopFileType fileType, String id, String permission, boolean active) {
    MenuItem menuItem = menuItemMap.get(id);
    if (menuItem == null || menuItem.isDisposed()) {
      return null;
    }
    boolean hasCapability = fileType.hasCapability(permission);
    menuItem.setEnabled(hasCapability && active);
    return menuItem;
  }

  /**
   * Gets menuItemMap
   *
   * @return value of menuItemMap
   */
  public Map<String, MenuItem> getMenuItemMap() {
    return menuItemMap;
  }

  /** @param menuItemMap The menuItemMap to set */
  public void setMenuItemMap(Map<String, MenuItem> menuItemMap) {
    this.menuItemMap = menuItemMap;
  }
}
