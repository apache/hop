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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPluginType;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.KeyboardShortcut;
import org.apache.hop.core.gui.plugin.menu.GuiMenuItem;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;

/** This class contains the widgets for Menu Bars */
public class GuiMenuWidgets extends BaseGuiWidgets {

  private Map<String, MenuItem> menuItemMap;
  private Map<String, KeyboardShortcut> shortcutMap;
  private Map<String, Boolean> menuEnabledMap;

  public GuiMenuWidgets() {
    super(UUID.randomUUID().toString());
    this.menuItemMap = new HashMap<>();
    this.shortcutMap = new HashMap<>();
    this.menuEnabledMap = new HashMap<>();
  }

  /** Pre-create shortcut plugin instances so their shortcuts work before menu is used. */
  public void ensureShortcutPluginInstancesRegistered() {
    GuiRegistry guiRegistry = GuiRegistry.getInstance();
    PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    for (String className : guiRegistry.getShortCutsMap().keySet()) {
      try {
        IPlugin plugin =
            pluginRegistry.getPlugins(GuiPluginType.class).stream()
                .filter(p -> p.getClassMap().values().contains(className))
                .findFirst()
                .orElse(null);
        if (plugin == null) {
          continue;
        }
        findGuiPluginInstance(pluginRegistry.getClassLoader(plugin), className, instanceId);
      } catch (Exception e) {
        LogChannel.UI.logDebug(
            "Could not pre-register shortcut plugin instance for "
                + className
                + ": "
                + e.getMessage());
      }
    }
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
                    guiMenuItem.getImage(),
                    guiMenuItem.getClassLoader(),
                    ConstUi.SMALL_ICON_SIZE,
                    ConstUi.SMALL_ICON_SIZE));
      }

      setMenuItemKeyboardShortcut(menuItem, guiMenuItem);
      if (StringUtils.isNotEmpty(guiMenuItem.getToolTip())
          && !EnvironmentUtils.getInstance().isWeb()) {
        menuItem.setToolTipText(guiMenuItem.getToolTip());
      }

      // Call the method to which the GuiWidgetElement annotation belongs.
      //
      menuItem.addListener(
          SWT.Selection,
          e -> {
            try {
              executeMenuItem(guiMenuItem, instanceId);
            } catch (Exception ex) {
              LogChannel.UI.logError(
                  "Unable to call method "
                      + guiMenuItem.getListenerMethod()
                      + " in singleton "
                      + guiMenuItem.getListenerClassName()
                      + " : "
                      + ex.getMessage(),
                  e);
            }
          });

      menuItemMap.put(guiMenuItem.getId(), menuItem);
      menuEnabledMap.put(guiMenuItem.getId(), true);

    } else {
      // We have a bunch of children, so we want to create a new drop-down menu in the parent menu
      //
      Menu menu = parentMenu;
      if (guiMenuItem.getId() != null) {
        menuItem = new MenuItem(parentMenu, SWT.CASCADE);
        menuItem.setText(Const.NVL(guiMenuItem.getLabel(), ""));
        setMenuItemKeyboardShortcut(menuItem, guiMenuItem);
        if (StringUtils.isNotEmpty(guiMenuItem.getToolTip())
            && !EnvironmentUtils.getInstance().isWeb()) {
          menuItem.setToolTipText(guiMenuItem.getToolTip());
        }
        menu = new Menu(shell, SWT.DROP_DOWN);
        menuItem.setMenu(menu);
        menuItemMap.put(guiMenuItem.getId(), menuItem);
        menuEnabledMap.put(guiMenuItem.getId(), true);
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

  public static void executeMenuItem(GuiMenuItem guiMenuItem, String instanceId) throws Exception {
    Object parentObject =
        findGuiPluginInstance(
            guiMenuItem.getClassLoader(), guiMenuItem.getListenerClassName(), instanceId);
    Method menuMethod = parentObject.getClass().getMethod(guiMenuItem.getListenerMethod());
    menuMethod.invoke(parentObject);
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
      // Do not set menu accelerators; keyboard shortcuts are handled only by HopGuiKeyHandler
      // so the correct context (focus) is used. Menu still shows shortcut text for discoverability.
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
    return ShortcutDisplayUtil.getShortcutDisplayString(shortcut);
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
    if (menuItem != null && !menuItem.isDisposed()) {
      menuItem.setEnabled(enabled);
    }
    menuEnabledMap.put(id, enabled);
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
    return enableMenuItem(fileType, null, id, permission, true);
  }

  public MenuItem enableMenuItem(
      IHopFileType fileType, IHopFileTypeHandler handler, String id, String permission) {
    return enableMenuItem(fileType, handler, id, permission, true);
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
    return enableMenuItem(fileType, null, id, permission, active);
  }

  /**
   * Enable or disable menu item based on capability. When handler is non-null, uses handler's
   * hasCapability (so handlers can disable e.g. Save for binary raw view); otherwise uses file
   * type.
   */
  public MenuItem enableMenuItem(
      IHopFileType fileType,
      IHopFileTypeHandler handler,
      String id,
      String permission,
      boolean active) {
    MenuItem menuItem = menuItemMap.get(id);
    boolean hasCapability =
        handler != null ? handler.hasCapability(permission) : fileType.hasCapability(permission);
    boolean enable = hasCapability && active;
    if (menuItem != null && enable != menuItem.isEnabled()) {
      menuItem.setEnabled(enable);
    }
    menuEnabledMap.put(id, enable);
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

  /**
   * @param menuItemMap The menuItemMap to set
   */
  public void setMenuItemMap(Map<String, MenuItem> menuItemMap) {
    this.menuItemMap = menuItemMap;
  }

  /**
   * Gets shortcutMap
   *
   * @return value of shortcutMap
   */
  public Map<String, KeyboardShortcut> getShortcutMap() {
    return shortcutMap;
  }

  /**
   * Sets shortcutMap
   *
   * @param shortcutMap value of shortcutMap
   */
  public void setShortcutMap(Map<String, KeyboardShortcut> shortcutMap) {
    this.shortcutMap = shortcutMap;
  }

  /**
   * Gets menuEnabledMap
   *
   * @return value of menuEnabledMap
   */
  public Map<String, Boolean> getMenuEnabledMap() {
    return menuEnabledMap;
  }

  /**
   * Sets menuEnabledMap
   *
   * @param menuEnabledMap value of menuEnabledMap
   */
  public void setMenuEnabledMap(Map<String, Boolean> menuEnabledMap) {
    this.menuEnabledMap = menuEnabledMap;
  }
}
