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
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarItem;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.TextSizeUtilFacade;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CLabel;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** This class contains the widgets for the GUI elements of a GUI Plugin */
public class GuiToolbarWidgets extends BaseGuiWidgets {

  private Map<String, GuiToolbarItem> guiToolBarMap;
  private Map<String, Control> widgetsMap;
  private Map<String, ToolItem> toolItemMap;

  public GuiToolbarWidgets() {
    super(UUID.randomUUID().toString());
    guiToolBarMap = new HashMap<>();
    widgetsMap = new HashMap<>();
    toolItemMap = new HashMap<>();
  }

  public void createToolbarWidgets(Composite parent, String root) {

    // Find the GUI Elements for the given toolbar root...
    //
    List<GuiToolbarItem> toolbarItems = GuiRegistry.getInstance().findGuiToolbarItems(root);
    if (toolbarItems.isEmpty()) {
      System.err.println("Create widgets: no GUI toolbar items found for root: " + root);
      return;
    }

    Collections.sort(toolbarItems);

    // Loop over the toolbar items, create and remember the widgets...
    //
    for (GuiToolbarItem toolbarItem : toolbarItems) {
      addToolbarWidgets(parent, toolbarItem);
    }

    // Force re-layout
    //
    parent.layout(true, true);

    // Clean up when the parent is disposed
    //
    addDeRegisterGuiPluginObjectListener(parent);
  }

  private void addToolbarWidgets(Composite parent, GuiToolbarItem toolbarItem) {

    if (toolbarItem.isIgnored()) {
      return;
    }

    // We might need it later...
    //
    guiToolBarMap.put(toolbarItem.getId(), toolbarItem);

    if (!(parent instanceof ToolBar)) {
      throw new RuntimeException(
          "We can only add toolbar items to a toolbar, not class " + parent.getClass().getName());
    }
    ToolBar toolBar = (ToolBar) parent;

    PropsUi props = PropsUi.getInstance();

    // We want to add a separator if the annotation asked for it
    // We also want to add a separator in case the toolbar element type isn't a button
    //
    if (toolbarItem.isAddingSeparator() || toolbarItem.getType() != GuiToolbarElementType.BUTTON) {
      new ToolItem(toolBar, SWT.SEPARATOR);
    }

    // Add a label in front of the item
    //
    if (toolbarItem.getType() != GuiToolbarElementType.LABEL
        && toolbarItem.getType() != GuiToolbarElementType.CHECKBOX
        && StringUtils.isNotEmpty(toolbarItem.getLabel())) {
      ToolItem labelSeparator = new ToolItem(toolBar, SWT.SEPARATOR);
      CLabel label =
          new CLabel(parent, SWT.CENTER | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT));
      label.setText(Const.NVL(toolbarItem.getLabel(), ""));
      label.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
      label.setBackground(toolBar.getBackground());
      label.pack();
      labelSeparator.setWidth(label.getSize().x);
      labelSeparator.setControl(label);
    }

    // Add the GUI element
    //
    switch (toolbarItem.getType()) {
      case LABEL:
        ToolItem labelSeparator = new ToolItem(toolBar, SWT.SEPARATOR);

        CLabel label =
            new CLabel(parent, SWT.CENTER | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT));
        label.setText(Const.NVL(toolbarItem.getLabel(), ""));
        label.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
        label.setBackground(toolBar.getBackground());
        label.pack();
        labelSeparator.setWidth(label.getSize().x);
        labelSeparator.setControl(label);
        toolItemMap.put(toolbarItem.getId(), labelSeparator);
        widgetsMap.put(toolbarItem.getId(), label);
        Listener listener =
            getListener(
                toolbarItem.getClassLoader(),
                toolbarItem.getListenerClass(),
                toolbarItem.getListenerMethod());
        label.addListener(SWT.MouseUp, listener);
        break;

      case BUTTON:
        ToolItem item = new ToolItem(toolBar, SWT.NONE);
        if (StringUtils.isNotEmpty(toolbarItem.getImage())) {
          item.setImage(
              GuiResource.getInstance()
                  .getImage(
                      toolbarItem.getImage(),
                      toolbarItem.getClassLoader(),
                      ConstUi.SMALL_ICON_SIZE,
                      ConstUi.SMALL_ICON_SIZE));
        }
        if (StringUtils.isNotEmpty(toolbarItem.getDisabledImage())) {
          item.setDisabledImage(
              GuiResource.getInstance()
                  .getImage(
                      toolbarItem.getDisabledImage(),
                      toolbarItem.getClassLoader(),
                      ConstUi.SMALL_ICON_SIZE,
                      ConstUi.SMALL_ICON_SIZE));
        }
        if (StringUtils.isNotEmpty(toolbarItem.getToolTip())) {
          item.setToolTipText(toolbarItem.getToolTip());
        }
        listener =
            getListener(
                toolbarItem.getClassLoader(),
                toolbarItem.getListenerClass(),
                toolbarItem.getListenerMethod());
        item.addListener(SWT.Selection, listener);
        toolItemMap.put(toolbarItem.getId(), item);
        break;

      case COMBO:
        ToolItem comboSeparator = new ToolItem(toolBar, SWT.SEPARATOR);
        Combo combo =
            new Combo(parent, SWT.SINGLE | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT));
        combo.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
        combo.setItems(getComboItems(toolbarItem));
        combo.pack();
        comboSeparator.setWidth(
            calculateComboWidth(combo)
                + toolbarItem.getExtraWidth()); // extra room for widget decorations
        comboSeparator.setControl(combo);
        listener =
            getListener(
                toolbarItem.getClassLoader(),
                toolbarItem.getListenerClass(),
                toolbarItem.getListenerMethod());
        combo.addListener(SWT.Selection, listener);
        toolItemMap.put(toolbarItem.getId(), comboSeparator);
        widgetsMap.put(toolbarItem.getId(), combo);
        break;

      case CHECKBOX:
        ToolItem checkboxSeparator = new ToolItem(toolBar, SWT.SEPARATOR);
        Button checkbox =
            new Button(parent, SWT.CHECK | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT));
        checkbox.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
        checkbox.setText(Const.NVL(toolbarItem.getLabel(), ""));
        checkbox.setBackground(toolBar.getBackground());
        checkbox.pack();
        checkboxSeparator.setWidth(
            checkbox.getSize().x
                + toolbarItem.getExtraWidth()); // extra room for widget decorations
        checkboxSeparator.setControl(checkbox);
        listener =
            getListener(
                toolbarItem.getClassLoader(),
                toolbarItem.getListenerClass(),
                toolbarItem.getListenerMethod());
        checkbox.addListener(SWT.Selection, listener);
        toolItemMap.put(toolbarItem.getId(), checkboxSeparator);
        widgetsMap.put(toolbarItem.getId(), checkbox);
        break;

      default:
        break;
    }
  }

  private int calculateComboWidth(Combo combo) {
    int maxWidth = combo.getSize().x;
    for (String item : combo.getItems()) {
      int width = TextSizeUtilFacade.textExtent(item).x;
      if (width > maxWidth) {
        maxWidth = width;
      }
    }
    return maxWidth;
  }

  public void enableToolbarItem(String id, boolean enabled) {
    ToolItem toolItem = toolItemMap.get(id);
    if (toolItem == null || toolItem.isDisposed()) {
      return;
    }
    if (enabled != toolItem.isEnabled()) {
      toolItem.setEnabled(enabled);
    }
  }

  /**
   * Find the toolbar item with the given ID. Check the capability in the given file type Enable or
   * disable accordingly.
   *
   * @param fileType
   * @param id The ID of the widget to look for
   * @param permission
   * @return The toolbar item or null if nothing is found
   */
  public ToolItem enableToolbarItem(IHopFileType fileType, String id, String permission) {
    return enableToolbarItem(fileType, id, permission, true);
  }

  /**
   * Find the toolbar item with the given ID. Check the capability in the given file type Enable or
   * disable accordingly.
   *
   * @param fileType
   * @param id The ID of the widget to look for
   * @param permission
   * @param active The state if the permission is available
   * @return The toolbar item or null if nothing is found
   */
  public ToolItem enableToolbarItem(
      IHopFileType fileType, String id, String permission, boolean active) {
    ToolItem item = findToolItem(id);
    if (item == null || item.isDisposed()) {
      return null;
    }
    boolean hasCapability = fileType.hasCapability(permission);
    item.setEnabled(hasCapability && active);
    return item;
  }

  public ToolItem findToolItem(String id) {
    return toolItemMap.get(id);
  }

  public void refreshComboItemList(String id) {
    GuiToolbarItem item = guiToolBarMap.get(id);
    if (item != null) {
      Control control = widgetsMap.get(id);
      if (control != null) {
        if (control instanceof Combo) {
          Combo combo = (Combo) control;
          combo.setItems(getComboItems(item));
        } else {
          System.err.println("toolbar item with id '" + id + "' : widget not of instance Combo");
        }
      } else {
        System.err.println(
            "toolbar item with id '" + id + "' : control not found when refreshing combo");
      }
    } else {
      System.err.println("toolbar item with id '" + id + "' : not found when refreshing combo");
    }
  }

  public void selectComboItem(String id, String string) {
    GuiToolbarItem item = guiToolBarMap.get(id);
    if (item != null) {
      Control control = widgetsMap.get(id);
      if (control != null) {
        if (control instanceof Combo) {
          Combo combo = (Combo) control;
          combo.setText(Const.NVL(string, ""));
          int index = Const.indexOfString(string, combo.getItems());
          if (index >= 0) {
            combo.select(index);
          }
        }
      }
    }
  }

  /**
   * Gets widgetsMap
   *
   * @return value of widgetsMap
   */
  public Map<String, Control> getWidgetsMap() {
    return widgetsMap;
  }

  /** @param widgetsMap The widgetsMap to set */
  public void setWidgetsMap(Map<String, Control> widgetsMap) {
    this.widgetsMap = widgetsMap;
  }

  /**
   * Gets toolItemMap
   *
   * @return value of toolItemMap
   */
  public Map<String, ToolItem> getToolItemMap() {
    return toolItemMap;
  }

  /** @param toolItemMap The toolItemMap to set */
  public void setToolItemMap(Map<String, ToolItem> toolItemMap) {
    this.toolItemMap = toolItemMap;
  }

  /**
   * Gets guiToolBarMap
   *
   * @return value of guiToolBarMap
   */
  public Map<String, GuiToolbarItem> getGuiToolBarMap() {
    return guiToolBarMap;
  }

  /** @param guiToolBarMap The guiToolBarMap to set */
  public void setGuiToolBarMap(Map<String, GuiToolbarItem> guiToolBarMap) {
    this.guiToolBarMap = guiToolBarMap;
  }
}
