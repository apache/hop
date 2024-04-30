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

package org.apache.hop.ui.core.gui;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.KeyboardShortcut;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarItem;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarItemFilter;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.widget.svg.SvgLabelFacade;
import org.apache.hop.ui.core.widget.svg.SvgLabelListener;
import org.apache.hop.ui.hopgui.TextSizeUtilFacade;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CLabel;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

/** This class contains the widgets for the GUI elements of a GUI Plugin */
public class GuiToolbarWidgets extends BaseGuiWidgets {

  private Map<String, GuiToolbarItem> guiToolBarMap;
  private Map<String, Control> widgetsMap;
  private Map<String, ToolItem> toolItemMap;
  private Color itemBackgroundColor;

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
      LogChannel.UI.logError("Create widgets: no GUI toolbar items found for root: " + root);
      return;
    }

    Collections.sort(toolbarItems);

    // Loop over the toolbar items, create and remember the widgets...
    //
    for (GuiToolbarItem toolbarItem : toolbarItems) {
      boolean add = lookupToolbarItemFilter(toolbarItem, root);
      if (add) {
        addToolbarWidgets(parent, toolbarItem);
      }
    }

    // Force re-layout
    //
    parent.layout(true, true);

    // Clean up when the parent is disposed
    //
    addDeRegisterGuiPluginObjectListener(parent);
  }

  private boolean lookupToolbarItemFilter(GuiToolbarItem toolbarItem, String root) {
    boolean show = true;
    try {
      Object guiPluginInstance =
          findGuiPluginInstance(toolbarItem.getClassLoader(), guiPluginClassName, instanceId);
      List<GuiToolbarItemFilter> itemFilters =
          GuiRegistry.getInstance().getToolbarItemFiltersMap().get(root);
      if (itemFilters != null && !itemFilters.isEmpty()) {
        for (GuiToolbarItemFilter itemFilter : itemFilters) {
          Class<?> guiPluginClass =
              itemFilter.getClassLoader().loadClass(itemFilter.getGuiPluginClassName());
          Method guiPluginMethod =
              guiPluginClass.getMethod(
                  itemFilter.getGuiPluginMethodName(), String.class, Object.class);
          boolean showItem =
              (boolean) guiPluginMethod.invoke(null, toolbarItem.getId(), guiPluginInstance);
          if (!showItem) {
            show = false;
            break;
          }
        }
      }
    } catch (Exception e) {
      LogChannel.UI.logError(
          "Error finding GUI plugin instance for class "
              + toolbarItem.getListenerClass()
              + " and instanceId="
              + instanceId,
          e);
    }
    return show;
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
      PropsUi.setLook(label, Props.WIDGET_STYLE_TOOLBAR);
      label.pack();
      labelSeparator.setWidth(label.getSize().x);
      labelSeparator.setControl(label);
    }

    // Add the GUI element
    //
    switch (toolbarItem.getType()) {
      case LABEL:
        addToolbarLabel(parent, toolbarItem, toolBar);
        break;

      case BUTTON:
        if (EnvironmentUtils.getInstance().isWeb()) {
          addWebToolbarButton(toolbarItem, toolBar);
        } else {
          addToolbarButton(toolbarItem, toolBar);
        }
        break;

      case COMBO:
        addToolbarCombo(parent, toolbarItem, toolBar);
        break;

      case CHECKBOX:
        addToolbarCheckbox(parent, toolbarItem, toolBar);
        break;

      default:
        break;
    }
  }

  private void addToolbarLabel(Composite parent, GuiToolbarItem toolbarItem, ToolBar toolBar) {
    ToolItem labelSeparator = new ToolItem(toolBar, SWT.SEPARATOR);
    CLabel label =
        new CLabel(parent, SWT.CENTER | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT));
    label.setText(Const.NVL(toolbarItem.getLabel(), ""));
    label.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
    PropsUi.setLook(label, Props.WIDGET_STYLE_TOOLBAR);
    label.pack();
    labelSeparator.setWidth(label.getSize().x);
    labelSeparator.setControl(label);
    toolItemMap.put(toolbarItem.getId(), labelSeparator);
    widgetsMap.put(toolbarItem.getId(), label);
    Listener listener = getListener(toolbarItem);
    label.addListener(SWT.MouseUp, listener);
  }

  private void addToolbarCombo(Composite parent, GuiToolbarItem toolbarItem, ToolBar toolBar) {
    ToolItem comboSeparator = new ToolItem(toolBar, SWT.SEPARATOR);
    Combo combo =
        new Combo(parent, SWT.SINGLE | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT));
    combo.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
    combo.setItems(getComboItems(toolbarItem));
    PropsUi.setLook(combo);
    combo.pack();
    comboSeparator.setWidth(
        calculateComboWidth(combo)
            + toolbarItem.getExtraWidth()); // extra room for widget decorations
    comboSeparator.setControl(combo);

    Listener listener = getListener(toolbarItem);
    combo.addListener(SWT.Selection, listener);
    combo.addListener(SWT.DefaultSelection, listener);
    toolItemMap.put(toolbarItem.getId(), comboSeparator);
    widgetsMap.put(toolbarItem.getId(), combo);
    PropsUi.setLook(combo, Props.WIDGET_STYLE_TOOLBAR);
  }

  private void addToolbarCheckbox(Composite parent, GuiToolbarItem toolbarItem, ToolBar toolBar) {
    ToolItem checkboxSeparator = new ToolItem(toolBar, SWT.SEPARATOR);
    Button checkbox =
        new Button(parent, SWT.CHECK | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT));
    checkbox.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
    checkbox.setText(Const.NVL(toolbarItem.getLabel(), ""));
    PropsUi.setLook(checkbox);
    checkbox.pack();
    checkboxSeparator.setWidth(
        checkbox.getSize().x + toolbarItem.getExtraWidth()); // extra room for widget decorations
    checkboxSeparator.setControl(checkbox);
    Listener listener = getListener(toolbarItem);
    checkbox.addListener(SWT.Selection, listener);
    toolItemMap.put(toolbarItem.getId(), checkboxSeparator);
    widgetsMap.put(toolbarItem.getId(), checkbox);
  }

  private void addToolbarButton(GuiToolbarItem toolbarItem, ToolBar toolBar) {
    ToolItem item = new ToolItem(toolBar, SWT.NONE);
    if (itemBackgroundColor != null) {
      item.setBackground(itemBackgroundColor);
    }
    String imageLocation = findImageFilename(toolbarItem);
    setImages(item, toolbarItem.getClassLoader(), imageLocation);
    if (StringUtils.isNotEmpty(toolbarItem.getToolTip())) {
      item.setToolTipText(toolbarItem.getToolTip());
    }
    Listener listener = getListener(toolbarItem);
    item.addListener(SWT.Selection, listener);
    toolItemMap.put(toolbarItem.getId(), item);
    setToolItemKeyboardShortcut(item, toolbarItem);
  }

  private String findImageFilename(GuiToolbarItem toolbarItem) {
    String imageLocation;
    if (StringUtils.isEmpty(toolbarItem.getImageMethod())) {
      imageLocation = toolbarItem.getImage();
    } else {
      // Call the GUI plugin
      //
      try {
        Class<?> imageMethodClass =
            toolbarItem.getClassLoader().loadClass(toolbarItem.getListenerClass());
        // A static method which receives the GUI plugin object which can help determine the icon
        // filename
        Method imageMethod = imageMethodClass.getMethod(toolbarItem.getImageMethod(), Object.class);
        // Find the registered GUI plugin object
        Object guiPluginInstance =
            findGuiPluginInstance(
                toolbarItem.getClassLoader(), toolbarItem.getListenerClass(), instanceId);
        imageLocation = (String) imageMethod.invoke(null, guiPluginInstance);
      } catch (Exception e) {
        imageLocation = null;
        LogChannel.UI.logError(
            "Error getting toolbar image filename with method "
                + toolbarItem.getListenerClass()
                + "."
                + toolbarItem.getImageMethod(),
            e);
      }
    }
    return imageLocation;
  }

  private void addWebToolbarButton(GuiToolbarItem toolbarItem, ToolBar toolBar) {
    ToolItem item = new ToolItem(toolBar, SWT.SEPARATOR);

    Label label = new Label(toolBar, SWT.NONE);
    if (StringUtils.isNotEmpty(toolbarItem.getToolTip())) {
      label.setToolTipText(toolbarItem.getToolTip());
    }
    Listener listener = SvgLabelListener.getInstance();
    label.addListener(SWT.MouseDown, listener);
    label.addListener(SWT.Hide, listener);
    label.addListener(SWT.Show, listener);
    label.addListener(SWT.MouseEnter, listener);
    label.addListener(SWT.MouseExit, listener);
    label.addListener(SWT.MouseDown, getListener(toolbarItem));
    label.pack();

    // Take into account zooming and the extra room for widget decorations
    //
    int size =
        (int)
            (ConstUi.SMALL_ICON_SIZE * PropsUi.getNativeZoomFactor() + toolbarItem.getExtraWidth());

    String imageFilename = findImageFilename(toolbarItem);
    SvgLabelFacade.setData(toolbarItem.getId(), label, imageFilename, size);
    item.setWidth(size);
    item.setControl(label);

    widgetsMap.put(toolbarItem.getId(), label);
    toolItemMap.put(toolbarItem.getId(), item);
  }

  /**
   * See if there's a shortcut worth mentioning and add it to tooltip...
   *
   * @param toolItem
   * @param guiToolbarItem
   */
  private void setToolItemKeyboardShortcut(ToolItem toolItem, GuiToolbarItem guiToolbarItem) {
    KeyboardShortcut shortcut =
        GuiRegistry.getInstance()
            .findKeyboardShortcut(
                guiToolbarItem.getListenerClass(),
                guiToolbarItem.getListenerMethod(),
                Const.isOSX());
    if (shortcut != null) {
      toolItem.setToolTipText(toolItem.getToolTipText() + " (" + shortcut.toString() + ')');
    }
  }

  private void setImages(ToolItem item, ClassLoader classLoader, String location) {
    GuiResource gr = GuiResource.getInstance();
    int width = ConstUi.SMALL_ICON_SIZE;
    int height = ConstUi.SMALL_ICON_SIZE;

    if (StringUtils.isNotEmpty(location)) {
      Image enabledImage = gr.getImage(location, classLoader, width, height);
      item.setImage(enabledImage);

      // Grayscale the normal image
      Image disabledImage = gr.getImage(location, classLoader, width, height, true);
      item.setDisabledImage(disabledImage);
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
    if (EnvironmentUtils.getInstance().isWeb()) {
      //
      Label label = (Label) widgetsMap.get(id);
      SvgLabelFacade.enable(toolItem, id, label, enabled);
    } else {
      if (enabled != toolItem.isEnabled()) {
        toolItem.setEnabled(enabled);
      }
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
    boolean enabled = hasCapability && active;
    if (enabled != item.isEnabled()) {
      boolean enable = hasCapability && active;
      if (EnvironmentUtils.getInstance().isWeb()) {
        Label label = (Label) widgetsMap.get(id);
        SvgLabelFacade.enable(null, id, label, enable);
      } else {
        item.setEnabled(enable);
      }
    }
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
          LogChannel.UI.logError(
              "toolbar item with id '" + id + "' : widget not of instance Combo");
        }
      } else {
        LogChannel.UI.logError(
            "toolbar item with id '" + id + "' : control not found when refreshing combo");
      }
    } else {
      LogChannel.UI.logError("toolbar item with id '" + id + "' : not found when refreshing combo");
    }
  }

  public void selectComboItem(String id, String string) {
    GuiToolbarItem item = guiToolBarMap.get(id);
    if (item != null) {
      Control control = widgetsMap.get(id);
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

  protected Listener getListener(GuiToolbarItem toolbarItem) {
    return getListener(
        toolbarItem.getClassLoader(),
        toolbarItem.getListenerClass(),
        toolbarItem.getListenerMethod());
  }

  /**
   * Gets widgetsMap
   *
   * @return value of widgetsMap
   */
  public Map<String, Control> getWidgetsMap() {
    return widgetsMap;
  }

  /**
   * @param widgetsMap The widgetsMap to set
   */
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

  /**
   * @param toolItemMap The toolItemMap to set
   */
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

  /**
   * @param guiToolBarMap The guiToolBarMap to set
   */
  public void setGuiToolBarMap(Map<String, GuiToolbarItem> guiToolBarMap) {
    this.guiToolBarMap = guiToolBarMap;
  }

  /**
   * Gets itemBackgroundColor
   *
   * @return value of itemBackgroundColor
   */
  public Color getItemBackgroundColor() {
    return itemBackgroundColor;
  }

  /**
   * Sets itemBackgroundColor
   *
   * @param itemBackgroundColor value of itemBackgroundColor
   */
  public void setItemBackgroundColor(Color itemBackgroundColor) {
    this.itemBackgroundColor = itemBackgroundColor;
  }
}
