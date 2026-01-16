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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.KeyboardShortcut;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarItem;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarItemFilter;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
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
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
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

  public static final String CONST_TOOLBAR_ITEM_WITH_ID = "toolbar item with id '";

  private Map<String, GuiToolbarItem> guiToolBarMap;
  @Getter private Map<String, Control> widgetsMap;
  private Map<String, ToolItem> toolItemMap;
  private Map<String, Label> textLabelMap; // For web/RWT: stores text labels separately
  private List<String> removeToolItems;
  private Color itemBackgroundColor;

  public GuiToolbarWidgets() {
    super(UUID.randomUUID().toString());
    guiToolBarMap = new HashMap<>();
    widgetsMap = new HashMap<>();
    toolItemMap = new HashMap<>();
    textLabelMap = new HashMap<>();
    removeToolItems = new ArrayList<>();
  }

  public void createToolbarWidgets(Composite parent, String root, List<String> diabledToolItems) {
    this.removeToolItems = diabledToolItems;
    createToolbarWidgets(parent, root);
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
      if (add && !removeToolItems.contains(toolbarItem.getId())) {
        addToolbarWidgets(parent, toolbarItem);
      }
    }

    // Force re-layout
    //
    parent.layout(true, true);
    parent.pack();

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
      if (!Utils.isEmpty(itemFilters)) {
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

    if (!(parent instanceof ToolBar toolBar)) {
      throw new RuntimeException(
          "We can only add toolbar items to a toolbar, not class " + parent.getClass().getName());
    }

    // We want to add a separator if the annotation asked for it
    // We also want to add a separator in case the toolbar element type isn't a button
    //
    if (toolbarItem.isAddingSeparator()) {
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
        addToolbarLabel(toolbarItem, toolBar);
        break;

      case BUTTON:
        if (EnvironmentUtils.getInstance().isWeb()) {
          addWebToolbarButton(toolbarItem, toolBar);
        } else {
          addToolbarButton(toolbarItem, toolBar);
        }
        break;

      case COMBO:
        addToolbarCombo(toolbarItem, toolBar);
        break;

      case CHECKBOX:
        addToolbarCheckbox(toolbarItem, toolBar);
        break;

      default:
        break;
    }
  }

  private void addToolbarLabel(GuiToolbarItem toolbarItem, ToolBar toolBar) {
    ToolItem labelSeparator = new ToolItem(toolBar, SWT.SEPARATOR);
    CLabel label =
        new CLabel(toolBar, SWT.CENTER | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT));
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

  private void addToolbarCombo(GuiToolbarItem toolbarItem, ToolBar toolBar) {
    ToolItem comboSeparator = new ToolItem(toolBar, SWT.SEPARATOR | SWT.BOTTOM);
    Combo combo =
        new Combo(
            toolBar,
            SWT.SINGLE
                | SWT.CENTER
                | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT)
                | (toolbarItem.isReadOnly() ? SWT.READ_ONLY : SWT.NONE));
    combo.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
    combo.setItems(getComboItems(toolbarItem));
    PropsUi.setLook(combo, Props.WIDGET_STYLE_TOOLBAR);
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

  private void addToolbarCheckbox(GuiToolbarItem toolbarItem, ToolBar toolBar) {
    ToolItem checkboxSeparator = new ToolItem(toolBar, SWT.SEPARATOR);
    Button checkbox =
        new Button(toolBar, SWT.CHECK | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT));
    checkbox.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
    checkbox.setText(Const.NVL(toolbarItem.getLabel(), ""));
    PropsUi.setLook(checkbox, Props.WIDGET_STYLE_TOOLBAR);
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

    // Create a Composite to hold both the image and text label
    Composite composite = new Composite(toolBar, SWT.NONE);
    GridLayout layout = new GridLayout(2, false);
    layout.marginWidth = 0;
    layout.marginHeight = 0;
    layout.horizontalSpacing = 4;
    layout.verticalSpacing = 0;
    composite.setLayout(layout);
    PropsUi.setLook(composite, Props.WIDGET_STYLE_TOOLBAR);

    // Create the image label
    Label imageLabel = new Label(composite, SWT.NONE);
    if (StringUtils.isNotEmpty(toolbarItem.getToolTip())) {
      imageLabel.setToolTipText(toolbarItem.getToolTip());
      composite.setToolTipText(toolbarItem.getToolTip());
    }
    Listener listener = SvgLabelListener.getInstance();
    Listener toolbarListener = getListener(toolbarItem);

    // Add SVG listeners to image label
    imageLabel.addListener(SWT.MouseDown, listener);
    imageLabel.addListener(SWT.Hide, listener);
    imageLabel.addListener(SWT.Show, listener);
    imageLabel.addListener(SWT.MouseEnter, listener);
    imageLabel.addListener(SWT.MouseExit, listener);

    // Also add the toolbar click handler to the image label
    imageLabel.addListener(SWT.MouseDown, toolbarListener);

    // Make the entire composite clickable (image + text)
    composite.addListener(SWT.MouseDown, toolbarListener);

    // Take into account zooming and the extra room for widget decorations
    //
    int size =
        (int)
            (ConstUi.SMALL_ICON_SIZE * PropsUi.getNativeZoomFactor() + toolbarItem.getExtraWidth());

    String imageFilename = findImageFilename(toolbarItem);
    // Create a unique DOM element ID by combining instance ID with toolbar item ID
    // This prevents ID collisions when multiple tabs have the same toolbar items
    String uniqueId = instanceId + "-" + toolbarItem.getId();
    SvgLabelFacade.setData(uniqueId, imageLabel, imageFilename, size);

    GridData imageData = new GridData(SWT.LEFT, SWT.CENTER, false, false);
    imageData.widthHint = size;
    imageData.heightHint = size;
    imageLabel.setLayoutData(imageData);

    // Create the text label (initially empty, will be set later if needed)
    Label textLabel = new Label(composite, SWT.NONE);
    textLabel.setText("");
    PropsUi.setLook(textLabel, Props.WIDGET_STYLE_TOOLBAR);
    if (StringUtils.isNotEmpty(toolbarItem.getToolTip())) {
      textLabel.setToolTipText(toolbarItem.getToolTip());
    }
    // Make text label part of the clickable button
    textLabel.addListener(SWT.MouseDown, toolbarListener);
    GridData textData = new GridData(SWT.LEFT, SWT.CENTER, false, false);
    textLabel.setLayoutData(textData);
    textLabel.setVisible(false); // Hidden until text is set

    composite.pack();
    item.setWidth(composite.getSize().x);
    item.setControl(composite);

    widgetsMap.put(toolbarItem.getId(), composite);
    textLabelMap.put(toolbarItem.getId(), textLabel);
    toolItemMap.put(toolbarItem.getId(), item);
  }

  /**
   * See if there's a shortcut worth mentioning and add it to tooltip...
   *
   * @param toolItem the ToolItem to get the shortcut for
   * @param guiToolbarItem the Toolbar where the item is part of
   */
  private void setToolItemKeyboardShortcut(ToolItem toolItem, GuiToolbarItem guiToolbarItem) {
    KeyboardShortcut shortcut =
        GuiRegistry.getInstance()
            .findKeyboardShortcut(
                guiToolbarItem.getListenerClass(),
                guiToolbarItem.getListenerMethod(),
                Const.isOSX());
    if (shortcut != null) {
      toolItem.setToolTipText(toolItem.getToolTipText() + " (" + shortcut + ')');
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
      // In web/RWT, the widget is a Composite containing the image label
      Control control = widgetsMap.get(id);
      if (control instanceof Composite composite) {
        // Find the image label (first child)
        Control[] children = composite.getChildren();
        if (children.length > 0 && children[0] instanceof Label imageLabel) {
          // Use the unique DOM element ID (instance ID + toolbar item ID)
          String uniqueId = instanceId + "-" + id;
          SvgLabelFacade.enable(toolItem, uniqueId, imageLabel, enabled);
        }
      }
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
   * @param fileType the IHopFileType to check
   * @param id The ID of the widget to look for
   * @param permission check if the filetype has the capability you want
   * @return The toolbar item or null if nothing is found
   */
  public ToolItem enableToolbarItem(IHopFileType fileType, String id, String permission) {
    return enableToolbarItem(fileType, id, permission, true);
  }

  /**
   * Find the toolbar item with the given ID. Check the capability in the given file type Enable or
   * disable accordingly.
   *
   * @param fileType the IHopFileType to check
   * @param id The ID of the widget to look for
   * @param permission check if the filetype has the capability you want
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
        // In web/RWT, the widget is a Composite containing the image label
        Control control = widgetsMap.get(id);
        if (control instanceof Composite composite) {
          // Find the image label (first child)
          Control[] children = composite.getChildren();
          if (children.length > 0 && children[0] instanceof Label imageLabel) {
            // Use the unique DOM element ID (instance ID + toolbar item ID)
            String uniqueId = instanceId + "-" + id;
            SvgLabelFacade.enable(null, uniqueId, imageLabel, enable);
          }
        }
        // Update ToolItem state so future checks work correctly
        item.setEnabled(enable);
      } else {
        item.setEnabled(enable);
      }
    }
    return item;
  }

  public ToolItem findToolItem(String id) {
    return toolItemMap.get(id);
  }

  /**
   * Set text on a toolbar item. Handles both SWT (desktop) and RWT (web) environments. In SWT, sets
   * text directly on the ToolItem. In RWT, updates the separate text Label next to the image.
   *
   * @param id The ID of the toolbar item
   * @param text The text to set
   */
  public void setToolbarItemText(String id, String text) {
    ToolItem toolItem = toolItemMap.get(id);
    if (toolItem == null || toolItem.isDisposed()) {
      return;
    }

    if (EnvironmentUtils.getInstance().isWeb()) {
      // In web/RWT, update the separate text label
      Label textLabel = textLabelMap.get(id);
      if (textLabel != null && !textLabel.isDisposed()) {
        String textToSet = Const.NVL(text, "");
        textLabel.setText(textToSet);
        textLabel.setVisible(!Utils.isEmpty(textToSet));

        // Update the composite width to accommodate the text
        Composite composite = (Composite) widgetsMap.get(id);
        if (composite != null && !composite.isDisposed()) {
          composite.pack();
          // Add extra margin to the right for spacing between toolbar items
          int extraMargin = !Utils.isEmpty(textToSet) ? 10 : 0;
          toolItem.setWidth(composite.getSize().x + extraMargin);

          // Force layout update
          ToolBar toolbar = toolItem.getParent();
          if (toolbar != null && !toolbar.isDisposed()) {
            toolbar.layout(true, true);
          }
        }
      }
    } else {
      // In SWT, set text directly on the ToolItem
      toolItem.setText(Const.NVL(text, ""));
    }
  }

  /**
   * Get text from a toolbar item. Handles both SWT (desktop) and RWT (web) environments. In SWT,
   * gets text directly from the ToolItem. In RWT, gets text from the separate text Label.
   *
   * @param id The ID of the toolbar item
   * @return The text on the toolbar item, or empty string if not found
   */
  public String getToolbarItemText(String id) {
    ToolItem toolItem = toolItemMap.get(id);
    if (toolItem == null || toolItem.isDisposed()) {
      return "";
    }

    if (EnvironmentUtils.getInstance().isWeb()) {
      // In web/RWT, get text from the separate text label
      Label textLabel = textLabelMap.get(id);
      if (textLabel != null && !textLabel.isDisposed()) {
        return Const.NVL(textLabel.getText(), "");
      }
      return "";
    } else {
      // In SWT, get text directly from the ToolItem
      return Const.NVL(toolItem.getText(), "");
    }
  }

  public void refreshComboItemList(String id) {
    GuiToolbarItem item = guiToolBarMap.get(id);
    if (item != null) {
      Control control = widgetsMap.get(id);
      if (control != null) {
        if (control instanceof Combo combo) {
          combo.setItems(getComboItems(item));
        } else {
          LogChannel.UI.logError(
              CONST_TOOLBAR_ITEM_WITH_ID + id + "' : widget not of instance Combo");
        }
      } else {
        LogChannel.UI.logError(
            CONST_TOOLBAR_ITEM_WITH_ID + id + "' : control not found when refreshing combo");
      }
    } else {
      LogChannel.UI.logError(
          CONST_TOOLBAR_ITEM_WITH_ID + id + "' : not found when refreshing combo");
    }
  }

  public void selectComboItem(String id, String string) {
    GuiToolbarItem item = guiToolBarMap.get(id);
    if (item != null) {
      Control control = widgetsMap.get(id);
      if (control instanceof Combo combo) {
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
}
