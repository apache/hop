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
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CLabel;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.layout.RowData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

/** This class contains the widgets for the GUI elements of a GUI Plugin */
public class GuiToolbarWidgets extends BaseGuiWidgets implements IToolbarWidgetRegistrar {

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

  /**
   * Create a toolbar parent control. Delegates to {@link ToolbarFacade}; on desktop returns a
   * ToolBar, on Hop Web (RAP) returns a Composite with RowLayout (wrap) so items wrap. Prefer
   * {@link ToolbarFacade#createToolbarContainer(Composite, int)} with {@link
   * #createToolbarWidgets(IToolbarContainer, String)} for a single code path.
   *
   * @param parent the parent composite
   * @param style SWT style (e.g. SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL)
   * @return the control to use for layout (container.getControl())
   */
  public static Control createToolbarParent(Composite parent, int style) {
    return ToolbarFacade.createToolbar(parent, style);
  }

  /**
   * Single-path toolbar creation using a container (from {@link
   * ToolbarFacade#createToolbarContainer}). No branching on environment; the container
   * implementation handles ToolBar vs web Composite.
   */
  public void createToolbarWidgets(IToolbarContainer container, String root) {
    List<GuiToolbarItem> toolbarItems = GuiRegistry.getInstance().findGuiToolbarItems(root);
    if (toolbarItems.isEmpty()) {
      LogChannel.UI.logError("Create widgets: no GUI toolbar items found for root: " + root);
      return;
    }

    Collections.sort(toolbarItems);

    for (GuiToolbarItem toolbarItem : toolbarItems) {
      boolean add = lookupToolbarItemFilter(toolbarItem, root);
      if (add && !removeToolItems.contains(toolbarItem.getId())) {
        guiToolBarMap.put(toolbarItem.getId(), toolbarItem);
        container.addItem(toolbarItem, this);
      }
    }

    Control control = container.getControl();
    if (control instanceof Composite composite) {
      composite.layout(true, true);
    }
    control.pack();
    addDeRegisterGuiPluginObjectListener(control);
  }

  /**
   * Same as {@link #createToolbarWidgets(IToolbarContainer, String)} but excludes the given item
   * ids from the toolbar.
   */
  public void createToolbarWidgets(
      IToolbarContainer container, String root, List<String> disabledToolItems) {
    this.removeToolItems = disabledToolItems != null ? disabledToolItems : new ArrayList<>();
    createToolbarWidgets(container, root);
  }

  @Override
  public void addItem(GuiToolbarItem item, Composite parent) {
    if (item.isIgnored()) {
      return;
    }
    // Switch between Hop GUI and Hop Web toolbars
    if (parent instanceof ToolBar toolBar) {
      addToolbarWidgetsToToolBar(toolBar, item);
    } else {
      addToolbarWidgetsToWebComposite(parent, item);
    }
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

  /** ToolBar (desktop) path: add one item to an SWT ToolBar. */
  private void addToolbarWidgetsToToolBar(ToolBar toolBar, GuiToolbarItem toolbarItem) {
    if (toolbarItem.isAddingSeparator()) {
      new ToolItem(toolBar, SWT.SEPARATOR);
    }

    if (toolbarItem.getType() != GuiToolbarElementType.LABEL
        && toolbarItem.getType() != GuiToolbarElementType.CHECKBOX
        && StringUtils.isNotEmpty(toolbarItem.getLabel())) {
      ToolItem labelSeparator = new ToolItem(toolBar, SWT.SEPARATOR);
      CLabel label =
          new CLabel(toolBar, SWT.CENTER | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT));
      label.setText(Const.NVL(toolbarItem.getLabel(), ""));
      label.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
      PropsUi.setLook(label, Props.WIDGET_STYLE_TOOLBAR);
      label.pack();
      labelSeparator.setWidth(label.getSize().x);
      labelSeparator.setControl(label);
    }

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

  /** Add toolbar items to a web Composite (Hop Web); no ToolItems, controls wrap by RowLayout. */
  private void addToolbarWidgetsToWebComposite(Composite parent, GuiToolbarItem toolbarItem) {
    if (toolbarItem.isAddingSeparator()) {
      addWebToolbarSeparator(parent);
    }

    if (toolbarItem.getType() != GuiToolbarElementType.LABEL
        && toolbarItem.getType() != GuiToolbarElementType.CHECKBOX
        && StringUtils.isNotEmpty(toolbarItem.getLabel())) {
      CLabel label =
          new CLabel(parent, SWT.CENTER | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT));
      label.setText(Const.NVL(toolbarItem.getLabel(), ""));
      label.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
      PropsUi.setLook(label, Props.WIDGET_STYLE_TOOLBAR);
      label.pack();
    }

    switch (toolbarItem.getType()) {
      case LABEL:
        addWebToolbarLabel(toolbarItem, parent);
        break;
      case BUTTON:
        addWebToolbarButtonToComposite(toolbarItem, parent);
        break;
      case COMBO:
        addWebToolbarCombo(toolbarItem, parent);
        break;
      case CHECKBOX:
        addWebToolbarCheckbox(toolbarItem, parent);
        break;
      default:
        break;
    }
  }

  /**
   * Vertical separator with groove look for Hop Web toolbar (matches desktop ToolItem SEPARATOR).
   * Uses toolbar background so no visible strip; draws groove full height with no top/bottom
   * margin.
   */
  private void addWebToolbarSeparator(Composite parent) {
    int width = 6;
    int height = (int) (ConstUi.SMALL_ICON_SIZE * PropsUi.getNativeZoomFactor()) + 6;
    Canvas canvas = new Canvas(parent, SWT.NONE);
    canvas.setLayoutData(new RowData(width, height));
    canvas.setBackground(parent.getBackground());
    PropsUi.setLook(canvas, Props.WIDGET_STYLE_TOOLBAR);
    canvas.addPaintListener(
        new PaintListener() {
          @Override
          public void paintControl(PaintEvent e) {
            GC gc = e.gc;
            Display display = e.display;
            Color highlight = display.getSystemColor(SWT.COLOR_WIDGET_LIGHT_SHADOW);
            Color shadow = display.getSystemColor(SWT.COLOR_WIDGET_NORMAL_SHADOW);
            int w = e.width;
            int h = e.height;
            int x = w / 2 - 1;
            gc.setForeground(highlight);
            gc.drawLine(x, 0, x, h - 1);
            gc.setForeground(shadow);
            gc.drawLine(x + 1, 0, x + 1, h - 1);
          }
        });
  }

  private void addWebToolbarLabel(GuiToolbarItem toolbarItem, Composite parent) {
    CLabel label =
        new CLabel(parent, SWT.CENTER | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT));
    label.setText(Const.NVL(toolbarItem.getLabel(), ""));
    label.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
    PropsUi.setLook(label, Props.WIDGET_STYLE_TOOLBAR);
    label.pack();
    widgetsMap.put(toolbarItem.getId(), label);
    Listener listener = getListener(toolbarItem);
    label.addListener(SWT.MouseUp, listener);
  }

  private void addWebToolbarCombo(GuiToolbarItem toolbarItem, Composite parent) {
    Combo combo =
        new Combo(
            parent,
            SWT.SINGLE
                | SWT.CENTER
                | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT)
                | (toolbarItem.isReadOnly() ? SWT.READ_ONLY : SWT.NONE));
    combo.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
    combo.setItems(getComboItems(toolbarItem));
    PropsUi.setLook(combo, Props.WIDGET_STYLE_TOOLBAR);
    combo.pack();
    int width = calculateComboWidth(combo) + toolbarItem.getExtraWidth();
    combo.setLayoutData(new RowData(width, SWT.DEFAULT));
    Listener listener = getListener(toolbarItem);
    combo.addListener(SWT.Selection, listener);
    combo.addListener(SWT.DefaultSelection, listener);
    widgetsMap.put(toolbarItem.getId(), combo);
  }

  private void addWebToolbarCheckbox(GuiToolbarItem toolbarItem, Composite parent) {
    Button checkbox =
        new Button(parent, SWT.CHECK | (toolbarItem.isAlignRight() ? SWT.RIGHT : SWT.LEFT));
    checkbox.setToolTipText(Const.NVL(toolbarItem.getToolTip(), ""));
    checkbox.setText(Const.NVL(toolbarItem.getLabel(), ""));
    PropsUi.setLook(checkbox, Props.WIDGET_STYLE_TOOLBAR);
    checkbox.pack();
    checkbox.setLayoutData(
        new RowData(checkbox.getSize().x + toolbarItem.getExtraWidth(), SWT.DEFAULT));
    Listener listener = getListener(toolbarItem);
    checkbox.addListener(SWT.Selection, listener);
    widgetsMap.put(toolbarItem.getId(), checkbox);
  }

  private void addWebToolbarButtonToComposite(GuiToolbarItem toolbarItem, Composite parent) {
    Composite composite = new Composite(parent, SWT.NONE);
    GridLayout layout = new GridLayout(2, false);
    layout.marginWidth = 0;
    layout.marginHeight = 0;
    layout.horizontalSpacing = 4;
    layout.verticalSpacing = 0;
    composite.setLayout(layout);
    PropsUi.setLook(composite, Props.WIDGET_STYLE_TOOLBAR);

    Label imageLabel = new Label(composite, SWT.NONE);
    if (StringUtils.isNotEmpty(toolbarItem.getToolTip())) {
      imageLabel.setToolTipText(toolbarItem.getToolTip());
      composite.setToolTipText(toolbarItem.getToolTip());
    }
    Listener listener = SvgLabelListener.getInstance();
    Listener toolbarListener = getListener(toolbarItem);

    imageLabel.addListener(SWT.MouseDown, listener);
    imageLabel.addListener(SWT.Hide, listener);
    imageLabel.addListener(SWT.Show, listener);
    imageLabel.addListener(SWT.MouseEnter, listener);
    imageLabel.addListener(SWT.MouseExit, listener);
    imageLabel.addListener(SWT.MouseUp, toolbarListener);
    composite.addListener(SWT.MouseUp, toolbarListener);

    int size =
        (int)
            (ConstUi.SMALL_ICON_SIZE * PropsUi.getNativeZoomFactor() + toolbarItem.getExtraWidth());

    String imageFilename = findImageFilename(toolbarItem);
    String uniqueId = instanceId + "-" + toolbarItem.getId();
    SvgLabelFacade.setData(uniqueId, imageLabel, imageFilename, size);
    composite.setData("iconSize", size);
    composite.setData("uniqueId", uniqueId);
    // Copy props to composite so client script (svg-label.js) has props.id when event.widget is the
    // composite
    composite.setData("props", imageLabel.getData("props"));

    GridData imageData = new GridData(SWT.LEFT, SWT.CENTER, false, false);
    imageData.widthHint = size;
    imageData.heightHint = size;
    imageLabel.setLayoutData(imageData);

    Label textLabel = new Label(composite, SWT.NONE);
    textLabel.setText("");
    PropsUi.setLook(textLabel, Props.WIDGET_STYLE_TOOLBAR);
    if (StringUtils.isNotEmpty(toolbarItem.getToolTip())) {
      textLabel.setToolTipText(toolbarItem.getToolTip());
    }
    textLabel.addListener(SWT.MouseUp, toolbarListener);
    GridData textData = new GridData(SWT.LEFT, SWT.CENTER, false, false);
    textLabel.setLayoutData(textData);
    textLabel.setVisible(false);

    composite.pack();
    composite.setLayoutData(new RowData(composite.getSize().x, composite.getSize().y));

    widgetsMap.put(toolbarItem.getId(), composite);
    textLabelMap.put(toolbarItem.getId(), textLabel);

    setToolItemKeyboardShortcutForComposite(composite, toolbarItem);
  }

  private void setToolItemKeyboardShortcutForComposite(
      Composite composite, GuiToolbarItem guiToolbarItem) {
    KeyboardShortcut shortcut =
        GuiRegistry.getInstance()
            .findKeyboardShortcut(
                guiToolbarItem.getListenerClass(),
                guiToolbarItem.getListenerMethod(),
                Const.isOSX());
    if (shortcut != null) {
      String tip = Const.NVL(guiToolbarItem.getToolTip(), "");
      composite.setToolTipText(tip + " (" + shortcut + ')');
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
    widgetsMap.put(toolbarItem.getId(), item.getParent());
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

    // Also add the toolbar click handler to the image label (MouseUp to match ToolItem.Selection
    // and avoid menu dismiss on RAP)
    imageLabel.addListener(SWT.MouseUp, toolbarListener);

    // Make the entire composite clickable (image + text)
    composite.addListener(SWT.MouseUp, toolbarListener);

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
    // Store so setToolbarItemImage() can update the icon when toggling state (e.g. show/hide
    // selected)
    composite.setData("iconSize", size);
    composite.setData("uniqueId", uniqueId);
    // Copy props to composite so client script (svg-label.js) has props.id when event.widget is the
    // composite
    composite.setData("props", imageLabel.getData("props"));

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
    textLabel.addListener(SWT.MouseUp, toolbarListener);
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
    Control control = widgetsMap.get(id);
    ToolItem toolItem = toolItemMap.get(id);

    // Web composite mode (Hop Web): no ToolItem, enable/disable the control only
    if (toolItem == null) {
      if (control != null && !control.isDisposed()) {
        if (control instanceof Composite composite) {
          Control[] children = composite.getChildren();
          if (children.length > 0 && children[0] instanceof Label imageLabel) {
            String uniqueId = instanceId + "-" + id;
            SvgLabelFacade.enable(null, uniqueId, imageLabel, enabled);
          }
          composite.setEnabled(enabled);
        } else {
          control.setEnabled(enabled);
        }
      }
      return;
    }
    if (toolItem.isDisposed()) {
      return;
    }
    if (EnvironmentUtils.getInstance().isWeb()) {
      if (control instanceof Composite composite) {
        Control[] children = composite.getChildren();
        if (children.length > 0 && children[0] instanceof Label imageLabel) {
          String uniqueId = instanceId + "-" + id;
          SvgLabelFacade.enable(toolItem, uniqueId, imageLabel, enabled);
        }
        composite.setEnabled(enabled);
      }
      if (enabled != toolItem.isEnabled()) {
        toolItem.setEnabled(enabled);
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
    boolean hasCapability = fileType.hasCapability(permission);
    boolean enable = hasCapability && active;

    if (item == null) {
      // Web composite mode: no ToolItem, enable/disable the control only
      enableToolbarItem(id, enable);
      return null;
    }
    if (item.isDisposed()) {
      return null;
    }
    if (enable != item.isEnabled()) {
      if (EnvironmentUtils.getInstance().isWeb()) {
        Control control = widgetsMap.get(id);
        if (control instanceof Composite composite) {
          Control[] children = composite.getChildren();
          if (children.length > 0 && children[0] instanceof Label imageLabel) {
            String uniqueId = instanceId + "-" + id;
            SvgLabelFacade.enable(null, uniqueId, imageLabel, enable);
          }
          composite.setEnabled(enable);
        }
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
   * Return the Control to use for positioning a popup menu for the given toolbar item. A ToolItem
   * is not a Control, and getControl() is only set for SEPARATOR items (the wrapped label/widget).
   * For BUTTON items we store the composite (image+text) in widgetsMap; use that for menu
   * positioning.
   *
   * @param id toolbar item id (e.g. {@code ID_TOOLBAR_ITEM_PROJECT})
   * @return the Control to use for getBounds() / toDisplay(), or null if not found
   */
  public Control getControlForMenu(String id) {
    return widgetsMap.get(id);
  }

  /**
   * Set the image of a toolbar item by path. Use this when toggling between two icons (e.g. show
   * only selected / show all). In desktop SWT this sets the ToolItem's image; in web RWT this
   * updates the SVG in the label so the icon change is visible.
   *
   * @param id the toolbar item id (from @GuiToolbarElement)
   * @param imagePath the image path (e.g. "ui/images/show-selected.svg")
   */
  public void setToolbarItemImage(String id, String imagePath) {
    if (StringUtils.isEmpty(imagePath)) {
      return;
    }
    Control control = widgetsMap.get(id);
    ToolItem toolItem = toolItemMap.get(id);

    if (EnvironmentUtils.getInstance().isWeb()
        && control instanceof Composite composite
        && !composite.isDisposed()) {
      Control[] children = composite.getChildren();
      if (children.length > 0 && children[0] instanceof Label imageLabel) {
        String uniqueId = (String) composite.getData("uniqueId");
        if (uniqueId != null) {
          SvgLabelFacade.updateImageSource(uniqueId, imageLabel, imagePath);
        }
      }
      return;
    }
    if (toolItem == null || toolItem.isDisposed()) {
      return;
    }
    Image image = GuiResource.getInstance().getImage(imagePath);
    if (image != null) {
      toolItem.setImage(image);
    }
  }

  /**
   * Set the tooltip on a toolbar item. Works for both ToolBar (desktop) and web Composite (Hop
   * Web).
   *
   * @param id the toolbar item id (from @GuiToolbarElement)
   * @param tooltip the tooltip text
   */
  public void setToolbarItemToolTip(String id, String tooltip) {
    Control control = widgetsMap.get(id);
    ToolItem toolItem = toolItemMap.get(id);
    if (control != null && !control.isDisposed()) {
      control.setToolTipText(Const.NVL(tooltip, ""));
    }
    if (toolItem != null && !toolItem.isDisposed()) {
      toolItem.setToolTipText(Const.NVL(tooltip, ""));
    }
  }

  /**
   * Set text on a toolbar item. Handles both SWT (desktop) and RWT (web) environments. In SWT, sets
   * text directly on the ToolItem. In RWT, updates the separate text Label next to the image.
   *
   * @param id The ID of the toolbar item
   * @param text The text to set
   */
  public void setToolbarItemText(String id, String text) {
    Label textLabel = textLabelMap.get(id);
    Composite composite = widgetsMap.get(id) instanceof Composite c ? c : null;
    ToolItem toolItem = toolItemMap.get(id);

    if (EnvironmentUtils.getInstance().isWeb() && (textLabel != null || composite != null)) {
      if (textLabel != null && !textLabel.isDisposed()) {
        String textToSet = Const.NVL(text, "");
        textLabel.setText(textToSet);
        textLabel.setVisible(!Utils.isEmpty(textToSet));
      }
      if (composite != null && !composite.isDisposed()) {
        composite.pack();
        int extraMargin = !Utils.isEmpty(Const.NVL(text, "")) ? 10 : 0;
        if (toolItem != null && !toolItem.isDisposed()) {
          toolItem.setWidth(composite.getSize().x + extraMargin);
          ToolBar toolbar = toolItem.getParent();
          if (toolbar != null && !toolbar.isDisposed()) {
            toolbar.layout(true, true);
          }
        } else {
          composite.setLayoutData(
              new RowData(composite.getSize().x + extraMargin, composite.getSize().y));
          composite.getParent().layout(true, true);
        }
      }
      return;
    }
    if (toolItem != null && !toolItem.isDisposed()) {
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
    Label textLabel = textLabelMap.get(id);
    if (textLabel != null && !textLabel.isDisposed()) {
      return Const.NVL(textLabel.getText(), "");
    }
    ToolItem toolItem = toolItemMap.get(id);
    if (toolItem != null && !toolItem.isDisposed()) {
      return Const.NVL(toolItem.getText(), "");
    }
    return "";
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
