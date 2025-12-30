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
 *
 */

package org.apache.hop.ui.hopgui.perspective.configuration.tabs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.KeyboardShortcut;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.ExpandBar;
import org.eclipse.swt.widgets.ExpandItem;
import org.eclipse.swt.widgets.Label;

@GuiPlugin
public class ConfigKeyboardShortcutsTab {

  private static final Class<?> PKG = BaseDialog.class;

  private Font monoFont;
  private Color keyBackgroundColor;

  public ConfigKeyboardShortcutsTab() {
    // Constructor for GuiPlugin instantiation
  }

  @GuiTab(
      id = "10150-config-perspective-keyboard-shortcuts-tab",
      parentId = ConfigurationPerspective.CONFIG_PERSPECTIVE_TABS,
      description = "Keyboard shortcuts tab")
  public void addKeyboardShortcutsTab(CTabFolder wTabFolder) {
    int margin = PropsUi.getMargin();

    CTabItem wShortcutsTab = new CTabItem(wTabFolder, SWT.NONE);
    wShortcutsTab.setFont(GuiResource.getInstance().getFontDefault());
    wShortcutsTab.setText("Keyboard Shortcuts");
    wShortcutsTab.setImage(GuiResource.getInstance().getImageEdit());

    // Main scrollable composite
    ScrolledComposite scrolledComposite =
        new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    scrolledComposite.setLayout(new FormLayout());

    Composite contentComposite = new Composite(scrolledComposite, SWT.NONE);
    PropsUi.setLook(contentComposite);
    contentComposite.setLayout(new FormLayout());

    // Initialize colors and fonts
    Display display = wTabFolder.getDisplay();
    keyBackgroundColor = new Color(display, 240, 240, 245);
    Color keyBorderColor = new Color(display, 180, 180, 190);

    // Create a monospace font for key labels
    FontData[] fontData = display.getSystemFont().getFontData();
    if (fontData.length > 0) {
      FontData monoFontData =
          new FontData(fontData[0].getName(), fontData[0].getHeight(), SWT.BOLD);
      monoFont = new Font(display, monoFontData);
    }

    // Get all keyboard shortcuts from the registry
    GuiRegistry guiRegistry = GuiRegistry.getInstance();
    Map<String, List<KeyboardShortcut>> shortcutsMap = guiRegistry.getShortCutsMap();

    // Determine if we're on macOS
    boolean isMacOS = Const.isOSX();

    // Create expandable sections grouped by class
    Control lastControl = null;
    List<String> classNames = new ArrayList<>(shortcutsMap.keySet());
    Collections.sort(classNames);

    for (String className : classNames) {
      List<KeyboardShortcut> allShortcuts = shortcutsMap.get(className);
      if (allShortcuts == null || allShortcuts.isEmpty()) {
        continue;
      }

      // Filter shortcuts to only show those relevant to the current OS
      List<KeyboardShortcut> shortcuts = new ArrayList<>();
      for (KeyboardShortcut shortcut : allShortcuts) {
        if (shortcut.isOsx() == isMacOS) {
          shortcuts.add(shortcut);
        }
      }

      // Skip this class if no relevant shortcuts after filtering
      if (shortcuts.isEmpty()) {
        continue;
      }

      // Sort shortcuts alphabetically by their formatted method names
      shortcuts.sort(
          (s1, s2) ->
              formatMethodName(s1.getParentMethodName())
                  .compareTo(formatMethodName(s2.getParentMethodName())));

      // Create expand bar for this class
      ExpandBar expandBar = new ExpandBar(contentComposite, SWT.NONE);
      PropsUi.setLook(expandBar);

      // Create the content composite for this expand item
      Composite expandContent = new Composite(expandBar, SWT.NONE);
      PropsUi.setLook(expandContent);
      FormLayout expandLayout = new FormLayout();
      expandLayout.marginWidth = PropsUi.getFormMargin();
      expandLayout.marginHeight = PropsUi.getFormMargin();
      expandContent.setLayout(expandLayout);

      // Add shortcuts to this section
      Control lastShortcut = null;
      for (KeyboardShortcut shortcut : shortcuts) {
        lastShortcut = createShortcutRow(expandContent, shortcut, lastShortcut, margin);
      }

      // Set the expand item
      ExpandItem expandItem = new ExpandItem(expandBar, SWT.NONE);
      expandItem.setText(getPluginName(className));
      expandItem.setControl(expandContent);
      expandItem.setExpanded(true);
      expandItem.setHeight(expandContent.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);

      // Set the ExpandBar layout (no height constraint)
      FormData fdExpandBar = new FormData();
      fdExpandBar.left = new FormAttachment(0, 0);
      fdExpandBar.right = new FormAttachment(100, 0);
      if (lastControl != null) {
        fdExpandBar.top = new FormAttachment(lastControl, margin);
      } else {
        fdExpandBar.top = new FormAttachment(0, margin);
      }
      expandBar.setLayoutData(fdExpandBar);

      // Add expand/collapse listeners to this specific expand bar
      expandBar.addListener(
          SWT.Expand,
          e ->
              Display.getDefault()
                  .asyncExec(
                      () -> {
                        if (!contentComposite.isDisposed() && !scrolledComposite.isDisposed()) {
                          contentComposite.layout();
                          scrolledComposite.setMinHeight(
                              contentComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
                        }
                      }));
      expandBar.addListener(
          SWT.Collapse,
          e ->
              Display.getDefault()
                  .asyncExec(
                      () -> {
                        if (!contentComposite.isDisposed() && !scrolledComposite.isDisposed()) {
                          contentComposite.layout();
                          scrolledComposite.setMinHeight(
                              contentComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
                        }
                      }));

      lastControl = expandBar;
    }

    // Add symbol reference section at the bottom for macOS
    if (Const.isOSX()) {
      lastControl = addSymbolReference(contentComposite, lastControl, margin);
    }

    // Add bottom spacer to ensure proper bottom margin
    Label bottomSpacer = new Label(contentComposite, SWT.NONE);
    FormData fdBottomSpacer = new FormData();
    fdBottomSpacer.left = new FormAttachment(0, 0);
    fdBottomSpacer.top = new FormAttachment(lastControl, margin * 2);
    fdBottomSpacer.height = 1;
    bottomSpacer.setLayoutData(fdBottomSpacer);

    // Setup scrolled composite
    contentComposite.layout();
    contentComposite.pack();
    scrolledComposite.setContent(contentComposite);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(contentComposite.getBounds().width);
    scrolledComposite.setMinHeight(contentComposite.getBounds().height);

    // Add dispose listener to clean up resources
    scrolledComposite.addDisposeListener(
        e -> {
          if (monoFont != null && !monoFont.isDisposed()) {
            monoFont.dispose();
          }
          if (keyBackgroundColor != null && !keyBackgroundColor.isDisposed()) {
            keyBackgroundColor.dispose();
          }
          if (keyBorderColor != null && !keyBorderColor.isDisposed()) {
            keyBorderColor.dispose();
          }
        });

    wShortcutsTab.setControl(scrolledComposite);
  }

  /**
   * Creates a row displaying a keyboard shortcut.
   *
   * @param parent The parent composite
   * @param shortcut The keyboard shortcut to display
   * @param lastControl The last control for positioning
   * @param margin The margin to use
   * @return The created composite
   */
  // Suppress the warning for "useless assignment" on lastKey as it is there for readability and
  // further chaining
  @SuppressWarnings("java:S1854")
  private Control createShortcutRow(
      Composite parent, KeyboardShortcut shortcut, Control lastControl, int margin) {
    Composite row = new Composite(parent, SWT.NONE);
    PropsUi.setLook(row);
    row.setLayout(new FormLayout());

    FormData fdRow = new FormData();
    fdRow.left = new FormAttachment(0, 0);
    fdRow.right = new FormAttachment(100, 0);
    if (lastControl != null) {
      fdRow.top = new FormAttachment(lastControl, margin / 2);
    } else {
      fdRow.top = new FormAttachment(0, 0);
    }
    row.setLayoutData(fdRow);

    // Create shortcut keys display on the left - fixed width column
    Composite keysComposite = new Composite(row, SWT.NONE);
    PropsUi.setLook(keysComposite);
    keysComposite.setLayout(new FormLayout());

    FormData fdKeys = new FormData();
    fdKeys.left = new FormAttachment(0, 0);
    fdKeys.top = new FormAttachment(0, 0);
    fdKeys.bottom = new FormAttachment(100, 0);
    fdKeys.width = 200; // Fixed width for shortcut column
    keysComposite.setLayoutData(fdKeys);

    // Add modifier keys and main key
    // Standard order: Control, Alt, Command, Shift (Command before Shift for better readability)
    boolean isMacOS = Const.isOSX();
    Control lastKey = null;
    if (shortcut.isControl()) {
      lastKey = createKeyBadge(keysComposite, isMacOS ? "⌃" : "Ctrl", lastKey, margin);
    }
    if (shortcut.isAlt()) {
      lastKey = createKeyBadge(keysComposite, isMacOS ? "⌥" : "Alt", lastKey, margin);
    }
    if (shortcut.isCommand()) {
      lastKey = createKeyBadge(keysComposite, isMacOS ? "⌘" : "Cmd", lastKey, margin);
    }
    if (shortcut.isShift()) {
      lastKey = createKeyBadge(keysComposite, isMacOS ? "⇧" : "Shift", lastKey, margin);
    }

    // Add the main key
    String keyText = getKeyText(shortcut.getKeyCode());
    if (!Utils.isEmpty(keyText)) {
      lastKey = createKeyBadge(keysComposite, keyText, lastKey, margin);
    }

    // Add method name on the right - starts at fixed position
    Label methodLabel = new Label(row, SWT.LEFT);
    PropsUi.setLook(methodLabel);
    methodLabel.setText(formatMethodName(shortcut.getParentMethodName()));

    FormData fdMethod = new FormData();
    fdMethod.left = new FormAttachment(keysComposite, margin);
    // Vertically center the label - align with badge center
    fdMethod.top = new FormAttachment(0, 0);
    fdMethod.right = new FormAttachment(100, 0);
    methodLabel.setLayoutData(fdMethod);

    return row;
  }

  /**
   * Creates a styled label that looks like a keyboard key.
   *
   * @param parent The parent composite
   * @param text The key text
   * @param lastControl The last control for positioning
   * @param margin The margin to use
   * @return The created label
   */
  private Label createKeyBadge(Composite parent, String text, Control lastControl, int margin) {
    Label key = new Label(parent, SWT.CENTER | SWT.BORDER);
    key.setText(text);
    key.setBackground(keyBackgroundColor);
    key.setForeground(parent.getDisplay().getSystemColor(SWT.COLOR_DARK_GRAY));
    if (monoFont != null) {
      key.setFont(monoFont);
    }

    FormData fdKey = new FormData();
    if (lastControl != null) {
      fdKey.left = new FormAttachment(lastControl, margin / 2);
    } else {
      fdKey.left = new FormAttachment(0, 0);
    }
    fdKey.top = new FormAttachment(0, 0);
    fdKey.bottom = new FormAttachment(100, 0);
    // Set width based on text length - Unicode symbols need less space than text labels
    fdKey.width = text.length() <= 1 ? 24 : text.length() * 14 + 12;
    key.setLayoutData(fdKey);

    return key;
  }

  /**
   * Converts a key code to a readable string.
   *
   * @param keyCode The SWT key code
   * @return A readable key string
   */
  private String getKeyText(int keyCode) {
    boolean isMacOS = Const.isOSX();

    // Spacebar
    if (keyCode == 32 || keyCode == ' ') {
      return "Space";
    }
    // Character upper
    else if (keyCode >= 65 && keyCode <= 90) {
      return String.valueOf((char) keyCode);
    }
    // Character lower
    else if (keyCode >= 97 && keyCode <= 122) {
      return String.valueOf(Character.toUpperCase((char) keyCode));
    }
    // Delete key
    else if (keyCode == 127) {
      return isMacOS ? "⌫" : "Del";
    }
    // Digit
    else if ((keyCode >= 48 && keyCode <= 57) || "+-/*".indexOf(keyCode) >= 0) {
      return String.valueOf((char) keyCode);
    }

    // Special keys (SWT constants)
    if ((keyCode & (1 << 24)) != 0) {
      switch (keyCode & (0xFFFF)) {
        case 1:
          return "↑";
        case 2:
          return "↓";
        case 3:
          return "←";
        case 4:
          return "→";
        case 5:
          return "PgUp";
        case 6:
          return "PgDn";
        case 7:
          return "Home";
        case 8:
          return "End";
        case 9:
          return "Ins";
        case 10:
          return "F1";
        case 11:
          return "F2";
        case 12:
          return "F3";
        case 13:
          return "F4";
        case 14:
          return "F5";
        case 15:
          return "F6";
        case 16:
          return "F7";
        case 17:
          return "F8";
        case 18:
          return "F9";
        case 19:
          return "F10";
        case 20:
          return "F11";
        case 21:
          return "F12";
        case 22:
          return "F13";
        case 23:
          return "F14";
        case 24:
          return "F15";
        case 25:
          return "F16";
        case 26:
          return "F17";
        case 27:
          return "F18";
        case 28:
          return "F19";
        case 29:
          return "F20";
        default:
          break;
      }
    }

    // ESC key
    if (keyCode == SWT.ESC) {
      return isMacOS ? "⎋" : "Esc";
    }

    return "";
  }

  /**
   * Formats a method name to be more readable.
   *
   * @param methodName The method name
   * @return Formatted method name
   */
  private String formatMethodName(String methodName) {
    if (Utils.isEmpty(methodName)) {
      return "";
    }

    // Convert camelCase to Title Case with spaces
    StringBuilder formatted = new StringBuilder();
    for (int i = 0; i < methodName.length(); i++) {
      char c = methodName.charAt(i);
      if (i == 0) {
        formatted.append(Character.toUpperCase(c));
      } else if (Character.isUpperCase(c)) {
        formatted.append(' ').append(c);
      } else {
        formatted.append(c);
      }
    }

    return formatted.toString();
  }

  /**
   * Gets a friendly plugin name from a fully qualified class name. Checks GuiPlugin annotation for
   * name or id, otherwise falls back to the simple class name.
   *
   * @param className The fully qualified class name
   * @return The plugin name, id, or simple class name
   */
  private String getPluginName(String className) {
    if (Utils.isEmpty(className)) {
      return "";
    }

    try {
      // Try to load the class and get its GuiPlugin annotation
      Class<?> clazz = Class.forName(className);
      GuiPlugin annotation = clazz.getAnnotation(GuiPlugin.class);

      if (annotation != null) {
        // First try to get the name
        if (!Utils.isEmpty(annotation.name())) {
          String name = TranslateUtil.translate(annotation.name(), clazz);
          if (!Utils.isEmpty(name)) {
            return name;
          }
        }

        // Fall back to id if name is empty
        if (!Utils.isEmpty(annotation.id())) {
          return annotation.id();
        }
      }
    } catch (Exception e) {
      // Fall through to simple class name
    }

    // Fallback to simple class name
    int lastDot = className.lastIndexOf('.');
    if (lastDot >= 0 && lastDot < className.length() - 1) {
      return className.substring(lastDot + 1);
    }
    return className;
  }

  /**
   * Adds a reference section showing what special keyboard symbols mean.
   *
   * @param parent The parent composite
   * @param lastControl The last control for positioning
   * @param margin The margin to use
   * @return The last control created in this section
   */
  private Control addSymbolReference(Composite parent, Control lastControl, int margin) {
    // Add separator line
    Label separator = new Label(parent, SWT.SEPARATOR | SWT.HORIZONTAL);
    FormData fdSeparator = new FormData();
    fdSeparator.left = new FormAttachment(0, 0);
    fdSeparator.right = new FormAttachment(100, 0);
    fdSeparator.top = new FormAttachment(lastControl, margin * 2);
    separator.setLayoutData(fdSeparator);

    // Add title
    Label titleLabel = new Label(parent, SWT.LEFT);
    PropsUi.setLook(titleLabel);
    titleLabel.setText(
        BaseMessages.getString(PKG, "ConfigKeyboardShortcutsTab.SymbolReference.Title"));
    titleLabel.setFont(monoFont);

    FormData fdTitle = new FormData();
    fdTitle.left = new FormAttachment(0, 0);
    fdTitle.top = new FormAttachment(separator, margin);
    titleLabel.setLayoutData(fdTitle);

    // Add reference items in a compact grid
    Composite refComposite = new Composite(parent, SWT.NONE);
    PropsUi.setLook(refComposite);
    refComposite.setLayout(new FormLayout());

    FormData fdRefComposite = new FormData();
    fdRefComposite.left = new FormAttachment(0, 0);
    fdRefComposite.right = new FormAttachment(100, 0);
    fdRefComposite.top = new FormAttachment(titleLabel, margin / 2);
    refComposite.setLayoutData(fdRefComposite);

    // Define symbols and their meanings (in priority order)
    String[][] symbols = {
      {"⌃", BaseMessages.getString(PKG, "ConfigKeyboardShortcutsTab.Key.Control")},
      {"⌥", BaseMessages.getString(PKG, "ConfigKeyboardShortcutsTab.Key.OptionAlt")},
      {"⌘", BaseMessages.getString(PKG, "ConfigKeyboardShortcutsTab.Key.Command")},
      {"⇧", BaseMessages.getString(PKG, "ConfigKeyboardShortcutsTab.Key.Shift")},
      {"⎋", BaseMessages.getString(PKG, "ConfigKeyboardShortcutsTab.Key.Escape")},
      {"⌫", BaseMessages.getString(PKG, "ConfigKeyboardShortcutsTab.Key.Delete")},
      {"↑", BaseMessages.getString(PKG, "ConfigKeyboardShortcutsTab.Key.UpArrow")},
      {"↓", BaseMessages.getString(PKG, "ConfigKeyboardShortcutsTab.Key.DownArrow")},
      {"←", BaseMessages.getString(PKG, "ConfigKeyboardShortcutsTab.Key.LeftArrow")},
      {"→", BaseMessages.getString(PKG, "ConfigKeyboardShortcutsTab.Key.RightArrow")}
    };

    Control lastRef = null;
    int itemsPerRow = 5;
    int currentItem = 0;

    for (String[] symbol : symbols) {
      Composite itemComposite = new Composite(refComposite, SWT.NONE);
      PropsUi.setLook(itemComposite);
      itemComposite.setLayout(new FormLayout());

      // Symbol badge
      Label symbolLabel = new Label(itemComposite, SWT.CENTER | SWT.BORDER);
      symbolLabel.setText(symbol[0]);
      symbolLabel.setBackground(keyBackgroundColor);
      symbolLabel.setForeground(itemComposite.getDisplay().getSystemColor(SWT.COLOR_DARK_GRAY));
      if (monoFont != null) {
        symbolLabel.setFont(monoFont);
      }

      FormData fdSymbol = new FormData();
      fdSymbol.left = new FormAttachment(0, 0);
      fdSymbol.top = new FormAttachment(0, 0);
      fdSymbol.width = 24;
      fdSymbol.height = 24;
      symbolLabel.setLayoutData(fdSymbol);

      // Description
      Label descLabel = new Label(itemComposite, SWT.LEFT);
      PropsUi.setLook(descLabel);
      descLabel.setText(symbol[1]);
      descLabel.setForeground(itemComposite.getDisplay().getSystemColor(SWT.COLOR_DARK_GRAY));

      FormData fdDesc = new FormData();
      fdDesc.left = new FormAttachment(symbolLabel, margin / 2);
      fdDesc.top = new FormAttachment(symbolLabel, 0, SWT.CENTER);
      fdDesc.right = new FormAttachment(100, 0);
      descLabel.setLayoutData(fdDesc);

      // Layout the item composite to compute its size
      itemComposite.layout(true, true);

      // Position the item composite
      FormData fdItem = new FormData();
      int column = currentItem % itemsPerRow;
      int row = currentItem / itemsPerRow;

      if (column == 0) {
        fdItem.left = new FormAttachment(0, 0);
      } else {
        fdItem.left = new FormAttachment((column * 100) / itemsPerRow, 0);
      }

      if (row == 0) {
        fdItem.top = new FormAttachment(0, 0);
      } else {
        // Find the item above this one
        fdItem.top = new FormAttachment(lastRef, margin / 2);
      }

      itemComposite.setLayoutData(fdItem);

      if (column == itemsPerRow - 1 || currentItem == symbols.length - 1) {
        lastRef = itemComposite;
      }

      currentItem++;
    }

    // Layout the reference composite and update its FormData with computed height
    refComposite.layout(true, true);
    fdRefComposite.height = refComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT).y;
    refComposite.setLayoutData(fdRefComposite);

    return refComposite;
  }
}
