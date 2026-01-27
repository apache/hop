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

package org.apache.hop.ui.hopgui;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.KeyboardShortcut;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.application.AbstractEntryPoint;
import org.eclipse.rap.rwt.client.service.JavaScriptLoader;
import org.eclipse.rap.rwt.client.service.StartupParameters;
import org.eclipse.rap.rwt.service.ResourceManager;
import org.eclipse.rap.rwt.widgets.WidgetUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;

public class HopWebEntryPoint extends AbstractEntryPoint {

  @Override
  protected void createContents(Composite parent) {
    // So drill-down and other GUI checks can use Const.getHopPlatformRuntime() from any thread
    System.setProperty(Const.HOP_PLATFORM_RUNTIME, "GUI");
    ResourceManager resourceManager = RWT.getResourceManager();
    JavaScriptLoader jsLoader = RWT.getClient().getService(JavaScriptLoader.class);

    // Load canvas zoom handler
    String jsLocation = resourceManager.getLocation("js/canvas-zoom.js");
    jsLoader.require(jsLocation);

    // Configure keyboard shortcuts for RAP dynamically from annotations
    // ACTIVE_KEYS tells RAP to send these key combinations to the server
    // CANCEL_KEYS prevents the browser from handling these shortcuts
    // Note: CTRL automatically maps to Command key on Mac
    Display display = parent.getDisplay();
    String[] shortcuts = buildKeyboardShortcuts();
    display.setData(RWT.ACTIVE_KEYS, shortcuts);
    display.setData(RWT.CANCEL_KEYS, shortcuts);

    // Transferring Widget Data for client-side canvas drawing instructions
    WidgetUtil.registerDataKeys("props");
    WidgetUtil.registerDataKeys("mode");
    WidgetUtil.registerDataKeys("nodes");
    WidgetUtil.registerDataKeys("hops");
    WidgetUtil.registerDataKeys("notes");
    WidgetUtil.registerDataKeys("startHopNode");
    WidgetUtil.registerDataKeys("resizeDirection");

    //  The following options are session specific.
    //
    StartupParameters serviceParams = RWT.getClient().getService(StartupParameters.class);
    List<String> args = new ArrayList<>();
    String[] options = {"user", "pass", "file"};
    for (String option : options) {
      if (serviceParams.getParameter(option) != null) {
        args.add("-" + option + "=" + serviceParams.getParameter(option));
      }
    }

    HopGui.getInstance().setCommandLineArguments(args);
    HopGui.getInstance().setShell(parent.getShell());
    HopGui.getInstance().setProps(PropsUi.getInstance());
    try {
      ExtensionPointHandler.callExtensionPoint(
          HopGui.getInstance().getLog(),
          HopGui.getInstance().getVariables(),
          HopExtensionPoint.HopGuiInit.id,
          HopGui.getInstance());
    } catch (Exception e) {
      HopGui.getInstance()
          .getLog()
          .logError("Error calling extension point plugin(s) for HopGuiInit", e);
    }

    HopGui.getInstance().open();
  }

  /**
   * Build keyboard shortcuts for RAP from all @GuiKeyboardShortcut and @GuiOsxKeyboardShortcut
   * annotations
   *
   * @return Array of shortcut strings in RAP format (e.g., "CTRL+C", "ALT+F1")
   */
  private String[] buildKeyboardShortcuts() {
    Set<String> shortcuts = new HashSet<>();

    // Get all keyboard shortcuts from GuiRegistry
    GuiRegistry registry = GuiRegistry.getInstance();
    Map<String, List<KeyboardShortcut>> allShortcuts = registry.getAllKeyboardShortcuts();

    if (allShortcuts == null) {
      return new String[0];
    }

    // Convert each shortcut to RAP format
    for (Map.Entry<String, List<KeyboardShortcut>> entry : allShortcuts.entrySet()) {
      List<KeyboardShortcut> shortcutList = entry.getValue();
      if (shortcutList != null) {
        for (KeyboardShortcut shortcut : shortcutList) {
          String rapShortcut = convertToRapFormat(shortcut);
          if (rapShortcut != null && !rapShortcut.isEmpty()) {
            shortcuts.add(rapShortcut);
          }
        }
      }
    }

    return shortcuts.toArray(new String[0]);
  }

  /**
   * Convert a KeyboardShortcut to RAP format
   *
   * @param shortcut The keyboard shortcut to convert
   * @return RAP format string (e.g., "CTRL+C", "ALT+SHIFT+F1") or null if invalid
   */
  private String convertToRapFormat(KeyboardShortcut shortcut) {
    if (shortcut.getKeyCode() == 0) {
      return null;
    }

    StringBuilder sb = new StringBuilder();

    // Add modifiers in RAP order
    if (shortcut.isAlt()) {
      sb.append("ALT+");
    }
    if (shortcut.isControl() || shortcut.isCommand()) {
      // CTRL maps to Command on Mac automatically
      sb.append("CTRL+");
    }
    if (shortcut.isShift()) {
      sb.append("SHIFT+");
    }

    // Convert keyCode to character or special key name
    int keyCode = shortcut.getKeyCode();

    // Character keys (a-z, A-Z)
    if (keyCode >= 65 && keyCode <= 90) {
      sb.append((char) keyCode);
    } else if (keyCode >= 97 && keyCode <= 122) {
      sb.append(Character.toUpperCase((char) keyCode));
    }
    // Digit keys (0-9)
    else if (keyCode >= 48 && keyCode <= 57) {
      sb.append((char) keyCode);
    }
    // Special characters
    else if (keyCode == '+'
        || keyCode == '-'
        || keyCode == '*'
        || keyCode == '/'
        || keyCode == '=') {
      sb.append((char) keyCode);
    }
    // SWT special keys (have bit 24 set)
    else if ((keyCode & (1 << 24)) != 0) {
      String specialKey = convertSwtKeyToRap(keyCode & 0xFFFF);
      if (specialKey != null) {
        sb.append(specialKey);
      } else {
        return null; // Unknown special key
      }
    }
    // DEL key
    else if (keyCode == SWT.DEL || keyCode == 127) {
      sb.append("DEL");
    }
    // ESC key
    else if (keyCode == SWT.ESC || keyCode == 27) {
      sb.append("ESC");
    }
    // Space
    else if (keyCode == ' ' || keyCode == 32) {
      sb.append("SPACE");
    } else {
      // Unknown key code, skip it
      return null;
    }

    return sb.toString();
  }

  /**
   * Convert SWT special key codes to RAP format
   *
   * @param swtKey SWT key code (with bit 24 masked off)
   * @return RAP key name or null if not supported
   */
  private String convertSwtKeyToRap(int swtKey) {
    switch (swtKey) {
      case 1:
        return "ARROW_UP";
      case 2:
        return "ARROW_DOWN";
      case 3:
        return "ARROW_LEFT";
      case 4:
        return "ARROW_RIGHT";
      case 5:
        return "PAGE_UP";
      case 6:
        return "PAGE_DOWN";
      case 7:
        return "HOME";
      case 8:
        return "END";
      case 9:
        return "INSERT";
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
      default:
        return null;
    }
  }
}
