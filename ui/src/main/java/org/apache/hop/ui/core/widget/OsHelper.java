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

package org.apache.hop.ui.core.widget;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;

public class OsHelper {

  public static final String WS_WIN32 = "win32";
  public static final String WS_MOTIF = "motif";
  public static final String WS_GTK = "gtk";
  public static final String WS_PHOTON = "photon";
  public static final String WS_CARBON = "carbon";
  public static final String WS_COCOA = "cocoa";
  public static final String WS_WPF = "wpf";
  public static final String WS_UNKNOWN = "unknown";

  /** NSMenuItemImageVisibilityVisible in the macOS 27 SDK (NSMenuItem.h). */
  private static final long NS_MENU_ITEM_IMAGE_VISIBILITY_VISIBLE = 1L;

  private static boolean menuItemImageVisibilityUnsupported;
  private static Field nsItemField;
  private static Method objcMsgSend;
  private static Long setPreferredImageVisibility;

  public static final boolean isWindows() {
    final String ws = SWT.getPlatform();
    return WS_WIN32.equals(ws) || WS_WPF.equals(ws);
  }

  public static final boolean isMac() {
    final String ws = SWT.getPlatform();
    return WS_CARBON.equals(ws) || WS_COCOA.equals(ws);
  }

  public static String customizeMenuitemText(String txt) {
    if (!isMac()) {
      return txt;
    }

    String[] parts = txt.split("\t");
    if (parts.length <= 1) {
      return txt;
    }

    List<String> items = new ArrayList<>();
    items.addAll(Arrays.asList(parts));

    String key = items.remove(items.size() - 1);
    key = key.toUpperCase().replaceAll("CTRL", "\u2318");
    key = key.toUpperCase().replaceAll("SHIFT", "\u21E7");
    key = key.toUpperCase().replaceAll("ALT", "\u2325");
    key = key.toUpperCase().replaceAll("ESC", "\u238B");
    key = key.toUpperCase().replaceAll("DEL", "\u2326");
    key = key.toUpperCase().replaceAll("UP", "\u2191");
    key = key.toUpperCase().replaceAll("DOWN", "\u2193");
    key = key.toUpperCase().replaceAll("LEFT", "\u2190");
    key = key.toUpperCase().replaceAll("RIGHT", "\u2192");

    key = key.replaceAll("-", "");
    key = key.replaceAll("\\+", "");

    // please note, the resulting string will be something like:
    // "Select All\t \u2318A"
    // ^ this variables is important so the menu does not get bound to a window
    // global accelerator
    // It's a workaround for apparently randomly enabled/disabled menu
    // items. In fact, they are just kept in synch
    // with global accelerators
    return StringUtils.join(items, "\t") + "\t " + key;
  }

  public static boolean setAppName() {

    if (isMac()) {
      // Sets the app name in main menu (so it works even when launching
      // from shell script)
      String appName = "Hop";
      Display.setAppName(appName);
    }

    return true;
  }

  // Safe: the stack trace goes to the local stderr only, never to a remote client
  @SuppressWarnings("java:S4507")
  public static void initOsHandlers(Display display) {

    // handle OpenDocument
    display.addListener(SWT.OpenDocument, event -> HopGui.getInstance().fileDelegate.fileOpen());

    // Handle Shell close i.e. CMD+Q on Mac, for example
    display.addListener(
        SWT.Close,
        event -> {
          try {
            HopGui.getInstance().menuFileExit();
          } catch (Exception e) {
            e.printStackTrace();
          }
        });

    // hook into the system menu on mac
    if (isMac()) {

      Menu m = display.getSystemMenu();
      MenuItem[] items = m.getItems();

      for (MenuItem item : items) {

        switch (item.getID()) {
          case SWT.ID_ABOUT:
            item.addListener(SWT.Selection, event -> HopGui.getInstance().menuHelpAbout());

            break;
          case SWT.ID_PREFERENCES:
            // The perspective is absent when it is switched off in disabledGuiElements.xml.
            //
            item.addListener(
                SWT.Selection,
                event -> {
                  ConfigurationPerspective perspective = HopGui.getConfigurationPerspective();
                  if (perspective != null) {
                    perspective.activate();
                  }
                });

            break;
          default:
            break;
        }
      }

      // macOS 27 hides menu item images unless the item opts in, so opt in every item of a
      // menu right before it opens.
      //
      display.addFilter(
          SWT.Show,
          event -> {
            if (event.widget instanceof Menu menu) {
              for (MenuItem item : menu.getItems()) {
                showMenuItemImage(item);
              }
            }
          });
    }
  }

  /**
   * Sets NSMenuItem.preferredImageVisibility to visible. Without it, macOS 27 hides the images of
   * menu items in applications linked against the macOS 27 SDK. SWT does not expose the property
   * yet and the Cocoa classes only exist in the macOS SWT jar, so this goes through reflection. On
   * older macOS versions the item does not respond to the selector and nothing happens.
   */
  private static void showMenuItemImage(MenuItem item) {
    if (menuItemImageVisibilityUnsupported || item.isDisposed()) {
      return;
    }
    try {
      if (setPreferredImageVisibility == null) {
        nsItemField = MenuItem.class.getDeclaredField("nsItem");
        nsItemField.setAccessible(true);
        Class<?> osClass = Class.forName("org.eclipse.swt.internal.cocoa.OS");
        objcMsgSend = osClass.getMethod("objc_msgSend", long.class, long.class, long.class);
        setPreferredImageVisibility =
            (Long)
                osClass
                    .getMethod("sel_registerName", String.class)
                    .invoke(null, "setPreferredImageVisibility:");
      }
      Object nsItem = nsItemField.get(item);
      if (nsItem == null) {
        return;
      }
      boolean supported =
          (Boolean)
              nsItem
                  .getClass()
                  .getMethod("respondsToSelector", long.class)
                  .invoke(nsItem, setPreferredImageVisibility);
      if (!supported) {
        menuItemImageVisibilityUnsupported = true;
        return;
      }
      long id = nsItem.getClass().getField("id").getLong(nsItem);
      objcMsgSend.invoke(
          null, id, setPreferredImageVisibility, NS_MENU_ITEM_IMAGE_VISIBILITY_VISIBLE);
    } catch (Exception e) {
      // Not running on Cocoa SWT, or SWT internals changed: keep the default visibility.
      menuItemImageVisibilityUnsupported = true;
    }
  }
}
