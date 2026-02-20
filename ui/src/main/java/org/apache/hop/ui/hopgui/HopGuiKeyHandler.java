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

package org.apache.hop.ui.hopgui;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.KeyboardShortcut;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Text;

public class HopGuiKeyHandler extends KeyAdapter {

  private static HopGuiKeyHandler singleton;

  public Set<Object> parentObjects;

  /** Parent -> Control (e.g. shell) so we try that parent when focus is in its window. */
  private final Map<Object, Control> parentToControl = new HashMap<>();

  private KeyboardShortcut lastShortcut;
  private long lastShortcutTime;

  private HopGuiKeyHandler() {
    this.parentObjects = new HashSet<>();
  }

  public static HopGuiKeyHandler getInstance() {
    if (singleton == null) {
      singleton = new HopGuiKeyHandler();
    }
    return singleton;
  }

  public void addParentObjectToHandle(Object parentObject) {
    parentObjects.add(parentObject);
  }

  /** Register parent with its window control so shortcuts in that window take precedence. */
  public void addParentObjectToHandle(Object parentObject, Control control) {
    parentObjects.add(parentObject);
    if (control != null) {
      parentToControl.put(parentObject, control);
    }
  }

  public void removeParentObjectToHandle(Object parentObject) {
    parentObjects.remove(parentObject);
    parentToControl.remove(parentObject);
  }

  @Override
  public void keyPressed(KeyEvent event) {
    if (!event.doit) {
      return;
    }

    // Ignore shortcuts inside Text, Combo, or StyledText widgets (including terminal)
    if (event.widget instanceof Text
        || event.widget instanceof Combo
        || event.widget instanceof CCombo
        || event.widget instanceof org.eclipse.swt.custom.StyledText) {
      // Ignore Copy/Cut/Paste/Select all - check both keyCode and character
      if ((event.stateMask & (SWT.CONTROL + SWT.COMMAND)) != 0) {
        char key = Character.toLowerCase((char) event.keyCode);
        if (key == 'a' || key == 'c' || key == 'v' || key == 'x') {
          return;
        }
      }
      // Ignore DEL and Backspace
      if (event.keyCode == SWT.DEL || event.character == SWT.BS) {
        return;
      }
    }

    List<Object> orderedParents = getParentObjectsInContextOrder(event.widget);
    for (Object parentObject : orderedParents) {
      List<KeyboardShortcut> shortcuts =
          GuiRegistry.getInstance().getKeyboardShortcuts(parentObject.getClass().getName());
      if (shortcuts != null) {
        for (KeyboardShortcut shortcut : shortcuts) {
          if (handleKey(parentObject, event, shortcut)) {
            event.doit = false;
            return;
          }
        }
      }
    }
  }

  /** Order: parents whose window has focus (closest first), then active perspectives, then rest. */
  private List<Object> getParentObjectsInContextOrder(Object focusedWidget) {
    List<Object> inFocus = new ArrayList<>();
    List<Object> fallback = new ArrayList<>();
    for (Object parent : parentObjects) {
      Control control = parent instanceof Control c ? c : parentToControl.get(parent);
      if (control != null && isWidgetInControlHierarchy(focusedWidget, control)) {
        inFocus.add(parent);
      } else {
        fallback.add(parent);
      }
    }
    inFocus.sort(
        Comparator.comparingInt(
            p -> {
              Control c = p instanceof Control x ? x : parentToControl.get(p);
              return c != null ? getDepthFromWidgetToControl(focusedWidget, c) : Integer.MAX_VALUE;
            }));
    fallback.sort(
        (a, b) -> {
          boolean aActive = isActivePerspective(a);
          boolean bActive = isActivePerspective(b);
          if (aActive && !bActive) return -1;
          if (!aActive && bActive) return 1;
          return 0;
        });
    List<Object> result = new ArrayList<>(inFocus);
    result.addAll(fallback);
    return result;
  }

  private boolean isActivePerspective(Object parent) {
    if (parent instanceof IHopPerspective perspective) {
      try {
        return perspective.isActive();
      } catch (Exception e) {
        return false;
      }
    }
    return false;
  }

  /** Depth from widget to control (1 = direct parent). */
  private int getDepthFromWidgetToControl(Object widget, Control control) {
    if (!(widget instanceof Control)) {
      return Integer.MAX_VALUE;
    }
    int depth = 0;
    Control current = (Control) widget;
    while (current != null) {
      if (current == control) {
        return depth;
      }
      depth++;
      try {
        current = current.getParent();
      } catch (Exception e) {
        return Integer.MAX_VALUE;
      }
    }
    return Integer.MAX_VALUE;
  }

  private boolean isParentInContext(
      Object parentObject, KeyEvent event, KeyboardShortcut shortcut) {
    if (parentObject instanceof Control control) {
      try {
        if (!control.isVisible()) {
          return shortcut.isGlobal();
        }
        return shortcut.isGlobal() || isWidgetInControlHierarchy(event.widget, control);
      } catch (SWTException e) {
        return false;
      }
    }
    if (parentObject instanceof IHopPerspective perspective) {
      try {
        return perspective.isActive() || shortcut.isGlobal();
      } catch (Exception e) {
        return false;
      }
    }
    return true;
  }

  private boolean handleKey(Object parentObject, KeyEvent event, KeyboardShortcut shortcut) {
    if (!isParentInContext(parentObject, event, shortcut)) {
      return false;
    }

    int keyCode = (event.keyCode & SWT.KEY_MASK);

    boolean alt = (event.stateMask & SWT.ALT) != 0;
    boolean shift = (event.stateMask & SWT.SHIFT) != 0;
    boolean control = (event.stateMask & SWT.CONTROL) != 0;
    boolean command = (event.stateMask & SWT.COMMAND) != 0;

    boolean matchOS = Const.isOSX() == shortcut.isOsx();

    if (keyCode == SWT.KEYPAD_ADD) keyCode = '+';
    else if (keyCode == SWT.KEYPAD_SUBTRACT) keyCode = '-';
    else if (keyCode == SWT.KEYPAD_MULTIPLY) keyCode = '*';
    else if (keyCode == SWT.KEYPAD_DIVIDE) keyCode = '/';
    else if (keyCode == SWT.KEYPAD_EQUAL) keyCode = '=';
    // Backtick: in SWT use event.character ('`' = 96); keyCode may be 0 or 192 (VK_OEM_3) on some
    // platforms
    else if (keyCode == 192) keyCode = '`';

    int shortcutKey = shortcut.getKeyCode();
    // Match by keyCode, or by event.character (SWT maps backtick/grave accent to event.character)
    boolean keyMatch =
        keyCode == shortcutKey || (shortcutKey != 0 && event.character == shortcutKey);
    boolean altMatch = shortcut.isAlt() == alt;
    boolean shiftMatch = shortcut.isShift() == shift;
    boolean controlMatch = shortcut.isControl() == control;
    boolean commandMatch = shortcut.isCommand() == command;

    if (matchOS && keyMatch && altMatch && shiftMatch && controlMatch && commandMatch) {
      // Only invoke if this shortcut is linked to this class (context)
      if (shortcut.getParentClassName() != null
          && !shortcut.getParentClassName().equals(parentObject.getClass().getName())) {
        return false;
      }
      try {
        Class<?> parentClass = parentObject.getClass();
        Method method = parentClass.getMethod(shortcut.getParentMethodName());
        if (method != null) {
          method.invoke(parentObject);
          return true;
        }
      } catch (Exception ex) {
        LogChannel.UI.logError(
            "Error calling keyboard shortcut method on parent object " + parentObject.toString(),
            ex);
      }
    }
    return false;
  }

  private boolean isWidgetInControlHierarchy(Object widget, Control control) {
    if (!(widget instanceof Control)) {
      return false;
    }

    Control current = (Control) widget;
    while (current != null) {
      if (current == control) {
        return true;
      }
      try {
        current = current.getParent();
      } catch (Exception e) {
        return false;
      }
    }
    return false;
  }
}
