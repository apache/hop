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
import org.apache.hop.core.security.ActionPermissionMapper;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Widget;

public class HopGuiKeyHandler extends KeyAdapter {

  private static HopGuiKeyHandler singleton;

  public Set<Object> parentObjects;

  /** Parent -> Control (e.g. shell) so we try that parent when focus is in its window. */
  private final Map<Object, Control> parentToControl = new HashMap<>();

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

    // Do not steal keys needed for native editing / caret movement inside text-like widgets.
    // StyledText is not available in RAP, so we check via reflection to avoid NoClassDefFoundError.
    // Bare ARROW_*/HOME/END would otherwise match canvas pan shortcuts (DragViewZoomBase) and break
    // caret navigation — especially in Hop Web where RAP CANCEL_KEYS can also block the browser
    // (see issue #7833). App shortcuts with CTRL/CMD/ALT (e.g. Ctrl+S, Ctrl+Arrow) still run.
    if (isTextLikeWidget(event.widget) && isNativeTextEditingKey(event)) {
      return;
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
    // On Mac (Hop Web), client sends Command as Ctrl in a synthetic event; treat as command for
    // osx shortcut matching.
    boolean effectiveCommand = command || (Const.isOSX() && shortcut.isCommand() && control);

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
    boolean controlMatch =
        shortcut.isControl() == control || (Const.isOSX() && shortcut.isCommand() && control);
    boolean commandMatch = shortcut.isCommand() == effectiveCommand;

    if (matchOS && keyMatch && altMatch && shiftMatch && controlMatch && commandMatch) {
      // Only invoke if this shortcut is linked to this class (context)
      if (shortcut.getParentClassName() != null
          && !shortcut.getParentClassName().equals(parentObject.getClass().getName())) {
        return false;
      }
      // RBAC: refuse shortcuts the current user is not allowed to perform (e.g. Ctrl+S for
      // read-only). Consume the key so the browser/OS does not fall through to a native action.
      String methodName = shortcut.getParentMethodName();
      if (!ActionPermissionMapper.allowsMethod(methodName)) {
        LogChannel.UI.logBasic("Keyboard shortcut blocked by security: method ''{0}''", methodName);
        return true;
      }
      try {
        Class<?> parentClass = parentObject.getClass();
        Method method = parentClass.getMethod(methodName);
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

  /**
   * Returns true if the widget is a Text, Combo, CCombo, or StyledText. StyledText is resolved via
   * reflection so it is not referenced when it is not on the classpath (e.g. in RAP/Hop Web).
   */
  private static boolean isTextLikeWidget(Widget widget) {
    if (widget == null) {
      return false;
    }
    return widget instanceof Text
        || widget instanceof Combo
        || widget instanceof CCombo
        || isStyledText(widget);
  }

  /**
   * Keys that text-like widgets must handle themselves: copy/cut/paste/select-all,
   * delete/backspace, and caret / selection navigation (arrows, home/end, page up/down) without
   * CTRL/CMD/ALT. Shift alone is allowed so Shift+Arrow selection stays in the widget.
   */
  private static boolean isNativeTextEditingKey(KeyEvent event) {
    if ((event.stateMask & (SWT.CONTROL | SWT.COMMAND)) != 0) {
      char key = Character.toLowerCase((char) event.keyCode);
      if (key == 'a' || key == 'c' || key == 'v' || key == 'x') {
        return true;
      }
    }
    if (event.keyCode == SWT.DEL || event.character == SWT.BS) {
      return true;
    }
    // Caret movement / selection: leave for the widget unless an app modifier is held
    boolean hasAppModifier = (event.stateMask & (SWT.CONTROL | SWT.COMMAND | SWT.ALT)) != 0;
    if (!hasAppModifier) {
      int keyCode = event.keyCode & SWT.KEY_MASK;
      return keyCode == SWT.ARROW_LEFT
          || keyCode == SWT.ARROW_RIGHT
          || keyCode == SWT.ARROW_UP
          || keyCode == SWT.ARROW_DOWN
          || keyCode == SWT.HOME
          || keyCode == SWT.END
          || keyCode == SWT.PAGE_UP
          || keyCode == SWT.PAGE_DOWN;
    }
    return false;
  }

  /**
   * Returns true if the widget is a StyledText. Uses reflection so that StyledText is not
   * referenced when it is not on the classpath (e.g. in RAP/Hop Web).
   */
  private static boolean isStyledText(Widget widget) {
    if (widget == null) {
      return false;
    }
    try {
      Class<?> st = Class.forName("org.eclipse.swt.custom.StyledText");
      return st.isInstance(widget);
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
