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
import java.util.concurrent.ConcurrentHashMap;
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
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.swt.widgets.Widget;

public class HopGuiKeyHandler extends KeyAdapter {

  /** Data key marking the terminal widget, which handles all keys itself. */
  public static final String HOP_TERMINAL_WIDGET = "HOP_TERMINAL_WIDGET";

  /** Widget classes that pass their key listeners on to a widget inside them. */
  private static final Map<Class<?>, Boolean> DELEGATING_KEY_LISTENERS = new ConcurrentHashMap<>();

  private static HopGuiKeyHandler singleton;

  public Set<Object> parentObjects;

  /** Parent -> Control (e.g. shell) so we try that parent when focus is in its window. */
  private final Map<Object, Control> parentToControl = new HashMap<>();

  /** Shells this handler covers, used to also cover the widgets they create later on. */
  private final Set<Shell> handledShells = new HashSet<>();

  /**
   * Displays with a focus filter. This handler is a singleton for the whole process while Hop Web
   * has a display per session, so the filter is installed once per display.
   */
  private final Set<Display> filteredDisplays = new HashSet<>();

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

  /**
   * Start handling the keys of a shell, including those of the widgets it creates later on.
   *
   * <p>The handler is a key listener that is attached to every widget of a shell. Widgets created
   * after that, when a dialog or metadata editor rebuilds part of its content (e.g. the OAuth 2
   * fields of a REST connection when the authentication type changes), have no key listener of
   * their own, so keyboard shortcuts like Ctrl+S do nothing while such a widget has the focus.
   * {@link #attachTo(Widget)} handles those, called from {@code PropsUi.setLook()} when the widget
   * is created and, on the desktop, when a widget receives the focus.
   *
   * @param display the display to listen to for focus changes
   * @param shell the shell whose widgets are handled
   */
  public void addHandledShell(Display display, Shell shell) {
    if (shell == null || shell.isDisposed() || !handledShells.add(shell)) {
      return;
    }
    shell.addDisposeListener(e -> handledShells.remove(shell));

    // Safety net for widgets that are created without PropsUi.setLook(). Hop Web has no use for it:
    // RAP does not fire focus events for a focus change made in the browser.
    //
    if (display != null && !display.isDisposed() && filteredDisplays.add(display)) {
      display.addFilter(SWT.FocusIn, event -> attachTo(event.widget));
      display.addListener(SWT.Dispose, e -> filteredDisplays.remove(display));
    }
  }

  /**
   * Attach this handler to a widget of a shell we handle, so its keyboard shortcuts work.
   *
   * <p>This has to happen when the widget is created: RAP only sends the key events of a widget to
   * the server when that widget has a key listener at the time it is rendered, so attaching later
   * is too late for Hop Web.
   *
   * @param widget the widget to handle the keys of
   * @return true if the handler was attached to the widget
   */
  public boolean attachTo(Widget widget) {
    if (!(widget instanceof Control control) || control.isDisposed()) {
      return false;
    }
    try {
      if (!handledShells.contains(control.getShell())
          || isInTerminalWidget(control)
          || delegatesKeyListeners(control)) {
        return false;
      }
      // Removing first avoids handling the key twice for widgets that already have the handler
      control.removeKeyListener(this);
      control.addKeyListener(this);
      return true;
    } catch (SWTException e) {
      return false;
    }
  }

  /**
   * Composite widgets like TextVar and ComboVar pass the key listeners they get on to the widget
   * inside them. They style themselves before creating that widget, so it is not always there yet,
   * and it gets the handler through its own {@code PropsUi.setLook()} call anyway.
   */
  private static boolean delegatesKeyListeners(Control control) {
    if (!(control instanceof Composite)) {
      return false;
    }
    return DELEGATING_KEY_LISTENERS.computeIfAbsent(
        control.getClass(),
        widgetClass -> {
          try {
            Method method = widgetClass.getMethod("addKeyListener", KeyListener.class);
            return !Control.class.equals(method.getDeclaringClass());
          } catch (NoSuchMethodException e) {
            return Boolean.FALSE;
          }
        });
  }

  /** The terminal widget and everything in it handles all keys itself. */
  private static boolean isInTerminalWidget(Control control) {
    Control current = control;
    while (current != null) {
      if (current.getData(HOP_TERMINAL_WIDGET) == Boolean.TRUE) {
        return true;
      }
      current = current.getParent();
    }
    return false;
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

    // Same for tables, trees and lists: they use the arrow keys to move through their rows. Those
    // widgets also live inside the pipeline and workflow graph (log, preview and result tabs) where
    // the arrow keys navigate the canvas.
    if (isRowNavigationWidget(event.widget) && isCaretNavigationKey(event)) {
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
   * Returns true if the widget is a Table, Tree or List: they navigate rows with the arrow keys.
   */
  private static boolean isRowNavigationWidget(Widget widget) {
    return widget instanceof Table
        || widget instanceof Tree
        || widget instanceof TableItem
        || widget instanceof TreeItem
        || widget instanceof org.eclipse.swt.widgets.List;
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
    return isCaretNavigationKey(event);
  }

  /**
   * Caret and row movement keys: the arrows, home/end and page up/down without CTRL/CMD/ALT. SHIFT
   * alone is allowed so extending a selection stays in the widget as well.
   */
  private static boolean isCaretNavigationKey(KeyEvent event) {
    if ((event.stateMask & (SWT.CONTROL | SWT.COMMAND | SWT.ALT)) != 0) {
      return false;
    }
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
