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
import org.apache.hop.core.security.ActionPermissionMapper;
import org.apache.hop.ui.core.widget.TextLineClipboard;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.util.EnvironmentUtils;
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
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
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

  private static HopGuiKeyHandler fallback;

  private static final ISingletonProvider PROVIDER = loadProvider();

  private static ISingletonProvider loadProvider() {
    try {
      return (ISingletonProvider) ImplementationLoader.newInstance(HopGuiKeyHandler.class);
    } catch (Throwable e) {
      // hop-ui unit tests have no rcp/rap *Impl on the classpath.
      return () -> {
        synchronized (HopGuiKeyHandler.class) {
          if (fallback == null) {
            fallback = new HopGuiKeyHandler();
          }
          return fallback;
        }
      };
    }
  }

  public Set<Object> parentObjects;

  /** Parent -> Control (e.g. shell) so we try that parent when focus is in its window. */
  private final Map<Object, Control> parentToControl = new HashMap<>();

  /** Shells this handler covers, used to also cover the widgets they create later on. */
  private final Set<Shell> handledShells = new HashSet<>();

  /**
   * Displays with a focus filter. Hop Web has one handler (and one display) per RAP UISession; the
   * desktop handler covers the single process display.
   */
  private final Set<Display> filteredDisplays = new HashSet<>();

  /**
   * Public no-arg constructor so RAP {@code SingletonUtil.getSessionInstance} can create a handler
   * per UISession. Call {@link #getInstance()} rather than constructing this yourself.
   */
  public HopGuiKeyHandler() {
    this.parentObjects = new HashSet<>();
  }

  public static HopGuiKeyHandler getInstance() {
    return (HopGuiKeyHandler) PROVIDER.getInstanceInternal();
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
    // The key filter covers every shell on this display, including dialogs that never register
    // here, so word-movement keys are not stolen and an empty Ctrl/Cmd+C/X copies or cuts the
    // current line (issue #8362).
    //
    if (display != null && !display.isDisposed() && filteredDisplays.add(display)) {
      display.addFilter(SWT.FocusIn, event -> attachTo(event.widget));
      display.addFilter(SWT.KeyDown, this::filterTextEditingKey);
      display.addListener(SWT.Dispose, e -> filteredDisplays.remove(display));
    }
  }

  /** Display filter: runs before widget listeners, for shells that never got this handler. */
  private void filterTextEditingKey(Event event) {
    try {
      if (applyTextEditingKey(
              event.widget, event.keyCode, event.stateMask, event.character, event.display)
          .consume) {
        event.doit = false;
      }
    } catch (SWTException e) {
      // The widget was disposed while the key was delivered.
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
   *
   * <p>Both ways of delegating count: overriding {@code addKeyListener()} and overriding the {@code
   * addListener()} that {@link Control#addKeyListener(KeyListener)} ends up calling. Widgets like
   * MetaSelectionLine and StyledTextComp only do the latter.
   */
  private static boolean delegatesKeyListeners(Control control) {
    if (!(control instanceof Composite)) {
      return false;
    }
    return DELEGATING_KEY_LISTENERS.computeIfAbsent(
        control.getClass(), HopGuiKeyHandler::isDelegatingClass);
  }

  private static Boolean isDelegatingClass(Class<?> widgetClass) {
    return isOverridden(widgetClass, Control.class, "addKeyListener", KeyListener.class)
        || isOverridden(widgetClass, Widget.class, "addListener", int.class, Listener.class);
  }

  /** Is the method of the given widget class declared below the class that normally declares it? */
  private static boolean isOverridden(
      Class<?> widgetClass, Class<?> declaringClass, String name, Class<?>... parameterTypes) {
    try {
      Method method = widgetClass.getMethod(name, parameterTypes);
      return !declaringClass.equals(method.getDeclaringClass());
    } catch (NoSuchMethodException e) {
      return false;
    }
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

    try {
      TextEditing textEditing =
          applyTextEditingKey(
              event.widget, event.keyCode, event.stateMask, event.character, event.display);
      if (textEditing.consume) {
        event.doit = false;
      }
      if (textEditing.stopShortcuts) {
        return;
      }
    } catch (SWTException e) {
      return;
    }

    // Same for tables, trees and lists: they use the arrow keys to move through their rows. Those
    // widgets also live inside the pipeline and workflow graph (log, preview and result tabs) where
    // the arrow keys navigate the canvas.
    if (isRowNavigationWidget(event.widget)
        && isCaretNavigationKey(event.keyCode, event.stateMask)) {
      return;
    }

    List<Object> orderedParents = getParentObjectsInContextOrder(event);
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
  private List<Object> getParentObjectsInContextOrder(KeyEvent event) {
    Object focusedWidget = event.widget;
    List<Object> inFocus = new ArrayList<>();
    List<Object> fallback = new ArrayList<>();
    for (Object parent : parentObjects) {
      Control control = parent instanceof Control c ? c : parentToControl.get(parent);
      if (control != null && !belongsToEventDisplay(control, event)) {
        continue;
      }
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
        if (!belongsToEventDisplay(control, event)) {
          return false;
        }
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

  /**
   * RAP forbids touching a widget whose Display belongs to another UISession. Skip those parents
   * instead of walking their widget tree.
   */
  private boolean belongsToEventDisplay(Control control, KeyEvent event) {
    if (control == null || control.isDisposed() || event == null) {
      return false;
    }
    try {
      Display controlDisplay = control.getDisplay();
      return event.display != null && event.display.equals(controlDisplay);
    } catch (SWTException e) {
      return false;
    }
  }

  private boolean isWidgetInControlHierarchy(Object widget, Control control) {
    if (!(widget instanceof Control) || control == null || control.isDisposed()) {
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
   * Keeps text editing in the widget that has the caret.
   *
   * <p>Horizontal Ctrl/Cmd/Alt+Left/Right stay with the widget (word movement on Windows and Linux,
   * Option+Left/Right on macOS, line edges for Command+Left/Right). They must not align or
   * distribute the graph. An empty Ctrl/Cmd+C or Ctrl/Cmd+X copies or cuts the current line on the
   * desktop; Hop Web does that in the browser, inside the key gesture. Other text keys (bare
   * arrows, Ctrl+A/V, typing) are not dispatched as shortcuts either. Ctrl+S and the vertical align
   * shortcuts still run.
   *
   * <p>{@code widgets.Event} and {@code events.KeyEvent} are not the same type, so callers pass the
   * fields. {@link TextEditing#stopShortcuts} means do not run a Hop shortcut. {@link
   * TextEditing#consume} means set {@code doit} false (the line was copied or cut here).
   */
  private TextEditing applyTextEditingKey(
      Widget widget, int keyCode, int stateMask, char character, Display display) {
    if (widget == null || widget.isDisposed()) {
      return TextEditing.PASS;
    }
    // The terminal handles every key itself, including word movement and copy.
    if (widget instanceof Control control && isInTerminalWidget(control)) {
      return TextEditing.PASS;
    }
    boolean textLike = isTextLikeWidget(widget);
    boolean webEditor = !textLike && isWebTextEditorFocused(display);
    if (!textLike && !webEditor) {
      return TextEditing.PASS;
    }
    if (isHorizontalWordKey(keyCode, stateMask)) {
      return TextEditing.STOP;
    }
    if (webEditor && isCopyOrCutKey(keyCode, stateMask)) {
      // Monaco already copied or cut. Do not also copy the graph, and do not cancel the key.
      return TextEditing.STOP;
    }
    if (!textLike || !isNativeTextEditingKey(keyCode, stateMask, character)) {
      return TextEditing.PASS;
    }
    // Hop Web copies the line in text-line-clipboard.js. A server clipboard write is outside the
    // key gesture and would also cut the line a second time if the script did not stop the event.
    if (!EnvironmentUtils.getInstance().isWeb()
        && isCopyOrCutKey(keyCode, stateMask)
        && TextLineClipboard.copyOrCutCurrentLine(widget, isCutKey(keyCode))) {
      return TextEditing.CONSUME;
    }
    return TextEditing.STOP;
  }

  /** Whether shortcut dispatch should stop, and whether the key event itself is consumed. */
  private static final class TextEditing {
    private static final TextEditing PASS = new TextEditing(false, false);
    private static final TextEditing STOP = new TextEditing(true, false);
    private static final TextEditing CONSUME = new TextEditing(true, true);

    private final boolean stopShortcuts;
    private final boolean consume;

    private TextEditing(boolean stopShortcuts, boolean consume) {
      this.stopShortcuts = stopShortcuts;
      this.consume = consume;
    }
  }

  private static boolean isWebTextEditorFocused(Display display) {
    try {
      if (display == null || display.isDisposed()) {
        return false;
      }
      return display.getData(HopGui.TEXT_EDITOR_FOCUS_DATA) != null;
    } catch (SWTException e) {
      return false;
    }
  }

  /**
   * Left/Right with Ctrl, Command or Alt, and optionally Shift. Ctrl+Alt is file navigation, not
   * word movement, so it is not included.
   */
  private static boolean isHorizontalWordKey(int keyCode, int stateMask) {
    int code = keyCode & SWT.KEY_MASK;
    if (code != SWT.ARROW_LEFT && code != SWT.ARROW_RIGHT) {
      return false;
    }
    boolean alt = (stateMask & SWT.ALT) != 0;
    boolean control = (stateMask & SWT.CONTROL) != 0;
    boolean command = (stateMask & SWT.COMMAND) != 0;
    if (alt && (control || command)) {
      return false;
    }
    return alt || control || command;
  }

  /** Ctrl/Cmd+C or Ctrl/Cmd+X with no Alt and no Shift. */
  private static boolean isCopyOrCutKey(int keyCode, int stateMask) {
    if ((stateMask & (SWT.ALT | SWT.SHIFT)) != 0) {
      return false;
    }
    if ((stateMask & (SWT.CONTROL | SWT.COMMAND)) == 0) {
      return false;
    }
    char key = Character.toLowerCase((char) keyCode);
    return key == 'c' || key == 'x';
  }

  private static boolean isCutKey(int keyCode) {
    return Character.toLowerCase((char) keyCode) == 'x';
  }

  /**
   * Keys that text-like widgets must handle themselves: copy/cut/paste/select-all,
   * delete/backspace, caret / selection navigation (arrows, home/end, page up/down) without
   * CTRL/CMD/ALT, and unmodified printable characters (including space).
   *
   * <p>Graph shortcuts such as Space (output fields) and {@code z} (open referenced object) must
   * not steal those keys from filter and search fields. App shortcuts with CTRL/CMD/ALT (e.g.
   * Ctrl+S) still run, except the horizontal word-movement keys handled above.
   */
  private static boolean isNativeTextEditingKey(int keyCode, int stateMask, char character) {
    if ((stateMask & (SWT.CONTROL | SWT.COMMAND)) != 0) {
      char key = Character.toLowerCase((char) keyCode);
      if (key == 'a' || key == 'c' || key == 'v' || key == 'x') {
        return true;
      }
    }
    if (keyCode == SWT.DEL || character == SWT.BS) {
      return true;
    }
    if (isCaretNavigationKey(keyCode, stateMask)) {
      return true;
    }
    return isUnmodifiedPrintableCharacter(keyCode, stateMask, character);
  }

  /**
   * Space, letters and punctuation with no CTRL/CMD/ALT. Shift may be held for capitals. SWT
   * reports space as {@link SWT#SPACE} and/or {@code character == ' '}.
   */
  private static boolean isUnmodifiedPrintableCharacter(
      int keyCode, int stateMask, char character) {
    if ((stateMask & (SWT.CONTROL | SWT.COMMAND | SWT.ALT)) != 0) {
      return false;
    }
    if (keyCode == SWT.SPACE || character == ' ') {
      return true;
    }
    return character >= 32 && character != SWT.DEL;
  }

  /**
   * Caret and row movement keys: the arrows, home/end and page up/down without CTRL/CMD/ALT. SHIFT
   * alone is allowed so extending a selection stays in the widget as well.
   */
  private static boolean isCaretNavigationKey(int keyCode, int stateMask) {
    if ((stateMask & (SWT.CONTROL | SWT.COMMAND | SWT.ALT)) != 0) {
      return false;
    }
    int code = keyCode & SWT.KEY_MASK;
    return code == SWT.ARROW_LEFT
        || code == SWT.ARROW_RIGHT
        || code == SWT.ARROW_UP
        || code == SWT.ARROW_DOWN
        || code == SWT.HOME
        || code == SWT.END
        || code == SWT.PAGE_UP
        || code == SWT.PAGE_DOWN;
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
