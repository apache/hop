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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.KeyboardShortcut;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.junit.jupiter.api.Test;

/**
 * Tests keyboard shortcut dispatching, in particular for shortcuts that a {@code @GuiPlugin} class
 * inherits from a super class. The canvas navigation keys (arrows, HOME, zoom) of the pipeline and
 * workflow graphs are declared on {@code DragViewZoomBase} and inherited, so they only work when
 * inherited shortcuts are dispatched.
 */
class HopGuiKeyHandlerTest {

  /** Stands in for DragViewZoomBase: declares the canvas navigation shortcut. */
  public static class NavigationBase {
    public int left;
    public int leftFast;

    @GuiKeyboardShortcut(key = SWT.ARROW_LEFT)
    @GuiOsxKeyboardShortcut(key = SWT.ARROW_LEFT)
    public void viewLeft() {
      left++;
    }

    @GuiKeyboardShortcut(shift = true, key = SWT.ARROW_LEFT)
    @GuiOsxKeyboardShortcut(shift = true, key = SWT.ARROW_LEFT)
    public void viewLeftFast() {
      leftFast++;
    }
  }

  /** Stands in for HopGuiPipelineGraph: the @GuiPlugin class that inherits the shortcuts. */
  public static class NavigationGraph extends NavigationBase {}

  @Test
  void shortcutInheritedFromSuperClassIsDispatched() {
    NavigationGraph graph = new NavigationGraph();
    registerShortcutsLikeHopGuiEnvironment(NavigationGraph.class);

    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(graph);
    try {
      keyHandler.keyPressed(canvasKey(SWT.ARROW_LEFT, SWT.NONE));
      assertEquals(1, graph.left, "Bare arrow key must reach the inherited canvas pan method");

      keyHandler.keyPressed(canvasKey(SWT.ARROW_LEFT, SWT.SHIFT));
      assertEquals(1, graph.leftFast, "Shift+arrow must reach the inherited large step method");
      assertEquals(1, graph.left, "Shift+arrow must not also trigger the small step");
    } finally {
      keyHandler.removeParentObjectToHandle(graph);
    }
  }

  @Test
  void arrowKeysAreLeftToTextWidgets() {
    NavigationGraph graph = new NavigationGraph();
    registerShortcutsLikeHopGuiEnvironment(NavigationGraph.class);

    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(graph);
    try {
      KeyEvent event = keyEvent(mock(Text.class), SWT.ARROW_LEFT, SWT.NONE);
      keyHandler.keyPressed(event);
      assertEquals(0, graph.left, "Arrow keys must stay in text widgets for caret movement");
    } finally {
      keyHandler.removeParentObjectToHandle(graph);
    }
  }

  /**
   * Pipeline graphs bind Space to "show output fields". That must not eat Space in a filter Text
   * (the palette tree search box).
   */
  public static class SpaceGraph {
    public int spaces;
    public int letters;

    @GuiKeyboardShortcut(key = ' ')
    @GuiOsxKeyboardShortcut(key = ' ')
    public void showOutputFields() {
      spaces++;
    }

    @GuiKeyboardShortcut(key = 'z')
    @GuiOsxKeyboardShortcut(key = 'z')
    public void openReferencedObject() {
      letters++;
    }
  }

  @Test
  void spaceAndLettersAreLeftToTextWidgets() {
    SpaceGraph graph = new SpaceGraph();
    registerShortcutsLikeHopGuiEnvironment(SpaceGraph.class);

    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(graph);
    try {
      KeyEvent spaceInText = keyEvent(mock(Text.class), SWT.SPACE, SWT.NONE);
      spaceInText.character = ' ';
      keyHandler.keyPressed(spaceInText);
      assertEquals(0, graph.spaces, "Space must type into text widgets, not run graph shortcuts");
      assertTrue(spaceInText.doit, "Space must not be consumed when a text widget has focus");

      KeyEvent letterInText = keyEvent(mock(Text.class), 'z', SWT.NONE);
      letterInText.character = 'z';
      keyHandler.keyPressed(letterInText);
      assertEquals(
          0, graph.letters, "Letters must type into text widgets, not run graph shortcuts");

      KeyEvent spaceOnCanvas = canvasKey(SWT.SPACE, SWT.NONE);
      spaceOnCanvas.character = ' ';
      keyHandler.keyPressed(spaceOnCanvas);
      assertEquals(1, graph.spaces, "Space on the canvas still runs the graph shortcut");
    } finally {
      keyHandler.removeParentObjectToHandle(graph);
    }
  }

  @Test
  void arrowKeysAreLeftToTablesAndTrees() {
    NavigationGraph graph = new NavigationGraph();
    registerShortcutsLikeHopGuiEnvironment(NavigationGraph.class);

    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(graph);
    try {
      // Tables and trees live inside the graph as well: log, preview and result tabs
      keyHandler.keyPressed(keyEvent(mock(Table.class), SWT.ARROW_LEFT, SWT.NONE));
      keyHandler.keyPressed(keyEvent(mock(Tree.class), SWT.ARROW_LEFT, SWT.NONE));
      keyHandler.keyPressed(keyEvent(mock(Table.class), SWT.ARROW_LEFT, SWT.SHIFT));
      assertEquals(0, graph.left, "Arrow keys must stay in tables and trees to move through rows");
      assertEquals(0, graph.leftFast, "Shift+arrow must stay in tables to extend the selection");
    } finally {
      keyHandler.removeParentObjectToHandle(graph);
    }
  }

  @Test
  void attachesToWidgetsCreatedAfterTheShellWasHandled() {
    // A metadata editor that rebuilds a section (e.g. the OAuth 2 fields of a REST connection)
    // creates widgets that never got the key listener, so Ctrl+S did nothing while they had focus.
    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    Shell shell = mock(Shell.class);
    Text lateCreatedWidget = mock(Text.class);
    when(lateCreatedWidget.getShell()).thenReturn(shell);

    assertFalse(
        keyHandler.attachTo(lateCreatedWidget),
        "Widgets of a shell we don't handle must be left alone");

    keyHandler.addHandledShell(null, shell);

    assertTrue(keyHandler.attachTo(lateCreatedWidget));
    verify(lateCreatedWidget).addKeyListener(keyHandler);
  }

  @Test
  void doesNotAttachToWidgetsThatPassKeyListenersOn() {
    // TextVar styles itself before creating the Text inside it and passes key listeners on to that
    // Text, so attaching while it is being built threw a NullPointerException. The Text inside it
    // is styled separately, which is where it gets the handler.
    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    Shell shell = mock(Shell.class);
    TextVar textVar = mock(TextVar.class);
    when(textVar.getShell()).thenReturn(shell);
    keyHandler.addHandledShell(null, shell);

    assertFalse(keyHandler.attachTo(textVar));
    verify(textVar, never()).addKeyListener(keyHandler);
  }

  @Test
  void doesNotAttachToTheTerminalWidget() {
    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    Shell shell = mock(Shell.class);
    Text terminalWidget = mock(Text.class);
    when(terminalWidget.getShell()).thenReturn(shell);
    when(terminalWidget.getData(HopGuiKeyHandler.HOP_TERMINAL_WIDGET)).thenReturn(Boolean.TRUE);
    keyHandler.addHandledShell(null, shell);

    assertFalse(keyHandler.attachTo(terminalWidget));
    verify(terminalWidget, never()).addKeyListener(keyHandler);
  }

  @Test
  void separateHandlersDoNotShareParentObjects() {
    HopGuiKeyHandler first = new HopGuiKeyHandler();
    HopGuiKeyHandler second = new HopGuiKeyHandler();
    Object parent = new Object();

    first.addParentObjectToHandle(parent);

    assertTrue(first.parentObjects.contains(parent));
    assertFalse(
        second.parentObjects.contains(parent),
        "Each RAP UISession must have its own key handler parent set");
  }

  @Test
  void pipelineGraphBindsArrowKeys() {
    assertGraphBindsArrowKeys(HopGuiPipelineGraph.class);
  }

  @Test
  void workflowGraphBindsArrowKeys() {
    assertGraphBindsArrowKeys(HopGuiWorkflowGraph.class);
  }

  private void assertGraphBindsArrowKeys(Class<?> graphClass) {
    registerShortcutsLikeHopGuiEnvironment(graphClass);
    List<KeyboardShortcut> shortcuts =
        GuiRegistry.getInstance().getKeyboardShortcuts(graphClass.getName());
    assertNotNull(shortcuts, "No keyboard shortcuts registered for " + graphClass.getName());

    for (int keyCode : new int[] {SWT.ARROW_LEFT, SWT.ARROW_RIGHT, SWT.ARROW_UP, SWT.ARROW_DOWN}) {
      for (boolean shift : new boolean[] {false, true}) {
        KeyboardShortcut shortcut = findArrowShortcut(shortcuts, keyCode, shift);
        assertNotNull(
            shortcut, "No " + (shift ? "shift+" : "") + "arrow key shortcut on " + graphClass);
        // This is what HopGuiKeyHandler checks before invoking the method
        assertEquals(
            graphClass.getName(),
            shortcut.getParentClassName(),
            "Shortcut must be linked to the graph class, not to the class declaring the method");
        try {
          assertNotNull(graphClass.getMethod(shortcut.getParentMethodName()));
        } catch (NoSuchMethodException e) {
          throw new AssertionError("Method not callable on " + graphClass, e);
        }
      }
    }
  }

  private KeyboardShortcut findArrowShortcut(
      List<KeyboardShortcut> shortcuts, int keyCode, boolean shift) {
    for (KeyboardShortcut shortcut : shortcuts) {
      if (shortcut.getKeyCode() == keyCode
          && shortcut.isShift() == shift
          && !shortcut.isAlt()
          && !shortcut.isControl()
          && !shortcut.isCommand()) {
        return shortcut;
      }
    }
    return null;
  }

  /** Register the shortcuts of a class the same way HopGuiEnvironment does at startup. */
  private void registerShortcutsLikeHopGuiEnvironment(Class<?> guiPluginClass) {
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.getKeyboardShortcuts(guiPluginClass.getName()) != null) {
      return; // The registry is a singleton: only register once per test run
    }
    for (Method method : findDeclaredMethods(guiPluginClass)) {
      GuiKeyboardShortcut shortcut = method.getAnnotation(GuiKeyboardShortcut.class);
      if (shortcut != null) {
        registry.addKeyboardShortcut(guiPluginClass.getName(), method, shortcut);
      }
      GuiOsxKeyboardShortcut osxShortcut = method.getAnnotation(GuiOsxKeyboardShortcut.class);
      if (osxShortcut != null) {
        registry.addKeyboardShortcut(guiPluginClass.getName(), method, osxShortcut);
      }
    }
  }

  private List<Method> findDeclaredMethods(Class<?> parentClass) {
    List<Method> methods = new ArrayList<>();
    Class<?> current = parentClass;
    while (current != null) {
      methods.addAll(List.of(current.getDeclaredMethods()));
      current = current.getSuperclass();
    }
    return methods;
  }

  private KeyEvent canvasKey(int keyCode, int stateMask) {
    return keyEvent(mock(Canvas.class), keyCode, stateMask);
  }

  private KeyEvent keyEvent(org.eclipse.swt.widgets.Widget widget, int keyCode, int stateMask) {
    Event event = new Event();
    event.widget = widget;
    event.keyCode = keyCode;
    event.stateMask = stateMask;
    event.doit = true;
    return new KeyEvent(event);
  }
}
