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

package org.apache.hop.ui.hopgui.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.IUndo;
import org.apache.hop.core.gui.Point;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.perspective.execution.DragViewZoomBase;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;

/**
 * Drives a pipeline or workflow canvas with real mouse events, so the whole
 * mouseDown/mouseMove/mouseUp state machine of the graph runs unchanged.
 *
 * <p>The canvas is painted by hand rather than addressed through SWTBot's widget matchers: the
 * graph is a single custom-painted {@link Canvas}, so the only handle on a transform or action is
 * the clickable area the painter registers for it.
 */
public abstract class GraphCanvasTestBase extends SwtBotTestBase {

  /** Screen distance used to probe the canvas-to-graph coordinate scale. */
  private static final int SCALE_PROBE_PIXELS = 1000;

  /** How long the helpers wait for a paint, a dialog or a dialog closing. */
  private static final int POLL_ATTEMPTS = 40;

  private static final int POLL_MILLIS = 50;

  /**
   * The Hop GUI event loop logs and swallows whatever a mouse handler throws, which is why a broken
   * canvas shows no error dialog. Mirror that, and assert on the collected failures instead.
   */
  protected final List<Throwable> swallowed = new CopyOnWriteArrayList<>();

  /** Looks up the clickable area a graph registered at a graph coordinate. */
  @FunctionalInterface
  protected interface AreaLookup {
    AreaOwner at(int x, int y);
  }

  // ---------------------------------------------------------------- mouse events

  /**
   * Dispatches one mouse event on the canvas, converting the graph coordinate to a canvas pixel,
   * and waits for the handler to finish.
   */
  protected void fire(
      Canvas canvas, int type, double scale, Point graphPoint, int button, int stateMask) {
    Event event = mouseEvent(scale, graphPoint, button, stateMask);
    display.syncExec(() -> dispatch(canvas, type, event));
  }

  /**
   * Same as {@link #fire} but without waiting. Use it for an event that may open a dialog: a dialog
   * runs its own event loop and would otherwise block this worker inside the event dispatch.
   */
  protected void fireAsync(
      Canvas canvas, int type, double scale, Point graphPoint, int button, int stateMask) {
    Event event = mouseEvent(scale, graphPoint, button, stateMask);
    display.asyncExec(() -> dispatch(canvas, type, event));
  }

  private void dispatch(Canvas canvas, int type, Event event) {
    try {
      canvas.notifyListeners(type, event);
    } catch (Throwable t) {
      swallowed.add(t);
    }
  }

  private static Event mouseEvent(double scale, Point graphPoint, int button, int stateMask) {
    Event event = new Event();
    event.button = button;
    event.stateMask = stateMask;
    event.count = 1;
    event.x = (int) Math.round(graphPoint.x * scale);
    event.y = (int) Math.round(graphPoint.y * scale);
    return event;
  }

  // ---------------------------------------------------------------- the Hop GUI

  private static HopGui hopGui;

  /**
   * The Hop GUI the graphs under test hang off. Menus and toolbars are the only thing missing: they
   * are built when the real application shell opens, which a test never does. Call it on the UI
   * thread.
   */
  protected static synchronized HopGui hopGui() {
    if (hopGui == null) {
      hopGui = new MenulessHopGui();
      hopGui.setProps(PropsUi.getInstance());
    }
    return hopGui;
  }

  /**
   * A Hop GUI whose main menu was never built - it is created when the application shell opens,
   * which a test never does. Without this, refreshing the Edit menu after an undo entry throws,
   * which aborts the very canvas handler under test half way and would be read as a defect. The
   * graphs themselves are the real ones.
   */
  private static class MenulessHopGui extends HopGui {
    private final GuiMenuWidgets emptyMenu = new GuiMenuWidgets();

    @Override
    public GuiMenuWidgets getMainMenuWidgets() {
      return emptyMenu;
    }

    @Override
    public void setUndoMenu(IUndo undoInterface) {
      // reads the menu widgets field directly, so it needs its own no-op
    }

    @Override
    public void handleFileCapabilities(
        IHopFileType fileType,
        IHopFileTypeHandler handler,
        boolean changed,
        boolean running,
        boolean paused) {
      // reads the menu widgets field directly, so it needs its own no-op
    }
  }

  // ---------------------------------------------------------------- keyboard

  /**
   * Hooks the real Hop GUI key handler up to the test shell. The graph registered itself as a
   * shortcut target in its constructor, but the handler was attached to the application shell,
   * which is not the one holding the canvas here.
   */
  protected static void attachKeyboardShortcuts(Shell shell) {
    hopGui().replaceKeyboardShortcutListeners(shell, HopGuiKeyHandler.getInstance());
  }

  /** Presses a key on the canvas, e.g. {@code SWT.ESC}. */
  protected void fireKey(Canvas canvas, int keyCode) {
    Event event = new Event();
    event.keyCode = keyCode;
    event.character = (char) keyCode;
    event.doit = true;
    display.syncExec(() -> dispatch(canvas, SWT.KeyDown, event));
  }

  // ---------------------------------------------------------------- coordinates

  /**
   * Canvas pixels per graph unit. Derived through the public {@code screen2real} so the tests do
   * not have to know about zoom factors or high-DPI scaling.
   */
  protected double canvasToGraphScale(DragViewZoomBase graph) {
    Point origin = onUi(() -> graph.screen2real(0, 0));
    Point far = onUi(() -> graph.screen2real(SCALE_PROBE_PIXELS, 0));
    return (double) SCALE_PROBE_PIXELS / (far.x - origin.x);
  }

  protected static Point midpoint(Point from, Point to) {
    return new Point((from.x + to.x) / 2, (from.y + to.y) / 2);
  }

  /**
   * Waits for the first paint - the painter is what registers the clickable areas - and returns the
   * centre of the icon of {@code owner} in graph coordinates.
   */
  protected Point awaitIcon(
      SWTBot bot, AreaLookup lookup, AreaOwner.AreaType iconType, Object owner, Point location) {
    int iconSize = onUi(() -> PropsUi.getInstance().getIconSize());
    Point centre = new Point(location.x + iconSize / 2, location.y + iconSize / 2);
    for (int attempt = 0; attempt < POLL_ATTEMPTS; attempt++) {
      AreaOwner areaOwner = onUi(() -> lookup.at(centre.x, centre.y));
      if (areaOwner != null) {
        assertEquals(iconType, areaOwner.getAreaType(), "expected to aim at the icon");
        assertSame(owner, areaOwner.getOwner(), "expected to aim at " + owner);
        return centre;
      }
      bot.sleep(POLL_MILLIS);
    }
    throw new AssertionError("the canvas never painted the icon of " + owner);
  }

  /** Guards a test's own assumption that a spot really is empty canvas. */
  protected void assertEmptyCanvas(AreaLookup lookup, Point... points) {
    for (Point point : points) {
      assertTrue(
          onUi(() -> lookup.at(point.x, point.y)) == null, "expected empty canvas at " + point);
    }
  }

  // ---------------------------------------------------------------- dialogs

  protected Set<Shell> openShells() {
    return onUi(() -> new HashSet<>(Arrays.asList(display.getShells())));
  }

  /** Polls for a shell that was not open before, e.g. a context dialog. */
  protected Shell awaitNewShell(SWTBot bot, Set<Shell> before) {
    for (int attempt = 0; attempt < POLL_ATTEMPTS; attempt++) {
      Shell found =
          onUi(
              () -> {
                for (Shell shell : display.getShells()) {
                  if (!shell.isDisposed() && !before.contains(shell)) {
                    return shell;
                  }
                }
                return null;
              });
      if (found != null) {
        return found;
      }
      bot.sleep(POLL_MILLIS);
    }
    return null;
  }

  /** Closes a dialog that opened, so its event loop hands the UI thread back. */
  protected void closeShell(SWTBot bot, Shell shell) {
    if (shell == null) {
      return;
    }
    display.asyncExec(
        () -> {
          if (!shell.isDisposed()) {
            shell.close();
          }
        });
    for (int attempt = 0; attempt < POLL_ATTEMPTS && !onUi(shell::isDisposed); attempt++) {
      bot.sleep(POLL_MILLIS);
    }
    display.asyncExec(
        () -> {
          if (!shell.isDisposed()) {
            shell.dispose();
          }
        });
  }

  /** The title of a dialog that opened, or null when nothing opened. */
  protected String titleOf(Shell shell) {
    return shell == null ? null : onUi(shell::getText);
  }

  // ---------------------------------------------------------------- graph state

  /**
   * Asserts that every named field holds its expected idle value, reporting all offenders at once.
   * The graph keeps the state of a gesture in private fields; once the gesture is over they have to
   * be back to their initial value, or the next gesture starts from a half-finished one.
   */
  protected static void assertGraphState(Object graph, Map<String, Object> expected) {
    List<String> stale = new ArrayList<>();
    expected.forEach(
        (name, want) -> {
          Object actual = privateField(graph, name);
          if (want == null ? actual != null : !want.equals(actual)) {
            stale.add(name + " = " + describe(actual) + " (expected " + describe(want) + ")");
          }
        });
    assertTrue(stale.isEmpty(), "the canvas kept state from the finished gesture: " + stale);
  }

  private static String describe(Object value) {
    return value == null ? "null" : value.toString();
  }

  /** Reads a private field of the graph so a test can assert on the state of a gesture. */
  protected static Object privateField(Object target, String name) {
    for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
      try {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
      } catch (NoSuchFieldException e) {
        // keep walking up the hierarchy
      } catch (IllegalAccessException e) {
        throw new AssertionError("Could not read " + type.getSimpleName() + "." + name, e);
      }
    }
    throw new AssertionError("No field '" + name + "' on " + target.getClass());
  }

  // ---------------------------------------------------------------- UI thread

  /** Runs {@code supplier} on the UI thread and hands its result back to the SWTBot worker. */
  protected static <T> T onUi(Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    AtomicReference<RuntimeException> failure = new AtomicReference<>();
    display.syncExec(
        () -> {
          try {
            result.set(supplier.get());
          } catch (RuntimeException e) {
            failure.set(e);
          }
        });
    if (failure.get() != null) {
      throw failure.get();
    }
    return result.get();
  }

  /** Runs {@code runnable} on the UI thread and waits for it. */
  protected static void onUi(Runnable runnable) {
    onUi(
        () -> {
          runnable.run();
          return null;
        });
  }
}
