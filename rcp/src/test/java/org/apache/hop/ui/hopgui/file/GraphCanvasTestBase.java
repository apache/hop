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
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.IUndo;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
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
      // The assertion only quotes the exception; the trace is what tells where it came from.
      t.printStackTrace();
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

  /**
   * Waits for the painter to register an area of {@code type} that {@code matches}, and returns a
   * graph coordinate inside it that resolves to that very area - the centre when it can be, else a
   * corner: a badge sits on top of its transform, hop or note, and its centre may fall on the area
   * underneath.
   */
  protected Point awaitArea(
      SWTBot bot,
      Object graph,
      AreaLookup lookup,
      AreaOwner.AreaType type,
      Predicate<AreaOwner> matches) {
    for (int attempt = 0; attempt < POLL_ATTEMPTS; attempt++) {
      Point hit =
          onUi(
              () -> {
                @SuppressWarnings("unchecked")
                List<AreaOwner> areaOwners = (List<AreaOwner>) privateField(graph, "areaOwners");
                for (AreaOwner areaOwner : new ArrayList<>(areaOwners)) {
                  if (areaOwner.getAreaType() == type && matches.test(areaOwner)) {
                    return visiblePointOf(areaOwner, lookup);
                  }
                }
                return null;
              });
      if (hit != null) {
        return hit;
      }
      bot.sleep(POLL_MILLIS);
    }
    throw new AssertionError("the canvas never painted an area of type " + type);
  }

  private static Point visiblePointOf(AreaOwner areaOwner, AreaLookup lookup) {
    Rectangle area = areaOwner.getArea();
    int inset = 2;
    Point[] candidates = {
      new Point(area.x + area.width / 2, area.y + area.height / 2),
      new Point(area.x + inset, area.y + inset),
      new Point(area.x + area.width - inset, area.y + inset),
      new Point(area.x + inset, area.y + area.height - inset),
      new Point(area.x + area.width - inset, area.y + area.height - inset),
    };
    for (Point candidate : candidates) {
      if (lookup.at(candidate.x, candidate.y) == areaOwner) {
        return candidate;
      }
    }
    return null;
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

  /**
   * Presses and releases a button without waiting for the handlers, and reports every dialog that
   * opened. Both halves of the click are posted up front on purpose: the press may open a dialog,
   * and that dialog runs its own event loop, which is what dispatches the release. Waiting for the
   * press would deadlock the test, and holding the release back until the dialog is gone would hide
   * the very ordering under test.
   */
  protected List<String> clickAndCatchDialogs(
      SWTBot bot, Canvas canvas, double scale, Point at, int button, int stateMask) {
    return clickAndCatchDialogs(bot, canvas, scale, at, button, stateMask, () -> {});
  }

  /**
   * Like {@link #clickAndCatchDialogs(SWTBot, Canvas, double, Point, int, int)}, and runs {@code
   * landed} on this thread once the canvas has handled both halves of the click, before the wait
   * for dialogs starts. That wait takes a couple of seconds when nothing opens, which is too late
   * to look at what the click leaves behind only briefly, like a balloon on a timer.
   */
  protected List<String> clickAndCatchDialogs(
      SWTBot bot,
      Canvas canvas,
      double scale,
      Point at,
      int button,
      int stateMask,
      Runnable landed) {
    Set<Shell> before = openShells();
    fireAsync(canvas, SWT.MouseDown, scale, at, button, stateMask);
    fireAsync(canvas, SWT.MouseUp, scale, at, button, stateMask | buttonMask(button));
    awaitPostedEvents();
    landed.run();
    return catchDialogs(bot, before);
  }

  /**
   * A right click as the platform delivers it - the press, the request for a context menu and the
   * release - without waiting for the handlers, since the request may open a dialog. SWT sends the
   * {@code MenuDetect} between press and release on GTK and macOS and after the release on Windows;
   * the graphs do not depend on the order, and the tests use the former.
   */
  protected List<String> contextClickAndCatchDialogs(
      SWTBot bot, Canvas canvas, double scale, Point at) {
    return contextClickAndCatchDialogs(bot, canvas, scale, at, () -> {});
  }

  /**
   * Like {@link #contextClickAndCatchDialogs(SWTBot, Canvas, double, Point)}, with a {@code landed}
   * hook as in {@link #clickAndCatchDialogs(SWTBot, Canvas, double, Point, int, int, Runnable)}.
   */
  protected List<String> contextClickAndCatchDialogs(
      SWTBot bot, Canvas canvas, double scale, Point at, Runnable landed) {
    Set<Shell> before = openShells();
    fireAsync(canvas, SWT.MouseDown, scale, at, 3, SWT.NONE);
    fireMenuDetectAsync(canvas, scale, at);
    fireAsync(canvas, SWT.MouseUp, scale, at, 3, SWT.BUTTON3);
    awaitPostedEvents();
    landed.run();
    return catchDialogs(bot, before);
  }

  /**
   * Returns once every event posted so far has been dispatched. The display runs its queue in
   * order, so a round trip posted after the events comes back after their handlers ran; a dialog
   * one of them opened dispatches the round trip from its own loop, so this cannot deadlock.
   */
  private void awaitPostedEvents() {
    onUi(() -> {});
  }

  /**
   * {@code MenuDetect} carries display coordinates, so the graph point is mapped on the UI thread.
   */
  private void fireMenuDetectAsync(Canvas canvas, double scale, Point graphPoint) {
    display.asyncExec(
        () -> {
          org.eclipse.swt.graphics.Point onDisplay =
              canvas.toDisplay(
                  (int) Math.round(graphPoint.x * scale), (int) Math.round(graphPoint.y * scale));
          Event event = new Event();
          event.x = onDisplay.x;
          event.y = onDisplay.y;
          event.detail = SWT.MENU_MOUSE;
          event.doit = true;
          dispatch(canvas, SWT.MenuDetect, event);
        });
  }

  /** The state mask bit SWT sets for the button being released. */
  protected static int buttonMask(int button) {
    return switch (button) {
      case 2 -> SWT.BUTTON2;
      case 3 -> SWT.BUTTON3;
      default -> SWT.BUTTON1;
    };
  }

  /**
   * Collects the titles of the dialogs that opened since {@code before}, in the order they
   * appeared, and closes every one of them so the event loops underneath are handed back.
   *
   * <p>Dialogs both stack and follow one another here: a dialog runs its own event loop, so
   * anything that loop dispatches can open a second dialog on top of the first, while the code
   * after the first dialog can open yet another one once it is gone. So a round gathers everything
   * that is up at the same time, closes the newest first - an older one cannot return while a newer
   * loop sits on top of it - and then looks again for whatever that let through.
   *
   * <p>Closing a dialog is the answer a test wants: Hop dialogs treat it as cancel, so a question
   * like "replace this transform?" is answered with no.
   */
  protected List<String> catchDialogs(SWTBot bot, Set<Shell> before) {
    List<String> titles = new ArrayList<>();
    Set<Shell> seen = new HashSet<>(before);
    for (List<Shell> round = awaitNewShells(bot, seen);
        !round.isEmpty();
        round = awaitNewShells(bot, seen)) {
      round.forEach(popup -> titles.add(titleOf(popup)));
      for (int i = round.size() - 1; i >= 0; i--) {
        closeShell(bot, round.get(i));
      }
    }
    return titles;
  }

  /**
   * Every dialog that is open at the same time, in the order it appeared. Adds them to {@code
   * seen}.
   */
  private List<Shell> awaitNewShells(SWTBot bot, Set<Shell> seen) {
    List<Shell> found = new ArrayList<>();
    for (Shell popup = awaitNewShell(bot, seen); popup != null; popup = awaitNewShell(bot, seen)) {
      found.add(popup);
      seen.add(popup);
    }
    return found;
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
