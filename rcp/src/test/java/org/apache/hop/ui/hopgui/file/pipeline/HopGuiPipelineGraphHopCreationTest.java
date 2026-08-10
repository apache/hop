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

package org.apache.hop.ui.hopgui.file.pipeline;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.hopgui.file.GraphCanvasTestBase;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Every way a user can draw a hop on the pipeline canvas, and the ways to abandon one. Each test
 * replays the gesture as real mouse events and then checks three things: the hop is there (or is
 * not), no dialog appeared that should not have, and the canvas is back in its idle state so the
 * next gesture does not start half-finished.
 *
 * <p>Covers <a href="https://github.com/apache/hop/issues/7768">issue #7768</a>.
 */
@Tag("uitest")
class HopGuiPipelineGraphHopCreationTest extends GraphCanvasTestBase {

  private static final String SOURCE_TRANSFORM = "Query Delete";
  private static final String TARGET_TRANSFORM = "Write to log";
  private static final String OTHER_INPUT_TRANSFORM = "Second input";
  private static final Point SOURCE_LOCATION = new Point(60, 60);
  private static final Point TARGET_LOCATION = new Point(280, 60);
  private static final Point OTHER_INPUT_LOCATION = new Point(500, 60);

  @BeforeAll
  static void registerGuiPlugins() throws HopException {
    // A context dialog bails out when the GUI registry holds no actions for the context, so
    // register the GUI plugins to get the same dialogs the user sees.
    HopGuiEnvironment.init();
  }

  // ------------------------------------------------------------------ create hop from the menu

  @Test
  void createHopActionThenClickingTheTargetCreatesTheHop() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);

          clickTransformAndPickCreateHop(graph, spots, source);

          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.target, 0, SWT.NONE);
          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.target, 1, SWT.NONE);
          String popup = releaseAndCatchDialog(bot, spots, spots.target, SWT.BUTTON1);

          assertAll(
              () -> assertHopExists(pipelineMeta, source, target),
              () -> assertNoDialog(popup),
              () -> assertNoFailures(),
              () -> assertCanvasIsIdle(graph));
        });
  }

  @Test
  void createHopActionThenClickingEmptyCanvasCancelsTheHop() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);
          Point sourceWas = new Point(source.getLocation());

          clickTransformAndPickCreateHop(graph, spots, source);

          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyA, 0, SWT.NONE);
          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.emptyA, 1, SWT.NONE);
          String popup = releaseAndCatchDialog(bot, spots, spots.emptyA, SWT.BUTTON1);

          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyB, 0, SWT.NONE);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyC, 0, SWT.NONE);

          assertAll(
              () -> assertNoHop(pipelineMeta, source, target),
              () -> assertNoDialog(popup),
              () -> assertNoFailures(),
              () -> assertDidNotMove(sourceWas, source),
              () -> assertCanvasIsIdle(graph));
        });
  }

  // ------------------------------------------------------------------ shift click, shift click

  @Test
  void shiftClickingSourceAndTargetCreatesTheHop() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);

          // SHIFT stays down for the whole gesture.
          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 1, SWT.SHIFT);
          fire(spots.canvas, SWT.MouseUp, spots.scale, spots.source, 1, SWT.SHIFT | SWT.BUTTON1);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.target, 0, SWT.SHIFT);
          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.target, 1, SWT.SHIFT);
          String popup = releaseAndCatchDialog(bot, spots, spots.target, SWT.SHIFT | SWT.BUTTON1);

          assertAll(
              () -> assertHopExists(pipelineMeta, source, target),
              () -> assertNoDialog(popup),
              () -> assertNoFailures(),
              () -> assertCanvasIsIdle(graph));
        });
  }

  // ------------------------------------------------------------------ shift click, plain click

  @Test
  void shiftClickingSourceThenPlainClickingTargetCreatesTheHop() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);

          // SHIFT is released after starting the hop.
          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 1, SWT.SHIFT);
          fire(spots.canvas, SWT.MouseUp, spots.scale, spots.source, 1, SWT.SHIFT | SWT.BUTTON1);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.target, 0, SWT.NONE);
          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.target, 1, SWT.NONE);
          String popup = releaseAndCatchDialog(bot, spots, spots.target, SWT.BUTTON1);

          assertAll(
              () -> assertHopExists(pipelineMeta, source, target),
              () -> assertNoDialog(popup),
              () -> assertNoFailures(),
              () -> assertCanvasIsIdle(graph));
        });
  }

  @Test
  void shiftClickingSourceThenClickingEmptyCanvasCancelsTheHop() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);
          Point sourceWas = new Point(source.getLocation());

          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 1, SWT.SHIFT);
          fire(spots.canvas, SWT.MouseUp, spots.scale, spots.source, 1, SWT.SHIFT | SWT.BUTTON1);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyA, 0, SWT.NONE);
          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.emptyA, 1, SWT.NONE);
          String popup = releaseAndCatchDialog(bot, spots, spots.emptyA, SWT.BUTTON1);

          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyB, 0, SWT.NONE);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyC, 0, SWT.NONE);

          assertAll(
              () -> assertNoHop(pipelineMeta, source, target),
              () -> assertNoDialog(popup),
              () -> assertNoFailures(),
              () -> assertDidNotMove(sourceWas, source),
              () -> assertCanvasIsIdle(graph));
        });
  }

  // ------------------------------------------------------------------ shift drag

  @Test
  void shiftDraggingOntoTheTargetCreatesTheHop() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);

          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 1, SWT.SHIFT);
          fire(
              spots.canvas,
              SWT.MouseMove,
              spots.scale,
              midpoint(spots.source, spots.target),
              0,
              SWT.SHIFT | SWT.BUTTON1);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.target, 0, SWT.SHIFT | SWT.BUTTON1);
          String popup = releaseAndCatchDialog(bot, spots, spots.target, SWT.SHIFT | SWT.BUTTON1);

          assertAll(
              () -> assertHopExists(pipelineMeta, source, target),
              () -> assertNoDialog(popup),
              () -> assertNoFailures(),
              () -> assertCanvasIsIdle(graph));
        });
  }

  /** Issue #7768: the drag is abandoned over empty canvas. */
  @Test
  void shiftDraggingOntoEmptyCanvasCancelsTheHop() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);
          Point sourceWas = new Point(source.getLocation());

          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 1, SWT.SHIFT);
          fire(
              spots.canvas,
              SWT.MouseMove,
              spots.scale,
              midpoint(spots.source, spots.emptyA),
              0,
              SWT.SHIFT | SWT.BUTTON1);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyA, 0, SWT.SHIFT | SWT.BUTTON1);
          fire(spots.canvas, SWT.MouseUp, spots.scale, spots.emptyA, 1, SWT.SHIFT | SWT.BUTTON1);

          Object pendingHop = privateField(graph, "startHopTransform");

          // Click somewhere else and move on, as in the report. Once the release above has
          // cancelled the hop this is an ordinary click on empty canvas, so the dialog offering to
          // create something here is expected - it is dismissed again right away.
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyB, 0, SWT.NONE);
          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.emptyB, 1, SWT.NONE);
          releaseAndCatchDialog(bot, spots, spots.emptyB, SWT.BUTTON1);
          fire(
              spots.canvas, SWT.MouseMove, spots.scale, midpoint(spots.emptyB, spots.emptyC), 0, 0);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyC, 0, SWT.NONE);

          assertAll(
              () ->
                  assertNull(
                      pendingHop,
                      "releasing over empty canvas must cancel the pending hop right away"),
              () -> assertNoHop(pipelineMeta, source, target),
              () -> assertNoFailures(),
              () -> assertDidNotMove(sourceWas, source),
              () -> assertCanvasIsIdle(graph));
        });
  }

  // ------------------------------------------------------------------ middle button

  @Test
  void middleDraggingOntoTheTargetCreatesTheHop() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);

          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 2, SWT.NONE);
          fire(
              spots.canvas,
              SWT.MouseMove,
              spots.scale,
              midpoint(spots.source, spots.target),
              0,
              SWT.BUTTON2);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.target, 0, SWT.BUTTON2);
          String popup = releaseAndCatchDialog(bot, spots, spots.target, 2, SWT.BUTTON2);

          assertAll(
              () -> assertHopExists(pipelineMeta, source, target),
              () -> assertNoDialog(popup),
              () -> assertNoFailures(),
              () -> assertCanvasIsIdle(graph));
        });
  }

  @Test
  void middleDraggingOntoEmptyCanvasCancelsTheHop() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);
          Point sourceWas = new Point(source.getLocation());

          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 2, SWT.NONE);
          fire(
              spots.canvas,
              SWT.MouseMove,
              spots.scale,
              midpoint(spots.source, spots.emptyA),
              0,
              SWT.BUTTON2);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyA, 0, SWT.BUTTON2);
          fire(spots.canvas, SWT.MouseUp, spots.scale, spots.emptyA, 2, SWT.BUTTON2);
          Object pendingHop = privateField(graph, "startHopTransform");

          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyB, 0, SWT.NONE);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyC, 0, SWT.NONE);

          assertAll(
              () ->
                  assertNull(
                      pendingHop,
                      "releasing over empty canvas must cancel the pending hop right away"),
              () -> assertNoHop(pipelineMeta, source, target),
              () -> assertNoFailures(),
              () -> assertDidNotMove(sourceWas, source),
              () -> assertCanvasIsIdle(graph));
        });
  }

  @Test
  void middleClickingSourceThenClickingTargetCreatesTheHop() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);

          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 2, SWT.NONE);
          fire(spots.canvas, SWT.MouseUp, spots.scale, spots.source, 2, SWT.BUTTON2);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.target, 0, SWT.NONE);
          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.target, 1, SWT.NONE);
          String popup = releaseAndCatchDialog(bot, spots, spots.target, SWT.BUTTON1);

          assertAll(
              () -> assertHopExists(pipelineMeta, source, target),
              () -> assertNoDialog(popup),
              () -> assertNoFailures(),
              () -> assertCanvasIsIdle(graph));
        });
  }

  @Test
  void middleClickingSourceThenClickingEmptyCanvasCancelsTheHop() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);
          Point sourceWas = new Point(source.getLocation());

          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 2, SWT.NONE);
          fire(spots.canvas, SWT.MouseUp, spots.scale, spots.source, 2, SWT.BUTTON2);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyA, 0, SWT.NONE);
          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.emptyA, 1, SWT.NONE);
          String popup = releaseAndCatchDialog(bot, spots, spots.emptyA, SWT.BUTTON1);

          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyB, 0, SWT.NONE);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyC, 0, SWT.NONE);

          assertAll(
              () -> assertNoHop(pipelineMeta, source, target),
              () -> assertNoDialog(popup),
              () -> assertNoFailures(),
              () -> assertDidNotMove(sourceWas, source),
              () -> assertCanvasIsIdle(graph));
        });
  }

  // ------------------------------------------------------------------ escape and right click

  @Test
  void escapeCancelsThePendingHop() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);
          Point sourceWas = new Point(source.getLocation());

          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 1, SWT.SHIFT);
          fire(spots.canvas, SWT.MouseUp, spots.scale, spots.source, 1, SWT.SHIFT | SWT.BUTTON1);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyA, 0, SWT.NONE);
          assertNotNull(
              privateField(graph, "startHopTransform"), "the hop should be pending by now");

          fireKey(spots.canvas, SWT.ESC);
          Object pendingHop = privateField(graph, "startHopTransform");

          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyB, 0, SWT.NONE);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyC, 0, SWT.NONE);

          assertAll(
              () -> assertNull(pendingHop, "escape must cancel the pending hop"),
              () -> assertNoHop(pipelineMeta, source, target),
              () -> assertNoFailures(),
              () -> assertDidNotMove(sourceWas, source),
              () -> assertCanvasIsIdle(graph));
        });
  }

  @Test
  void rightClickingEmptyCanvasCancelsThePendingHop() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);
          Point sourceWas = new Point(source.getLocation());

          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 1, SWT.SHIFT);
          fire(spots.canvas, SWT.MouseUp, spots.scale, spots.source, 1, SWT.SHIFT | SWT.BUTTON1);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyA, 0, SWT.NONE);

          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.emptyA, 3, SWT.NONE);
          String popup = releaseAndCatchDialog(bot, spots, spots.emptyA, 3, SWT.BUTTON3);

          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyB, 0, SWT.NONE);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.emptyC, 0, SWT.NONE);

          assertAll(
              () -> assertNoHop(pipelineMeta, source, target),
              () -> assertNoDialog(popup),
              () -> assertNoFailures(),
              () -> assertDidNotMove(sourceWas, source),
              () -> assertCanvasIsIdle(graph));
        });
  }

  // ------------------------------------------------- target reading another row layout already

  /**
   * The target already reads from a transform that sends a different row layout, so completing the
   * hop runs the layout check, which reports the mismatch and offers to merge the streams. Those
   * two are the whole story: the click drew a hop, so the context dialog of the transform it landed
   * on must not appear on top of them.
   *
   * <p>The check runs from the mouseDown that completes the hop, and its dialogs run their own
   * event loop - which is what dispatches the release of that same click, long before mouseDown is
   * done with it. Getting that release ignored is the point here.
   */
  @Test
  void createHopActionThenClickingATargetMixingRowLayoutsOnlyRunsTheLayoutCheck() {
    onCanvas(
        HopGuiPipelineGraphHopCreationTest::buildPipelineMixingRowLayouts,
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);

          clickTransformAndPickCreateHop(graph, spots, source);

          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.target, 0, SWT.NONE);
          List<String> popups = clickAndCatchDialogs(bot, spots, spots.target, SWT.NONE);

          assertAll(
              () -> assertHopExists(pipelineMeta, source, target),
              () -> assertOnlyTheLayoutCheckDialogs(popups),
              () -> assertNoFailures(),
              () -> assertCanvasIsIdle(graph));
        });
  }

  @Test
  void shiftClickingATargetMixingRowLayoutsOnlyRunsTheLayoutCheck() {
    onCanvas(
        HopGuiPipelineGraphHopCreationTest::buildPipelineMixingRowLayouts,
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);

          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 1, SWT.SHIFT);
          fire(spots.canvas, SWT.MouseUp, spots.scale, spots.source, 1, SWT.SHIFT | SWT.BUTTON1);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.target, 0, SWT.SHIFT);
          List<String> popups = clickAndCatchDialogs(bot, spots, spots.target, SWT.SHIFT);

          assertAll(
              () -> assertHopExists(pipelineMeta, source, target),
              () -> assertOnlyTheLayoutCheckDialogs(popups),
              () -> assertNoFailures(),
              () -> assertCanvasIsIdle(graph));
        });
  }

  @Test
  void shiftDraggingOntoATargetMixingRowLayoutsOnlyRunsTheLayoutCheck() {
    onCanvas(
        HopGuiPipelineGraphHopCreationTest::buildPipelineMixingRowLayouts,
        (bot, graph, pipelineMeta, spots) -> {
          TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
          TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);

          fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 1, SWT.SHIFT);
          fire(
              spots.canvas,
              SWT.MouseMove,
              spots.scale,
              midpoint(spots.source, spots.target),
              0,
              SWT.SHIFT | SWT.BUTTON1);
          fire(spots.canvas, SWT.MouseMove, spots.scale, spots.target, 0, SWT.SHIFT | SWT.BUTTON1);
          Set<Shell> before = openShells();
          fireAsync(
              spots.canvas, SWT.MouseUp, spots.scale, spots.target, 1, SWT.SHIFT | SWT.BUTTON1);
          List<String> popups = catchDialogs(bot, before);

          assertAll(
              () -> assertHopExists(pipelineMeta, source, target),
              () -> assertOnlyTheLayoutCheckDialogs(popups),
              () -> assertNoFailures(),
              () -> assertCanvasIsIdle(graph));
        });
  }

  // ------------------------------------------------------------------ gesture building blocks

  /**
   * Clicks the transform and runs the "Create hop" item of the context dialog that pops up. Only
   * the picking of the item is short-circuited - the click that opens the dialog is a real one, so
   * the state it leaves behind is real too.
   */
  private void clickTransformAndPickCreateHop(
      HopGuiPipelineGraph graph, Spots spots, TransformMeta transform) {
    // Defer the context dialog and cancel it, the way a following click does, so the test does
    // not have to drive the dialog itself.
    onUi(() -> PropsUi.getInstance().setUseDoubleClickOnCanvas(true));
    fire(spots.canvas, SWT.MouseDown, spots.scale, spots.source, 1, SWT.NONE);
    fire(spots.canvas, SWT.MouseUp, spots.scale, spots.source, 1, SWT.BUTTON1);
    cancelPendingContextDialog(graph);
    onUi(() -> PropsUi.getInstance().setUseDoubleClickOnCanvas(false));

    onUi(
        () ->
            graph.newHopCandidate(
                new HopGuiPipelineTransformContext(
                    graph.getPipelineMeta(), transform, graph, new Point(spots.source))));
  }

  private void cancelPendingContextDialog(HopGuiPipelineGraph graph) {
    Object pending = privateField(graph, "pendingShowActionDialogRunnable");
    if (pending instanceof Runnable runnable) {
      onUi(() -> display.timerExec(-1, runnable));
    }
  }

  /**
   * Releases the mouse button and reports the title of the dialog that opened, or null when none
   * did. Any dialog is closed again so the canvas event loop is handed back.
   */
  private String releaseAndCatchDialog(SWTBot bot, Spots spots, Point at, int stateMask) {
    return releaseAndCatchDialog(bot, spots, at, 1, stateMask);
  }

  private String releaseAndCatchDialog(
      SWTBot bot, Spots spots, Point at, int button, int stateMask) {
    Set<Shell> before = openShells();
    fireAsync(spots.canvas, SWT.MouseUp, spots.scale, at, button, stateMask);
    Shell popup = awaitNewShell(bot, before);
    String title = titleOf(popup);
    closeShell(bot, popup);
    return title;
  }

  /**
   * Presses and releases the left button without waiting for the handlers. Both halves of the click
   * are posted up front on purpose: the press may open a dialog, and that dialog runs its own event
   * loop, which is what dispatches the release. Waiting for the press would deadlock the test, and
   * holding the release back until the dialog is gone would hide the very ordering under test.
   */
  private List<String> clickAndCatchDialogs(SWTBot bot, Spots spots, Point at, int stateMask) {
    Set<Shell> before = openShells();
    fireAsync(spots.canvas, SWT.MouseDown, spots.scale, at, 1, stateMask);
    fireAsync(spots.canvas, SWT.MouseUp, spots.scale, at, 1, stateMask | SWT.BUTTON1);
    return catchDialogs(bot, before);
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
  private List<String> catchDialogs(SWTBot bot, Set<Shell> before) {
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

  // ------------------------------------------------------------------ assertions

  private void assertHopExists(PipelineMeta meta, TransformMeta from, TransformMeta to) {
    assertNotNull(
        meta.findPipelineHop(from, to),
        "the gesture should have created the hop " + from + " → " + to);
  }

  private void assertNoHop(PipelineMeta meta, TransformMeta from, TransformMeta to) {
    assertNull(meta.findPipelineHop(from, to), "the abandoned gesture must not create a hop");
  }

  private void assertNoDialog(String dialogTitle) {
    assertNull(dialogTitle, "no dialog may open here, but one did");
  }

  /**
   * Mixing row layouts is worth reporting, and a Dummy target is worth an offer to replace it with
   * a Stream Schema Merge. Those two are all: the click completed a hop, so it may not also be read
   * as a plain click on the transform and open the context dialog on top of them.
   */
  private void assertOnlyTheLayoutCheckDialogs(List<String> dialogTitles) {
    assertEquals(
        List.of(
            BaseMessages.getString(HopGui.class, "HopGui.LayoutCheck.Dialog.MismatchTitle"),
            BaseMessages.getString(HopGui.class, "HopGui.LayoutCheck.Dialog.ReplaceDummyTitle")),
        dialogTitles,
        "the layout check is all this gesture may report");
  }

  private void assertNoFailures() {
    assertTrue(swallowed.isEmpty(), "the canvas must not throw, but got " + swallowed);
  }

  private static void assertDidNotMove(Point was, TransformMeta transform) {
    assertEquals(
        was.toString(),
        transform.getLocation().toString(),
        "the transform followed the mouse pointer");
  }

  /** Everything the graph remembers about a gesture has to be back to its initial value. */
  private static void assertCanvasIsIdle(HopGuiPipelineGraph graph) {
    Map<String, Object> idle = new LinkedHashMap<>();
    idle.put("startHopTransform", null);
    idle.put("endHopTransform", null);
    idle.put("endHopLocation", null);
    idle.put("candidate", null);
    idle.put("currentTransform", null);
    idle.put("selectedTransform", null);
    idle.put("selectedTransforms", null);
    idle.put("selectionRegion", null);
    idle.put("splitHop", false);
    idle.put("dragSelection", false);
    idle.put("avoidContextDialog", false);
    idle.put("lastButton", 0);
    assertGraphState(graph, idle);
  }

  // ------------------------------------------------------------------ scene

  /** What a test needs to aim at: the canvas, its scale and the interesting graph coordinates. */
  private record Spots(
      Canvas canvas,
      double scale,
      Point source,
      Point target,
      Point emptyA,
      Point emptyB,
      Point emptyC) {}

  @FunctionalInterface
  private interface CanvasTest {
    void run(SWTBot bot, HopGuiPipelineGraph graph, PipelineMeta pipelineMeta, Spots spots);
  }

  private void onCanvas(CanvasTest test) {
    onCanvas(HopGuiPipelineGraphHopCreationTest::buildPipeline, test);
  }

  private void onCanvas(Supplier<PipelineMeta> scene, CanvasTest test) {
    AtomicReference<HopGuiPipelineGraph> graphRef = new AtomicReference<>();
    AtomicReference<PipelineMeta> metaRef = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setSize(1000, 700);
          shell.setLayout(new FormLayout());
          // Keep the single click on the canvas synchronous and deterministic.
          PropsUi.getInstance().setUseDoubleClickOnCanvas(false);

          PipelineMeta pipelineMeta = scene.get();
          metaRef.set(pipelineMeta);
          graphRef.set(
              new HopGuiPipelineGraph(
                  shell,
                  hopGui(),
                  new ExplorerPerspective(),
                  pipelineMeta,
                  new HopPipelineFileType<>()));
          attachKeyboardShortcuts(shell);
        },
        bot -> {
          HopGuiPipelineGraph graph = graphRef.get();
          PipelineMeta pipelineMeta = metaRef.get();
          test.run(bot, graph, pipelineMeta, aim(bot, graph, pipelineMeta));
        });
  }

  /** Locates both icons and three spots of empty canvas, clear of the icons and of the minimap. */
  private Spots aim(SWTBot bot, HopGuiPipelineGraph graph, PipelineMeta pipelineMeta) {
    Canvas canvas = onUi(graph::getCanvas);
    double scale = canvasToGraphScale(graph);
    AreaLookup lookup = graph::getVisibleAreaOwner;

    Point source =
        awaitIcon(
            bot,
            lookup,
            AreaOwner.AreaType.TRANSFORM_ICON,
            pipelineMeta.findTransform(SOURCE_TRANSFORM),
            SOURCE_LOCATION);
    Point target =
        awaitIcon(
            bot,
            lookup,
            AreaOwner.AreaType.TRANSFORM_ICON,
            pipelineMeta.findTransform(TARGET_TRANSFORM),
            TARGET_LOCATION);

    Rectangle bounds = onUi(canvas::getBounds);
    Point visible = onUi(() -> graph.screen2real(bounds.width, bounds.height));
    Point emptyA = new Point((int) (visible.x * 0.50), (int) (visible.y * 0.45));
    Point emptyB = new Point((int) (visible.x * 0.30), (int) (visible.y * 0.60));
    Point emptyC = new Point((int) (visible.x * 0.20), (int) (visible.y * 0.72));
    assertEmptyCanvas(lookup, emptyA, emptyB, emptyC);

    return new Spots(canvas, scale, source, target, emptyA, emptyB, emptyC);
  }

  /** Two transforms, far enough apart to aim at either icon, with no hop between them yet. */
  private static PipelineMeta buildPipeline() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("hop-creation");

    TransformMeta source = transform(SOURCE_TRANSFORM, SOURCE_LOCATION);
    TransformMeta target = transform(TARGET_TRANSFORM, TARGET_LOCATION);
    pipelineMeta.addTransform(source);
    pipelineMeta.addTransform(target);
    return pipelineMeta;
  }

  /**
   * The same two transforms, plus a third one that already feeds the target - and sends a different
   * row layout than the source does. Drawing the second hop into the target is what makes the row
   * layout check report a mismatch.
   *
   * <p>The target is a {@code Dummy}, exactly as in the report, so the layout check also offers to
   * replace it with a Stream Schema Merge. The tests decline that offer.
   */
  private static PipelineMeta buildPipelineMixingRowLayouts() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("hop-creation-mixed-layouts");

    TransformMeta source = fieldTransform(SOURCE_TRANSFORM, SOURCE_LOCATION, "one");
    TransformMeta other = fieldTransform(OTHER_INPUT_TRANSFORM, OTHER_INPUT_LOCATION, "one", "two");
    TransformMeta target = transform(TARGET_TRANSFORM, TARGET_LOCATION);
    pipelineMeta.addTransform(source);
    pipelineMeta.addTransform(other);
    pipelineMeta.addTransform(target);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(other, target));
    return pipelineMeta;
  }

  private static TransformMeta transform(String name, Point location) {
    TransformMeta transformMeta =
        new TransformMeta("Dummy", name, new BaseTransformMeta<ITransform, ITransformData>());
    transformMeta.setLocation(location.x, location.y);
    return transformMeta;
  }

  /** A transform putting the given fields on the stream, so that two of them can disagree. */
  private static TransformMeta fieldTransform(String name, Point location, String... fields) {
    TransformMeta transformMeta =
        new TransformMeta(
            "Dummy",
            name,
            new BaseTransformMeta<ITransform, ITransformData>() {
              @Override
              public void getFields(
                  IRowMeta inputRowMeta,
                  String transformName,
                  IRowMeta[] info,
                  TransformMeta nextTransform,
                  IVariables variables,
                  IHopMetadataProvider metadataProvider) {
                for (String field : fields) {
                  inputRowMeta.addValueMeta(new ValueMetaString(field));
                }
              }
            });
    transformMeta.setLocation(location.x, location.y);
    return transformMeta;
  }
}
