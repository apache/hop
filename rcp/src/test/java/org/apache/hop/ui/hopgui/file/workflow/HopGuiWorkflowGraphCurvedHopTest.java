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

package org.apache.hop.ui.hopgui.file.workflow;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.HopLineShape;
import org.apache.hop.core.gui.HopPath;
import org.apache.hop.core.gui.Point;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.hopgui.file.GraphCanvasTestBase;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Finding a hop under the mouse when hops are drawn as curves. A curved hop does not follow the
 * straight line between the two icon centres, so a click on the curve away from that line must
 * still find the hop. The click is a ctrl-click, which flips the enabled state of the hop it finds.
 */
@Tag("uitest")
class HopGuiWorkflowGraphCurvedHopTest extends GraphCanvasTestBase {

  private static final String SOURCE_ACTION = "Source";
  private static final String TARGET_ACTION = "Target";
  private static final Point SOURCE_LOCATION = new Point(60, 60);
  private static final Point TARGET_LOCATION = new Point(460, 300);

  /** Where on the curve the test clicks, as a fraction of its length. */
  private static final double CLICK_FRACTION = 0.25;

  /** How far the click must be from the straight line, so that only the curve can find it. */
  private static final double MIN_DISTANCE_FROM_STRAIGHT_LINE = 15;

  @BeforeAll
  static void registerGuiPlugins() throws HopException {
    HopGuiEnvironment.init();
  }

  @AfterEach
  void restoreStraightHops() {
    onUi(() -> PropsUi.getInstance().setHopLineShape(HopLineShape.STRAIGHT));
  }

  @Test
  void ctrlClickingACurvedHopAwayFromTheStraightLineFindsTheHop() {
    onCanvas(
        HopLineShape.CURVED,
        (bot, workflowMeta, spots) -> {
          WorkflowHopMeta hop = theHop(workflowMeta);

          String popup = ctrlClick(bot, spots);
          assertAll(
              "a ctrl-click on the curve should find the hop and disable it",
              () -> assertFalse(hop.isEnabled(), "the click should have disabled the hop"),
              () -> assertNull(popup, "no dialog may open, but got " + popup),
              () -> assertTrue(swallowed.isEmpty(), "the canvas threw " + swallowed));
        });
  }

  @Test
  void withStraightHopsTheSamePointIsNotOnTheHop() {
    onCanvas(
        HopLineShape.STRAIGHT,
        (bot, workflowMeta, spots) -> {
          WorkflowHopMeta hop = theHop(workflowMeta);

          ctrlClick(bot, spots);
          assertAll(
              "a straight hop does not pass through the point on the curve",
              () -> assertTrue(hop.isEnabled(), "the click should have missed the hop"),
              () -> assertTrue(swallowed.isEmpty(), "the canvas threw " + swallowed));
        });
  }

  private static WorkflowHopMeta theHop(WorkflowMeta workflowMeta) {
    WorkflowHopMeta hop =
        workflowMeta.findWorkflowHop(
            workflowMeta.findAction(SOURCE_ACTION), workflowMeta.findAction(TARGET_ACTION));
    assertNotNull(hop, "the scene should hold the hop under test");
    assertTrue(hop.isEnabled(), "the hop starts enabled");
    return hop;
  }

  /** Ctrl-clicks the point on the curve and reports the title of any dialog that opened. */
  private String ctrlClick(SWTBot bot, Spots spots) {
    Set<Shell> before = openShells();
    fire(spots.canvas, SWT.MouseDown, spots.scale, spots.onCurve, 1, SWT.MOD1);
    fireAsync(spots.canvas, SWT.MouseUp, spots.scale, spots.onCurve, 1, SWT.MOD1 | buttonMask(1));
    Shell popup = awaitNewShell(bot, before);
    String title = titleOf(popup);
    closeShell(bot, popup);
    return title;
  }

  // ------------------------------------------------------------------ scene

  /** The canvas, its scale and a point on the curved hop that is off the straight line. */
  private record Spots(Canvas canvas, double scale, Point onCurve) {}

  @FunctionalInterface
  private interface CanvasTest {
    void run(SWTBot bot, WorkflowMeta workflowMeta, Spots spots);
  }

  private void onCanvas(HopLineShape shape, CanvasTest test) {
    AtomicReference<HopGuiWorkflowGraph> graphRef = new AtomicReference<>();
    AtomicReference<WorkflowMeta> metaRef = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setSize(1000, 700);
          // The workflow graph gives itself GridData, so its parent lays out with a GridLayout.
          shell.setLayout(new GridLayout(1, false));
          // Keep the single click on the canvas synchronous and deterministic.
          PropsUi.getInstance().setUseDoubleClickOnCanvas(false);
          PropsUi.getInstance().setHopLineShape(shape);

          WorkflowMeta workflowMeta = buildWorkflow();
          metaRef.set(workflowMeta);
          graphRef.set(
              new HopGuiWorkflowGraph(
                  shell,
                  hopGui(),
                  new ExplorerPerspective(),
                  workflowMeta,
                  new HopWorkflowFileType<>()));
        },
        bot -> {
          HopGuiWorkflowGraph graph = graphRef.get();
          WorkflowMeta workflowMeta = metaRef.get();
          test.run(bot, workflowMeta, aim(bot, graph, workflowMeta));
        });
  }

  /** Locates the icons and returns a point on the curved hop that is off the straight line. */
  private Spots aim(SWTBot bot, HopGuiWorkflowGraph graph, WorkflowMeta workflowMeta) {
    Canvas canvas = onUi(graph::getCanvas);
    double scale = canvasToGraphScale(graph);
    AreaLookup lookup = graph::getVisibleAreaOwner;
    int iconSize = onUi(() -> PropsUi.getInstance().getIconSize());

    Point source =
        awaitIcon(
            bot,
            lookup,
            AreaOwner.AreaType.ACTION_ICON,
            workflowMeta.findAction(SOURCE_ACTION),
            SOURCE_LOCATION);
    Point target =
        awaitIcon(
            bot,
            lookup,
            AreaOwner.AreaType.ACTION_ICON,
            workflowMeta.findAction(TARGET_ACTION),
            TARGET_LOCATION);

    // The same curve the painter draws and the graph looks the hop up with.
    double[] point =
        HopPath.curved(source.x, source.y, target.x, target.y, iconSize, true, true)
            .pointAt(CLICK_FRACTION);
    Point onCurve = new Point((int) Math.round(point[0]), (int) Math.round(point[1]));

    double fromStraightLine =
        HopPath.straight(source.x, source.y, target.x, target.y).distanceTo(onCurve.x, onCurve.y);
    assertTrue(
        fromStraightLine >= MIN_DISTANCE_FROM_STRAIGHT_LINE,
        "the click must be off the straight line, but is only " + fromStraightLine + " away");
    // Nothing else may sit there, or the click would hit that instead of the hop.
    assertEmptyCanvas(lookup, onCurve);

    return new Spots(canvas, scale, onCurve);
  }

  /** A single hop from the top left to the bottom right, so the curve bends away from the line. */
  private static WorkflowMeta buildWorkflow() {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("curved-hop");

    ActionMeta source = action(SOURCE_ACTION, SOURCE_LOCATION);
    ActionMeta target = action(TARGET_ACTION, TARGET_LOCATION);
    workflowMeta.addAction(source);
    workflowMeta.addAction(target);
    workflowMeta.addWorkflowHop(new WorkflowHopMeta(source, target));
    return workflowMeta;
  }

  private static ActionMeta action(String name, Point location) {
    ActionMeta actionMeta = new ActionMeta(new TestAction(name));
    actionMeta.setLocation(location.x, location.y);
    return actionMeta;
  }

  /** The smallest possible action: the canvas only needs a name, a location and an icon slot. */
  private static class TestAction extends ActionBase {
    TestAction(String name) {
      super(name, "");
    }

    @Override
    public Result execute(Result prevResult, int nr) {
      return prevResult;
    }

    @Override
    public Object clone() {
      return new TestAction(getName());
    }
  }
}
