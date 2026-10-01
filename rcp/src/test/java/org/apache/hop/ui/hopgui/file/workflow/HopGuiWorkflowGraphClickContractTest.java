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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.Result;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Point;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.HopToolTip;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.hopgui.file.GraphCanvasTestBase;
import org.apache.hop.ui.hopgui.palette.GraphPalette;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The "Clicks" table of the canvas mouse gestures page in the user manual ({@code
 * docs/hop-user-manual/modules/ROOT/pages/hop-gui/canvas-mouse.adoc}), one test per cell, for the
 * workflow canvas. The row names below are the "Where you click" column of that table: keep them in
 * step, the table is the contract and this class is what pins it.
 *
 * <p>Each cell replays the click as real mouse events and checks what the manual says: which dialog
 * opened (by title) or that none did, the side effect on the workflow (selected, cleared, hop
 * evaluation cycled), that the canvas swallowed no exception, and that no gesture state is left
 * behind.
 */
@Tag("uitest")
class HopGuiWorkflowGraphClickContractTest extends GraphCanvasTestBase {

  private static final String SOURCE_ACTION = "Check";
  private static final String TARGET_ACTION = "Dummy";
  private static final Point SOURCE_LOCATION = new Point(60, 60);
  private static final Point TARGET_LOCATION = new Point(280, 60);
  private static final Point NOTE_LOCATION = new Point(60, 250);

  private static final int LEFT = 1;
  private static final int RIGHT = 3;

  @BeforeAll
  static void registerGuiPlugins() throws HopException {
    // A context dialog bails out when the GUI registry holds no actions for the context, so
    // register the GUI plugins to get the same dialogs the user sees.
    HopGuiEnvironment.init();
  }

  // ------------------------------------------------------------------ the table

  /**
   * Where the manual says to click, in the words of its table. A row that groups several badges is
   * exercised through one of them and named after that one.
   */
  private enum Where {
    EMPTY("Empty canvas, nothing selected"),
    EMPTY_PALETTE_SHOWN("Empty canvas, nothing selected, palette tree shown"),
    EMPTY_SELECTED("Empty canvas, something selected"),
    ICON("Transform / action icon"),
    NAME("Transform / action name"),
    HOP_LINE("Hop line"),
    EVALUATION_BADGE("Hop badge on a workflow: unconditional / true / false"),
    INFO_BADGE("Badge on an action: info (description)"),
    NOTE("Note");

    final String label;

    Where(String label) {
      this.label = label;
    }
  }

  /**
   * One cell: the mode, the spot, the button, the dialog the manual promises (or none) and the side
   * effect.
   */
  private record Cell(
      boolean rightClickMode, Where where, int button, String dialog, Consumer<Scene> sideEffect) {
    @Override
    public String toString() {
      return where.label
          + " - "
          + (button == LEFT ? "left click" : "right click")
          + (rightClickMode ? " (use right click for the context dialog)" : "");
    }
  }

  static Stream<Cell> cells() {
    String workflowDialog = msg("HopGuiWorkflowGraph.ContextualActionDialog.Workflow.Header");
    String workflowActionsDialog =
        msg("HopGuiWorkflowGraph.ContextualActionDialog.WorkflowActions.Header");
    String targetDialog =
        msg("HopGuiWorkflowGraph.ContextualActionDialog.Action.Header", TARGET_ACTION);
    String sourceDialog =
        msg("HopGuiWorkflowGraph.ContextualActionDialog.Action.Header", SOURCE_ACTION);
    String hopDialog = msg("HopGuiWorkflowGraph.ContextualActionDialog.Hop.Header");
    String noteDialog = msg("HopGuiWorkflowGraph.ContextualActionDialog.Note.Header");
    String properties = BaseMessages.getString(ActionDummy.class, "ActionDummyDialog.Title");
    String description = msg("WorkflowGraph.Dialog.EditDescription.Title");

    // The "Left click" and "Right click (default)" columns of the table.
    Stream<Cell> defaultMode =
        Stream.of(
            new Cell(false, Where.EMPTY, LEFT, workflowDialog, Scene::nothingSelected),
            new Cell(false, Where.EMPTY, RIGHT, null, Scene::nothingSelected),
            new Cell(
                false,
                Where.EMPTY_PALETTE_SHOWN,
                LEFT,
                workflowActionsDialog,
                Scene::nothingSelected),
            new Cell(false, Where.EMPTY_PALETTE_SHOWN, RIGHT, null, Scene::nothingSelected),
            new Cell(false, Where.EMPTY_SELECTED, LEFT, null, Scene::selectionCleared),
            new Cell(false, Where.EMPTY_SELECTED, RIGHT, null, Scene::targetStillSelected),
            new Cell(false, Where.ICON, LEFT, targetDialog, Scene::targetSelected),
            new Cell(false, Where.ICON, RIGHT, null, Scene::nothingSelected),
            new Cell(false, Where.NAME, LEFT, properties, Scene::nothingSelected),
            new Cell(false, Where.NAME, RIGHT, null, Scene::nothingSelected),
            new Cell(false, Where.HOP_LINE, LEFT, hopDialog, Scene::hopUnconditional),
            new Cell(false, Where.HOP_LINE, RIGHT, null, Scene::hopUnconditional),
            new Cell(false, Where.EVALUATION_BADGE, LEFT, null, Scene::hopCycledToTrue),
            new Cell(false, Where.EVALUATION_BADGE, RIGHT, null, Scene::hopUnconditional),
            new Cell(false, Where.INFO_BADGE, LEFT, description, Scene::nothingSelected),
            new Cell(false, Where.INFO_BADGE, RIGHT, null, Scene::nothingSelected),
            new Cell(false, Where.NOTE, LEFT, noteDialog, Scene::noteSelected),
            new Cell(false, Where.NOTE, RIGHT, null, Scene::nothingSelected));

    // The "Right click (Use right click for the context dialog: on)" column, and what the left
    // click still does in that mode.
    Stream<Cell> rightClickMode =
        Stream.of(
            new Cell(true, Where.EMPTY, LEFT, null, Scene::nothingSelected),
            new Cell(true, Where.EMPTY, RIGHT, workflowDialog, Scene::nothingSelected),
            new Cell(true, Where.EMPTY_PALETTE_SHOWN, LEFT, null, Scene::nothingSelected),
            new Cell(
                true,
                Where.EMPTY_PALETTE_SHOWN,
                RIGHT,
                workflowActionsDialog,
                Scene::nothingSelected),
            new Cell(true, Where.EMPTY_SELECTED, LEFT, null, Scene::selectionCleared),
            new Cell(true, Where.EMPTY_SELECTED, RIGHT, workflowDialog, Scene::targetStillSelected),
            new Cell(true, Where.ICON, LEFT, null, Scene::targetSelected),
            new Cell(true, Where.ICON, RIGHT, targetDialog, Scene::targetSelected),
            new Cell(true, Where.NAME, LEFT, properties, Scene::nothingSelected),
            new Cell(true, Where.NAME, RIGHT, targetDialog, Scene::targetSelected),
            new Cell(true, Where.HOP_LINE, LEFT, null, Scene::hopUnconditional),
            new Cell(true, Where.HOP_LINE, RIGHT, hopDialog, Scene::hopUnconditional),
            new Cell(true, Where.EVALUATION_BADGE, LEFT, null, Scene::hopCycledToTrue),
            new Cell(true, Where.EVALUATION_BADGE, RIGHT, hopDialog, Scene::hopUnconditional),
            new Cell(true, Where.INFO_BADGE, LEFT, description, Scene::nothingSelected),
            new Cell(true, Where.INFO_BADGE, RIGHT, sourceDialog, Scene::sourceSelected),
            new Cell(true, Where.NOTE, LEFT, null, Scene::noteSelected),
            new Cell(true, Where.NOTE, RIGHT, noteDialog, Scene::noteSelected));

    return Stream.concat(defaultMode, rightClickMode);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cells")
  void clicking(Cell cell) {
    onCanvas(
        cell,
        scene -> {
          Point at = scene.aim(cell.where);

          List<String> dialogs =
              cell.button == RIGHT
                  ? contextClickAndCatchDialogs(
                      scene.bot, scene.canvas, scene.scale, at, scene::noteBalloon)
                  : clickAndCatchDialogs(
                      scene.bot, scene.canvas, scene.scale, at, LEFT, SWT.NONE, scene::noteBalloon);

          assertAll(
              () -> assertDialogs(cell.dialog, dialogs),
              () -> cell.sideEffect.accept(scene),
              () -> assertNoFailures(),
              () -> assertCanvasIsIdle(scene.graph));
        });
  }

  /**
   * The "Selection cleared" balloon is a notice, not a hover tooltip: moving the mouse over the
   * empty canvas must leave it up, its timer takes it down.
   */
  @Test
  void movingTheMouseKeepsTheSelectionClearedBalloon() {
    Cell cell = new Cell(false, Where.EMPTY_SELECTED, LEFT, null, Scene::selectionCleared);
    onCanvas(
        cell,
        scene -> {
          Point at = scene.aim(cell.where);
          Point nearby = new Point(at.x + 10, at.y + 10);
          assertEmptyCanvas(scene.lookup, nearby);

          List<String> dialogs =
              clickAndCatchDialogs(
                  scene.bot,
                  scene.canvas,
                  scene.scale,
                  at,
                  LEFT,
                  SWT.NONE,
                  () -> {
                    fire(scene.canvas, SWT.MouseMove, scene.scale, nearby, 0, SWT.NONE);
                    scene.noteBalloon();
                  });

          assertAll(
              () -> assertDialogs(null, dialogs),
              () -> cell.sideEffect.accept(scene),
              () -> assertNoFailures());
        });
  }

  // ------------------------------------------------------------------ assertions

  private static void assertDialogs(String expected, List<String> actual) {
    assertEquals(
        expected == null ? List.of() : List.of(expected),
        actual,
        expected == null ? "no dialog may open here" : "the dialog the manual promises");
  }

  private void assertNoFailures() {
    assertTrue(swallowed.isEmpty(), "the canvas must not throw, but got " + swallowed);
  }

  /** Whatever the click did, it may not leave a gesture half finished. */
  private static void assertCanvasIsIdle(HopGuiWorkflowGraph graph) {
    Map<String, Object> idle = new LinkedHashMap<>();
    idle.put("startHopAction", null);
    idle.put("endHopAction", null);
    idle.put("endHopLocation", null);
    idle.put("hopCandidate", null);
    idle.put("selectedAction", null);
    idle.put("selectedActions", null);
    idle.put("selectionRegion", null);
    idle.put("dragSelection", false);
    idle.put("actionDragStartScreen", null);
    idle.put("avoidContextDialog", false);
    idle.put("ignoreNextClick", false);
    idle.put("lastButton", 0);
    assertGraphState(graph, idle);
  }

  // ------------------------------------------------------------------ scene

  /** The canvas under test, the workflow on it and where its parts were painted. */
  private final class Scene {
    final SWTBot bot;
    final HopGuiWorkflowGraph graph;
    final WorkflowMeta workflowMeta;
    final Canvas canvas;
    final double scale;
    final AreaLookup lookup;
    final ActionMeta source;
    final ActionMeta target;
    final WorkflowHopMeta hop;
    final NotePadMeta note;

    Scene(SWTBot bot, HopGuiWorkflowGraph graph, WorkflowMeta workflowMeta) {
      this.bot = bot;
      this.graph = graph;
      this.workflowMeta = workflowMeta;
      this.canvas = onUi(graph::getCanvas);
      this.scale = canvasToGraphScale(graph);
      this.lookup = graph::getVisibleAreaOwner;
      this.source = workflowMeta.findAction(SOURCE_ACTION);
      this.target = workflowMeta.findAction(TARGET_ACTION);
      this.hop = workflowMeta.findWorkflowHop(source, target);
      this.note = workflowMeta.getNote(0);
    }

    /** The graph coordinate to click for a row of the table. */
    Point aim(Where where) {
      return switch (where) {
        case EMPTY, EMPTY_PALETTE_SHOWN, EMPTY_SELECTED -> emptySpot();
        case ICON ->
            awaitIcon(bot, lookup, AreaOwner.AreaType.ACTION_ICON, target, TARGET_LOCATION);
        case NAME ->
            awaitArea(
                bot, graph, lookup, AreaOwner.AreaType.ACTION_NAME, a -> a.getParent() == target);
        case HOP_LINE -> hopLine();
        case EVALUATION_BADGE ->
            awaitArea(
                bot, graph, lookup, AreaOwner.AreaType.WORKFLOW_HOP_ICON, a -> a.getOwner() == hop);
        case INFO_BADGE ->
            awaitArea(
                bot,
                graph,
                lookup,
                AreaOwner.AreaType.ACTION_INFO_ICON,
                a -> a.getOwner() == source);
        case NOTE ->
            awaitArea(bot, graph, lookup, AreaOwner.AreaType.NOTE, a -> a.getOwner() == note);
      };
    }

    /** A spot of empty canvas, clear of the icons, the note and the minimap. */
    private Point emptySpot() {
      // Wait for the paint first: an empty spot is only empty once the areas are registered.
      awaitIcon(bot, lookup, AreaOwner.AreaType.ACTION_ICON, source, SOURCE_LOCATION);
      Rectangle bounds = onUi(canvas::getBounds);
      Point visible = onUi(() -> graph.screen2real(bounds.width, bounds.height));
      Point empty = new Point((int) (visible.x * 0.60), (int) (visible.y * 0.45));
      assertEmptyCanvas(lookup, empty);
      return empty;
    }

    /**
     * A point on the hop, past the evaluation badge that sits at 40% of the way and the arrow at
     * the middle, and short of the target icon.
     */
    private Point hopLine() {
      Point from = awaitIcon(bot, lookup, AreaOwner.AreaType.ACTION_ICON, source, SOURCE_LOCATION);
      Point to = awaitIcon(bot, lookup, AreaOwner.AreaType.ACTION_ICON, target, TARGET_LOCATION);
      Point on = new Point(from.x + (int) ((to.x - from.x) * 0.7), from.y);
      assertEmptyCanvas(lookup, on);
      return on;
    }

    // -------------------------------------------------------------- side effects

    void targetSelected() {
      assertTrue(target.isSelected(), "the click should have selected " + TARGET_ACTION);
      assertEquals(List.of(target), workflowMeta.getSelectedActions(), "only the clicked action");
    }

    void sourceSelected() {
      assertTrue(source.isSelected(), "the click should have selected " + SOURCE_ACTION);
      assertEquals(List.of(source), workflowMeta.getSelectedActions(), "only the clicked action");
    }

    void targetStillSelected() {
      assertTrue(target.isSelected(), TARGET_ACTION + " should still be selected");
    }

    void noteSelected() {
      assertTrue(note.isSelected(), "the click should have selected the note");
    }

    /**
     * The text of the balloon the click put up, or null when it put up none. Taken the moment the
     * click has landed: the balloon is on a timer, so by the time the wait for dialogs is over it
     * may well be gone again.
     */
    String balloon;

    void noteBalloon() {
      HopToolTip toolTip = (HopToolTip) privateField(graph, "toolTip");
      balloon = onUi(() -> toolTip.isVisible() ? toolTip.getText() : null);
    }

    /** The manual promises the selection is gone and a "Selection cleared" balloon says so. */
    void selectionCleared() {
      nothingSelected();
      assertNotNull(balloon, "the click should have put up the 'Selection cleared' balloon");
      assertTrue(
          balloon.contains("Selection cleared"),
          "the balloon should say the selection was cleared, not " + balloon);
    }

    void nothingSelected() {
      assertTrue(workflowMeta.getSelectedActions().isEmpty(), "no action may be selected");
      assertTrue(workflowMeta.getSelectedNotes().isEmpty(), "no note may be selected");
    }

    void hopUnconditional() {
      assertTrue(hop.isUnconditional(), "the hop should still be unconditional");
    }

    void hopCycledToTrue() {
      assertFalse(hop.isUnconditional(), "the click should have cycled the hop off unconditional");
      assertTrue(hop.isEvaluation(), "the first cycle goes to 'true'");
    }
  }

  private void onCanvas(Cell cell, Consumer<Scene> test) {
    Where where = cell.where;
    AtomicReference<HopGuiWorkflowGraph> graphRef = new AtomicReference<>();
    AtomicReference<WorkflowMeta> metaRef = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setSize(1000, 700);
          // The workflow graph gives itself GridData, so its parent lays out with a GridLayout.
          shell.setLayout(new GridLayout(1, false));
          // The mode of the cell's column, and the palette tree only for its row.
          PropsUi.getInstance().setUseDoubleClickOnCanvas(false);
          PropsUi.getInstance().setUseRightClickForContextDialog(cell.rightClickMode);
          HopConfig.setGuiProperty(
              GraphPalette.CONFIG_KEY, where == Where.EMPTY_PALETTE_SHOWN ? "Y" : "N");

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
          try {
            Scene scene = new Scene(bot, graphRef.get(), metaRef.get());
            if (where == Where.EMPTY_SELECTED) {
              // After the graph exists: its constructor clears every selection.
              onUi(
                  () -> {
                    scene.target.setSelected(true);
                    scene.graph.redraw();
                  });
            }
            test.accept(scene);
          } finally {
            onUi(
                () -> {
                  PropsUi.getInstance().setUseRightClickForContextDialog(false);
                  HopConfig.setGuiProperty(GraphPalette.CONFIG_KEY, "N");
                });
          }
        });
  }

  /**
   * An evaluating action with a description (info badge) hopping to a real Dummy action (whose name
   * click opens the real properties dialog), and a note. The hop out of an evaluating action is the
   * one whose badge cycles unconditional / true / false.
   */
  private static WorkflowMeta buildWorkflow() {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("click-contract");

    ActionMeta source = new ActionMeta(new EvaluatingAction(SOURCE_ACTION));
    source.setLocation(SOURCE_LOCATION.x, SOURCE_LOCATION.y);
    source.setDescription("Checks something");
    ActionMeta target = new ActionMeta(new ActionDummy());
    target.setName(TARGET_ACTION);
    target.setLocation(TARGET_LOCATION.x, TARGET_LOCATION.y);
    workflowMeta.addAction(source);
    workflowMeta.addAction(target);
    // Start where the badge's cycle starts: a new hop is a 'true' hop, not an unconditional one.
    WorkflowHopMeta hop = new WorkflowHopMeta(source, target);
    hop.setUnconditional(true);
    workflowMeta.addWorkflowHop(hop);

    workflowMeta.addNote(new NotePadMeta("A note", NOTE_LOCATION.x, NOTE_LOCATION.y, 150, 60));
    return workflowMeta;
  }

  /** The smallest evaluating action: its outgoing hops carry the unconditional/true/false badge. */
  private static class EvaluatingAction extends ActionBase {
    EvaluatingAction(String name) {
      super(name, "");
    }

    @Override
    public boolean isEvaluation() {
      return true;
    }

    @Override
    public Result execute(Result prevResult, int nr) {
      return prevResult;
    }

    @Override
    public Object clone() {
      return new EvaluatingAction(getName());
    }
  }

  private static String msg(String key, String... parameters) {
    return BaseMessages.getString(HopGuiWorkflowGraph.class, key, parameters);
  }
}
