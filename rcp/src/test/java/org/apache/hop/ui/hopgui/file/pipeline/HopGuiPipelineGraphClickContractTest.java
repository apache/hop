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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Point;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.HopToolTip;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.hopgui.file.GraphCanvasTestBase;
import org.apache.hop.ui.hopgui.palette.GraphPalette;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The "Clicks" table of the canvas mouse gestures page in the user manual ({@code
 * docs/hop-user-manual/modules/ROOT/pages/hop-gui/canvas-mouse.adoc}), one test per cell, for the
 * pipeline canvas. The row names below are the "Where you click" column of that table: keep them in
 * step, the table is the contract and this class is what pins it.
 *
 * <p>Each cell replays the click as real mouse events and checks what the manual says: which dialog
 * opened (by title) or that none did, the side effect on the pipeline (selected, cleared), that the
 * canvas swallowed no exception, and that no gesture state is left behind.
 */
@Tag("uitest")
class HopGuiPipelineGraphClickContractTest extends GraphCanvasTestBase {

  private static final String SOURCE_TRANSFORM = "Read";
  private static final String TARGET_TRANSFORM = "Write";
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
    HOP_COPY_BADGE("Hop badge on a pipeline: copies, row distribution"),
    INFO_BADGE("Badge on a transform: info (description)"),
    COPIES_BADGE("Badge on a transform: copies"),
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
    String pipelineDialog = msg("PipelineGraph.ContextualActionDialog.Pipeline.Header");
    String pipelineActionsDialog =
        msg("PipelineGraph.ContextualActionDialog.PipelineActions.Header");
    String transformDialog =
        msg("PipelineGraph.ContextualActionDialog.Transform.Header", SOURCE_TRANSFORM);
    String hopDialog = msg("PipelineGraph.ContextualActionDialog.Hop.Header");
    String noteDialog = msg("PipelineGraph.ContextualActionDialog.Note.Header");
    String properties = BaseMessages.getString(DummyMeta.class, "DummyDialog.Shell.Title");
    String description = msg("PipelineGraph.Dialog.TransformDescription.Title");
    String copies = msg("PipelineGraph.Dialog.NrOfCopiesOfTransform.Title");

    // The "Left click" and "Right click (default)" columns of the table.
    Stream<Cell> defaultMode =
        Stream.of(
            new Cell(false, Where.EMPTY, LEFT, pipelineDialog, Scene::nothingSelected),
            new Cell(false, Where.EMPTY, RIGHT, null, Scene::nothingSelected),
            new Cell(
                false,
                Where.EMPTY_PALETTE_SHOWN,
                LEFT,
                pipelineActionsDialog,
                Scene::nothingSelected),
            new Cell(false, Where.EMPTY_PALETTE_SHOWN, RIGHT, null, Scene::nothingSelected),
            new Cell(false, Where.EMPTY_SELECTED, LEFT, null, Scene::selectionCleared),
            new Cell(false, Where.EMPTY_SELECTED, RIGHT, null, Scene::sourceStillSelected),
            new Cell(false, Where.ICON, LEFT, transformDialog, Scene::sourceSelected),
            new Cell(false, Where.ICON, RIGHT, null, Scene::nothingSelected),
            new Cell(false, Where.NAME, LEFT, properties, Scene::nothingSelected),
            new Cell(false, Where.NAME, RIGHT, null, Scene::nothingSelected),
            new Cell(false, Where.HOP_LINE, LEFT, hopDialog, Scene::nothingSelected),
            new Cell(false, Where.HOP_LINE, RIGHT, null, Scene::nothingSelected),
            new Cell(false, Where.HOP_COPY_BADGE, LEFT, hopDialog, Scene::nothingSelected),
            new Cell(false, Where.HOP_COPY_BADGE, RIGHT, null, Scene::nothingSelected),
            new Cell(false, Where.INFO_BADGE, LEFT, description, Scene::nothingSelected),
            new Cell(false, Where.INFO_BADGE, RIGHT, null, Scene::nothingSelected),
            new Cell(false, Where.COPIES_BADGE, LEFT, copies, Scene::nothingSelected),
            new Cell(false, Where.COPIES_BADGE, RIGHT, null, Scene::nothingSelected),
            new Cell(false, Where.NOTE, LEFT, noteDialog, Scene::noteSelected),
            new Cell(false, Where.NOTE, RIGHT, null, Scene::nothingSelected));

    // The "Right click (Use right click for the context dialog: on)" column, and what the left
    // click still does in that mode.
    Stream<Cell> rightClickMode =
        Stream.of(
            new Cell(true, Where.EMPTY, LEFT, null, Scene::nothingSelected),
            new Cell(true, Where.EMPTY, RIGHT, pipelineDialog, Scene::nothingSelected),
            new Cell(true, Where.EMPTY_PALETTE_SHOWN, LEFT, null, Scene::nothingSelected),
            new Cell(
                true,
                Where.EMPTY_PALETTE_SHOWN,
                RIGHT,
                pipelineActionsDialog,
                Scene::nothingSelected),
            new Cell(true, Where.EMPTY_SELECTED, LEFT, null, Scene::selectionCleared),
            new Cell(true, Where.EMPTY_SELECTED, RIGHT, pipelineDialog, Scene::sourceStillSelected),
            new Cell(true, Where.ICON, LEFT, null, Scene::sourceSelected),
            new Cell(true, Where.ICON, RIGHT, transformDialog, Scene::sourceSelected),
            new Cell(true, Where.NAME, LEFT, properties, Scene::nothingSelected),
            new Cell(true, Where.NAME, RIGHT, transformDialog, Scene::sourceSelected),
            new Cell(true, Where.HOP_LINE, LEFT, null, Scene::nothingSelected),
            new Cell(true, Where.HOP_LINE, RIGHT, hopDialog, Scene::nothingSelected),
            new Cell(true, Where.HOP_COPY_BADGE, LEFT, hopDialog, Scene::nothingSelected),
            new Cell(true, Where.HOP_COPY_BADGE, RIGHT, hopDialog, Scene::nothingSelected),
            new Cell(true, Where.INFO_BADGE, LEFT, description, Scene::nothingSelected),
            new Cell(true, Where.INFO_BADGE, RIGHT, transformDialog, Scene::sourceSelected),
            new Cell(true, Where.COPIES_BADGE, LEFT, copies, Scene::nothingSelected),
            new Cell(true, Where.COPIES_BADGE, RIGHT, transformDialog, Scene::sourceSelected),
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
                  ? contextClickAndCatchDialogs(scene.bot, scene.canvas, scene.scale, at)
                  : clickAndCatchDialogs(scene.bot, scene.canvas, scene.scale, at, LEFT, SWT.NONE);

          assertAll(
              () -> assertDialogs(cell.dialog, dialogs),
              () -> cell.sideEffect.accept(scene),
              () -> assertNoFailures(),
              () -> assertCanvasIsIdle(scene.graph));
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
  private static void assertCanvasIsIdle(HopGuiPipelineGraph graph) {
    Map<String, Object> idle = new LinkedHashMap<>();
    idle.put("startHopTransform", null);
    idle.put("endHopTransform", null);
    idle.put("endHopLocation", null);
    idle.put("candidate", null);
    idle.put("selectedTransform", null);
    idle.put("selectedTransforms", null);
    idle.put("selectionRegion", null);
    idle.put("dragSelection", false);
    idle.put("iconDragStartScreen", null);
    idle.put("avoidContextDialog", false);
    idle.put("lastButton", 0);
    assertGraphState(graph, idle);
  }

  // ------------------------------------------------------------------ scene

  /** The canvas under test, the pipeline on it and where its parts were painted. */
  private final class Scene {
    final SWTBot bot;
    final HopGuiPipelineGraph graph;
    final PipelineMeta pipelineMeta;
    final Canvas canvas;
    final double scale;
    final AreaLookup lookup;
    final TransformMeta source;
    final TransformMeta target;
    final PipelineHopMeta hop;
    final NotePadMeta note;

    Scene(SWTBot bot, HopGuiPipelineGraph graph, PipelineMeta pipelineMeta) {
      this.bot = bot;
      this.graph = graph;
      this.pipelineMeta = pipelineMeta;
      this.canvas = onUi(graph::getCanvas);
      this.scale = canvasToGraphScale(graph);
      this.lookup = graph::getVisibleAreaOwner;
      this.source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
      this.target = pipelineMeta.findTransform(TARGET_TRANSFORM);
      this.hop = pipelineMeta.findPipelineHop(source, target);
      this.note = pipelineMeta.getNote(0);
    }

    /** The graph coordinate to click for a row of the table. */
    Point aim(Where where) {
      return switch (where) {
        case EMPTY, EMPTY_PALETTE_SHOWN, EMPTY_SELECTED -> emptySpot();
        case ICON ->
            awaitIcon(bot, lookup, AreaOwner.AreaType.TRANSFORM_ICON, source, SOURCE_LOCATION);
        case NAME ->
            awaitArea(
                bot,
                graph,
                lookup,
                AreaOwner.AreaType.TRANSFORM_NAME,
                a -> a.getParent() == source);
        case HOP_LINE -> hopLine();
        case HOP_COPY_BADGE ->
            awaitArea(
                bot, graph, lookup, AreaOwner.AreaType.HOP_COPY_ICON, a -> a.getOwner() == hop);
        case INFO_BADGE ->
            awaitArea(
                bot,
                graph,
                lookup,
                AreaOwner.AreaType.TRANSFORM_INFO_ICON,
                a -> a.getOwner() == source);
        case COPIES_BADGE ->
            awaitArea(
                bot,
                graph,
                lookup,
                AreaOwner.AreaType.TRANSFORM_COPIES_TEXT,
                a -> a.getOwner() == source);
        case NOTE ->
            awaitArea(bot, graph, lookup, AreaOwner.AreaType.NOTE, a -> a.getOwner() == note);
      };
    }

    /** A spot of empty canvas, clear of the icons, the note and the minimap. */
    private Point emptySpot() {
      // Wait for the paint first: an empty spot is only empty once the areas are registered.
      awaitIcon(bot, lookup, AreaOwner.AreaType.TRANSFORM_ICON, source, SOURCE_LOCATION);
      Rectangle bounds = onUi(canvas::getBounds);
      Point visible = onUi(() -> graph.screen2real(bounds.width, bounds.height));
      Point empty = new Point((int) (visible.x * 0.60), (int) (visible.y * 0.45));
      assertEmptyCanvas(lookup, empty);
      return empty;
    }

    /**
     * A point on the hop, past the copies badge that sits at 40% of the way and the arrow at the
     * middle, and short of the target icon.
     */
    private Point hopLine() {
      Point from =
          awaitIcon(bot, lookup, AreaOwner.AreaType.TRANSFORM_ICON, source, SOURCE_LOCATION);
      Point to = awaitIcon(bot, lookup, AreaOwner.AreaType.TRANSFORM_ICON, target, TARGET_LOCATION);
      Point on = new Point(from.x + (int) ((to.x - from.x) * 0.7), from.y);
      assertEmptyCanvas(lookup, on);
      assertTrue(
          onUi(() -> graph.findPipelineHop(on.x, on.y) == hop), "expected to aim at the hop");
      return on;
    }

    // -------------------------------------------------------------- side effects

    void sourceSelected() {
      assertTrue(source.isSelected(), "the click should have selected " + SOURCE_TRANSFORM);
      assertEquals(
          List.of(source), pipelineMeta.getSelectedTransforms(), "only the clicked transform");
    }

    void sourceStillSelected() {
      assertTrue(source.isSelected(), SOURCE_TRANSFORM + " should still be selected");
    }

    void noteSelected() {
      assertTrue(note.isSelected(), "the click should have selected the note");
    }

    /** The manual promises the selection is gone and a "Selection cleared" balloon says so. */
    void selectionCleared() {
      nothingSelected();
      HopToolTip toolTip = (HopToolTip) privateField(graph, "toolTip");
      assertTrue(onUi(toolTip::isVisible), "the 'Selection cleared' balloon should be showing");
      assertTrue(
          onUi(toolTip::getText).contains("Selection cleared"),
          "the balloon should say the selection was cleared, not " + onUi(toolTip::getText));
    }

    void nothingSelected() {
      assertTrue(pipelineMeta.getSelectedTransforms().isEmpty(), "no transform may be selected");
      assertTrue(pipelineMeta.getSelectedNotes().isEmpty(), "no note may be selected");
    }
  }

  private void onCanvas(Cell cell, Consumer<Scene> test) {
    Where where = cell.where;
    AtomicReference<HopGuiPipelineGraph> graphRef = new AtomicReference<>();
    AtomicReference<PipelineMeta> metaRef = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setSize(1000, 700);
          shell.setLayout(new FormLayout());
          // The mode of the cell's column, and the palette tree only for its row.
          PropsUi.getInstance().setUseDoubleClickOnCanvas(false);
          PropsUi.getInstance().setUseRightClickForContextDialog(cell.rightClickMode);
          HopConfig.setGuiProperty(
              GraphPalette.CONFIG_KEY, where == Where.EMPTY_PALETTE_SHOWN ? "Y" : "N");

          PipelineMeta pipelineMeta = buildPipeline();
          metaRef.set(pipelineMeta);
          graphRef.set(
              new HopGuiPipelineGraph(
                  shell,
                  hopGui(),
                  new ExplorerPerspective(),
                  pipelineMeta,
                  new HopPipelineFileType<>()));
        },
        bot -> {
          try {
            Scene scene = new Scene(bot, graphRef.get(), metaRef.get());
            if (where == Where.EMPTY_SELECTED) {
              // After the graph exists: its constructor clears every selection.
              onUi(
                  () -> {
                    scene.source.setSelected(true);
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
   * Two Dummy transforms with a hop between them, a note, and every badge the table needs: the
   * source has a description (info badge), runs two copies (copies badge) and copies rows instead
   * of distributing them (copies badge on the hop).
   */
  private static PipelineMeta buildPipeline() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("click-contract");

    TransformMeta source = transform(SOURCE_TRANSFORM, SOURCE_LOCATION);
    source.setDescription("Reads the rows");
    source.setCopiesString("2");
    source.setDistributes(false);
    TransformMeta target = transform(TARGET_TRANSFORM, TARGET_LOCATION);
    pipelineMeta.addTransform(source);
    pipelineMeta.addTransform(target);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(source, target));

    NotePadMeta note = new NotePadMeta("A note", NOTE_LOCATION.x, NOTE_LOCATION.y, 150, 60);
    pipelineMeta.addNote(note);
    return pipelineMeta;
  }

  /** A real Dummy transform, so that the name click can open the real properties dialog. */
  private static TransformMeta transform(String name, Point location) {
    TransformMeta transformMeta = new TransformMeta("Dummy", name, new DummyMeta());
    transformMeta.setLocation(location.x, location.y);
    return transformMeta;
  }

  private static String msg(String key, String... parameters) {
    return BaseMessages.getString(HopGui.class, key, parameters);
  }
}
