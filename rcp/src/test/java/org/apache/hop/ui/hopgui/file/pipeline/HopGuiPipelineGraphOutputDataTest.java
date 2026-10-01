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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.hopgui.file.GraphCanvasTestBase;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Issue #8595: the output-rows badge sits on the corner of a transform and part way along a hop, so
 * the mouse-up of a move often lands on it. That release ends the move. A click on the badge still
 * opens the rows, once, and leaves the canvas idle so the next click is not another move.
 */
@Tag("uitest")
class HopGuiPipelineGraphOutputDataTest extends GraphCanvasTestBase {

  private static final String SOURCE_TRANSFORM = "Read";
  private static final String TARGET_TRANSFORM = "Write";
  private static final Point SOURCE_LOCATION = new Point(80, 80);
  private static final Point TARGET_LOCATION = new Point(320, 80);
  private static final int DRAG_Y = 160;

  @BeforeAll
  static void registerGuiPlugins() throws HopException {
    HopGuiEnvironment.init();
  }

  @Test
  void clickingTheBadgeOpensTheRowsOnce() {
    onCanvas(
        scene -> {
          Point badge = scene.outputBadge(scene.source);
          Point before = scene.locationOf(scene.source);

          Set<Shell> openBefore = openShells();
          fireAsync(scene.canvas, SWT.MouseDown, scene.scale, badge, 1, SWT.NONE);
          fireAsync(scene.canvas, SWT.MouseUp, scene.scale, badge, 1, SWT.BUTTON1);
          Shell rows = awaitNewShell(scene.bot, openBefore);
          assertNotNull(rows, "a click on the output-data badge opens the rows");
          assertEquals(scene.transformRowsTitle, titleOf(rows));

          // The rows dialog runs its own event loop, so this second click is delivered while the
          // first dialog is still up. It must not open another one, or stack them until Escape.
          fireAsync(scene.canvas, SWT.MouseDown, scene.scale, badge, 1, SWT.NONE);
          fireAsync(scene.canvas, SWT.MouseUp, scene.scale, badge, 1, SWT.BUTTON1);

          assertEquals(List.of(scene.transformRowsTitle), catchDialogs(scene.bot, openBefore));
          assertEquals(
              before, scene.locationOf(scene.source), "a click must not move the transform");
          assertTrue(scene.source.isSelected(), "viewing the rows keeps the selection");
          assertIdle(scene.graph);
        });
  }

  @Test
  void releasingADragOnTheBadgeDoesNotOpenTheRows() {
    onCanvas(
        scene -> {
          Point icon = scene.icon(scene.source);
          Point beforeSource = scene.locationOf(scene.source);
          Point beforeTarget = scene.locationOf(scene.target);

          fire(scene.canvas, SWT.MouseDown, scene.scale, icon, 1, SWT.NONE);
          Point drop = new Point(icon.x, icon.y + DRAG_Y);
          fire(scene.canvas, SWT.MouseMove, scene.scale, drop, 0, SWT.BUTTON1);

          Point badge = scene.outputBadge(scene.source);
          List<String> dialogs = release(scene, badge);

          Point afterSource = scene.locationOf(scene.source);
          Point afterTarget = scene.locationOf(scene.target);
          assertTrue(dialogs.isEmpty(), "releasing a drag on the output-data badge opens no rows");
          assertTrue(
              afterSource.y - beforeSource.y > 100, "the dragged transform should have moved down");
          assertEquals(beforeSource.x, afterSource.x);
          assertEquals(
              afterSource.y - beforeSource.y,
              afterTarget.y - beforeTarget.y,
              "every selected transform moves together");
          assertIdle(scene.graph);

          // The next click is a click again: one rows dialog, and the transform stays where the
          // drag left it.
          List<String> after = click(scene, scene.outputBadge(scene.source));
          assertEquals(List.of(scene.transformRowsTitle), after);
          assertEquals(afterSource, scene.locationOf(scene.source));
          assertIdle(scene.graph);
        });
  }

  @Test
  void draggingFromTheBadgeMovesTheTransform() {
    onCanvas(
        scene -> {
          Point badge = scene.outputBadge(scene.source);
          Point before = scene.locationOf(scene.source);
          Point drop = new Point(badge.x, badge.y + DRAG_Y);

          fire(scene.canvas, SWT.MouseDown, scene.scale, badge, 1, SWT.NONE);
          fire(scene.canvas, SWT.MouseMove, scene.scale, drop, 0, SWT.BUTTON1);
          // The badge was the grab point, so it is still under the pointer when the button comes
          // up.
          List<String> dialogs = release(scene, drop);

          assertTrue(
              dialogs.isEmpty(),
              "a drag that starts on the output-data badge does not open the rows");
          assertTrue(scene.locationOf(scene.source).y - before.y > 100);
          assertIdle(scene.graph);
        });
  }

  @Test
  void hopBadgeClickOpensRowsAndADragReleaseDoesNot() {
    onCanvas(
        scene -> {
          List<String> clicked = click(scene, scene.hopBadge());
          assertEquals(List.of(scene.hopRowsTitle), clicked);
          assertIdle(scene.graph);

          Point icon = scene.icon(scene.source);
          Point before = scene.locationOf(scene.source);
          fire(scene.canvas, SWT.MouseDown, scene.scale, icon, 1, SWT.NONE);
          fire(
              scene.canvas,
              SWT.MouseMove,
              scene.scale,
              new Point(icon.x, icon.y + DRAG_Y),
              0,
              SWT.BUTTON1);
          List<String> dragged = release(scene, scene.hopBadge());

          assertTrue(
              dragged.isEmpty(), "releasing a drag on the hop output-data badge opens no rows");
          assertTrue(scene.locationOf(scene.source).y - before.y > 100);
          assertIdle(scene.graph);
        });
  }

  private List<String> click(Scene scene, Point at) {
    return clickAndCatchDialogs(scene.bot, scene.canvas, scene.scale, at, 1, SWT.NONE);
  }

  /** Mouse-up posted without waiting, so a rows dialog cannot deadlock the test. */
  private List<String> release(Scene scene, Point at) {
    Set<Shell> before = openShells();
    fireAsync(scene.canvas, SWT.MouseUp, scene.scale, at, 1, SWT.BUTTON1);
    return catchDialogs(scene.bot, before);
  }

  private static void assertIdle(HopGuiPipelineGraph graph) {
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
    idle.put("iconDragCommitted", false);
    idle.put("outputDataPressed", false);
    idle.put("showingOutputRows", false);
    idle.put("avoidContextDialog", false);
    idle.put("lastButton", 0);
    idle.put("clickedPipelineHop", null);
    assertGraphState(graph, idle);
  }

  private void onCanvas(Consumer<Scene> test) {
    AtomicReference<HopGuiPipelineGraph> graphRef = new AtomicReference<>();
    AtomicReference<PipelineMeta> metaRef = new AtomicReference<>();
    int[] previousGrid = new int[1];

    withScene(
        shell -> {
          shell.setSize(1000, 700);
          shell.setLayout(new FormLayout());
          PropsUi props = PropsUi.getInstance();
          previousGrid[0] = props.getCanvasGridSize();
          props.setCanvasGridSize(1);
          props.setUseDoubleClickOnCanvas(false);
          props.setUseRightClickForContextDialog(false);

          PipelineMeta pipelineMeta = buildPipeline();
          metaRef.set(pipelineMeta);
          HopGuiPipelineGraph graph =
              new HopGuiPipelineGraph(
                  shell,
                  hopGui(),
                  new ExplorerPerspective(),
                  pipelineMeta,
                  new HopPipelineFileType<>());
          graph.pipeline = new LocalPipelineEngine(pipelineMeta);
          graph.setOutputRowsMap(outputRows());
          graph.setOutputHopRowsMap(hopRows(pipelineMeta));
          pipelineMeta.findTransform(SOURCE_TRANSFORM).setSelected(true);
          pipelineMeta.findTransform(TARGET_TRANSFORM).setSelected(true);
          graphRef.set(graph);
        },
        bot -> {
          try {
            test.accept(new Scene(bot, graphRef.get(), metaRef.get()));
          } finally {
            onUi(() -> PropsUi.getInstance().setCanvasGridSize(previousGrid[0]));
          }
        });
  }

  private static PipelineMeta buildPipeline() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("output-data");
    TransformMeta source = transform(SOURCE_TRANSFORM, SOURCE_LOCATION);
    TransformMeta target = transform(TARGET_TRANSFORM, TARGET_LOCATION);
    pipelineMeta.addTransform(source);
    pipelineMeta.addTransform(target);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(source, target));
    return pipelineMeta;
  }

  private static TransformMeta transform(String name, Point location) {
    TransformMeta transformMeta = new TransformMeta("Dummy", name, new DummyMeta());
    transformMeta.setLocation(location.x, location.y);
    return transformMeta;
  }

  private static Map<String, RowBuffer> outputRows() {
    Map<String, RowBuffer> rows = new HashMap<>();
    rows.put(SOURCE_TRANSFORM, sample());
    rows.put(TARGET_TRANSFORM, sample());
    return rows;
  }

  private static Map<String, RowBuffer> hopRows(PipelineMeta pipelineMeta) {
    TransformMeta source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
    TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);
    Map<String, RowBuffer> rows = new HashMap<>();
    rows.put(source.getName() + "\t" + target.getName(), sample());
    return rows;
  }

  private static RowBuffer sample() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id"));
    RowBuffer buffer = new RowBuffer(rowMeta);
    buffer.addRow("1");
    return buffer;
  }

  private static String rowsTitle(String name) {
    return BaseMessages.getString(
        HopGui.class, "PipelineGraph.ViewOutput.OutputDialog.Header", name);
  }

  private final class Scene {
    final SWTBot bot;
    final HopGuiPipelineGraph graph;
    final PipelineMeta pipelineMeta;
    final Canvas canvas;
    final double scale;
    final TransformMeta source;
    final TransformMeta target;
    final String transformRowsTitle;
    final String hopRowsTitle;

    Scene(SWTBot bot, HopGuiPipelineGraph graph, PipelineMeta pipelineMeta) {
      this.bot = bot;
      this.graph = graph;
      this.pipelineMeta = pipelineMeta;
      this.canvas = onUi(graph::getCanvas);
      this.scale = canvasToGraphScale(graph);
      this.source = pipelineMeta.findTransform(SOURCE_TRANSFORM);
      this.target = pipelineMeta.findTransform(TARGET_TRANSFORM);
      this.transformRowsTitle = rowsTitle(SOURCE_TRANSFORM);
      this.hopRowsTitle = rowsTitle(SOURCE_TRANSFORM + " → " + TARGET_TRANSFORM);
    }

    Point icon(TransformMeta transform) {
      return awaitIcon(
          bot,
          graph::getVisibleAreaOwner,
          AreaOwner.AreaType.TRANSFORM_ICON,
          transform,
          transform == source ? SOURCE_LOCATION : TARGET_LOCATION);
    }

    Point outputBadge(TransformMeta transform) {
      return awaitArea(
          bot,
          graph,
          graph::getVisibleAreaOwner,
          AreaOwner.AreaType.TRANSFORM_OUTPUT_DATA,
          area -> area.getParent() == transform);
    }

    Point hopBadge() {
      PipelineHopMeta hop = pipelineMeta.findPipelineHop(source, target);
      return awaitArea(
          bot,
          graph,
          graph::getVisibleAreaOwner,
          AreaOwner.AreaType.HOP_OUTPUT_DATA,
          area -> area.getParent() == hop);
    }

    Point locationOf(TransformMeta transform) {
      Point location = onUi(transform::getLocation);
      return new Point(location.x, location.y);
    }
  }
}
