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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Point;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
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
 * Enabling and disabling a hop straight from the canvas, with ctrl-click and with the middle mouse
 * button. Next to the state of the hop itself, each test checks what the engine will see: {@link
 * PipelineMeta} caches the previous transforms of a transform, and a hop that is disabled without
 * invalidating that cache leaves the transform looking for a row set which was never allocated,
 * failing the next run with "Unable to find input rowset!".
 *
 * <p>Covers <a href="https://github.com/apache/hop/issues/7841">issue #7841</a>.
 */
@Tag("uitest")
class HopGuiPipelineGraphHopEnablingTest extends GraphCanvasTestBase {

  private static final String FIRST_INPUT = "First input";
  private static final String SECOND_INPUT = "Second input";
  private static final String TARGET_TRANSFORM = "Write to log";
  private static final Point FIRST_INPUT_LOCATION = new Point(60, 60);
  private static final Point SECOND_INPUT_LOCATION = new Point(60, 300);
  private static final Point TARGET_LOCATION = new Point(460, 180);

  @BeforeAll
  static void registerGuiPlugins() throws HopException {
    HopGuiEnvironment.init();
  }

  @Test
  void ctrlClickingAHopDisablesItAndTellsTheEngineAboutIt() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) ->
            assertTogglingHop(bot, graph, pipelineMeta, spots, 1, SWT.MOD1));
  }

  @Test
  void middleClickingAHopDisablesItAndTellsTheEngineAboutIt() {
    onCanvas(
        (bot, graph, pipelineMeta, spots) ->
            assertTogglingHop(bot, graph, pipelineMeta, spots, 2, SWT.NONE));
  }

  /**
   * Disables the second hop into the target with the given gesture and enables it again, checking
   * the hop and the previous transforms of the target after each click.
   */
  private void assertTogglingHop(
      SWTBot bot,
      HopGuiPipelineGraph graph,
      PipelineMeta pipelineMeta,
      Spots spots,
      int button,
      int stateMask) {
    TransformMeta first = pipelineMeta.findTransform(FIRST_INPUT);
    TransformMeta second = pipelineMeta.findTransform(SECOND_INPUT);
    TransformMeta target = pipelineMeta.findTransform(TARGET_TRANSFORM);
    PipelineHopMeta hop = pipelineMeta.findPipelineHop(second, target);
    assertNotNull(hop, "the scene should hold the hop that is toggled here");

    // Whatever the user did before reaching for the hop - a run, a dialog - left the previous
    // transforms of the target cached, with both hops still enabled.
    assertPreviousTransforms(pipelineMeta, target, first, second);

    String disablingPopup = clickHop(bot, spots, button, stateMask);
    assertAll(
        "ctrl/middle click should disable the hop",
        () -> assertFalse(hop.isEnabled(), "the click should have disabled the hop"),
        () -> assertPreviousTransforms(pipelineMeta, target, first),
        () -> assertNoDialog(disablingPopup),
        () -> assertNoFailures());

    String enablingPopup = clickHop(bot, spots, button, stateMask);
    assertAll(
        "clicking the hop again should enable it",
        () -> assertTrue(hop.isEnabled(), "the second click should have enabled the hop again"),
        () -> assertPreviousTransforms(pipelineMeta, target, first, second),
        () -> assertNoDialog(enablingPopup),
        () -> assertNoFailures());
  }

  /** Clicks the hop and reports the title of any dialog that opened, which there should not be. */
  private String clickHop(SWTBot bot, Spots spots, int button, int stateMask) {
    Set<Shell> before = openShells();
    fire(spots.canvas, SWT.MouseDown, spots.scale, spots.hop, button, stateMask);
    fireAsync(
        spots.canvas, SWT.MouseUp, spots.scale, spots.hop, button, stateMask | buttonMask(button));
    Shell popup = awaitNewShell(bot, before);
    String title = titleOf(popup);
    closeShell(bot, popup);
    return title;
  }

  private static int buttonMask(int button) {
    return button == 2 ? SWT.BUTTON2 : SWT.BUTTON1;
  }

  // ------------------------------------------------------------------ assertions

  /**
   * The transforms feeding {@code target}, as the engine asks for them when it hands every
   * transform its input row sets. A disabled hop may not show up here.
   */
  private void assertPreviousTransforms(
      PipelineMeta pipelineMeta, TransformMeta target, TransformMeta... expected) {
    List<String> names =
        onUi(() -> pipelineMeta.findPreviousTransforms(target, true)).stream()
            .map(TransformMeta::getName)
            .sorted()
            .toList();
    List<String> wanted = Arrays.stream(expected).map(TransformMeta::getName).sorted().toList();
    assertEquals(
        wanted, names, "the transforms the engine will read " + target.getName() + " from");
  }

  private void assertNoDialog(String dialogTitle) {
    assertNull(dialogTitle, "no dialog may open here, but one did");
  }

  private void assertNoFailures() {
    assertTrue(swallowed.isEmpty(), "the canvas must not throw, but got " + swallowed);
  }

  // ------------------------------------------------------------------ scene

  /** What a test needs to aim at: the canvas, its scale and the middle of the hop under test. */
  private record Spots(Canvas canvas, double scale, Point hop) {}

  @FunctionalInterface
  private interface CanvasTest {
    void run(SWTBot bot, HopGuiPipelineGraph graph, PipelineMeta pipelineMeta, Spots spots);
  }

  private void onCanvas(CanvasTest test) {
    AtomicReference<HopGuiPipelineGraph> graphRef = new AtomicReference<>();
    AtomicReference<PipelineMeta> metaRef = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setSize(1000, 700);
          shell.setLayout(new FormLayout());
          // Keep the single click on the canvas synchronous and deterministic.
          PropsUi.getInstance().setUseDoubleClickOnCanvas(false);

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
          HopGuiPipelineGraph graph = graphRef.get();
          PipelineMeta pipelineMeta = metaRef.get();
          test.run(bot, graph, pipelineMeta, aim(bot, graph, pipelineMeta));
        });
  }

  /**
   * Locates the icons and returns the middle of the hop between the second input and the target.
   */
  private Spots aim(SWTBot bot, HopGuiPipelineGraph graph, PipelineMeta pipelineMeta) {
    Canvas canvas = onUi(graph::getCanvas);
    double scale = canvasToGraphScale(graph);
    AreaLookup lookup = graph::getVisibleAreaOwner;

    awaitIcon(
        bot,
        lookup,
        AreaOwner.AreaType.TRANSFORM_ICON,
        pipelineMeta.findTransform(FIRST_INPUT),
        FIRST_INPUT_LOCATION);
    Point second =
        awaitIcon(
            bot,
            lookup,
            AreaOwner.AreaType.TRANSFORM_ICON,
            pipelineMeta.findTransform(SECOND_INPUT),
            SECOND_INPUT_LOCATION);
    Point target =
        awaitIcon(
            bot,
            lookup,
            AreaOwner.AreaType.TRANSFORM_ICON,
            pipelineMeta.findTransform(TARGET_TRANSFORM),
            TARGET_LOCATION);

    // The hop is a line between the two icon centres, which is how the graph looks it up too.
    return new Spots(canvas, scale, midpoint(second, target));
  }

  /** Two transforms feeding a third one, so that disabling one hop still leaves the target fed. */
  private static PipelineMeta buildPipeline() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("hop-enabling");

    TransformMeta first = transform(FIRST_INPUT, FIRST_INPUT_LOCATION);
    TransformMeta second = transform(SECOND_INPUT, SECOND_INPUT_LOCATION);
    TransformMeta target = transform(TARGET_TRANSFORM, TARGET_LOCATION);
    pipelineMeta.addTransform(first);
    pipelineMeta.addTransform(second);
    pipelineMeta.addTransform(target);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(first, target));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(second, target));
    return pipelineMeta;
  }

  private static TransformMeta transform(String name, Point location) {
    TransformMeta transformMeta =
        new TransformMeta("Dummy", name, new BaseTransformMeta<ITransform, ITransformData>());
    transformMeta.setLocation(location.x, location.y);
    return transformMeta;
  }
}
