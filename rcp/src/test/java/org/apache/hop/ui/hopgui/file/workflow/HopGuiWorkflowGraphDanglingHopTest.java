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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.hopgui.file.GraphCanvasTestBase;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Opening a workflow whose hop names an action that is not in the file - a stale name left behind
 * by a rename or by deleting the action elsewhere - and working on the canvas afterwards.
 *
 * <p>The XML de-serializer resolves an unresolvable reference to null rather than failing the whole
 * file, so such a hop reaches the workflow with one end missing. Half a hop is of no use to anyone:
 * it is not drawn, it is not executed, saving the file writes it back without its target, and the
 * loop check of the next hop the user draws walks straight into it. So the workflow drops it while
 * loading, which is what these tests hold it to.
 *
 * <p>This is the state {@code integration-tests/transforms/main-0012-fuzzymatch.hwf} was in before
 * <a href="https://github.com/apache/hop/pull/7989">#7989</a>, and the workflow side of <a
 * href="https://github.com/apache/hop/issues/8128">issue #8128</a>.
 */
@Tag("uitest")
class HopGuiWorkflowGraphDanglingHopTest extends GraphCanvasTestBase {

  private static final String START_ACTION = "Start";
  private static final String SOURCE_ACTION = "Run Fuzzy match tests";
  private static final String TARGET_ACTION = "Verify results";

  /** The name the hop in the file points at. No action carries it. */
  private static final String MISSING_ACTION = "Run Group By tests";

  private static final Point START_LOCATION = new Point(60, 60);
  private static final Point SOURCE_LOCATION = new Point(280, 60);
  private static final Point TARGET_LOCATION = new Point(500, 60);

  @TempDir private Path folder;

  @BeforeAll
  static void registerGuiPlugins() throws HopException {
    HopGuiEnvironment.init();
  }

  /** Every hop that survives the load has to be a hop the user can see, run and save. */
  @Test
  void openingTheFileLeavesNoHalfBuiltHop() throws Exception {
    WorkflowMeta workflowMeta = openWorkflowFile();

    List<String> halfBuilt = new ArrayList<>();
    for (WorkflowHopMeta hop : workflowMeta.getWorkflowHops()) {
      if (hop.getFromAction() == null || hop.getToAction() == null) {
        halfBuilt.add(hop.toString());
      }
    }

    assertTrue(
        halfBuilt.isEmpty(),
        "the hop names an action that is not in the file, so it must not be kept half built: "
            + halfBuilt);
  }

  /** Opening the file and saving it again may not quietly rewrite the hop without its target. */
  @Test
  void savingTheOpenedFileDoesNotWriteAHopWithoutATarget() throws Exception {
    WorkflowMeta workflowMeta = openWorkflowFile();

    String xml = workflowMeta.getXml(new Variables());
    for (String hop : hopsOf(xml)) {
      assertTrue(
          hop.contains("<from>") && hop.contains("<to>"),
          "saving wrote a hop that has lost an end, corrupting the file further: " + hop);
    }
  }

  /** The canvas has to paint the workflow the user just opened. */
  @Test
  void theCanvasPaintsTheOpenedWorkflow() throws Exception {
    onCanvas(
        (bot, graph, workflowMeta, spots) ->
            assertAll(
                () -> assertNotNull(spots.source, "the source action was never painted"),
                () -> assertNotNull(spots.target, "the target action was never painted"),
                () -> assertNoFailures()));
  }

  /**
   * Drawing a hop on the workflow that was just opened. The loop check runs over every hop of the
   * workflow, so a half-built one would break a gesture that has nothing to do with it.
   */
  @Test
  void drawingANewHopWorksAfterOpeningTheFile() throws Exception {
    onCanvas(
        (bot, graph, workflowMeta, spots) -> {
          ActionMeta source = workflowMeta.findAction(SOURCE_ACTION);
          ActionMeta target = workflowMeta.findAction(TARGET_ACTION);

          // Shift-drag from the source onto the target, exactly as in the hop creation suite.
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

          WorkflowHopMeta created = awaitHop(bot, workflowMeta, source, target);

          assertAll(
              () -> assertNoFailures(),
              () -> assertNull(popup, "no dialog may open here, but one did: " + popup),
              () ->
                  assertNotNull(
                      created,
                      "the gesture should have created the hop " + source + " → " + target));
        });
  }

  /**
   * Waits for the hop the gesture asked for: the graph finishes a gesture through the event loop,
   * so the hop is not there the instant the mouse button comes back up.
   */
  private WorkflowHopMeta awaitHop(
      SWTBot bot, WorkflowMeta workflowMeta, ActionMeta from, ActionMeta to) {
    for (int attempt = 0; attempt < 40; attempt++) {
      WorkflowHopMeta hop = onUi(() -> workflowMeta.findWorkflowHop(from, to));
      if (hop != null) {
        return hop;
      }
      bot.sleep(50);
    }
    return null;
  }

  /**
   * Releases the mouse button and reports the title of the dialog that opened, or null when none
   * did. Any dialog is closed again so the canvas event loop is handed back - a release is
   * dispatched asynchronously because a dialog runs an event loop of its own.
   */
  private String releaseAndCatchDialog(SWTBot bot, Spots spots, Point at, int stateMask) {
    Set<Shell> before = openShells();
    fireAsync(spots.canvas, SWT.MouseUp, spots.scale, at, 1, stateMask);
    Shell popup = awaitNewShell(bot, before);
    String title = titleOf(popup);
    closeShell(bot, popup);
    return title;
  }

  // ------------------------------------------------------------------ assertions

  private void assertNoFailures() {
    assertTrue(swallowed.isEmpty(), "the canvas must not throw, but got " + swallowed);
  }

  // ------------------------------------------------------------------ the file

  /** Loads the workflow from disk the way the Hop GUI does when the user opens the file. */
  private WorkflowMeta openWorkflowFile() throws Exception {
    Path file = folder.resolve("dangling-hop.hwf");
    Files.write(file, workflowXml().getBytes(StandardCharsets.UTF_8));
    return new WorkflowMeta(
        new Variables(), file.toAbsolutePath().toString(), new MemoryMetadataProvider());
  }

  /** Start and two actions, with the only hop pointing at a name no action carries. */
  private static String workflowXml() {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<workflow>\n"
        + "  <name>dangling-hop</name>\n"
        + "  <actions>\n"
        + startAction()
        + dummyAction(SOURCE_ACTION, SOURCE_LOCATION)
        + dummyAction(TARGET_ACTION, TARGET_LOCATION)
        + "  </actions>\n"
        + "  <hops>\n"
        + "    <hop>\n"
        + "      <from>"
        + START_ACTION
        + "</from>\n"
        + "      <to>"
        + MISSING_ACTION
        + "</to>\n"
        + "      <enabled>Y</enabled>\n"
        + "      <evaluation>Y</evaluation>\n"
        + "      <unconditional>Y</unconditional>\n"
        + "    </hop>\n"
        + "  </hops>\n"
        + "</workflow>\n";
  }

  private static String startAction() {
    return "    <action>\n"
        + "      <name>"
        + START_ACTION
        + "</name>\n"
        + "      <type>SPECIAL</type>\n"
        + "      <start>Y</start>\n"
        + "      <repeat>N</repeat>\n"
        + "      <schedulerType>0</schedulerType>\n"
        + "      <intervalSeconds>0</intervalSeconds>\n"
        + "      <intervalMinutes>60</intervalMinutes>\n"
        + "      <hour>12</hour>\n"
        + "      <minutes>0</minutes>\n"
        + "      <weekDay>1</weekDay>\n"
        + "      <DayOfMonth>1</DayOfMonth>\n"
        + "      <parallel>N</parallel>\n"
        + "      <xloc>"
        + START_LOCATION.x
        + "</xloc>\n"
        + "      <yloc>"
        + START_LOCATION.y
        + "</yloc>\n"
        + "      <attributes/>\n"
        + "    </action>\n";
  }

  private static String dummyAction(String name, Point location) {
    return "    <action>\n"
        + "      <name>"
        + name
        + "</name>\n"
        + "      <type>DUMMY</type>\n"
        + "      <parallel>N</parallel>\n"
        + "      <xloc>"
        + location.x
        + "</xloc>\n"
        + "      <yloc>"
        + location.y
        + "</yloc>\n"
        + "      <attributes/>\n"
        + "    </action>\n";
  }

  /** The {@code <hop>} elements of a serialized workflow, one string each. */
  private static List<String> hopsOf(String xml) {
    List<String> hops = new ArrayList<>();
    int from = xml.indexOf("<hop>");
    while (from >= 0) {
      int to = xml.indexOf("</hop>", from);
      if (to < 0) {
        fail("unbalanced <hop> element in the serialized workflow");
      }
      hops.add(xml.substring(from, to + "</hop>".length()));
      from = xml.indexOf("<hop>", to);
    }
    return hops;
  }

  // ------------------------------------------------------------------ scene

  /** What a test needs to aim at: the canvas, its scale and the interesting graph coordinates. */
  private record Spots(Canvas canvas, double scale, Point source, Point target) {}

  @FunctionalInterface
  private interface CanvasTest {
    void run(SWTBot bot, HopGuiWorkflowGraph graph, WorkflowMeta workflowMeta, Spots spots);
  }

  private void onCanvas(CanvasTest test) throws Exception {
    WorkflowMeta workflowMeta = openWorkflowFile();
    AtomicReference<HopGuiWorkflowGraph> graphRef = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setSize(1000, 700);
          shell.setLayout(new GridLayout(1, false));
          PropsUi.getInstance().setUseDoubleClickOnCanvas(false);

          graphRef.set(
              new HopGuiWorkflowGraph(
                  shell,
                  hopGui(),
                  new TreelessExplorerPerspective(),
                  workflowMeta,
                  new HopWorkflowFileType<>()));
          attachKeyboardShortcuts(shell);
        },
        bot -> {
          HopGuiWorkflowGraph graph = graphRef.get();
          test.run(bot, graph, workflowMeta, aim(bot, graph, workflowMeta));
        });
  }

  /**
   * The perspective of a test never built its file tree - that happens when the real application
   * shell opens - so the tree update a file-backed graph asks for on every refresh has to be a
   * no-op here. The graph itself is the real one.
   */
  private static class TreelessExplorerPerspective extends ExplorerPerspective {
    @Override
    public void updateTreeItem(IHopFileTypeHandler fileTypeHandler) {
      // the tree this would walk is only built by the application shell
    }
  }

  private Spots aim(SWTBot bot, HopGuiWorkflowGraph graph, WorkflowMeta workflowMeta) {
    Canvas canvas = onUi(graph::getCanvas);
    double scale = canvasToGraphScale(graph);
    AreaLookup lookup = graph::getVisibleAreaOwner;

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

    assertEquals(3, workflowMeta.nrActions(), "the file holds Start and two actions");
    return new Spots(canvas, scale, source, target);
  }
}
