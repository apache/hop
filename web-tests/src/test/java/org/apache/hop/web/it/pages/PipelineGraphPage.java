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

package org.apache.hop.web.it.pages;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.WebDriverWait;

/**
 * The pipeline graph of the active tab.
 *
 * <p>The graph is drawn on a {@code <canvas>}, so there is nothing to click by name. Hop Web lays
 * an SVG the server rendered over that canvas and publishes, on the same element, the model behind
 * that picture: the area owners, which say where each transform was drawn and what it is called
 * (see {@code canvas-svg.js}). This class reads that model and clicks at the coordinates it gives.
 *
 * <p>Reading the SVG instead does not work. Its {@code <text>} nodes are whatever the drawing
 * happens to contain, and a dozen transform icons spell something themselves - "AWSSNS", "csv",
 * "AXO", "&lt;/&gt;" - so an icon is indistinguishable from a transform name. Every one of those
 * transforms failed the dialog sweep for that reason alone.
 */
public class PipelineGraphPage {

  /**
   * The overlay Hop Web draws the graph on, and hangs the graph model off.
   *
   * <p>One per session rather than one per tab: the client keeps a single renderer and moves its
   * overlay to whichever canvas is on top, so there is nothing to disambiguate.
   */
  private static final String OVERLAY =
      "const overlay=[...document.querySelectorAll('[data-hop-canvas=\"graph\"]')]"
          + ".find(e=>e.offsetParent!==null)"
          + "||document.querySelector('[data-hop-canvas=\"graph\"]');";

  /** Every transform on the graph, with the rectangle it is drawn in right now. */
  private static final String NODES =
      OVERLAY + "return (overlay&&overlay.hopGraph)? overlay.hopGraph.nodes() : [];";

  /**
   * The graph canvas of the tab on screen.
   *
   * <p>Taken from the widget Hop names rather than by size: several tabs have a canvas of exactly
   * the same size in exactly the same place, and a perspective that is not on top keeps its own.
   */
  private static final String GRAPH_CANVAS =
      "const widget=[...document.querySelectorAll('[data-hop-id=\"pipeline-graph-canvas\"]')]"
          + ".find(e=>e.offsetParent!==null);"
          + "if(widget){const inner=widget.querySelector('canvas');if(inner)return inner;}"
          + "return [...document.querySelectorAll('canvas')].find(c=>{"
          + "const r=c.getBoundingClientRect();"
          + "return r.width>500&&r.height>400&&c.offsetParent!==null;})||null;";

  /**
   * Where a transform sits relative to the middle of the canvas, which is how a click is addressed,
   * plus how big its icon is drawn.
   *
   * <p>The model gives viewport coordinates, so the canvas it has to be measured against is read at
   * the same moment: a scrolled or resized window moves one without moving the other.
   */
  private static final String TRANSFORM_AT =
      OVERLAY
          + "if(!overlay||!overlay.hopGraph)return null;"
          + "const name=arguments[0];const canvas=arguments[1];"
          + "const node=overlay.hopGraph.nodes().find(n=>n.name===name);"
          + "if(!node)return null;"
          + "const r=canvas.getBoundingClientRect();"
          + "return [Math.round(node.viewportX-(r.x+r.width/2)),"
          + "Math.round(node.viewportY-(r.y+r.height/2)),"
          + "Math.round(node.width)];";

  private final WebDriver driver;
  private final WebDriverWait wait;

  PipelineGraphPage(WebDriver driver, WebDriverWait wait) {
    this.driver = driver;
    this.wait = wait;
  }

  void awaitReady() {
    wait.until(d -> canvas() != null && hasGraphModel());
    // Also the wait for the new tab to actually be the one on screen.
    wait.until(d -> isEmpty());
  }

  /** Waits until the graph on screen is the one holding this transform. */
  void awaitLabel(String label) {
    wait.until(d -> canvas() != null && contains(label));
  }

  /** Whether the client has published a graph yet, which it does on the first paint. */
  private boolean hasGraphModel() {
    return Boolean.TRUE.equals(
        ((JavascriptExecutor) driver)
            .executeScript(OVERLAY + "return !!(overlay&&overlay.hopGraph);"));
  }

  /** Whether the graph holds no transforms. */
  public boolean isEmpty() {
    return transformNames().isEmpty();
  }

  public WebElement canvas() {
    return (WebElement) ((JavascriptExecutor) driver).executeScript(GRAPH_CANVAS);
  }

  /** The transforms on the graph, named as the pipeline names them. */
  public List<String> transformNames() {
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> nodes =
        (List<Map<String, Object>>) ((JavascriptExecutor) driver).executeScript(NODES);
    return nodes.stream().map(node -> String.valueOf(node.get("name"))).toList();
  }

  /** What the graph holds, under the name the tests have always called it. */
  public List<String> labels() {
    return transformNames();
  }

  public boolean contains(String label) {
    return transformNames().contains(label);
  }

  private long count(String transformName) {
    return transformNames().stream().filter(transformName::equals).count();
  }

  /** How many entries after the highlight to try before giving up on a name. */
  private static final int MAX_CANDIDATES = 3;

  /**
   * Drops a transform on the graph, at an offset from the middle of the canvas.
   *
   * <p>Typing the name and accepting the highlight is not enough on its own, because the context
   * dialog does not always rank an exact name match first - see {@link ContextDialog#choose(String,
   * int)}. So this waits for the transform it asked for, and if something else turned up instead,
   * removes it and takes the next entry.
   */
  public void addTransform(String transformName, int offsetX, int offsetY) {
    // How many of these the graph held before, rather than whether it held any: a transform is not
    // always the first thing added to a pipeline.
    long before = count(transformName);
    List<String> namesBefore = transformNames();
    for (int candidate = 0; candidate < MAX_CANDIDATES; candidate++) {
      clickAt(offsetX, offsetY);
      contextDialog().choose(transformName, candidate);

      if (awaitAdded(transformName, before)) {
        return;
      }
      String wrong = somethingElseAdded(namesBefore);
      if (wrong != null) {
        actOnTransform(wrong, "Delete", offsetX, offsetY);
        wait.until(d -> transformNames().size() == namesBefore.size());
      }
    }
    throw new AssertionError(
        "Could not add '"
            + transformName
            + "': none of the first "
            + MAX_CANDIDATES
            + " entries matching that search was it, the graph holds "
            + transformNames());
  }

  /** Whether one more transform of this name turned up within the short wait. */
  private boolean awaitAdded(String transformName, long before) {
    try {
      shortWait().until(d -> count(transformName) > before);
      return true;
    } catch (TimeoutException e) {
      return false;
    }
  }

  /** The name of whatever else landed on the graph, or null if nothing did. */
  private String somethingElseAdded(List<String> namesBefore) {
    return transformNames().stream()
        .filter(name -> !namesBefore.contains(name))
        .findFirst()
        .orElse(null);
  }

  private WebDriverWait shortWait() {
    return HopGuiPage.waitFor(driver, Duration.ofSeconds(5));
  }

  public void addTransform(String transformName) {
    addTransform(transformName, 0, 0);
  }

  /** Opens the transform's own context dialog by clicking it, and picks an entry from it. */
  public void actOnTransform(String transformName, String action, int offsetX, int offsetY) {
    clickAt(offsetX, offsetY);
    ContextDialog dialog = contextDialog();
    // The context dialog of a transform names it in the title, which is the cheapest confirmation
    // that the click landed on the transform and not on empty canvas.
    wait.until(d -> dialog.title() != null && dialog.title().contains(transformName));
    dialog.choose(action);
  }

  public ContextDialog contextDialog() {
    ContextDialog dialog = new ContextDialog(driver, wait);
    try {
      dialog.awaitOpen();
    } catch (TimeoutException e) {
      // The usual reason the context dialog never comes is that Hop has a modal error dialog up.
      HopGuiPage.failIfErrorDialog(driver);
      throw e;
    }
    return dialog;
  }

  /** The offset from the middle of the canvas at which this transform is drawn. */
  public int[] transformOffset(String transformName) {
    List<Number> drawn = drawnAt(transformName);
    return new int[] {drawn.get(0).intValue(), drawn.get(1).intValue()};
  }

  /** How big this transform's icon is drawn, which is what a zoom level changes. */
  public int transformIconSize(String transformName) {
    return drawnAt(transformName).get(2).intValue();
  }

  private List<Number> drawnAt(String transformName) {
    @SuppressWarnings("unchecked")
    List<Number> drawn =
        (List<Number>)
            ((JavascriptExecutor) driver).executeScript(TRANSFORM_AT, transformName, canvas());
    if (drawn == null) {
      throw new AssertionError(
          "The graph does not hold a transform called '"
              + transformName
              + "', only "
              + transformNames());
    }
    return drawn;
  }

  /**
   * Drags a transform across the canvas.
   *
   * <p>In steps rather than in one jump: a single move is one mouse event, and the graph only
   * starts a drag once it has seen the pointer move while the button is down.
   */
  public void dragTransform(String transformName, int dx, int dy) {
    int[] from = transformOffset(transformName);
    new Actions(driver)
        .moveToElement(canvas(), from[0], from[1])
        .clickAndHold()
        .moveByOffset(dx / 2, dy / 2)
        .moveByOffset(dx - dx / 2, dy - dy / 2)
        .release()
        .perform();
  }

  /** Undoes the last change to the graph. */
  public void undo(HopGuiPage hopGui) {
    hopGui.clickWidget(UNDO);
  }

  /** Redoes what was undone. */
  public void redo(HopGuiPage hopGui) {
    hopGui.clickWidget(REDO);
  }

  /** Clicks a transform where it is drawn, wherever the pipeline put it. */
  public void clickTransform(String transformName) {
    int[] offset = transformOffset(transformName);
    clickAt(offset[0], offset[1]);
  }

  /** Opens a transform's context dialog where the transform is, and picks an entry from it. */
  public void actOnTransform(String transformName, String action) {
    clickTransform(transformName);
    ContextDialog dialog = contextDialog();
    wait.until(d -> dialog.title() != null && dialog.title().contains(transformName));
    dialog.choose(action);
  }

  /** Clicks the canvas at an offset from its middle. */
  public void clickAt(int offsetX, int offsetY) {
    new Actions(driver).moveToElement(canvas(), offsetX, offsetY).click().perform();
  }

  /**
   * Previews one transform and waits for its rows.
   *
   * <p>Two dialogs deep: the context action opens the debug dialog, where "Quick Launch" runs the
   * pipeline far enough to fill the transform's preview and then shows the rows.
   */
  public PreviewDataDialog preview(HopGuiPage hopGui, String transformName) {
    actOnTransform(transformName, "Preview & debug output");
    hopGui.awaitDialog();
    hopGui.clickButton("Quick Launch");
    HopGuiPage.waitFor(driver, EXECUTION_TIMEOUT)
        .until(d -> "Examine preview data".equals(hopGui.topDialogTitle()));
    return new PreviewDataDialog(driver, hopGui);
  }

  /** The graph's own toolbar, whose ids are declared on {@code HopGuiPipelineGraph}. */
  private static final String TOOLBAR = "HopGuiPipelineGraph-ToolBar-";

  public static final By RUN = HopGuiPage.testId(TOOLBAR + "10010-Run");
  public static final By PREVIEW = HopGuiPage.testId(TOOLBAR + "10050-Preview");
  public static final By UNDO = HopGuiPage.testId(TOOLBAR + "10100-Undo");
  public static final By REDO = HopGuiPage.testId(TOOLBAR + "10110-Redo");
  public static final By ZOOM_IN = HopGuiPage.testId(TOOLBAR + "10520-Zoom-In");
  public static final By ZOOM_OUT = HopGuiPage.testId(TOOLBAR + "10510-Zoom-Out");

  /** How long a sample pipeline may take to start, run and report itself finished. */
  private static final Duration EXECUTION_TIMEOUT = Duration.ofSeconds(60);

  /**
   * Runs the pipeline and waits until every transform reports itself finished.
   *
   * <p>Only pipelines that have been saved can be run: Hop asks an unsaved one to be saved first,
   * which is a different dialog and a different test.
   */
  public ExecutionResultsPanel run(HopGuiPage hopGui, String... transforms) {
    hopGui.clickWidget(RUN);
    // "Run Options" carries the run configuration, the log level and the parameters. Everything
    // in it is already set the way the pipeline wants, so the test only has to launch.
    hopGui.awaitDialog();
    hopGui.clickButton("Launch");
    hopGui.awaitNoDialog();

    ExecutionResultsPanel results = new ExecutionResultsPanel(driver, hopGui);
    results.selectTab("Metrics");
    // Waiting for "everything on screen has finished" is not enough: the table fills in transform
    // by transform, so the first transform to finish would satisfy it while the rest are still
    // running. The caller says which transforms it expects, and all of them have to be there.
    HopGuiPage.waitFor(driver, EXECUTION_TIMEOUT)
        .until(
            d -> {
              List<Map<String, String>> metrics = results.metrics();
              return metrics.size() >= transforms.length
                  && metrics.stream().allMatch(row -> "Finished".equals(row.get("Status")))
                  && Stream.of(transforms).allMatch(name -> results.metricsOf(name) != null);
            });
    return results;
  }
}
