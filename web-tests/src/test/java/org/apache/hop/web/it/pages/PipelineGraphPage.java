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
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.WebDriverWait;

/**
 * The pipeline graph of the active tab.
 *
 * <p>The graph is drawn on a {@code <canvas>}, so there is nothing to click by name. What it does
 * have is the SVG that Hop Web renders server side and lays over that canvas (see {@code
 * CanvasSvgFacadeImpl}); its {@code <text>} nodes carry the transform names, which is what the
 * assertions here read. Interaction still goes to the canvas, but only ever at coordinates this
 * class chose itself.
 */
public class PipelineGraphPage {

  /** The graph canvas is the only large visible canvas; every other one is a sash or a header. */
  private static final String GRAPH_CANVAS =
      "return [...document.querySelectorAll('canvas')].find(c=>{"
          + "const r=c.getBoundingClientRect();"
          + "return r.width>500&&r.height>400&&c.offsetParent!==null;})||null;";

  private static final String GRAPH_TEXTS =
      "const svg=[...document.querySelectorAll('svg')].find(s=>s.getBoundingClientRect().width>100);"
          + "return svg? [...svg.querySelectorAll('text')].map(t=>t.textContent.trim())"
          + ".filter(t=>t.length>0) : [];";

  /**
   * What an empty graph draws: the hint telling you where to click. Captured once from the first
   * pipeline of the session, which is empty beyond doubt because nothing else exists yet.
   *
   * <p>It has to be a session constant rather than something each page re-reads. Reading it per
   * pipeline raced the tab switch: the labels still belonged to the previous tab, so a leftover
   * transform got recorded as what "empty" looks like, and nothing added afterwards ever registered
   * as new.
   */
  private static List<String> emptyLabels;

  private final WebDriver driver;
  private final WebDriverWait wait;

  PipelineGraphPage(WebDriver driver, WebDriverWait wait) {
    this.driver = driver;
    this.wait = wait;
  }

  void awaitReady() {
    wait.until(d -> canvas() != null);
    if (emptyLabels == null) {
      wait.until(d -> !labels().isEmpty());
      emptyLabels = labels();
    } else {
      // Also the wait for the new tab to actually be the one on screen.
      wait.until(d -> isEmpty());
    }
  }

  /** Whether the graph holds no transforms, as opposed to holding no labels at all. */
  public boolean isEmpty() {
    return labels().equals(emptyLabels);
  }

  public WebElement canvas() {
    return (WebElement) ((JavascriptExecutor) driver).executeScript(GRAPH_CANVAS);
  }

  /** Every label currently drawn on the graph: transform names, and hop and note text. */
  public List<String> labels() {
    @SuppressWarnings("unchecked")
    List<String> texts = (List<String>) ((JavascriptExecutor) driver).executeScript(GRAPH_TEXTS);
    return texts;
  }

  public boolean contains(String label) {
    return labels().contains(label);
  }

  /** How many entries after the highlight to try before giving up on a name. */
  private static final int MAX_CANDIDATES = 3;

  /**
   * Drops a transform on the graph, at an offset from the middle of the canvas.
   *
   * <p>Typing the name and accepting the highlight is not enough on its own, because the context
   * dialog does not rank an exact name match first - see {@link ContextDialog#choose(String, int)}.
   * So this checks what actually landed, and if it is the wrong transform, removes it and takes the
   * next entry instead.
   */
  public void addTransform(String transformName, int offsetX, int offsetY) {
    for (int candidate = 0; candidate < MAX_CANDIDATES; candidate++) {
      clickAt(offsetX, offsetY);
      contextDialog().choose(transformName, candidate);

      String added = awaitAdded();
      if (transformName.equals(added)) {
        return;
      }
      if (added != null) {
        actOnTransform(added, "Delete", offsetX, offsetY);
        wait.until(d -> isEmpty());
      }
    }
    throw new AssertionError(
        "Could not add '"
            + transformName
            + "': none of the first "
            + MAX_CANDIDATES
            + " entries matching that search was it");
  }

  /** The label that appeared on the graph, or null if nothing did. */
  private String awaitAdded() {
    try {
      return shortWait().until(d -> addedLabel());
    } catch (TimeoutException e) {
      return null;
    }
  }

  private String addedLabel() {
    return labels().stream().filter(label -> !emptyLabels.contains(label)).findFirst().orElse(null);
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
    dialog.awaitOpen();
    return dialog;
  }

  /** Clicks the canvas at an offset from its middle. */
  public void clickAt(int offsetX, int offsetY) {
    new Actions(driver).moveToElement(canvas(), offsetX, offsetY).click().perform();
  }
}
