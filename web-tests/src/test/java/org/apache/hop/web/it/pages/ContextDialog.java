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

import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.WebDriverWait;

/**
 * The Hop context dialog: the searchable grid of transforms and actions.
 *
 * <p>Its entries are painted on a canvas and cannot be addressed individually, but the dialog puts
 * the keyboard focus in its search field and Enter accepts the highlighted entry. Typing and
 * pressing Enter therefore replaces the coordinate arithmetic the earlier tests used to click the
 * first tile - arithmetic that stopped working once the dialog grew taller than the window.
 */
public class ContextDialog {

  /**
   * The context dialog names itself: "Select the action to execute or the transform to create:", or
   * "Select the action to take on transform 'X':".
   *
   * <p>It used to be detected as "a second large canvas over the graph", which is true of it but
   * also true of transform dialogs that draw on a canvas themselves - Filter rows puts its
   * condition editor on one - so closing the context dialog appeared never to finish.
   */
  private static final String TITLE_PREFIX = "Select the action";

  /**
   * A fingerprint of what the dialog is showing: its own size plus the description it prints for
   * the highlighted entry. Both change as the search narrows, and neither depends on guessing which
   * canvas belongs to the dialog - a guess that could come up empty and report "settled"
   * immediately, so Enter fired before the results had filtered at all.
   */
  private static final String RENDER_STATE =
      "const sh=[...document.body.children].filter(d=>{"
          + "if(d.tagName!=='DIV')return false;"
          + "const z=parseInt(getComputedStyle(d).zIndex);"
          + "const r=d.getBoundingClientRect();"
          + "return z>=100000&&r.width>100&&r.height>100;});"
          + "const top=sh[sh.length-1];"
          + "if(!top)return '';"
          + "const r=top.getBoundingClientRect();"
          + "return Math.round(r.width)+'x'+Math.round(r.height)+'|'+(top.innerText||'');";

  private final WebDriver driver;
  private final WebDriverWait wait;

  ContextDialog(WebDriver driver, WebDriverWait wait) {
    this.driver = driver;
    this.wait = wait;
  }

  void awaitOpen() {
    wait.until(d -> isOpen());
  }

  public boolean isOpen() {
    String title = title();
    return title != null && title.startsWith(TITLE_PREFIX);
  }

  public String title() {
    return HopGuiPage.topDialogTitle(driver);
  }

  /** Filters the entries and accepts the one left highlighted. */
  public void choose(String search) {
    choose(search, 0);
  }

  /**
   * Filters the entries and accepts the one {@code skip} places after the highlight.
   *
   * <p>The highlighted entry is not necessarily the one that was typed: searching for the exact
   * name "Null if" highlights "If null", and "Table input" does not highlight "Table input". Hop
   * used to expose {@code HOP_CONTEXT_DIALOG_STRICT_SEARCH} ("Needed for automated UI testing") for
   * precisely this, but it was removed with the search rework in #6125 and nothing replaced it, so
   * the caller has to walk the entries and check what it got.
   */
  public void choose(String search, int skip) {
    // The dialog focuses its search field on open, so the keystrokes need no target.
    new Actions(driver).sendKeys(search).perform();
    awaitFilterSettled();
    for (int i = 0; i < skip; i++) {
      new Actions(driver).sendKeys(Keys.ARROW_RIGHT).perform();
    }
    if (skip > 0) {
      awaitFilterSettled();
    }
    new Actions(driver).sendKeys(Keys.ENTER).perform();
    wait.until(d -> !isOpen());
  }

  public void cancel() {
    new Actions(driver).sendKeys(Keys.ESCAPE).perform();
    wait.until(d -> !isOpen());
  }

  /**
   * Waits until the filtered list stops changing size. Filtering is a server round trip, and Enter
   * pressed while it is still in flight accepts whichever entry was highlighted beforehand.
   */
  private void awaitFilterSettled() {
    String[] previous = {null};
    wait.until(
        d -> {
          String state = renderState();
          boolean unchanged = state.equals(previous[0]);
          previous[0] = state;
          return unchanged;
        });
  }

  private String renderState() {
    Object state = ((JavascriptExecutor) driver).executeScript(RENDER_STATE);
    return state == null ? "" : state.toString();
  }
}
