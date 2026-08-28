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
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.WebDriverWait;

/** The Hop GUI shell: main toolbar, the "new" menu, and the dialogs stacked on top of it. */
public class HopGuiPage {

  /**
   * Every visible RAP shell that is big enough to be a window, outermost first. Index 0 is the Hop
   * GUI itself, so anything beyond it is an open dialog. A shell's first line of text is its title,
   * which is how these tests identify dialogs - far steadier than the positional {@code
   * //body/div[5]/div[1]} the previous suite matched on.
   */
  private static final String SHELL_TITLES =
      "return [...document.body.children].filter(d=>{"
          + "if(d.tagName!=='DIV')return false;"
          + "const z=parseInt(getComputedStyle(d).zIndex);"
          + "const r=d.getBoundingClientRect();"
          + "return z>=100000&&r.width>100&&r.height>100;})"
          + ".map(d=>(d.innerText||'').split('\\n')[0].trim());";

  /**
   * How often to re-check a condition. The default of 500ms is the single biggest cost in this
   * suite: nothing it waits for is slow, but every wait was rounded up to half a second, so a
   * transform that takes 200ms of real work billed several seconds.
   */
  public static final Duration POLL_INTERVAL = Duration.ofMillis(50);

  /** A wait that polls often enough not to dominate the measurement. */
  public static WebDriverWait waitFor(WebDriver driver, Duration timeout) {
    return (WebDriverWait) new WebDriverWait(driver, timeout).pollingEvery(POLL_INTERVAL);
  }

  private final WebDriver driver;
  private final WebDriverWait wait;

  public HopGuiPage(WebDriver driver, Duration timeout) {
    this.driver = driver;
    this.wait = waitFor(driver, timeout);
  }

  /**
   * Locates a widget by the GUI element id it was declared with, for example {@code
   * toolbar-10010-new} from {@code HopGui.ID_MAIN_TOOLBAR_NEW}.
   *
   * <p>The DOM id is not the GUI element id: {@code GuiToolbarWidgets} prefixes it with the id of
   * the HopGui instance owning the widget, and since the RAP session isolation work (issue #8047)
   * that is a UUID minted per session. {@code toolbar-10010-new} therefore reaches the browser as
   * {@code dffad89a-...-toolbar-10010-new} and can only be matched on its suffix. Matching the
   * exact id is what silently broke the previous generation of these tests.
   */
  public static By guiElement(String guiElementId) {
    return By.cssSelector("[id$='-" + guiElementId + "']");
  }

  public static final By NEW_FILE = guiElement("toolbar-10010-new");
  public static final By OPEN_FILE = guiElement("toolbar-10020-open");
  public static final By SAVE_FILE = guiElement("toolbar-10040-save");

  /** Titles of the dialogs currently open on top of the Hop GUI, innermost last. */
  public static List<String> openDialogTitles(WebDriver driver) {
    @SuppressWarnings("unchecked")
    List<String> titles = (List<String>) ((JavascriptExecutor) driver).executeScript(SHELL_TITLES);
    // Drop the Hop GUI shell itself; only the dialogs above it are interesting.
    return titles.isEmpty() ? titles : titles.subList(1, titles.size());
  }

  public static String topDialogTitle(WebDriver driver) {
    List<String> titles = openDialogTitles(driver);
    return titles.isEmpty() ? null : titles.get(titles.size() - 1);
  }

  public List<String> openDialogTitles() {
    return openDialogTitles(driver);
  }

  public String topDialogTitle() {
    return topDialogTitle(driver);
  }

  /**
   * Closes the welcome dialog if this Hop Web was configured to show it. The image used by the
   * daily job turns it off through hop-config.json, but a developer pointing the tests at their own
   * Hop Web usually has not.
   */
  public void dismissWelcomeDialog() {
    if (!openDialogTitles().isEmpty()) {
      closeTopDialog();
    }
  }

  /**
   * Waits for a dialog to appear and returns its title.
   *
   * <p>Choosing an entry that opens a dialog only tells you the context dialog closed; the dialog
   * itself can take noticeably longer to build - Cassandra input is one that does - so anything
   * reading the title straight afterwards is racing it.
   */
  public String awaitDialog() {
    return wait.until(d -> topDialogTitle());
  }

  /** How long to give Escape before falling back to the dialog's own button. */
  private static final Duration ESCAPE_GRACE = Duration.ofSeconds(2);

  /**
   * Buttons that dismiss a dialog without applying anything. Not "OK": some of these dialogs are
   * the real transform dialog and confirming would change the pipeline.
   */
  private static final List<String> DISMISS_BUTTONS = List.of("Cancel", "Close");

  /**
   * Closes one dialog, identified by title, and waits until it is really gone.
   *
   * <p>Counted per title rather than tested for absence: a transform can stack two dialogs with the
   * same name - CSV file input shows a notice called "CSV file input" and then its actual dialog -
   * so "no dialog by that name" would never come true.
   *
   * <p>Escape does not reach every dialog; that same notice ignores it. Falling back to the
   * dialog's own dismiss button is what a user would do anyway.
   */
  public void closeDialog(String title) {
    long before = countOpen(title);
    new Actions(driver).sendKeys(Keys.ESCAPE).perform();
    if (closed(title, before, ESCAPE_GRACE)) {
      return;
    }
    for (String label : DISMISS_BUTTONS) {
      if (clickIfVisible(label)) {
        break;
      }
    }
    wait.until(d -> countOpen(title) < before);
  }

  /** Closes every open dialog, however many deep they are stacked. */
  public void closeAllDialogs() {
    for (int attempt = 0; attempt < 5 && !openDialogTitles().isEmpty(); attempt++) {
      closeTopDialog();
    }
  }

  private long countOpen(String title) {
    return openDialogTitles().stream().filter(title::equals).count();
  }

  private boolean closed(String title, long before, Duration timeout) {
    try {
      waitFor(driver, timeout).until(d -> countOpen(title) < before);
      return true;
    } catch (TimeoutException e) {
      return false;
    }
  }

  /** Clicks a button by its label if it is on screen, without waiting for one that is not. */
  public boolean clickIfVisible(String label) {
    return driver.findElements(parentOfLabelled(label)).stream()
        .filter(WebElement::isDisplayed)
        .findFirst()
        .map(
            element -> {
              click(element);
              return true;
            })
        .orElse(false);
  }

  /** Sends Escape and waits until whatever was on top is gone. */
  public void closeTopDialog() {
    String title = topDialogTitle();
    if (title == null) {
      return;
    }
    closeDialog(title);
  }

  /** Creates a new pipeline and returns its graph, ready to be edited. */
  public PipelineGraphPage newPipeline() {
    clickWidget(NEW_FILE);
    clickMenuItem("Pipeline");
    PipelineGraphPage graph = new PipelineGraphPage(driver, wait);
    graph.awaitReady();
    return graph;
  }

  /**
   * Clicks a toolbar item. The GUI element id lands on the item's {@code <img>}, but the widget
   * carrying the click listener is two levels up.
   */
  public void clickWidget(By locator) {
    WebElement image = wait.until(d -> d.findElement(locator));
    click(image.findElement(By.xpath("./../..")));
  }

  /**
   * Whether a visible element carries exactly this label, which for a file that was just created is
   * its tab.
   */
  public boolean hasTab(String title) {
    return driver.findElements(labelled(title)).stream().anyMatch(WebElement::isDisplayed);
  }

  /** Clicks an entry in an open menu by its label. */
  public void clickMenuItem(String label) {
    click(visibleByLabel(label));
  }

  /**
   * The widget carrying a given label. RAP paints the text in a child div, so the element that
   * takes the click is its parent.
   */
  private WebElement visibleByLabel(String label) {
    return wait.until(
        d ->
            d.findElements(parentOfLabelled(label)).stream()
                .filter(WebElement::isDisplayed)
                .findFirst()
                .orElse(null));
  }

  private void click(WebElement element) {
    if (element == null) {
      throw new NoSuchElementException("No element to click");
    }
    new Actions(driver).moveToElement(element).click().perform();
  }

  /**
   * An element whose text is this label.
   *
   * <p>Compared with the whitespace normalised, because Hop pads its button labels: the Cancel
   * button's text node is literally {@code " Cancel "}, so an exact match silently finds nothing
   * and any fallback built on it quietly does nothing at all.
   */
  private static By labelled(String label) {
    return By.xpath("//div[normalize-space(text())=" + xpathLiteral(label) + "]");
  }

  /** The widget owning a label: RAP paints the text in a child of the element that is clicked. */
  private static By parentOfLabelled(String label) {
    return By.xpath("//div[normalize-space(text())=" + xpathLiteral(label) + "]/..");
  }

  /** Quotes a label for XPath, which has no escape character of its own. */
  static String xpathLiteral(String value) {
    if (!value.contains("'")) {
      return "'" + value + "'";
    }
    return "concat('" + value.replace("'", "',\"'\",'") + "')";
  }
}
