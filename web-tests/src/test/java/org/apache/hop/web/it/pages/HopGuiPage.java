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
   * Locates a widget by the name Hop Web gives it in the DOM, which for anything declared through a
   * {@code GuiToolbarElement} is that annotation's id - {@code toolbar-10010-new}, {@code
   * HopGuiPipelineGraph-ToolBar-10010-Run}, {@code ExplorerPerspective-Toolbar-10300-Refresh}.
   *
   * <p>Hop puts these on the widget itself (see {@code TestIdFacade}), so the match is the element
   * that takes the click. Before that these tests went by the id RAP renders inside an icon's
   * markup, which is only present on widgets that draw an SVG, is prefixed with a per-session UUID
   * so it could only be matched on its suffix, and sits two levels below the widget that listens.
   *
   * <p>The id is not unique: every open file has a graph toolbar carrying the same ids, so callers
   * take the visible match.
   */
  public static By testId(String hopId) {
    return By.cssSelector("[data-hop-id='" + hopId + "']");
  }

  /** Same thing under the name the tests used before ids reached the widgets themselves. */
  public static By guiElement(String guiElementId) {
    return testId(guiElementId);
  }

  public static final By NEW_FILE = testId("toolbar-10010-new");
  public static final By OPEN_FILE = testId("toolbar-10020-open");
  public static final By SAVE_FILE = testId("toolbar-10040-save");
  public static final By SAVE_FILE_AS = testId("toolbar-10050-save-as");

  /** The project shown in the bottom toolbar, which is also the button that changes it. */
  public static final By PROJECT = testId("toolbar-item-10000-project");

  /** The environment shown next to it. */
  public static final By ENVIRONMENT = testId("toolbar-item-20000-environment");

  /**
   * The text fields of the dialog on top, in the order the dialog lays them out.
   *
   * <p>By position rather than by name, because a RAP text field carries neither: {@code
   * GuiToolbarWidgets} only puts ids on the widgets declared through the GUI element annotations,
   * and a dialog builds its fields in plain SWT code.
   */
  private static final String TOP_INPUTS =
      "const shells=[...document.body.children].filter(d=>{"
          + "if(d.tagName!=='DIV')return false;"
          + "const z=parseInt(getComputedStyle(d).zIndex);"
          + "const r=d.getBoundingClientRect();"
          + "return z>=100000&&r.width>100&&r.height>100;});"
          + "const top=shells[shells.length-1]||document;"
          + "return [...top.querySelectorAll('input')]"
          + ".filter(i=>i.type==='text'&&i.offsetParent!==null);";

  /** The title Hop gives the dialog it reports a failure in. */
  public static final String ERROR_DIALOG = "Error";

  /**
   * Turns "an error dialog is in the way" into a failure that says so.
   *
   * <p>Hop reports a failure in a modal dialog, and everything a test tries afterwards - clicking
   * the canvas, opening the context dialog - simply does not happen, so the test dies of a timeout
   * naming whatever it happened to be waiting for. The error itself is on screen the whole time.
   */
  public static void failIfErrorDialog(WebDriver driver) {
    if (!openDialogTitles(driver).contains(ERROR_DIALOG)) {
      return;
    }
    Object text = ((JavascriptExecutor) driver).executeScript(TOP_SHELL_TEXT);
    throw new AssertionError("Hop Web is showing an error dialog: " + text);
  }

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

  /** Everything the dialog on top has written on it, or empty when there is no dialog. */
  public String topDialogText() {
    Object text = ((JavascriptExecutor) driver).executeScript(TOP_SHELL_TEXT);
    return text == null ? "" : text.toString();
  }

  private static final String TOP_SHELL_TEXT =
      "const shells=[...document.body.children].filter(d=>{"
          + "if(d.tagName!=='DIV')return false;"
          + "const z=parseInt(getComputedStyle(d).zIndex);"
          + "const r=d.getBoundingClientRect();"
          + "return z>=100000&&r.width>100&&r.height>100;});"
          + "const top=shells[shells.length-1];"
          + "return top?top.innerText:'';";

  /**
   * Closes the welcome dialog if this Hop Web was configured to show it. The image used by the
   * daily job turns it off through hop-config.json, but a developer pointing the tests at their own
   * Hop Web usually has not.
   */
  public void dismissWelcomeDialog() {
    if (openDialogTitles().isEmpty()) {
      return;
    }
    try {
      closeTopDialog();
      return;
    } catch (RuntimeException e) {
      // Not everything that looks like a dialog is one. A freshly loaded Hop Web puts a loading
      // splash on top (issue #8182) that has no button to press and goes away by itself, so this
      // waits it out rather than failing - and if something else really is stuck there, the test
      // that then cannot do its work says far more about it than a failure in setup would.
      System.out.println("Could not close " + topDialogTitle() + ", waiting for it to go: " + e);
    }
    try {
      waitFor(driver, SPLASH_GRACE).until(d -> openDialogTitles().isEmpty());
    } catch (RuntimeException e) {
      System.out.println("Still on screen: " + openDialogTitles());
    }
  }

  /** How long a loading splash may still be up after the toolbar has appeared. */
  private static final Duration SPLASH_GRACE = Duration.ofSeconds(5);

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
   * Buttons that dismiss a dialog without applying anything. Not "OK" first: some of these dialogs
   * are the real transform dialog and confirming would change the pipeline. "OK" is the last
   * resort, because a message dialog - an error Hop is reporting, above all - has nothing else, and
   * one of those left standing is modal: it blocks every test that comes after it.
   */
  private static final List<String> DISMISS_BUTTONS = List.of("Cancel", "Close", "OK");

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

  /**
   * Whether an element is really on screen.
   *
   * <p>{@code isDisplayed()} is not enough once more than one file is open: every tab keeps its
   * whole widget tree in the document, so a label like "Metrics" exists once per tab and all but
   * one of them are drawn nowhere. Clicking one of those fails with "has no size and location",
   * which is the browser saying exactly this.
   */
  private boolean isOnScreen(WebElement element) {
    Object onScreen =
        ((JavascriptExecutor) driver)
            .executeScript(
                "const r=arguments[0].getBoundingClientRect();"
                    + "return r.width>0&&r.height>0&&r.bottom>0&&r.right>0"
                    + "&&r.top<window.innerHeight&&r.left<window.innerWidth;",
                element);
    return Boolean.TRUE.equals(onScreen);
  }

  /** Clicks a button by its label if it is on screen, without waiting for one that is not. */
  public boolean clickIfVisible(String label) {
    return driver.findElements(parentOfLabelled(label)).stream()
        .filter(this::isOnScreen)
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

  /** The text fields of the dialog on top, in layout order. */
  public List<WebElement> dialogInputs() {
    @SuppressWarnings("unchecked")
    List<WebElement> inputs =
        (List<WebElement>) ((JavascriptExecutor) driver).executeScript(TOP_INPUTS);
    return inputs;
  }

  /**
   * Replaces what a text field contains.
   *
   * <p>Selects with a triple click rather than a select-all chord, which would have to be Control
   * on Linux and Command on macOS - and the platform that decides is the browser's, not the one
   * running the tests, so a containerised browser on a Mac would need the Linux one.
   *
   * <p>Not {@code clear()} either: that empties the DOM element without the RAP client noticing, so
   * the server keeps the old text and the field silently reverts.
   */
  public void enterText(WebElement input, String text) {
    new Actions(driver).moveToElement(input).click().click().click().perform();
    input.sendKeys(text);
    wait.until(d -> text.equals(input.getDomProperty("value")));
  }

  /** Replaces what the n-th text field of the dialog on top contains. */
  public void enterDialogText(int index, String text) {
    enterText(dialogInputs().get(index), text);
  }

  /**
   * Types into the field a dialog puts next to a given label.
   *
   * <p>Hop lays its dialogs out as a column of labels with their fields to the right, and neither
   * carries anything a test could match on, so the field is found by where it is drawn: the text
   * input on the same line as the label, to the right of it.
   */
  public void enterDialogField(String label, String text) {
    WebElement field =
        (WebElement) ((JavascriptExecutor) driver).executeScript(FIELD_BESIDE_LABEL, label);
    if (field == null) {
      throw new AssertionError(
          "The dialog '" + topDialogTitle() + "' has no field next to a label '" + label + "'");
    }
    enterText(field, text);
  }

  private static final String FIELD_BESIDE_LABEL =
      "const wanted=arguments[0];"
          + "const shells=[...document.body.children].filter(d=>{"
          + "if(d.tagName!=='DIV')return false;"
          + "const z=parseInt(getComputedStyle(d).zIndex);"
          + "const r=d.getBoundingClientRect();"
          + "return z>=100000&&r.width>100&&r.height>100;});"
          + "const top=shells[shells.length-1];"
          + "if(!top)return null;"
          + "const label=[...top.querySelectorAll('div')].find("
          + "d=>d.children.length===0&&d.textContent.trim()===wanted);"
          + "if(!label)return null;"
          + "const lr=label.getBoundingClientRect();"
          + "const line=lr.y+lr.height/2;"
          + "let best=null,bestX=Infinity;"
          + "[...top.querySelectorAll('input')].forEach(i=>{"
          + "if(i.type!=='text'||i.offsetParent===null)return;"
          + "const r=i.getBoundingClientRect();"
          + "if(Math.abs(r.y+r.height/2-line)>12||r.x<lr.x)return;"
          + "if(r.x<bestX){bestX=r.x;best=i;}});"
          + "return best;";

  /** Clicks a button by its label, waiting for it to be there. */
  public void clickButton(String label) {
    click(visibleByLabel(label));
  }

  /**
   * Opens a file through Hop Web's own file dialog.
   *
   * <p>Web has no native file dialog to fall back on, so this is Hop's {@code HopVfsFileDialog} -
   * an entirely different implementation from the one the fat client uses on the same button, and
   * one that only these tests ever exercise.
   */
  public void openFile(String path) {
    clickWidget(OPEN_FILE);
    awaitDialog();
    enterDialogText(0, path);
    clickButton("Open");
    awaitNoDialog();
  }

  /** Saves the active file under a new name, through that same file dialog. */
  public void saveFileAs(String path) {
    clickWidget(SAVE_FILE_AS);
    awaitDialog();
    enterDialogText(0, path);
    clickButton("Save");
    // Saving over a file that is already there asks first.
    if ("Warning".equals(topDialogTitle())) {
      clickButton("Yes");
    }
    awaitNoDialog();
  }

  /** Waits until nothing is stacked on top of the Hop GUI any more. */
  public void awaitNoDialog() {
    wait.until(d -> openDialogTitles().isEmpty());
  }

  /** Opens a pipeline file and returns its graph once the transform named is on screen. */
  public PipelineGraphPage openPipeline(String path, String transformOnIt) {
    openFile(path);
    PipelineGraphPage graph = new PipelineGraphPage(driver, wait);
    graph.awaitLabel(transformOnIt);
    return graph;
  }

  /** Creates a new pipeline and returns its graph, ready to be edited. */
  public PipelineGraphPage newPipeline() {
    clickWidget(NEW_FILE);
    clickMenuItem("Pipeline");
    PipelineGraphPage graph = new PipelineGraphPage(driver, wait);
    graph.awaitReady();
    return graph;
  }

  /** Clicks a widget, waiting for it to be the one on screen. */
  public void clickWidget(By locator) {
    click(visible(locator));
  }

  /**
   * The first match that is actually on screen.
   *
   * <p>The first match in the DOM is not necessarily it: every open file has a graph toolbar of its
   * own and all of them carry the same ids, and a perspective that is not on top keeps its widgets
   * around rather than disposing them.
   */
  public WebElement visible(By locator) {
    WebElement element =
        wait.until(
            d ->
                d.findElements(locator).stream().filter(this::isOnScreen).findFirst().orElse(null));
    if (element == null) {
      throw new NoSuchElementException("Nothing visible matches " + locator);
    }
    return element;
  }

  /** Whether anything matching this locator is on screen right now. */
  public boolean isVisible(By locator) {
    return driver.findElements(locator).stream().anyMatch(this::isOnScreen);
  }

  /**
   * Switches to a perspective by its plugin id - {@code explorer-perspective}, {@code
   * metadata-perspective}, {@code execution-perspective}, {@code configuration}.
   *
   * <p>Returns once that perspective's own content is the one on screen rather than once the button
   * has been clicked: the sidebar buttons are icons that all look alike, and Hop swaps the
   * perspectives by moving one control of a stack to the top, so the content is what says which
   * perspective actually won.
   */
  public void switchToPerspective(String perspectiveId) {
    clickWidget(testId(PERSPECTIVE_PREFIX + perspectiveId));
    wait.until(d -> isVisible(perspectiveContent(perspectiveId)));
  }

  /** The perspective on screen, by plugin id, or null while none of the known ones is up. */
  public String activePerspective() {
    List<WebElement> contents =
        driver.findElements(By.cssSelector("[data-hop-id^='" + PERSPECTIVE_CONTENT_PREFIX + "']"));
    return contents.stream()
        .filter(this::isOnScreen)
        .map(e -> e.getAttribute("data-hop-id").substring(PERSPECTIVE_CONTENT_PREFIX.length()))
        .findFirst()
        .orElse(null);
  }

  private static final String PERSPECTIVE_PREFIX = "perspective-";

  private static final String PERSPECTIVE_CONTENT_PREFIX = "perspective-content-";

  private static By perspectiveContent(String perspectiveId) {
    return testId(PERSPECTIVE_CONTENT_PREFIX + perspectiveId);
  }

  /** The project the GUI says it is working in. */
  public String projectName() {
    return visible(PROJECT).getText().trim();
  }

  /** The environment the GUI says it is working in, empty when none is chosen. */
  public String environmentName() {
    return visible(ENVIRONMENT).getText().trim();
  }

  /**
   * Whether a visible element carries exactly this label, which for a file that was just created is
   * its tab.
   */
  public boolean hasTab(String title) {
    return driver.findElements(labelled(title)).stream().anyMatch(this::isOnScreen);
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
                .filter(this::isOnScreen)
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
