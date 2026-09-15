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

package org.apache.hop.web.it;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import org.apache.hop.web.it.pages.HopGuiPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.WebDriverWait;

/** Shared setup: one Hop Web, one browser, and artifacts written whenever a test fails. */
@ExtendWith(FailureArtifacts.class)
public abstract class HopWebTestBase {

  /** Long enough for Hop Web to open a transform dialog on a loaded CI machine. */
  protected static final Duration TIMEOUT = Duration.ofSeconds(30);

  protected static WebDriver driver;
  protected static WebDriverWait wait;
  protected static HopGuiPage hopGui;

  @BeforeAll
  static void startHopWeb() {
    HopWebEnvironment environment = HopWebEnvironment.get();
    environment.openUi();
    driver = environment.getDriver();
    wait = HopGuiPage.waitFor(driver, TIMEOUT);
    hopGui = new HopGuiPage(driver, TIMEOUT);
    hopGui.dismissWelcomeDialog();
  }

  /**
   * Leaves the GUI with no dialog open, whatever the test did.
   *
   * <p>One test tripping over an open dialog used to take the rest of the suite down with it; the
   * previous version reacted by reloading the browser mid-run, which lost the session and produced
   * a cascade of failures that all pointed at the wrong test.
   */
  private int serverLogMark;

  @BeforeEach
  void markLogs() {
    serverLogMark = HopWebEnvironment.get().serverLog().length();
    BrowserConsole.drain(driver);
  }

  /**
   * Fails the test if either side of Hop Web reported a failure while it ran, even though the page
   * looked fine.
   *
   * <p>Plenty goes wrong without reaching what a test asserts on: a dialog can open perfectly while
   * the thread it started to populate its widgets dies, and the browser can fail to apply what the
   * server sent without the server ever hearing about it. Asserting only on what is visible
   * declares both green.
   */
  @AfterEach
  void failOnCrashes() {
    List<String> crashes = serverCrashes();
    List<String> browserErrors = BrowserConsole.errors(driver);
    String errorDialog = openErrorDialog();
    assertTrue(
        crashes.isEmpty() && browserErrors.isEmpty() && errorDialog == null,
        () ->
            "Hop Web reported failures during this test:"
                + (crashes.isEmpty() ? "" : "\n  server: " + String.join("\n          ", crashes))
                + (browserErrors.isEmpty()
                    ? ""
                    : "\n  browser: " + String.join("\n           ", browserErrors))
                + (errorDialog == null ? "" : "\n  dialog: " + errorDialog));
  }

  /**
   * What Hop is reporting in an error dialog, if it put one on screen.
   *
   * <p>The third place a failure can surface, and the one neither log sees: Hop catches the
   * exception, tells the user about it in a dialog and writes nothing anywhere. Without this a test
   * only notices when the modal dialog blocks whatever it does next - which is usually the next
   * test, so the failure gets reported against innocent code.
   */
  private String openErrorDialog() {
    if (!hopGui.openDialogTitles().contains(HopGuiPage.ERROR_DIALOG)) {
      return null;
    }
    String text = hopGui.topDialogText().replace("\n", " ");
    return text.length() > 500 ? text.substring(0, 500) + "..." : text;
  }

  private List<String> serverCrashes() {
    String log = HopWebEnvironment.get().serverLog();
    if (log.length() <= serverLogMark) {
      return List.of();
    }
    return ServerLog.crashes(log.substring(serverLogMark));
  }

  @AfterEach
  void closeLeftoverDialogs() {
    try {
      hopGui.closeAllDialogs();
    } catch (RuntimeException e) {
      // A dialog that refuses to close is the next test's problem to report, not this test's:
      // failing here would replace a real failure with a cleanup one.
      System.out.println("Could not close dialog " + hopGui.topDialogTitle() + ": " + e);
    }
  }
}
