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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.web.it.pages.HopGuiPage;
import org.apache.hop.web.it.pages.PipelineGraphPage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.WebDriver;

/**
 * Two people using the same Hop Web at the same time.
 *
 * <p>This is the failure mode that belongs to Hop Web alone. The fat client has one user, one
 * window and one set of images, so anything held in a {@code static} field is correct there and
 * shared by everybody here. Isolating that state took a release of its own (issue #8047), and a
 * cache of session scoped images left in a static field is what made Hop Web throw "Argument not
 * valid" at whoever was still working after somebody else's session ended (issue #3508).
 *
 * <p>A second browser rather than a second window: windows of one browser share the session cookie,
 * so Hop Web would hand both the same session and there would be nothing to isolate.
 */
@DisplayName("Two sessions at once")
class HopWebSessionTest extends HopWebTestBase {

  private static final String MINE = "Generate rows";
  private static final String THEIRS = "Dummy (do nothing)";

  /**
   * Leaves a healthy session behind for whatever runs next.
   *
   * <p>Not tidiness: a session that has been through this comes out damaged. Hop Web currently
   * fails to save its GUI options once a second session has been opened - "Could not find file with
   * URI .../hop-config.json.new ... no base URI was provided" - and puts a modal error dialog up,
   * which every later test in that session would then fail on rather than on its own subject.
   */
  @AfterAll
  static void startAFreshSession() {
    HopWebEnvironment.get().reopenUi();
    try {
      hopGui.dismissWelcomeDialog();
    } catch (RuntimeException e) {
      // Cleanup, not a test: whatever is still on screen is the next test's to report.
      System.out.println("Could not clear the reloaded GUI: " + e);
    }
  }

  @Test
  @DisplayName("what one session builds is invisible to the other")
  void sessionsDoNotShareTheirGui() {
    WebDriver other = HopWebEnvironment.get().openAnotherBrowser();
    try {
      HopGuiPage otherGui = new HopGuiPage(other, TIMEOUT);
      otherGui.dismissWelcomeDialog();

      PipelineGraphPage mine = hopGui.newPipeline();
      mine.addTransform(MINE);
      PipelineGraphPage theirs = otherGui.newPipeline();
      theirs.addTransform(THEIRS);

      assertTrue(mine.contains(MINE), () -> "this session lost its own work: " + mine.labels());
      assertFalse(
          mine.contains(THEIRS), () -> "this session sees the other one's work: " + mine.labels());
      assertTrue(
          theirs.contains(THEIRS), () -> "the other session lost its work: " + theirs.labels());
      assertFalse(
          theirs.contains(MINE),
          () -> "the other session sees this one's work: " + theirs.labels());
      assertEquals(
          List.of(), BrowserConsole.errors(other), "the second session's browser reported errors");
    } finally {
      HopWebEnvironment.get().closeBrowser(other);
    }
  }

  @Test
  @DisplayName("a session going away leaves the other one working")
  void oneSessionEndingDoesNotBreakTheOther() {
    WebDriver other = HopWebEnvironment.get().openAnotherBrowser();
    HopGuiPage otherGui = new HopGuiPage(other, TIMEOUT);
    otherGui.dismissWelcomeDialog();
    PipelineGraphPage theirs = otherGui.newPipeline();
    theirs.addTransform(THEIRS);
    // Opening a dialog is what puts the shared, session scoped resources - images above all - to
    // work, so the session that is about to end has really used them.
    theirs.actOnTransform(THEIRS, "Edit", 0, 0);
    otherGui.awaitDialog();
    otherGui.closeAllDialogs();

    HopWebEnvironment.get().closeBrowser(other);

    // Everything the departed session touched, done again here. If its images went with it, this
    // is where it shows - as "Argument not valid" in the server log, which the base class fails on.
    PipelineGraphPage mine = hopGui.newPipeline();
    mine.addTransform(MINE);
    mine.actOnTransform(MINE, "Edit", 0, 0);
    assertEquals(MINE, hopGui.topDialogTitle(), "the dialog did not open after the other session");
    hopGui.closeTopDialog();
    assertTrue(mine.contains(MINE), () -> "the graph stopped drawing: " + mine.labels());
  }
}
