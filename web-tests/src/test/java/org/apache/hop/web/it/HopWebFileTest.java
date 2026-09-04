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

import org.apache.hop.web.it.pages.HopGuiPage;
import org.apache.hop.web.it.pages.PipelineGraphPage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Saving a file and getting it back.
 *
 * <p>Hop Web cannot use the operating system's file dialog the way the fat client does, so both
 * buttons go through Hop's own {@code HopVfsFileDialog} - code the desktop Hop never runs. Until
 * now the suite asserted that the Save and Open buttons were on the toolbar and never pressed
 * either.
 *
 * <p>Each test reads its file back in a <em>new</em> session. Opening it in the session that wrote
 * it proves very little: Hop would simply bring the tab it already has to the front, and a file
 * that was never written would pass.
 */
@DisplayName("Saving and opening files")
class HopWebFileTest extends HopWebTestBase {

  private static final String FIRST = "Generate rows";
  private static final String SECOND = "Dummy (do nothing)";

  /** A file of this test's own, so nothing depends on what another test left behind. */
  private String scratchFile(String name) {
    return HopWebEnvironment.scratchFolder()
        + "/hop-web-it-"
        + name
        + "-"
        + System.currentTimeMillis()
        + ".hpl";
  }

  /** Reloads the GUI, which is the only way to be sure nothing is open any more. */
  private void inANewSession() {
    HopWebEnvironment.get().reopenUi();
    hopGui.dismissWelcomeDialog();
  }

  @Test
  @DisplayName("a pipeline can be saved and read back")
  void savesAndReopens() {
    String path = scratchFile("save-as");
    PipelineGraphPage graph = hopGui.newPipeline();
    graph.addTransform(FIRST);

    hopGui.saveFileAs(path);

    inANewSession();
    PipelineGraphPage reopened = hopGui.openPipeline(path, FIRST);
    assertTrue(reopened.contains(FIRST), () -> "the file came back as " + reopened.labels());
  }

  @Test
  @DisplayName("saving again keeps the changes made since")
  void savesChanges() {
    String path = scratchFile("save");
    PipelineGraphPage graph = hopGui.newPipeline();
    graph.addTransform(FIRST);
    hopGui.saveFileAs(path);

    // Save, not Save as: the file already has a name, so this is the button a user presses all
    // day and the one that stopped working in issue #6362.
    graph.addTransform(SECOND, 150, 0);
    hopGui.clickWidget(HopGuiPage.SAVE_FILE);

    inANewSession();
    PipelineGraphPage reopened = hopGui.openPipeline(path, FIRST);
    assertTrue(
        reopened.contains(SECOND), () -> "the second transform is missing: " + reopened.labels());
  }
}
