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

package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Switching the linter off has to stop marking files.
 *
 * <p>The canvas overlays already followed the switch. The Explorer and an open editor did not: the
 * file that was open kept being linted, and a file that had already been marked stayed red.
 */
class DisabledLinterTest {

  @AfterEach
  void restore() {
    LinterConfigPlugin config = LinterConfigPlugin.getInstance();
    config.setLinterEnabled(true);
    config.saveToHopConfig();
    LintResultsManager.getInstance().clearResults();
  }

  @Test
  void switchingTheLinterOffClearsStoredFindings() {
    LintResultsManager manager = LintResultsManager.getInstance();
    manager.updateResultsForFile(
        "/tmp/example.hwf",
        List.of(
            new LintResult(
                "HOP-CHECK", "SQL", "ERROR", "SQL cannot be null or blank.", "/tmp/example.hwf")));
    assertFalse(manager.getAllResults().isEmpty());

    LinterConfigPlugin config = LinterConfigPlugin.getInstance();
    config.setLinterEnabled(false);
    config.applyEnabledState();

    assertTrue(manager.getAllResults().isEmpty(), "a disabled linter must not leave a file marked");
  }

  @Test
  void leavingTheLinterOnKeepsStoredFindings() {
    LintResultsManager manager = LintResultsManager.getInstance();
    manager.updateResultsForFile(
        "/tmp/example.hwf",
        List.of(new LintResult("HOP-CHECK", "SQL", "ERROR", "still there", "/tmp/example.hwf")));

    LinterConfigPlugin config = LinterConfigPlugin.getInstance();
    config.setLinterEnabled(true);
    config.applyEnabledState();

    assertFalse(manager.getAllResults().isEmpty());
  }

  @Test
  void theExplorerStopsDecoratingWhenTheLinterIsOff() {
    LinterConfigPlugin config = LinterConfigPlugin.getInstance();
    config.setLinterEnabled(false);
    config.saveToHopConfig();
    assertFalse(LintStatusFilePainter.decoratesExplorerFiles());

    config = LinterConfigPlugin.getInstance();
    config.setLinterEnabled(true);
    config.saveToHopConfig();
    assertTrue(LintStatusFilePainter.decoratesExplorerFiles());
  }

  @Test
  void anOpenEditorIsNotLintedWhenTheLinterIsOff() {
    LinterConfigPlugin config = LinterConfigPlugin.getInstance();
    config.setLinterEnabled(false);
    config.saveToHopConfig();

    HopGuiAbstractGraph graph = mock(HopGuiAbstractGraph.class);
    EditorLintSupport.onGraphUpdate(graph);
    EditorLintSupport.onNewGraph(graph);
    BackgroundLintService.getInstance().scheduleGraphLint(graph, true);

    verifyNoInteractions(graph);
  }
}
