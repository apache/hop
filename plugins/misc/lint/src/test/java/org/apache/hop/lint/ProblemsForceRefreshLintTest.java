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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.DbCache;
import org.apache.hop.lint.BackgroundLintService.OpenEditor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A force refresh re-lints the editors it is not already checking, and a pass that started before
 * the cache was cleared does not put the old remarks back.
 */
class ProblemsForceRefreshLintTest {

  @AfterEach
  void restore() {
    LinterConfigPlugin config = LinterConfigPlugin.getInstance();
    config.setLinterEnabled(true);
    config.saveToHopConfig();
    LintResultsManager.getInstance().clearResults();
    DbCache.getInstance().clear(null);
  }

  @Test
  void otherOpenEditorsAreScheduledAndTheActiveOneIsNot() throws Exception {
    BackgroundLintService service = BackgroundLintService.getInstance();
    Object active = new Object();
    Object other = new Object();
    File activeFile = File.createTempFile("force-refresh-active", ".hpl");
    File otherFile = File.createTempFile("force-refresh-other", ".hpl");
    activeFile.deleteOnExit();
    otherFile.deleteOnExit();
    service.getTracker().markChecked(activeFile.getAbsolutePath());
    service.getTracker().markChecked(otherFile.getAbsolutePath());

    List<Object> scheduled = new ArrayList<>();
    service.refreshOpenEditorsExcept(
        active,
        List.of(
            new OpenEditor(active, activeFile.getAbsolutePath()),
            new OpenEditor(other, otherFile.getAbsolutePath())),
        scheduled::add);

    assertEquals(List.of(other), scheduled);
    assertTrue(service.getTracker().needsLinting(activeFile.getAbsolutePath()));
    assertTrue(service.getTracker().needsLinting(otherFile.getAbsolutePath()));
  }

  @Test
  void aPassStartedBeforeTheClearDoesNotPublish() {
    enableLinter();
    BackgroundLintService service = BackgroundLintService.getInstance();
    LintResultsManager manager = LintResultsManager.getInstance();
    manager.clearResults();
    int generation = DbCache.getInstance().getGeneration();
    DbCache.getInstance().clear(null);

    assertFalse(
        service.publishIfCurrent(
            "/tmp/force-refresh.hpl",
            List.of(remark("too late")),
            service.currentGeneration(),
            generation));
    assertTrue(manager.getResultsForFile("/tmp/force-refresh.hpl").isEmpty());
  }

  @Test
  void aPassStartedAfterTheClearPublishes() {
    enableLinter();
    BackgroundLintService service = BackgroundLintService.getInstance();
    LintResultsManager manager = LintResultsManager.getInstance();
    manager.clearResults();
    DbCache.getInstance().clear(null);
    int generation = DbCache.getInstance().getGeneration();

    assertTrue(
        service.publishIfCurrent(
            "/tmp/force-refresh.hpl",
            List.of(remark("current")),
            service.currentGeneration(),
            generation));
    assertEquals(
        "current", manager.getResultsForFile("/tmp/force-refresh.hpl").get(0).getMessage());
  }

  private static void enableLinter() {
    LinterConfigPlugin config = LinterConfigPlugin.getInstance();
    config.setLinterEnabled(true);
    config.saveToHopConfig();
  }

  private static LintResult remark(String message) {
    return new LintResult("HOP-CHECK", "Table output", "ERROR", message, "/tmp/force-refresh.hpl");
  }
}
