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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A folder filter has to match everything beneath the folder. The file filter looks a single path
 * up, so handing it a folder matched nothing and a "lint this folder" run had nothing to show.
 */
class LintResultsFolderFilterTest {

  private LintResultsManager manager;

  private static LintResult finding(String file) {
    return new LintResult(
        "R-001",
        "rule",
        "WARNING",
        "message",
        file,
        LintSourceRef.pipeline("p"),
        LintResult.Origin.LINT);
  }

  @BeforeEach
  void reset() {
    manager = LintResultsManager.getInstance();
    manager.clearResults();
  }

  @AfterEach
  void clear() {
    manager.clearResults();
  }

  @Test
  void aFolderPathFindsNothingThroughTheFileLookup() {
    manager.updateResults(List.of(finding("/project/pipelines/load.hpl")));

    assertTrue(
        manager.getResultsForFile("/project/pipelines").isEmpty(),
        "a folder is not a file: the per-file lookup cannot answer for one");
  }

  @Test
  void everyFindingUnderTheFolderIsAvailableToFilter() {
    manager.updateResults(
        List.of(
            finding("/project/pipelines/load.hpl"),
            finding("/project/pipelines/sub/transform.hpl"),
            finding("/project/workflows/main.hwf")));

    String prefix = "/project/pipelines/";
    long underFolder =
        manager.getAllResults().stream()
            .filter(r -> LintPathUtils.normalizePath(r.getFileName()).startsWith(prefix))
            .count();

    assertEquals(2, underFolder, "both pipelines, and not the workflow in a sibling folder");
  }
}
