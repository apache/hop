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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Clicking the canvas totals when a file is clean must do nothing at all.
 *
 * <p>It used to fall through to the results window, so a pipeline with no findings answered a click
 * with an empty window. Both reveal paths report "nothing to reveal" by returning false, which is
 * what the click handlers rely on to stay silent.
 */
class CleanFileTotalsClickTest {

  private static final String CLEAN_FILE = "/project/pipelines/clean.hpl";

  private LintResultsManager manager;

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
  void revealingAFileWithNoFindingsDoesNothing() {
    assertFalse(
        PipelineProblemsTabSync.revealForFile(CLEAN_FILE),
        "a clean pipeline has nothing to reveal, so the click is a no-op");
    assertFalse(
        WorkflowProblemsTabSync.revealForFile("/project/workflows/clean.hwf"),
        "and the same for a clean workflow");
  }

  @Test
  void revealingAnUnknownFileDoesNothing() {
    assertFalse(PipelineProblemsTabSync.revealForFile(null));
    assertFalse(WorkflowProblemsTabSync.revealForFile(null));
  }
}
