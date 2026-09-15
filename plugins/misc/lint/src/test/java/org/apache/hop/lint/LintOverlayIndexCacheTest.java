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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The canvas painters ask for this index once per element per repaint, so it is built once and held
 * until the findings change. These tests pin down both halves: that it is reused, and that it does
 * not go stale.
 */
class LintOverlayIndexCacheTest {

  private static final String FILE = "/project/pipelines/load.hpl";

  private LintResultsManager manager;

  private static LintResult finding(String ruleId, String transform) {
    return new LintResult(
        ruleId,
        "Rule " + ruleId,
        "WARNING",
        "message",
        FILE,
        LintSourceRef.transform(transform),
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
  void indexesTheFindingsForAFile() {
    manager.updateResults(List.of(finding("R-001", "Table Input")));

    Map<String, List<LintResult>> index =
        manager.getOverlayIndex(FILE, LintSourceRef.Kind.TRANSFORM);

    assertEquals(1, index.size());
    assertEquals(1, index.get("Table Input").size());
  }

  @Test
  void repeatedLookupsReuseTheSameIndex() {
    manager.updateResults(List.of(finding("R-001", "Table Input")));

    Map<String, List<LintResult>> first =
        manager.getOverlayIndex(FILE, LintSourceRef.Kind.TRANSFORM);
    Map<String, List<LintResult>> second =
        manager.getOverlayIndex(FILE, LintSourceRef.Kind.TRANSFORM);

    // Same instance, not merely an equal one: a painter calling this per element per frame must
    // not pay for rebuilding it.
    assertSame(first, second);
  }

  @Test
  void newResultsInvalidateTheIndex() {
    manager.updateResults(List.of(finding("R-001", "Table Input")));
    manager.getOverlayIndex(FILE, LintSourceRef.Kind.TRANSFORM);

    manager.updateResults(List.of(finding("R-002", "Select Values")));
    Map<String, List<LintResult>> index =
        manager.getOverlayIndex(FILE, LintSourceRef.Kind.TRANSFORM);

    assertTrue(index.containsKey("Select Values"), "the index must reflect the new findings");
    assertFalse(index.containsKey("Table Input"), "the old finding must be gone");
  }

  @Test
  void updatingOneFileInvalidatesTheIndex() {
    manager.updateResults(List.of(finding("R-001", "Table Input")));
    manager.getOverlayIndex(FILE, LintSourceRef.Kind.TRANSFORM);

    manager.updateResultsForFile(FILE, List.of(finding("R-003", "Sort Rows")));
    Map<String, List<LintResult>> index =
        manager.getOverlayIndex(FILE, LintSourceRef.Kind.TRANSFORM);

    assertTrue(index.containsKey("Sort Rows"));
  }

  @Test
  void clearingResultsEmptiesTheIndex() {
    manager.updateResults(List.of(finding("R-001", "Table Input")));
    manager.getOverlayIndex(FILE, LintSourceRef.Kind.TRANSFORM);

    manager.clearResults();

    assertTrue(manager.getOverlayIndex(FILE, LintSourceRef.Kind.TRANSFORM).isEmpty());
  }
}
