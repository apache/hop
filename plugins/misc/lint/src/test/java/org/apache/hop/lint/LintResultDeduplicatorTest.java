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
import org.junit.jupiter.api.Test;

class LintResultDeduplicatorTest {

  private static final String FILE = "/project/pipelines/load.hpl";

  private static LintResult result(
      String ruleId, String message, String severity, LintResult.Origin origin) {
    return new LintResult(
        ruleId, ruleId, severity, message, FILE, LintSourceRef.pipeline("load"), origin);
  }

  /**
   * The bucketing matches on the words in a message so a lint finding and Hop's own remark about
   * the same thing collapse. That must not reach across two lint rules: wording them similarly is
   * not the same as them being the same finding, and collapsing them let one rule hide another.
   */
  @Test
  void twoLintRulesAreNotCollapsedJustForSharingWords() {
    List<LintResult> results =
        List.of(
            result("DOC-001", "description must not be empty", "WARNING", LintResult.Origin.LINT),
            result(
                "LOCAL-900",
                "big and undocumented [description Not Empty]",
                "ERROR",
                LintResult.Origin.LINT));

    List<LintResult> deduplicated = LintResultDeduplicator.deduplicate(results);

    assertEquals(2, deduplicated.size(), "distinct rules are distinct findings");
    assertTrue(deduplicated.stream().anyMatch(r -> "DOC-001".equals(r.getRuleId())));
    assertTrue(deduplicated.stream().anyMatch(r -> "LOCAL-900".equals(r.getRuleId())));
  }

  /** The case the bucketing exists for: Hop's own remark yields to the lint rule saying it. */
  @Test
  void hopsOwnRemarkYieldsToTheLintRuleReportingTheSameThing() {
    List<LintResult> results =
        List.of(
            result("HOP-CHECK", "transform is not used", "WARNING", LintResult.Origin.HOP_NATIVE),
            result("TRANS-002", "orphaned transform", "WARNING", LintResult.Origin.LINT));

    List<LintResult> deduplicated = LintResultDeduplicator.deduplicate(results);

    assertEquals(1, deduplicated.size());
    assertEquals("TRANS-002", deduplicated.get(0).getRuleId());
  }

  @Test
  void theSameFindingTwiceIsReportedOnce() {
    LintResult once =
        result("DOC-001", "description must not be empty", "WARNING", LintResult.Origin.LINT);
    assertEquals(1, LintResultDeduplicator.deduplicate(List.of(once, once)).size());
  }
}
