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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Set;
import org.junit.jupiter.api.Test;

/** The checks the rule editor runs on OK, before anything is written to hop-lint.yml. */
class RuleBuilderValidationTest {

  /**
   * An empty extra clause passed the editor, which only checked the clause on show, and the save
   * then failed with a NullPointerException message.
   */
  @Test
  void anEmptyClauseIsIncomplete() {
    assertEquals(
        "Field must be selected", RuleBuilderDialog.clauseProblem(new RuleClause("", null, "")));
    assertEquals(
        "Condition must be selected",
        RuleBuilderDialog.clauseProblem(new RuleClause("name", null, "")));
    assertEquals(
        "Value is required for this condition",
        RuleBuilderDialog.clauseProblem(new RuleClause("name", RuleCondition.MATCHES_PATTERN, "")));
    assertNull(RuleBuilderDialog.clauseProblem(new RuleClause("name", RuleCondition.NOT_NULL, "")));
  }

  @Test
  void aRuleIdMustBeUsableAndFree() {
    Set<String> taken = Set.of("DOC-001");

    assertNull(RuleBuilderDialog.ruleIdProblem("PROJECT-001", taken));
    assertNull(RuleBuilderDialog.ruleIdProblem("naming.pipelines_2", taken));
    assertNotNull(RuleBuilderDialog.ruleIdProblem("", taken));
    assertNotNull(RuleBuilderDialog.ruleIdProblem("my rule", taken));
    assertNotNull(RuleBuilderDialog.ruleIdProblem("-001", taken));
    assertEquals(
        "Another rule already has ID 'doc-001'",
        RuleBuilderDialog.ruleIdProblem("doc-001", taken),
        "ids are compared ignoring case, as hop-lint.yml matches them");
  }
}
