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
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  /**
   * A value its condition cannot use was accepted, and the rule never reported anything.
   *
   * @see <a href="https://github.com/apache/hop/issues/9071">#9071</a>
   */
  @Test
  void aValueTheConditionCannotUseIsRefused() {
    for (String value : new String[] {"abc", "${MAXT}", "3,0"}) {
      assertNotNull(
          RuleBuilderDialog.clauseProblem(
              new RuleClause("transformCount", RuleCondition.MAX_VALUE, value),
              RuleTarget.PIPELINE,
              false),
          value);
    }
    assertNull(
        RuleBuilderDialog.clauseProblem(
            new RuleClause("transformCount", RuleCondition.MAX_VALUE, "3.5"),
            RuleTarget.PIPELINE,
            false));
    assertNotNull(
        RuleBuilderDialog.clauseProblem(
            new RuleClause("transforms", RuleCondition.MAX_COLLECTION_SIZE, "2.5"),
            RuleTarget.PIPELINE,
            false));
    String pattern =
        RuleBuilderDialog.clauseProblem(
            new RuleClause("name", RuleCondition.MATCHES_PATTERN, "[a-"),
            RuleTarget.PIPELINE,
            false);
    assertNotNull(pattern);
    assertTrue(pattern.contains("[a-"), pattern);
  }

  @Test
  void aConditionThatDoesNotFitTheFieldIsRefused() {
    String problem =
        RuleBuilderDialog.clauseProblem(
            new RuleClause("transformCount", RuleCondition.NOT_NULL, ""),
            RuleTarget.PIPELINE,
            false);

    assertEquals(
        "NOT_NULL does not apply to transformCount, so the rule can never report anything",
        problem);
  }

  /**
   * The text conditions read any value as text, so they work on a connection's port, which is text,
   * and on a list of target transforms. These were refused although the rules report.
   */
  @Test
  void aConditionTheExecutorCanFireIsKept() {
    assertNull(
        RuleBuilderDialog.clauseProblem(
            new RuleClause("port", RuleCondition.NO_HARDCODED, ""),
            RuleTarget.DATABASE_CONNECTION,
            false));
    assertNull(
        RuleBuilderDialog.clauseProblem(
            new RuleClause("port", RuleCondition.MATCHES_PATTERN, "^\\d+$"),
            RuleTarget.DATABASE_CONNECTION,
            false));
    assertNull(
        RuleBuilderDialog.clauseProblem(
            new RuleClause("targetTransforms", RuleCondition.CONTAINS, "Abort"),
            RuleTarget.TRANSFORM,
            false));
    assertNull(
        RuleBuilderDialog.clauseProblem(
            new RuleClause("transformCount", RuleCondition.MATCHES_PATTERN, "^\\d$"),
            RuleTarget.PIPELINE,
            false));
  }

  /** What the executor can never report: each of these reads as a clean result. */
  @Test
  void aConditionTheExecutorCannotFireIsRefused() {
    assertNotNull(
        RuleBuilderDialog.clauseProblem(
            new RuleClause("transformCount", RuleCondition.NOT_EMPTY, ""),
            RuleTarget.PIPELINE,
            false));
    assertNotNull(
        RuleBuilderDialog.clauseProblem(
            new RuleClause("hasNotes", RuleCondition.MAX_VALUE, "1"), RuleTarget.PIPELINE, false));
    assertNotNull(
        RuleBuilderDialog.clauseProblem(
            new RuleClause("attributes", RuleCondition.MIN_VALUE, "1"),
            RuleTarget.DATABASE_CONNECTION,
            false));
    assertNotNull(
        RuleBuilderDialog.clauseProblem(
            new RuleClause("name", RuleCondition.MAX_COLLECTION_SIZE, "3"),
            RuleTarget.PIPELINE,
            false));
    // The secret fields of a transform match every setting by name, whatever its type.
    assertNull(
        RuleBuilderDialog.clauseProblem(
            new RuleClause("password", RuleCondition.NOT_EMPTY_COLLECTION, ""),
            RuleTarget.TRANSFORM,
            false));
  }

  /**
   * A rule narrowed with appliesTo can read a plugin's own setting of the same name, whose type is
   * not known, so only its value is judged.
   */
  @Test
  void aScopedRuleKeepsAConditionTheEditorWouldNotSuggest() {
    assertNull(
        RuleBuilderDialog.clauseProblem(
            new RuleClause("port", RuleCondition.MATCHES_PATTERN, "^\\d+$"),
            RuleTarget.TRANSFORM,
            true));
    assertNull(
        RuleBuilderDialog.clauseProblem(
            new RuleClause("commitSize", RuleCondition.MAX_VALUE, "1000"),
            RuleTarget.TRANSFORM,
            false));
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
