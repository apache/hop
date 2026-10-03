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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.lint.registry.HopCoreRulePack;
import org.apache.hop.lint.registry.ProjectLintYamlExporter;
import org.apache.hop.lint.registry.RuleRegistry;
import org.junit.jupiter.api.Test;

/**
 * What the rule editor offers, and what the rule manager writes for one rule.
 *
 * <p>The editor guessed a field's type from its name and only offered the matching conditions, so a
 * rule on {@code isDummy}, {@code isReferenced} or {@code isOrphaned} opened with an empty field or
 * condition and could not be saved at all, not even to change its severity.
 *
 * @see <a href="https://github.com/apache/hop/issues/8734">#8734</a>
 */
public class RuleEditorChoicesTest {

  /** Every rule the core pack ships opens in the editor with its own field and condition. */
  @Test
  public void everyCoreRuleFitsTheEditor() {
    List<String> problems = new ArrayList<>();
    for (CustomLintRule rule : new HopCoreRulePack().loadRules()) {
      if (rule.isNativeVerify()) {
        continue;
      }
      for (RuleClause clause : rule.getClauses()) {
        String where = rule.generateRuleId() + " " + clause.getTargetField();
        // A rule scoped to plugin types reads that plugin's own settings, which no fixed list
        // can name; the editor keeps such a field rather than offering it.
        if (rule.getAppliesTo().isEmpty()
            && !RuleTargetFields.getFieldsForTarget(rule.getTarget())
                .contains(clause.getTargetField())) {
          problems.add(where + ": field not offered for " + rule.getTarget());
        }
        if (!RuleTargetFields.getCompatibleConditions(clause.getTargetField())
            .contains(clause.getCondition())) {
          problems.add(where + ": " + clause.getCondition() + " not offered");
        }
      }
    }
    assertTrue(problems.isEmpty(), String.join("\n", problems));
  }

  @Test
  public void isAndHasFieldsAreFlags() {
    for (String field : List.of("isDummy", "isReferenced", "isOrphaned", "hasNotes")) {
      assertEquals(
          List.of(RuleCondition.MUST_BE_TRUE, RuleCondition.MUST_BE_FALSE),
          RuleTargetFields.getCompatibleConditions(field),
          field);
    }
    for (String field : List.of("issuer", "hash", "isbn")) {
      assertFalse(
          RuleTargetFields.getCompatibleConditions(field).contains(RuleCondition.MUST_BE_TRUE),
          field);
    }
  }

  /** A Putki rule reads a Table Output's own truncateTable setting, and PUTKI-ENV-006 a port. */
  @Test
  public void aFieldOrConditionTheEditorWouldNotSuggestIsKept() {
    List<String> fields = RuleTargetFields.getFieldChoices(RuleTarget.TRANSFORM, "truncateTable");
    assertTrue(fields.contains("truncateTable"));
    assertTrue(fields.containsAll(RuleTargetFields.getFieldsForTarget(RuleTarget.TRANSFORM)));

    assertTrue(
        RuleTargetFields.getConditionChoices("port", RuleCondition.NO_HARDCODED)
            .contains(RuleCondition.NO_HARDCODED));
    assertEquals(
        RuleTargetFields.getCompatibleConditions("port"),
        RuleTargetFields.getConditionChoices("port", null));
  }

  @Test
  public void anUnchangedPackRuleNeedsNoEntry() {
    CustomLintRule rule = coreRule("DOC-001");

    assertNull(ProjectLintYamlExporter.entryFor(rule));
  }

  @Test
  public void aTunedPackRuleWritesOnlyWhatChanged() {
    CustomLintRule rule = coreRule("DOC-001");
    rule.setSeverity("INFO");

    assertEquals(Map.of("severity", "INFO"), ProjectLintYamlExporter.entryFor(rule));
  }

  @Test
  public void aProjectRuleWithoutParametersWritesNone() {
    CustomLintRule rule = coreRule("DOC-001");
    rule.setId("CRM-001");
    rule.setPackOwner(org.apache.hop.lint.registry.RulePackOwner.PROJECT);

    Map<String, Object> entry = ProjectLintYamlExporter.entryFor(rule);

    assertEquals("custom", entry.get("type"));
    assertFalse(entry.containsKey("parameters"), entry.toString());
  }

  private static CustomLintRule coreRule(String id) {
    return RuleRegistry.getInstance().resolve(null).getRules().stream()
        .filter(rule -> id.equals(rule.generateRuleId()))
        .findFirst()
        .orElseThrow()
        .copy();
  }
}
