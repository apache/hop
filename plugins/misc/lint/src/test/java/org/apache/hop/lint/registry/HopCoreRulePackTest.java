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
package org.apache.hop.lint.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.lint.CustomLintRule;
import org.junit.jupiter.api.Test;

public class HopCoreRulePackTest {

  @Test
  public void loadsCoreRulesFromClasspath() {
    List<CustomLintRule> rules = new HopCoreRulePack().loadRules();

    assertFalse(rules.isEmpty());
    assertTrue(rules.stream().anyMatch(rule -> "DB-001".equals(rule.generateRuleId())));
    // No rule from another pack may leak into the Apache one: a vendor pack standing in for a core
    // rule has to declare it in its overrides: block, where it is visible, rather than arriving
    // here.
    assertTrue(rules.stream().allMatch(rule -> rule.getPackOwner() == RulePackOwner.APACHE));
    assertTrue(
        rules.stream().allMatch(rule -> RulePackIds.HOP_CORE.equals(rule.getPackId())),
        "every rule in the core pack must belong to the core pack");
  }

  /**
   * The pack ships a composed rule as a worked example of the format, so a parse regression in the
   * allOf/anyOf handling shows up here rather than in someone's project.
   */
  @Test
  public void shipsAComposedRuleAsAWorkedExample() {
    CustomLintRule composed =
        new HopCoreRulePack()
            .loadRules().stream()
                .filter(rule -> "SQL-002".equals(rule.generateRuleId()))
                .findFirst()
                .orElseThrow();

    assertTrue(composed.isComposed(), "SQL-002 is the example of a multi-clause rule");
    assertEquals(2, composed.getClauses().size());
    assertFalse(composed.isEnabled(), "worked examples ship disabled");
  }

  /**
   * The core pack ships to every Hop user with these rules on, so the enabled set is a policy
   * decision, not an incidental one: a rule is only enabled here if a violation is a defect in any
   * project. Adding one is a deliberate act, and this test is where that gets noticed.
   */
  @Test
  public void onlyUniversallyDefensibleRulesAreEnabledByDefault() {
    List<String> enabled =
        new HopCoreRulePack()
            .loadRules().stream()
                .filter(CustomLintRule::isEnabled)
                .map(CustomLintRule::generateRuleId)
                .sorted()
                .toList();

    assertEquals(
        List.of("DB-001", "NAMING-004", "SEC-002", "SEC-003", "TRANS-002", "WORKFLOW-002"),
        enabled);
  }

  /**
   * Everything else ships as a worked example. Naming conventions and size ceilings are house
   * style; enabling them by default would fail Hop's own sample projects.
   */
  @Test
  public void houseStyleRulesShipDisabled() {
    List<CustomLintRule> rules = new HopCoreRulePack().loadRules();

    assertTrue(
        rules.stream()
            .filter(
                rule ->
                    rule.generateRuleId().startsWith("NAMING-001")
                        || rule.generateRuleId().startsWith("NAMING-002")
                        || rule.generateRuleId().startsWith("STRUCT-")
                        || rule.generateRuleId().startsWith("DOC-")
                        || rule.generateRuleId().startsWith("PERF-"))
            .noneMatch(CustomLintRule::isEnabled));
  }

  /** Secrets in the project are the one thing the platform treats as a hard error. */
  @Test
  public void secretRulesAreErrors() {
    List<CustomLintRule> rules = new HopCoreRulePack().loadRules();

    assertTrue(
        rules.stream()
            .filter(rule -> rule.generateRuleId().matches("DB-001|SEC-\\d+"))
            .allMatch(rule -> "ERROR".equals(rule.getSeverity())));
  }
}
