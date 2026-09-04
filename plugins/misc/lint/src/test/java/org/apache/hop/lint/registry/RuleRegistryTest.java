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

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.apache.hop.lint.CustomLintRule;
import org.apache.hop.lint.RuleCombinator;
import org.junit.jupiter.api.Test;

public class RuleRegistryTest {

  @Test
  public void discoveryAlwaysIncludesHopCorePack() {
    List<IHopLintRulePack> packs = new RulePackDiscovery().discoverAll();
    assertTrue(packs.stream().anyMatch(pack -> RulePackIds.HOP_CORE.equals(pack.getPackId())));
  }

  @Test
  public void loadsHopCorePackByDefault() {
    EffectiveRuleSet rules = RuleRegistry.getInstance().resolve(null);
    // Assert on membership, not an exact count: the core pack's contents are expected to be
    // tuned, and a count assertion turns every rule change into a test failure.
    assertFalse(rules.getRules().isEmpty());
    assertTrue(rules.getRules().stream().anyMatch(rule -> "DB-001".equals(rule.generateRuleId())));
    assertTrue(
        rules.getRules().stream().anyMatch(rule -> "TRANS-002".equals(rule.generateRuleId())));
    assertTrue(
        rules.getRules().stream().allMatch(rule -> rule.getPackOwner() == RulePackOwner.APACHE));
  }

  @Test
  public void projectYamlCanRetuneAThreshold() throws Exception {
    // Retuning a threshold is the commonest override, and it used to be dropped on the floor: the
    // rule was enabled as asked while the pack's own ceiling quietly stayed in force.
    File projectYaml = File.createTempFile("hop-lint", ".yml");
    Files.writeString(
        projectYaml.toPath(),
        "rules:\n  STRUCT-001:\n    enabled: true\n    conditionValue: \"5\"\n");
    try {
      EffectiveRuleSet rules = RuleRegistry.getInstance().resolve(projectYaml);
      CustomLintRule struct001 =
          rules.getRules().stream()
              .filter(rule -> "STRUCT-001".equals(rule.generateRuleId()))
              .findFirst()
              .orElseThrow();
      assertTrue(struct001.isEnabled());
      assertEquals("5", struct001.getConditionValue());
    } finally {
      Files.deleteIfExists(projectYaml.toPath());
    }
  }

  @Test
  public void projectYamlCanDisablePackRule() throws Exception {
    File projectYaml = File.createTempFile("hop-lint", ".yml");
    Files.writeString(projectYaml.toPath(), "rules:\n  TRANS-002:\n    enabled: false\n");
    try {
      EffectiveRuleSet rules = RuleRegistry.getInstance().resolve(projectYaml);
      CustomLintRule trans002 =
          rules.getRules().stream()
              .filter(rule -> "TRANS-002".equals(rule.generateRuleId()))
              .findFirst()
              .orElseThrow();
      assertFalse(trans002.isEnabled());
    } finally {
      projectYaml.delete();
    }
  }

  @Test
  public void projectYamlCanAddLocalRule() throws Exception {
    File projectYaml = File.createTempFile("hop-lint", ".yml");
    Files.writeString(
        projectYaml.toPath(),
        "rules:\n"
            + "  LOCAL-001:\n"
            + "    type: custom\n"
            + "    enabled: true\n"
            + "    severity: ERROR\n"
            + "    target: PIPELINE\n"
            + "    targetField: name\n"
            + "    condition: NOT_EMPTY\n"
            + "    name: Local Pipeline Name\n");
    try {
      EffectiveRuleSet rules = RuleRegistry.getInstance().resolve(projectYaml);
      assertEquals(
          RuleRegistry.getInstance().resolve(null).getRules().size() + 1, rules.getRules().size());
      assertTrue(
          rules.getRules().stream().anyMatch(rule -> "LOCAL-001".equals(rule.generateRuleId())));
    } finally {
      projectYaml.delete();
    }
  }

  @Test
  public void exporterWritesOverridesOnly() throws Exception {
    CustomLintRule doc001 =
        RuleRegistry.getInstance().resolve(null).getRules().stream()
            .filter(rule -> "TRANS-002".equals(rule.generateRuleId()))
            .findFirst()
            .orElseThrow()
            .copy();
    doc001.setEnabled(false);
    String yaml = ProjectLintYamlExporter.export(java.util.List.of(doc001));
    assertTrue(yaml.contains("TRANS-002"));
    assertTrue(yaml.contains("enabled: false"));
    assertFalse(yaml.contains("type: custom"));
  }

  @Test
  public void projectYamlCanDefineAComposedRule() throws Exception {
    File projectYaml = File.createTempFile("hop-lint", ".yml");
    Files.writeString(
        projectYaml.toPath(),
        """
        rules:
          LOCAL-900:
            type: custom
            enabled: true
            severity: ERROR
            target: PIPELINE
            name: "Big and undocumented"
            allOf:
              - targetField: transformCount
                condition: MAX_VALUE
                conditionValue: "20"
              - targetField: description
                condition: NOT_EMPTY
        """);
    try {
      EffectiveRuleSet rules = RuleRegistry.getInstance().resolve(projectYaml);
      CustomLintRule composed =
          rules.getRules().stream()
              .filter(rule -> "LOCAL-900".equals(rule.generateRuleId()))
              .findFirst()
              .orElseThrow();

      assertTrue(composed.isComposed());
      assertEquals(RuleCombinator.ALL_OF, composed.getCombinator());
      assertEquals(2, composed.getClauses().size());
      // The first clause is kept in the rule's own fields, so anything reading a simple rule
      // still sees something sensible.
      assertEquals("transformCount", composed.getTargetField());
      assertEquals("description", composed.getAdditionalClauses().get(0).getTargetField());

      // And it survives being written back out.
      String yaml = ProjectLintYamlExporter.export(java.util.List.of(composed));
      assertTrue(yaml.contains("allOf"), "a composed rule round-trips as allOf");
      assertTrue(yaml.contains("transformCount"));
      assertTrue(yaml.contains("description"));
    } finally {
      projectYaml.delete();
    }
  }

  @Test
  public void exporterWritesAPackRuleInFullWhenItIsRedefined() throws Exception {
    // Tuning a pack rule is an override. Changing what it looks at is a redefinition, and an
    // override block cannot carry that: written as one, the new target and field would be dropped
    // and the rule would go on checking what it always did.
    CustomLintRule redefined =
        RuleRegistry.getInstance().resolve(null).getRules().stream()
            .filter(rule -> "TRANS-002".equals(rule.generateRuleId()))
            .findFirst()
            .orElseThrow()
            .copy();
    redefined.setTargetField("hasDisabledHops");

    String yaml = ProjectLintYamlExporter.export(java.util.List.of(redefined));

    assertTrue(yaml.contains("TRANS-002"));
    assertTrue(yaml.contains("type: custom"), "a redefined pack rule is written out in full");
    assertTrue(yaml.contains("hasDisabledHops"));
  }

  @Test
  public void exporterStillWritesAnOverrideWhenOnlyAThresholdChanges() throws Exception {
    CustomLintRule retuned =
        RuleRegistry.getInstance().resolve(null).getRules().stream()
            .filter(rule -> "STRUCT-001".equals(rule.generateRuleId()))
            .findFirst()
            .orElseThrow()
            .copy();
    retuned.setConditionValue("30");

    String yaml = ProjectLintYamlExporter.export(java.util.List.of(retuned));

    assertTrue(yaml.contains("conditionValue: '30'") || yaml.contains("conditionValue: \"30\""));
    assertFalse(yaml.contains("type: custom"), "a retuned pack rule stays an override");
  }

  /**
   * Pack rules are cached, so each caller has to get its own copies. Handing out the cached objects
   * would let one caller's edit — the rule manager toggling a rule, a project overlay disabling one
   * — silently change what every later lint run sees.
   */
  @Test
  public void resolutionsDoNotShareMutableRules() {
    CustomLintRule first =
        RuleRegistry.getInstance().resolve(null).getRules().stream()
            .filter(rule -> "TRANS-002".equals(rule.generateRuleId()))
            .findFirst()
            .orElseThrow();
    assertTrue(first.isEnabled(), "precondition: TRANS-002 ships enabled");

    first.setEnabled(false);
    first.setSeverity("INFO");

    CustomLintRule second =
        RuleRegistry.getInstance().resolve(null).getRules().stream()
            .filter(rule -> "TRANS-002".equals(rule.generateRuleId()))
            .findFirst()
            .orElseThrow();

    assertTrue(second.isEnabled(), "a previous caller's edit leaked into the cache");
    assertEquals("WARNING", second.getSeverity());
  }

  /**
   * A pack may not quietly stand in for another pack's rule. Without this, a third-party pack could
   * ship its own DB-001 and replace Apache's hardcoded-password check with something weaker, and
   * the rule list would look unchanged.
   */
  @Test
  public void aPackCannotSilentlyReplaceAnotherPacksRule() {
    Map<String, CustomLintRule> merged = new java.util.LinkedHashMap<>();
    RuleRegistry.mergePack(
        merged, packOf("hop-core", List.of(), ruleFrom("hop-core", "DB-001", "ERROR")));

    RuleRegistry.mergePack(merged, packOf("acme", List.of(), ruleFrom("acme", "DB-001", "INFO")));

    CustomLintRule surviving = merged.get("DB-001");
    assertEquals("hop-core", surviving.getPackId(), "the squatting pack took over the rule id");
    assertEquals("ERROR", surviving.getSeverity());
  }

  /** Declaring the intent is what makes it allowed. */
  @Test
  public void aDeclaredOverrideIsApplied() {
    Map<String, CustomLintRule> merged = new java.util.LinkedHashMap<>();
    RuleRegistry.mergePack(
        merged, packOf("hop-core", List.of(), ruleFrom("hop-core", "DB-001", "ERROR")));

    RuleRegistry.mergePack(
        merged, packOf("acme", List.of("DB-001"), ruleFrom("acme", "DB-001", "INFO")));

    CustomLintRule surviving = merged.get("DB-001");
    assertEquals("acme", surviving.getPackId());
    assertEquals("INFO", surviving.getSeverity());
  }

  /** A pack redefining its own rule is not a collision. */
  @Test
  public void aPackMayRedefineItsOwnRule() {
    Map<String, CustomLintRule> merged = new java.util.LinkedHashMap<>();
    RuleRegistry.mergePack(
        merged, packOf("acme", List.of(), ruleFrom("acme", "ACME-001", "WARNING")));
    RuleRegistry.mergePack(
        merged, packOf("acme", List.of(), ruleFrom("acme", "ACME-001", "ERROR")));

    assertEquals("ERROR", merged.get("ACME-001").getSeverity());
  }

  /** Rule ids are matched without regard to case, as they are hand-written in YAML. */
  @Test
  public void overrideDeclarationIgnoresCase() {
    Map<String, CustomLintRule> merged = new java.util.LinkedHashMap<>();
    RuleRegistry.mergePack(
        merged, packOf("hop-core", List.of(), ruleFrom("hop-core", "DB-001", "ERROR")));

    RuleRegistry.mergePack(
        merged, packOf("acme", List.of("db-001"), ruleFrom("acme", "DB-001", "INFO")));

    assertEquals("acme", merged.get("DB-001").getPackId());
  }

  private CustomLintRule ruleFrom(String packId, String ruleId, String severity) {
    CustomLintRule rule = new CustomLintRule();
    rule.setId(ruleId);
    rule.setPackId(packId);
    rule.setSeverity(severity);
    rule.setEnabled(true);
    return rule;
  }

  private IHopLintRulePack packOf(String packId, List<String> overrides, CustomLintRule... rules) {
    return new IHopLintRulePack() {
      @Override
      public String getPackId() {
        return packId;
      }

      @Override
      public String getDisplayName() {
        return packId;
      }

      @Override
      public RulePackOwner getOwner() {
        return RulePackOwner.VENDOR;
      }

      @Override
      public int getPriority() {
        return 500;
      }

      @Override
      public List<CustomLintRule> loadRules() {
        return List.of(rules);
      }

      @Override
      public List<String> getOverrides() {
        return overrides;
      }
    };
  }

  /** A project overlay must not contaminate the cached pack rules either. */
  @Test
  public void projectOverlayDoesNotLeakIntoLaterResolutions() throws Exception {
    File projectYaml = File.createTempFile("hop-lint", ".yml");
    Files.writeString(projectYaml.toPath(), "rules:\n  TRANS-002:\n    enabled: false\n");
    try {
      assertFalse(
          RuleRegistry.getInstance().resolve(projectYaml).getRules().stream()
              .filter(rule -> "TRANS-002".equals(rule.generateRuleId()))
              .findFirst()
              .orElseThrow()
              .isEnabled());

      assertTrue(
          RuleRegistry.getInstance().resolve(null).getRules().stream()
              .filter(rule -> "TRANS-002".equals(rule.generateRuleId()))
              .findFirst()
              .orElseThrow()
              .isEnabled(),
          "the project overlay disabled the rule for every later run");
    } finally {
      projectYaml.delete();
    }
  }
}
