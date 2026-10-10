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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.IHopLoggingEventListener;
import org.apache.hop.core.logging.LogLevel;
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

  /**
   * Toggling HOP-CHECK in the rule manager is how a project decides it wants Hop's own verify
   * remarks back at the severity the transforms meant, or gone. It has to survive the round trip
   * through the project's hop-lint.yml — a native rule written out with the custom rule's shape
   * would carry a null target and read back as a rule that checks nothing.
   */
  @Test
  public void aNativeRuleSurvivesTheRoundTripThroughTheProjectYaml() throws Exception {
    CustomLintRule hopCheck =
        RuleRegistry.getInstance().resolve(null).getRules().stream()
            .filter(rule -> "HOP-CHECK".equals(rule.generateRuleId()))
            .findFirst()
            .orElseThrow()
            .copy();
    hopCheck.setSeverity("ERROR");

    String yaml = ProjectLintYamlExporter.export(java.util.List.of(hopCheck));
    assertTrue(yaml.contains("HOP-CHECK"));
    assertTrue(yaml.contains("severity: ERROR"));
    assertFalse(yaml.contains("target:"), "a native rule has nothing to evaluate against");

    File projectYaml = File.createTempFile("hop-lint", ".yml");
    try {
      Files.writeString(projectYaml.toPath(), yaml);
      CustomLintRule readBack =
          RuleRegistry.getInstance().resolve(projectYaml).getRules().stream()
              .filter(rule -> "HOP-CHECK".equals(rule.generateRuleId()))
              .findFirst()
              .orElseThrow();

      assertTrue(readBack.isNativeVerify());
      assertEquals("ERROR", readBack.getSeverity());
    } finally {
      projectYaml.delete();
    }
  }

  /** A project can name a check of its own, and the narrowing has to come back with the rule. */
  @Test
  public void aProjectCanDefineItsOwnNativeRule() throws Exception {
    File projectYaml = File.createTempFile("hop-lint", ".yml");
    try {
      Files.writeString(
          projectYaml.toPath(),
          """
          rules:
            HOP-CHECK-TABLEINPUT-SQL:
              type: native
              enabled: false
              appliesTo:
                - TableInput
              messageKey: "org.apache.hop.pipeline.transforms.tableinput:TableInputMeta.CheckResult.NoInput"
              name: "Table Input SQL remark"
          """);

      CustomLintRule rule =
          RuleRegistry.getInstance().resolve(projectYaml).getRules().stream()
              .filter(r -> "HOP-CHECK-TABLEINPUT-SQL".equals(r.generateRuleId()))
              .findFirst()
              .orElseThrow();

      assertTrue(rule.isNativeVerify());
      assertFalse(rule.isEnabled());
      assertEquals(java.util.List.of("TableInput"), rule.getAppliesTo());
      assertTrue(rule.getMessageKey().endsWith(":TableInputMeta.CheckResult.NoInput"));
    } finally {
      projectYaml.delete();
    }
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

  /**
   * A typo in hop-lint.yml used to change nothing and say nothing: SQL-002 stayed disabled.
   *
   * @see <a href="https://github.com/apache/hop/issues/8731">#8731</a>
   */
  @Test
  public void anUnknownRuleIdIsReportedWithTheClosestId() throws Exception {
    File projectYaml = File.createTempFile("hop-lint", ".yml");
    Files.writeString(projectYaml.toPath(), "rules:\n  SQL-02:\n    enabled: true\n");
    try {
      EffectiveRuleSet rules = RuleRegistry.getInstance().resolve(projectYaml);

      assertEquals(1, rules.getWarnings().size(), rules.getWarnings().toString());
      String warning = rules.getWarnings().get(0);
      assertTrue(warning.contains("'SQL-02'"), warning);
      assertTrue(warning.contains("Did you mean SQL-002?"), warning);
      assertFalse(
          rules.getRules().stream()
              .filter(rule -> "SQL-002".equals(rule.generateRuleId()))
              .findFirst()
              .orElseThrow()
              .isEnabled());
    } finally {
      Files.deleteIfExists(projectYaml.toPath());
    }
  }

  @Test
  public void aRuleIdInAnotherCaseIsTheSameRule() throws Exception {
    File projectYaml = File.createTempFile("hop-lint", ".yml");
    Files.writeString(projectYaml.toPath(), "rules:\n  trans-002:\n    enabled: false\n");
    try {
      EffectiveRuleSet rules = RuleRegistry.getInstance().resolve(projectYaml);

      assertTrue(rules.getWarnings().isEmpty(), rules.getWarnings().toString());
      assertFalse(
          rules.getRules().stream()
              .filter(rule -> "TRANS-002".equals(rule.generateRuleId()))
              .findFirst()
              .orElseThrow()
              .isEnabled());
    } finally {
      Files.deleteIfExists(projectYaml.toPath());
    }
  }

  /**
   * Hop Gui resolves the rules for every file it lints and on every background check, so a warning
   * logged each time filled the log for as long as the project kept the id.
   */
  @Test
  public void anUnknownRuleIdIsLoggedOnce() throws Exception {
    HopLogStore.init();
    File projectYaml = File.createTempFile("hop-lint", ".yml");
    Files.writeString(projectYaml.toPath(), "rules:\n  NO-SUCH-RULE:\n    enabled: true\n");
    List<String> logged = new ArrayList<>();
    IHopLoggingEventListener listener =
        event -> {
          if (event.getLevel() == LogLevel.MINIMAL
              && String.valueOf(event.getMessage()).contains("'NO-SUCH-RULE'")) {
            logged.add(String.valueOf(event.getMessage()));
          }
        };
    HopLogStore.getAppender().addLoggingEventListener(listener);
    try {
      EffectiveRuleSet first = RuleRegistry.getInstance().resolve(projectYaml);
      EffectiveRuleSet second = RuleRegistry.getInstance().resolve(projectYaml);

      assertEquals(1, logged.size(), logged.toString());
      assertEquals(1, first.getWarnings().size());
      assertEquals(first.getWarnings(), second.getWarnings(), "every resolution still reports it");
    } finally {
      HopLogStore.getAppender().removeLoggingEventListener(listener);
      Files.deleteIfExists(projectYaml.toPath());
    }
  }

  @Test
  public void anIdFarFromAnyRuleGetsNoSuggestion() throws Exception {
    String warning =
        RuleRegistry.unknownRuleWarning(
            "COMPLETELY-DIFFERENT", List.of("SQL-002", "DB-001"), new File("hop-lint.yml"));

    assertFalse(warning.contains("Did you mean"), warning);
  }

  /**
   * A pack that cannot be read is skipped by Hop Gui, but its error is kept, so hop lint and the
   * Run Linter action can fail with the pack's own message instead of passing without its rules.
   *
   * @see <a href="https://github.com/apache/hop/issues/8826">#8826</a>
   */
  @Test
  public void aBrokenPackIsRecordedWithItsOwnError() throws Exception {
    File yaml = File.createTempFile("broken-pack", ".yml");
    yaml.deleteOnExit();
    Files.writeString(yaml.toPath(), "rules:\n  - id: [unterminated\n");
    IHopLintRulePack broken =
        EagerRulePack.of(new FileYamlRulePack(yaml, "acme", "Acme", RulePackOwner.VENDOR, 100));
    RulePackDiscovery discovery =
        new RulePackDiscovery() {
          @Override
          public List<IHopLintRulePack> discoverAll() {
            return List.of(new HopCoreRulePack(), broken);
          }
        };

    RuleRegistry registry = new RuleRegistry(discovery);

    List<String> errors = registry.getPackErrors();
    assertEquals(1, errors.size(), errors.toString());
    assertTrue(errors.get(0).startsWith("Rule pack 'acme' could not be loaded"), errors.get(0));
    // The parser's own words, with the line, and not only "Failed to load rule pack from ...".
    assertTrue(errors.get(0).contains("line 2"), errors.get(0));
    assertFalse(registry.resolve(null).getRules().isEmpty(), "the other packs still load");
  }

  @Test
  public void packsThatLoadLeaveNoErrors() {
    RulePackDiscovery discovery =
        new RulePackDiscovery() {
          @Override
          public List<IHopLintRulePack> discoverAll() {
            return List.of(new HopCoreRulePack());
          }
        };

    assertTrue(new RuleRegistry(discovery).getPackErrors().isEmpty());
  }

  /** A cause chain that loops back on itself must not hang the registry while it holds its lock. */
  @Test
  public void aLoopingCauseChainEnds() {
    Exception outer = new Exception("outer");
    Exception inner = new Exception("inner", outer);
    outer.initCause(inner);

    assertEquals("outer: inner", RuleRegistry.messagesOf(outer));
  }
}
