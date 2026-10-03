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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.lint.registry.RuleRegistry;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Fixes to the core rule pack and to how rules are read and evaluated.
 *
 * @see <a href="https://github.com/apache/hop/issues/8733">#8733</a>
 */
public class CoreRulePackFixesTest {

  @BeforeAll
  static void loadPlugins() throws Exception {
    HopEnvironment.init();
  }

  private static CustomLintRule coreRule(String id) {
    CustomLintRule rule =
        RuleRegistry.getInstance().resolve(null).getRules().stream()
            .filter(r -> id.equals(r.generateRuleId()))
            .findFirst()
            .orElseThrow()
            .copy();
    rule.setEnabled(true);
    return rule;
  }

  // ------------------------------------------------------------------ NAMING-004

  /**
   * Hop Gui names a new transform after its plugin, with a number when the name is taken. Only
   * "Transform 1" counted, which Hop Gui never generates.
   */
  @Test
  public void theNamesHopGuiGeneratesAreDefaultNames() {
    for (String name :
        List.of(
            "Dummy (do nothing)", "Dummy (do nothing) 2", "dummy (DO nothing) 12", "Transform 1")) {
      assertTrue(
          CustomRuleExecutor.hasDefaultGeneratedName(name, "Dummy", TransformPluginType.class),
          name);
    }
    for (String name : List.of("Load customers", "Dummy (do nothing) copy", "Dummy")) {
      assertFalse(
          CustomRuleExecutor.hasDefaultGeneratedName(name, "Dummy", TransformPluginType.class),
          name);
    }
  }

  @Test
  public void naming004ReportsADefaultTransformName() {
    TransformMeta dummy = new TransformMeta();
    dummy.setName("Dummy (do nothing) 2");
    dummy.setTransformPluginId("Dummy");

    assertEquals(
        1, CustomRuleExecutor.executeRule(coreRule("NAMING-004"), dummy, "/tmp/p.hpl").size());
  }

  /** Every workflow starts at Start, and there is no better name for it. */
  @Test
  public void theStartActionNeverHasADefaultName() {
    ActionMeta start = new ActionMeta(new ActionStart());
    start.setName("Start");
    CustomLintRule rule = coreRule("NAMING-004");
    rule.setTarget(RuleTarget.ACTION);

    assertTrue(CustomRuleExecutor.executeRule(rule, start, "/tmp/w.hwf").isEmpty());
  }

  @Test
  public void listFieldsShowsTheFieldsEveryTransformHas() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream systemOut = System.out;
    try {
      System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
      new CommandLine(new LintCommand()).execute("--list-fields", "Dummy");
    } finally {
      System.setOut(systemOut);
    }

    String listing = out.toString(StandardCharsets.UTF_8);
    assertTrue(listing.contains("Fields on every transform:"), listing);
    assertTrue(listing.contains("hasDefaultName"), listing);
    assertTrue(listing.contains("isOrphaned"), listing);
  }

  // ------------------------------------------------------------------ SQL-002 and allOf

  /** Stands in for a Table Input: the two fields SQL-002 reads. */
  public static class FakeTableInputMeta extends BaseTransformMeta {
    private String sql;
    private String rowLimit;

    FakeTableInputMeta(String sql, String rowLimit) {
      this.sql = sql;
      this.rowLimit = rowLimit;
    }
  }

  private static boolean sql002Reports(String sql, String rowLimit) {
    TransformMeta tableInput =
        new TransformMeta("TableInput", "Read customers", new FakeTableInputMeta(sql, rowLimit));
    return !CustomRuleExecutor.executeRule(coreRule("SQL-002"), tableInput, "/tmp/p.hpl").isEmpty();
  }

  /** Both clauses were the wrong way round, so the rule could not fire on what it describes. */
  @Test
  public void sql002ReportsOnlyAnUnboundedSelectStar() {
    assertTrue(sql002Reports("SELECT * FROM customers", "0"), "SELECT *, no limit");
    assertTrue(sql002Reports("select *\nfrom customers", ""), "SELECT *, empty limit");
    assertFalse(sql002Reports("SELECT * FROM customers", "100"), "SELECT *, limit 100");
    assertFalse(sql002Reports("SELECT * FROM customers", "${LIMIT}"), "limit from a variable");
    assertFalse(sql002Reports("SELECT id, name FROM customers", "0"), "named columns");
  }

  @Test
  public void isEmptyRequiresTheFieldToBeUnset() {
    CustomLintRule rule = new CustomLintRule();
    rule.setEnabled(true);
    rule.setSeverity("WARNING");
    rule.setTarget(RuleTarget.PIPELINE);
    rule.setTargetField("description");
    rule.setCondition(RuleCondition.IS_EMPTY);

    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("load");
    assertTrue(CustomRuleExecutor.executeRule(rule, pipeline, "/tmp/p.hpl").isEmpty(), "unset");

    pipeline.setDescription("Loads the customers");
    assertEquals(1, CustomRuleExecutor.executeRule(rule, pipeline, "/tmp/p.hpl").size(), "set");
  }

  /**
   * An allOf rule written without type: custom was taken for an override of a rule that does not
   * exist, and never ran.
   */
  @Test
  public void aComposedRuleNeedsNoTypeCustom() throws Exception {
    File projectYaml = File.createTempFile("hop-lint", ".yml");
    Files.writeString(
        projectYaml.toPath(),
        """
        rules:
          HTTP-001:
            target: TRANSFORM
            severity: WARNING
            allOf:
              - targetField: url
                condition: MATCHES_PATTERN
                conditionValue: "^https://.*"
              - targetField: httpLogin
                condition: IS_EMPTY
        """);
    try {
      CustomLintRule rule =
          RuleRegistry.getInstance().resolve(projectYaml).getRules().stream()
              .filter(r -> "HTTP-001".equals(r.generateRuleId()))
              .findFirst()
              .orElseThrow();

      assertEquals(2, rule.getClauses().size());
      assertEquals(RuleCombinator.ALL_OF, rule.getCombinator());
    } finally {
      Files.deleteIfExists(projectYaml.toPath());
    }
  }
}
