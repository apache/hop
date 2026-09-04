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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

/**
 * Tests for the newer {@link CustomRuleExecutor} behaviours: HOP targets, the configurable
 * blocking-transform list and DatabaseMeta field extraction.
 */
public class CustomRuleExecutorTest {

  private CustomLintRule baseRule(RuleTarget target, String field, RuleCondition condition) {
    CustomLintRule rule = new CustomLintRule();
    rule.setEnabled(true);
    rule.setSeverity("WARNING");
    rule.setTarget(target);
    rule.setTargetField(field);
    rule.setCondition(condition);
    rule.setName("Test Rule");
    rule.setDescription("Test rule description");
    return rule;
  }

  @Test
  public void flagsDisabledPipelineHop() {
    TransformMeta from = new TransformMeta();
    from.setName("A");
    TransformMeta to = new TransformMeta();
    to.setName("B");
    PipelineHopMeta hop = new PipelineHopMeta(from, to, false);

    CustomLintRule rule = baseRule(RuleTarget.HOP, "enabled", RuleCondition.MUST_BE_TRUE);

    List<LintResult> results = CustomRuleExecutor.executeRule(rule, hop, "/tmp/test.hpl");

    assertEquals(1, results.size());
    assertEquals(LintSourceRef.Kind.HOP, results.get(0).getSource().getKind());
    assertTrue(results.get(0).getSource().getName().contains("A -> B"));
  }

  @Test
  public void enabledPipelineHopPasses() {
    TransformMeta from = new TransformMeta();
    from.setName("A");
    TransformMeta to = new TransformMeta();
    to.setName("B");
    PipelineHopMeta hop = new PipelineHopMeta(from, to, true);

    CustomLintRule rule = baseRule(RuleTarget.HOP, "enabled", RuleCondition.MUST_BE_TRUE);

    List<LintResult> results = CustomRuleExecutor.executeRule(rule, hop, "/tmp/test.hpl");

    assertTrue(results.isEmpty());
  }

  @Test
  public void usesConfiguredBlockingTransformList() {
    TransformMeta custom = new TransformMeta();
    custom.setName("My Blocker");
    custom.setTransformPluginId("MyCustomBlockingTransform");

    CustomLintRule rule =
        baseRule(RuleTarget.TRANSFORM, "isBlockingTransform", RuleCondition.MUST_BE_FALSE);
    rule.getAdditionalParameters()
        .put("blockingTransforms", Arrays.asList("MyCustomBlockingTransform"));

    List<LintResult> results = CustomRuleExecutor.executeRule(rule, custom, "/tmp/test.hpl");

    assertEquals(1, results.size());
  }

  @Test
  public void defaultBlockingListIgnoresUnlistedPlugin() {
    TransformMeta custom = new TransformMeta();
    custom.setName("My Blocker");
    custom.setTransformPluginId("MyCustomBlockingTransform");

    CustomLintRule rule =
        baseRule(RuleTarget.TRANSFORM, "isBlockingTransform", RuleCondition.MUST_BE_FALSE);

    List<LintResult> results = CustomRuleExecutor.executeRule(rule, custom, "/tmp/test.hpl");

    assertTrue(results.isEmpty());
  }

  /**
   * Hop returns null for a description that was never set, which is precisely the case a
   * "description required" rule exists to catch. Treating null as passing made the rule fire only
   * on a description explicitly set to the empty string — that is, almost never.
   */
  @Test
  public void notEmptyFlagsAnAbsentValue() {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("test");
    assertNull(pipeline.getDescription(), "precondition: an unset description is null");

    CustomLintRule rule = baseRule(RuleTarget.PIPELINE, "description", RuleCondition.NOT_EMPTY);

    List<LintResult> results = CustomRuleExecutor.executeRule(rule, pipeline, "/tmp/test.hpl");

    assertEquals(1, results.size());
  }

  @Test
  public void notEmptyFlagsABlankValue() {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("test");
    pipeline.setDescription("");

    CustomLintRule rule = baseRule(RuleTarget.PIPELINE, "description", RuleCondition.NOT_EMPTY);

    assertEquals(1, CustomRuleExecutor.executeRule(rule, pipeline, "/tmp/test.hpl").size());
  }

  @Test
  public void notEmptyAcceptsAPresentValue() {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("test");
    pipeline.setDescription("Loads the customer dimension.");

    CustomLintRule rule = baseRule(RuleTarget.PIPELINE, "description", RuleCondition.NOT_EMPTY);

    assertTrue(CustomRuleExecutor.executeRule(rule, pipeline, "/tmp/test.hpl").isEmpty());
  }

  /**
   * Without plugin scoping, a rule aimed at one transform type runs against every transform in the
   * pipeline. That is what made checks like "Table Output must not truncate" unwritable.
   */
  @Test
  public void appliesToRestrictsARuleToTheNamedTransformType() {
    TransformMeta tableOutput = new TransformMeta();
    tableOutput.setName("Write customers");
    tableOutput.setTransformPluginId("TableOutput");

    TransformMeta dummy = new TransformMeta();
    dummy.setName("Placeholder");
    dummy.setTransformPluginId("Dummy");

    CustomLintRule rule = baseRule(RuleTarget.TRANSFORM, "name", RuleCondition.NOT_EMPTY);
    rule.setCondition(RuleCondition.MATCHES_PATTERN);
    rule.setConditionValue("^nothing-matches-this$");
    rule.setAppliesTo(List.of("TableOutput"));

    assertEquals(1, CustomRuleExecutor.executeRule(rule, tableOutput, "/tmp/t.hpl").size());
    assertTrue(
        CustomRuleExecutor.executeRule(rule, dummy, "/tmp/t.hpl").isEmpty(),
        "the rule fired on a transform type it does not apply to");
  }

  @Test
  public void appliesToMatchingIgnoresCase() {
    TransformMeta tableOutput = new TransformMeta();
    tableOutput.setName("Write");
    tableOutput.setTransformPluginId("TableOutput");

    CustomLintRule rule = baseRule(RuleTarget.TRANSFORM, "name", RuleCondition.MATCHES_PATTERN);
    rule.setConditionValue("^nothing-matches-this$");
    rule.setAppliesTo(List.of("tableoutput"));

    assertEquals(1, CustomRuleExecutor.executeRule(rule, tableOutput, "/tmp/t.hpl").size());
  }

  /**
   * A plugin-scoped rule asserts the field exists on the types it names, so a wrong field name is
   * reported instead of passing quietly. Silence here is what made the earlier rules untrustworthy:
   * a typo and a clean result looked identical.
   */
  @Test
  public void scopedRuleReportsAFieldTheTransformDoesNotHave() {
    TransformMeta tableOutput = new TransformMeta();
    tableOutput.setName("Write customers");
    tableOutput.setTransformPluginId("TableOutput");

    CustomLintRule rule = baseRule(RuleTarget.TRANSFORM, "noSuchField", RuleCondition.NOT_EMPTY);
    rule.setId("BAD-001");
    rule.setAppliesTo(List.of("TableOutput"));

    List<LintResult> results = CustomRuleExecutor.executeRule(rule, tableOutput, "/tmp/t.hpl");

    assertEquals(1, results.size());
    assertEquals("ERROR", results.get(0).getSeverity());
    assertTrue(results.get(0).getMessage().contains("noSuchField"));
  }

  /**
   * An unscoped rule runs across every transform, most of which legitimately lack the field, so the
   * same situation stays silent there.
   */
  @Test
  public void unscopedRuleStaysQuietAboutAMissingField() {
    TransformMeta dummy = new TransformMeta();
    dummy.setName("Placeholder");
    dummy.setTransformPluginId("Dummy");

    CustomLintRule rule = baseRule(RuleTarget.TRANSFORM, "noSuchField", RuleCondition.NOT_EMPTY);

    assertTrue(CustomRuleExecutor.executeRule(rule, dummy, "/tmp/t.hpl").isEmpty());
  }

  /** An empty appliesTo keeps the original behaviour, so existing rules are unaffected. */
  @Test
  public void emptyAppliesToRunsAgainstEveryTransform() {
    TransformMeta dummy = new TransformMeta();
    dummy.setName("Placeholder");
    dummy.setTransformPluginId("Dummy");

    CustomLintRule rule = baseRule(RuleTarget.TRANSFORM, "name", RuleCondition.MATCHES_PATTERN);
    rule.setConditionValue("^nothing-matches-this$");

    assertEquals(1, CustomRuleExecutor.executeRule(rule, dummy, "/tmp/t.hpl").size());
  }

  /**
   * A rule the engine cannot evaluate must never look like a rule that found nothing. It is
   * reported against the rule itself so the misconfiguration is visible.
   */
  @Test
  public void unevaluatableConditionIsReportedRatherThanPassing() {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("test");

    CustomLintRule rule = baseRule(RuleTarget.PIPELINE, "transformCount", RuleCondition.MAX_VALUE);
    rule.setId("BROKEN-001");
    rule.setCondition(null); // stands in for a condition this build cannot evaluate

    List<LintResult> results = CustomRuleExecutor.executeRule(rule, pipeline, "/tmp/test.hpl");

    assertEquals(1, results.size());
    assertEquals("ERROR", results.get(0).getSeverity());
    assertTrue(results.get(0).getMessage().contains("Rule execution failed"));
  }
}
