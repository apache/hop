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

import java.util.List;
import java.util.Set;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Every field the rule editor and {@code hop lint --list-fields} offer for a connection, transform
 * or action resolves.
 *
 * <p>{@code databaseType}, {@code attributes}, {@code errorHandling} and others were offered but
 * never read, so a rule written against one of them never fired and said nothing about why.
 *
 * @see <a href="https://github.com/apache/hop/issues/8815">#8815</a>
 */
public class OfferedFieldsTest {

  /**
   * Offered for transforms and actions, but read from the plugin's own field of that name, which
   * most plugins do not have.
   */
  private static final Set<String> PLUGIN_OWN_FIELDS = Set.of("apiKey", "token");

  @AfterEach
  public void clearSubject() {
    CustomRuleExecutor.setSubject(null);
  }

  /** Can send its errors to another transform. */
  public static class ErrorHandlingMeta extends BaseTransformMeta {
    @Override
    public boolean supportsErrorHandling() {
      return true;
    }
  }

  private static CustomLintRule rule(RuleTarget target, String field, RuleCondition condition) {
    CustomLintRule rule = new CustomLintRule();
    rule.setId("TEST-001");
    rule.setEnabled(true);
    rule.setSeverity("WARNING");
    rule.setTarget(target);
    rule.setTargetField(field);
    rule.setCondition(condition);
    rule.setName("Test rule");
    return rule;
  }

  private static DatabaseMeta connection() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("CRM");
    return databaseMeta;
  }

  private static List<LintResult> lint(CustomLintRule rule, Object hopObject) {
    return CustomRuleExecutor.executeRule(rule, hopObject, "/tmp/test");
  }

  private static void assertResolves(RuleTarget target, Object hopObject, String pluginId) {
    for (String field : RuleTargetFields.getFieldsForTarget(target)) {
      if (PLUGIN_OWN_FIELDS.contains(field)) {
        continue;
      }
      CustomLintRule rule = rule(target, field, RuleCondition.NOT_NULL);
      if (pluginId != null) {
        // Scoped, so that a field which does not resolve is reported rather than skipped.
        rule.setAppliesTo(List.of(pluginId));
      }
      for (LintResult result : lint(rule, hopObject)) {
        assertFalse(
            result.getMessage().contains("does not exist"),
            target + "." + field + ": " + result.getMessage());
      }
    }
  }

  // ------------------------------------------------------------------ every offered field

  @Test
  public void everyConnectionFieldResolves() {
    assertResolves(RuleTarget.DATABASE_CONNECTION, connection(), null);
  }

  @Test
  public void everyTransformFieldResolves() {
    PipelineMeta pipeline = new PipelineMeta();
    TransformMeta transformMeta = new TransformMeta("Fake", "read", new ErrorHandlingMeta());
    pipeline.addTransform(transformMeta);
    CustomRuleExecutor.setSubject(pipeline);

    assertResolves(RuleTarget.TRANSFORM, transformMeta, "Fake");
  }

  @Test
  public void everyActionFieldResolves() {
    WorkflowMeta workflow = new WorkflowMeta();
    ActionMeta actionMeta = new ActionMeta(new ActionDummy("check"));
    workflow.addAction(actionMeta);
    CustomRuleExecutor.setSubject(workflow);

    assertResolves(RuleTarget.ACTION, actionMeta, actionMeta.getAction().getPluginId());
  }

  // ------------------------------------------------------------------ connections

  /** Every connection has a type, so IS_EMPTY reports each one. It used to report none. */
  @Test
  public void databaseTypeIsThePluginId() {
    DatabaseMeta databaseMeta = connection();

    List<LintResult> results =
        lint(
            rule(RuleTarget.DATABASE_CONNECTION, "databaseType", RuleCondition.IS_EMPTY),
            databaseMeta);

    assertEquals(1, results.size());
    assertTrue(
        results.get(0).getMessage().contains(databaseMeta.getPluginId()),
        results.get(0).getMessage());
  }

  @Test
  public void attributesIncludeTheExtraOptionsButNotTheirValues() {
    DatabaseMeta databaseMeta = connection();
    databaseMeta.getAttributes().clear();
    databaseMeta.addExtraOption("POSTGRESQL", "sslpassword", "hunter2");

    CustomLintRule rule =
        rule(RuleTarget.DATABASE_CONNECTION, "attributes", RuleCondition.MAX_COLLECTION_SIZE);
    rule.setConditionValue("0");
    List<LintResult> results = lint(rule, databaseMeta);

    assertEquals(1, results.size());
    String message = results.get(0).getMessage();
    assertTrue(message.contains("EXTRA_OPTION_POSTGRESQL.sslpassword"), message);
    assertFalse(message.contains("hunter2"), message);
  }

  @Test
  public void noAttributesIsAnEmptyCollection() {
    DatabaseMeta databaseMeta = connection();
    databaseMeta.getAttributes().clear();

    assertEquals(
        1,
        lint(
                rule(
                    RuleTarget.DATABASE_CONNECTION,
                    "attributes",
                    RuleCondition.NOT_EMPTY_COLLECTION),
                databaseMeta)
            .size());
  }

  /** A connection property the linter has no case for is read by getter. */
  @Test
  public void otherConnectionPropertiesResolve() {
    DatabaseMeta databaseMeta = connection();
    databaseMeta.setManualUrl("jdbc:postgresql://db/crm");

    CustomLintRule rule =
        rule(RuleTarget.DATABASE_CONNECTION, "manualUrl", RuleCondition.NOT_CONTAINS);
    rule.setConditionValue("postgresql");

    assertEquals(1, lint(rule, databaseMeta).size());
  }

  /** Every connection has the same fields, so a name that matches none of them is a typo. */
  @Test
  public void anUnknownConnectionFieldIsReported() {
    List<LintResult> results =
        lint(
            rule(RuleTarget.DATABASE_CONNECTION, "hostnmae", RuleCondition.NOT_EMPTY),
            connection());

    assertEquals(1, results.size());
    assertEquals("ERROR", results.get(0).getSeverity());
    assertTrue(
        results.get(0).getMessage().contains("connection 'CRM'"), results.get(0).getMessage());
  }

  // ------------------------------------------------------------------ transforms

  private static PipelineMeta pipelineWithErrorHop(boolean errorHandlingEnabled) {
    PipelineMeta pipeline = new PipelineMeta();
    TransformMeta read = new TransformMeta("Fake", "read", new ErrorHandlingMeta());
    TransformMeta write = new TransformMeta("Dummy", "write", null);
    TransformMeta errors = new TransformMeta("Dummy", "errors", null);
    pipeline.addTransform(read);
    pipeline.addTransform(write);
    pipeline.addTransform(errors);
    pipeline.addPipelineHop(new PipelineHopMeta(read, write));
    pipeline.addPipelineHop(new PipelineHopMeta(read, errors));
    TransformErrorMeta errorMeta = new TransformErrorMeta(read, errors);
    errorMeta.setEnabled(errorHandlingEnabled);
    read.setTransformErrorMeta(errorMeta);
    CustomRuleExecutor.setSubject(pipeline);
    return pipeline;
  }

  @Test
  public void transformErrorHandling() {
    CustomLintRule rule = rule(RuleTarget.TRANSFORM, "errorHandling", RuleCondition.MUST_BE_TRUE);

    assertTrue(lint(rule, pipelineWithErrorHop(true).findTransform("read")).isEmpty());
    assertEquals(1, lint(rule, pipelineWithErrorHop(false).findTransform("read")).size());
    // A transform that cannot handle errors does not, rather than failing the rule.
    assertEquals(1, lint(rule, pipelineWithErrorHop(true).findTransform("write")).size());
  }

  @Test
  public void targetTransformsAreTheNextTransforms() {
    PipelineMeta pipeline = pipelineWithErrorHop(true);
    CustomLintRule rule =
        rule(RuleTarget.TRANSFORM, "targetTransforms", RuleCondition.MAX_COLLECTION_SIZE);
    rule.setConditionValue("1");

    List<LintResult> results = lint(rule, pipeline.findTransform("read"));
    assertEquals(1, results.size());
    assertTrue(
        results.get(0).getMessage().contains("[write, errors]"), results.get(0).getMessage());
    assertTrue(lint(rule, pipeline.findTransform("write")).isEmpty());
  }

  @Test
  public void distributes() {
    TransformMeta transformMeta = new TransformMeta("Dummy", "copy", null);
    transformMeta.setDistributes(false);

    assertEquals(
        1,
        lint(rule(RuleTarget.TRANSFORM, "distributes", RuleCondition.MUST_BE_TRUE), transformMeta)
            .size());
  }

  // ------------------------------------------------------------------ actions

  private static WorkflowMeta workflow(boolean failureHop) {
    WorkflowMeta workflow = new WorkflowMeta();
    ActionMeta check = new ActionMeta(new ActionDummy("check"));
    ActionMeta load = new ActionMeta(new ActionDummy("load"));
    ActionMeta alert = new ActionMeta(new ActionDummy("alert"));
    workflow.addAction(check);
    workflow.addAction(load);
    workflow.addAction(alert);
    workflow.addWorkflowHop(new WorkflowHopMeta(check, load));
    if (failureHop) {
      WorkflowHopMeta onFailure = new WorkflowHopMeta(check, alert);
      onFailure.setEvaluation(false);
      workflow.addWorkflowHop(onFailure);
    }
    CustomRuleExecutor.setSubject(workflow);
    return workflow;
  }

  /** An action handles its errors with a hop followed on failure. */
  @Test
  public void actionErrorHandling() {
    CustomLintRule rule = rule(RuleTarget.ACTION, "errorHandling", RuleCondition.MUST_BE_TRUE);

    assertTrue(lint(rule, workflow(true).findAction("check")).isEmpty());
    assertEquals(1, lint(rule, workflow(false).findAction("check")).size());
  }

  @Test
  public void targetActionsAreTheNextActions() {
    CustomLintRule rule =
        rule(RuleTarget.ACTION, "targetActions", RuleCondition.MAX_COLLECTION_SIZE);
    rule.setConditionValue("1");

    List<LintResult> results = lint(rule, workflow(true).findAction("check"));
    assertEquals(1, results.size());
    assertTrue(results.get(0).getMessage().contains("[load, alert]"), results.get(0).getMessage());
    assertTrue(lint(rule, workflow(false).findAction("check")).isEmpty());
  }
}
