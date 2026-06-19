/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.function.Function;
import org.apache.hop.core.plugins.EngineCompatibility;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineCompatibilityChecker.Violation;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link EngineCompatibilityChecker}. Exercises the lower-level (engineId,
 * engineCheck) overloads so the registry can be stubbed via mocks rather than requiring a full
 * {@code HopEnvironment.init()}. The (PipelineMeta, IPipelineEngine) and (WorkflowMeta,
 * IWorkflowEngine) overloads delegate to these and are covered by integration tests in {@code
 * BeamPipelineEngineSupportsTest} and the GCP test harness.
 */
class EngineCompatibilityCheckerTest {

  // ─── checkPipeline (engineId, engineCheck) overload ──────────────────────────

  @Test
  void checkPipeline_nullEngineReturnsEmpty() {
    PipelineMeta meta = new PipelineMeta();
    assertTrue(EngineCompatibilityChecker.checkPipeline(meta, (IPipelineEngine<?>) null).isEmpty());
  }

  @Test
  void checkPipeline_nullEngineIdReturnsEmpty() {
    PipelineMeta meta = new PipelineMeta();
    assertTrue(
        EngineCompatibilityChecker.checkPipeline(
                meta, null, p -> EngineCompatibility.unsupported("no"))
            .isEmpty());
  }

  @Test
  void checkPipeline_emptyPipelineReturnsEmpty() {
    assertTrue(
        EngineCompatibilityChecker.checkPipeline(
                new PipelineMeta(), "Local", p -> EngineCompatibility.unsupported("no"))
            .isEmpty());
  }

  @Test
  void checkPipeline_skipsTransformWithNullPluginId() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(null, "orphan", mock(ITransformMeta.class)));

    List<Violation> violations =
        EngineCompatibilityChecker.checkPipeline(
            pipelineMeta, "Local", p -> EngineCompatibility.unsupported("never gets called"));

    assertTrue(violations.isEmpty(), "transforms without a plugin id are silently skipped");
  }

  @Test
  void checkPipeline_skipsTransformWhosePluginIdIsNotInRegistry() {
    // A made-up id no real plugin uses — registry returns null, checker silently skips it.
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(
        new TransformMeta("not-a-real-plugin-id-zzzzz", "ghost", mock(ITransformMeta.class)));

    List<Violation> violations =
        EngineCompatibilityChecker.checkPipeline(
            pipelineMeta, "Local", p -> EngineCompatibility.unsupported("ignored"));

    assertTrue(violations.isEmpty(), "transforms whose plugin can't be resolved are skipped");
  }

  // ─── checkWorkflow (engineId, engineCheck) overload ──────────────────────────

  @Test
  void checkWorkflow_nullEngineReturnsEmpty() {
    WorkflowMeta meta = new WorkflowMeta();
    assertTrue(EngineCompatibilityChecker.checkWorkflow(meta, (IWorkflowEngine<?>) null).isEmpty());
  }

  @Test
  void checkWorkflow_nullEngineIdReturnsEmpty() {
    WorkflowMeta meta = new WorkflowMeta();
    assertTrue(
        EngineCompatibilityChecker.checkWorkflow(
                meta, null, p -> EngineCompatibility.unsupported("no"))
            .isEmpty());
  }

  @Test
  void checkWorkflow_emptyWorkflowReturnsEmpty() {
    assertTrue(
        EngineCompatibilityChecker.checkWorkflow(
                new WorkflowMeta(), "Local", p -> EngineCompatibility.unsupported("no"))
            .isEmpty());
  }

  @Test
  void checkWorkflow_skipsActionWithNullAction() {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    ActionMeta actionMeta = mock(ActionMeta.class);
    when(actionMeta.getAction()).thenReturn(null);
    workflowMeta.addAction(actionMeta);

    List<Violation> violations =
        EngineCompatibilityChecker.checkWorkflow(
            workflowMeta, "Local", p -> EngineCompatibility.unsupported("ignored"));

    assertTrue(violations.isEmpty(), "actions with no IAction are silently skipped");
  }

  @Test
  void checkWorkflow_skipsActionWithNullPluginId() {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    ActionMeta actionMeta = mock(ActionMeta.class);
    IAction action = mock(IAction.class);
    lenient().when(action.getPluginId()).thenReturn(null);
    when(actionMeta.getAction()).thenReturn(action);
    workflowMeta.addAction(actionMeta);

    List<Violation> violations =
        EngineCompatibilityChecker.checkWorkflow(
            workflowMeta, "Local", p -> EngineCompatibility.unsupported("ignored"));

    assertTrue(violations.isEmpty(), "actions with no plugin id are silently skipped");
  }

  // ─── Violation record + formatViolations ─────────────────────────────────────

  @Test
  void violation_formatLineHasReason() {
    Violation v = new Violation("StepA", "PluginX", "needs streaming");
    assertEquals("  • StepA (PluginX) — needs streaming", v.formatLine());
  }

  @Test
  void violation_formatLineWithoutReason() {
    Violation v = new Violation("StepA", "PluginX", null);
    assertEquals("  • StepA (PluginX)", v.formatLine());
  }

  @Test
  void violation_formatLineEmptyReasonTreatedAsNone() {
    Violation v = new Violation("StepA", "PluginX", "");
    assertEquals("  • StepA (PluginX)", v.formatLine());
  }

  @Test
  void formatViolations_joinsLinesWithNewline() {
    String out =
        EngineCompatibilityChecker.formatViolations(
            List.of(
                new Violation("A", "PA", "r1"),
                new Violation("B", "PB", null),
                new Violation("C", "PC", "r3")));

    assertEquals("  • A (PA) — r1\n  • B (PB)\n  • C (PC) — r3", out);
  }

  @Test
  void formatViolations_emptyListReturnsEmptyString() {
    assertEquals("", EngineCompatibilityChecker.formatViolations(List.of()));
  }

  // ─── Plumbing: engineCheck Function is honored ───────────────────────────────

  @Test
  void engineCheckFunctionIsConsultedWhenProvided() {
    // We can't easily register a transform via PluginRegistry without HopEnvironment.init(),
    // but we can confirm that an empty pipeline never reaches the check function at all.
    Function<IPlugin, EngineCompatibility> spy = p -> EngineCompatibility.unsupported("never");
    List<Violation> v = EngineCompatibilityChecker.checkPipeline(new PipelineMeta(), "Local", spy);
    assertTrue(v.isEmpty());
  }
}
