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

package org.apache.hop.lineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.Test;

/**
 * A pipeline's lineage run has to say which run started it. The engine knows either the action that
 * ran it (inside its workflow) or the parent pipeline of a sub-pipeline; the job name it reports is
 * the one the OpenLineage mapper gives that parent, so a collector can draw the hierarchy.
 */
class LineagePipelineParentTest {

  private static LineageContext.Builder context() {
    return LineageContext.builder();
  }

  @Test
  void pipelineRunByAWorkflowActionNamesTheActionAsItsParent() {
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    ActionBase action = mock(ActionBase.class);
    ILogChannel actionLog = mock(ILogChannel.class);
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("nightly-load");
    when(pipeline.getParent()).thenReturn(action);
    when(pipeline.getParentWorkflow()).thenReturn(workflow);
    when(workflow.getWorkflowMeta()).thenReturn(workflowMeta);
    when(action.getName()).thenReturn("Run orders pipeline");
    when(action.getLogChannel()).thenReturn(actionLog);
    when(actionLog.getLogChannelId()).thenReturn("action-run-id");

    LineageContext.Builder ctx = context();
    LineageRunLifecycleEmitter.fillPipelineParentFields(ctx, pipeline);
    LineageContext built = ctx.build();

    assertEquals(
        "nightly-load/Run orders pipeline",
        built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_JOB_NAME));
    assertEquals(
        "nightly-load",
        built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_WORKFLOW_NAME));
    assertEquals(
        "Run orders pipeline",
        built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_ACTION_NAME));
    assertEquals(
        "action-run-id", built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_RUN_ID));
  }

  @Test
  void subPipelineNamesTheParentPipelineAsItsParent() {
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> parentPipeline = mock(IPipelineEngine.class);
    PipelineMeta parentMeta = new PipelineMeta();
    parentMeta.setName("orders-parent");
    when(pipeline.getParentPipeline()).thenReturn(parentPipeline);
    when(parentPipeline.getPipelineMeta()).thenReturn(parentMeta);
    when(parentPipeline.getLogChannelId()).thenReturn("parent-run-id");

    LineageContext.Builder ctx = context();
    LineageRunLifecycleEmitter.fillPipelineParentFields(ctx, pipeline);
    LineageContext built = ctx.build();

    assertEquals(
        "orders-parent",
        built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_JOB_NAME));
    assertEquals(
        "orders-parent",
        built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_PIPELINE_NAME));
    assertEquals(
        "parent-run-id", built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_RUN_ID));
  }

  @Test
  void workflowRunByAWorkflowActionNamesTheActionAsItsParent() {
    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> child = mock(IWorkflowEngine.class);
    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> parent = mock(IWorkflowEngine.class);
    ActionBase action = mock(ActionBase.class);
    ILogChannel actionLog = mock(ILogChannel.class);
    WorkflowMeta parentMeta = new WorkflowMeta();
    parentMeta.setName("nightly-load");
    when(child.getParent()).thenReturn(action);
    when(child.getParentWorkflow()).thenReturn(parent);
    when(parent.getWorkflowMeta()).thenReturn(parentMeta);
    when(parent.getLogChannelId()).thenReturn("parent-workflow-run-id");
    when(action.getName()).thenReturn("Run sub-workflow");
    when(action.getLogChannel()).thenReturn(actionLog);
    when(actionLog.getLogChannelId()).thenReturn("action-run-id");

    LineageContext.Builder ctx = context();
    LineageRunLifecycleEmitter.fillWorkflowParentFields(ctx, child);
    LineageContext built = ctx.build();

    assertEquals(
        "nightly-load/Run sub-workflow",
        built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_JOB_NAME));
    assertEquals(
        "action-run-id", built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_RUN_ID));
  }

  // A Pipeline Executor or mapping runs its pipeline as its logging parent. The parent is that
  // transform's run, <pipeline>/<transform>: the same shape as a pipeline run by a workflow action.
  @Test
  void subPipelineRunByATransformNamesThatTransform() {
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> parentPipeline = mock(IPipelineEngine.class);
    BaseTransform<?, ?> executor = mock(BaseTransform.class);
    ILogChannel executorLog = mock(ILogChannel.class);
    PipelineMeta parentMeta = new PipelineMeta();
    parentMeta.setName("orders-parent");
    when(pipeline.getParentPipeline()).thenReturn(parentPipeline);
    when(pipeline.getParent()).thenReturn(executor);
    when(parentPipeline.getPipelineMeta()).thenReturn(parentMeta);
    when(parentPipeline.getLogChannelId()).thenReturn("parent-run-id");
    when(executor.getTransformName()).thenReturn("Run per order");
    when(executor.getLogChannel()).thenReturn(executorLog);
    when(executorLog.getLogChannelId()).thenReturn("executor-run-id");

    LineageContext.Builder ctx = context();
    LineageRunLifecycleEmitter.fillPipelineParentFields(ctx, pipeline);
    LineageContext built = ctx.build();

    assertEquals(
        "orders-parent/Run per order",
        built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_JOB_NAME));
    assertEquals(
        "executor-run-id",
        built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_RUN_ID));
    assertEquals(
        "orders-parent",
        built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_PIPELINE_NAME));
    assertEquals(
        "Run per order",
        built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_TRANSFORM_NAME));
  }

  // A Workflow Executor is the logging parent of the workflow it runs: same shape.
  @Test
  void workflowRunByATransformNamesThatTransform() {
    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> parentPipeline = mock(IPipelineEngine.class);
    BaseTransform<?, ?> executor = mock(BaseTransform.class);
    ILogChannel executorLog = mock(ILogChannel.class);
    PipelineMeta parentMeta = new PipelineMeta();
    parentMeta.setName("orders-parent");
    when(workflow.getParentPipeline()).thenReturn(parentPipeline);
    when(workflow.getParent()).thenReturn(executor);
    when(parentPipeline.getPipelineMeta()).thenReturn(parentMeta);
    when(executor.getTransformName()).thenReturn("Run workflow per order");
    when(executor.getLogChannel()).thenReturn(executorLog);
    when(executorLog.getLogChannelId()).thenReturn("executor-run-id");

    LineageContext.Builder ctx = context();
    LineageRunLifecycleEmitter.fillWorkflowParentFields(ctx, workflow);
    LineageContext built = ctx.build();

    assertEquals(
        "orders-parent/Run workflow per order",
        built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_JOB_NAME));
    assertEquals(
        "executor-run-id",
        built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_RUN_ID));
  }

  // Only a top-level run flushes the lineage hub when it completes.
  @Test
  void onlyRunsWithoutAParentAreTopLevel() {
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> topLevel = mock(IPipelineEngine.class);
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> subPipeline = mock(IPipelineEngine.class);
    when(subPipeline.getParentPipeline()).thenReturn(topLevel);
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> runByAction = mock(IPipelineEngine.class);
    when(runByAction.getParent()).thenReturn(mock(ActionBase.class));
    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> childWorkflow = mock(IWorkflowEngine.class);
    when(childWorkflow.getParentWorkflow()).thenReturn(workflow);

    assertTrue(LineageRunLifecycleEmitter.isTopLevel(topLevel));
    assertFalse(LineageRunLifecycleEmitter.isTopLevel(subPipeline));
    assertFalse(LineageRunLifecycleEmitter.isTopLevel(runByAction));
    assertTrue(LineageRunLifecycleEmitter.isTopLevel(workflow));
    assertFalse(LineageRunLifecycleEmitter.isTopLevel(childWorkflow));
  }

  @Test
  void topLevelPipelineHasNoParentFields() {
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);

    LineageContext.Builder ctx = context();
    LineageRunLifecycleEmitter.fillPipelineParentFields(ctx, pipeline);
    LineageContext built = ctx.build();

    assertNull(built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_JOB_NAME));
    assertNull(built.getAttributes().get(LineageRunLifecycleEmitter.ATTR_PARENT_RUN_ID));
  }
}
