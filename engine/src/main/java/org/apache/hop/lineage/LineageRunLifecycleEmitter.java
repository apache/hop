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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineagePortableFilename;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload.Transition;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.workflow.WorkflowExecutionExtension;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * Builds {@link LineageEvent}s for pipeline and workflow run lifecycle and emits them to {@link
 * LineageHub}.
 */
public final class LineageRunLifecycleEmitter {

  public static final String OUTCOME = "outcome";
  public static final String FAILED = "failed";
  public static final String SUCCESS = "success";

  /**
   * The run that started this pipeline or workflow, when the engine knows it: the workflow action,
   * or the transform of a parent pipeline (Pipeline Executor, mapping), that ran it. {@code
   * parentRunId} is that subject's log-channel id and {@code parentJobName} is the job name the
   * lineage mapper gives it (e.g. {@code my-workflow/Run orders} or {@code my-pipeline/Run per
   * row}); both are needed to form a ParentRunFacet. Sinks that write the job tree themselves
   * (DataHub) read the same pair.
   */
  public static final String ATTR_PARENT_RUN_ID = "parentRunId";

  public static final String ATTR_PARENT_JOB_NAME = "parentJobName";

  /**
   * The parts {@link #ATTR_PARENT_JOB_NAME} is made of, for a sink that models workflows and
   * pipelines apart from the jobs inside them (DataHub): the workflow and action that ran this run,
   * or the pipeline and transform (Pipeline Executor, mapping) that did. A job name cannot be split
   * back reliably, since names may contain the separator.
   */
  public static final String ATTR_PARENT_WORKFLOW_NAME = "parentWorkflowName";

  public static final String ATTR_PARENT_ACTION_NAME = "parentActionName";
  public static final String ATTR_PARENT_PIPELINE_NAME = "parentPipelineName";
  public static final String ATTR_PARENT_TRANSFORM_NAME = "parentTransformName";

  /**
   * The file of the workflow or pipeline named by {@link #ATTR_PARENT_WORKFLOW_NAME} or {@link
   * #ATTR_PARENT_PIPELINE_NAME}, in its portable form ({@code ${PROJECT_HOME}/...}) when it lies in
   * the project. Names are not unique across files; a sink that identifies flows by file needs it
   * to find the parent.
   */
  public static final String ATTR_PARENT_FILENAME = "parentFilename";

  /** The description the subject's metadata gives it (pipeline, workflow, transform, action). */
  public static final String ATTR_DESCRIPTION = "description";

  private static void fillDescription(LineageContext.Builder ctx, String description) {
    if (!Utils.isEmpty(description)) {
      ctx.putAttribute(ATTR_DESCRIPTION, description);
    }
  }

  private LineageRunLifecycleEmitter() {}

  /**
   * Records who started this run, when the engine knows: an action that ran a pipeline or workflow,
   * or a parent pipeline that ran a sub-pipeline. Left out entirely for a top-level run.
   */
  private static void fillParentFields(
      LineageContext.Builder ctx, String parentJobName, String parentRunId) {
    if (!Utils.isEmpty(parentJobName)) {
      ctx.putAttribute(ATTR_PARENT_JOB_NAME, parentJobName);
    }
    if (!Utils.isEmpty(parentRunId)) {
      ctx.putAttribute(ATTR_PARENT_RUN_ID, parentRunId);
    }
  }

  /**
   * The run that started a pipeline: its sub-pipeline parent, or the workflow action that ran it
   * (with the workflow that owns the action). Package-private so the naming rule can be checked
   * without starting an engine.
   */
  static void fillPipelineParentFields(
      LineageContext.Builder ctx, IPipelineEngine<PipelineMeta> pipeline) {
    IPipelineEngine<PipelineMeta> parentPipeline = pipeline.getParentPipeline();
    if (parentPipeline != null) {
      fillPipelineParentFields(ctx, parentPipeline, pipeline.getParent());
      return;
    }
    if (pipeline.getParent() instanceof IAction parentAction) {
      fillActionParentFields(ctx, parentAction, pipeline.getParentWorkflow());
    }
  }

  /**
   * A run started from a pipeline. When the transform that started it (Pipeline Executor, mapping,
   * Workflow Executor) is its logging parent, the parent is that transform's run, named {@code
   * <pipeline>/<transform>}: the same shape as a run started by a workflow action. Only when the
   * transform is unknown does the parent pipeline's own run stand in.
   */
  private static void fillPipelineParentFields(
      LineageContext.Builder ctx, IPipelineEngine<PipelineMeta> parentPipeline, Object parent) {
    PipelineMeta parentMeta = parentPipeline.getPipelineMeta();
    String parentPipelineName = parentMeta != null ? parentMeta.getName() : null;
    if (Utils.isEmpty(parentPipelineName)) {
      fillParentFields(ctx, null, parentPipeline.getLogChannelId());
      return;
    }
    ctx.putAttribute(ATTR_PARENT_PIPELINE_NAME, parentPipelineName);
    // The same file the parent's own events report (see emitPipeline).
    String parentFilename = parentPipeline.getFilename();
    fillParentFilename(
        ctx,
        Utils.isEmpty(parentFilename) ? parentMeta.getFilename() : parentFilename,
        parentPipeline);
    if (parent instanceof ITransform transform && !Utils.isEmpty(transform.getTransformName())) {
      ctx.putAttribute(ATTR_PARENT_TRANSFORM_NAME, transform.getTransformName());
      ILogChannel transformLog = transform.getLogChannel();
      String transformRunId = transformLog != null ? transformLog.getLogChannelId() : null;
      if (!Utils.isEmpty(transformRunId)) {
        fillParentFields(
            ctx, parentPipelineName + "/" + transform.getTransformName(), transformRunId);
        return;
      }
    }
    fillParentFields(ctx, parentPipelineName, parentPipeline.getLogChannelId());
  }

  /**
   * The run that started a workflow: the transform that ran it (Workflow Executor), the workflow
   * action that ran it (with the workflow that owns the action), or failing that the parent
   * workflow itself.
   */
  static void fillWorkflowParentFields(
      LineageContext.Builder ctx, IWorkflowEngine<WorkflowMeta> workflow) {
    IPipelineEngine<PipelineMeta> parentPipeline = workflow.getParentPipeline();
    if (parentPipeline != null) {
      fillPipelineParentFields(ctx, parentPipeline, workflow.getParent());
      return;
    }
    IWorkflowEngine<WorkflowMeta> parentWorkflow = workflow.getParentWorkflow();
    if (workflow.getParent() instanceof IAction parentAction) {
      fillActionParentFields(ctx, parentAction, parentWorkflow);
      return;
    }
    WorkflowMeta parentMeta = parentWorkflow != null ? parentWorkflow.getWorkflowMeta() : null;
    if (parentMeta != null) {
      fillParentFields(ctx, parentMeta.getName(), parentWorkflow.getLogChannelId());
    }
  }

  /**
   * Whether a pipeline is a top-level run: not a sub-pipeline, not started by a workflow action.
   * Only a top-level run always waits for the lineage hub to drain when it completes. A nested run
   * (a Pipeline Executor row, a mapping, a Pipeline action) waits only once the queue is filling up
   * ({@link LineageHub#flushIfBacklogged()}): a full drain after every nested run would sit on the
   * path of every caller waiting for it, and no drain at all would let the queue overflow.
   */
  public static boolean isTopLevel(IPipelineEngine<PipelineMeta> pipeline) {
    return pipeline != null
        && pipeline.getParentPipeline() == null
        && pipeline.getParentWorkflow() == null
        && !(pipeline.getParent() instanceof IAction);
  }

  /** Whether a workflow is a top-level run; see {@link #isTopLevel(IPipelineEngine)}. */
  public static boolean isTopLevel(IWorkflowEngine<WorkflowMeta> workflow) {
    return workflow != null
        && workflow.getParentPipeline() == null
        && workflow.getParentWorkflow() == null
        && !(workflow.getParent() instanceof IAction);
  }

  /** An action is named {@code <workflow>/<action>}, the convention the lineage mapper uses. */
  private static void fillActionParentFields(
      LineageContext.Builder ctx,
      IAction parentAction,
      IWorkflowEngine<WorkflowMeta> parentWorkflow) {
    WorkflowMeta parentMeta = parentWorkflow != null ? parentWorkflow.getWorkflowMeta() : null;
    String parentWorkflowName = parentMeta != null ? parentMeta.getName() : null;
    String parentJobName =
        !Utils.isEmpty(parentWorkflowName) && !Utils.isEmpty(parentAction.getName())
            ? parentWorkflowName + "/" + parentAction.getName()
            : null;
    ILogChannel parentLog = parentAction.getLogChannel();
    fillParentFields(ctx, parentJobName, parentLog != null ? parentLog.getLogChannelId() : null);
    if (parentJobName != null) {
      ctx.putAttribute(ATTR_PARENT_WORKFLOW_NAME, parentWorkflowName);
      ctx.putAttribute(ATTR_PARENT_ACTION_NAME, parentAction.getName());
      fillParentFilename(ctx, parentMeta.getFilename(), parentWorkflow);
    }
  }

  private static void fillParentFilename(
      LineageContext.Builder ctx, String filename, IVariables variables) {
    if (Utils.isEmpty(filename)) {
      return;
    }
    String portable = LineagePortableFilename.portableKey(filename, variables);
    ctx.putAttribute(ATTR_PARENT_FILENAME, Utils.isEmpty(portable) ? filename : portable);
  }

  private static void fillFilenameFields(
      LineageContext.Builder ctx, String filename, IVariables variables) {
    ctx.hopFilename(filename);
    ctx.hopFilenamePortableKey(LineagePortableFilename.portableKey(filename, variables));
  }

  /**
   * Correlation context for a running transform instance. Used for lifecycle, transform-schema, and
   * file I/O lineage events.
   */
  public static LineageContext.Builder transformContextBuilder(ITransform transform) {
    if (transform == null) {
      return null;
    }
    IPipelineEngine<PipelineMeta> pipeline = transform.getPipeline();
    PipelineMeta meta = pipeline != null ? pipeline.getPipelineMeta() : null;
    String pipelineName = meta != null ? meta.getName() : null;
    String filename = pipeline != null ? pipeline.getFilename() : null;
    if (Utils.isEmpty(filename) && meta != null) {
      filename = meta.getFilename();
    }
    IVariables filenameVars = pipeline != null ? pipeline : transform;
    LineageContext.Builder ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.TRANSFORM)
            .logChannelId(transform.getLogChannel().getLogChannelId())
            .transformName(transform.getTransformName())
            .copyNr(String.valueOf(transform.getCopy()))
            .pipelineName(pipelineName);
    fillFilenameFields(ctx, filename, filenameVars);
    if (pipeline != null) {
      ctx.putAttribute("pipelineLogChannelId", pipeline.getLogChannelId());
      String parentId = pipeline.getVariable(Const.INTERNAL_VARIABLE_PIPELINE_PARENT_ID, null);
      if (!Utils.isEmpty(parentId)) {
        ctx.putAttribute("parentPipelineLogChannelId", parentId);
      }
    }
    return ctx;
  }

  /**
   * Correlation context for a workflow action instance that is currently executing — used for the
   * file and HTTP I/O events emitted from inside {@link IAction#execute}.
   *
   * <p>The log channel id is the action instance's own channel, not the workflow's: the workflow
   * clones the action and hands the clone a fresh log channel per execution, so this is the id that
   * correlates an I/O event with the action's own lifecycle events and log lines. The workflow's
   * channel remains available as the {@code workflowLogChannelId} attribute. Falls back to the
   * workflow's channel only when the action has no channel of its own.
   */
  public static LineageContext.Builder actionContextBuilder(
      IWorkflowEngine<WorkflowMeta> workflow, IAction action) {
    if (workflow == null || action == null) {
      return null;
    }
    WorkflowMeta meta = workflow.getWorkflowMeta();
    String workflowName = meta != null ? meta.getName() : null;
    String filename = meta != null ? meta.getFilename() : null;

    ILogChannel actionLog = action.getLogChannel();
    String actionLogChannelId = actionLog != null ? actionLog.getLogChannelId() : null;

    LineageContext.Builder ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.ACTION)
            .logChannelId(
                !Utils.isEmpty(actionLogChannelId)
                    ? actionLogChannelId
                    : workflow.getLogChannelId())
            .workflowName(workflowName)
            .actionName(action.getName());
    if (!Utils.isEmpty(filename)) {
      fillFilenameFields(ctx, filename, workflow);
    }
    ctx.putAttribute("workflowLogChannelId", workflow.getLogChannelId());
    if (!Utils.isEmpty(action.getPluginId())) {
      ctx.putAttribute("actionPluginId", action.getPluginId());
    }
    fillDescription(ctx, action.getDescription());
    return ctx;
  }

  static LineageContext.Builder transformContextBuilder(TransformMetaDataCombi combi) {
    if (combi == null || combi.transform == null) {
      return null;
    }
    LineageContext.Builder ctx = transformContextBuilder(combi.transform);
    if (!Utils.isEmpty(combi.transformName)) {
      ctx.transformName(combi.transformName);
    }
    ctx.copyNr(String.valueOf(combi.copy));
    if (combi.transformMeta != null) {
      fillDescription(ctx, combi.transformMeta.getDescription());
    }
    return ctx;
  }

  public static void emitPipeline(
      IPipelineEngine<PipelineMeta> pipeline, RunLifecyclePhase phase, String detail) {
    PipelineMeta meta = pipeline.getPipelineMeta();
    String name = meta != null ? meta.getName() : null;
    String filename = pipeline.getFilename();
    if (Utils.isEmpty(filename) && meta != null) {
      filename = meta.getFilename();
    }
    LineageContext.Builder ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .logChannelId(pipeline.getLogChannelId())
            .pipelineName(name);
    fillFilenameFields(ctx, filename, pipeline);
    String pluginId = pipeline.getPluginId();
    if (!Utils.isEmpty(pluginId)) {
      ctx.putAttribute("pipelineEnginePluginId", pluginId);
    }
    // A pipeline run by a workflow action knows the action (its logging parent) and the workflow
    // that owns it; a sub-pipeline knows its parent pipeline and the transform that ran it. Either
    // way the parent is that action's or transform's run, named <owner>/<job> as the mapper does.
    fillPipelineParentFields(ctx, pipeline);
    if (meta != null) {
      fillDescription(ctx, meta.getDescription());
    }
    ctx.putAttribute("errors", String.valueOf(pipeline.getErrors()));
    if (phase == RunLifecyclePhase.FINISHED || phase == RunLifecyclePhase.FAILED) {
      ctx.putAttribute(OUTCOME, phase == RunLifecyclePhase.FAILED ? FAILED : SUCCESS);
      LineageRowCounts.putPipelineTotals(ctx, pipeline);
    }
    LineageContext context = ctx.build();
    LineageHub.getInstance()
        .emit(
            LineageEvent.of(
                LineageEventKind.RUN_LIFECYCLE,
                context,
                new RunLifecycleLineagePayload(phase, detail)));

    // The hop graph is a fact about the pipeline, not about a phase: emit it once, next to the
    // STARTED event. Sinks that attribute I/O to individual transforms need it to know which
    // transform fed which; see PipelineGraphLineagePayload. A pipeline without hops gets an empty
    // graph: without one a sink cannot tell it from an engine that sends no graph at all.
    //
    if (phase == RunLifecyclePhase.STARTED && meta != null) {
      LineageHub.getInstance()
          .emit(
              LineageEvent.of(
                  LineageEventKind.PIPELINE_GRAPH,
                  context,
                  new PipelineGraphLineagePayload(pipelineTransitions(meta))));
    }
  }

  /**
   * The enabled hops of a pipeline, in pipeline order, as {@code from -> to} transitions.
   *
   * <p>Hops with an unknown end are skipped (a half-edited hop can exist in memory), as are hops
   * without names. Informational hops stay in: data flows through them, even though they do no I/O
   * of their own, so dropping them would break a chain for anything that propagates datasets along
   * it.
   */
  public static List<Transition> pipelineTransitions(PipelineMeta meta) {
    if (meta == null) {
      return List.of();
    }
    List<Transition> transitions = new ArrayList<>();
    for (PipelineHopMeta hop : meta.getHops()) {
      if (hop == null
          || !hop.isEnabled()
          || hop.getFromTransform() == null
          || hop.getToTransform() == null) {
        continue;
      }
      String from = hop.getFromTransform().getName();
      String to = hop.getToTransform().getName();
      if (!Utils.isEmpty(from) && !Utils.isEmpty(to)) {
        transitions.add(new Transition(from, to));
      }
    }
    return transitions;
  }

  public static void emitWorkflow(
      IWorkflowEngine<WorkflowMeta> workflow, RunLifecyclePhase phase, String detail) {
    WorkflowMeta meta = workflow.getWorkflowMeta();
    String name = meta != null ? meta.getName() : null;
    String filename = meta != null ? meta.getFilename() : null;
    LineageContext.Builder ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.WORKFLOW)
            .logChannelId(workflow.getLogChannelId())
            .workflowName(name);
    fillFilenameFields(ctx, filename, workflow);
    fillWorkflowParentFields(ctx, workflow);
    if (meta != null) {
      fillDescription(ctx, meta.getDescription());
    }
    Result result = workflow.getResult();
    ctx.putAttribute("errors", String.valueOf(result != null ? result.getNrErrors() : 0));
    if (result != null) {
      ctx.putAttribute("nrErrors", String.valueOf(result.getNrErrors()));
      ctx.putAttribute("result", String.valueOf(result.getResult()));
    }
    if (phase == RunLifecyclePhase.FINISHED || phase == RunLifecyclePhase.FAILED) {
      ctx.putAttribute(OUTCOME, phase == RunLifecyclePhase.FAILED ? FAILED : SUCCESS);
    }
    LineageContext context = ctx.build();
    LineageHub.getInstance()
        .emit(
            LineageEvent.of(
                LineageEventKind.RUN_LIFECYCLE,
                context,
                new RunLifecycleLineagePayload(phase, detail)));

    // Like a pipeline's, the hop graph is a fact about the workflow: emitted once, next to STARTED,
    // and empty when the workflow has no hops.
    if (phase == RunLifecyclePhase.STARTED && meta != null) {
      LineageHub.getInstance()
          .emit(
              LineageEvent.of(
                  LineageEventKind.WORKFLOW_GRAPH,
                  context,
                  new PipelineGraphLineagePayload(workflowTransitions(meta))));
    }
  }

  /**
   * The enabled hops of a workflow, in workflow order, as {@code from -> to} transitions between
   * action names. Every hop counts, whatever its condition: each is a way the workflow can go.
   */
  public static List<Transition> workflowTransitions(WorkflowMeta meta) {
    if (meta == null) {
      return List.of();
    }
    List<Transition> transitions = new ArrayList<>();
    for (int i = 0; i < meta.nrWorkflowHops(); i++) {
      WorkflowHopMeta hop = meta.getWorkflowHop(i);
      if (hop == null
          || !hop.isEnabled()
          || hop.getFromAction() == null
          || hop.getToAction() == null) {
        continue;
      }
      String from = hop.getFromAction().getName();
      String to = hop.getToAction().getName();
      if (!Utils.isEmpty(from) && !Utils.isEmpty(to)) {
        transitions.add(new Transition(from, to));
      }
    }
    return transitions;
  }

  /** Terminal failure for a workflow at {@code WorkflowFinish} time. */
  public static boolean workflowFailed(IWorkflowEngine<WorkflowMeta> workflow, Result result) {
    Result r = result != null ? result : (workflow != null ? workflow.getResult() : null);
    if (r != null) {
      if (r.getNrErrors() > 0) {
        return true;
      }
      return !r.getResult();
    }
    return false;
  }

  /** Terminal failure for a pipeline at {@code PipelineCompleted} time. */
  public static boolean pipelineFailed(IPipelineEngine<PipelineMeta> pipeline) {
    return pipeline.getErrors() > 0;
  }

  public static void emitTransform(
      TransformMetaDataCombi combi, RunLifecyclePhase phase, String detail) {
    LineageContext.Builder ctx = transformContextBuilder(combi);
    if (ctx == null) {
      return;
    }
    ITransform transform = combi.transform;
    ctx.putAttribute("transformErrors", String.valueOf(transform.getErrors()));
    if (phase == RunLifecyclePhase.FINISHED || phase == RunLifecyclePhase.FAILED) {
      ctx.putAttribute(OUTCOME, phase == RunLifecyclePhase.FAILED ? FAILED : SUCCESS);
      LineageRowCounts.putTransformCounts(ctx, transform);
    }
    LineageHub.getInstance()
        .emit(
            LineageEvent.of(
                LineageEventKind.RUN_LIFECYCLE,
                ctx.build(),
                new RunLifecycleLineagePayload(phase, detail)));
  }

  public static boolean transformFailed(TransformMetaDataCombi combi) {
    return combi != null && combi.transform != null && combi.transform.getErrors() > 0;
  }

  /**
   * Action {@code STARTED}, emitted by {@link LineageActionStartListener} once the workflow has
   * cloned the action: the clone's log channel is the action run's identity, the same one its
   * {@code FINISHED} event, its I/O events and any pipeline it starts refer to. The {@code
   * WorkflowBeforeActionExecution} extension point fires before that clone exists, which is why the
   * start is not emitted from there.
   */
  public static void emitActionStarted(IWorkflowEngine<WorkflowMeta> wf, IAction action) {
    if (wf == null || action == null) {
      return;
    }
    LineageContext.Builder ctx = actionContextBuilder(wf, action);
    LineageHub.getInstance()
        .emit(
            LineageEvent.of(
                LineageEventKind.RUN_LIFECYCLE,
                ctx.build(),
                new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null)));
  }

  /**
   * Action {@code FAILED} for a run that started but whose {@code execute()} threw: the workflow
   * then skips the extension point that ends it. See {@link
   * LineageActionStartListener#failUnfinishedActions(IWorkflowEngine)}.
   */
  static void emitActionFailedWithoutResult(IWorkflowEngine<WorkflowMeta> wf, IAction action) {
    LineageContext.Builder ctx = actionContextBuilder(wf, action);
    if (ctx == null) {
      return;
    }
    ctx.putAttribute(OUTCOME, FAILED);
    LineageHub.getInstance()
        .emit(
            LineageEvent.of(
                LineageEventKind.RUN_LIFECYCLE,
                ctx.build(),
                new RunLifecycleLineagePayload(
                    RunLifecyclePhase.FAILED, "The action ended without a result")));
  }

  /**
   * Action {@code FINISHED} or {@code FAILED}. An action the workflow skipped (an extension point
   * set {@code executeAction} to false) never ran and has no run of its own, so it emits nothing.
   */
  public static void emitWorkflowAction(
      WorkflowExecutionExtension extension, RunLifecyclePhase phase, String detail) {
    if (extension == null
        || extension.workflow == null
        || Utils.isEmpty(extension.actionLogChannelId)) {
      return;
    }
    IWorkflowEngine<WorkflowMeta> wf = extension.workflow;
    ActionMeta am = extension.actionMeta;
    String actionName = am != null ? am.getName() : null;
    WorkflowMeta wmeta = wf.getWorkflowMeta();
    String workflowName = wmeta != null ? wmeta.getName() : null;
    String filename = wmeta != null ? wmeta.getFilename() : null;

    LineageContext.Builder ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.ACTION)
            .logChannelId(extension.actionLogChannelId)
            .workflowName(workflowName)
            .actionName(actionName);
    fillFilenameFields(ctx, filename, wf);

    ctx.putAttribute("workflowLogChannelId", wf.getLogChannelId());

    if (am != null && am.getAction() != null && am.getAction().getPluginId() != null) {
      ctx.putAttribute("actionPluginId", am.getAction().getPluginId());
    }
    if (am != null) {
      fillDescription(ctx, am.getDescription());
    }

    if (extension.actionExecutionResult != null) {
      ctx.putAttribute("nrErrors", String.valueOf(extension.actionExecutionResult.getNrErrors()));
      ctx.putAttribute("result", String.valueOf(extension.actionExecutionResult.getResult()));
    }

    if (phase == RunLifecyclePhase.FINISHED || phase == RunLifecyclePhase.FAILED) {
      ctx.putAttribute(OUTCOME, phase == RunLifecyclePhase.FAILED ? FAILED : SUCCESS);
    }

    LineageHub.getInstance()
        .emit(
            LineageEvent.of(
                LineageEventKind.RUN_LIFECYCLE,
                ctx.build(),
                new RunLifecycleLineagePayload(phase, detail)));
  }

  public static boolean workflowActionFailed(WorkflowExecutionExtension extension) {
    if (extension == null || !extension.executeAction) {
      return false;
    }
    Result ar = extension.actionExecutionResult;
    if (ar == null) {
      return false;
    }
    return ar.getNrErrors() > 0 || !ar.getResult();
  }
}
