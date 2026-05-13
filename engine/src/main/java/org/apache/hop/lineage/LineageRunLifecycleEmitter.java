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

import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineagePortableFilename;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowExecutionExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * Builds {@link LineageEvent}s for pipeline and workflow run lifecycle and emits them to {@link
 * LineageHub}.
 */
public final class LineageRunLifecycleEmitter {

  private LineageRunLifecycleEmitter() {}

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

  static LineageContext.Builder transformContextBuilder(TransformMetaDataCombi combi) {
    if (combi == null || combi.transform == null) {
      return null;
    }
    LineageContext.Builder ctx = transformContextBuilder(combi.transform);
    if (!Utils.isEmpty(combi.transformName)) {
      ctx.transformName(combi.transformName);
    }
    ctx.copyNr(String.valueOf(combi.copy));
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
    ctx.putAttribute("errors", String.valueOf(pipeline.getErrors()));
    if (phase == RunLifecyclePhase.FINISHED || phase == RunLifecyclePhase.FAILED) {
      ctx.putAttribute("outcome", phase == RunLifecyclePhase.FAILED ? "failed" : "success");
    }
    LineageHub.getInstance()
        .emit(
            LineageEvent.of(
                LineageEventKind.RUN_LIFECYCLE,
                ctx.build(),
                new RunLifecycleLineagePayload(phase, detail)));
  }

  public static void emitWorkflow(Workflow workflow, RunLifecyclePhase phase, String detail) {
    WorkflowMeta meta = workflow.getWorkflowMeta();
    String name = meta != null ? meta.getName() : null;
    String filename = meta != null ? meta.getFilename() : null;
    LineageContext.Builder ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.WORKFLOW)
            .logChannelId(workflow.getLogChannelId())
            .workflowName(name);
    fillFilenameFields(ctx, filename, workflow);
    ctx.putAttribute("errors", String.valueOf(workflow.getErrors()));
    Result result = workflow.getResult();
    if (result != null) {
      ctx.putAttribute("nrErrors", String.valueOf(result.getNrErrors()));
      ctx.putAttribute("result", String.valueOf(result.getResult()));
    }
    if (phase == RunLifecyclePhase.FINISHED || phase == RunLifecyclePhase.FAILED) {
      ctx.putAttribute("outcome", phase == RunLifecyclePhase.FAILED ? "failed" : "success");
    }
    LineageHub.getInstance()
        .emit(
            LineageEvent.of(
                LineageEventKind.RUN_LIFECYCLE,
                ctx.build(),
                new RunLifecycleLineagePayload(phase, detail)));
  }

  /** Terminal failure for a workflow at {@code WorkflowFinish} time. */
  public static boolean workflowFailed(Workflow workflow, Result result) {
    if (workflow.getErrors() > 0) {
      return true;
    }
    if (result != null) {
      if (result.getNrErrors() > 0) {
        return true;
      }
      return !result.getResult();
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
      ctx.putAttribute("outcome", phase == RunLifecyclePhase.FAILED ? "failed" : "success");
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

  public static void emitWorkflowAction(
      WorkflowExecutionExtension extension, RunLifecyclePhase phase, String detail) {
    if (extension == null || extension.workflow == null) {
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
            .workflowName(workflowName)
            .actionName(actionName);
    fillFilenameFields(ctx, filename, wf);

    ctx.putAttribute("workflowLogChannelId", wf.getLogChannelId());
    ctx.putAttribute("executeAction", String.valueOf(extension.executeAction));

    if (am != null && am.getAction() != null && am.getAction().getPluginId() != null) {
      ctx.putAttribute("actionPluginId", am.getAction().getPluginId());
    }

    if (phase == RunLifecyclePhase.STARTED) {
      ctx.logChannelId(wf.getLogChannelId());
      if (am != null && am.getAction() != null) {
        ctx.putAttribute(
            "actionTemplateLogChannelId", am.getAction().getLogChannel().getLogChannelId());
      }
    } else {
      String actionLogId = extension.actionLogChannelId;
      ctx.logChannelId(!Utils.isEmpty(actionLogId) ? actionLogId : wf.getLogChannelId());
    }

    if (extension.actionExecutionResult != null) {
      ctx.putAttribute("nrErrors", String.valueOf(extension.actionExecutionResult.getNrErrors()));
      ctx.putAttribute("result", String.valueOf(extension.actionExecutionResult.getResult()));
    }

    if (phase == RunLifecyclePhase.FINISHED || phase == RunLifecyclePhase.FAILED) {
      ctx.putAttribute("outcome", phase == RunLifecyclePhase.FAILED ? "failed" : "success");
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
