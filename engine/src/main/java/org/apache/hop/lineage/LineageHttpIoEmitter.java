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

import org.apache.hop.core.util.Utils;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineagePortableFilename;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.HttpLineagePayload;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * Emits {@link LineageEventKind#HTTP_IO} with {@link HttpLineagePayload} for HTTP client calls from
 * pipeline transforms or workflow actions.
 */
public final class LineageHttpIoEmitter {

  private LineageHttpIoEmitter() {}

  public static void emitTransformHttpIo(ITransform transform, HttpLineagePayload payload) {
    if (transform == null || payload == null) {
      return;
    }
    LineageContext.Builder ctx = LineageRunLifecycleEmitter.transformContextBuilder(transform);
    if (ctx == null) {
      return;
    }
    if (!Utils.isEmpty(transform.getTransformPluginId())) {
      ctx.putAttribute("transformPluginId", transform.getTransformPluginId());
    }
    LineageHub.getInstance().emit(LineageEvent.of(LineageEventKind.HTTP_IO, ctx.build(), payload));
  }

  public static void emitWorkflowActionHttpIo(
      IWorkflowEngine<WorkflowMeta> workflow, IAction action, HttpLineagePayload payload) {
    if (workflow == null || action == null || payload == null) {
      return;
    }
    WorkflowMeta meta = workflow.getWorkflowMeta();
    String workflowName = meta != null ? meta.getName() : null;
    String filename = meta != null ? meta.getFilename() : null;

    LineageContext.Builder ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.ACTION)
            .logChannelId(workflow.getLogChannelId())
            .workflowName(workflowName)
            .actionName(action.getName());
    if (!Utils.isEmpty(filename)) {
      ctx.hopFilename(filename);
      ctx.hopFilenamePortableKey(LineagePortableFilename.portableKey(filename, workflow));
    }
    ctx.putAttribute("workflowLogChannelId", workflow.getLogChannelId());
    if (!Utils.isEmpty(action.getPluginId())) {
      ctx.putAttribute("actionPluginId", action.getPluginId());
    }

    LineageHub.getInstance().emit(LineageEvent.of(LineageEventKind.HTTP_IO, ctx.build(), payload));
  }
}
