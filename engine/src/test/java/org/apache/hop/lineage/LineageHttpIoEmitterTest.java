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

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.HttpDirection;
import org.apache.hop.lineage.model.HttpLineagePayload;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LineageHttpIoEmitterTest {

  @Test
  void emitTransformHttpIo_postsHttpIoEvent() {
    LineageHub hub = mock(LineageHub.class);

    ITransform tr = mock(ITransform.class);
    ILogChannel logCh = mock(ILogChannel.class);
    when(logCh.getLogChannelId()).thenReturn("tr-log-http");
    when(tr.getLogChannel()).thenReturn(logCh);
    when(tr.getTransformName()).thenReturn("HTTP client");
    when(tr.getCopy()).thenReturn(0);
    when(tr.getTransformPluginId()).thenReturn("HTTP");

    PipelineMeta pm = new PipelineMeta();
    pm.setName("pipe-http");
    pm.setFilename("/tmp/p.hpl");
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.getPipelineMeta()).thenReturn(pm);
    when(pipeline.getFilename()).thenReturn("/tmp/p.hpl");
    when(pipeline.getLogChannelId()).thenReturn("pipe-log");
    when(tr.getPipeline()).thenReturn(pipeline);

    HttpLineagePayload payload =
        new HttpLineagePayload(
            HttpDirection.CLIENT, "GET", "https://example.test/x", 200, null, 10L, 5L, true, null);

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      LineageHttpIoEmitter.emitTransformHttpIo(tr, payload);
    }

    verify(hub)
        .emit(
            argThat(
                (LineageEvent e) ->
                    e.getKind() == LineageEventKind.HTTP_IO
                        && e.getContext().getSubjectType() == LineageSubjectType.TRANSFORM
                        && e.getPayload() instanceof HttpLineagePayload p
                        && p.getUrl().equals("https://example.test/x")
                        && p.getResponseBytes() == 10L));
  }

  @Test
  void emitWorkflowActionHttpIo_postsHttpIoEvent() {
    LineageHub hub = mock(LineageHub.class);

    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("wf");
    meta.setFilename("/tmp/w.hwf");
    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> wf = mock(IWorkflowEngine.class);
    when(wf.getWorkflowMeta()).thenReturn(meta);
    when(wf.getLogChannelId()).thenReturn("wf-log");

    ActionBase action = mock(ActionBase.class);
    when(action.getName()).thenReturn("HTTP");
    when(action.getPluginId()).thenReturn("HTTP");

    HttpLineagePayload payload =
        new HttpLineagePayload(
            HttpDirection.CLIENT, "POST", "https://api.test/", 201, 100L, 200L, 20L, true, null);

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      LineageHttpIoEmitter.emitWorkflowActionHttpIo(wf, action, payload);
    }

    verify(hub)
        .emit(
            argThat(
                (LineageEvent e) ->
                    e.getKind() == LineageEventKind.HTTP_IO
                        && e.getContext().getSubjectType() == LineageSubjectType.ACTION
                        && e.getPayload() instanceof HttpLineagePayload p
                        && p.getRequestBytes() == 100L));
  }
}
