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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload.Transition;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * The graph a sink receives must describe the pipeline that ran: enabled hops only, in pipeline
 * order, and never a transition with a missing end.
 */
class LineagePipelineGraphTest {

  private static TransformMeta transform(String name) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName(name);
    return transformMeta;
  }

  @Test
  void chainIsReportedInPipelineOrder() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta read = transform("Read orders");
    TransformMeta write = transform("Write copy");
    TransformMeta pause = transform("Pause");
    meta.getTransforms().add(read);
    meta.getTransforms().add(write);
    meta.getTransforms().add(pause);
    meta.getHops().add(new PipelineHopMeta(read, write));
    meta.getHops().add(new PipelineHopMeta(write, pause));

    assertEquals(
        List.of(new Transition("Read orders", "Write copy"), new Transition("Write copy", "Pause")),
        LineageRunLifecycleEmitter.pipelineTransitions(meta));
  }

  @Test
  void disabledHopIsNotPartOfTheGraphTheRunExecuted() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("A");
    TransformMeta b = transform("B");
    meta.getHops().add(new PipelineHopMeta(a, b, false));

    assertTrue(LineageRunLifecycleEmitter.pipelineTransitions(meta).isEmpty());
  }

  @Test
  void hopWithoutAnEndIsSkipped() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta a = transform("A");
    PipelineHopMeta noEnds = new PipelineHopMeta();
    noEnds.setEnabled(true);
    PipelineHopMeta noTo = new PipelineHopMeta(a, null);
    noTo.setEnabled(true);
    PipelineHopMeta noFrom = new PipelineHopMeta(null, a);
    noFrom.setEnabled(true);
    meta.getHops().add(noEnds);
    meta.getHops().add(noTo);
    meta.getHops().add(noFrom);

    assertTrue(LineageRunLifecycleEmitter.pipelineTransitions(meta).isEmpty());
  }

  @Test
  void hopWithAnUnnamedEndIsSkipped() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta named = transform("A");
    TransformMeta unnamed = new TransformMeta();
    meta.getHops().add(new PipelineHopMeta(named, unnamed));

    assertTrue(LineageRunLifecycleEmitter.pipelineTransitions(meta).isEmpty());
  }

  @Test
  void diamondKeepsBothBranches() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta read = transform("Read");
    TransformMeta upper = transform("Upper");
    TransformMeta lower = transform("Lower");
    TransformMeta join = transform("Join");
    meta.getHops().add(new PipelineHopMeta(read, upper));
    meta.getHops().add(new PipelineHopMeta(read, lower));
    meta.getHops().add(new PipelineHopMeta(upper, join));
    meta.getHops().add(new PipelineHopMeta(lower, join));

    assertEquals(
        List.of(
            new Transition("Read", "Upper"),
            new Transition("Read", "Lower"),
            new Transition("Upper", "Join"),
            new Transition("Lower", "Join")),
        LineageRunLifecycleEmitter.pipelineTransitions(meta));
  }

  @Test
  void noMetaMeansNoGraph() {
    assertTrue(LineageRunLifecycleEmitter.pipelineTransitions(null).isEmpty());
  }

  @Test
  void startEmitsTheGraphWithTheRunContext() {
    List<LineageEvent> events = emitPipeline(chain(), RunLifecyclePhase.STARTED);

    assertEquals(2, events.size());
    assertEquals(LineageEventKind.RUN_LIFECYCLE, events.get(0).getKind());
    LineageEvent graph = events.get(1);
    assertEquals(LineageEventKind.PIPELINE_GRAPH, graph.getKind());
    assertSame(events.get(0).getContext(), graph.getContext());
    assertEquals(
        List.of(new Transition("Read", "Write")),
        ((PipelineGraphLineagePayload) graph.getPayload()).getTransitions());
  }

  @Test
  void otherPhasesDoNotRepeatTheGraph() {
    for (RunLifecyclePhase phase : RunLifecyclePhase.values()) {
      if (phase == RunLifecyclePhase.STARTED) {
        continue;
      }
      List<LineageEvent> events = emitPipeline(chain(), phase);
      assertEquals(1, events.size(), phase.name());
      assertEquals(LineageEventKind.RUN_LIFECYCLE, events.get(0).getKind(), phase.name());
    }
  }

  @Test
  void pipelineWithoutHopsEmitsNoGraph() {
    PipelineMeta meta = new PipelineMeta();
    meta.getTransforms().add(transform("Alone"));

    List<LineageEvent> events = emitPipeline(meta, RunLifecyclePhase.STARTED);

    assertEquals(1, events.size());
    assertEquals(LineageEventKind.RUN_LIFECYCLE, events.get(0).getKind());
  }

  private static PipelineMeta chain() {
    PipelineMeta meta = new PipelineMeta();
    TransformMeta read = transform("Read");
    TransformMeta write = transform("Write");
    meta.getTransforms().add(read);
    meta.getTransforms().add(write);
    meta.getHops().add(new PipelineHopMeta(read, write));
    return meta;
  }

  @SuppressWarnings("unchecked")
  private static List<LineageEvent> emitPipeline(PipelineMeta meta, RunLifecyclePhase phase) {
    LineageHub hub = mock(LineageHub.class);
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.getPipelineMeta()).thenReturn(meta);
    when(pipeline.getLogChannelId()).thenReturn("pipeline-log");

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      LineageRunLifecycleEmitter.emitPipeline(pipeline, phase, null);
    }

    ArgumentCaptor<LineageEvent> captor = ArgumentCaptor.forClass(LineageEvent.class);
    verify(hub, atLeastOnce()).emit(captor.capture());
    return captor.getAllValues();
  }
}
