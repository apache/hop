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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class LineageRowCountsTest {

  @Test
  void aFinishedTransformCarriesTheCountersOfItsCopy() {
    ITransform transform = transform(10, 9, 0, 9, 2, 1);
    TransformMetaDataCombi combi = new TransformMetaDataCombi();
    combi.transform = transform;
    combi.transformName = "write";
    combi.copy = 1;

    Map<String, String> attributes =
        lifecycleAttributes(
            () ->
                LineageRunLifecycleEmitter.emitTransform(combi, RunLifecyclePhase.FINISHED, null));

    assertEquals(10L, LineageRowCounts.get(attributes, LineageRowCounts.LINES_READ));
    assertEquals(9L, LineageRowCounts.get(attributes, LineageRowCounts.LINES_WRITTEN));
    assertEquals(0L, LineageRowCounts.get(attributes, LineageRowCounts.LINES_INPUT));
    assertEquals(9L, LineageRowCounts.get(attributes, LineageRowCounts.LINES_OUTPUT));
    assertEquals(2L, LineageRowCounts.get(attributes, LineageRowCounts.LINES_UPDATED));
    assertEquals(1L, LineageRowCounts.get(attributes, LineageRowCounts.LINES_REJECTED));
  }

  @Test
  void aStartingTransformHasNoCountersYet() {
    TransformMetaDataCombi combi = new TransformMetaDataCombi();
    combi.transform = transform(0, 0, 0, 0, 0, 0);
    combi.transformName = "write";

    Map<String, String> attributes =
        lifecycleAttributes(
            () -> LineageRunLifecycleEmitter.emitTransform(combi, RunLifecyclePhase.STARTED, null));

    assertFalse(attributes.containsKey(LineageRowCounts.LINES_OUTPUT));
  }

  @Test
  void aPipelineCarriesTheTotalsOfAllItsTransformsAndCopies() {
    Pipeline pipeline = mock(Pipeline.class);
    when(pipeline.getLogChannelId()).thenReturn("pipeline-1");
    List<IEngineComponent> components =
        List.of(
            transform(0, 100, 100, 0, 0, 0),
            transform(100, 60, 0, 60, 0, 3),
            transform(100, 40, 0, 40, 5, 0));
    when(pipeline.getComponents()).thenReturn(components);

    Map<String, String> attributes =
        lifecycleAttributes(
            () ->
                LineageRunLifecycleEmitter.emitPipeline(
                    pipeline, RunLifecyclePhase.FINISHED, null));

    assertEquals(100L, LineageRowCounts.get(attributes, LineageRowCounts.LINES_INPUT));
    assertEquals(100L, LineageRowCounts.get(attributes, LineageRowCounts.LINES_OUTPUT));
    assertEquals(5L, LineageRowCounts.get(attributes, LineageRowCounts.LINES_UPDATED));
    assertEquals(3L, LineageRowCounts.get(attributes, LineageRowCounts.LINES_REJECTED));
    // Rows passed between transforms don't add up across transforms
    assertNull(LineageRowCounts.get(attributes, LineageRowCounts.LINES_READ));
  }

  @Test
  void aStoppedPipelineSaysSo() {
    Pipeline pipeline = mock(Pipeline.class);
    when(pipeline.getLogChannelId()).thenReturn("pipeline-2");
    List<IEngineComponent> components = List.of(transform(0, 10, 0, 10, 0, 0));
    when(pipeline.getComponents()).thenReturn(components);
    when(pipeline.isStopped()).thenReturn(true);

    Map<String, String> attributes =
        lifecycleAttributes(
            () ->
                LineageRunLifecycleEmitter.emitPipeline(
                    pipeline, RunLifecyclePhase.FINISHED, null));

    assertEquals("true", attributes.get(LineageRowCounts.STOPPED));
  }

  @Test
  void anotherEngineSendsNoTotals() {
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.getLogChannelId()).thenReturn("beam-1");
    List<IEngineComponent> components = List.of(transform(0, 0, 7, 7, 0, 0));
    when(pipeline.getComponents()).thenReturn(components);

    Map<String, String> attributes =
        lifecycleAttributes(
            () ->
                LineageRunLifecycleEmitter.emitPipeline(
                    pipeline, RunLifecyclePhase.FINISHED, null));

    assertNull(LineageRowCounts.get(attributes, LineageRowCounts.LINES_OUTPUT));
  }

  @Test
  void aCounterThatIsMissingOrNotANumberIsNull() {
    assertNull(LineageRowCounts.get(null, LineageRowCounts.LINES_OUTPUT));
    assertNull(LineageRowCounts.get(Map.of(), LineageRowCounts.LINES_OUTPUT));
    assertNull(
        LineageRowCounts.get(
            Map.of(LineageRowCounts.LINES_OUTPUT, "x"), LineageRowCounts.LINES_OUTPUT));
    assertEquals(
        42L,
        LineageRowCounts.get(
            Map.of(LineageRowCounts.LINES_OUTPUT, " 42 "), LineageRowCounts.LINES_OUTPUT));
  }

  private static ITransform transform(
      long read, long written, long input, long output, long updated, long rejected) {
    ITransform transform = mock(ITransform.class);
    ILogChannel log = mock(ILogChannel.class);
    when(log.getLogChannelId()).thenReturn("transform-log");
    when(transform.getLogChannel()).thenReturn(log);
    when(transform.getTransformName()).thenReturn("write");
    when(transform.getLinesRead()).thenReturn(read);
    when(transform.getLinesWritten()).thenReturn(written);
    when(transform.getLinesInput()).thenReturn(input);
    when(transform.getLinesOutput()).thenReturn(output);
    when(transform.getLinesUpdated()).thenReturn(updated);
    when(transform.getLinesRejected()).thenReturn(rejected);
    return transform;
  }

  /** The attributes of the lifecycle event the emitter sends to the hub. */
  private static Map<String, String> lifecycleAttributes(Runnable emit) {
    LineageHub hub = mock(LineageHub.class);
    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      emit.run();
    }
    ArgumentCaptor<LineageEvent> captor = ArgumentCaptor.forClass(LineageEvent.class);
    verify(hub, atLeastOnce()).emit(captor.capture());
    return captor.getAllValues().stream()
        .filter(event -> event.getKind() == LineageEventKind.RUN_LIFECYCLE)
        .findFirst()
        .orElseThrow()
        .getContext()
        .getAttributes();
  }
}
