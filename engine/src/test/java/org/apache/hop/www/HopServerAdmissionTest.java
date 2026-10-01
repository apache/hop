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

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.PipelineConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.junit.jupiter.api.Test;

class HopServerAdmissionTest {

  @Test
  void parseMaxConcurrentTreatsBlankAsUnlimited() {
    assertEquals(0, HopServerAdmission.parseMaxConcurrent((String) null));
    assertEquals(0, HopServerAdmission.parseMaxConcurrent(""));
    assertEquals(4, HopServerAdmission.parseMaxConcurrent("4"));
  }

  @Test
  void querySuffixOnlyWhenCapped() {
    assertEquals("", HopServerAdmission.querySuffix(0));
    assertEquals("&max_concurrent=3", HopServerAdmission.querySuffix(3));
  }

  @Test
  void finishedPipelinesDoNotOccupySlots() {
    PipelineMap pipelines = new PipelineMap();
    IPipelineEngine<PipelineMeta> running = mockEngine(false);
    IPipelineEngine<PipelineMeta> finished = mockEngine(true);
    pipelines.addPipeline("run", "1", running, mock(PipelineConfiguration.class));
    pipelines.addPipeline("done", "2", finished, mock(PipelineConfiguration.class));

    assertEquals(1, HopServerAdmission.countOccupyingSlots(pipelines, new WorkflowMap()));
  }

  @Test
  void admitAndAddRefusesWhenAtCapacity() {
    PipelineMap pipelines = new PipelineMap();
    pipelines.addPipeline("run", "1", mockEngine(false), mock(PipelineConfiguration.class));
    pipelines.addPipeline("run2", "2", mockEngine(false), mock(PipelineConfiguration.class));

    assertThrows(
        HopServerAtCapacityException.class,
        () -> HopServerAdmission.admitAndAdd(pipelines, new WorkflowMap(), 2, () -> {}));
  }

  @Test
  void admitAndAddAllowsWhenUnderCapacity() throws HopServerAtCapacityException {
    PipelineMap pipelines = new PipelineMap();
    pipelines.addPipeline("run", "1", mockEngine(false), mock(PipelineConfiguration.class));
    boolean[] added = {false};
    HopServerAdmission.admitAndAdd(pipelines, new WorkflowMap(), 2, () -> added[0] = true);
    assertTrue(added[0]);
  }

  @Test
  void retryableOnlyBeforeAContainerIsAssigned() {
    HopException atCapacity = new HopServerAtCapacityException(2, 2);
    assertTrue(HopServerAdmission.isRetryableRegistrationFailure(atCapacity, null));
    assertFalse(HopServerAdmission.isRetryableRegistrationFailure(atCapacity, "container-1"));
    assertTrue(
        HopServerAdmission.isRetryableRegistrationFailure(
            new HopException("SERVER_AT_CAPACITY: 2 occupying slots, max 2"), null));
    assertTrue(
        HopServerAdmission.isRetryableRegistrationFailure(
            new HopException("Error preparing load-balanced pipeline", atCapacity), null));
    assertTrue(
        HopServerAdmission.isRetryableRegistrationFailure(
            new HopException(
                "No eligible Hop server in the load-balancing group:\n  - hop-server-a: at capacity (2/2)"),
            null));
  }

  @SuppressWarnings("unchecked")
  private static IPipelineEngine<PipelineMeta> mockEngine(boolean finished) {
    IPipelineEngine<PipelineMeta> engine = mock(IPipelineEngine.class);
    when(engine.isFinished()).thenReturn(finished);
    return engine;
  }
}
