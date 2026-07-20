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

package org.apache.hop.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.junit.jupiter.api.Test;

class ExecutionStateBuilderMetricsTest {

  @Test
  void resolveDurationMsPrefersExecutionDuration() {
    IEngineComponent component = mock(IEngineComponent.class);
    when(component.getExecutionDuration()).thenReturn(2500L);

    assertEquals(2500L, ExecutionStateBuilder.resolveDurationMs(component));
  }

  @Test
  void resolveDurationMsFallsBackToFirstLastRowDates() {
    IEngineComponent component = mock(IEngineComponent.class);
    when(component.getExecutionDuration()).thenReturn(0L);
    Date first = new Date(1_000_000L);
    Date last = new Date(1_005_000L);
    when(component.getFirstRowReadDate()).thenReturn(first);
    when(component.getLastRowWrittenDate()).thenReturn(last);

    assertEquals(5000L, ExecutionStateBuilder.resolveDurationMs(component));
  }

  @Test
  void parseSpeedRowsPerSecondHandlesFormattedValues() {
    assertEquals(1234L, ExecutionStateBuilder.parseSpeedRowsPerSecond(" 1,234"));
    assertEquals(10L, ExecutionStateBuilder.parseSpeedRowsPerSecond("10.4"));
    assertNull(ExecutionStateBuilder.parseSpeedRowsPerSecond("-"));
    assertNull(ExecutionStateBuilder.parseSpeedRowsPerSecond(""));
    assertNull(ExecutionStateBuilder.parseSpeedRowsPerSecond(null));
  }

  @Test
  void resolveSpeedFromEngineMap() {
    IEngineComponent component = mock(IEngineComponent.class);
    EngineMetrics metrics = new EngineMetrics();
    Map<IEngineComponent, String> speedMap = new HashMap<>();
    speedMap.put(component, " 500");
    metrics.setComponentSpeedMap(speedMap);

    assertEquals(500L, ExecutionStateBuilder.resolveSpeedRowsPerSecond(component, metrics, 1000L));
  }

  @Test
  void resolveSpeedFromRowCountsAndDuration() {
    IEngineComponent component = mock(IEngineComponent.class);
    when(component.getLinesInput()).thenReturn(0L);
    when(component.getLinesRead()).thenReturn(1000L);
    when(component.getLinesOutput()).thenReturn(0L);
    when(component.getLinesUpdated()).thenReturn(0L);
    when(component.getLinesWritten()).thenReturn(1000L);
    when(component.getLinesRejected()).thenReturn(0L);

    // 1000 rows over 2 seconds => 500 r/s
    assertEquals(
        500L,
        ExecutionStateBuilder.resolveSpeedRowsPerSecond(component, new EngineMetrics(), 2000L));
  }
}
