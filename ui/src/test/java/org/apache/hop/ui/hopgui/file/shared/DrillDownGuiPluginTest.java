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

package org.apache.hop.ui.hopgui.file.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DrillDownGuiPluginTest {

  @AfterEach
  void tearDown() {
    DrillDownGuiPlugin.cleanupSession("gui-a");
    DrillDownGuiPlugin.cleanupSession("gui-b");
    DrillDownGuiPlugin.cleanupSession("session-1");
  }

  @Test
  void cleanupOnRunStartDoesNotClearAnotherSession() {
    IPipelineEngine<PipelineMeta> engineA = engine("gui-a", "log-a");
    IPipelineEngine<PipelineMeta> engineB = engine("gui-b", "log-b");

    DrillDownGuiPlugin.registerRunningPipeline("log-a", engineA);
    DrillDownGuiPlugin.registerRunningPipeline("log-b", engineB);

    DrillDownGuiPlugin.cleanupOnRunStart("gui-a");

    assertNull(DrillDownGuiPlugin.runningPipeline("gui-a", "log-a"));
    assertSame(engineB, DrillDownGuiPlugin.runningPipeline("gui-b", "log-b"));
  }

  @Test
  void hopGuiIdWalksExtensionData() {
    IPipelineEngine<PipelineMeta> engine = engine("session-1", "log-1");
    assertEquals("session-1", DrillDownGuiPlugin.hopGuiIdOf(engine));
  }

  @SuppressWarnings("unchecked")
  private static IPipelineEngine<PipelineMeta> engine(String hopGuiId, String logChannelId) {
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    Map<String, Object> data = new HashMap<>();
    data.put(DrillDownGuiPlugin.HOP_GUI_ID, hopGuiId);
    when(pipeline.getExtensionDataMap()).thenReturn(data);
    when(pipeline.getLogChannelId()).thenReturn(logChannelId);
    when(pipeline.getParent()).thenReturn(null);
    return pipeline;
  }
}
