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

package org.apache.hop.ui.hopgui.perspective.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.local.LocalWorkflowRunConfiguration;
import org.junit.jupiter.api.Test;

class WorkflowExecutionViewerTest {

  @Test
  void testFindFirstReplayRunConfiguration_NullProvider() {
    assertNull(WorkflowExecutionViewer.findFirstReplayRunConfiguration(null));
  }

  @Test
  void testFindFirstReplayRunConfiguration_EmptyList() throws Exception {
    IHopMetadataProvider provider = mock(IHopMetadataProvider.class);
    @SuppressWarnings("unchecked")
    IHopMetadataSerializer<WorkflowRunConfiguration> serializer =
        mock(IHopMetadataSerializer.class);
    when(provider.getSerializer(WorkflowRunConfiguration.class)).thenReturn(serializer);
    when(serializer.loadAll()).thenReturn(Collections.emptyList());

    assertNull(WorkflowExecutionViewer.findFirstReplayRunConfiguration(provider));
  }

  @Test
  void testFindFirstReplayRunConfiguration_MatchesPluginId() throws Exception {
    IHopMetadataProvider provider = mock(IHopMetadataProvider.class);
    @SuppressWarnings("unchecked")
    IHopMetadataSerializer<WorkflowRunConfiguration> serializer =
        mock(IHopMetadataSerializer.class);
    when(provider.getSerializer(WorkflowRunConfiguration.class)).thenReturn(serializer);

    List<WorkflowRunConfiguration> list = new ArrayList<>();
    WorkflowRunConfiguration local = new WorkflowRunConfiguration();
    local.setName("local");
    local.setEngineRunConfiguration(new LocalWorkflowRunConfiguration());
    list.add(local);

    WorkflowRunConfiguration replay = new WorkflowRunConfiguration();
    replay.setName("replay-run");
    IWorkflowEngineRunConfiguration replayEngineConfig =
        mock(IWorkflowEngineRunConfiguration.class);
    when(replayEngineConfig.getEnginePluginId()).thenReturn("Replay");
    replay.setEngineRunConfiguration(replayEngineConfig);
    list.add(replay);

    when(serializer.loadAll()).thenReturn(list);

    assertEquals("replay-run", WorkflowExecutionViewer.findFirstReplayRunConfiguration(provider));
  }

  @Test
  void testFindFirstReplayRunConfiguration_MatchesPluginName() throws Exception {
    IHopMetadataProvider provider = mock(IHopMetadataProvider.class);
    @SuppressWarnings("unchecked")
    IHopMetadataSerializer<WorkflowRunConfiguration> serializer =
        mock(IHopMetadataSerializer.class);
    when(provider.getSerializer(WorkflowRunConfiguration.class)).thenReturn(serializer);

    List<WorkflowRunConfiguration> list = new ArrayList<>();
    WorkflowRunConfiguration replay = new WorkflowRunConfiguration();
    replay.setName("custom-replay");
    IWorkflowEngineRunConfiguration replayEngineConfig =
        mock(IWorkflowEngineRunConfiguration.class);
    when(replayEngineConfig.getEnginePluginId()).thenReturn("other");
    when(replayEngineConfig.getEnginePluginName()).thenReturn("Hop replay workflow engine");
    replay.setEngineRunConfiguration(replayEngineConfig);
    list.add(replay);

    when(serializer.loadAll()).thenReturn(list);

    assertEquals(
        "custom-replay", WorkflowExecutionViewer.findFirstReplayRunConfiguration(provider));
  }

  @Test
  void testFindFirstReplayRunConfiguration_FallbackToConfigName() throws Exception {
    IHopMetadataProvider provider = mock(IHopMetadataProvider.class);
    @SuppressWarnings("unchecked")
    IHopMetadataSerializer<WorkflowRunConfiguration> serializer =
        mock(IHopMetadataSerializer.class);
    when(provider.getSerializer(WorkflowRunConfiguration.class)).thenReturn(serializer);

    List<WorkflowRunConfiguration> list = new ArrayList<>();
    WorkflowRunConfiguration replay = new WorkflowRunConfiguration();
    replay.setName("my-replay-config");
    replay.setEngineRunConfiguration(new LocalWorkflowRunConfiguration());
    list.add(replay);

    when(serializer.loadAll()).thenReturn(list);

    assertEquals(
        "my-replay-config", WorkflowExecutionViewer.findFirstReplayRunConfiguration(provider));
  }
}
