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

package org.apache.hop.replay.engine;

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

class WorkflowReplayGuiPluginTest {

  @Test
  void nullProviderReturnsNull() {
    assertNull(WorkflowReplayGuiPlugin.findFirstReplayRunConfiguration(null));
  }

  @Test
  void emptyListReturnsNull() throws Exception {
    IHopMetadataProvider provider = providerWith(Collections.emptyList());
    assertNull(WorkflowReplayGuiPlugin.findFirstReplayRunConfiguration(provider));
  }

  @Test
  void matchesReplayPluginIdOnly() throws Exception {
    WorkflowRunConfiguration local = new WorkflowRunConfiguration();
    local.setName("no-replay-local");
    local.setEngineRunConfiguration(new LocalWorkflowRunConfiguration());

    WorkflowRunConfiguration namedLikeReplay = new WorkflowRunConfiguration();
    namedLikeReplay.setName("my-replay-config");
    IWorkflowEngineRunConfiguration other = mock(IWorkflowEngineRunConfiguration.class);
    when(other.getEnginePluginId()).thenReturn("Local");
    when(other.getEnginePluginName()).thenReturn("Hop replay workflow engine");
    namedLikeReplay.setEngineRunConfiguration(other);

    WorkflowRunConfiguration replay = new WorkflowRunConfiguration();
    replay.setName("replay-run");
    IWorkflowEngineRunConfiguration replayEngine = mock(IWorkflowEngineRunConfiguration.class);
    when(replayEngine.getEnginePluginId()).thenReturn("Replay");
    replay.setEngineRunConfiguration(replayEngine);

    List<WorkflowRunConfiguration> list = new ArrayList<>();
    list.add(local);
    list.add(namedLikeReplay);
    list.add(replay);

    assertEquals(
        "replay-run", WorkflowReplayGuiPlugin.findFirstReplayRunConfiguration(providerWith(list)));
  }

  @SuppressWarnings("unchecked")
  private static IHopMetadataProvider providerWith(List<WorkflowRunConfiguration> configs)
      throws Exception {
    IHopMetadataProvider provider = mock(IHopMetadataProvider.class);
    IHopMetadataSerializer<WorkflowRunConfiguration> serializer =
        mock(IHopMetadataSerializer.class);
    when(provider.getSerializer(WorkflowRunConfiguration.class)).thenReturn(serializer);
    when(serializer.loadAll()).thenReturn(configs);
    return provider;
  }
}
