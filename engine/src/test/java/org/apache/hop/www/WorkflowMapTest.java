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

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkflowMapTest {

  private WorkflowMap map;

  @BeforeEach
  void setUp() {
    map = new WorkflowMap();
  }

  @Test
  void addWorkflowValidatesNameAndId() {
    IWorkflowEngine<WorkflowMeta> wf = mock(IWorkflowEngine.class);
    WorkflowConfiguration cfg = mock(WorkflowConfiguration.class);
    assertThrows(RuntimeException.class, () -> map.addWorkflow("", "id", wf, cfg));
    assertThrows(RuntimeException.class, () -> map.addWorkflow("n", null, wf, cfg));
  }

  @Test
  void addGetRemoveFindAndList() {
    IWorkflowEngine<WorkflowMeta> wf = mock(IWorkflowEngine.class);
    when(wf.getWorkflowName()).thenReturn("W1");
    when(wf.getContainerId()).thenReturn("cid-w");
    WorkflowConfiguration cfg = mock(WorkflowConfiguration.class);

    map.addWorkflow("W1", "cid-w", wf, cfg);

    HopServerObjectEntry entry = new HopServerObjectEntry("W1", "cid-w");
    assertSame(wf, map.getWorkflow(entry));
    assertSame(wf, map.getWorkflow("W1"));
    assertSame(cfg, map.getConfiguration(entry));
    assertSame(wf, map.findWorkflow("cid-w"));
    assertSame(wf, map.findWorkflow("W1", "cid-w"));
    assertSame(wf, map.findWorkflow("W1", null));

    assertNotNull(map.getFirstHopServerObjectEntry("W1"));

    List<HopServerObjectEntry> keys = map.getWorkflowObjects();
    assertEquals(1, keys.size());

    map.removeWorkflow(entry);
    assertNull(map.getWorkflow(entry));
    assertTrue(map.getWorkflowObjects().isEmpty());
  }

  @Test
  void replaceWorkflowReplacesExisting() {
    IWorkflowEngine<WorkflowMeta> oldWf = mock(IWorkflowEngine.class);
    when(oldWf.getWorkflowName()).thenReturn("W2");
    when(oldWf.getContainerId()).thenReturn("id2");
    WorkflowConfiguration cfg = mock(WorkflowConfiguration.class);
    map.addWorkflow("W2", "id2", oldWf, cfg);

    IWorkflowEngine<WorkflowMeta> newWf = mock(IWorkflowEngine.class);
    when(newWf.getWorkflowName()).thenReturn("W2");
    when(newWf.getContainerId()).thenReturn("id2");

    map.replaceWorkflow(oldWf, newWf, cfg);
    assertSame(newWf, map.getWorkflow("W2"));
  }

  @Test
  void hopServerConfigRoundTrip() {
    HopServerConfig cfg = mock(HopServerConfig.class);
    map.setHopServerConfig(cfg);
    assertSame(cfg, map.getHopServerConfig());
  }
}
