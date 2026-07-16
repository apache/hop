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

package org.apache.hop.workflow.actions.databricks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.Result;
import org.apache.hop.databricks.client.DatabricksJobsClient;
import org.apache.hop.databricks.client.DatabricksRunLifeCycleState;
import org.apache.hop.databricks.client.DatabricksRunStatus;
import org.apache.hop.databricks.metadata.DatabricksConnection;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ActionDatabricksJobRunTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private MemoryMetadataProvider metadataProvider;
  private DatabricksJobsClient client;

  @BeforeEach
  void setUp() throws Exception {
    metadataProvider = new MemoryMetadataProvider();
    DatabricksConnection conn = new DatabricksConnection();
    conn.setName("dbx");
    conn.setHost("https://example.databricks.net");
    conn.setToken("tok");
    metadataProvider.getSerializer(DatabricksConnection.class).save(conn);

    client = mock(DatabricksJobsClient.class);
  }

  @Test
  void cloneCopiesFields() {
    ActionDatabricksJobRun action = new ActionDatabricksJobRun("run");
    action.setConnectionName("dbx");
    action.setJobId("12");
    action.setRunMode(ActionDatabricksJobRun.MODE_RUN_EXISTING);
    ActionDatabricksJobRun clone = (ActionDatabricksJobRun) action.clone();
    assertEquals("dbx", clone.getConnectionName());
    assertEquals("12", clone.getJobId());
  }

  @Test
  void failsWithoutConnection() {
    ActionDatabricksJobRun action = new ActionDatabricksJobRun("run");
    action.setMetadataProvider(metadataProvider);
    Result result = new Result();
    action.execute(result, 0);
    assertEquals(1, result.getNrErrors());
    assertFalse(result.getResult());
  }

  @Test
  void runExistingWaitSuccess() throws Exception {
    when(client.runNow(anyLong(), anyMap())).thenReturn(100L);
    when(client.getRun(100L))
        .thenReturn(
            new DatabricksRunStatus(
                100L,
                12L,
                DatabricksRunLifeCycleState.TERMINATED,
                "SUCCESS",
                "ok",
                "https://example/runs/100"));

    ActionDatabricksJobRun action = new ActionDatabricksJobRun("run");
    action.setMetadataProvider(metadataProvider);
    action.setConnectionName("dbx");
    action.setJobId("12");
    action.setRunMode(ActionDatabricksJobRun.MODE_RUN_EXISTING);
    action.setWaitMode(ActionDatabricksJobRun.WAIT_WAIT);
    action.setPollIntervalSeconds("1");
    action.setTimeoutSeconds("30");
    action.setClientFactory((c, v) -> client);

    Result result = new Result();
    action.execute(result, 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertEquals("12", action.getVariable("DatabricksJobId"));
    assertEquals("100", action.getVariable("DatabricksRunId"));
    assertEquals("SUCCESS", action.getVariable("DatabricksStatus"));
    verify(client).runNow(12L, java.util.Map.of());
    verify(client).getRun(100L);
  }

  @Test
  void fireAndForgetDoesNotPoll() throws Exception {
    when(client.runNow(anyLong(), anyMap())).thenReturn(5L);

    ActionDatabricksJobRun action = new ActionDatabricksJobRun("run");
    action.setMetadataProvider(metadataProvider);
    action.setConnectionName("dbx");
    action.setJobId("9");
    action.setWaitMode(ActionDatabricksJobRun.WAIT_FIRE_AND_FORGET);
    action.setClientFactory((c, v) -> client);

    Result result = new Result();
    action.execute(result, 0);

    assertTrue(result.getResult());
    assertEquals("5", action.getVariable("DatabricksRunId"));
    verify(client, never()).getRun(anyLong());
  }

  @Test
  void submitOnceUsesSubmitJson() throws Exception {
    when(client.submitRun(anyString())).thenReturn(77L);
    when(client.getRun(77L))
        .thenReturn(
            new DatabricksRunStatus(
                77L, null, DatabricksRunLifeCycleState.TERMINATED, "SUCCESS", null, null));

    ActionDatabricksJobRun action = new ActionDatabricksJobRun("run");
    action.setMetadataProvider(metadataProvider);
    action.setConnectionName("dbx");
    action.setRunMode(ActionDatabricksJobRun.MODE_SUBMIT_ONCE);
    action.setSubmitRunJson("{\"tasks\":[]}");
    action.setWaitMode(ActionDatabricksJobRun.WAIT_WAIT);
    action.setPollIntervalSeconds("1");
    action.setClientFactory((c, v) -> client);

    Result result = new Result();
    action.execute(result, 0);

    assertTrue(result.getResult());
    verify(client).submitRun("{\"tasks\":[]}");
  }

  @Test
  void failedRunMarksResultFalse() throws Exception {
    when(client.runNow(anyLong(), anyMap())).thenReturn(1L);
    when(client.getRun(1L))
        .thenReturn(
            new DatabricksRunStatus(
                1L, 2L, DatabricksRunLifeCycleState.TERMINATED, "FAILED", "boom", null));

    ActionDatabricksJobRun action = new ActionDatabricksJobRun("run");
    action.setMetadataProvider(metadataProvider);
    action.setConnectionName("dbx");
    action.setJobId("2");
    action.setWaitMode(ActionDatabricksJobRun.WAIT_WAIT);
    action.setPollIntervalSeconds("1");
    action.setClientFactory((c, v) -> client);

    Result result = new Result();
    action.execute(result, 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    assertEquals("FAILED", action.getVariable("DatabricksStatus"));
    assertNotNull(action.getVariable("DatabricksError"));
  }
}
