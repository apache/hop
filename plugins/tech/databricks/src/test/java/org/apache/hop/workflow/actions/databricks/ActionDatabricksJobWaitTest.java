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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
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

class ActionDatabricksJobWaitTest {

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
  void waitsForSuccess() throws Exception {
    when(client.getRun(anyLong()))
        .thenReturn(
            new DatabricksRunStatus(
                55L, 3L, DatabricksRunLifeCycleState.TERMINATED, "SUCCESS", null, "http://x"));

    ActionDatabricksJobWait action = new ActionDatabricksJobWait("wait");
    action.setMetadataProvider(metadataProvider);
    action.setConnectionName("dbx");
    action.setRunId("55");
    action.setPollIntervalSeconds("1");
    action.setClientFactory((c, v) -> client);

    Result result = new Result();
    action.execute(result, 0);

    assertTrue(result.getResult());
    assertEquals("SUCCESS", action.getVariable("DatabricksStatus"));
    assertEquals("55", action.getVariable("DatabricksRunId"));
  }

  @Test
  void failsWithoutRunId() {
    ActionDatabricksJobWait action = new ActionDatabricksJobWait("wait");
    action.setMetadataProvider(metadataProvider);
    action.setConnectionName("dbx");
    action.setRunId("");
    action.setClientFactory((c, v) -> client);

    Result result = new Result();
    action.execute(result, 0);
    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void failedStatusMarksActionFailed() throws Exception {
    when(client.getRun(anyLong()))
        .thenReturn(
            new DatabricksRunStatus(
                1L, 2L, DatabricksRunLifeCycleState.TERMINATED, "FAILED", "nope", null));

    ActionDatabricksJobWait action = new ActionDatabricksJobWait("wait");
    action.setMetadataProvider(metadataProvider);
    action.setConnectionName("dbx");
    action.setRunId("1");
    action.setClientFactory((c, v) -> client);

    Result result = new Result();
    action.execute(result, 0);
    assertFalse(result.getResult());
    assertEquals("FAILED", action.getVariable("DatabricksStatus"));
  }
}
