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

package org.apache.hop.server.loadbalance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.www.HopServerPipelineStatus;
import org.apache.hop.www.HopServerStatus;
import org.apache.hop.www.HopServerWorkflowStatus;
import org.junit.jupiter.api.Test;

class HopServerProbeTest {

  @Test
  void occupyingSlotsIgnoreFinishedAndStopped() {
    HopServerStatus status = new HopServerStatus();
    status.setPipelineStatusList(
        List.of(
            pipeline(Pipeline.STRING_RUNNING),
            pipeline(Pipeline.STRING_WAITING),
            pipeline(Pipeline.STRING_FINISHED),
            pipeline(Pipeline.STRING_STOPPED)));
    status.setWorkflowStatusList(
        List.of(workflow(true, false), workflow(false, true), workflow(false, false)));

    ServerHealthSnapshot snapshot = new ServerHealthSnapshot();
    HopServerProbe.applyStatus(snapshot, status);

    assertEquals(1, snapshot.getRunningPipelines());
    assertEquals(1, snapshot.getFinishedPipelines());
    assertEquals(1, snapshot.getRunningWorkflows());
    assertEquals(1, snapshot.getFinishedWorkflows());
    assertEquals(4, snapshot.getOccupyingSlots());
    assertTrue(snapshot.getLoadAvg() == null || snapshot.getLoadAvg() == 0.0);
  }

  private static HopServerPipelineStatus pipeline(String statusDescription) {
    return new HopServerPipelineStatus("p", "id", statusDescription);
  }

  private static HopServerWorkflowStatus workflow(boolean running, boolean finished) {
    HopServerWorkflowStatus status = new HopServerWorkflowStatus();
    if (running) {
      status.setStatusDescription("Running");
    } else if (finished) {
      status.setStatusDescription("Finished");
    } else {
      status.setStatusDescription("Waiting");
    }
    return status;
  }
}
