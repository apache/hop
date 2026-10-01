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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class LoadBalancingAssignmentStoreTest {

  @Test
  void writesAndReadsAssignmentJson() throws Exception {
    Path folder = Files.createTempDirectory("hop-lb-");
    LoadBalancingAssignmentStore store =
        new LoadBalancingAssignmentStore(new Variables(), LogChannel.GENERAL);

    LoadBalancingAssignment assignment = new LoadBalancingAssignment();
    assignment.setExecutionId("exec-1");
    assignment.setRunConfigurationName("pool");
    assignment.setExecutorName("demo");
    assignment.setServerName("server-a");
    assignment.setAttempt(2);
    assignment.setStatus(LoadBalancingAssignment.STATUS_RUNNING);
    assignment.setAlgorithm(LoadBalancingAlgorithm.EVEN_LOAD.getCode());

    store.save(folder.toString(), assignment);
    LoadBalancingAssignment loaded = store.load(folder.toString(), "pool", "exec-1");

    assertNotNull(loaded);
    assertEquals("exec-1", loaded.getExecutionId());
    assertEquals("server-a", loaded.getServerName());
    assertEquals(2, loaded.getAttempt());
    assertEquals(LoadBalancingAssignment.STATUS_RUNNING, loaded.getStatus());
  }

  @Test
  void sanitizeReplacesUnsafeCharacters() {
    assertEquals("a_b.hpl", LoadBalancingAssignmentStore.sanitize("a/b.hpl"));
    assertEquals("unnamed", LoadBalancingAssignmentStore.sanitize(""));
  }
}
