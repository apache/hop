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

package org.apache.hop.ai.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ai.advisor.AiAdvisorLocations;
import org.apache.hop.ai.advisor.AiAdvisorOpenRequest;
import org.junit.jupiter.api.Test;

class AiAdvisorSessionStoreTest {

  @Test
  void openReusesSessionForSameAdvisorAndArtifact() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    AiAdvisorOpenRequest request = new AiAdvisorOpenRequest();
    request.setAdvisorPluginId("pipeline-advisor");
    request.setLocation(AiAdvisorLocations.PIPELINE_GRAPH);
    request.setArtifactName("orders.hpl");
    request.setTitle("Orders pipeline");

    AiAdvisorSession first = store.open(request);
    AiAdvisorSession second = store.open(request);
    assertSame(first, second);
    assertEquals(1, store.getSessions().size());
  }

  @Test
  void differentTopicsStaySeparate() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    AiAdvisorOpenRequest pipeline = new AiAdvisorOpenRequest();
    pipeline.setAdvisorPluginId("pipeline-advisor");
    pipeline.setLocation(AiAdvisorLocations.PIPELINE_GRAPH);
    pipeline.setArtifactName("orders.hpl");

    AiAdvisorOpenRequest workflow = new AiAdvisorOpenRequest();
    workflow.setAdvisorPluginId("workflow-advisor");
    workflow.setLocation(AiAdvisorLocations.WORKFLOW_GRAPH);
    workflow.setArtifactName("load.hwf");

    store.open(pipeline);
    store.open(workflow);
    assertEquals(2, store.getSessions().size());
  }

  @Test
  void removeKeepsRemainingActive() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    AiAdvisorOpenRequest firstReq = new AiAdvisorOpenRequest();
    firstReq.setReuseExisting(false);
    firstReq.setTitle("One");
    AiAdvisorOpenRequest secondReq = new AiAdvisorOpenRequest();
    secondReq.setReuseExisting(false);
    secondReq.setTitle("Two");
    AiAdvisorSession first = store.open(firstReq);
    AiAdvisorSession second = store.open(secondReq);
    store.remove(second.getId());
    assertEquals(first.getId(), store.getActiveSessionId());
    assertEquals(1, store.getSessions().size());
  }

  @Test
  void customLocationIsFirstClass() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    AiAdvisorOpenRequest vault = new AiAdvisorOpenRequest();
    vault.setAdvisorPluginId("data-vault-advisor");
    vault.setLocation("data-vault-graph");
    vault.setAreaLabel("Data Vault");
    vault.setArtifactName("sales.dv");
    AiAdvisorSession session = store.open(vault);
    assertEquals("data-vault-graph", session.getLocation());
    assertEquals("Data Vault", session.areaLabel());
    assertSame(session, store.open(vault));
  }

  @Test
  void areaLabelsGroupWork() {
    AiAdvisorSession session = new AiAdvisorSession();
    session.setAreaLabel("Pipelines");
    assertEquals("Pipelines", session.areaLabel());
    session.setAreaLabel("Workflows");
    assertEquals("Workflows", session.areaLabel());
    session.setAreaLabel("");
    assertEquals("General", session.areaLabel());
    session.setTitle("Tuning");
    assertEquals("Tuning", session.displayTitle());
  }

  @Test
  void listenersFireOnChange() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    int[] count = {0};
    store.addListener(() -> count[0]++);
    store.open(new AiAdvisorOpenRequest());
    assertTrue(count[0] > 0);
  }

  @Test
  void nestedFireChangedDoesNotRecurse() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    int[] count = {0};
    store.addListener(
        () -> {
          count[0]++;
          store.fireChanged();
        });
    store.fireChanged();
    assertEquals(1, count[0]);
  }
}
