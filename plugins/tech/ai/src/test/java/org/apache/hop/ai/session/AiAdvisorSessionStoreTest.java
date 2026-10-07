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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.apache.hop.ai.advisor.AiAdvisorLocations;
import org.apache.hop.ai.advisor.AiAdvisorOpenRequest;
import org.apache.hop.ai.config.HopAiConfigSingleton;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.IAuditManager;
import org.apache.hop.history.local.LocalAuditManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AiAdvisorSessionStoreTest {

  @Test
  void exitingHopGuiStopsAQuestionThatStillWaits() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    AiAdvisorSession session = new AiAdvisorSession();
    AiAdvisorTurn turn = new AiAdvisorTurn();
    turn.setUserPrompt("Why did it fail?");
    session.addTurn(turn);
    session.setWorking(true);
    store.add(session);

    store.stopWaitingQuestions();

    assertFalse(session.isWorking());
    assertTrue(session.isCancelled(), "a late answer must not be recorded");
    assertTrue(turn.getErrorMessage().contains("Hop GUI was closed"), turn.getErrorMessage());
  }

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
  void savingANewPipelineKeepsItsSession() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    Artifact pipeline = new Artifact(null);
    AiAdvisorSession first = store.open(pipelineRequest(pipeline, "New pipeline"));

    // The first save gives the pipeline a file and with it a new name.
    pipeline.filename = "/project/orders.hpl";
    AiAdvisorSession second = store.open(pipelineRequest(pipeline, "orders"));

    assertSame(first, second);
    assertEquals(1, store.getSessions().size());
    assertEquals("orders", second.getTitle());
    assertEquals("orders", second.getArtifactName());
  }

  @Test
  void pipelinesWithTheSameNameGetTheirOwnSession() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    AiAdvisorSession first =
        store.open(pipelineRequest(new Artifact("/project/a/orders.hpl"), "orders"));
    AiAdvisorSession second =
        store.open(pipelineRequest(new Artifact("/project/b/orders.hpl"), "orders"));
    AiAdvisorSession unsaved1 = store.open(pipelineRequest(new Artifact(null), "New pipeline"));
    AiAdvisorSession unsaved2 = store.open(pipelineRequest(new Artifact(null), "New pipeline"));

    assertNotSame(first, second);
    assertNotSame(unsaved1, unsaved2);
    assertEquals(4, store.getSessions().size());
  }

  @Test
  void reopeningAFileKeepsItsSession() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    AiAdvisorSession first =
        store.open(pipelineRequest(new Artifact("/project/orders.hpl"), "orders"));
    // Closing and reopening the tab loads a new pipeline object for the same file.
    Artifact reopened = new Artifact("/project/orders.hpl");
    AiAdvisorSession second = store.open(pipelineRequest(reopened, "orders"));

    assertSame(first, second);
    assertSame(reopened, second.getArtifact());
  }

  @Test
  void openingOnThePipelineClearsAnEarlierTransformFocus() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    Artifact pipeline = new Artifact("/project/orders.hpl");
    AiAdvisorOpenRequest onTransform = pipelineRequest(pipeline, "orders");
    onTransform.setFocusNodeName("Table input");
    AiAdvisorSession session = store.open(onTransform);
    assertEquals("Table input", session.getFocusNodeName());

    store.open(pipelineRequest(pipeline, "orders"));
    assertEquals("", session.getFocusNodeName());
  }

  private static AiAdvisorOpenRequest pipelineRequest(Artifact artifact, String name) {
    AiAdvisorOpenRequest request = new AiAdvisorOpenRequest();
    request.setAdvisorPluginId("pipeline-advisor");
    request.setLocation(AiAdvisorLocations.PIPELINE_GRAPH);
    request.setArtifact(artifact);
    request.setArtifactName(name);
    request.setTitle(name);
    return request;
  }

  private static final class Artifact implements IHasFilename {
    private String filename;

    private Artifact(String filename) {
      this.filename = filename;
    }

    @Override
    public String getFilename() {
      return filename;
    }
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
  void openCopiesAttributesAndReuseMerges() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    AiAdvisorOpenRequest request = new AiAdvisorOpenRequest();
    request.setAdvisorPluginId("data-vault-advisor");
    request.setLocation("data-vault-graph");
    request.setArtifactName("sales.dv");
    request.getAttributes().put("catalog", "sales");
    AiAdvisorSession session = store.open(request);
    assertEquals("sales", session.getAttributes().get("catalog"));

    AiAdvisorOpenRequest reuse = new AiAdvisorOpenRequest();
    reuse.setAdvisorPluginId("data-vault-advisor");
    reuse.setLocation("data-vault-graph");
    reuse.setArtifactName("sales.dv");
    reuse.getAttributes().put("focusHub", "H_CUSTOMER");
    reuse.getAttributes().put("catalog", "marketing");
    AiAdvisorSession same = store.open(reuse);
    assertSame(session, same);
    assertEquals("marketing", same.getAttributes().get("catalog"));
    assertEquals("H_CUSTOMER", same.getAttributes().get("focusHub"));
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

  @Test
  void sessionsBelongToTheProjectTheyWereStartedIn() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    String[] project = {"sales"};
    store.scope = () -> project[0];
    AiAdvisorSession sales =
        store.open(pipelineRequest(new Artifact("/sales/orders.hpl"), "orders"));

    project[0] = "finance";
    assertTrue(store.getSessions().isEmpty(), "the other project's sessions are not shown");
    assertNull(store.getActiveSession());
    AiAdvisorSession finance =
        store.open(pipelineRequest(new Artifact("/finance/orders.hpl"), "orders"));
    assertNotSame(sales, finance);

    project[0] = "sales";
    assertEquals(List.of(sales), store.getSessions());
    assertSame(sales, store.getActiveSession(), "falls back to a session of this project");
  }

  @Test
  void removingASessionCancelsItsRequest() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    AiAdvisorSession session = store.open(pipelineRequest(new Artifact("/p.hpl"), "p"));
    session.setWorking(true);
    store.remove(session.getId());
    assertTrue(session.isCancelled());
  }

  @Test
  void closingTheFileReleasesItAndReopeningFindsTheConversation() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    Artifact open = new Artifact("/project/orders.hpl");
    AiAdvisorSession session = store.open(pipelineRequest(open, "orders"));
    session.setLogSupplier(() -> "log");

    store.release(open);
    assertNull(session.getArtifact(), "a closed file is not kept in memory");
    assertNull(session.getLogSupplier());

    Artifact reopened = new Artifact("/project/orders.hpl");
    assertSame(session, store.open(pipelineRequest(reopened, "orders")));
    assertSame(reopened, session.getArtifact());
  }

  @Test
  void anUnlinkedSessionCanBeLinkedToTheOpenFile() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    AiAdvisorOpenRequest general = new AiAdvisorOpenRequest();
    general.setReuseExisting(false);
    general.setTitle("New session");
    AiAdvisorSession session = store.open(general);
    assertEquals(AiAdvisorLocations.PERSPECTIVE, session.getLocation());

    Artifact pipeline = new Artifact("/project/orders.hpl");
    store.link(session, pipelineRequest(pipeline, "orders"));
    assertSame(pipeline, session.getArtifact());
    assertEquals(AiAdvisorLocations.PIPELINE_GRAPH, session.getLocation());
    assertEquals("pipeline-advisor", session.getAdvisorPluginId());
    assertEquals("orders", session.getTitle());
  }

  @Test
  void switchingKeepConversationsOnLaterKeepsTheSavedOnes(@TempDir Path audit) throws Exception {
    IAuditManager original = AuditManager.getInstance().getActiveAuditManager();
    boolean keep = HopAiConfigSingleton.getConfig().isKeepConversations();
    AuditManager.getInstance().setActiveAuditManager(new LocalAuditManager(audit.toString()));
    try {
      AiAdvisorSession saved = new AiAdvisorSession();
      saved.setTitle("saved");
      AiAdvisorSessionArchive.save("project", List.of(saved));

      AiAdvisorSessionStore store = new AiAdvisorSessionStore();
      store.persistent = true;
      store.scope = () -> "project";
      HopAiConfigSingleton.getConfig().setKeepConversations(false);
      assertTrue(store.getSessions().isEmpty(), "nothing is read while the option is off");
      AiAdvisorSession fresh = new AiAdvisorSession();
      fresh.setTitle("fresh");
      store.add(fresh);

      HopAiConfigSingleton.getConfig().setKeepConversations(true);
      assertEquals(2, store.getSessions().size(), "the saved session is read now");
      store.saveNow();
      assertEquals(2, AiAdvisorSessionArchive.load("project").size(), "and is not overwritten");
    } finally {
      HopAiConfigSingleton.getConfig().setKeepConversations(keep);
      AuditManager.getInstance().setActiveAuditManager(original);
    }
  }
}
