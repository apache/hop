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

package org.apache.hop.execution.caching;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionDataSetMeta;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionSelector;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BaseCachingExecutionInfoLocationTest {

  @BeforeAll
  static void initLogging() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  @Test
  void missingMaxCacheAgeKeepsTheOneDayDefault() throws Exception {
    FakeLocation location = new FakeLocation();
    assertEquals(BaseCachingExecutionInfoLocation.LEGACY_MAX_CACHE_AGE, location.getMaxCacheAge());

    location.initialize(new Variables(), null);
    try {
      assertEquals(86_400_000, location.maxAge);
    } finally {
      location.close();
    }
  }

  @Test
  void explicitMaxCacheAgeIsUsed() throws Exception {
    FakeLocation location = new FakeLocation();
    location.setMaxCacheAge(BaseCachingExecutionInfoLocation.NEW_LOCATION_MAX_CACHE_AGE);

    location.initialize(new Variables(), null);
    try {
      assertEquals(600_000, location.maxAge);
    } finally {
      location.close();
    }
  }

  @Test
  void loggingTextAppendsDeltasAndKeepsTheNewestCharacters() throws Exception {
    FakeLocation location = new FakeLocation();
    String id = UUID.randomUUID().toString();
    location.registerExecution(pipeline(id, "Pipeline"));

    ExecutionState first = pipelineState(id, "hello", 5);
    location.updateExecutionState(first);
    assertEquals("hello", location.getExecutionState(id).getLoggingText());

    location.updateExecutionState(pipelineState(id, " world", 9));
    assertEquals("hello world", location.getExecutionState(id).getLoggingText());

    location.updateExecutionState(pipelineState(id, "replaced", null));
    assertEquals("replaced", location.getExecutionState(id).getLoggingText());

    String head = "H".repeat(BaseCachingExecutionInfoLocation.MAX_CACHED_LOGGING_TEXT_CHARS);
    String tail = "T".repeat(32);
    location.updateExecutionState(pipelineState(id, head, 10));
    location.updateExecutionState(pipelineState(id, tail, 11));
    String capped = location.getExecutionState(id).getLoggingText();
    assertEquals(BaseCachingExecutionInfoLocation.MAX_CACHED_LOGGING_TEXT_CHARS, capped.length());
    assertTrue(capped.endsWith(tail));
    assertTrue(
        capped.startsWith(
            "H"
                .repeat(
                    BaseCachingExecutionInfoLocation.MAX_CACHED_LOGGING_TEXT_CHARS
                        - tail.length())));
  }

  @Test
  void lookupMissDoesNotRefreshLastRead() throws Exception {
    FakeLocation location = new FakeLocation();
    String id = UUID.randomUUID().toString();
    location.registerExecution(pipeline(id, "Kept"));
    Date readAt = new Date(1_000L);
    location.getCache().get(id).setLastRead(readAt);

    assertNull(location.findCacheEntry("missing"));
    assertEquals(readAt, location.getCache().get(id).getLastRead());

    assertNotNull(location.findCacheEntry(id));
    assertTrue(location.getCache().get(id).getLastRead().after(readAt));
  }

  @Test
  void evictionKeepsADirtyEntryWhenPersistFails() throws Exception {
    FakeLocation location = new FakeLocation();
    location.maxSize = 1;
    String kept = UUID.randomUUID().toString();
    String added = UUID.randomUUID().toString();
    location.registerExecution(pipeline(kept, "Kept"));
    location.getCache().get(kept).setDirty(true);
    location.failPersist = true;

    assertThrows(HopException.class, () -> location.registerExecution(pipeline(added, "Added")));

    assertEquals(2, location.getCache().size());
    assertTrue(location.getCache().containsKey(kept));
    assertTrue(location.getCache().containsKey(added));
  }

  @Test
  void closeLeavesUnpersistedEntriesAndRetriesThem() throws Exception {
    FakeLocation location = new FakeLocation();
    String id = UUID.randomUUID().toString();
    location.registerExecution(pipeline(id, "Retry"));
    location.getCache().get(id).setDirty(true);
    location.failPersist = true;

    assertThrows(HopException.class, location::close);
    assertTrue(location.getCache().containsKey(id));

    location.failPersist = false;
    location.close();
    assertTrue(location.getCache().isEmpty());
    assertTrue(location.persisted.contains(id));
  }

  @Test
  void testParentChildLinkResolution() throws Exception {
    FakeLocation location = new FakeLocation();
    String workflowId = "wf-1";
    String actionId = "act-1";
    String pipelineId = "pipe-1";

    Execution wf = new Execution();
    wf.setId(workflowId);
    wf.setName("Workflow");
    wf.setExecutionType(ExecutionType.Workflow);
    wf.setRegistrationDate(new Date());

    Execution action = new Execution();
    action.setId(actionId);
    action.setName("ActionPipeline");
    action.setParentId(workflowId);
    action.setExecutionType(ExecutionType.Action);
    action.setRegistrationDate(new Date());

    Execution pipe = new Execution();
    pipe.setId(pipelineId);
    pipe.setName("Pipeline");
    pipe.setParentId(workflowId);
    pipe.setLaunchedById(actionId);
    pipe.setExecutionType(ExecutionType.Pipeline);
    pipe.setRegistrationDate(new Date());

    location.registerExecution(wf);
    location.registerExecution(action);
    location.registerExecution(pipe);

    assertEquals(workflowId, location.findParentId(pipelineId));
    assertEquals(workflowId, location.findParentId(actionId));
    assertNull(location.findParentId(workflowId));

    Execution retrievedAction = location.getExecution(actionId);
    assertNotNull(retrievedAction);
    assertEquals(actionId, retrievedAction.getId());
    assertEquals(ExecutionType.Action, retrievedAction.getExecutionType());
    assertEquals(java.util.List.of(pipelineId), retrievedAction.getChildExecutionIds());

    java.util.List<Execution> actionChildren = location.findExecutions(actionId);
    assertEquals(1, actionChildren.size());
    assertEquals(pipelineId, actionChildren.get(0).getId());

    java.util.List<Execution> wfChildren = location.findExecutions(workflowId);
    assertTrue(wfChildren.stream().anyMatch(e -> e.getId().equals(actionId)));
  }

  @Test
  void childLinkKeepsANewerStoredParentState() throws Exception {
    FakeLocation location = new FakeLocation();
    CacheEntry running = parentEntry("wf", "running", new Date(1_000L), null);
    CacheEntry finished = parentEntry("wf", "Finished", new Date(5_000L), new Date(5_000L));
    finished.setInternalId("doc-9");
    location.scriptedLoads.add(running);
    location.scriptedLoads.add(finished);

    location.registerExecution(launchedPipeline("pipe", "wf", "action"));

    assertNull(location.getCache().get("wf"));
    assertSame(finished, location.lastPersisted);
    assertEquals("Finished", location.lastPersisted.getExecutionState().getStatusDescription());
    assertNotNull(location.lastPersisted.getExecutionState().getExecutionEndDate());
    assertEquals("doc-9", location.lastPersisted.getInternalId());
    Execution launcher = location.lastPersisted.peekChildExecution("action");
    assertNotNull(launcher);
    assertEquals(List.of("pipe"), launcher.getChildExecutionIds());
  }

  @Test
  void childLinkKeepsTheLoadedParentWhenTheStoredCopyIsNotNewer() throws Exception {
    FakeLocation location = new FakeLocation();
    CacheEntry loaded = parentEntry("wf", "Finished", new Date(5_000L), new Date(5_000L));
    CacheEntry stored = parentEntry("wf", "Finished", new Date(5_000L), new Date(5_000L));
    stored.setInternalId("doc-1");
    location.scriptedLoads.add(loaded);
    location.scriptedLoads.add(stored);

    location.registerExecution(launchedPipeline("pipe", "wf", "action"));

    assertSame(loaded, location.lastPersisted);
    assertEquals("doc-1", location.lastPersisted.getInternalId());
    assertEquals(
        List.of("pipe"),
        location.lastPersisted.peekChildExecution("action").getChildExecutionIds());
  }

  @Test
  void childLinkDoesNotReplaceAParentAlreadyInThisLocation() throws Exception {
    FakeLocation location = new FakeLocation();
    Execution workflow = pipeline("wf", "Workflow");
    workflow.setExecutionType(ExecutionType.Workflow);
    location.registerExecution(workflow);
    ExecutionState running = new ExecutionState();
    running.setId("wf");
    running.setExecutionType(ExecutionType.Workflow);
    running.setStatusDescription("running");
    running.setUpdateTime(new Date(1_000L));
    location.updateExecutionState(running);

    CacheEntry finished = parentEntry("wf", "Finished", new Date(9_000L), new Date(9_000L));
    location.scriptedLoads.add(finished);
    int loadsBeforeChild = location.loadCount;

    location.registerExecution(launchedPipeline("pipe", "wf", "action"));

    assertEquals(loadsBeforeChild, location.loadCount);
    assertEquals("running", location.getExecutionState("wf").getStatusDescription());
    assertEquals(List.of("pipe"), location.getExecution("action").getChildExecutionIds());
  }

  @Test
  void attachOnlyLocationKeepsAFinishedStateWhenItLoadedARunningCopy() throws Exception {
    FakeLocation attach = new FakeLocation();
    CacheEntry running = parentEntry("wf", "running", new Date(9_000L), null);
    running.setInternalId("doc-1");
    CacheEntry finished = parentEntry("wf", "Finished", new Date(1_000L), new Date(5_000L));
    finished.setInternalId("doc-1");
    finished.getExecutionState().setLoggingText("done");
    finished.addChildExecution(action("kept-action"));
    attach.scriptedLoads.add(running);
    attach.scriptedLoads.add(finished);

    attach.registerExecution(action("action"));
    assertTrue(attach.getCache().get("wf").isLoadedCopy());
    assertEquals(1, attach.loadCount);

    attach.close();

    assertEquals(2, attach.loadCount);
    assertEquals("Finished", attach.lastPersisted.getExecutionState().getStatusDescription());
    assertNotNull(attach.lastPersisted.getExecutionState().getExecutionEndDate());
    assertEquals("done", attach.lastPersisted.getExecutionState().getLoggingText());
    assertEquals("doc-1", attach.lastPersisted.getInternalId());
    assertNotNull(attach.lastPersisted.peekChildExecution("action"));
    assertNotNull(attach.lastPersisted.peekChildExecution("kept-action"));
  }

  @Test
  void attachOnlyLocationKeepsAFinishedStateWhenItLoadedBeforeAnyState() throws Exception {
    FakeLocation attach = new FakeLocation();
    CacheEntry loaded = new CacheEntry();
    loaded.setId("wf");
    loaded.setName("wf");
    Execution workflow = new Execution();
    workflow.setId("wf");
    workflow.setName("wf");
    workflow.setExecutionType(ExecutionType.Workflow);
    workflow.setRegistrationDate(new Date(1_000L));
    loaded.setExecution(workflow);
    loaded.setInternalId("doc-2");
    CacheEntry finished = parentEntry("wf", "Finished", new Date(5_000L), new Date(5_000L));
    finished.setInternalId("doc-2");
    attach.scriptedLoads.add(loaded);
    attach.scriptedLoads.add(finished);

    attach.registerExecution(action("action"));
    attach.close();

    assertEquals("Finished", attach.lastPersisted.getExecutionState().getStatusDescription());
    assertNotNull(attach.lastPersisted.getExecutionState().getExecutionEndDate());
    assertNotNull(attach.lastPersisted.peekChildExecution("action"));
  }

  @Test
  void registeringLocationWritesItsOwnFinishedStateWithoutReloading() throws Exception {
    FakeLocation location = new FakeLocation();
    Execution workflow = pipeline("wf", "Workflow");
    workflow.setExecutionType(ExecutionType.Workflow);
    location.registerExecution(workflow);
    ExecutionState finished = new ExecutionState();
    finished.setId("wf");
    finished.setExecutionType(ExecutionType.Workflow);
    finished.setStatusDescription("Finished");
    finished.setUpdateTime(new Date(5_000L));
    finished.setExecutionEndDate(new Date(5_000L));
    location.updateExecutionState(finished);

    location.close();

    assertEquals(0, location.loadCount);
    assertEquals("Finished", location.lastPersisted.getExecutionState().getStatusDescription());
    assertNotNull(location.lastPersisted.getExecutionState().getExecutionEndDate());
  }

  @Test
  void aFinishedParentStateIsNewerThanARunningOne() {
    CacheEntry running = parentEntry("wf", "running", new Date(9_000L), null);
    CacheEntry finished = parentEntry("wf", "Finished", new Date(1_000L), new Date(1_000L));

    assertTrue(BaseCachingExecutionInfoLocation.isNewerParentState(finished, running));
    assertFalse(BaseCachingExecutionInfoLocation.isNewerParentState(running, finished));
    assertTrue(
        BaseCachingExecutionInfoLocation.isNewerParentState(
            parentEntry("wf", "running", new Date(9_000L), null),
            parentEntry("wf", "running", new Date(1_000L), null)));
    assertFalse(
        BaseCachingExecutionInfoLocation.isNewerParentState(
            parentEntry("wf", "running", new Date(1_000L), null),
            parentEntry("wf", "running", new Date(1_000L), null)));
  }

  @Test
  void testAddExecutionDataRetainsNonEmptySamples() {
    CacheEntry entry = new CacheEntry();
    entry.setId("pipe-1");

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("col1"));

    RowBuffer bufferWithRows = new RowBuffer(rowMeta);
    bufferWithRows.addRow(new Object[] {"value1"});

    ExecutionData data1 = new ExecutionData();
    data1.setOwnerId("all-transforms");
    data1.getDataSets().put("set1", bufferWithRows);
    data1
        .getSetMetaData()
        .put("set1", new ExecutionDataSetMeta("set1", "log1", "transform1", "0", "desc1"));

    entry.addExecutionData(data1);

    assertEquals(1, entry.getExecutionData("all-transforms").getDataSets().get("set1").size());

    // Subsequent tick with empty rows
    RowBuffer emptyBuffer = new RowBuffer(rowMeta);
    ExecutionData data2 = new ExecutionData();
    data2.setOwnerId("all-transforms");
    data2.getDataSets().put("set1", emptyBuffer);
    data2
        .getSetMetaData()
        .put("set1", new ExecutionDataSetMeta("set1", "log1", "transform1", "0", "desc1"));

    entry.addExecutionData(data2);

    // Verify samples were preserved
    assertEquals(1, entry.getExecutionData("all-transforms").getDataSets().get("set1").size());
  }

  private static Execution action(String id) {
    Execution execution = new Execution();
    execution.setId(id);
    execution.setName(id);
    execution.setParentId("wf");
    execution.setExecutionType(ExecutionType.Action);
    execution.setRegistrationDate(new Date(1_000L));
    return execution;
  }

  private static Execution launchedPipeline(String id, String parentId, String launchedById) {
    Execution execution = pipeline(id, id);
    execution.setParentId(parentId);
    execution.setLaunchedById(launchedById);
    return execution;
  }

  private static CacheEntry parentEntry(String id, String status, Date updateTime, Date endDate) {
    Execution execution = new Execution();
    execution.setId(id);
    execution.setName(id);
    execution.setExecutionType(ExecutionType.Workflow);
    execution.setRegistrationDate(new Date(1_000L));
    CacheEntry entry = new CacheEntry();
    entry.setId(id);
    entry.setExecution(execution);
    ExecutionState state = new ExecutionState();
    state.setId(id);
    state.setExecutionType(ExecutionType.Workflow);
    state.setStatusDescription(status);
    state.setUpdateTime(updateTime);
    state.setExecutionEndDate(endDate);
    entry.setExecutionState(state);
    return entry;
  }

  private static Execution pipeline(String id, String name) {
    Execution execution = new Execution();
    execution.setId(id);
    execution.setName(name);
    execution.setExecutionType(ExecutionType.Pipeline);
    execution.setRegistrationDate(new Date());
    return execution;
  }

  private static ExecutionState pipelineState(
      String id, String loggingText, Integer lastLogLineNr) {
    ExecutionState state = new ExecutionState();
    state.setId(id);
    state.setExecutionType(ExecutionType.Pipeline);
    state.setLoggingText(loggingText);
    state.setLastLogLineNr(lastLogLineNr);
    return state;
  }

  private static final class FakeLocation extends BaseCachingExecutionInfoLocation {
    private boolean failPersist;
    private final Set<String> persisted = new HashSet<>();
    private final List<CacheEntry> scriptedLoads = new ArrayList<>();
    private int scriptedLoadIndex;
    private CacheEntry lastPersisted;
    private int loadCount;
    private String pluginId;
    private String pluginName;

    @Override
    public String getPluginId() {
      return pluginId;
    }

    @Override
    public void setPluginId(String pluginId) {
      this.pluginId = pluginId;
    }

    @Override
    public String getPluginName() {
      return pluginName;
    }

    @Override
    public void setPluginName(String pluginName) {
      this.pluginName = pluginName;
    }

    @Override
    public BaseCachingExecutionInfoLocation clone() {
      return new FakeLocation();
    }

    @Override
    protected void persistCacheEntry(CacheEntry cacheEntry) throws HopException {
      if (failPersist) {
        throw new HopException("persist failed for " + cacheEntry.getId());
      }
      persisted.add(cacheEntry.getId());
      lastPersisted = cacheEntry;
      cacheEntry.setDirty(false);
      cacheEntry.setLastWritten(new Date());
    }

    @Override
    protected CacheEntry loadCacheEntry(String executionId) {
      loadCount++;
      if (scriptedLoadIndex >= scriptedLoads.size()) {
        return null;
      }
      return scriptedLoads.get(scriptedLoadIndex++);
    }

    @Override
    protected void deleteCacheEntry(CacheEntry cacheEntry) {
      // Not used by these tests.
    }

    @Override
    protected void retrieveIds(
        boolean includeChildren, Set<DatedId> ids, int limit, IExecutionSelector selector) {
      // Not used by these tests.
    }
  }
}
