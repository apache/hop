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

package org.apache.hop.execution.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.execution.DefaultExecutionSelector;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionSelector;
import org.apache.hop.execution.LastPeriod;
import org.apache.hop.execution.caching.CacheEntry;
import org.apache.hop.execution.caching.DatedId;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CachingDatabaseExecutionInfoLocationTest {

  private CachingDatabaseExecutionInfoLocation location;
  private DatabaseMeta databaseMeta;
  private Variables variables;
  private MemoryMetadataProvider metadataProvider;

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
  }

  @BeforeEach
  void setUp() throws Exception {
    variables = new Variables();
    metadataProvider = new MemoryMetadataProvider();

    // DB_CLOSE_DELAY=-1 keeps the in-memory DB alive after the DDL connection closes
    databaseMeta =
        new DatabaseMeta(
            "h2-exec",
            "H2",
            "Native",
            "",
            "mem:exec_info_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1",
            "",
            "",
            "");
    databaseMeta.setSupportsBooleanDataType(true);
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);

    location = new CachingDatabaseExecutionInfoLocation();
    location.setConnectionName("h2-exec");
    location.setTableName(CachingDatabaseExecutionInfoLocation.DEFAULT_TABLE_NAME);
    location.setPersistenceDelay("60000");
    location.setMaxCacheAge("86400000");
    location.setDatabaseMeta(databaseMeta);

    // Create table using generated DDL
    String ddl = location.buildDdl(variables);
    assertTrue(ddl.toUpperCase().contains("CREATE"));
    assertTrue(
        ddl.contains(CachingDatabaseExecutionInfoLocation.DEFAULT_TABLE_NAME)
            || ddl.contains("hop_executions"));

    Database db =
        new Database(
            new LoggingObject("CachingDatabaseExecutionInfoLocationTest"), variables, databaseMeta);
    db.connect();
    try {
      db.execStatements(ddl);
    } finally {
      db.disconnect();
    }

    location.initialize(variables, metadataProvider);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (location != null) {
      location.close();
    }
  }

  @Test
  void persistLoadAndDeleteRoundTrip() throws Exception {
    String id = UUID.randomUUID().toString();
    CacheEntry entry = sampleEntry(id, "MyPipeline", ExecutionType.Pipeline, false, "Finished");

    location.persistCacheEntry(entry);

    CacheEntry loaded = location.loadCacheEntry(id);
    assertNotNull(loaded);
    assertEquals(id, loaded.getId());
    assertEquals("MyPipeline", loaded.getName());
    assertNotNull(loaded.getExecution());
    assertEquals(ExecutionType.Pipeline, loaded.getExecution().getExecutionType());
    assertNotNull(loaded.getExecutionState());
    assertFalse(loaded.getExecutionState().isFailed());

    location.deleteCacheEntry(entry);
    assertNull(location.loadCacheEntry(id));
  }

  @Test
  void upsertUpdatesFilterColumnsAndJson() throws Exception {
    String id = UUID.randomUUID().toString();
    CacheEntry entry = sampleEntry(id, "FlowA", ExecutionType.Workflow, false, "Running");
    location.persistCacheEntry(entry);

    entry.getExecutionState().setFailed(true);
    entry.getExecutionState().setStatusDescription("Finished");
    entry.getExecutionState().setExecutionEndDate(new Date());
    entry.setName("FlowA");
    location.persistCacheEntry(entry);

    CacheEntry loaded = location.loadCacheEntry(id);
    assertNotNull(loaded);
    assertTrue(loaded.getExecutionState().isFailed());
    assertTrue(loaded.getExecutionState().getStatusDescription().startsWith("Finished"));

    // Still a single row: retrieveIds returns one id
    Set<DatedId> ids = new HashSet<>();
    location.retrieveIds(false, ids, 100, IExecutionSelector.ALL);
    assertEquals(1, ids.size());
  }

  @Test
  void retrieveIdsFiltersByTypeAndFailed() throws Exception {
    location.persistCacheEntry(
        sampleEntry(UUID.randomUUID().toString(), "p1", ExecutionType.Pipeline, false, "Finished"));
    location.persistCacheEntry(
        sampleEntry(UUID.randomUUID().toString(), "p2", ExecutionType.Pipeline, true, "Finished"));
    location.persistCacheEntry(
        sampleEntry(UUID.randomUUID().toString(), "w1", ExecutionType.Workflow, false, "Finished"));

    Set<DatedId> pipelineIds = new HashSet<>();
    location.retrieveIds(
        false,
        pipelineIds,
        100,
        new DefaultExecutionSelector(
            false, false, false, false, false, true, null, LastPeriod.ONE_YEAR));
    assertEquals(2, pipelineIds.size());

    Set<DatedId> failedIds = new HashSet<>();
    location.retrieveIds(
        false,
        failedIds,
        100,
        new DefaultExecutionSelector(
            false, true, false, false, false, false, null, LastPeriod.ONE_YEAR));
    assertEquals(1, failedIds.size());

    Set<DatedId> workflowIds = new HashSet<>();
    location.retrieveIds(
        false,
        workflowIds,
        100,
        new DefaultExecutionSelector(
            false, false, false, false, true, false, null, LastPeriod.ONE_YEAR));
    assertEquals(1, workflowIds.size());
  }

  @Test
  void retrieveIdsFiltersByName() throws Exception {
    location.persistCacheEntry(
        sampleEntry(
            UUID.randomUUID().toString(), "AlphaPipe", ExecutionType.Pipeline, false, "Finished"));
    location.persistCacheEntry(
        sampleEntry(
            UUID.randomUUID().toString(), "BetaPipe", ExecutionType.Pipeline, false, "Finished"));

    Set<DatedId> ids = new HashSet<>();
    location.retrieveIds(
        false,
        ids,
        100,
        new DefaultExecutionSelector(
            false, false, false, false, false, false, "alpha", LastPeriod.ONE_YEAR));
    assertEquals(1, ids.size());
  }

  @Test
  void registerExecutionPersistsParent() throws Exception {
    String id = UUID.randomUUID().toString();
    Execution execution = new Execution();
    execution.setId(id);
    execution.setName("Registered");
    execution.setExecutionType(ExecutionType.Pipeline);
    execution.setExecutionStartDate(new Date());
    execution.setRegistrationDate(new Date());

    location.registerExecution(execution);

    CacheEntry loaded = location.loadCacheEntry(id);
    assertNotNull(loaded);
    assertEquals("Registered", loaded.getName());
  }

  @Test
  void getExecutionIdsReturnsNewestFirst() throws Exception {
    String oldId = UUID.randomUUID().toString();
    String newId = UUID.randomUUID().toString();

    CacheEntry oldEntry = sampleEntry(oldId, "Old", ExecutionType.Pipeline, false, "Finished");
    oldEntry.getExecution().setExecutionStartDate(new Date(System.currentTimeMillis() - 60_000));
    location.persistCacheEntry(oldEntry);

    CacheEntry newEntry = sampleEntry(newId, "New", ExecutionType.Pipeline, false, "Finished");
    newEntry.getExecution().setExecutionStartDate(new Date());
    location.persistCacheEntry(newEntry);

    // Clear memory cache so list comes from DB
    location.clearCaches();

    List<String> ids = location.getExecutionIds(false, 10);
    assertTrue(ids.size() >= 2);
    assertEquals(newId, ids.get(0));
  }

  @Test
  void persistReopensAClosedJdbcConnection() throws Exception {
    String id = UUID.randomUUID().toString();
    location.persistCacheEntry(sampleEntry(id, "Before", ExecutionType.Workflow, false, "Running"));

    Connection closed = location.getDatabase().getConnection();
    closed.close();
    assertTrue(closed.isClosed());

    location.persistCacheEntry(sampleEntry(id, "Before", ExecutionType.Workflow, true, "Finished"));

    Connection reopened = location.getDatabase().getConnection();
    assertNotSame(closed, reopened);
    assertFalse(reopened.isClosed());
    assertNull(location.getDatabase().getConnectionGroup());
    assertTrue(reopened.getAutoCommit());

    CacheEntry loaded = location.loadCacheEntry(id);
    assertNotNull(loaded);
    assertTrue(loaded.getExecutionState().isFailed());
    assertTrue(loaded.getExecutionState().getStatusDescription().startsWith("Finished"));
  }

  @Test
  void initializeCancelsThePreviousTimerAndDropsTheClosedConnection() throws Exception {
    Timer firstTimer = location.getCacheTimer();
    assertNotNull(firstTimer);
    Connection firstConnection = location.getDatabase().getConnection();
    firstConnection.close();

    location.initialize(variables, metadataProvider);

    assertThrows(
        IllegalStateException.class,
        () ->
            firstTimer.schedule(
                new TimerTask() {
                  @Override
                  public void run() {
                    // Cancelled timers reject new tasks.
                  }
                },
                10_000L));
    assertNotSame(firstTimer, location.getCacheTimer());
    assertFalse(location.getDatabase().getConnection().isClosed());
    assertNull(location.getDatabase().getConnectionGroup());

    String id = UUID.randomUUID().toString();
    location.persistCacheEntry(
        sampleEntry(id, "AfterReinit", ExecutionType.Workflow, false, "Finished"));
    assertNotNull(location.loadCacheEntry(id));
  }

  @Test
  void closeIsIdempotentAndStopsTheCacheTimer() throws Exception {
    Timer timer = location.getCacheTimer();
    String id = UUID.randomUUID().toString();
    location.persistCacheEntry(
        sampleEntry(id, "ToClose", ExecutionType.Pipeline, false, "Finished"));

    location.close();
    location.close();

    assertNull(location.getDatabase());
    assertThrows(
        IllegalStateException.class,
        () ->
            timer.schedule(
                new TimerTask() {
                  @Override
                  public void run() {
                    // Cancelled timers reject new tasks.
                  }
                },
                10_000L));

    HopException exception =
        assertThrows(
            HopException.class,
            () ->
                location.persistCacheEntry(
                    sampleEntry(
                        UUID.randomUUID().toString(),
                        "AfterClose",
                        ExecutionType.Workflow,
                        false,
                        "Running")));
    assertTrue(
        exception.getMessage().toLowerCase().contains("closed")
            || (exception.getCause() != null
                && exception.getCause().getMessage() != null
                && exception.getCause().getMessage().toLowerCase().contains("closed")));
  }

  @Test
  void recognizesClosedConnectionFailures() {
    assertTrue(
        CachingDatabaseExecutionInfoLocation.isClosedConnectionFailure(
            new SQLException("This connection has been closed.", "08003")));
    assertFalse(
        CachingDatabaseExecutionInfoLocation.isClosedConnectionFailure(
            new SQLException("syntax error", "42000")));
    SQLException nested = new SQLException("An error occurred executing SQL");
    nested.initCause(new SQLException("This connection has been closed."));
    assertTrue(
        CachingDatabaseExecutionInfoLocation.isClosedConnectionFailure(
            new HopException("wrapper", nested)));
  }

  @Test
  void buildDdlContainsIndexes() throws Exception {
    String ddl = location.buildDdl(variables);
    assertTrue(ddl.toLowerCase().contains("create"));
    assertTrue(ddl.contains("idx_hop_exec_start") || ddl.toLowerCase().contains("index"));
    assertTrue(
        ddl.contains(CachingDatabaseExecutionInfoLocation.COL_JSON)
            || ddl.toLowerCase().contains("json")
            || ddl.toLowerCase().contains("clob")
            || ddl.toLowerCase().contains("varchar")
            || ddl.toLowerCase().contains("text")
            || ddl.toLowerCase().contains("character"));
  }

  @Test
  void lruCacheEvictionEnforcesMaxSize() throws Exception {
    location.setMaxCacheSize("2");
    location.initialize(variables, metadataProvider);

    String id1 = UUID.randomUUID().toString();
    String id2 = UUID.randomUUID().toString();
    String id3 = UUID.randomUUID().toString();

    Execution exec1 = new Execution();
    exec1.setId(id1);
    exec1.setName("Exec1");
    exec1.setExecutionType(ExecutionType.Pipeline);
    exec1.setExecutionStartDate(new Date());
    exec1.setRegistrationDate(new Date());

    Execution exec2 = new Execution();
    exec2.setId(id2);
    exec2.setName("Exec2");
    exec2.setExecutionType(ExecutionType.Pipeline);
    exec2.setExecutionStartDate(new Date());
    exec2.setRegistrationDate(new Date());

    Execution exec3 = new Execution();
    exec3.setId(id3);
    exec3.setName("Exec3");
    exec3.setExecutionType(ExecutionType.Pipeline);
    exec3.setExecutionStartDate(new Date());
    exec3.setRegistrationDate(new Date());

    location.registerExecution(exec1);
    location.registerExecution(exec2);
    assertEquals(2, location.getCache().size());
    assertTrue(location.getCache().containsKey(id1));
    assertTrue(location.getCache().containsKey(id2));

    // Registering the 3rd execution should evict the oldest (id1)
    location.registerExecution(exec3);
    assertEquals(2, location.getCache().size());
    assertFalse(location.getCache().containsKey(id1));
    assertTrue(location.getCache().containsKey(id2));
    assertTrue(location.getCache().containsKey(id3));

    // Evicted entry was persisted and can still be retrieved
    Execution loaded1 = location.getExecution(id1);
    assertNotNull(loaded1);
    assertEquals("Exec1", loaded1.getName());
  }

  @Test
  void closeClearsCacheMap() throws Exception {
    String id = UUID.randomUUID().toString();
    Execution exec = new Execution();
    exec.setId(id);
    exec.setName("ToClose");
    exec.setExecutionType(ExecutionType.Pipeline);
    exec.setExecutionStartDate(new Date());
    exec.setRegistrationDate(new Date());

    location.registerExecution(exec);
    assertFalse(location.getCache().isEmpty());

    location.close();
    assertTrue(location.getCache().isEmpty());
  }

  @Test
  void retrieveIdsWithChildrenLoadsChildrenCorrectly() throws Exception {
    String parentId = UUID.randomUUID().toString();
    String childId = UUID.randomUUID().toString();

    CacheEntry parent =
        sampleEntry(parentId, "ParentPipeline", ExecutionType.Pipeline, false, "Finished");

    Execution child = new Execution();
    child.setId(childId);
    child.setParentId(parentId);
    child.setName("ChildPipeline");
    child.setExecutionType(ExecutionType.Pipeline);
    child.setExecutionStartDate(new Date());
    child.setRegistrationDate(new Date());

    parent.addChildExecution(child);
    location.persistCacheEntry(parent);

    // Clear memory cache so retrieveIds loads from DB
    location.clearCaches();

    Set<DatedId> ids = new HashSet<>();
    location.retrieveIds(true, ids, 100, IExecutionSelector.ALL);
    assertEquals(2, ids.size());
    Set<String> idStrings = new HashSet<>();
    ids.forEach(d -> idStrings.add(d.getId()));
    assertTrue(idStrings.contains(parentId));
    assertTrue(idStrings.contains(childId));
  }

  private static CacheEntry sampleEntry(
      String id, String name, ExecutionType type, boolean failed, String status) {
    Execution execution = new Execution();
    execution.setId(id);
    execution.setName(name);
    execution.setExecutionType(type);
    execution.setExecutionStartDate(new Date());
    execution.setRegistrationDate(new Date());

    ExecutionState state = new ExecutionState();
    state.setId(id);
    state.setName(name);
    state.setExecutionType(type);
    state.setFailed(failed);
    state.setStatusDescription(status);
    state.setExecutionEndDate(new Date());
    state.setUpdateTime(new Date());

    CacheEntry entry = new CacheEntry();
    entry.setId(id);
    entry.setName(name);
    entry.setExecution(execution);
    entry.setExecutionState(state);
    entry.calculateSummary();
    return entry;
  }
}
