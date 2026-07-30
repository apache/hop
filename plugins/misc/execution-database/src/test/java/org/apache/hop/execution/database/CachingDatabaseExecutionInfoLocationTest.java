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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
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
