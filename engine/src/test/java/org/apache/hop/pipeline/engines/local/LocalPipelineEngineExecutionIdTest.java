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

package org.apache.hop.pipeline.engines.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionStateBuilder;
import org.apache.hop.execution.IExecutionSelector;
import org.apache.hop.execution.caching.BaseCachingExecutionInfoLocation;
import org.apache.hop.execution.caching.CacheEntry;
import org.apache.hop.execution.caching.DatedId;
import org.apache.hop.pipeline.PipelineMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class LocalPipelineEngineExecutionIdTest {

  @BeforeAll
  static void initLogging() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  @Test
  void registeredExecutionUsesTheLogChannelTheTimerWillRead() throws Exception {
    ILogChannel channel = new LogChannel("kafka-consumer");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("kafka-sub");
    LocalPipelineEngine engine = new LocalPipelineEngine(pipelineMeta);
    engine.setLogChannel(channel);
    engine.setMetadataProvider(
        new org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider());

    CapturingLocation plugin = new CapturingLocation();
    ExecutionInfoLocation location = new ExecutionInfoLocation();
    location.setExecutionInfoLocation(plugin);
    engine.setExecutionInfoLocation(location);

    engine.registerPipelineExecutionInformation();

    assertEquals(channel.getLogChannelId(), plugin.registeredId);
    assertEquals(
        channel.getLogChannelId(), ExecutionStateBuilder.fromExecutor(engine, 0).build().getId());
    assertTrue(plugin.getCache().get(channel.getLogChannelId()).isSingleWriter());
  }

  @Test
  void aNegativeLineRequestIsAFullSnapshot() {
    ILogChannel channel = new LogChannel("snapshot");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("snapshot");
    LocalPipelineEngine engine = new LocalPipelineEngine(pipelineMeta);
    engine.setLogChannel(channel);

    ExecutionState full = ExecutionStateBuilder.fromExecutor(engine, -1).build();
    assertNull(full.getLastLogLineNr());
    assertNull(ExecutionStateBuilder.fromExecutor(engine, null).build().getLastLogLineNr());

    ExecutionState delta = ExecutionStateBuilder.fromExecutor(engine, 0).build();
    assertNotNull(delta.getLastLogLineNr());
  }

  @Test
  void stopAllClosesTheLocationOnlyForASingleThreadedEngine() throws Exception {
    CapturingLocation normalLocation = new CapturingLocation();
    LocalPipelineEngine normal = engineWith(normalLocation);
    normal.stopAll();
    assertEquals(0, normalLocation.closes);

    CapturingLocation singleThreadedLocation = new CapturingLocation();
    LocalPipelineEngine singleThreaded = engineWith(singleThreadedLocation);
    singleThreaded.setPipelineType(PipelineMeta.PipelineType.SingleThreaded);
    singleThreaded.stopAll();
    assertEquals(1, singleThreadedLocation.closes);
  }

  private static LocalPipelineEngine engineWith(CapturingLocation plugin) {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("stop");
    LocalPipelineEngine engine = new LocalPipelineEngine(pipelineMeta);
    engine.setMetadataProvider(
        new org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider());
    ExecutionInfoLocation location = new ExecutionInfoLocation();
    location.setExecutionInfoLocation(plugin);
    engine.setExecutionInfoLocation(location);
    return engine;
  }

  private static final class CapturingLocation extends BaseCachingExecutionInfoLocation {
    private String registeredId;
    private int closes;

    @Override
    public synchronized void close() throws org.apache.hop.core.exception.HopException {
      closes++;
      super.close();
    }

    @Override
    public void registerExecution(Execution execution)
        throws org.apache.hop.core.exception.HopException {
      registeredId = execution.getId();
      super.registerExecution(execution);
    }

    @Override
    public BaseCachingExecutionInfoLocation clone() {
      return new CapturingLocation();
    }

    @Override
    protected void persistCacheEntry(CacheEntry cacheEntry) {
      cacheEntry.setDirty(false);
    }

    @Override
    protected CacheEntry loadCacheEntry(String executionId) {
      return null;
    }

    @Override
    protected void deleteCacheEntry(CacheEntry cacheEntry) {}

    @Override
    protected void retrieveIds(
        boolean includeChildren,
        java.util.Set<DatedId> ids,
        int limit,
        IExecutionSelector selector) {}

    @Override
    public String getPluginId() {
      return "capturing";
    }

    @Override
    public void setPluginId(String pluginId) {}

    @Override
    public String getPluginName() {
      return "capturing";
    }

    @Override
    public void setPluginName(String pluginName) {}
  }
}
