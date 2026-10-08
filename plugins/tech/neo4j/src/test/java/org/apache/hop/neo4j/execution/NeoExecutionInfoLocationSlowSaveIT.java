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

package org.apache.hop.neo4j.execution;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Issue #8808: a pipeline must finish, and a stop must return, while a save to the execution
 * information location takes long. Saves to a large Neo4j database without indexes can take
 * minutes. The test makes a save wait for as long as it wants: it holds a write lock on the
 * pipeline's Execution node in an open transaction, so the timer tick blocks inside Neo4j.
 */
class NeoExecutionInfoLocationSlowSaveIT {

  private static final String PASSWORD = "hop-it-8808";
  private static final String INJECTOR = "injector";
  private static final Duration EXPECTED_WITHIN = Duration.ofSeconds(15);

  private static Neo4jContainer<?> neo4j;
  private static Driver driver;

  private ExecutorService executor;
  private LocalPipelineEngine pipeline;
  private RowProducer rowProducer;
  private Session blockerSession;
  private Transaction blocker;
  private boolean waitedUntilFinished;

  @BeforeAll
  static void setUpNeo4j() throws Exception {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable(),
        "Docker is required for NeoExecutionInfoLocationSlowSaveIT");
    HopEnvironment.init();
    neo4j =
        new Neo4jContainer<>(DockerImageName.parse("neo4j:5.26"))
            .withAdminPassword(PASSWORD)
            .withEnv("NEO4J_server_memory_heap_initial__size", "256m")
            .withEnv("NEO4J_server_memory_heap_max__size", "512m")
            .withEnv("NEO4J_server_memory_pagecache_size", "64m")
            .withStartupTimeout(Duration.ofMinutes(5));
    try {
      neo4j.start();
    } catch (Exception e) {
      abort("Neo4j container did not become ready: " + e.getMessage());
    }
    driver = GraphDatabase.driver(neo4j.getBoltUrl(), AuthTokens.basic("neo4j", PASSWORD));
  }

  @AfterAll
  static void tearDownNeo4j() {
    if (driver != null) {
      driver.close();
    }
    if (neo4j != null) {
      neo4j.stop();
    }
  }

  @BeforeEach
  void startPipelineWithAStuckSave() throws Exception {
    executor = Executors.newCachedThreadPool();

    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    NeoConnection connection = new NeoConnection();
    connection.setName("neo4j");
    connection.setServer(neo4j.getHost());
    connection.setBoltPort(Integer.toString(neo4j.getMappedPort(7687)));
    connection.setUsername("neo4j");
    connection.setPassword(PASSWORD);
    metadataProvider.getSerializer(NeoConnection.class).save(connection);

    NeoExecutionInfoLocation neoLocation = new NeoExecutionInfoLocation();
    neoLocation.setConnectionName("neo4j");
    ExecutionInfoLocation location = new ExecutionInfoLocation();
    location.setName("neo4j");
    location.setExecutionInfoLocation(neoLocation);
    // Tick right away, then not again during the test: the first save is the one that blocks.
    location.setDataLoggingDelay("0");
    location.setDataLoggingInterval("600000");
    metadataProvider.getSerializer(ExecutionInfoLocation.class).save(location);

    PipelineRunConfiguration runConfiguration =
        new PipelineRunConfiguration(
            "local-neo4j",
            "",
            "neo4j",
            new ArrayList<>(),
            new LocalPipelineRunConfiguration(),
            null,
            false);

    pipeline = new LocalPipelineEngine(slowSavePipeline());
    pipeline.setMetadataProvider(metadataProvider);
    pipeline.setPipelineRunConfiguration(runConfiguration);
    pipeline.prepareExecution();
    rowProducer = pipeline.addRowProducer(INJECTOR, 0);

    // prepareExecution() registered the Execution node. Lock it before the first tick.
    //
    blockerSession = driver.session();
    blocker = blockerSession.beginTransaction();
    blocker
        .run(
            "MATCH (e:Execution { id : $id }) SET e.lockedByIt8808 = true",
            Map.of("id", pipeline.getLogChannelId()))
        .consume();

    pipeline.startThreads();

    awaitTrue(
        NeoExecutionInfoLocationSlowSaveIT::aSaveIsWaitingInNeo4j,
        Duration.ofSeconds(30),
        "The first execution information save never started");
  }

  @AfterEach
  void releaseTheSaveAndFinish() throws Exception {
    try {
      releaseBlocker();
      if (pipeline != null && !waitedUntilFinished) {
        rowProducer.finished();
        waitUntilFinished();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void thePipelineFinishesWhileASaveIsInProgress() throws Exception {
    rowProducer.finished();

    awaitTrue(
        pipeline::isFinished,
        EXPECTED_WITHIN,
        "All transforms are done but the pipeline did not finish while a save to the execution"
            + " information location was in progress");

    releaseBlocker();
    waitUntilFinished();

    // The final save still runs after the slow one, and stores the end of the execution.
    //
    try (Session session = driver.session()) {
      Record record =
          session
              .run(
                  "MATCH (e:Execution { id : $id }) RETURN e.executionEndDate AS endDate",
                  Map.of("id", pipeline.getLogChannelId()))
              .single();
      assertNotNull(record.get("endDate").asObject(), "The final execution state was not saved");
    }
  }

  @Test
  void stoppingThePipelineReturnsWhileASaveIsInProgress() throws Exception {
    // This is what the Stop button of Hop GUI calls, on the UI thread.
    //
    Future<?> stop = executor.submit(pipeline::stopAll);
    try {
      stop.get(EXPECTED_WITHIN.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      fail(
          "Stopping the pipeline did not return while a save to the execution information"
              + " location was in progress");
    }
    assertTrue(pipeline.isStopped());
  }

  private static PipelineMeta slowSavePipeline() {
    PluginRegistry registry = PluginRegistry.getInstance();
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("it-8808-slow-save");

    InjectorMeta injectorMeta = new InjectorMeta();
    TransformMeta injector =
        new TransformMeta(
            registry.getPluginId(TransformPluginType.class, injectorMeta), INJECTOR, injectorMeta);
    DummyMeta dummyMeta = new DummyMeta();
    TransformMeta dummy =
        new TransformMeta(
            registry.getPluginId(TransformPluginType.class, dummyMeta), "dummy", dummyMeta);
    pipelineMeta.addTransform(injector);
    pipelineMeta.addTransform(dummy);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(injector, dummy));
    return pipelineMeta;
  }

  /** A tick of the execution information timer is waiting for the lock this test holds. */
  private static boolean aSaveIsWaitingInNeo4j() {
    for (StackTraceElement[] stack : Thread.getAllStackTraces().values()) {
      for (StackTraceElement frame : stack) {
        if (frame.getClassName().equals(NeoExecutionInfoLocation.class.getName())
            && frame.getMethodName().equals("updateExecutionState")) {
          return true;
        }
      }
    }
    return false;
  }

  /** Only once: waitUntilFinished() takes the single finished signal off a queue. */
  private void waitUntilFinished() throws Exception {
    waitedUntilFinished = true;
    executor.submit(pipeline::waitUntilFinished).get(2, TimeUnit.MINUTES);
  }

  private void releaseBlocker() {
    if (blocker != null) {
      blocker.rollback();
      blocker.close();
      blocker = null;
    }
    if (blockerSession != null) {
      blockerSession.close();
      blockerSession = null;
    }
  }

  private static void awaitTrue(BooleanSupplier condition, Duration timeout, String message)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        fail(message + " (waited " + timeout.toSeconds() + "s)");
      }
      Thread.sleep(100);
    }
  }
}
