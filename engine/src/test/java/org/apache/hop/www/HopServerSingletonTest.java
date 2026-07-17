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

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.function.BooleanSupplier;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.WorkflowConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class HopServerSingletonTest {

  @Test
  void installPurgeTimerDoesNothingWhenTimeoutIsZeroViaEnvironment() {
    HopServerConfig config = mock(HopServerConfig.class);
    when(config.getObjectTimeoutMinutes()).thenReturn(0);
    System.setProperty(Const.HOP_SERVER_OBJECT_TIMEOUT_MINUTES, "0");

    ILogChannel log = mock(ILogChannel.class);
    HopServerSingleton.installPurgeTimer(config, log, new PipelineMap(), new WorkflowMap());

    verify(log, never()).logBasic(contains("Installing timer"));
  }

  @Test
  void installPurgeTimerLogsWhenTimeoutPositive() {
    HopServerConfig config = mock(HopServerConfig.class);
    when(config.getObjectTimeoutMinutes()).thenReturn(15);
    System.clearProperty(Const.HOP_SERVER_OBJECT_TIMEOUT_MINUTES);

    ILogChannel log = mock(ILogChannel.class);
    HopServerSingleton.installPurgeTimer(config, log, new PipelineMap(), new WorkflowMap());

    verify(log).logBasic(contains("Installing timer"));
  }

  /**
   * The log line limits of the server configuration have to reach the log store, or they are read
   * and reported but never applied. See issue #2796.
   */
  @Test
  void logStoreUsesTheConfiguredLogLimits() {
    HopServerConfig config = new HopServerConfig();
    config.setMaxLogLines(123);
    config.setMaxLogTimeoutMinutes(7);

    HopServerSingleton.configureLogStore(config);

    assertEquals(123, HopLogStore.getAppender().getMaxNrLines());
    assertEquals(7, HopLogStore.getMaxLogTimeoutMinutes());
  }

  /**
   * The two log limits are configured independently: setting one of them may not silently drop the
   * other back to "no limit". See issue #2796.
   */
  @Test
  void configuringOnlyTheLineLimitKeepsTheLogTimeout() {
    HopLogStore.init();
    int defaultMaxLogTimeoutMinutes = HopLogStore.getMaxLogTimeoutMinutes();

    HopServerConfig config = new HopServerConfig();
    config.setMaxLogLines(1000);

    HopServerSingleton.configureLogStore(config);

    assertEquals(1000, HopLogStore.getAppender().getMaxNrLines());
    assertEquals(
        defaultMaxLogTimeoutMinutes,
        HopLogStore.getMaxLogTimeoutMinutes(),
        "Log lines should still time out when only a line limit is configured");
  }

  /** The variable is used for the log limits when the configuration file does not say otherwise. */
  @Test
  void logLimitsUseTheVariablesWithoutConfiguration() {
    System.setProperty(Const.HOP_MAX_LOG_SIZE_IN_LINES, "321");
    System.setProperty(Const.HOP_MAX_LOG_TIMEOUT_IN_MINUTES, "77");
    try {
      HopServerConfig config = new HopServerConfig();

      assertEquals(321, HopServerSingleton.determineMaxLogLines(config));
      assertEquals(77, HopServerSingleton.determineMaxLogTimeoutMinutes(config));

      // ... and the configuration file wins over the variable.
      config.setMaxLogLines(10);
      config.setMaxLogTimeoutMinutes(20);
      assertEquals(10, HopServerSingleton.determineMaxLogLines(config));
      assertEquals(20, HopServerSingleton.determineMaxLogTimeoutMinutes(config));
    } finally {
      System.clearProperty(Const.HOP_MAX_LOG_SIZE_IN_LINES);
      System.clearProperty(Const.HOP_MAX_LOG_TIMEOUT_IN_MINUTES);
    }
  }

  /**
   * A server that configures no limits keeps the defaults of the log store, the way it behaved
   * before the configured limits were passed on.
   */
  @Test
  void logStoreKeepsItsDefaultsWithoutConfiguredLogLimits() {
    HopLogStore.init();
    int defaultMaxLogLines = HopLogStore.getAppender().getMaxNrLines();
    int defaultMaxLogTimeoutMinutes = HopLogStore.getMaxLogTimeoutMinutes();

    HopServerSingleton.configureLogStore(new HopServerConfig());

    assertEquals(defaultMaxLogLines, HopLogStore.getAppender().getMaxNrLines());
    assertEquals(defaultMaxLogTimeoutMinutes, HopLogStore.getMaxLogTimeoutMinutes());
  }

  /**
   * A server that configures no object timeout still purges after a day. The status page reports
   * this value, so it may not claim that stale objects are never cleaned up. See issue #2796.
   */
  @Test
  void objectTimeoutFallsBackToADayWithoutConfigurationOrVariable() {
    System.clearProperty(Const.HOP_SERVER_OBJECT_TIMEOUT_MINUTES);

    assertEquals(1440, HopServerSingleton.determineObjectTimeoutMinutes(new HopServerConfig()));
  }

  /** The variable is used when the configuration file does not say otherwise. */
  @Test
  void objectTimeoutUsesTheVariableWithoutConfiguration() {
    System.setProperty(Const.HOP_SERVER_OBJECT_TIMEOUT_MINUTES, "60");
    try {
      assertEquals(60, HopServerSingleton.determineObjectTimeoutMinutes(new HopServerConfig()));
    } finally {
      System.clearProperty(Const.HOP_SERVER_OBJECT_TIMEOUT_MINUTES);
    }
  }

  /** The configuration file takes precedence over the variable. */
  @Test
  void objectTimeoutFromConfigurationBeatsTheVariable() {
    System.setProperty(Const.HOP_SERVER_OBJECT_TIMEOUT_MINUTES, "60");
    try {
      HopServerConfig config = new HopServerConfig();
      config.setObjectTimeoutMinutes(15);

      assertEquals(15, HopServerSingleton.determineObjectTimeoutMinutes(config));
    } finally {
      System.clearProperty(Const.HOP_SERVER_OBJECT_TIMEOUT_MINUTES);
    }
  }

  /** A pipeline that ran longer ago than the object timeout is purged. */
  @Test
  void purgeTimerRemovesStalePipeline() throws Exception {
    PipelineMap pipelineMap = new PipelineMap();
    pipelineMap.addPipeline("stale", "p1", finishedPipeline(minutesAgo(5)), null);

    installPurgeTimer(pipelineMap, new WorkflowMap());

    waitUntil(() -> pipelineMap.getPipelineObjects().isEmpty());
    assertTrue(pipelineMap.getPipelineObjects().isEmpty(), "The stale pipeline should be purged");
  }

  /** A pipeline that ran more recently than the object timeout is kept. */
  @Test
  void purgeTimerKeepsRecentPipeline() throws Exception {
    PipelineMap pipelineMap = new PipelineMap();
    pipelineMap.addPipeline("recent", "p1", finishedPipeline(new Date()), null);

    installPurgeTimer(pipelineMap, new WorkflowMap());
    Thread.sleep(500);

    assertEquals(1, pipelineMap.getPipelineObjects().size(), "The recent pipeline should be kept");
  }

  /**
   * The server has to keep purging for as long as it runs. A finished workflow that is not stale
   * enough to be purged used to stop the timer, after which nothing was ever cleaned up again and
   * the memory of the server kept growing. See issue #2796.
   */
  @Test
  void purgeTimerKeepsPurgingAfterSeeingAFinishedWorkflow() throws Exception {
    PipelineMap pipelineMap = new PipelineMap();
    WorkflowMap workflowMap = new WorkflowMap();

    // A workflow that finished just now, so it is not stale enough to be purged.
    workflowMap.addWorkflow(
        "recent", "w1", finishedWorkflow(new Date()), mock(WorkflowConfiguration.class));

    installPurgeTimer(pipelineMap, workflowMap);

    // Give the timer the time to run over the workflow at least once.
    Thread.sleep(500);

    // A pipeline that is stale enough to be purged shows up afterwards.
    pipelineMap.addPipeline("stale", "p1", finishedPipeline(minutesAgo(5)), null);

    waitUntil(() -> pipelineMap.getPipelineObjects().isEmpty());
    assertTrue(
        pipelineMap.getPipelineObjects().isEmpty(),
        "The stale pipeline should still be purged after a finished workflow was seen");
  }

  private void installPurgeTimer(PipelineMap pipelineMap, WorkflowMap workflowMap) {
    HopServerConfig config = mock(HopServerConfig.class);
    when(config.getObjectTimeoutMinutes()).thenReturn(1);
    System.clearProperty(Const.HOP_SERVER_OBJECT_TIMEOUT_MINUTES);

    HopServerSingleton.installPurgeTimer(
        config, mock(ILogChannel.class), pipelineMap, workflowMap, 50L);
  }

  private static Date minutesAgo(int minutes) {
    return new Date(System.currentTimeMillis() - minutes * 60_000L);
  }

  private IPipelineEngine<PipelineMeta> finishedPipeline(Date executionStartDate) {
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.isFinished()).thenReturn(true);
    when(pipeline.getExecutionStartDate()).thenReturn(executionStartDate);
    when(pipeline.getLogChannelId()).thenReturn("pipeline-log-channel");
    return pipeline;
  }

  private IWorkflowEngine<WorkflowMeta> finishedWorkflow(Date executionStartDate) {
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    when(workflow.isFinished()).thenReturn(true);
    when(workflow.getExecutionStartDate()).thenReturn(executionStartDate);
    when(workflow.getLogChannelId()).thenReturn("workflow-log-channel");
    return workflow;
  }

  private void waitUntil(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 5000L;
    while (System.currentTimeMillis() < deadline && !condition.getAsBoolean()) {
      Thread.sleep(50);
    }
  }
}
