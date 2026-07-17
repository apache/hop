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

import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.server.HopServerMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

public class HopServerSingleton {

  private static final Class<?> PKG = org.apache.hop.www.HopServer.class;

  private static HopServerConfig hopServerConfig;
  private static HopServerSingleton hopServerSingleton;
  private static org.apache.hop.www.HopServer hopServer;

  private ILogChannel log;

  private PipelineMap pipelineMap;
  private WorkflowMap workflowMap;

  /**
   * Set to true once a graceful shutdown has been initiated. While the server is shutting down it
   * refuses new work (adding/running pipelines, workflows, ...) but keeps serving status requests.
   * It is static because there is a single Hop server per JVM.
   */
  private static final AtomicBoolean shuttingDown = new AtomicBoolean(false);

  private HopServerSingleton(HopServerConfig config) throws HopException {
    HopEnvironment.init();
    configureLogStore(config);

    this.log = new LogChannel("HopServer");
    pipelineMap = new PipelineMap();
    pipelineMap.setHopServerConfig(config);
    workflowMap = new WorkflowMap();
    workflowMap.setHopServerConfig(config);

    installPurgeTimer(config, log, pipelineMap, workflowMap);

    HopServerMeta hopServer = config.getHopServer();
    if (hopServer != null) {
      int port = WebServer.DEFAULT_PORT;
      if (!Utils.isEmpty(hopServer.getPort())) {
        try {
          port = Integer.parseInt(hopServer.getPort());
        } catch (Exception e) {
          log.logError(
              BaseMessages.getString(
                  PKG, "HopServer.Error.CanNotPartPort", hopServer.getHostname(), "" + port),
              e);
        }
      }
    }
  }

  /** How often the server looks for stale objects to purge, in milliseconds. */
  private static final long PURGE_INTERVAL_MS = 20000L;

  /** How long a stale object is kept when neither the configuration nor the variable says so. */
  private static final int DEFAULT_OBJECT_TIMEOUT_MINUTES = 24 * 60;

  /**
   * Hand the log line limits of the server over to the log store. Without this the max_log_lines
   * and max_log_timeout_minutes of the server configuration are read and reported, but never
   * applied, and the log store keeps running with its own defaults. See issue #2796.
   *
   * <p>A limit of zero or lower keeps the default of the log store, the way it has always behaved
   * for a server that does not configure one.
   */
  static void configureLogStore(final HopServerConfig config) {
    // Both limits are resolved separately on purpose. Handing the log store the two configured
    // values as they are would make it take both from the configuration as soon as one of them is
    // set, so configuring only a line limit would leave the log lines without a timeout.
    //
    HopLogStore.init(
        determineMaxLogLines(config),
        determineMaxLogTimeoutMinutes(config),
        EnvUtil.getSystemProperty(Const.HOP_REDIRECT_STDOUT, "N").equalsIgnoreCase("Y"),
        EnvUtil.getSystemProperty(Const.HOP_REDIRECT_STDERR, "N").equalsIgnoreCase("Y"));
  }

  /**
   * The number of log lines a server actually keeps: the configuration file takes precedence over
   * the variable, which in turn takes precedence over the default.
   *
   * @param config the configuration of the server
   * @return the number of lines, 0 or lower means that every line is kept
   */
  public static int determineMaxLogLines(final HopServerConfig config) {
    if (config.getMaxLogLines() > 0) {
      return config.getMaxLogLines();
    }
    return Const.toInt(
        EnvUtil.getSystemProperty(Const.HOP_MAX_LOG_SIZE_IN_LINES), Const.MAX_NR_LOG_LINES);
  }

  /**
   * The age a server actually lets its log lines reach: the configuration file takes precedence
   * over the variable, which in turn takes precedence over the default.
   *
   * @param config the configuration of the server
   * @return the timeout in minutes, 0 or lower means that log lines are never cleaned up
   */
  public static int determineMaxLogTimeoutMinutes(final HopServerConfig config) {
    if (config.getMaxLogTimeoutMinutes() > 0) {
      return config.getMaxLogTimeoutMinutes();
    }
    return Const.toInt(
        EnvUtil.getSystemProperty(Const.HOP_MAX_LOG_TIMEOUT_IN_MINUTES),
        Const.MAX_LOG_LINE_TIMEOUT_MINUTES);
  }

  public static void installPurgeTimer(
      final HopServerConfig config,
      final ILogChannel log,
      final PipelineMap pipelineMap,
      final WorkflowMap workflowMap) {
    installPurgeTimer(config, log, pipelineMap, workflowMap, PURGE_INTERVAL_MS);
  }

  /**
   * The timeout a server actually purges stale objects with: the configuration file takes
   * precedence over the variable, which in turn takes precedence over the default of one day. A
   * server that configures nothing still purges, so this never resolves to zero unless it was asked
   * for.
   *
   * @param config the configuration of the server
   * @return the timeout in minutes, 0 or lower means that objects are never purged
   */
  public static int determineObjectTimeoutMinutes(final HopServerConfig config) {
    if (config.getObjectTimeoutMinutes() > 0) {
      return config.getObjectTimeoutMinutes();
    }
    String systemTimeout = EnvUtil.getSystemProperty(Const.HOP_SERVER_OBJECT_TIMEOUT_MINUTES, null);
    if (!Utils.isEmpty(systemTimeout)) {
      return Const.toInt(systemTimeout, DEFAULT_OBJECT_TIMEOUT_MINUTES);
    }
    return DEFAULT_OBJECT_TIMEOUT_MINUTES;
  }

  /**
   * @param purgeIntervalMs how often to look for stale objects, so that a test does not have to
   *     wait for the interval the server itself uses
   */
  static void installPurgeTimer(
      final HopServerConfig config,
      final ILogChannel log,
      final PipelineMap pipelineMap,
      final WorkflowMap workflowMap,
      final long purgeIntervalMs) {

    final int objectTimeout = determineObjectTimeoutMinutes(config);

    // If we need to time out finished or idle objects, we should create a timer
    // in the background to clean
    //
    if (objectTimeout > 0) {

      log.logBasic("Installing timer to purge stale objects after " + objectTimeout + " minutes.");

      Timer timer = new Timer(true);

      final AtomicBoolean busy = new AtomicBoolean(false);
      TimerTask timerTask =
          new TimerTask() {
            @Override
            public void run() {
              if (!busy.get()) {
                busy.set(true);

                try {
                  // Check all pipelines...
                  //
                  for (HopServerObjectEntry entry : pipelineMap.getPipelineObjects()) {
                    IPipelineEngine<PipelineMeta> pipeline = pipelineMap.getPipeline(entry);

                    // See if the pipeline is finished or stopped.
                    //
                    if (pipeline != null
                        && (pipeline.isFinished() || pipeline.isStopped())
                        && pipeline.getExecutionStartDate() != null) {
                      // check the last log time
                      //
                      int diffInMinutes =
                          (int)
                              Math.floor(
                                  (System.currentTimeMillis()
                                          - pipeline.getExecutionStartDate().getTime())
                                      / 60000.0);
                      if (diffInMinutes >= objectTimeout) {
                        // Let's remove this from the pipeline map...
                        //
                        pipelineMap.removePipeline(entry);

                        // Remove the logging information from the log registry & central log store
                        //
                        LoggingRegistry.getInstance()
                            .removeIncludingChildren(pipeline.getLogChannelId());
                        HopLogStore.discardLines(pipeline.getLogChannelId(), false);

                        log.logBasic(
                            "Cleaned up pipeline "
                                + entry.getName()
                                + " with id "
                                + entry.getId()
                                + " from "
                                + pipeline.getExecutionStartDate()
                                + ", diff="
                                + diffInMinutes);
                      }
                    }
                  }

                  // And the workflows...
                  //
                  for (HopServerObjectEntry entry : workflowMap.getWorkflowObjects()) {
                    IWorkflowEngine<WorkflowMeta> workflow = workflowMap.getWorkflow(entry);

                    // See if the workflow is finished or stopped.
                    //
                    if (workflow != null
                        && (workflow.isFinished() || workflow.isStopped())
                        && workflow.getExecutionStartDate() != null) {
                      // check the last log time
                      //
                      int diffInMinutes =
                          (int)
                              Math.floor(
                                  (System.currentTimeMillis()
                                          - workflow.getExecutionStartDate().getTime())
                                      / 60000.0);
                      if (diffInMinutes >= objectTimeout) {
                        // Let's remove this from the workflow map...
                        //
                        String id = workflowMap.getWorkflow(entry).getLogChannelId();
                        LoggingRegistry.getInstance().removeLogChannelFileWriterBuffer(id);

                        workflowMap.removeWorkflow(entry);

                        log.logBasic(
                            "Cleaned up workflow "
                                + entry.getName()
                                + " with id "
                                + entry.getId()
                                + " from "
                                + workflow.getExecutionStartDate());
                      }
                    }
                  }

                } finally {
                  busy.set(false);
                }
              }
            }
          };

      // Search for stale objects at every interval:
      //
      timer.schedule(timerTask, purgeIntervalMs, purgeIntervalMs);
    }
  }

  public static HopServerSingleton getInstance() {
    try {
      if (hopServerSingleton == null) {
        if (hopServerConfig == null) {
          hopServerConfig = new HopServerConfig();
          HopServerMeta hopServer = new HopServerMeta();
          hopServerConfig.setHopServer(hopServer);
        }

        hopServerSingleton = new HopServerSingleton(hopServerConfig);

        String serverObjectId = UUID.randomUUID().toString();
        SimpleLoggingObject servletLoggingObject =
            new SimpleLoggingObject("HopServerSingleton", LoggingObjectType.HOP_SERVER, null);
        servletLoggingObject.setContainerObjectId(serverObjectId);
        servletLoggingObject.setLogLevel(LogLevel.BASIC);

        return hopServerSingleton;
      } else {
        return hopServerSingleton;
      }
    } catch (HopException ke) {
      throw new HopRuntimeException(ke);
    }
  }

  /**
   * @return true when a graceful shutdown of the server has been initiated. Safe to call from
   *     servlets at any time.
   */
  public static boolean isServerShuttingDown() {
    return shuttingDown.get();
  }

  public static void setServerShuttingDown(boolean value) {
    shuttingDown.set(value);
  }

  public PipelineMap getPipelineMap() {
    return pipelineMap;
  }

  public void setPipelineMap(PipelineMap pipelineMap) {
    this.pipelineMap = pipelineMap;
  }

  public WorkflowMap getWorkflowMap() {
    return workflowMap;
  }

  public void setWorkflowMap(WorkflowMap workflowMap) {
    this.workflowMap = workflowMap;
  }

  public static HopServerConfig getHopServerConfig() {
    return hopServerConfig;
  }

  public static void setHopServerConfig(HopServerConfig hopServerConfig) {
    HopServerSingleton.hopServerConfig = hopServerConfig;
  }

  public static void setHopServer(org.apache.hop.www.HopServer hopServer) {
    HopServerSingleton.hopServer = hopServer;
  }

  public static org.apache.hop.www.HopServer getHopServer() {
    return HopServerSingleton.hopServer;
  }

  public ILogChannel getLog() {
    return log;
  }
}
