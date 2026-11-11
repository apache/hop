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

package org.apache.hop.lineage.openlineage;

import io.openlineage.client.OpenLineage.RunEvent;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.model.HttpLineagePayload;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.plugin.LineageSinkPlugin;
import org.apache.hop.lineage.spi.ILineageSink;

/**
 * Hop lineage sink that maps neutral {@link LineageEvent} batches to OpenLineage JSON and POSTs
 * them to an OpenLineage-compatible collector (e.g. Marquez).
 *
 * <p>The mapping, the asynchronous queue and the spool are not specific to the OpenLineage HTTP
 * API: a sink for a collector with an API of its own (DataHub) extends this class and supplies its
 * own {@link #createEmitter emitter}, which receives the same mapped events.
 */
@LineageSinkPlugin(
    id = "openlineage",
    name = "OpenLineage",
    description = "Maps Hop lineage events to OpenLineage JSON and POSTs to a collector")
public class OpenLineageLineageSink implements ILineageSink {

  /**
   * Keeps {@link #accept} and {@link #shutdown} apart. The hub gives its worker a few seconds to
   * stop before it shuts the sinks down, so an {@code accept} can still be mapping a batch then;
   * the mapper's state is not thread-safe, and shutdown takes the mapper and dispatcher away.
   */
  private final Object lock = new Object();

  private OpenLineageSinkConfig config;
  private OpenLineageEventMapper mapper;
  private volatile OpenLineageAsyncDispatcher dispatcher;
  private OpenLineageSpool spool;
  private ILogChannel log;

  /** The name this sink goes by in its log lines. */
  protected String sinkName() {
    return "OpenLineage";
  }

  /** Reads this sink's configuration. A subclass with variables of its own maps them here. */
  protected OpenLineageSinkConfig readConfig(IVariables variables) throws HopException {
    try {
      return OpenLineageSinkConfig.fromVariables(variables);
    } catch (IllegalArgumentException e) {
      throw new HopException(e.getMessage(), e);
    }
  }

  /**
   * How the names of this sink's folders in the spool start. A sink that shares the spool code
   * gives its own prefix, so it does not replay another sink's events when both are configured with
   * one folder.
   */
  protected String spoolPrefix() {
    return "";
  }

  /** Adjusts the mapping before it is built, for a collector that needs a different shape. */
  protected void configureMapper(OpenLineageEventMapper.Builder builder) {
    // The OpenLineage HTTP API takes the mapping as configured.
  }

  /** What delivers the mapped events: an HTTP POST to the collector. */
  protected OpenLineageEmitter createEmitter(OpenLineageSinkConfig config, ILogChannel log)
      throws HopException {
    return new OpenLineageHttpClient(config, log);
  }

  /** Where the events go, for the start-up log line. */
  protected String describeTarget(OpenLineageSinkConfig config) {
    return "url="
        + config.getCollectorUrl()
        + ", namespace="
        + config.getNamespace()
        + ", producer="
        + config.getProducer()
        + ", jobs="
        + (config.isTransformJobs() ? "per transform (forward propagation)" : "per pipeline")
        + ", parent facets="
        + (config.isParentRunsEnabled() ? "on" : "off")
        + ", file dataset naming="
        + (config.isFileSpecNaming() ? "spec" : "legacy");
  }

  @Override
  public void init(IVariables variables, ILogChannel log) throws HopException {
    this.log = log;
    this.config = readConfig(variables);
    OpenLineageEventMapper.Builder mapperBuilder =
        OpenLineageEventMapper.builder(config.getNamespace(), config.getProducer())
            .sqlParser(RelationalSqlParser.create(config.isSqlParseEnabled(), log))
            .fileSpecNaming(config.isFileSpecNaming())
            .transformJobs(config.isTransformJobs())
            .parentRuns(config.isParentRunsEnabled());
    configureMapper(mapperBuilder);
    this.mapper = mapperBuilder.build();
    this.spool =
        OpenLineageSpool.open(
            config.getSpoolDirectory(), spoolPrefix(), config.getSpoolMaxBytes(), log);
    this.dispatcher =
        new OpenLineageAsyncDispatcher(
            config.getBufferSize(),
            config.getOverflowPolicy(),
            createEmitter(config, log),
            log,
            config.getMetricsIntervalMs(),
            config.getEnqueueTimeoutMs(),
            spool);
    String msg =
        sinkName()
            + " lineage sink initialized: "
            + describeTarget(config)
            + ", spool="
            + (spool == null ? "off" : spool.getLocation());
    log.logBasic(msg);
    // LineageHub log channel is easy to miss in the GUI — mirror to GENERAL.
    LogChannel.GENERAL.logBasic(msg);
  }

  @Override
  public void accept(List<LineageEvent> events) throws HopException {
    if (events == null || events.isEmpty()) {
      return;
    }
    List<LineageEvent> copy = new ArrayList<>(events.size());
    for (LineageEvent event : events) {
      if (event.getKind() == LineageEventKind.HTTP_IO
          && event.getPayload() instanceof HttpLineagePayload http) {
        if (log.isDebug()) {
          log.logDebug(
              "Skipping HTTP_IO lineage event in v1: " + http.getMethod() + " " + http.getUrl());
        }
        continue;
      }
      if (event.getKind() == LineageEventKind.TRANSFORM_SCHEMA) {
        if (log.isDebug()) {
          log.logDebug(
              "Skipping TRANSFORM_SCHEMA in v1 (Hop row metadata, not a physical dataset)");
        }
        continue;
      }
      copy.add(event);
    }
    if (copy.isEmpty()) {
      return;
    }
    synchronized (lock) {
      if (mapper == null || dispatcher == null) {
        // Shut down while the hub still had this batch: nothing is left to send it.
        log.logDetailed(
            sinkName() + " sink is shut down; " + copy.size() + " lineage event(s) not sent");
        return;
      }
      List<RunEvent> runEvents = mapper.mapBatch(copy);
      if (!runEvents.isEmpty()) {
        dispatcher.enqueueAll(runEvents);
      }
    }
  }

  @Override
  public void shutdown() throws HopException {
    OpenLineageAsyncDispatcher current = dispatcher;
    if (current != null) {
      // An accept still handing events over must not wait for buffer capacity while this waits
      // for it.
      current.stopWaiting();
    }
    synchronized (lock) {
      if (dispatcher != null) {
        // A dataset observation whose run never reported its end has no run event left to carry
        // it. Send those as standalone events before the queue drains, so that nothing observed is
        // dropped.
        List<RunEvent> carriedNowhere = mapper != null ? mapper.flushObservations() : List.of();
        if (!carriedNowhere.isEmpty()) {
          log.logBasic(
              sinkName()
                  + " sink: "
                  + carriedNowhere.size()
                  + " dataset observation(s) had no run completion to carry them; sending them as"
                  + " standalone events");
          dispatcher.enqueueAll(carriedNowhere);
        }
        long drainMs = config != null ? config.getShutdownDrainMs() : 30_000;
        try {
          dispatcher.awaitDrain(drainMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        dispatcher.close();
        log.logBasic(
            sinkName()
                + " sink metrics: sent="
                + dispatcher.getSentCount()
                + ", failed="
                + dispatcher.getFailedCount()
                + ", rejected="
                + dispatcher.getRejectedCount()
                + ", dropped="
                + dispatcher.getDroppedCount());
        dispatcher = null;
      }
      mapper = null;
      config = null;
      spool = null;
    }
  }
}
