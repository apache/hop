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
 */
@LineageSinkPlugin(
    id = "openlineage",
    name = "OpenLineage",
    description = "Maps Hop lineage events to OpenLineage JSON and POSTs to a collector")
public class OpenLineageLineageSink implements ILineageSink {

  private OpenLineageSinkConfig config;
  private OpenLineageEventMapper mapper;
  private OpenLineageHttpClient httpClient;
  private OpenLineageAsyncDispatcher dispatcher;
  private ILogChannel log;

  @Override
  public void init(IVariables variables, ILogChannel log) throws HopException {
    this.log = log;
    try {
      this.config = OpenLineageSinkConfig.fromVariables(variables);
    } catch (IllegalArgumentException e) {
      throw new HopException(e.getMessage(), e);
    }
    this.mapper =
        new OpenLineageEventMapper(
            config.getNamespace(),
            config.getProducer(),
            RelationalSqlParser.create(config.isSqlParseEnabled(), log),
            config.isFileSpecNaming());
    this.httpClient = new OpenLineageHttpClient(config, log);
    this.dispatcher =
        new OpenLineageAsyncDispatcher(
            config.getBufferSize(),
            config.getOverflowPolicy(),
            httpClient,
            log,
            config.getMetricsIntervalMs(),
            config.getEnqueueTimeoutMs());
    String msg =
        "OpenLineage lineage sink initialized: url="
            + config.getCollectorUrl()
            + ", namespace="
            + config.getNamespace()
            + ", producer="
            + config.getProducer();
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
    List<RunEvent> runEvents = mapper.mapBatch(copy);
    if (!runEvents.isEmpty()) {
      dispatcher.enqueueAll(runEvents);
    }
  }

  @Override
  public void shutdown() throws HopException {
    if (dispatcher != null) {
      long drainMs = config != null ? config.getShutdownDrainMs() : 30_000;
      try {
        dispatcher.awaitDrain(drainMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      long dropped = dispatcher.getDroppedCount();
      dispatcher.close();
      if (httpClient != null) {
        log.logBasic(
            "OpenLineage sink metrics: sent="
                + httpClient.getSuccessCount()
                + ", failed="
                + httpClient.getFailureCount()
                + ", dropped="
                + dropped);
      }
      dispatcher = null;
    }
    httpClient = null;
    mapper = null;
    config = null;
  }
}
