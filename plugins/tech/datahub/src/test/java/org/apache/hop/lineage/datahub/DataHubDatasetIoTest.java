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
package org.apache.hop.lineage.datahub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.openlineage.client.OpenLineage.RunEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.lineage.datahub.DataHubTranslator.Mcp;
import org.apache.hop.lineage.model.DatasetIoOperation;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A Kafka topic a transform reads or writes, as DataHub's own Kafka source names it. */
class DataHubDatasetIoTest {

  private static final String FLOW = "urn:li:dataFlow:(hop,orders.hpl,prod)";
  private static final String CONSUME = "urn:li:dataJob:(" + FLOW + ",Consume)";
  private static final String PRODUCE = "urn:li:dataJob:(" + FLOW + ",Produce)";
  private static final String ORDERS = "urn:li:dataset:(urn:li:dataPlatform:kafka,orders,PROD)";
  private static final String ENRICHED =
      "urn:li:dataset:(urn:li:dataPlatform:kafka,orders-enriched,PROD)";
  private static final String KAFKA = "kafka://kafka:9092";

  private final DataHubTranslator translator =
      new DataHubTranslator("PROD", "prod", null, "urn:li:corpuser:datahub", true);

  @BeforeAll
  static void initLogStore() {
    HopLogStore.init();
  }

  private List<Mcp> all(List<RunEvent> events) {
    List<Mcp> proposals = new ArrayList<>();
    for (RunEvent event : events) {
      proposals.addAll(translator.translate(event).proposals());
    }
    return proposals;
  }

  private static List<Mcp> find(List<Mcp> proposals, String urn, String aspect) {
    return proposals.stream()
        .filter(m -> m.entityUrn().equals(urn) && m.aspectName().equals(aspect))
        .toList();
  }

  /**
   * A consumer runs until it is stopped: its topic shows before the run ends, which never comes.
   */
  @Test
  void aTopicShowsOnItsTaskWhileTheRunGoesOn() {
    HopEvents run = new HopEvents("orders");
    List<RunEvent> events =
        HopEvents.mapper()
            .mapBatch(
                List.of(
                    run.pipeline(RunLifecyclePhase.STARTED),
                    run.graph("Consume>Produce"),
                    run.transform("Consume", RunLifecyclePhase.STARTED),
                    run.transform("Produce", RunLifecyclePhase.STARTED),
                    run.dataset("Consume", DatasetIoOperation.READ, KAFKA, "orders"),
                    run.dataset("Produce", DatasetIoOperation.WRITE, KAFKA, "orders-enriched")));
    List<Mcp> proposals = all(events);

    List<Mcp> consumed = find(proposals, CONSUME, "dataJobInputOutput");
    assertFalse(consumed.isEmpty(), "no edges for the consumer in " + proposals);
    assertTrue(
        consumed.get(0).value().toString().contains(ORDERS), consumed.get(0).value().toString());
    List<Mcp> produced = find(proposals, PRODUCE, "dataJobInputOutput");
    assertFalse(produced.isEmpty(), "no edges for the producer in " + proposals);
    assertTrue(
        produced.get(0).value().toString().contains(ENRICHED), produced.get(0).value().toString());

    String consumeRun =
        DataHubUrns.dataProcessInstance(UUID.fromString(run.transformRunId("Consume")));
    List<Mcp> runEvents = find(proposals, consumeRun, "dataProcessInstanceRunEvent");
    assertFalse(runEvents.isEmpty(), "the consumer's run started");
    for (Mcp runEvent : runEvents) {
      JsonNode value = runEvent.value();
      assertEquals("STARTED", value.path("status").asText(), "the run goes on: " + value);
    }
  }

  @Test
  void aRunThatEndsKeepsItsTopics() {
    HopEvents run = new HopEvents("orders");
    List<RunEvent> events =
        HopEvents.mapper()
            .mapBatch(
                List.of(
                    run.pipeline(RunLifecyclePhase.STARTED),
                    run.graph("Consume>Produce"),
                    run.dataset("Consume", DatasetIoOperation.READ, KAFKA, "orders"),
                    run.transform("Consume", RunLifecyclePhase.FINISHED, 0, 3),
                    run.transform("Produce", RunLifecyclePhase.FINISHED, 3, 3),
                    run.pipeline(RunLifecyclePhase.FINISHED)));
    List<Mcp> proposals = all(events);

    List<Mcp> consumed = find(proposals, CONSUME, "dataJobInputOutput");
    JsonNode last = consumed.get(consumed.size() - 1).value();
    assertTrue(last.toString().contains(ORDERS), "the end keeps the topic: " + last);
  }
}
