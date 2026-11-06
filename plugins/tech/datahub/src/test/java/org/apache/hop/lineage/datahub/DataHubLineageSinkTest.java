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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** The sink through its {@link org.apache.hop.lineage.spi.ILineageSink} contract. */
class DataHubLineageSinkTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String INGEST = "/aspects?action=ingestProposalBatch";

  @RegisterExtension
  static WireMockExtension gms =
      WireMockExtension.newInstance()
          .options(WireMockConfiguration.wireMockConfig().dynamicPort())
          .build();

  @TempDir Path spool;

  @BeforeAll
  static void initLogStore() {
    HopLogStore.init();
  }

  @BeforeEach
  void reset() {
    gms.resetAll();
    gms.stubFor(
        post(urlEqualTo("/api/graphql"))
            .willReturn(aResponse().withStatus(200).withBody("{\"data\":{\"entities\":[]}}")));
  }

  private Variables variables(String url) {
    Variables variables = new Variables();
    variables.setVariable(DataHubSinkConfig.HOP_LINEAGE_DATAHUB_URL, url);
    variables.setVariable("HOP_LINEAGE_DATAHUB_SPOOL_DIR", spool.toString());
    variables.setVariable("HOP_LINEAGE_DATAHUB_RETRY_BACKOFF_MS", "1");
    variables.setVariable("HOP_LINEAGE_DATAHUB_SHUTDOWN_DRAIN_MS", "5000");
    return variables;
  }

  private static List<LineageEvent> run(HopEvents hop) {
    return List.of(
        hop.pipeline(RunLifecyclePhase.STARTED),
        hop.graph("Read>Write"),
        hop.transform("Read", RunLifecyclePhase.STARTED),
        hop.transform("Write", RunLifecyclePhase.STARTED),
        hop.read("Read", "file:///data/in.csv", HopEvents.column("id", "Integer")),
        hop.write("Write", "file:///out/x.csv", HopEvents.column("id", "Integer")),
        hop.transform("Read", RunLifecyclePhase.FINISHED),
        hop.transform("Write", RunLifecyclePhase.FINISHED),
        hop.pipeline(RunLifecyclePhase.FINISHED));
  }

  private static Set<String> entitiesWritten() throws Exception {
    Set<String> urns = new HashSet<>();
    for (var request : gms.findAll(postRequestedFor(urlEqualTo(INGEST)))) {
      for (JsonNode proposal : MAPPER.readTree(request.getBodyAsString()).get("proposals")) {
        urns.add(proposal.get("entityUrn").asText());
      }
    }
    return urns;
  }

  @Test
  void aPipelineRunArrivesAsAFlowWithItsJobsRunsAndDatasets() throws Exception {
    gms.stubFor(post(urlEqualTo(INGEST)).willReturn(aResponse().withStatus(200).withBody("{}")));
    HopEvents hop = new HopEvents("orders");

    DataHubLineageSink sink = new DataHubLineageSink();
    sink.init(variables("http://localhost:" + gms.getPort()), new LogChannel("DataHubSinkTest"));
    sink.accept(run(hop));
    sink.shutdown();

    Set<String> urns = entitiesWritten();
    String flow = "urn:li:dataFlow:(hop,orders,prod)";
    assertTrue(urns.contains(flow));
    assertTrue(urns.contains("urn:li:dataJob:(" + flow + ",Read)"));
    assertTrue(urns.contains("urn:li:dataJob:(" + flow + ",Write)"));
    assertTrue(urns.contains("urn:li:dataProcessInstance:" + hop.pipelineRunId()));
    assertTrue(urns.contains("urn:li:dataset:(urn:li:dataPlatform:file,/out/x.csv,PROD)"));
    try (var files = Files.list(spool)) {
      assertEquals(0, files.filter(f -> f.toString().endsWith(".json")).count());
    }
  }

  // A run made while DataHub was down is written by the next process, translated in full.
  @Test
  void whatAnOutageLeftInTheSpoolIsWrittenByTheNextProcess() throws Exception {
    HopEvents hop = new HopEvents("orders");
    DataHubLineageSink during = new DataHubLineageSink();
    during.init(variablesWithShortDrain(), new LogChannel("DataHubSinkOutage"));
    during.accept(run(hop));
    during.shutdown();
    try (var files = Files.list(spool)) {
      assertTrue(files.anyMatch(f -> f.toString().endsWith(".json")), "the run waits on disk");
    }

    gms.stubFor(post(urlEqualTo(INGEST)).willReturn(aResponse().withStatus(200).withBody("{}")));
    DataHubLineageSink after = new DataHubLineageSink();
    after.init(variables("http://localhost:" + gms.getPort()), new LogChannel("DataHubSinkAfter"));
    after.shutdown();

    Set<String> urns = entitiesWritten();
    assertTrue(urns.contains("urn:li:dataJob:(urn:li:dataFlow:(hop,orders,prod),Write)"));
    try (var files = Files.list(spool)) {
      assertEquals(0, files.filter(f -> f.toString().endsWith(".json")).count());
    }
  }

  /** A GMS that is not there (nothing listens on port 1), and a drain that does not wait long. */
  private Variables variablesWithShortDrain() {
    Variables variables = variables("http://localhost:1");
    variables.setVariable("HOP_LINEAGE_DATAHUB_MAX_RETRIES", "0");
    variables.setVariable("HOP_LINEAGE_DATAHUB_SHUTDOWN_DRAIN_MS", "1000");
    variables.setVariable("HOP_LINEAGE_DATAHUB_CONNECT_TIMEOUT_MS", "200");
    return variables;
  }

  @Test
  void aMissingUrlFailsTheInitialisation() {
    assertThrows(
        HopException.class,
        () -> new DataHubLineageSink().init(new Variables(), new LogChannel("DataHubSinkNoUrl")));
  }
}
