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
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.notContaining;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.openlineage.client.OpenLineage.RunEvent;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.datahub.DataHubSinkConfig.SchemaPolicy;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.lineage.openlineage.OpenLineageEmitter.Result;
import org.apache.hop.lineage.openlineage.OpenLineageSinkConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Delivery to a WireMock stand-in for DataHub GMS. */
class DataHubEmitterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String INGEST = "/aspects?action=ingestProposalBatch";
  private static final String GRAPHQL = "/api/graphql";
  private static final String OUT = "urn:li:dataset:(urn:li:dataPlatform:file,/out/x.csv,PROD)";

  @RegisterExtension
  static WireMockExtension gms =
      WireMockExtension.newInstance()
          .options(WireMockConfiguration.wireMockConfig().dynamicPort())
          .build();

  @BeforeAll
  static void initLogStore() {
    HopLogStore.init();
  }

  @BeforeEach
  void reset() {
    gms.resetAll();
  }

  private DataHubEmitter emitter(SchemaPolicy policy, boolean lifecycle) throws Exception {
    Variables variables = new Variables();
    variables.setVariable(OpenLineageSinkConfig.VAR_URL, "http://localhost:" + gms.getPort());
    variables.setVariable(OpenLineageSinkConfig.VAR_RETRY_BACKOFF_MS, "1");
    OpenLineageSinkConfig http = OpenLineageSinkConfig.fromVariables(variables);
    return new DataHubEmitter(
        new DataHubTranslator("PROD", "prod", null, "urn:li:corpuser:datahub", lifecycle),
        new DataHubClient("http://localhost:" + gms.getPort(), "secret-token", http),
        policy,
        "urn:li:corpuser:datahub",
        2,
        1,
        new LogChannel("DataHubEmitterTest"));
  }

  /** The Write transform's COMPLETE: it wrote /out/x.csv, with a schema. */
  private static RunEvent writeCompleted() {
    for (RunEvent event :
        new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv")) {
      if (event.getJob().getName().equals("orders/Write")
          && event.getEventType() == RunEvent.EventType.COMPLETE) {
        return event;
      }
    }
    throw new AssertionError("no Write completion");
  }

  private static void datahubHas(String entitiesJson) {
    gms.stubFor(
        post(urlEqualTo(GRAPHQL))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody("{\"data\":{\"entities\":" + entitiesJson + "}}")));
  }

  private static void ingestAnswers(int status) {
    gms.stubFor(post(urlEqualTo(INGEST)).willReturn(aResponse().withStatus(status).withBody("{}")));
  }

  private static List<JsonNode> proposalsSent() throws Exception {
    List<JsonNode> proposals = new ArrayList<>();
    for (var request : gms.findAll(postRequestedFor(urlEqualTo(INGEST)))) {
      MAPPER.readTree(request.getBodyAsString()).get("proposals").forEach(proposals::add);
    }
    return proposals;
  }

  private static boolean sent(List<JsonNode> proposals, String urn, String aspect) {
    return proposals.stream()
        .anyMatch(
            p ->
                p.get("entityUrn").asText().equals(urn)
                    && p.get("aspectName").asText().equals(aspect));
  }

  @Test
  void writesAnEventInOneBatchWithTheToken() throws Exception {
    ingestAnswers(200);
    datahubHas("[{\"urn\":\"" + OUT + "\",\"exists\":false,\"schemaMetadata\":null}]");

    assertEquals(Result.SENT, emitter(SchemaPolicy.MISSING, false).emit(writeCompleted()));

    gms.verify(
        1,
        postRequestedFor(urlEqualTo(INGEST))
            .withHeader("Authorization", equalTo("Bearer secret-token")));
    List<JsonNode> proposals = proposalsSent();
    assertTrue(sent(proposals, DataHubUrns.HOP_PLATFORM_URN, "dataPlatformInfo"));
    assertTrue(sent(proposals, OUT, "schemaMetadata"), "a dataset without a schema gets Hop's");
  }

  // A dataset DataHub's own database or file ingestion describes keeps that description.
  @Test
  void leavesASchemaAnotherSourceWroteAlone() throws Exception {
    ingestAnswers(200);
    datahubHas(
        "[{\"urn\":\"" + OUT + "\",\"exists\":true,\"schemaMetadata\":{\"name\":\"public.x\"}}]");

    assertEquals(Result.SENT, emitter(SchemaPolicy.MISSING, false).emit(writeCompleted()));

    assertFalse(sent(proposalsSent(), OUT, "schemaMetadata"));
  }

  @Test
  void replacesASchemaHopWroteEarlier() throws Exception {
    ingestAnswers(200);
    datahubHas("[{\"urn\":\"" + OUT + "\",\"exists\":true,\"schemaMetadata\":{\"name\":\"hop\"}}]");

    emitter(SchemaPolicy.MISSING, false).emit(writeCompleted());

    assertTrue(sent(proposalsSent(), OUT, "schemaMetadata"));
  }

  // DataHub cannot be read (a token without read rights, a GraphQL error): the lineage still goes
  // through, the schema decision is skipped.
  @Test
  void anUnreadableDataHubStillGetsTheLineage() throws Exception {
    ingestAnswers(200);
    gms.stubFor(post(urlEqualTo(GRAPHQL)).willReturn(aResponse().withStatus(500)));

    assertEquals(Result.SENT, emitter(SchemaPolicy.MISSING, false).emit(writeCompleted()));

    List<JsonNode> proposals = proposalsSent();
    assertFalse(sent(proposals, OUT, "schemaMetadata"));
    assertTrue(sent(proposals, OUT, "status"));
  }

  // One invalid aspect fails the whole batch; sent one by one, it only costs that aspect.
  @Test
  void aRefusedBatchIsSentOneProposalAtATime() throws Exception {
    gms.stubFor(
        post(urlEqualTo(INGEST))
            .withRequestBody(containing("schemaMetadata"))
            .willReturn(aResponse().withStatus(400).withBody("{\"message\":\"invalid\"}")));
    gms.stubFor(
        post(urlEqualTo(INGEST))
            .withRequestBody(notContaining("schemaMetadata"))
            .willReturn(aResponse().withStatus(200).withBody("{}")));
    datahubHas("[]");

    DataHubEmitter emitter = emitter(SchemaPolicy.ALWAYS, false);
    assertEquals(Result.SENT, emitter.emit(writeCompleted()));

    List<JsonNode> accepted = new ArrayList<>();
    for (var request : gms.findAll(postRequestedFor(urlEqualTo(INGEST)))) {
      if (!request.getBodyAsString().contains("schemaMetadata")) {
        MAPPER.readTree(request.getBodyAsString()).get("proposals").forEach(accepted::add);
      }
    }
    assertTrue(sent(accepted, OUT, "status"), "the rest of the event is still written");
  }

  // A wrong token is a configuration problem: the event is kept, not retried on the spot, and not
  // broken up into single proposals either.
  @Test
  void unauthorizedKeepsTheEventForLater() throws Exception {
    ingestAnswers(401);

    assertEquals(Result.RETRY, emitter(SchemaPolicy.NEVER, false).emit(writeCompleted()));
    gms.verify(1, postRequestedFor(urlEqualTo(INGEST)));
  }

  @Test
  void anUnavailableGmsIsRetriedAndThenKept() throws Exception {
    ingestAnswers(503);

    assertEquals(Result.RETRY, emitter(SchemaPolicy.NEVER, false).emit(writeCompleted()));
    gms.verify(3, postRequestedFor(urlEqualTo(INGEST)));
  }

  // A deprecation a person wrote is not lifted by a run writing the dataset again.
  @Test
  void aWriteLiftsOnlyAHopDeprecation() throws Exception {
    ingestAnswers(200);
    datahubHas(
        "[{\"urn\":\""
            + OUT
            + "\",\"exists\":true,\"deprecation\":{\"deprecated\":true,\"note\":\"Use v2 - Alice\"}}]");
    emitter(SchemaPolicy.NEVER, true).emit(writeCompleted());
    assertFalse(sent(proposalsSent(), OUT, "deprecation"));

    gms.resetAll();
    ingestAnswers(200);
    datahubHas(
        "[{\"urn\":\""
            + OUT
            + "\",\"exists\":true,\"deprecation\":{\"deprecated\":true,\"note\":\"Hop: deleted by"
            + " orders/Clean.\"}}]");
    emitter(SchemaPolicy.NEVER, true).emit(writeCompleted());
    List<JsonNode> proposals = proposalsSent();
    JsonNode cleared =
        proposals.stream()
            .filter(p -> p.get("aspectName").asText().equals("deprecation"))
            .findFirst()
            .orElseThrow();
    assertFalse(
        MAPPER.readTree(cleared.get("aspect").get("value").asText()).get("deprecated").asBoolean());
  }

  // The platform info goes out with the first successful event of a process, not with every one.
  @Test
  void thePlatformIsDescribedOnce() throws Exception {
    ingestAnswers(200);
    DataHubEmitter emitter = emitter(SchemaPolicy.NEVER, false);
    HopEvents hop = new HopEvents("orders");
    for (RunEvent event :
        HopEvents.mapper()
            .mapBatch(
                List.of(
                    hop.pipeline(RunLifecyclePhase.STARTED),
                    hop.pipeline(RunLifecyclePhase.FINISHED)))) {
      emitter.emit(event);
    }
    long described =
        proposalsSent().stream()
            .filter(p -> p.get("aspectName").asText().equals("dataPlatformInfo"))
            .count();
    assertEquals(1, described);
  }

  // One batch can lift Hop's deprecation of a file (a copy writes it) and then set a new one (a
  // move takes it away): decided in order, the last one wins.
  @Test
  void lifecycleChangesInOneBatchAreDecidedInOrder() throws Exception {
    ingestAnswers(200);
    String landing = "urn:li:dataset:(urn:li:dataPlatform:file,/landing/a.csv,PROD)";
    datahubHas(
        "[{\"urn\":\""
            + landing
            + "\",\"exists\":true,\"deprecation\":{\"deprecated\":true,\"note\":\"Hop: moved"
            + " earlier.\"}}]");
    HopEvents hop = new HopEvents("archive");
    List<RunEvent> events =
        HopEvents.mapper()
            .mapBatch(
                List.of(
                    hop.graph("Copy>Move"),
                    hop.write("Copy", "file:///landing/a.csv"),
                    hop.move("Move", "file:///landing/a.csv", "file:///archive/a.csv"),
                    hop.transform("Copy", RunLifecyclePhase.FINISHED),
                    hop.transform("Move", RunLifecyclePhase.FINISHED),
                    hop.pipeline(RunLifecyclePhase.FINISHED)));

    emitter(SchemaPolicy.NEVER, true).emitAll(events);

    List<JsonNode> deprecations =
        proposalsSent().stream()
            .filter(
                p ->
                    p.get("aspectName").asText().equals("deprecation")
                        && p.get("entityUrn").asText().equals(landing))
            .toList();
    JsonNode last =
        MAPPER.readTree(
            deprecations.get(deprecations.size() - 1).get("aspect").get("value").asText());
    assertTrue(last.get("deprecated").asBoolean(), "the move retires the file again");
  }

  // A transform removed from the pipeline is retired in DataHub; a task of a same-named pipeline
  // in another file is not.
  @Test
  void retiresTheTasksAPipelineNoLongerHas() throws Exception {
    ingestAnswers(200);
    // WireMock answers with the stub added last that matches, so the flow query's stub goes last.
    datahubHas("[]");
    String flow = "urn:li:dataFlow:(hop,orders,prod)";
    String old = "urn:li:dataJob:(" + flow + ",Old transform)";
    String elsewhere = "urn:li:dataJob:(" + flow + ",Other project)";
    String read = "urn:li:dataJob:(" + flow + ",Read)";
    gms.stubFor(
        post(urlEqualTo(GRAPHQL))
            .withRequestBody(containing("DataFlow"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(
                        "{\"data\":{\"entities\":[{\"urn\":\""
                            + flow
                            + "\",\"jobs\":{\"relationships\":["
                            + job(read, "/projects/demo/orders.hpl")
                            + ","
                            + job(old, "/projects/demo/orders.hpl")
                            + ","
                            + job(elsewhere, "/projects/other/orders.hpl")
                            + "]}}]}}")));

    emitter(SchemaPolicy.NEVER, false)
        .emitAll(new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv"));

    List<JsonNode> removed =
        proposalsSent().stream()
            .filter(
                p ->
                    p.get("aspectName").asText().equals("status")
                        && p.get("aspect").get("value").asText().contains("true"))
            .toList();
    assertEquals(1, removed.size());
    assertEquals(old, removed.get(0).get("entityUrn").asText());
  }

  private static String job(String urn, String filename) {
    return "{\"entity\":{\"urn\":\""
        + urn
        + "\",\"status\":{\"removed\":false},\"properties\":{\"customProperties\":[{\"key\":\"filename\","
        + "\"value\":\""
        + filename
        + "\"}]}}}";
  }

  // WireMock does not parse GraphQL; an unbalanced query only fails against a real DataHub.
  @Test
  void theGraphqlQueriesAreBalanced() {
    for (String query : List.of(DataHubEmitter.DATASETS_QUERY, DataHubEmitter.FLOW_JOBS_QUERY)) {
      int depth = 0;
      for (char c : query.toCharArray()) {
        depth += c == '{' || c == '(' || c == '[' ? 1 : c == '}' || c == ')' || c == ']' ? -1 : 0;
        assertTrue(depth >= 0, query);
      }
      assertEquals(0, depth, query);
    }
  }
}
