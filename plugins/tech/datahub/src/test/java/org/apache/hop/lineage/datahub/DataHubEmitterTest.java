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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.openlineage.client.OpenLineage.RunEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
    return emitter(policy, lifecycle, 0L);
  }

  /**
   * @param replayedBeforeMs events older than this count as left behind by an earlier process
   */
  private DataHubEmitter emitter(SchemaPolicy policy, boolean lifecycle, long replayedBeforeMs)
      throws Exception {
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
        new LogChannel("DataHubEmitterTest"),
        replayedBeforeMs);
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

  // DataHub cannot be read for now (down, a proxy in the way, a token without read rights): the
  // lineage still goes through, and the event is kept so the schema decision is made later.
  @Test
  void anUnreadableDataHubGetsTheLineageAndTheEventIsKept() throws Exception {
    ingestAnswers(200);
    gms.stubFor(post(urlEqualTo(GRAPHQL)).willReturn(aResponse().withStatus(500)));

    assertEquals(Result.RETRY, emitter(SchemaPolicy.MISSING, false).emit(writeCompleted()));

    List<JsonNode> proposals = proposalsSent();
    assertFalse(sent(proposals, OUT, "schemaMetadata"));
    assertTrue(sent(proposals, OUT, "status"));
  }

  // A query DataHub cannot run (a field an older version lacks) fails the same way every time:
  // retrying would only keep the event forever, so the decision is skipped.
  @Test
  void aQueryDataHubRefusesSkipsTheDecision() throws Exception {
    ingestAnswers(200);
    gms.stubFor(
        post(urlEqualTo(GRAPHQL))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(
                        "{\"errors\":[{\"message\":\"Validation error\","
                            + "\"extensions\":{\"classification\":\"ValidationError\"}}]}")));

    assertEquals(Result.SENT, emitter(SchemaPolicy.MISSING, false).emit(writeCompleted()));
    assertFalse(sent(proposalsSent(), OUT, "schemaMetadata"));
  }

  // A token without the privilege to read gets a 403, as HTTP status or inside the GraphQL answer.
  // That lasts until someone grants it, and events go out in order: kept, the event would hold up
  // all lineage behind it. It is written without the decisions that need the read.
  @Test
  void aReadDataHubForbidsSkipsTheDecisions() throws Exception {
    ingestAnswers(200);
    gms.stubFor(
        post(urlEqualTo(GRAPHQL))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(
                        "{\"errors\":[{\"message\":\"Unauthorized\","
                            + "\"extensions\":{\"code\":403,\"type\":\"UNAUTHORIZED\"}}]}")));

    assertEquals(Result.SENT, emitter(SchemaPolicy.MISSING, false).emit(writeCompleted()));
    List<JsonNode> proposals = proposalsSent();
    assertFalse(sent(proposals, OUT, "schemaMetadata"));
    assertTrue(sent(proposals, OUT, "status"));

    gms.resetAll();
    ingestAnswers(200);
    gms.stubFor(post(urlEqualTo(GRAPHQL)).willReturn(aResponse().withStatus(403)));
    List<RunEvent> run =
        new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv");
    List<Result> results = emitter(SchemaPolicy.MISSING, false).emitAll(run);
    assertTrue(results.stream().allMatch(r -> r == Result.SENT), results.toString());
  }

  // One invalid aspect fails the whole batch; split until it is found, it only costs that aspect,
  // in far fewer requests than one per proposal.
  @Test
  void aRefusedBatchIsSplitUntilTheBadProposalIsFound() throws Exception {
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
    int requests = gms.findAll(postRequestedFor(urlEqualTo(INGEST))).size();
    assertTrue(
        requests <= 1 + 2 * (int) Math.ceil(Math.log(accepted.size() + 1.0) / Math.log(2)),
        requests + " requests");
  }

  // Many bad proposals cost a bounded number of requests: what is not singled out by then is
  // refused together, rather than holding up the queue with a request per proposal.
  @Test
  void isolatingRefusedProposalsHasABudget() throws Exception {
    ingestAnswers(400);
    datahubHas("[]");
    List<RunEvent> events = new ArrayList<>();
    for (int run = 0; run < 6; run++) {
      events.addAll(
          new HopEvents("orders" + run).readThenWrite("file:///data/in.csv", "file:///out/x.csv"));
    }

    List<Result> results = emitter(SchemaPolicy.NEVER, false).emitAll(events);

    assertTrue(results.stream().allMatch(r -> r == Result.REJECTED), results.toString());
    DataHubTranslator translator =
        new DataHubTranslator("PROD", "prod", null, "urn:li:corpuser:datahub", false);
    int proposals =
        1 + events.stream().mapToInt(e -> translator.translate(e).proposals().size()).sum();
    int batches = (proposals + DataHubEmitter.MAX_BATCH - 1) / DataHubEmitter.MAX_BATCH;
    int requests = gms.findAll(postRequestedFor(urlEqualTo(INGEST))).size();
    assertTrue(requests <= batches * (1 + DataHubEmitter.MAX_ISOLATION_REQUESTS), requests + "");
    assertTrue(requests < proposals, "not one request per proposal");
  }

  // Waiting between attempts doubles, up to a ceiling, however many retries are configured.
  @Test
  void theBackoffIsCapped() throws Exception {
    DataHubEmitter emitter = emitter(SchemaPolicy.NEVER, false);
    assertEquals(1, emitter.backoff(0));
    assertEquals(8, emitter.backoff(3));
    assertEquals(DataHubEmitter.MAX_BACKOFF_MS, emitter.backoff(16));
    assertEquals(DataHubEmitter.MAX_BACKOFF_MS, emitter.backoff(Integer.MAX_VALUE));
  }

  // Of the events in one dispatcher batch, only those whose proposals did not get through are
  // kept; the ones DataHub took are done, even though a later request failed.
  @Test
  void eachEventOfABatchGetsItsOwnOutcome() throws Exception {
    ingestAnswers(200);
    datahubHas("[]");
    gms.stubFor(
        post(urlEqualTo(INGEST))
            .withRequestBody(containing("orders7.hpl"))
            .willReturn(aResponse().withStatus(503)));
    List<RunEvent> first = new ArrayList<>();
    List<RunEvent> events = new ArrayList<>();
    for (int run = 0; run < 8; run++) {
      List<RunEvent> runEvents =
          new HopEvents("orders" + run).readThenWrite("file:///data/in.csv", "file:///out/x.csv");
      if (run == 0) {
        first.addAll(runEvents);
      }
      events.addAll(runEvents);
    }

    List<Result> results = emitter(SchemaPolicy.NEVER, false).emitAll(events);

    assertTrue(
        results.subList(0, first.size()).stream().allMatch(r -> r == Result.SENT),
        results.toString());
    assertEquals(Result.RETRY, results.get(results.size() - 1));
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
    String flow = "urn:li:dataFlow:(hop,orders.hpl,prod)";
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
                            + job(read, "${PROJECT_HOME}/orders.hpl")
                            + ","
                            + job(old, "${PROJECT_HOME}/orders.hpl")
                            + ","
                            + job(elsewhere, "${PROJECT_HOME}/other/orders.hpl")
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

  private static final String FLOW = "urn:li:dataFlow:(hop,orders.hpl,prod)";
  private static final String READ = "urn:li:dataJob:(" + FLOW + ",Read)";
  private static final String WRITE = "urn:li:dataJob:(" + FLOW + ",Write)";

  private static void graphqlAnswers(String queryPart, ObjectNode data) {
    ObjectNode body = MAPPER.createObjectNode();
    body.set("data", data);
    gms.stubFor(
        post(urlEqualTo(GRAPHQL))
            .withRequestBody(containing(queryPart))
            .willReturn(aResponse().withStatus(200).withBody(body.toString())));
  }

  /** DataHub's answer to the runs query: each task's newest run started at the given time. */
  private static void newestRuns(Map<String, Long> startedAt) {
    ObjectNode data = MAPPER.createObjectNode();
    ArrayNode entities = data.putArray("entities");
    startedAt.forEach(
        (urn, time) -> {
          ObjectNode job = entities.addObject().put("urn", urn);
          job.putObject("runs")
              .putArray("runs")
              .addObject()
              .putObject("properties")
              .putObject("created")
              .put("time", time);
        });
    graphqlAnswers("runs(start", data);
  }

  private static List<JsonNode> sentFor(List<JsonNode> proposals, String urn, String aspect) {
    return proposals.stream()
        .filter(
            p ->
                p.get("entityUrn").asText().equals(urn)
                    && p.get("aspectName").asText().equals(aspect))
        .toList();
  }

  private static JsonNode value(JsonNode proposal) throws Exception {
    return MAPPER.readTree(proposal.get("aspect").get("value").asText());
  }

  // A run whose events wait in the spool while a newer run of the same pipeline goes through is
  // written as a run, but what it says about the pipeline's tasks does not replace the newer
  // version's.
  @Test
  void aLateEventDoesNotUndoANewerRun() throws Exception {
    ingestAnswers(200);
    datahubHas("[]");
    DataHubEmitter emitter = emitter(SchemaPolicy.NEVER, false);
    emitter.emitAll(
        new HopEvents("orders", "v2")
            .later(60_000)
            .readThenWrite("file:///data/in.csv", "file:///out/v2.csv"));
    gms.resetRequests();

    HopEvents late = new HopEvents("orders", "v1");
    List<Result> results =
        emitter.emitAll(late.readThenWrite("file:///data/in.csv", "file:///out/v1.csv"));

    assertTrue(results.stream().allMatch(r -> r == Result.SENT), results.toString());
    List<JsonNode> proposals = proposalsSent();
    assertTrue(sentFor(proposals, WRITE, "dataJobInputOutput").isEmpty(), "v2's outputs stay");
    assertTrue(
        sent(proposals, "urn:li:dataset:(urn:li:dataPlatform:file,/out/v1.csv,PROD)", "status"));
    assertTrue(
        sent(
            proposals,
            DataHubUrns.dataProcessInstance(UUID.fromString(late.pipelineRunId())),
            "dataProcessInstanceRunEvent"),
        "the late run itself is still recorded");
  }

  // The same, across processes: an event an earlier process left in the spool is older than the
  // task's newest run in DataHub, so the task keeps what that newer run wrote.
  @Test
  void aReplayedEventOlderThanTheTasksLatestRunLeavesTheTaskAlone() throws Exception {
    ingestAnswers(200);
    datahubHas("[]");
    newestRuns(Map.of(WRITE, HopEvents.STARTED_AT + 3_600_000L));

    emitter(SchemaPolicy.NEVER, false, Long.MAX_VALUE)
        .emitAll(new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv"));

    List<JsonNode> proposals = proposalsSent();
    assertTrue(sentFor(proposals, WRITE, "dataJobInputOutput").isEmpty());
    assertTrue(sentFor(proposals, WRITE, "dataJobInfo").isEmpty());
    assertFalse(sentFor(proposals, READ, "dataJobInputOutput").isEmpty(), "Read has not run since");
    assertTrue(sent(proposals, OUT, "status"), "the datasets are still written");
  }

  // A transform added in a newer version of the pipeline has run since the late run being
  // checked: it is that run's pipeline that is out of date, not the transform.
  @Test
  void aLateRunDoesNotRetireTasksThatRanSince() throws Exception {
    ingestAnswers(200);
    datahubHas("[]");
    String sort = "urn:li:dataJob:(" + FLOW + ",Sort rows)";
    newestRuns(Map.of(sort, HopEvents.STARTED_AT + 60_000L));
    gms.stubFor(
        post(urlEqualTo(GRAPHQL))
            .withRequestBody(containing("IsPartOf"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(
                        "{\"data\":{\"entities\":[{\"urn\":\""
                            + FLOW
                            + "\",\"jobs\":{\"relationships\":["
                            + job(READ, "${PROJECT_HOME}/orders.hpl")
                            + ","
                            + job(sort, "${PROJECT_HOME}/orders.hpl")
                            + "]}}]}}")));

    emitter(SchemaPolicy.NEVER, false)
        .emitAll(new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv"));

    assertTrue(sentFor(proposalsSent(), sort, "status").isEmpty());
  }

  // A pipeline that two workflows run hangs under both: writing its first transform's inputs and
  // outputs for one keeps the other's link, which DataHub already has.
  @Test
  void aFirstTaskKeepsTheParentsOtherWorkflowsGaveIt() throws Exception {
    ingestAnswers(200);
    datahubHas("[]");
    String weekly = "urn:li:dataJob:(urn:li:dataFlow:(hop,weekly.hwf,prod),Run orders)";
    String nightly = "urn:li:dataJob:(urn:li:dataFlow:(hop,nightly.hwf,prod),Run orders)";
    String formerUpstream = "urn:li:dataJob:(" + FLOW + ",Generate rows)";
    ObjectNode stored = MAPPER.createObjectNode();
    stored.putArray("inputDatajobEdges").addObject().put("destinationUrn", weekly);
    ((ArrayNode) stored.get("inputDatajobEdges")).addObject().put("destinationUrn", formerUpstream);
    ObjectNode data = MAPPER.createObjectNode();
    data.putArray("entities")
        .addObject()
        .put("urn", READ)
        .putArray("aspects")
        .addObject()
        .put("aspectName", "dataJobInputOutput")
        .put("payload", stored.toString());
    graphqlAnswers("aspectNames", data);

    emitter(SchemaPolicy.NEVER, false)
        .emitAll(
            new HopEvents("orders")
                .startedBy("nightly", "Run orders")
                .readThenWrite("file:///data/in.csv", "file:///out/x.csv"));

    List<JsonNode> proposals = proposalsSent();
    JsonNode io =
        value(
            sentFor(proposals, READ, "dataJobInputOutput").stream()
                .filter(p -> p.get("changeType").asText().equals("UPSERT"))
                .findFirst()
                .orElseThrow());
    List<String> parents = new ArrayList<>();
    io.get("inputDatajobEdges").forEach(e -> parents.add(e.get("destinationUrn").asText()));
    assertEquals(List.of(weekly), parents, "the other workflow's link stays, the old hop goes");
    assertTrue(
        sentFor(proposals, READ, "dataJobInputOutput").stream()
            .anyMatch(
                p ->
                    p.get("changeType").asText().equals("PATCH")
                        && p.get("aspect").get("value").asText().contains(nightly)),
        "this run's parent is linked as well");
  }

  // Without knowing which parents DataHub has, the first task's inputs and outputs wait: written
  // now, they could drop a workflow's link.
  @Test
  void aFirstTaskWaitsWhenItsParentsCannotBeRead() throws Exception {
    ingestAnswers(200);
    datahubHas("[]");
    gms.stubFor(
        post(urlEqualTo(GRAPHQL))
            .withRequestBody(containing("aspectNames"))
            .willReturn(aResponse().withStatus(503)));
    List<RunEvent> events =
        new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv");

    List<Result> results = emitter(SchemaPolicy.NEVER, false).emitAll(events);

    List<JsonNode> proposals = proposalsSent();
    assertTrue(
        sentFor(proposals, READ, "dataJobInputOutput").stream()
            .noneMatch(p -> p.get("changeType").asText().equals("UPSERT")));
    assertTrue(results.contains(Result.RETRY));
    assertTrue(sent(proposals, OUT, "status"), "the rest is written");
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
    for (String query :
        List.of(
            DataHubEmitter.DATASETS_QUERY,
            DataHubEmitter.FLOW_JOBS_QUERY,
            DataHubEmitter.JOB_RUNS_QUERY,
            DataHubEmitter.JOB_IO_QUERY)) {
      int depth = 0;
      for (char c : query.toCharArray()) {
        depth += c == '{' || c == '(' || c == '[' ? 1 : c == '}' || c == ')' || c == ']' ? -1 : 0;
        assertTrue(depth >= 0, query);
      }
      assertEquals(0, depth, query);
    }
  }
}
