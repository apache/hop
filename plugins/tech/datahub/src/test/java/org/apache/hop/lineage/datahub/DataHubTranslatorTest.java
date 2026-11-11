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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.lineage.LineageRunLifecycleEmitter;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.datahub.DataHubTranslator.LifecycleChange;
import org.apache.hop.lineage.datahub.DataHubTranslator.Mcp;
import org.apache.hop.lineage.datahub.DataHubTranslator.Translation;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload.Transition;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.lineage.openlineage.OpenLineageEventMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** What a Hop run looks like in DataHub: flows, jobs, runs and the datasets in between. */
class DataHubTranslatorTest {

  private static final String FLOW = "urn:li:dataFlow:(hop,orders.hpl,prod)";
  private static final String READ = "urn:li:dataJob:(" + FLOW + ",Read)";
  private static final String WRITE = "urn:li:dataJob:(" + FLOW + ",Write)";
  private static final String IN = "urn:li:dataset:(urn:li:dataPlatform:file,/data/in.csv,PROD)";
  private static final String OUT = "urn:li:dataset:(urn:li:dataPlatform:file,/out/x.csv,PROD)";

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

  private static Mcp one(List<Mcp> proposals, String urn, String aspect) {
    List<Mcp> found = find(proposals, urn, aspect);
    assertFalse(found.isEmpty(), "no " + aspect + " for " + urn + " in " + proposals);
    return found.get(found.size() - 1);
  }

  private static List<String> destinations(JsonNode edges) {
    List<String> urns = new ArrayList<>();
    edges.forEach(edge -> urns.add(edge.get("destinationUrn").asText()));
    return urns;
  }

  // A pipeline is one flow and its transforms are the jobs inside it — not one flow per transform.
  @Test
  void aPipelineIsAFlowAndItsTransformsAreItsJobs() {
    List<Mcp> proposals =
        all(new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv"));

    assertEquals("orders", one(proposals, FLOW, "dataFlowInfo").value().get("name").asText());
    assertEquals(
        "Pipeline", one(proposals, FLOW, "subTypes").value().get("typeNames").get(0).asText());
    JsonNode write = one(proposals, WRITE, "dataJobInfo").value();
    assertEquals("Write", write.get("name").asText());
    assertEquals(FLOW, write.get("flowUrn").asText());
    assertEquals(
        "Transform", one(proposals, WRITE, "subTypes").value().get("typeNames").get(0).asText());
    assertTrue(
        proposals.stream().noneMatch(m -> m.entityUrn().startsWith("urn:li:dataFlow:(hop,orders/")),
        "no flow per transform");
  }

  // Descriptions from the Hop metadata describe the pipeline and its tasks. Only the pipeline's own
  // events write the pipeline's info, so a transform's start cannot erase its description.
  @Test
  void descriptionsComeFromTheHopMetadata() {
    List<Mcp> proposals =
        all(new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv"));

    List<Mcp> flowInfo = find(proposals, FLOW, "dataFlowInfo");
    assertEquals(1, flowInfo.size(), "only the pipeline's start writes the pipeline's info");
    assertEquals("Pipeline orders", flowInfo.get(0).value().get("description").asText());
    assertEquals(
        "Transform Write",
        one(proposals, WRITE, "dataJobInfo").value().get("description").asText());
  }

  // Each transform holds its own datasets, and the hop graph links the jobs: a.csv → Read → Write →
  // x.csv, with no direct a.csv → Write shortcut.
  @Test
  void transformsHoldTheirOwnDatasetsAndTheHopGraphLinksThem() {
    List<Mcp> proposals =
        all(new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv"));

    JsonNode read = one(proposals, READ, "dataJobInputOutput").value();
    assertEquals(List.of(IN), destinations(read.get("inputDatasetEdges")));
    assertEquals(List.of(), destinations(read.get("outputDatasetEdges")));

    JsonNode write = one(proposals, WRITE, "dataJobInputOutput").value();
    assertEquals(List.of(), destinations(write.get("inputDatasetEdges")));
    assertEquals(List.of(OUT), destinations(write.get("outputDatasetEdges")));
    assertEquals(List.of(READ), destinations(write.get("inputDatajobEdges")));
  }

  // Runs are named for humans, belong to their job, and point at the run that started them.
  @Test
  void aRunHasAReadableNameItsJobAndItsParentRun() {
    HopEvents hop = new HopEvents("orders");
    List<Mcp> proposals = all(hop.readThenWrite("file:///data/in.csv", "file:///out/x.csv"));
    String writeRun = "urn:li:dataProcessInstance:" + hop.transformRunId("Write");

    assertEquals(
        "orders/Write 2025-10-18T10:00Z",
        one(proposals, writeRun, "dataProcessInstanceProperties").value().get("name").asText());
    JsonNode relationships = one(proposals, writeRun, "dataProcessInstanceRelationships").value();
    assertEquals(WRITE, relationships.get("parentTemplate").asText());
    assertEquals(
        "urn:li:dataProcessInstance:" + hop.pipelineRunId(),
        relationships.get("parentInstance").asText());

    List<Mcp> states = find(proposals, writeRun, "dataProcessInstanceRunEvent");
    assertEquals("STARTED", states.get(0).value().get("status").asText());
    assertEquals("COMPLETE", states.get(1).value().get("status").asText());
    assertEquals("SUCCESS", states.get(1).value().get("result").get("type").asText());

    String pipelineRun = "urn:li:dataProcessInstance:" + hop.pipelineRunId();
    assertEquals(
        FLOW,
        one(proposals, pipelineRun, "dataProcessInstanceRelationships")
            .value()
            .get("parentTemplate")
            .asText());
  }

  @Test
  void aFailedRunEndsInFailure() {
    HopEvents hop = new HopEvents("orders");
    List<RunEvent> events =
        HopEvents.mapper()
            .mapBatch(
                List.of(
                    hop.pipeline(RunLifecyclePhase.STARTED),
                    hop.pipeline(RunLifecyclePhase.FAILED)));

    Mcp ended =
        find(
                all(events),
                "urn:li:dataProcessInstance:" + hop.pipelineRunId(),
                "dataProcessInstanceRunEvent")
            .get(1);
    assertEquals("FAILURE", ended.value().get("result").get("type").asText());
  }

  // A pipeline run by a workflow action continues the workflow's graph: its first transforms list
  // the action as their upstream job.
  @Test
  void aPipelineRunByAnActionHangsUnderThatAction() {
    List<Mcp> proposals =
        all(
            new HopEvents("orders")
                .startedBy("nightly", "Run orders")
                .readThenWrite("file:///data/in.csv", "file:///out/x.csv"));

    String action = "urn:li:dataJob:(urn:li:dataFlow:(hop,nightly.hwf,prod),Run orders)";
    List<Mcp> links =
        proposals.stream()
            .filter(
                m -> m.changeType().equals("PATCH") && m.aspectName().equals("dataJobInputOutput"))
            .toList();
    assertEquals(1, links.size(), "only the root transform is linked");
    assertEquals(READ, links.get(0).entityUrn());
    assertEquals(action, links.get(0).value().get(0).get("value").get("destinationUrn").asText());
  }

  // Names with the characters that delimit a URN are encoded, the way DataHub's own clients do.
  @Test
  void namesThatWouldBreakAUrnAreEncoded() {
    List<Mcp> proposals =
        all(
            new HopEvents("load (eu, us)")
                .readThenWrite("file:///data/a,b.csv", "file:///out/x.csv"));

    String flow = "urn:li:dataFlow:(hop,load %28eu%2C us%29.hpl,prod)";
    assertEquals(
        "load (eu, us)", one(proposals, flow, "dataFlowInfo").value().get("name").asText());
    assertNotNull(
        one(proposals, "urn:li:dataset:(urn:li:dataPlatform:file,/data/a%2Cb.csv,PROD)", "status"));
  }

  // The schema of a dataset Hop read or wrote is offered with DataHub types, marked as Hop's.
  @Test
  void datasetSchemasAreTypedAndMarkedAsHops() {
    List<Translation> translations = new ArrayList<>();
    for (RunEvent event :
        new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv")) {
      translations.add(translator.translate(event));
    }
    JsonNode schema =
        translations.stream()
            .flatMap(t -> t.schemas().stream())
            .filter(s -> s.datasetUrn().equals(OUT))
            .findFirst()
            .orElseThrow()
            .schemaMetadata()
            .value();
    assertEquals(DataHubTranslator.HOP_SCHEMA_NAME, schema.get("schemaName").asText());
    JsonNode id = schema.get("fields").get(0);
    assertEquals("id", id.get("fieldPath").asText());
    assertTrue(id.get("type").get("type").has("com.linkedin.schema.NumberType"));
  }

  // A pipeline of one transform has no hops, and so an empty graph. Its datasets stay with that
  // transform: no job named after the pipeline appears next to it.
  @Test
  void aLoneTransformKeepsItsDatasets() {
    HopEvents hop = new HopEvents("orders");
    List<Mcp> proposals =
        all(
            HopEvents.mapper()
                .mapBatch(
                    List.of(
                        hop.pipeline(RunLifecyclePhase.STARTED),
                        hop.graph(),
                        hop.transform("Write", RunLifecyclePhase.STARTED),
                        hop.write("Write", "file:///out/x.csv", HopEvents.column("id", "Integer")),
                        hop.transform("Write", RunLifecyclePhase.FINISHED),
                        hop.pipeline(RunLifecyclePhase.FINISHED))));

    JsonNode write = one(proposals, WRITE, "dataJobInputOutput").value();
    assertEquals(List.of(OUT), destinations(write.get("outputDatasetEdges")));
    assertTrue(find(proposals, "urn:li:dataJob:(" + FLOW + ",orders)", "dataJobInfo").isEmpty());
  }

  // A query reads some of a table's columns. Offered as the table's schema, that subset would
  // replace the full one a Table Output or DataHub's own ingestion wrote. A file read describes the
  // whole file, and a write what was written, so those are offered.
  @Test
  void aTableReadOffersNoSchemaButAFileReadAndATableWriteDo() {
    RunEvent mapped =
        new HopEvents("orders")
            .readThenWrite("file:///data/in.csv", "file:///out/x.csv").stream()
                .filter(
                    e ->
                        e.getEventType() == RunEvent.EventType.COMPLETE
                            && e.getInputs() != null
                            && !e.getInputs().isEmpty())
                .findFirst()
                .orElseThrow();
    OpenLineage ol = new OpenLineage(URI.create("https://hop.apache.org/test"));
    OpenLineage.DatasetFacets countryOnly =
        ol.newDatasetFacetsBuilder()
            .schema(
                ol.newSchemaDatasetFacetBuilder()
                    .fields(
                        List.of(
                            ol.newSchemaDatasetFacetFieldsBuilder()
                                .name("country")
                                .type("string")
                                .build()))
                    .build())
            .build();
    RunEvent event =
        ol.newRunEventBuilder()
            .eventType(mapped.getEventType())
            .eventTime(mapped.getEventTime())
            .run(mapped.getRun())
            .job(mapped.getJob())
            .inputs(
                List.of(
                    ol.newInputDatasetBuilder()
                        .namespace("postgres://db:5432")
                        .name("shop.public.customers")
                        .facets(
                            withSource(ol, countryOnly, "postgres://db:5432", "postgres://db:5432"))
                        .build(),
                    ol.newInputDatasetBuilder()
                        .namespace("file")
                        .name("/data/in.csv")
                        .facets(withSource(ol, countryOnly, "in.csv", "file:///data/in.csv"))
                        .build()))
            .outputs(
                List.of(
                    ol.newOutputDatasetBuilder()
                        .namespace("postgres://db:5432")
                        .name("shop.public.report")
                        .facets(
                            withSource(ol, countryOnly, "postgres://db:5432", "postgres://db:5432"))
                        .build()))
            .build();

    Set<String> offered = new HashSet<>();
    translator.translate(event).schemas().forEach(s -> offered.add(s.datasetUrn()));
    assertEquals(
        Set.of(IN, "urn:li:dataset:(urn:li:dataPlatform:postgres,shop.public.report,PROD)"),
        offered);
  }

  // A file read describes the whole file on any file system, not only on the ones DataHub has a
  // source for: an SFTP read keeps its schema.
  @Test
  void anSftpReadOffersItsSchema() {
    String sftp = "urn:li:dataset:(urn:li:dataPlatform:sftp,/in/orders.csv,PROD)";
    Set<String> offered = new HashSet<>();
    for (RunEvent event :
        new HopEvents("orders")
            .readThenWrite("sftp://files.example.com/in/orders.csv", "file:///out/x.csv")) {
      translator.translate(event).schemas().forEach(s -> offered.add(s.datasetUrn()));
    }
    assertEquals(Set.of(sftp, OUT), offered);
  }

  /**
   * The facets with the data source the mapper writes: the database for a table (its namespace),
   * the file itself for a file.
   */
  private static OpenLineage.DatasetFacets withSource(
      OpenLineage ol, OpenLineage.DatasetFacets facets, String name, String uri) {
    return ol.newDatasetFacetsBuilder()
        .schema(facets.getSchema())
        .dataSource(ol.newDatasourceDatasetFacet(name, URI.create(uri)))
        .build();
  }

  @Test
  void aDeletedFileIsDeprecatedAndAMovedOneNamesItsSuccessor() {
    HopEvents hop = new HopEvents("archive");
    List<RunEvent> events =
        HopEvents.mapper()
            .mapBatch(
                List.of(
                    hop.pipeline(RunLifecyclePhase.STARTED),
                    hop.graph("Move>Clean"),
                    hop.move("Move", "file:///landing/a.csv", "file:///archive/a.csv"),
                    hop.delete("Clean", "file:///landing/b.csv"),
                    hop.transform("Move", RunLifecyclePhase.FINISHED),
                    hop.transform("Clean", RunLifecyclePhase.FINISHED),
                    hop.pipeline(RunLifecyclePhase.FINISHED)));
    List<LifecycleChange> changes = new ArrayList<>();
    events.forEach(e -> changes.addAll(translator.translate(e).lifecycle()));

    String landingA = "urn:li:dataset:(urn:li:dataPlatform:file,/landing/a.csv,PROD)";
    String archiveA = "urn:li:dataset:(urn:li:dataPlatform:file,/archive/a.csv,PROD)";
    String landingB = "urn:li:dataset:(urn:li:dataPlatform:file,/landing/b.csv,PROD)";
    LifecycleChange moved =
        changes.stream().filter(c -> c.datasetUrn().equals(landingA)).findFirst().orElseThrow();
    assertEquals(LifecycleChange.Kind.DEPRECATE, moved.kind());
    assertEquals(archiveA, moved.replacementUrn());
    LifecycleChange deleted =
        changes.stream().filter(c -> c.datasetUrn().equals(landingB)).findFirst().orElseThrow();
    assertEquals(LifecycleChange.Kind.DEPRECATE, deleted.kind());
    assertNull(deleted.replacementUrn());
  }

  @Test
  void lifecycleIsLeftAloneUnlessEnabled() {
    DataHubTranslator off =
        new DataHubTranslator("PROD", "prod", null, "urn:li:corpuser:datahub", false);
    HopEvents hop = new HopEvents("archive");
    List<RunEvent> events =
        HopEvents.mapper()
            .mapBatch(
                List.of(
                    hop.graph("Clean>Clean2"),
                    hop.delete("Clean", "file:///landing/b.csv"),
                    hop.transform("Clean", RunLifecyclePhase.FINISHED),
                    hop.pipeline(RunLifecyclePhase.FINISHED)));
    events.forEach(e -> assertTrue(off.translate(e).lifecycle().isEmpty()));
  }

  // An event replayed from the spool by a later process is translated exactly like a live one.
  @Test
  void aReplayedEventTranslatesLikeALiveOne() {
    for (RunEvent live :
        new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv")) {
      RunEvent replayed =
          OpenLineageClientUtils.runEventFromJson(OpenLineageClientUtils.toJson(live));
      assertEquals(translator.translate(live), translator.translate(replayed));
    }
  }

  // Hop names are not unique: two files holding a pipeline of the same name are two flows, each
  // with its own tasks, named by its path in the project.
  @Test
  void twoFilesWithPipelinesOfTheSameNameAreTwoFlows() {
    List<Mcp> proposals =
        all(new HopEvents("load").in("sales").readThenWrite("file:///a.csv", "file:///b.csv"));
    proposals.addAll(
        all(new HopEvents("load").in("finance").readThenWrite("file:///c.csv", "file:///d.csv")));

    String sales = "urn:li:dataFlow:(hop,sales/load.hpl,prod)";
    String finance = "urn:li:dataFlow:(hop,finance/load.hpl,prod)";
    assertEquals("load", one(proposals, sales, "dataFlowInfo").value().get("name").asText());
    assertEquals("load", one(proposals, finance, "dataFlowInfo").value().get("name").asText());
    assertNotNull(one(proposals, "urn:li:dataJob:(" + sales + ",Write)", "dataJobInfo"));
    assertNotNull(one(proposals, "urn:li:dataJob:(" + finance + ",Write)", "dataJobInfo"));
  }

  // The sink ends a run the engine never reported the end of: the run is over and did not succeed,
  // and nothing is said about its task, whose datasets it never reported.
  @Test
  void anAbortedRunOnlyEndsTheRun() {
    HopEvents hop = new HopEvents("orders");
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org/test")
            .transformJobs(true)
            .propagateInputs(false)
            .abortOpenRuns(true)
            .build();
    mapper.mapBatch(List.of(hop.pipeline(RunLifecyclePhase.STARTED)));
    List<RunEvent> flushed = mapper.flushObservations();
    assertEquals(1, flushed.size());
    assertEquals(RunEvent.EventType.ABORT, flushed.get(0).getEventType());

    List<Mcp> proposals = translator.translate(flushed.get(0)).proposals();
    assertEquals(
        List.of("dataProcessInstanceRunEvent"), proposals.stream().map(Mcp::aspectName).toList());
    JsonNode ended = proposals.get(0).value();
    assertEquals("COMPLETE", ended.get("status").asText());
    assertEquals("FAILURE", ended.get("result").get("type").asText());
  }

  @Test
  void aPlatformInstanceKeepsInstallationsApart() {
    DataHubTranslator instance =
        new DataHubTranslator("PROD", "prod", "emea", "urn:li:corpuser:datahub", false);
    List<Mcp> proposals = new ArrayList<>();
    for (RunEvent event :
        new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv")) {
      proposals.addAll(instance.translate(event).proposals());
    }
    String flow = "urn:li:dataFlow:(hop,emea.orders.hpl,prod)";
    assertEquals(
        "urn:li:dataPlatformInstance:(urn:li:dataPlatform:hop,emea)",
        one(proposals, flow, "dataPlatformInstance").value().get("instance").asText());
  }

  private static LineageContext workflow(String name, String... parentWorkflowAction) {
    LineageContext.Builder ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.WORKFLOW)
            .workflowName(name)
            .logChannelId(HopEvents.uuid("workflow:" + name));
    if (parentWorkflowAction.length == 2) {
      ctx.putAttribute(
          LineageRunLifecycleEmitter.ATTR_PARENT_WORKFLOW_NAME, parentWorkflowAction[0]);
      ctx.putAttribute(LineageRunLifecycleEmitter.ATTR_PARENT_ACTION_NAME, parentWorkflowAction[1]);
    }
    return ctx.build();
  }

  private static LineageContext action(String workflow, String action) {
    return LineageContext.builder()
        .subjectType(LineageSubjectType.ACTION)
        .workflowName(workflow)
        .actionName(action)
        .logChannelId(HopEvents.uuid("action:" + workflow + "/" + action))
        .putAttribute("workflowLogChannelId", HopEvents.uuid("workflow:" + workflow))
        .build();
  }

  private static LineageEvent event(LineageContext ctx, LineageEventKind kind, Object payload) {
    return new LineageEvent(
        UUID.randomUUID().toString(),
        1_760_781_600_000L,
        kind,
        ctx,
        (org.apache.hop.lineage.model.LineagePayload) payload);
  }

  private static LineageEvent lifecycle(LineageContext ctx, RunLifecyclePhase phase) {
    return event(ctx, LineageEventKind.RUN_LIFECYCLE, new RunLifecycleLineagePayload(phase, null));
  }

  // A workflow's actions are linked the way its hops run: Load leads to Report.
  @Test
  void actionsAreLinkedInTheOrderTheWorkflowRunsThem() {
    List<RunEvent> events =
        HopEvents.mapper()
            .mapBatch(
                List.of(
                    lifecycle(workflow("nightly"), RunLifecyclePhase.STARTED),
                    event(
                        workflow("nightly"),
                        LineageEventKind.WORKFLOW_GRAPH,
                        new PipelineGraphLineagePayload(
                            List.of(
                                new Transition("Start", "Load"),
                                new Transition("Load", "Report")))),
                    lifecycle(action("nightly", "Load"), RunLifecyclePhase.FINISHED),
                    lifecycle(action("nightly", "Report"), RunLifecyclePhase.FINISHED),
                    lifecycle(workflow("nightly"), RunLifecyclePhase.FINISHED)));
    List<Mcp> proposals = all(events);

    String flow = "urn:li:dataFlow:(hop,nightly,prod)";
    JsonNode report =
        one(proposals, "urn:li:dataJob:(" + flow + ",Report)", "dataJobInputOutput").value();
    assertEquals(
        List.of("urn:li:dataJob:(" + flow + ",Load)"),
        destinations(report.get("inputDatajobEdges")));
  }

  // A workflow run by an action of another workflow starts under that action.
  @Test
  void aChildWorkflowStartsUnderTheActionThatRanIt() {
    List<RunEvent> events =
        HopEvents.mapper()
            .mapBatch(
                List.of(
                    lifecycle(workflow("child", "nightly", "Run child"), RunLifecyclePhase.STARTED),
                    event(
                        workflow("child", "nightly", "Run child"),
                        LineageEventKind.WORKFLOW_GRAPH,
                        new PipelineGraphLineagePayload(List.of(new Transition("Start", "Work")))),
                    lifecycle(action("child", "Start"), RunLifecyclePhase.FINISHED),
                    lifecycle(action("child", "Work"), RunLifecyclePhase.FINISHED),
                    lifecycle(
                        workflow("child", "nightly", "Run child"), RunLifecyclePhase.FINISHED)));

    Mcp link =
        all(events).stream()
            .filter(
                m -> m.changeType().equals("PATCH") && m.aspectName().equals("dataJobInputOutput"))
            .findFirst()
            .orElseThrow();
    assertEquals("urn:li:dataJob:(urn:li:dataFlow:(hop,child,prod),Start)", link.entityUrn());
    assertEquals(
        "urn:li:dataJob:(urn:li:dataFlow:(hop,nightly,prod),Run child)",
        link.value().get(0).get("value").get("destinationUrn").asText());
  }

  // Two files copied in one run are two copies, each linked to its own source, not a 2x2 product.
  @Test
  void eachCopiedFileIsLinkedToItsOwnSource() {
    HopEvents hop = new HopEvents("archive");
    LineageContext copy = hop.transformContext("Copy");
    List<RunEvent> events =
        HopEvents.mapper()
            .mapBatch(
                List.of(
                    hop.graph("Copy>Done"),
                    event(
                        copy,
                        LineageEventKind.FILE_IO,
                        new FileIoLineagePayload(
                            FileIoOperation.COPY,
                            "file:///in/a.csv",
                            "file:///out/a.csv",
                            1L,
                            true,
                            null)),
                    event(
                        copy,
                        LineageEventKind.FILE_IO,
                        new FileIoLineagePayload(
                            FileIoOperation.COPY,
                            "file:///in/b.csv",
                            "file:///out/b.csv",
                            1L,
                            true,
                            null)),
                    hop.transform("Copy", RunLifecyclePhase.FINISHED),
                    hop.pipeline(RunLifecyclePhase.FINISHED)));
    List<Mcp> proposals = all(events);

    String outA = "urn:li:dataset:(urn:li:dataPlatform:file,/out/a.csv,PROD)";
    JsonNode upstream = one(proposals, outA, "upstreamLineage").value().get(0).get("value");
    assertEquals(
        "urn:li:dataset:(urn:li:dataPlatform:file,/in/a.csv,PROD)",
        upstream.get("dataset").asText());
    assertEquals("COPY", upstream.get("type").asText());
    JsonNode io =
        one(
                proposals,
                "urn:li:dataJob:(urn:li:dataFlow:(hop,archive.hpl,prod),Copy)",
                "dataJobInputOutput")
            .value();
    assertTrue(destinations(io.get("inputDatasetEdges")).isEmpty(), "no all-to-all pairs");
    assertTrue(destinations(io.get("outputDatasetEdges")).isEmpty());
  }

  // A run that went through says which tasks its pipeline has now; a failed one does not.
  @Test
  void aSuccessfulRunListsThePipelinesTasksForRetiringOldOnes() {
    List<Translation> translations = new ArrayList<>();
    for (RunEvent event :
        new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv")) {
      translations.add(translator.translate(event));
    }
    List<DataHubTranslator.StaleCheck> checks =
        translations.stream().flatMap(t -> t.staleChecks().stream()).toList();
    assertEquals(1, checks.size());
    assertEquals(FLOW, checks.get(0).flowUrn());
    assertEquals(Set.of(READ, WRITE), checks.get(0).currentJobUrns());
    assertEquals("${PROJECT_HOME}/orders.hpl", checks.get(0).filename());

    HopEvents failing = new HopEvents("orders");
    for (RunEvent event :
        HopEvents.mapper()
            .mapBatch(
                List.of(
                    failing.graph("Read>Write"),
                    failing.transform("Read", RunLifecyclePhase.FINISHED),
                    failing.pipeline(RunLifecyclePhase.FAILED)))) {
      assertTrue(translator.translate(event).staleChecks().isEmpty());
    }
  }

  // A local file is listed by its name; its identity stays the absolute path.
  @Test
  void aLocalFileIsShownByItsName() {
    List<Mcp> proposals =
        all(new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv"));

    JsonNode properties = one(proposals, OUT, "datasetProperties").value();
    assertEquals("x.csv", properties.get("name").asText());
    assertEquals("/out/x.csv", properties.get("qualifiedName").asText());
  }
}
