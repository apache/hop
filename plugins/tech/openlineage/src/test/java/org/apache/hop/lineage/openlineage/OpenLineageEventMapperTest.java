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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.lineage.LineageRunLifecycleEmitter;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.model.FileIoContentSchema;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.FileIoTabularColumn;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.LineageFieldSchema;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload.Transition;
import org.apache.hop.lineage.model.RelationalIoLineagePayload;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.lineage.model.RelationalLifecycle;
import org.apache.hop.lineage.model.RelationalTable;
import org.apache.hop.lineage.model.RelationalWriteColumn;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.lineage.model.TransformSchemaDirection;
import org.apache.hop.lineage.model.TransformSchemaLineagePayload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OpenLineageEventMapperTest {

  private static final String NAMESPACE = "hop";
  private static final String PRODUCER = "https://example.com/hop-openlineage-sink";

  private OpenLineageEventMapper mapper;
  private ObjectMapper objectMapper;

  @BeforeEach
  void setUp() {
    objectMapper = new ObjectMapper();
    mapper = new OpenLineageEventMapper(NAMESPACE, PRODUCER, objectMapper);
  }

  // Hop log-channel ids are UUID strings (LoggingRegistry uses UUID.randomUUID()), so runId is
  // emitted as that UUID verbatim per the dataset-identity contract.
  private static final String PIPELINE_RUN_UUID = "11111111-1111-1111-1111-111111111111";

  /**
   * Observes an I/O event the way the sink does — map the observation, then the run's terminal
   * event — and returns that terminal event, which is the one that carries the datasets.
   */
  private JsonNode mapCarriedByRunCompletion(LineageEvent io) throws Exception {
    return mapCarriedByRunCompletion(mapper, io);
  }

  private static JsonNode mapCarriedByRunCompletion(OpenLineageEventMapper m, LineageEvent io)
      throws Exception {
    // The observation carries nothing by itself: its datasets wait for the run to end.
    assertFalse(m.map(io).isPresent());
    return m.map(runCompletionOf(io)).orElseThrow();
  }

  /**
   * The COMPLETE event of the run an observation belongs to: the same job and run id the I/O event
   * is correlated to, reported as that run ending.
   */
  private static LineageEvent runCompletionOf(LineageEvent io) {
    LineageContext ioCtx = io.getContext();
    return new LineageEvent(
        "evt-complete",
        1_700_009_000_000L,
        LineageEventKind.RUN_LIFECYCLE,
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .pipelineName(ioCtx.getPipelineName())
            .logChannelId(OpenLineageEventMapper.resolveFileIoRunId(ioCtx, io.getEventId()))
            .build(),
        new RunLifecycleLineagePayload(RunLifecyclePhase.FINISHED, "success"));
  }

  @Test
  void runLifecycleStartedMapsToStartRunEvent() {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .logChannelId(PIPELINE_RUN_UUID)
            .pipelineName("demo-pipeline")
            .build();
    LineageEvent event =
        new LineageEvent(
            "evt-001",
            1_700_000_000_000L,
            LineageEventKind.RUN_LIFECYCLE,
            ctx,
            new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null));

    JsonNode ol = mapper.map(event).orElseThrow();
    assertEquals("START", ol.get("eventType").asText());
    assertEquals(PRODUCER, ol.get("producer").asText());
    assertEquals(PIPELINE_RUN_UUID, ol.get("run").get("runId").asText());
    assertEquals(NAMESPACE, ol.get("job").get("namespace").asText());
    assertEquals("demo-pipeline", ol.get("job").get("name").asText());
    assertTrue(ol.get("eventTime").asText().endsWith("Z"));
  }

  @Test
  void runLifecycleFinishedMapsToCompleteRunEvent() {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.WORKFLOW)
            .logChannelId("wf-log-456")
            .workflowName("daily-etl")
            .build();
    LineageEvent event =
        new LineageEvent(
            "evt-002",
            1_700_000_100_000L,
            LineageEventKind.RUN_LIFECYCLE,
            ctx,
            new RunLifecycleLineagePayload(RunLifecyclePhase.FINISHED, "success"));

    JsonNode ol = mapper.map(event).orElseThrow();
    assertEquals("COMPLETE", ol.get("eventType").asText());
    assertEquals("daily-etl", ol.get("job").get("name").asText());
  }

  @Test
  void runLifecycleFailedMapsToFailRunEvent() {
    LineageEvent event =
        new LineageEvent(
            "evt-003",
            1_700_000_200_000L,
            LineageEventKind.RUN_LIFECYCLE,
            LineageContext.builder()
                .subjectType(LineageSubjectType.PIPELINE)
                .pipelineName("broken")
                .logChannelId("run-fail")
                .build(),
            new RunLifecycleLineagePayload(RunLifecyclePhase.FAILED, "errors"));

    JsonNode ol = mapper.map(event).orElseThrow();
    assertEquals("FAIL", ol.get("eventType").asText());
  }

  @Test
  void finishingPhaseIsSkipped() {
    LineageEvent event =
        new LineageEvent(
            "evt-finishing",
            1L,
            LineageEventKind.RUN_LIFECYCLE,
            LineageContext.empty(),
            new RunLifecycleLineagePayload(RunLifecyclePhase.FINISHING, null));
    assertFalse(mapper.map(event).isPresent());
  }

  @Test
  void fileIoReadCarriesInputDatasetOnRunCompletion() throws Exception {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.TRANSFORM)
            .pipelineName("csv-load")
            .logChannelId("transform-log")
            .transformName("Text file input")
            .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN_UUID)
            .build();
    LineageEvent event =
        new LineageEvent(
            "evt-file",
            1_700_001_000_000L,
            LineageEventKind.FILE_IO,
            ctx,
            new FileIoLineagePayload(
                FileIoOperation.READ, "file:///data/customers.csv", null, 1024L, true, null));

    JsonNode completion = mapCarriedByRunCompletion(event);
    assertEquals("COMPLETE", completion.get("eventType").asText());
    assertEquals(PIPELINE_RUN_UUID, completion.get("run").get("runId").asText());
    assertEquals("csv-load", completion.get("job").get("name").asText());
    JsonNode input = completion.get("inputs").get(0);
    assertEquals("/data/customers.csv", input.get("name").asText());
    assertEquals(
        "file:///data/customers.csv", input.get("facets").get("dataSource").get("uri").asText());
  }

  // With spec naming (the mapper's default) a file dataset uses the OpenLineage naming spec (scheme
  // namespace +
  // path) instead of the legacy job namespace + bare filename, so it reconciles across engines.
  @Test
  void fileIoSpecNamingUsesSchemeNamespaceAndPath() throws Exception {
    OpenLineageEventMapper specMapper =
        new OpenLineageEventMapper(
            NAMESPACE, PRODUCER, objectMapper, RelationalSqlParser.create(false, null));
    LineageEvent event =
        new LineageEvent(
            "evt-file-spec",
            1L,
            LineageEventKind.FILE_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("export")
                .logChannelId("pipe-log")
                .build(),
            new FileIoLineagePayload(
                FileIoOperation.WRITE,
                null,
                "s3://bucket/warehouse/out.parquet",
                2048L,
                true,
                null));

    JsonNode output = mapCarriedByRunCompletion(specMapper, event).get("outputs").get(0);
    assertEquals("s3://bucket", output.get("namespace").asText());
    assertEquals("warehouse/out.parquet", output.get("name").asText());
    // A3: a file write replaces its target -> OVERWRITE lifecycle state.
    assertEquals(
        "OVERWRITE",
        output.get("facets").get("lifecycleStateChange").get("lifecycleStateChange").asText());
  }

  // Without spec naming a file dataset keeps the legacy identity of Hop 2.20: the job namespace and
  // the file name, the full URI only in the dataSource facet.
  @Test
  void fileIoLegacyNamingUsesJobNamespaceAndFileName() throws Exception {
    OpenLineageEventMapper legacyMapper =
        OpenLineageEventMapper.builder(NAMESPACE, PRODUCER)
            .objectMapper(objectMapper)
            .fileSpecNaming(false)
            .build();
    LineageEvent event =
        new LineageEvent(
            "evt-file-legacy",
            1L,
            LineageEventKind.FILE_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("export")
                .logChannelId("pipe-log")
                .build(),
            new FileIoLineagePayload(
                FileIoOperation.WRITE,
                null,
                "s3://bucket/warehouse/out.parquet",
                2048L,
                true,
                null));

    JsonNode output = mapCarriedByRunCompletion(legacyMapper, event).get("outputs").get(0);
    assertEquals(NAMESPACE, output.get("namespace").asText());
    assertEquals("out.parquet", output.get("name").asText());
    assertEquals(
        "s3://bucket/warehouse/out.parquet",
        output.get("facets").get("dataSource").get("uri").asText());
  }

  // A move renames the source into the target: the target is RENAME and carries the source as its
  // previous identifier, so a sink can retire the old dataset and point at the new one.
  @Test
  void fileMoveRenamesTheSourceIntoTheTarget() throws Exception {
    OpenLineageEventMapper specMapper =
        new OpenLineageEventMapper(
            NAMESPACE, PRODUCER, objectMapper, RelationalSqlParser.create(false, null));
    LineageEvent event =
        new LineageEvent(
            "evt-file-move",
            1L,
            LineageEventKind.FILE_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("archive")
                .logChannelId("pipe-log")
                .build(),
            new FileIoLineagePayload(
                FileIoOperation.MOVE,
                "s3://bucket/landing/orders.csv",
                "s3://bucket/archive/2026/orders.csv",
                2048L,
                true,
                null));

    JsonNode completion = mapCarriedByRunCompletion(specMapper, event);
    assertEquals("landing/orders.csv", completion.get("inputs").get(0).get("name").asText());
    JsonNode output = completion.get("outputs").get(0);
    JsonNode lifecycle = output.get("facets").get("lifecycleStateChange");
    assertEquals("RENAME", lifecycle.get("lifecycleStateChange").asText());
    assertEquals("s3://bucket", lifecycle.get("previousIdentifier").get("namespace").asText());
    assertEquals("landing/orders.csv", lifecycle.get("previousIdentifier").get("name").asText());
  }

  // Moving a file onto the identity it already has is a rewrite, not a rename of itself.
  @Test
  void fileMoveOntoItsOwnIdentityIsAnOverwrite() throws Exception {
    LineageEvent event =
        new LineageEvent(
            "evt-file-self-move",
            1L,
            LineageEventKind.FILE_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("archive")
                .logChannelId("pipe-log")
                .build(),
            new FileIoLineagePayload(
                FileIoOperation.MOVE,
                "file:///data/orders.csv",
                "file:///data/orders.csv",
                2048L,
                true,
                null));

    JsonNode output = mapCarriedByRunCompletion(event).get("outputs").get(0);
    JsonNode lifecycle = output.get("facets").get("lifecycleStateChange");
    assertEquals("OVERWRITE", lifecycle.get("lifecycleStateChange").asText());
    assertFalse(lifecycle.has("previousIdentifier"));
  }

  /**
   * Every run event says which engine produced it, so a collector can name the orchestrator
   * (DataHub reads processing_engine), and what its job is in Hop terms. The Hop facet has to
   * survive a trip through JSON: that is how a spooled event is replayed.
   */
  @Test
  void runEventsCarryTheEngineAndTheJobsHopIdentity() throws Exception {
    LineageEvent event =
        new LineageEvent(
            "evt-identity",
            1L,
            LineageEventKind.RUN_LIFECYCLE,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("nightly.load")
                .transformName("Read a/b")
                .logChannelId(PIPELINE_RUN_UUID)
                .hopFilename("/projects/demo/nightly.hpl")
                .build(),
            new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null));

    RunEvent mapped = mapper.toRunEvents(event).get(0);
    RunEvent replayed =
        OpenLineageClientUtils.runEventFromJson(OpenLineageClientUtils.toJson(mapped));

    assertEquals("hop", replayed.getRun().getFacets().getProcessing_engine().getName());
    assertEquals("TRANSFORM", replayed.getJob().getFacets().getJobType().getJobType());
    Map<String, Object> hop =
        replayed
            .getJob()
            .getFacets()
            .getAdditionalProperties()
            .get(OpenLineageEventMapper.HOP_JOB_FACET)
            .getAdditionalProperties();
    assertEquals("TRANSFORM", hop.get("subjectType"));
    assertEquals("nightly.load", hop.get("pipelineName"));
    assertEquals("Read a/b", hop.get("transformName"));
    assertEquals("/projects/demo/nightly.hpl", hop.get("filename"));
  }

  // A pipeline run by an action names that action's workflow and name separately, so a collector
  // does not have to split "workflow/action" back apart.
  @Test
  void aPipelineRunByAnActionNamesItsParentInTheHopFacet() {
    LineageEvent event =
        new LineageEvent(
            "evt-child",
            1L,
            LineageEventKind.RUN_LIFECYCLE,
            LineageContext.builder()
                .subjectType(LineageSubjectType.PIPELINE)
                .pipelineName("load/orders")
                .logChannelId(PIPELINE_RUN_UUID)
                .putAttribute(OpenLineageEventMapper.ATTR_PARENT_JOB_NAME, "nightly/Run load")
                .putAttribute(OpenLineageEventMapper.ATTR_PARENT_RUN_ID, "action-run")
                .putAttribute(LineageRunLifecycleEmitter.ATTR_PARENT_WORKFLOW_NAME, "nightly")
                .putAttribute(LineageRunLifecycleEmitter.ATTR_PARENT_ACTION_NAME, "Run load")
                .build(),
            new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null));

    Map<String, Object> hop =
        mapper
            .toRunEvents(event)
            .get(0)
            .getJob()
            .getFacets()
            .getAdditionalProperties()
            .get(OpenLineageEventMapper.HOP_JOB_FACET)
            .getAdditionalProperties();
    assertEquals("nightly", hop.get(LineageRunLifecycleEmitter.ATTR_PARENT_WORKFLOW_NAME));
    assertEquals("Run load", hop.get(LineageRunLifecycleEmitter.ATTR_PARENT_ACTION_NAME));
  }

  private static LineageContext workflowContext() {
    return LineageContext.builder()
        .subjectType(LineageSubjectType.WORKFLOW)
        .workflowName("nightly")
        .logChannelId("22222222-2222-2222-2222-222222222222")
        .build();
  }

  private static LineageContext actionContext(String action) {
    return LineageContext.builder()
        .subjectType(LineageSubjectType.ACTION)
        .workflowName("nightly")
        .actionName(action)
        .logChannelId(UUID.nameUUIDFromBytes(action.getBytes()).toString())
        .putAttribute(
            OpenLineageEventMapper.ATTR_WORKFLOW_LOG_CHANNEL_ID,
            "22222222-2222-2222-2222-222222222222")
        .build();
  }

  private static Map<String, Object> hopFacet(RunEvent event) {
    return event
        .getJob()
        .getFacets()
        .getAdditionalProperties()
        .get(OpenLineageEventMapper.HOP_JOB_FACET)
        .getAdditionalProperties();
  }

  // The workflow's hops say which action follows which; the workflow's end names where it starts.
  @Test
  void actionsNameTheActionsThatLeadToThem() {
    List<RunEvent> events =
        mapper.mapBatch(
            List.of(
                new LineageEvent(
                    "wf-start",
                    1L,
                    LineageEventKind.RUN_LIFECYCLE,
                    workflowContext(),
                    new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null)),
                new LineageEvent(
                    "wf-graph",
                    1L,
                    LineageEventKind.WORKFLOW_GRAPH,
                    workflowContext(),
                    new PipelineGraphLineagePayload(
                        List.of(
                            new Transition("Start", "Load"), new Transition("Load", "Report")))),
                new LineageEvent(
                    "report-end",
                    2L,
                    LineageEventKind.RUN_LIFECYCLE,
                    actionContext("Report"),
                    new RunLifecycleLineagePayload(RunLifecyclePhase.FINISHED, null)),
                new LineageEvent(
                    "wf-end",
                    3L,
                    LineageEventKind.RUN_LIFECYCLE,
                    workflowContext(),
                    new RunLifecycleLineagePayload(RunLifecyclePhase.FINISHED, null))));

    assertEquals(
        List.of("Load"), hopFacet(events.get(1)).get(OpenLineageEventMapper.UPSTREAM_ACTIONS));
    assertEquals(
        List.of("Start"), hopFacet(events.get(2)).get(OpenLineageEventMapper.ROOT_ACTIONS));
  }

  // A copy keeps its source on the target, so a run copying several files keeps the real pairs.
  @Test
  void aCopiedFileNamesItsSource() throws Exception {
    LineageEvent copy =
        new LineageEvent(
            "evt-copy",
            1L,
            LineageEventKind.FILE_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("archive")
                .logChannelId("pipe-log")
                .build(),
            new FileIoLineagePayload(
                FileIoOperation.COPY, "file:///in/a.csv", "file:///out/a.csv", 1L, true, null));

    JsonNode output = mapCarriedByRunCompletion(copy).get("outputs").get(0);
    JsonNode source = output.get("facets").get(OpenLineageEventMapper.COPIED_FROM_FACET);
    assertEquals("file", source.get("namespace").asText());
    assertEquals("/in/a.csv", source.get("name").asText());
  }

  @Test
  void fileIoWithNoUrisIsSkipped() {
    LineageEvent event =
        new LineageEvent(
            "empty",
            1L,
            LineageEventKind.FILE_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("p")
                .logChannelId("t")
                .build(),
            new FileIoLineagePayload(FileIoOperation.READ, null, null, null, true, null));
    assertFalse(mapper.map(event).isPresent());
  }

  @Test
  void fileIoWriteCarriesOutputDatasetOnRunCompletion() throws Exception {
    LineageEvent event =
        new LineageEvent(
            "evt-write",
            1L,
            LineageEventKind.FILE_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.PIPELINE)
                .pipelineName("export")
                .logChannelId("pipe-log")
                .build(),
            new FileIoLineagePayload(
                FileIoOperation.WRITE, null, "s3://bucket/out/result.parquet", 2048L, true, null));

    JsonNode completion = mapCarriedByRunCompletion(event);
    assertEquals(
        "s3://bucket/out/result.parquet",
        completion.get("outputs").get(0).get("facets").get("dataSource").get("uri").asText());
  }

  @Test
  void transformSchemaIsSkippedInV1() {
    LineageEvent event =
        new LineageEvent(
            "evt-schema",
            1L,
            LineageEventKind.TRANSFORM_SCHEMA,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("schema-pipeline")
                .transformName("Select values")
                .build(),
            new TransformSchemaLineagePayload(
                TransformSchemaDirection.INPUT,
                List.of(new LineageFieldSchema("id", "Integer", 9, 0))));
    assertFalse(mapper.map(event).isPresent());
  }

  // The hop graph does not change the OpenLineage output yet: the pipeline job keeps its datasets.
  @Test
  void pipelineGraphIsSkipped() {
    LineageEvent event =
        new LineageEvent(
            "evt-graph",
            1L,
            LineageEventKind.PIPELINE_GRAPH,
            LineageContext.builder()
                .subjectType(LineageSubjectType.PIPELINE)
                .logChannelId(PIPELINE_RUN_UUID)
                .pipelineName("graph-pipeline")
                .build(),
            new PipelineGraphLineagePayload(List.of(new Transition("Read", "Write"))));
    assertFalse(mapper.map(event).isPresent());
  }

  @Test
  void fileIoReadCarriesSchemaFacetOnRunCompletion() throws Exception {
    FileIoContentSchema contentSchema =
        FileIoContentSchema.tabularWithMergedTree(
            "csv", List.of(new FileIoTabularColumn("id", "Integer", 9, 0, null, null, false)));
    LineageEvent event =
        new LineageEvent(
            "evt-file-schema",
            1L,
            LineageEventKind.FILE_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("csv-load")
                .logChannelId("transform-log")
                .transformName("Text file input")
                .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, "pipeline-log")
                .build(),
            new FileIoLineagePayload(
                FileIoOperation.READ,
                "file:///data/customers.csv",
                null,
                1024L,
                true,
                null,
                contentSchema));

    JsonNode input = mapCarriedByRunCompletion(event).get("inputs").get(0);
    assertEquals("id", input.get("facets").get("schema").get("fields").get(0).get("name").asText());
    assertEquals(
        "int", input.get("facets").get("schema").get("fields").get(0).get("type").asText());
  }

  @Test
  void transformJobNameIncludesPipelineAndTransform() {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.TRANSFORM)
            .pipelineName("etl")
            .transformName("Filter rows")
            .build();
    assertEquals("etl/Filter rows", OpenLineageEventMapper.resolveJobName(ctx));
  }

  @Test
  void runUuidPassesThroughValidUuidAndDerivesDeterministicallyOtherwise() {
    java.util.UUID uuid = java.util.UUID.randomUUID();
    assertEquals(uuid, OpenLineageEventMapper.toRunUuid(uuid.toString()));
    // Non-UUID strings derive a stable UUID (same input -> same output).
    assertEquals(
        OpenLineageEventMapper.toRunUuid("fallback-job-12345678"),
        OpenLineageEventMapper.toRunUuid("fallback-job-12345678"));
  }

  @Test
  void resolveRunIdFallsBackToJobNameAndEventId() {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .pipelineName("fallback-job")
            .build();
    assertEquals(
        "fallback-job-12345678", OpenLineageEventMapper.resolveRunId(ctx, "12345678-abcd"));
  }

  @Test
  void transformRunLifecycleCarriesParentFacetToPipelineRun() {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.TRANSFORM)
            .pipelineName("csv-load")
            .transformName("Text file input")
            .logChannelId("22222222-2222-2222-2222-222222222222")
            .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN_UUID)
            .build();
    LineageEvent event =
        new LineageEvent(
            "evt-transform-start",
            1_700_000_000_000L,
            LineageEventKind.RUN_LIFECYCLE,
            ctx,
            new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null));

    JsonNode parent = mapper.map(event).orElseThrow().get("run").get("facets").get("parent");
    assertEquals(PIPELINE_RUN_UUID, parent.get("run").get("runId").asText());
    assertEquals("csv-load", parent.get("job").get("name").asText());
    assertEquals(NAMESPACE, parent.get("job").get("namespace").asText());
  }

  @Test
  void pipelineRunLifecycleHasNoParentFacet() {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .pipelineName("top-level")
            .logChannelId(PIPELINE_RUN_UUID)
            .build();
    LineageEvent event =
        new LineageEvent(
            "evt-pipe-start",
            1_700_000_000_000L,
            LineageEventKind.RUN_LIFECYCLE,
            ctx,
            new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null));

    JsonNode facets = mapper.map(event).orElseThrow().get("run").get("facets");
    assertFalse(facets.has("parent"));
  }

  // An action is a job of its own inside the workflow, like a transform is inside its pipeline.
  // Filing actions under the workflow name made every action of a workflow one job.
  @Test
  void actionJobNameIncludesWorkflowAndAction() {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.ACTION)
            .workflowName("nightly-load")
            .actionName("Run orders pipeline")
            .build();

    assertEquals("nightly-load/Run orders pipeline", OpenLineageEventMapper.resolveJobName(ctx));
  }

  @Test
  void actionRunLifecycleCarriesParentFacetToWorkflowRun() throws Exception {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.ACTION)
            .workflowName("nightly-load")
            .actionName("Run orders pipeline")
            .logChannelId("33333333-3333-3333-3333-333333333333")
            .putAttribute(OpenLineageEventMapper.ATTR_WORKFLOW_LOG_CHANNEL_ID, PIPELINE_RUN_UUID)
            .build();
    LineageEvent event =
        new LineageEvent(
            "evt-action-start",
            1_700_000_000_000L,
            LineageEventKind.RUN_LIFECYCLE,
            ctx,
            new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null));

    JsonNode mapped = mapper.map(event).orElseThrow();
    assertEquals("nightly-load/Run orders pipeline", mapped.get("job").get("name").asText());
    JsonNode parent = mapped.get("run").get("facets").get("parent");
    assertEquals(PIPELINE_RUN_UUID, parent.get("run").get("runId").asText());
    assertEquals("nightly-load", parent.get("job").get("name").asText());
  }

  // A pipeline started by a workflow action knows the action's job name, so the run attaches to it
  // the same way a transform attaches to its pipeline.
  @Test
  void pipelineRunLifecycleCarriesParentFacetWhenTheEngineNamesIt() throws Exception {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .pipelineName("orders")
            .logChannelId(PIPELINE_RUN_UUID)
            .putAttribute(
                OpenLineageEventMapper.ATTR_PARENT_RUN_ID, "44444444-4444-4444-4444-444444444444")
            .putAttribute(
                OpenLineageEventMapper.ATTR_PARENT_JOB_NAME, "nightly-load/Run orders pipeline")
            .build();
    LineageEvent event =
        new LineageEvent(
            "evt-pipe-start",
            1_700_000_000_000L,
            LineageEventKind.RUN_LIFECYCLE,
            ctx,
            new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null));

    JsonNode parent = mapper.map(event).orElseThrow().get("run").get("facets").get("parent");
    assertEquals("44444444-4444-4444-4444-444444444444", parent.get("run").get("runId").asText());
    assertEquals("nightly-load/Run orders pipeline", parent.get("job").get("name").asText());
    assertEquals(NAMESPACE, parent.get("job").get("namespace").asText());
  }

  // A collector that builds the job tree from the events themselves (DataHub) turns the facet off.
  @Test
  void mapperWithoutParentFacetsOmitsThemEvenWhenTheParentIsKnown() throws Exception {
    OpenLineageEventMapper noParents =
        OpenLineageEventMapper.builder(NAMESPACE, PRODUCER)
            .objectMapper(objectMapper)
            .parentRuns(false)
            .build();
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.TRANSFORM)
            .pipelineName("csv-load")
            .transformName("Text file input")
            .logChannelId("22222222-2222-2222-2222-222222222222")
            .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN_UUID)
            .build();
    LineageEvent event =
        new LineageEvent(
            "evt-transform-start",
            1_700_000_000_000L,
            LineageEventKind.RUN_LIFECYCLE,
            ctx,
            new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null));

    JsonNode facets = noParents.map(event).orElseThrow().get("run").get("facets");
    assertFalse(facets.has("parent"));
  }

  // §3 — a relational write carries an output dataset keyed by the database namespace + qualified
  // name (NOT the Hop job namespace), correlated to the pipeline run, with a column schema.
  @Test
  void relationalWriteCarriesOutputDatasetOnRunCompletion() throws Exception {
    FileIoContentSchema schema =
        FileIoContentSchema.tabularWithMergedTree(
            "jdbc", List.of(new FileIoTabularColumn("id", "Integer", 9, 0, null, null, false)));
    LineageEvent event =
        new LineageEvent(
            "evt-wh-write",
            1_700_002_000_000L,
            LineageEventKind.RELATIONAL_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("load-orders")
                .logChannelId("transform-log")
                .transformName("Table output")
                .putAttribute(
                    OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN_UUID)
                .build(),
            new RelationalIoLineagePayload(
                RelationalIoOperation.WRITE,
                "postgres://db:5432",
                List.of(),
                List.of(new RelationalTable("analytics", "staging", "orders")),
                null,
                schema,
                true,
                null));

    JsonNode completion = mapCarriedByRunCompletion(event);
    assertEquals("COMPLETE", completion.get("eventType").asText());
    assertEquals(PIPELINE_RUN_UUID, completion.get("run").get("runId").asText());
    assertEquals("load-orders", completion.get("job").get("name").asText());
    JsonNode output = completion.get("outputs").get(0);
    assertEquals("postgres://db:5432", output.get("namespace").asText());
    assertEquals("analytics.staging.orders", output.get("name").asText());
    assertEquals(
        "id", output.get("facets").get("schema").get("fields").get(0).get("name").asText());
    assertFalse(completion.has("inputs"));
  }

  // A lookup per row, or a loop over the same file, observes one dataset many times. The run holds
  // it once, so its completion stays small however many rows there were.
  @Test
  void aDatasetObservedManyTimesIsCarriedOnce() throws Exception {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.TRANSFORM)
            .pipelineName("lookup-per-row")
            .logChannelId("transform-log")
            .transformName("Database lookup")
            .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN_UUID)
            .build();
    LineageEvent lookup = null;
    for (int i = 0; i < 1_000; i++) {
      lookup =
          new LineageEvent(
              "evt-lookup-" + i,
              1_700_002_000_000L + i,
              LineageEventKind.RELATIONAL_IO,
              ctx,
              new RelationalIoLineagePayload(
                  RelationalIoOperation.READ,
                  "postgres://db:5432",
                  List.of(new RelationalTable("analytics", "dim", "customer")),
                  List.of(),
                  null,
                  null,
                  true,
                  null));
      assertFalse(mapper.map(lookup).isPresent());
    }

    JsonNode completion = mapper.map(runCompletionOf(lookup)).orElseThrow();
    assertEquals(1, completion.get("inputs").size());
    assertEquals("analytics.dim.customer", completion.get("inputs").get(0).get("name").asText());
  }

  /**
   * A delete names the affected table as an output dataset, but attaches no {@code schema} facet:
   * the transform's input row shape describes what it read to find the rows, not columns the table
   * gained.
   */
  @Test
  void relationalDeleteCarriesOutputDatasetWithoutASchemaFacet() throws Exception {
    LineageEvent event =
        new LineageEvent(
            "evt-rel-delete",
            1_700_002_000_000L,
            LineageEventKind.RELATIONAL_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("purge-orders")
                .logChannelId("transform-log")
                .transformName("Delete")
                .putAttribute(
                    OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN_UUID)
                .build(),
            new RelationalIoLineagePayload(
                RelationalIoOperation.DELETE,
                "postgres://db:5432",
                List.of(),
                List.of(new RelationalTable("analytics", "staging", "orders")),
                null,
                null,
                true,
                null));

    JsonNode completion = mapCarriedByRunCompletion(event);
    JsonNode output = completion.get("outputs").get(0);
    assertEquals("postgres://db:5432", output.get("namespace").asText());
    assertEquals("analytics.staging.orders", output.get("name").asText());
    assertFalse(output.get("facets").has("schema"));
    assertFalse(output.get("facets").has("columnLineage"));
    assertFalse(completion.has("inputs"));
  }

  // A truncate-then-insert write (RelationalLifecycle.OVERWRITE) records an OVERWRITE lifecycle
  // state
  // on the output dataset.
  @Test
  void relationalWriteWithOverwriteLifecycleCarriesFacetOnRunCompletion() throws Exception {
    LineageEvent event =
        new LineageEvent(
            "evt-wh-overwrite",
            1L,
            LineageEventKind.RELATIONAL_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("load-orders")
                .logChannelId("transform-log")
                .transformName("Table output")
                .build(),
            new RelationalIoLineagePayload(
                RelationalIoOperation.WRITE,
                "postgres://db:5432",
                List.of(),
                List.of(new RelationalTable("analytics", "staging", "orders")),
                null,
                null,
                null,
                null,
                List.of(),
                RelationalLifecycle.OVERWRITE,
                true,
                null));

    JsonNode output = mapCarriedByRunCompletion(event).get("outputs").get(0);
    assertEquals(
        "OVERWRITE",
        output.get("facets").get("lifecycleStateChange").get("lifecycleStateChange").asText());
  }

  // A read carries its source table as an input dataset and the SQL text as a job facet on the
  // run's
  // terminal event.
  @Test
  void relationalReadCarriesInputDatasetAndSqlJobFacetOnRunCompletion() throws Exception {
    String sql = "SELECT id FROM analytics.staging.orders";
    LineageEvent event =
        new LineageEvent(
            "evt-wh-read",
            1L,
            LineageEventKind.RELATIONAL_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("read-orders")
                .logChannelId("transform-log")
                .transformName("Table input")
                .build(),
            new RelationalIoLineagePayload(
                RelationalIoOperation.READ,
                "postgres://db:5432",
                List.of(new RelationalTable("analytics", "staging", "orders")),
                List.of(),
                sql,
                null,
                true,
                null));

    JsonNode completion = mapCarriedByRunCompletion(event);
    assertEquals("analytics.staging.orders", completion.get("inputs").get(0).get("name").asText());
    assertEquals("postgres://db:5432", completion.get("inputs").get(0).get("namespace").asText());
    assertEquals(sql, completion.get("job").get("facets").get("sql").get("query").asText());
  }

  // The rows a Table Input produces are the query's result, not the table: their aliases and
  // expressions must not become the source table's schema. The table declares only the columns the
  // statement reads from it.
  @Test
  void relationalReadDeclaresTheColumnsItReadsNotItsResultRows() throws Exception {
    OpenLineageEventMapper parsingMapper =
        new OpenLineageEventMapper(
            NAMESPACE, PRODUCER, objectMapper, RelationalSqlParser.create(true, null));
    FileIoContentSchema resultRows =
        new FileIoContentSchema(
            "jdbc",
            List.of(
                new FileIoTabularColumn("order_id", "Integer", 0, 0, null, null, false),
                new FileIoTabularColumn("customer", "String", 0, 0, null, null, false)),
            List.of());
    LineageEvent event =
        new LineageEvent(
            "evt-wh-projection",
            1L,
            LineageEventKind.RELATIONAL_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("read-orders")
                .logChannelId("transform-log")
                .transformName("Table input")
                .build(),
            new RelationalIoLineagePayload(
                RelationalIoOperation.READ,
                "postgres://db:5432",
                List.of(),
                List.of(),
                "SELECT id AS order_id, UPPER(name) AS customer FROM analytics.staging.orders",
                resultRows,
                true,
                null));

    JsonNode input = mapCarriedByRunCompletion(parsingMapper, event).get("inputs").get(0);
    List<String> fields = new ArrayList<>();
    input
        .get("facets")
        .get("schema")
        .get("fields")
        .forEach(f -> fields.add(f.get("name").asText()));
    assertEquals(Set.of("id", "name"), Set.copyOf(fields));
  }

  // With SQL parsing enabled, a read that carries only SQL (no engine-resolved tables) has its
  // source table recovered from the statement and carried on the run's terminal event.
  @Test
  void relationalReadParsesSqlIntoInputDatasetWhenParserEnabled() throws Exception {
    OpenLineageEventMapper parsingMapper =
        new OpenLineageEventMapper(
            NAMESPACE, PRODUCER, objectMapper, RelationalSqlParser.create(true, null));
    LineageEvent event =
        new LineageEvent(
            "evt-wh-parse",
            1L,
            LineageEventKind.RELATIONAL_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("read-orders")
                .logChannelId("transform-log")
                .transformName("Table input")
                .build(),
            new RelationalIoLineagePayload(
                RelationalIoOperation.READ,
                "postgres://db:5432",
                List.of(),
                List.of(),
                "SELECT id FROM analytics.staging.orders",
                null,
                true,
                null));

    JsonNode completion = mapCarriedByRunCompletion(parsingMapper, event);
    assertEquals("analytics.staging.orders", completion.get("inputs").get(0).get("name").asText());
    assertEquals("postgres://db:5432", completion.get("inputs").get(0).get("namespace").asText());
  }

  // A schema-qualified read (public.orders_source, no catalog) has the connection catalog prepended
  // so it stitches with a write of hop_database.public.orders_source for the same table.
  @Test
  void relationalReadParsedTableIsQualifiedWithDefaultCatalog() throws Exception {
    OpenLineageEventMapper parsingMapper =
        new OpenLineageEventMapper(
            NAMESPACE, PRODUCER, objectMapper, RelationalSqlParser.create(true, null));
    LineageEvent event =
        new LineageEvent(
            "evt-wh-catalog",
            1L,
            LineageEventKind.RELATIONAL_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("read-orders")
                .logChannelId("transform-log")
                .transformName("Table input")
                .build(),
            new RelationalIoLineagePayload(
                RelationalIoOperation.READ,
                "postgres://db:5432",
                List.of(),
                List.of(),
                "SELECT id FROM public.orders_source",
                null,
                "hop_database",
                true,
                null));

    JsonNode completion = mapCarriedByRunCompletion(parsingMapper, event);
    assertEquals(
        "hop_database.public.orders_source", completion.get("inputs").get(0).get("name").asText());
  }

  // An INSERT … SELECT (EXEC) is parsed into an output table with a columnLineage facet whose input
  // fields reference the source table by the same relational identity as the input datasets.
  @Test
  void relationalExecCarriesColumnLineageOnRunCompletion() throws Exception {
    OpenLineageEventMapper parsingMapper =
        new OpenLineageEventMapper(
            NAMESPACE, PRODUCER, objectMapper, RelationalSqlParser.create(true, null));
    LineageEvent event =
        new LineageEvent(
            "evt-wh-exec",
            1L,
            LineageEventKind.RELATIONAL_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("etl")
                .logChannelId("t")
                .transformName("Execute SQL")
                .build(),
            new RelationalIoLineagePayload(
                RelationalIoOperation.EXEC,
                "postgres://db:5432",
                List.of(),
                List.of(),
                "INSERT INTO analytics.marts.daily_orders "
                    + "SELECT o.id FROM analytics.staging.orders o",
                null,
                true,
                null));

    JsonNode completion = mapCarriedByRunCompletion(parsingMapper, event);
    JsonNode output = completion.get("outputs").get(0);
    assertEquals("analytics.marts.daily_orders", output.get("name").asText());
    JsonNode inputField =
        output.get("facets").get("columnLineage").get("fields").get("id").get("inputFields").get(0);
    assertEquals("postgres://db:5432", inputField.get("namespace").asText());
    assertEquals("analytics.staging.orders", inputField.get("name").asText());
    assertEquals("id", inputField.get("field").asText());
    assertEquals("analytics.staging.orders", completion.get("inputs").get(0).get("name").asText());
    // The SQL-recovered write declares its output columns (from the column lineage) so a collector
    // has fields to render the column-level graph on.
    assertEquals(
        "id", output.get("facets").get("schema").get("fields").get(0).get("name").asText());
  }

  // Stream-path column lineage: a Table Input read registers its stream fields; a later Table
  // Output write resolves each column back through its origin transform to the source column, and
  // the write's output dataset gets a columnLineage facet (target ← source), even across events.
  @Test
  void relationalWriteColumnsCorrelateToReadForColumnLineage() throws Exception {
    OpenLineageEventMapper m =
        new OpenLineageEventMapper(
            NAMESPACE, PRODUCER, objectMapper, RelationalSqlParser.create(true, null));

    // 1. Table Input READ, parsed into source columns, registered under run "run-1".
    LineageEvent read =
        new LineageEvent(
            "evt-read",
            1L,
            LineageEventKind.RELATIONAL_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("copy")
                .logChannelId("read-log")
                .transformName("Read source")
                .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, "run-1")
                .build(),
            new RelationalIoLineagePayload(
                RelationalIoOperation.READ,
                "postgres://db:5432",
                List.of(),
                List.of(),
                "SELECT id, amount FROM public.orders_source",
                null,
                "hop_database",
                null,
                List.of(),
                true,
                null));
    assertFalse(m.map(read).isPresent());

    // 2. Table Output WRITE with per-column provenance pointing at the read's transform.
    IRowMeta written = new RowMeta();
    written.addValueMeta(new ValueMetaInteger("id"));
    written.addValueMeta(new ValueMetaNumber("amount"));
    LineageEvent write =
        new LineageEvent(
            "evt-write",
            2L,
            LineageEventKind.RELATIONAL_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("copy")
                .logChannelId("write-log")
                .transformName("Write target")
                .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, "run-1")
                .build(),
            new RelationalIoLineagePayload(
                RelationalIoOperation.WRITE,
                "postgres://db:5432",
                List.of(),
                List.of(new RelationalTable("hop_database", "public", "orders_target")),
                null,
                contentSchema(written),
                "hop_database",
                null,
                List.of(
                    new RelationalWriteColumn("id", "id", "Read source"),
                    new RelationalWriteColumn("amount", "amount", "Read source")),
                true,
                null));

    JsonNode completion = mapCarriedByRunCompletion(m, write);
    JsonNode output = completion.get("outputs").get(0);
    assertEquals("hop_database.public.orders_target", output.get("name").asText());
    JsonNode idInput =
        output.get("facets").get("columnLineage").get("fields").get("id").get("inputFields").get(0);
    assertEquals("hop_database.public.orders_source", idInput.get("name").asText());
    assertEquals("id", idInput.get("field").asText());
    assertEquals("postgres://db:5432", idInput.get("namespace").asText());
  }

  private static FileIoContentSchema contentSchema(IRowMeta rowMeta) {
    java.util.List<FileIoTabularColumn> cols = new ArrayList<>();
    for (org.apache.hop.core.row.IValueMeta v : rowMeta.getValueMetaList()) {
      cols.add(new FileIoTabularColumn(v.getName(), v.getTypeDesc(), 0, 0, null, null, false));
    }
    return new FileIoContentSchema("jdbc", cols, List.of());
  }

  // No resolvable relational namespace (e.g. an embedded database) → nothing stitchable, skipped.
  @Test
  void relationalWithNoNamespaceIsSkipped() {
    LineageEvent event =
        new LineageEvent(
            "evt-wh-nons",
            1L,
            LineageEventKind.RELATIONAL_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("p")
                .logChannelId("t")
                .build(),
            new RelationalIoLineagePayload(
                RelationalIoOperation.WRITE,
                null,
                List.of(),
                List.of(new RelationalTable("db", "s", "orders")),
                null,
                null,
                true,
                null));
    assertFalse(mapper.map(event).isPresent());
  }

  // A read whose tables are not yet resolved (SQL awaiting parsing) has no datasets → skipped.
  @Test
  void relationalWithNoTablesIsSkipped() {
    LineageEvent event =
        new LineageEvent(
            "evt-wh-notables",
            1L,
            LineageEventKind.RELATIONAL_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("p")
                .logChannelId("t")
                .build(),
            new RelationalIoLineagePayload(
                RelationalIoOperation.READ,
                "postgres://db:5432",
                List.of(),
                List.of(),
                "SELECT 1",
                null,
                true,
                null));
    assertFalse(mapper.map(event).isPresent());
  }

  /**
   * An observation that arrives after its run reported COMPLETE has no run event left to carry it.
   * It is sent as its own dataset event, which is how this sink used to send every observation,
   * rather than dropped.
   */
  @Test
  void observationAfterRunCompletionIsSentOnItsOwn() throws Exception {
    LineageEvent read = fileIoOf("evt-early-read", FileIoOperation.READ, "file:///data/in.csv");
    LineageEvent write = fileIoOf("evt-late-write", FileIoOperation.WRITE, "file:///data/out.csv");

    // A read of that run, seen while the run was still going...
    assertFalse(mapper.map(read).isPresent());

    // ...the run then ends, carrying what it observed...
    JsonNode completion = mapper.map(runCompletionOf(write)).orElseThrow();
    assertEquals("/data/in.csv", completion.get("inputs").get(0).get("name").asText());

    // ...and a later observation of the same run has no run event left to carry it.
    JsonNode ol = mapper.map(write).orElseThrow();
    assertEquals("OTHER", ol.get("eventType").asText());
    assertEquals(PIPELINE_RUN_UUID, ol.get("run").get("runId").asText());
    assertEquals("late-io", ol.get("job").get("name").asText());
    assertEquals(
        "file:///data/out.csv",
        ol.get("outputs").get(0).get("facets").get("dataSource").get("uri").asText());
  }

  private LineageEvent fileIoOf(String eventId, FileIoOperation operation, String uri) {
    boolean read = operation == FileIoOperation.READ;
    return new LineageEvent(
        eventId,
        1_700_003_000_000L,
        LineageEventKind.FILE_IO,
        LineageContext.builder()
            .subjectType(LineageSubjectType.TRANSFORM)
            .pipelineName("late-io")
            .logChannelId("transform-log")
            .transformName("Text file output")
            .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN_UUID)
            .build(),
        new FileIoLineagePayload(operation, read ? uri : null, read ? null : uri, 1L, true, null));
  }

  /**
   * A run that never reports its end - an execution that crashed, or one the engine never got to
   * finish - leaves nothing to carry its datasets. The sink asks for those observations at
   * shutdown, and they come out as the standalone events this sink used to send for every
   * observation, so an abnormal end does not silently cost the run its lineage.
   */
  @Test
  void observationsWithoutARunCompletionAreFlushedAsStandaloneEvents() throws Exception {
    LineageEvent read =
        new LineageEvent(
            "evt-abandoned",
            1_700_004_000_000L,
            LineageEventKind.FILE_IO,
            LineageContext.builder()
                .subjectType(LineageSubjectType.TRANSFORM)
                .pipelineName("abandoned")
                .logChannelId("transform-log")
                .transformName("Text file input")
                .putAttribute(
                    OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN_UUID)
                .build(),
            new FileIoLineagePayload(
                FileIoOperation.READ, "file:///data/in.csv", null, 1L, true, null));

    assertFalse(mapper.map(read).isPresent());

    List<RunEvent> leftovers = mapper.flushObservations();
    assertEquals(1, leftovers.size());
    JsonNode ol = objectMapper.readTree(OpenLineageClientUtils.toJson(leftovers.get(0)));
    assertEquals("OTHER", ol.get("eventType").asText());
    assertEquals(PIPELINE_RUN_UUID, ol.get("run").get("runId").asText());
    assertEquals("/data/in.csv", ol.get("inputs").get(0).get("name").asText());

    // Flushed once: a second shutdown has nothing left to report.
    assertTrue(mapper.flushObservations().isEmpty());
  }

  /**
   * A run that started and never ended is ended with an ABORT at shutdown when the sink asks for it
   * (DataHub keeps run state); by default the flush only carries observations, as before.
   */
  @Test
  void openRunsAreAbortedAtShutdownOnlyWhenAsked() {
    LineageContext pipeline =
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .pipelineName("open")
            .logChannelId(PIPELINE_RUN_UUID)
            .build();
    LineageEvent started =
        new LineageEvent(
            "evt-open-start",
            1_700_004_000_000L,
            LineageEventKind.RUN_LIFECYCLE,
            pipeline,
            new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null));
    LineageEvent ended =
        new LineageEvent(
            "evt-open-end",
            1_700_004_001_000L,
            LineageEventKind.RUN_LIFECYCLE,
            pipeline,
            new RunLifecycleLineagePayload(RunLifecyclePhase.FINISHED, null));

    mapper.mapBatch(List.of(started));
    assertTrue(mapper.flushObservations().isEmpty());

    OpenLineageEventMapper aborting =
        OpenLineageEventMapper.builder(NAMESPACE, PRODUCER).abortOpenRuns(true).build();
    aborting.mapBatch(List.of(started));
    List<RunEvent> flushed = aborting.flushObservations();
    assertEquals(1, flushed.size());
    assertEquals(RunEvent.EventType.ABORT, flushed.get(0).getEventType());
    assertEquals(PIPELINE_RUN_UUID, flushed.get(0).getRun().getRunId().toString());
    assertTrue(aborting.flushObservations().isEmpty(), "aborted once");

    // A run that did end is not aborted.
    aborting.mapBatch(List.of(started, ended));
    assertTrue(aborting.flushObservations().isEmpty());
  }

  /**
   * Two statements in one run: no single statement may claim the job, so the datasets still travel
   * on the run's terminal event, without a SQL facet.
   */
  @Test
  void severalStatementsInOneRunLeaveNoSqlJobFacet() throws Exception {
    LineageEvent firstRead =
        relationalReadOf("a-orders", "orders_source", "SELECT id FROM public.orders_source");
    LineageEvent secondRead =
        relationalReadOf("b-customers", "customers", "SELECT id FROM public.customers");

    assertFalse(mapper.map(firstRead).isPresent());
    assertFalse(mapper.map(secondRead).isPresent());

    JsonNode completion = mapper.map(runCompletionOf(secondRead)).orElseThrow();
    assertEquals(2, completion.get("inputs").size());
    assertEquals(
        "analytics.public.orders_source", completion.get("inputs").get(0).get("name").asText());
    assertEquals(
        "analytics.public.customers", completion.get("inputs").get(1).get("name").asText());
    assertFalse(completion.get("job").get("facets").has("sql"));
  }

  private LineageEvent relationalReadOf(String eventId, String table, String sql) {
    return new LineageEvent(
        eventId,
        1_700_005_000_000L,
        LineageEventKind.RELATIONAL_IO,
        LineageContext.builder()
            .subjectType(LineageSubjectType.TRANSFORM)
            .pipelineName("two-reads")
            .logChannelId("transform-log")
            .transformName("Table input")
            .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN_UUID)
            .build(),
        new RelationalIoLineagePayload(
            RelationalIoOperation.READ,
            "postgres://db:5432",
            List.of(new RelationalTable("analytics", "public", table)),
            List.of(),
            sql,
            null,
            true,
            null));
  }
}
