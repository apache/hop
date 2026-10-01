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
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.model.FileIoContentSchema;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.FileIoTabularColumn;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.LineageFieldSchema;
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

  @Test
  void runLifecycleStartedMapsToStartRunEvent() throws Exception {
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
  void runLifecycleFinishedMapsToCompleteRunEvent() throws Exception {
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
  void runLifecycleFailedMapsToFailRunEvent() throws Exception {
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
  void fileIoReadMapsInputDatasetUri() throws Exception {
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

    JsonNode ol = mapper.map(event).orElseThrow();
    assertEquals("OTHER", ol.get("eventType").asText());
    assertEquals(PIPELINE_RUN_UUID, ol.get("run").get("runId").asText());
    assertEquals("csv-load", ol.get("job").get("name").asText());
    JsonNode input = ol.get("inputs").get(0);
    assertEquals("customers.csv", input.get("name").asText());
    assertEquals(
        "file:///data/customers.csv", input.get("facets").get("dataSource").get("uri").asText());
  }

  // With spec naming enabled a file dataset uses the OpenLineage naming spec (scheme namespace +
  // path) instead of the legacy job namespace + bare filename, so it reconciles across engines.
  @Test
  void fileIoSpecNamingUsesSchemeNamespaceAndPath() throws Exception {
    OpenLineageEventMapper specMapper =
        new OpenLineageEventMapper(
            NAMESPACE, PRODUCER, objectMapper, RelationalSqlParser.create(false, null), true);
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

    JsonNode output = specMapper.map(event).orElseThrow().get("outputs").get(0);
    assertEquals("s3://bucket", output.get("namespace").asText());
    assertEquals("warehouse/out.parquet", output.get("name").asText());
    // A3: a file write replaces its target -> OVERWRITE lifecycle state.
    assertEquals(
        "OVERWRITE",
        output.get("facets").get("lifecycleStateChange").get("lifecycleStateChange").asText());
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
  void fileIoWriteMapsOutputDatasetUri() throws Exception {
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

    JsonNode ol = mapper.map(event).orElseThrow();
    assertEquals(
        "s3://bucket/out/result.parquet",
        ol.get("outputs").get(0).get("facets").get("dataSource").get("uri").asText());
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

  @Test
  void fileIoReadMapsSchemaFacetFromContentSchema() throws Exception {
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

    JsonNode input = mapper.map(event).orElseThrow().get("inputs").get(0);
    assertEquals("id", input.get("facets").get("schema").get("fields").get(0).get("name").asText());
    assertEquals(
        "Integer", input.get("facets").get("schema").get("fields").get(0).get("type").asText());
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
  void transformRunLifecycleCarriesParentFacetToPipelineRun() throws Exception {
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
  void pipelineRunLifecycleHasNoParentFacet() throws Exception {
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

  // §3 — a relational write maps to an output dataset keyed by the database namespace + qualified
  // name (NOT the Hop job namespace), correlated to the pipeline run, with a column schema.
  @Test
  void relationalWriteMapsOutputDatasetWithContractIdentity() throws Exception {
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

    JsonNode ol = mapper.map(event).orElseThrow();
    assertEquals("OTHER", ol.get("eventType").asText());
    assertEquals(PIPELINE_RUN_UUID, ol.get("run").get("runId").asText());
    assertEquals("load-orders", ol.get("job").get("name").asText());
    JsonNode output = ol.get("outputs").get(0);
    assertEquals("postgres://db:5432", output.get("namespace").asText());
    assertEquals("analytics.staging.orders", output.get("name").asText());
    assertEquals(
        "id", output.get("facets").get("schema").get("fields").get(0).get("name").asText());
    assertFalse(ol.has("inputs"));
  }

  /**
   * A delete names the affected table as an output dataset, but attaches no {@code schema} facet:
   * the transform's input row shape describes what it read to find the rows, not columns the table
   * gained.
   */
  @Test
  void relationalDeleteMapsOutputDatasetWithoutASchemaFacet() throws Exception {
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

    JsonNode ol = mapper.map(event).orElseThrow();
    JsonNode output = ol.get("outputs").get(0);
    assertEquals("postgres://db:5432", output.get("namespace").asText());
    assertEquals("analytics.staging.orders", output.get("name").asText());
    assertFalse(output.get("facets").has("schema"));
    assertFalse(output.get("facets").has("columnLineage"));
    assertFalse(ol.has("inputs"));
  }

  // A truncate-then-insert write (RelationalLifecycle.OVERWRITE) records an OVERWRITE lifecycle
  // state
  // on the output dataset.
  @Test
  void relationalWriteWithOverwriteLifecycleEmitsFacet() throws Exception {
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

    JsonNode output = mapper.map(event).orElseThrow().get("outputs").get(0);
    assertEquals(
        "OVERWRITE",
        output.get("facets").get("lifecycleStateChange").get("lifecycleStateChange").asText());
  }

  // A read carries its source table as an input dataset and the SQL text as a job facet.
  @Test
  void relationalReadMapsInputDatasetAndSqlJobFacet() throws Exception {
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

    JsonNode ol = mapper.map(event).orElseThrow();
    assertEquals("analytics.staging.orders", ol.get("inputs").get(0).get("name").asText());
    assertEquals("postgres://db:5432", ol.get("inputs").get(0).get("namespace").asText());
    assertEquals(sql, ol.get("job").get("facets").get("sql").get("query").asText());
  }

  // With SQL parsing enabled, a read that carries only SQL (no engine-resolved tables) has its
  // source table recovered from the statement and emitted as an input dataset.
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

    JsonNode ol = parsingMapper.map(event).orElseThrow();
    assertEquals("analytics.staging.orders", ol.get("inputs").get(0).get("name").asText());
    assertEquals("postgres://db:5432", ol.get("inputs").get(0).get("namespace").asText());
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

    JsonNode ol = parsingMapper.map(event).orElseThrow();
    assertEquals("hop_database.public.orders_source", ol.get("inputs").get(0).get("name").asText());
  }

  // An INSERT … SELECT (EXEC) is parsed into an output table with a columnLineage facet whose input
  // fields reference the source table by the same relational identity as the input datasets.
  @Test
  void relationalExecMapsColumnLineageOnOutputDataset() throws Exception {
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

    JsonNode ol = parsingMapper.map(event).orElseThrow();
    JsonNode output = ol.get("outputs").get(0);
    assertEquals("analytics.marts.daily_orders", output.get("name").asText());
    JsonNode inputField =
        output.get("facets").get("columnLineage").get("fields").get("id").get("inputFields").get(0);
    assertEquals("postgres://db:5432", inputField.get("namespace").asText());
    assertEquals("analytics.staging.orders", inputField.get("name").asText());
    assertEquals("id", inputField.get("field").asText());
    assertEquals("analytics.staging.orders", ol.get("inputs").get(0).get("name").asText());
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
    m.map(read).orElseThrow();

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

    JsonNode ol = m.map(write).orElseThrow();
    JsonNode output = ol.get("outputs").get(0);
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
}
