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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacets;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.ParentRunFacet;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.client.OpenLineageClientUtils;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lineage.LineageFileIdentity;
import org.apache.hop.lineage.LineageRelationalIdentity;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.model.FileIoContentSchema;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.FileIoTabularColumn;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineagePayload;
import org.apache.hop.lineage.model.RelationalIoLineagePayload;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.lineage.model.RelationalLifecycle;
import org.apache.hop.lineage.model.RelationalTable;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;

/**
 * Maps Hop {@link LineageEvent} instances to OpenLineage {@link RunEvent}s using the official
 * {@code openlineage-java} client models, so the emitted JSON is spec-correct by construction.
 *
 * <p>The {@code (namespace, name)} and {@code runId} identities produced here are the locked
 * contract in the "OpenLineage dataset identity" page of the user manual — they must not change
 * without a contract version bump. Hop log-channel ids are already UUID strings ({@code
 * LoggingRegistry#registerLoggingSource} uses {@code UUID.randomUUID()}), so {@code runId =
 * logChannelId} stays byte-identical once typed as a {@link UUID}; only synthetic fallbacks are
 * derived deterministically.
 */
public final class OpenLineageEventMapper {

  /**
   * Hop context attributes set by {@code LineageRunLifecycleEmitter} that carry the parent run's
   * log-channel id, used to build OpenLineage {@link ParentRunFacet}s so transform/action runs
   * attach to their pipeline/workflow run instead of appearing as disconnected jobs.
   */
  static final String ATTR_PIPELINE_LOG_CHANNEL_ID = "pipelineLogChannelId";

  static final String ATTR_PARENT_PIPELINE_LOG_CHANNEL_ID = "parentPipelineLogChannelId";
  static final String ATTR_WORKFLOW_LOG_CHANNEL_ID = "workflowLogChannelId";

  private final OpenLineage ol;
  private final ObjectMapper objectMapper;
  private final String namespace;
  private final RelationalSqlParser sqlParser;
  private final boolean fileSpecNaming;
  private final RelationalColumnLineageCorrelator columnLineageCorrelator =
      new RelationalColumnLineageCorrelator();

  public OpenLineageEventMapper(String namespace, String producer) {
    this(namespace, producer, OpenLineageClientUtils.newObjectMapper());
  }

  public OpenLineageEventMapper(String namespace, String producer, ObjectMapper objectMapper) {
    this(namespace, producer, objectMapper, RelationalSqlParser.create(false, null));
  }

  public OpenLineageEventMapper(String namespace, String producer, RelationalSqlParser sqlParser) {
    this(namespace, producer, OpenLineageClientUtils.newObjectMapper(), sqlParser);
  }

  public OpenLineageEventMapper(
      String namespace, String producer, RelationalSqlParser sqlParser, boolean fileSpecNaming) {
    this(namespace, producer, OpenLineageClientUtils.newObjectMapper(), sqlParser, fileSpecNaming);
  }

  public OpenLineageEventMapper(
      String namespace, String producer, ObjectMapper objectMapper, RelationalSqlParser sqlParser) {
    this(namespace, producer, objectMapper, sqlParser, false);
  }

  public OpenLineageEventMapper(
      String namespace,
      String producer,
      ObjectMapper objectMapper,
      RelationalSqlParser sqlParser,
      boolean fileSpecNaming) {
    this.ol = new OpenLineage(URI.create(producer));
    this.namespace = namespace;
    this.objectMapper = objectMapper;
    this.sqlParser = sqlParser;
    this.fileSpecNaming = fileSpecNaming;
  }

  /** Maps a single Hop event to zero or one OpenLineage {@link RunEvent}s. */
  public Optional<RunEvent> toRunEvent(LineageEvent event) {
    if (event == null) {
      return Optional.empty();
    }
    return switch (event.getKind()) {
      case RUN_LIFECYCLE -> mapRunLifecycle(event);
      case FILE_IO -> mapFileIo(event);
      case RELATIONAL_IO -> mapRelationalIo(event);
      case TRANSFORM_SCHEMA -> Optional.empty();
      case HTTP_IO -> Optional.empty();
    };
  }

  /** Maps a batch of Hop events to OpenLineage {@link RunEvent}s (used by the sink). */
  public List<RunEvent> mapBatch(List<LineageEvent> events) {
    List<RunEvent> mapped = new ArrayList<>();
    for (LineageEvent event : events) {
      toRunEvent(event).ifPresent(mapped::add);
    }
    return mapped;
  }

  /**
   * Maps a single event to its OpenLineage JSON document (as a {@link JsonNode}). Thin wrapper over
   * {@link #toRunEvent} retained for unit tests; production delivery uses the {@link RunEvent}
   * objects directly via the client.
   */
  public Optional<JsonNode> map(LineageEvent event) {
    return toRunEvent(event).map(this::toJsonNode);
  }

  private JsonNode toJsonNode(RunEvent event) {
    try {
      return objectMapper.readTree(OpenLineageClientUtils.toJson(event));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize OpenLineage event", e);
    }
  }

  private Optional<RunEvent> mapRunLifecycle(LineageEvent event) {
    LineagePayload payload = event.getPayload();
    if (!(payload instanceof RunLifecycleLineagePayload lifecycle)) {
      return Optional.empty();
    }
    EventType eventType = toOpenLineageRunEventType(lifecycle.getPhase());
    if (eventType == null) {
      return Optional.empty();
    }
    LineageContext ctx = event.getContext();
    ZonedDateTime eventTime = toUtc(event.getTimestampMillis());

    // A finished pipeline run's column-lineage correlation state is no longer needed.
    if (ctx != null
        && ctx.getSubjectType() == LineageSubjectType.PIPELINE
        && (eventType == EventType.COMPLETE || eventType == EventType.FAIL)) {
      columnLineageCorrelator.clearRun(ctx.getLogChannelId());
    }

    OpenLineage.JobBuilder jobBuilder =
        ol.newJobBuilder().namespace(namespace).name(resolveJobName(ctx));
    if (ctx != null
        && ctx.getSubjectType() == LineageSubjectType.TRANSFORM
        && !Utils.isEmpty(ctx.getTransformName())) {
      jobBuilder.facets(
          ol.newJobFacetsBuilder()
              .jobType(ol.newJobTypeJobFacet("BATCH", "HOP", "TRANSFORM"))
              .build());
    }

    RunEvent runEvent =
        ol.newRunEventBuilder()
            .eventType(eventType)
            .eventTime(eventTime)
            .run(runNode(resolveRunId(ctx, event.getEventId()), eventTime, parentFacet(ctx)))
            .job(jobBuilder.build())
            .build();
    return Optional.of(runEvent);
  }

  /**
   * Builds a {@link ParentRunFacet} pointing at the run that started this subject, when the engine
   * provided the parent's log-channel id and a resolvable parent job name. Returns empty otherwise
   * (a ParentRunFacet requires a job namespace + name).
   */
  private ParentRunFacet parentFacet(LineageContext ctx) {
    if (ctx == null) {
      return null;
    }
    String parentRunId = null;
    String parentJobName = null;
    switch (ctx.getSubjectType()) {
      case TRANSFORM -> {
        parentRunId = ctx.getAttributes().get(ATTR_PIPELINE_LOG_CHANNEL_ID);
        parentJobName = ctx.getPipelineName();
      }
      case ACTION -> {
        parentRunId = ctx.getAttributes().get(ATTR_WORKFLOW_LOG_CHANNEL_ID);
        parentJobName = ctx.getWorkflowName();
      }
      case PIPELINE, WORKFLOW ->
          // A nested pipeline/workflow knows its parent run id but not the parent's job name,
          // so it cannot form a valid ParentRunFacet here; left for a future engine enrichment.
          parentRunId = null;
      default -> parentRunId = null;
    }
    if (Utils.isEmpty(parentRunId) || Utils.isEmpty(parentJobName)) {
      return null;
    }
    return ol.newParentRunFacet(
        ol.newParentRunFacetRun(toRunUuid(parentRunId)),
        ol.newParentRunFacetJob(namespace, parentJobName),
        null);
  }

  private Optional<RunEvent> mapFileIo(LineageEvent event) {
    LineagePayload payload = event.getPayload();
    if (!(payload instanceof FileIoLineagePayload fileIo)) {
      return Optional.empty();
    }
    LineageContext ctx = event.getContext();

    List<InputDataset> inputs = new ArrayList<>();
    List<OutputDataset> outputs = new ArrayList<>();
    addFileIoDatasets(fileIo, inputs, outputs);
    if (inputs.isEmpty() && outputs.isEmpty()) {
      return Optional.empty();
    }

    ZonedDateTime eventTime = toUtc(event.getTimestampMillis());
    RunEvent runEvent =
        ol.newRunEventBuilder()
            .eventType(EventType.OTHER)
            .eventTime(eventTime)
            .run(runNode(resolveFileIoRunId(ctx, event.getEventId()), eventTime, null))
            .job(ol.newJobBuilder().namespace(namespace).name(resolveFileIoJobName(ctx)).build())
            .inputs(inputs.isEmpty() ? null : inputs)
            .outputs(outputs.isEmpty() ? null : outputs)
            .build();
    return Optional.of(runEvent);
  }

  private void addFileIoDatasets(
      FileIoLineagePayload fileIo, List<InputDataset> inputs, List<OutputDataset> outputs) {
    FileIoOperation op = fileIo.getOperation();
    String sourceUri = fileIo.getSourceUri();
    String targetUri = fileIo.getTargetUri();
    FileIoContentSchema schema = fileIo.getContentSchema();

    switch (op) {
      case READ -> {
        if (!Utils.isEmpty(sourceUri)) {
          inputs.add(inputDataset(sourceUri, schema));
        }
      }
      case WRITE, DELETE -> {
        String uri = !Utils.isEmpty(targetUri) ? targetUri : sourceUri;
        if (!Utils.isEmpty(uri)) {
          outputs.add(outputDataset(uri, schema, op));
        }
      }
      case COPY, MOVE -> {
        if (!Utils.isEmpty(sourceUri)) {
          inputs.add(inputDataset(sourceUri, schema));
        }
        if (!Utils.isEmpty(targetUri)) {
          outputs.add(outputDataset(targetUri, schema, op));
        }
      }
      default -> {
        // no-op
      }
    }
  }

  private InputDataset inputDataset(String uri, FileIoContentSchema contentSchema) {
    LineageFileIdentity.Identity id = fileDatasetId(uri);
    return ol.newInputDatasetBuilder()
        .namespace(id.namespace())
        .name(id.name())
        .facets(fileDatasetFacets(uri, contentSchema, null))
        .build();
  }

  private OutputDataset outputDataset(
      String uri, FileIoContentSchema contentSchema, FileIoOperation operation) {
    LineageFileIdentity.Identity id = fileDatasetId(uri);
    return ol.newOutputDatasetBuilder()
        .namespace(id.namespace())
        .name(id.name())
        .facets(fileDatasetFacets(uri, contentSchema, fileLifecycle(operation)))
        .build();
  }

  /**
   * Lifecycle state of a written file dataset. File outputs replace their target, so a write (or
   * the target of a copy/move) is {@code OVERWRITE} and a delete is {@code DROP}. Append-mode
   * outputs are not distinguished yet — that would need the mode carried on the event. Reads have
   * no state.
   */
  private OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange fileLifecycle(
      FileIoOperation operation) {
    if (operation == null) {
      return null;
    }
    return switch (operation) {
      case WRITE, COPY, MOVE ->
          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
      case DELETE -> OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP;
      default -> null;
    };
  }

  /**
   * The OpenLineage {@code (namespace, name)} for a file/object-store URI. With {@code
   * HOP_LINEAGE_OPENLINEAGE_FILE_SPEC_NAMING} enabled this follows the OpenLineage naming spec (VFS
   * scheme → namespace, path → name via {@link LineageFileIdentity}) so a file reconciles across
   * transforms and engines; otherwise it keeps the legacy job-namespace + filename identity for
   * backward compatibility.
   */
  private LineageFileIdentity.Identity fileDatasetId(String uri) {
    if (fileSpecNaming) {
      LineageFileIdentity.Identity id = LineageFileIdentity.of(uri);
      if (id != null) {
        return id;
      }
    }
    return new LineageFileIdentity.Identity(namespace, nameFromUri(uri));
  }

  private DatasetFacets fileDatasetFacets(
      String uri,
      FileIoContentSchema contentSchema,
      OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycle) {
    OpenLineage.DatasetFacetsBuilder facets = ol.newDatasetFacetsBuilder();
    URI parsedUri = toUriOrNull(uri);
    if (parsedUri != null) {
      facets.dataSource(ol.newDatasourceDatasetFacet(nameFromUri(uri), parsedUri));
    }
    List<SchemaDatasetFacetFields> fields = schemaFields(contentSchema);
    if (!fields.isEmpty()) {
      facets.schema(ol.newSchemaDatasetFacet(fields));
    }
    if (lifecycle != null) {
      facets.lifecycleStateChange(ol.newLifecycleStateChangeDatasetFacet(lifecycle, null));
    }
    return facets.build();
  }

  private List<SchemaDatasetFacetFields> schemaFields(FileIoContentSchema contentSchema) {
    if (contentSchema == null || contentSchema.getColumns().isEmpty()) {
      return List.of();
    }
    List<SchemaDatasetFacetFields> fields = new ArrayList<>(contentSchema.getColumns().size());
    for (FileIoTabularColumn column : contentSchema.getColumns()) {
      OpenLineage.SchemaDatasetFacetFieldsBuilder field =
          ol.newSchemaDatasetFacetFieldsBuilder().name(column.getName());
      if (!Utils.isEmpty(column.getHopTypeName())) {
        field.type(column.getHopTypeName());
      }
      fields.add(field.build());
    }
    return fields;
  }

  /**
   * Maps a {@code RELATIONAL_IO} event to a {@link RunEvent} whose input/output datasets carry the
   * relational identity of the "OpenLineage dataset identity" page of the user manual: the dataset
   * {@code namespace} is the physical database ({@code postgres://host:port}), <b>not</b> the Hop
   * job namespace, and the dataset {@code name} is the engine-computed {@code
   * database.schema.table}. Emitting datasets in the relational namespace is what lets a Hop write
   * and, e.g., a dbt read of the same table land on one node in the lineage graph.
   */
  private Optional<RunEvent> mapRelationalIo(LineageEvent event) {
    LineagePayload payload = event.getPayload();
    if (!(payload instanceof RelationalIoLineagePayload relational)) {
      return Optional.empty();
    }
    String datasetNamespace = relational.getDatasetNamespace();
    if (Utils.isEmpty(datasetNamespace)) {
      // No resolvable relational namespace (e.g. an embedded/file database): nothing to stitch on.
      return Optional.empty();
    }
    LineageContext ctx = event.getContext();
    FileIoContentSchema schema = relational.getOutputSchema();

    // Prefer tables the engine already resolved (e.g. Table Output's known target); when it only
    // handed us SQL (Table Input, Execute SQL), recover the tables — and column lineage — by
    // parsing the statement.
    List<RelationalTable> inTables = relational.getInputs();
    List<RelationalTable> outTables = relational.getOutputs();
    List<RelationalSqlParser.ColumnEdge> columnEdges = List.of();
    if (inTables.isEmpty() && outTables.isEmpty() && !Utils.isEmpty(relational.getSqlText())) {
      RelationalSqlParser.Tables parsed =
          sqlParser.parse(relational.getSqlText(), sqlDialect(datasetNamespace));
      inTables = parsed.inputs();
      outTables = parsed.outputs();
      columnEdges = parsed.columnLineage();
    }

    // Tables recovered from SQL may name neither catalog nor schema; fill them from the connection
    // so a read of table/schema.table shares one identity with a write of catalog.schema.table.
    String defaultCatalog = relational.getDefaultCatalog();
    String defaultSchema = relational.getDefaultSchema();
    inTables = qualify(inTables, defaultCatalog, defaultSchema);
    outTables = qualify(outTables, defaultCatalog, defaultSchema);

    // Column-level lineage for the stream path: a read records how its stream fields map to source
    // columns; a write resolves its columns back through the transform that produced each stream
    // field (RelationalWriteColumn.originTransform) to the matching read.
    String runId = pipelineLogChannelId(ctx);
    String transformName = ctx != null ? ctx.getTransformName() : null;
    if (relational.getOperation() == RelationalIoOperation.READ && !inTables.isEmpty()) {
      columnLineageCorrelator.registerRead(runId, transformName, columnEdges, inTables);
    } else if (relational.getOperation() == RelationalIoOperation.WRITE
        && !relational.getWriteColumns().isEmpty()) {
      columnEdges = columnLineageCorrelator.correlate(runId, relational.getWriteColumns());
    }

    List<String> outNames = new ArrayList<>();
    for (RelationalTable table : outTables) {
      String name = LineageRelationalIdentity.datasetName(table);
      if (!Utils.isEmpty(name)) {
        outNames.add(name);
      }
    }
    // Column lineage maps output fields to input fields, so it belongs on the single output dataset
    // of an INSERT … SELECT. With zero or several output tables there is no unambiguous target.
    OpenLineage.ColumnLineageDatasetFacet columnLineage =
        outNames.size() == 1
            ? columnLineageFacet(datasetNamespace, columnEdges, defaultCatalog, defaultSchema)
            : null;
    // A write recovered from SQL has no row schema of its own; declare its columns from the column
    // lineage so collectors (e.g. Marquez) have output fields to render the column-level graph on.
    FileIoContentSchema outputSchema =
        schema == null && columnLineage != null ? schemaFromColumnEdges(columnEdges) : schema;

    OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycle =
        relationalLifecycle(relational.getLifecycle());
    List<OutputDataset> outputs = new ArrayList<>();
    for (String name : outNames) {
      outputs.add(
          ol.newOutputDatasetBuilder()
              .namespace(datasetNamespace)
              .name(name)
              .facets(
                  relationalDatasetFacets(datasetNamespace, outputSchema, columnLineage, lifecycle))
              .build());
    }

    // A read's output row shape describes the single source it selected from; attach it only when
    // there is exactly one input and no outputs, to avoid mislabelling a multi-table join.
    boolean schemaOnInput = outputs.isEmpty() && inTables.size() == 1;
    List<InputDataset> inputs = new ArrayList<>();
    for (RelationalTable table : inTables) {
      String name = LineageRelationalIdentity.datasetName(table);
      if (Utils.isEmpty(name)) {
        continue;
      }
      inputs.add(
          ol.newInputDatasetBuilder()
              .namespace(datasetNamespace)
              .name(name)
              .facets(
                  relationalDatasetFacets(
                      datasetNamespace, schemaOnInput ? schema : null, null, null))
              .build());
    }

    if (inputs.isEmpty() && outputs.isEmpty()) {
      // Read events carry only SQL until a sink parses it into tables — nothing to emit yet.
      return Optional.empty();
    }

    ZonedDateTime eventTime = toUtc(event.getTimestampMillis());
    RunEvent runEvent =
        ol.newRunEventBuilder()
            .eventType(EventType.OTHER)
            .eventTime(eventTime)
            .run(runNode(resolveFileIoRunId(ctx, event.getEventId()), eventTime, null))
            .job(relationalJob(ctx, relational.getSqlText()))
            .inputs(inputs.isEmpty() ? null : inputs)
            .outputs(outputs.isEmpty() ? null : outputs)
            .build();
    return Optional.of(runEvent);
  }

  /**
   * Job for a relational event, correlated to the pipeline run like file I/O, with an optional
   * {@code SQLJobFacet} carrying the statement text when the transform provided it (Table Input,
   * Execute SQL).
   */
  private OpenLineage.Job relationalJob(LineageContext ctx, String sqlText) {
    OpenLineage.JobBuilder job =
        ol.newJobBuilder().namespace(namespace).name(resolveFileIoJobName(ctx));
    if (!Utils.isEmpty(sqlText)) {
      job.facets(ol.newJobFacetsBuilder().sql(ol.newSQLJobFacet(sqlText, null)).build());
    }
    return job.build();
  }

  /**
   * SQL dialect for the parser, derived from the database namespace scheme ({@code
   * postgres://db:5432} → {@code postgres}). Namespaces without a host (e.g. {@code bigquery}) are
   * themselves the dialect; null lets the parser fall back to its default grammar.
   */
  static String sqlDialect(String datasetNamespace) {
    if (Utils.isEmpty(datasetNamespace)) {
      return null;
    }
    int sep = datasetNamespace.indexOf("://");
    return sep > 0 ? datasetNamespace.substring(0, sep) : datasetNamespace;
  }

  private DatasetFacets relationalDatasetFacets(
      String datasetNamespace,
      FileIoContentSchema contentSchema,
      OpenLineage.ColumnLineageDatasetFacet columnLineage,
      OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycle) {
    OpenLineage.DatasetFacetsBuilder facets = ol.newDatasetFacetsBuilder();
    URI parsedUri = toUriOrNull(datasetNamespace);
    if (parsedUri != null) {
      facets.dataSource(ol.newDatasourceDatasetFacet(datasetNamespace, parsedUri));
    }
    List<SchemaDatasetFacetFields> fields = schemaFields(contentSchema);
    if (!fields.isEmpty()) {
      facets.schema(ol.newSchemaDatasetFacet(fields));
    }
    if (columnLineage != null) {
      facets.columnLineage(columnLineage);
    }
    if (lifecycle != null) {
      facets.lifecycleStateChange(ol.newLifecycleStateChangeDatasetFacet(lifecycle, null));
    }
    return facets.build();
  }

  /**
   * Maps the neutral relational lifecycle to the OpenLineage lifecycle-state enum (names align).
   */
  private static OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange
      relationalLifecycle(RelationalLifecycle lifecycle) {
    if (lifecycle == null) {
      return null;
    }
    return switch (lifecycle) {
      case OVERWRITE -> OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
      case TRUNCATE -> OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.TRUNCATE;
      case DROP -> OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP;
      case CREATE -> OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE;
    };
  }

  /**
   * Builds a {@code columnLineage} facet from the parser's per-field edges: each output field maps
   * to the input relational-table columns it derives from, referenced by the same {@code
   * (namespace, name)} identity as the input datasets. Returns null when no edge resolves to a
   * usable input.
   */
  private OpenLineage.ColumnLineageDatasetFacet columnLineageFacet(
      String datasetNamespace,
      List<RelationalSqlParser.ColumnEdge> edges,
      String defaultCatalog,
      String defaultSchema) {
    if (edges == null || edges.isEmpty()) {
      return null;
    }
    OpenLineage.ColumnLineageDatasetFacetFieldsBuilder fieldsBuilder =
        ol.newColumnLineageDatasetFacetFieldsBuilder();
    boolean anyField = false;
    for (RelationalSqlParser.ColumnEdge edge : edges) {
      List<OpenLineage.InputField> inputFields = new ArrayList<>();
      for (RelationalSqlParser.FieldRef ref : edge.inputs()) {
        RelationalTable table =
            LineageRelationalIdentity.withDefaults(ref.table(), defaultCatalog, defaultSchema);
        String name = LineageRelationalIdentity.datasetName(table);
        if (Utils.isEmpty(name) || Utils.isEmpty(ref.field())) {
          continue;
        }
        inputFields.add(ol.newInputField(datasetNamespace, name, ref.field(), null));
      }
      if (inputFields.isEmpty()) {
        continue;
      }
      fieldsBuilder.put(
          edge.outputField(),
          ol.newColumnLineageDatasetFacetFieldsAdditional(inputFields, null, null));
      anyField = true;
    }
    if (!anyField) {
      return null;
    }
    return ol.newColumnLineageDatasetFacet(fieldsBuilder.build(), null);
  }

  /**
   * Builds a tabular schema from the distinct output fields of the column lineage, so an
   * SQL-recovered write (which carries no row schema) still declares its columns. Types are unknown
   * from SQL parsing and are left unset.
   */
  private static FileIoContentSchema schemaFromColumnEdges(
      List<RelationalSqlParser.ColumnEdge> edges) {
    List<FileIoTabularColumn> columns = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();
    for (RelationalSqlParser.ColumnEdge edge : edges) {
      if (!Utils.isEmpty(edge.outputField()) && seen.add(edge.outputField())) {
        columns.add(new FileIoTabularColumn(edge.outputField(), null, 0, 0, null, null, false));
      }
    }
    return new FileIoContentSchema("jdbc", columns, List.of());
  }

  /** Returns {@code tables} with each blank catalog/schema filled from the connection defaults. */
  private static List<RelationalTable> qualify(
      List<RelationalTable> tables, String defaultCatalog, String defaultSchema) {
    if ((Utils.isEmpty(defaultCatalog) && Utils.isEmpty(defaultSchema)) || tables.isEmpty()) {
      return tables;
    }
    List<RelationalTable> qualified = new ArrayList<>(tables.size());
    for (RelationalTable table : tables) {
      qualified.add(LineageRelationalIdentity.withDefaults(table, defaultCatalog, defaultSchema));
    }
    return qualified;
  }

  private OpenLineage.Run runNode(String runId, ZonedDateTime eventTime, ParentRunFacet parent) {
    OpenLineage.RunFacetsBuilder facets =
        ol.newRunFacetsBuilder().nominalTime(ol.newNominalTimeRunFacet(eventTime, null));
    if (parent != null) {
      facets.parent(parent);
    }
    return ol.newRunBuilder().runId(toRunUuid(runId)).facets(facets.build()).build();
  }

  private static ZonedDateTime toUtc(long timestampMillis) {
    return Instant.ofEpochMilli(timestampMillis).atZone(ZoneOffset.UTC);
  }

  /**
   * Converts a Hop run id to the UUID OpenLineage requires. Hop log-channel ids are already UUID
   * strings (used as-is to preserve the locked contract); only synthetic fallbacks are derived
   * deterministically so the same string always yields the same UUID.
   */
  static UUID toRunUuid(String runId) {
    try {
      return UUID.fromString(runId);
    } catch (IllegalArgumentException notAUuid) {
      return UUID.nameUUIDFromBytes(("hop:" + runId).getBytes(StandardCharsets.UTF_8));
    }
  }

  private static URI toUriOrNull(String uri) {
    if (Utils.isEmpty(uri)) {
      return null;
    }
    try {
      return URI.create(uri);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  static EventType toOpenLineageRunEventType(RunLifecyclePhase phase) {
    return switch (phase) {
      case STARTED -> EventType.START;
      case FINISHED -> EventType.COMPLETE;
      case FAILED -> EventType.FAIL;
      case FINISHING -> null;
    };
  }

  static String resolveRunId(LineageContext ctx, String eventId) {
    if (ctx != null && !Utils.isEmpty(ctx.getLogChannelId())) {
      return ctx.getLogChannelId();
    }
    String jobName = resolveJobName(ctx);
    String suffix = eventId != null && eventId.length() >= 8 ? eventId.substring(0, 8) : "unknown";
    return jobName + "-" + suffix;
  }

  /**
   * File I/O from a transform is correlated to the parent pipeline run when Hop provides {@link
   * #ATTR_PIPELINE_LOG_CHANNEL_ID}, so Marquez shows inputs/outputs on the pipeline job the user
   * opened (not a separate transform-only run).
   */
  static String resolveFileIoRunId(LineageContext ctx, String eventId) {
    String pipelineRunId = pipelineLogChannelId(ctx);
    if (!Utils.isEmpty(pipelineRunId)) {
      return pipelineRunId;
    }
    return resolveRunId(ctx, eventId);
  }

  static String resolveFileIoJobName(LineageContext ctx) {
    if (ctx != null && !Utils.isEmpty(ctx.getPipelineName())) {
      return ctx.getPipelineName();
    }
    return resolveJobName(ctx);
  }

  static String pipelineLogChannelId(LineageContext ctx) {
    if (ctx == null || ctx.getAttributes().isEmpty()) {
      return null;
    }
    return ctx.getAttributes().get(ATTR_PIPELINE_LOG_CHANNEL_ID);
  }

  static String resolveJobName(LineageContext ctx) {
    if (ctx == null) {
      return "unknown";
    }
    if (ctx.getSubjectType() == LineageSubjectType.TRANSFORM
        && !Utils.isEmpty(ctx.getTransformName())) {
      if (!Utils.isEmpty(ctx.getPipelineName())) {
        return ctx.getPipelineName() + "/" + ctx.getTransformName();
      }
      return ctx.getTransformName();
    }
    if (!Utils.isEmpty(ctx.getPipelineName())) {
      return ctx.getPipelineName();
    }
    if (!Utils.isEmpty(ctx.getWorkflowName())) {
      return ctx.getWorkflowName();
    }
    if (!Utils.isEmpty(ctx.getTransformName())) {
      return ctx.getTransformName();
    }
    if (!Utils.isEmpty(ctx.getActionName())) {
      return ctx.getActionName();
    }
    return "unknown";
  }

  static String nameFromUri(String uri) {
    if (Utils.isEmpty(uri)) {
      return "dataset";
    }
    int query = uri.indexOf('?');
    String path = query >= 0 ? uri.substring(0, query) : uri;
    int slash = path.lastIndexOf('/');
    if (slash >= 0 && slash < path.length() - 1) {
      return path.substring(slash + 1);
    }
    return path;
  }
}
