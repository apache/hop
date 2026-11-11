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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lineage.LineageFileIdentity;
import org.apache.hop.lineage.LineageRelationalIdentity;
import org.apache.hop.lineage.LineageRunLifecycleEmitter;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.model.FileIoContentSchema;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.FileIoTabularColumn;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineagePayload;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload.Transition;
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
 *
 * <p>File and relational I/O is observed while a run is in flight and carried on that run's
 * terminal event, where OpenLineage expects to find a run's datasets. See {@link #observe}.
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

  /**
   * Parent run id and parent job name for a pipeline or workflow that was started by another
   * subject (an action running a pipeline, or a pipeline running a sub-pipeline). Without the name
   * no ParentRunFacet can be built: OpenLineage needs the parent's job namespace and name.
   */
  static final String ATTR_PARENT_RUN_ID = "parentRunId";

  static final String ATTR_PARENT_JOB_NAME = "parentJobName";

  /**
   * How many runs to hold observations for, and how many pipeline runs to hold transform state for.
   * Well above any realistic number of concurrently running pipelines. A run pushed out of this
   * window is not lost: what it observed is sent as a standalone event (see {@link #drainEvicted}).
   */
  static final int MAX_PENDING_RUNS = 1_024;

  /** Key of the job facet that says what a job is in Hop terms (pipeline, transform, ...). */
  public static final String HOP_JOB_FACET = "hop";

  /** {@value #HOP_JOB_FACET} facet: the transforms that feed a transform, from the hop graph. */
  public static final String UPSTREAM_TRANSFORMS = "upstreamTransforms";

  /** {@value #HOP_JOB_FACET} facet: a pipeline's transforms that no transform feeds. */
  public static final String ROOT_TRANSFORMS = "rootTransforms";

  /**
   * Dataset facet on the target of a file copy or move, naming its source ({@code namespace},
   * {@code name}).
   */
  public static final String COPIED_FROM_FACET = "hopCopiedFrom";

  /**
   * {@value #HOP_JOB_FACET} facet on a pipeline's or workflow's terminal event: every transform or
   * action it is made of, so a collector can retire the jobs of ones that were removed or renamed.
   */
  public static final String TRANSFORMS = "transforms";

  public static final String ACTIONS = "actions";

  /**
   * {@value #HOP_JOB_FACET} facet: the actions that lead to an action, from the workflow's hops.
   */
  public static final String UPSTREAM_ACTIONS = "upstreamActions";

  /** {@value #HOP_JOB_FACET} facet: a workflow's actions that no hop leads to (its start). */
  public static final String ROOT_ACTIONS = "rootActions";

  /** {@code processing_engine} name: lets a collector (DataHub) tell which engine ran a job. */
  public static final String PROCESSING_ENGINE_NAME = "hop";

  private final OpenLineage ol;
  private final URI producer;
  private final ObjectMapper objectMapper;
  private final String namespace;
  private final RelationalSqlParser sqlParser;
  private final boolean parentRuns;
  private final boolean transformJobs;
  private final boolean propagateInputs;
  private final boolean fileSpecNaming;
  private final String engineVersion;
  private final RelationalColumnLineageCorrelator columnLineageCorrelator =
      new RelationalColumnLineageCorrelator();

  /** Runs pushed out of {@link #pendingRuns} before they ended; drained as standalone events. */
  private final Queue<PendingRun> evictedRuns = new ConcurrentLinkedQueue<>();

  /** Pipeline runs pushed out of {@link #pipelineRuns} before they ended. */
  private final Queue<PipelineRunState> evictedPipelineRuns = new ConcurrentLinkedQueue<>();

  /**
   * Datasets observed for runs that have not reported a terminal event yet, keyed by the job and
   * run that observation belongs to. See {@link #observe}.
   */
  private final Map<String, PendingRun> pendingRuns =
      Collections.synchronizedMap(new BoundedMap<>(MAX_PENDING_RUNS, evictedRuns::add));

  /**
   * The runs whose terminal event has been mapped, so a dataset observed for one of them after the
   * fact is recognised as having no event left to carry it. Kept apart from {@link #pendingRuns} so
   * the many terminal events of a large pipeline cannot evict the runs still collecting datasets.
   */
  private final Set<String> endedRuns =
      Collections.synchronizedSet(
          Collections.newSetFromMap(new BoundedMap<String, Boolean>(MAX_PENDING_RUNS, null)));

  /**
   * With transform jobs on, what each running pipeline run has collected so far: its hop graph, the
   * datasets of each transform and the transforms' terminal events, which wait for the pipeline's
   * own terminal event. Only then is everything a transform consumed known — an upstream writer can
   * report its I/O after a downstream transform finished, and the graph can arrive after the first
   * I/O — so that is when transform jobs are given their datasets.
   */
  private final Map<String, PipelineRunState> pipelineRuns =
      Collections.synchronizedMap(new BoundedMap<>(MAX_PENDING_RUNS, evictedPipelineRuns::add));

  /**
   * The hop graph of each running workflow, from its {@code WORKFLOW_GRAPH} event, so an action's
   * event can name the actions that lead to it. Dropped when the workflow ends.
   */
  private final Map<String, List<Transition>> workflowGraphs =
      Collections.synchronizedMap(new BoundedMap<>(MAX_PENDING_RUNS, null));

  /** Pipeline runs that ended, so a late transform event is not held for a pipeline gone by. */
  private final Set<String> endedPipelineRuns =
      Collections.synchronizedSet(
          Collections.newSetFromMap(new BoundedMap<String, Boolean>(MAX_PENDING_RUNS, null)));

  public OpenLineageEventMapper(String namespace, String producer) {
    this(builder(namespace, producer));
  }

  public OpenLineageEventMapper(String namespace, String producer, ObjectMapper objectMapper) {
    this(builder(namespace, producer).objectMapper(objectMapper));
  }

  public OpenLineageEventMapper(String namespace, String producer, RelationalSqlParser sqlParser) {
    this(builder(namespace, producer).sqlParser(sqlParser));
  }

  public OpenLineageEventMapper(
      String namespace, String producer, ObjectMapper objectMapper, RelationalSqlParser sqlParser) {
    this(builder(namespace, producer).objectMapper(objectMapper).sqlParser(sqlParser));
  }

  private OpenLineageEventMapper(Builder builder) {
    this.producer = URI.create(builder.producer);
    this.ol = new OpenLineage(producer);
    this.namespace = builder.namespace;
    this.objectMapper = builder.objectMapper;
    this.sqlParser = builder.sqlParser;
    this.parentRuns = builder.parentRuns;
    this.transformJobs = builder.transformJobs;
    this.propagateInputs = builder.propagateInputs;
    this.fileSpecNaming = builder.fileSpecNaming;
    this.engineVersion = hopVersion();
  }

  public static Builder builder(String namespace, String producer) {
    return new Builder(namespace, producer);
  }

  /** Options for a mapper; everything but the namespace and producer has a default. */
  public static final class Builder {
    private final String namespace;
    private final String producer;
    private ObjectMapper objectMapper = OpenLineageClientUtils.newObjectMapper();
    private RelationalSqlParser sqlParser = RelationalSqlParser.create(false, null);
    private boolean transformJobs;
    private boolean propagateInputs = true;
    private boolean parentRuns = true;
    private boolean fileSpecNaming = true;

    private Builder(String namespace, String producer) {
      this.namespace = namespace;
      this.producer = producer;
    }

    public Builder objectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    public Builder sqlParser(RelationalSqlParser sqlParser) {
      this.sqlParser = sqlParser;
      return this;
    }

    /**
     * One job per transform, each holding the datasets it read plus what its upstream transforms
     * produced, instead of every dataset on the pipeline's job. Off by default.
     */
    public Builder transformJobs(boolean transformJobs) {
      this.transformJobs = transformJobs;
      return this;
    }

    /**
     * With transform jobs, whether a transform's job also lists what its upstream transforms
     * produced as inputs (on by default). That is what gives OpenLineage collectors, which derive
     * dataset edges through jobs, a real dataset pair on each job. A collector that links jobs to
     * each other instead (DataHub, from the {@value #HOP_JOB_FACET} facet's {@code
     * upstreamTransforms}) turns it off, so each job lists only the datasets it touched itself.
     */
    public Builder propagateInputs(boolean propagateInputs) {
      this.propagateInputs = propagateInputs;
      return this;
    }

    /** Whether runs carry a {@code ParentRunFacet}. On by default. */
    public Builder parentRuns(boolean parentRuns) {
      this.parentRuns = parentRuns;
      return this;
    }

    /**
     * Whether file datasets follow the OpenLineage naming spec (on by default). Off keeps the job
     * namespace + file name identity of Hop 2.20 and earlier, which the OpenLineage sink still uses
     * unless {@code HOP_LINEAGE_OPENLINEAGE_FILE_SPEC_NAMING} is {@code Y}.
     */
    public Builder fileSpecNaming(boolean fileSpecNaming) {
      this.fileSpecNaming = fileSpecNaming;
      return this;
    }

    public OpenLineageEventMapper build() {
      return new OpenLineageEventMapper(this);
    }
  }

  private static String hopVersion() {
    String[] version = new HopVersionProvider().getVersion();
    return version.length > 0 && !Utils.isEmpty(version[0]) ? version[0] : "unknown";
  }

  /**
   * Maps a single Hop event to the OpenLineage {@link RunEvent}s it completes: usually none or one.
   *
   * <p>An I/O event maps to no event of its own: it is recorded against the run it belongs to and
   * travels on that run's terminal event instead ({@link #observe}). With transform jobs on, a
   * transform's terminal event waits for its pipeline's, which then releases all of them.
   */
  public List<RunEvent> toRunEvents(LineageEvent event) {
    if (event == null) {
      return List.of();
    }
    return switch (event.getKind()) {
      case RUN_LIFECYCLE -> mapRunLifecycle(event);
      case PIPELINE_GRAPH -> {
        rememberGraph(event);
        yield List.of();
      }
      case WORKFLOW_GRAPH -> {
        if (event.getPayload() instanceof PipelineGraphLineagePayload graph) {
          workflowGraphs.put(
              resolveRunId(event.getContext(), event.getEventId()), graph.getTransitions());
        }
        yield List.of();
      }
      case FILE_IO -> mapFileIo(event).map(List::of).orElse(List.of());
      case RELATIONAL_IO -> mapRelationalIo(event).map(List::of).orElse(List.of());
      case TRANSFORM_SCHEMA, HTTP_IO -> List.of();
    };
  }

  /** Remembers the hop graph so a transform's terminal event can forward its upstream datasets. */
  private void rememberGraph(LineageEvent event) {
    if (!transformJobs || !(event.getPayload() instanceof PipelineGraphLineagePayload graph)) {
      return;
    }
    LineageContext ctx = event.getContext();
    String pipelineRunId = resolveRunId(ctx, event.getEventId());
    if (!Utils.isEmpty(pipelineRunId) && !endedPipelineRuns.contains(pipelineRunId)) {
      pipelineRun(pipelineRunId, resolveJobName(ctx), JobIdentity.of(ctx)).graph =
          graph.getTransitions();
    }
  }

  /** Maps a batch of Hop events to OpenLineage {@link RunEvent}s (used by the sink). */
  public List<RunEvent> mapBatch(List<LineageEvent> events) {
    List<RunEvent> mapped = new ArrayList<>();
    for (LineageEvent event : events) {
      mapped.addAll(toRunEvents(event));
    }
    mapped.addAll(drainEvicted());
    return mapped;
  }

  /**
   * Maps a single event to the OpenLineage JSON document of its own run (as a {@link JsonNode}).
   * Thin wrapper over {@link #toRunEvents} retained for unit tests; production delivery uses the
   * {@link RunEvent} objects directly via the client.
   */
  public Optional<JsonNode> map(LineageEvent event) {
    List<RunEvent> events = toRunEvents(event);
    return events.isEmpty()
        ? Optional.empty()
        : Optional.of(toJsonNode(events.get(events.size() - 1)));
  }

  private JsonNode toJsonNode(RunEvent event) {
    try {
      return objectMapper.readTree(OpenLineageClientUtils.toJson(event));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize OpenLineage event", e);
    }
  }

  private static EventType eventTypeOf(LineageEvent event) {
    return event.getPayload() instanceof RunLifecycleLineagePayload lifecycle
        ? toOpenLineageRunEventType(lifecycle.getPhase())
        : null;
  }

  private List<RunEvent> mapRunLifecycle(LineageEvent event) {
    EventType eventType = eventTypeOf(event);
    if (eventType == null) {
      return List.of();
    }
    LineageContext ctx = event.getContext();
    if (!isTerminal(eventType)) {
      return List.of(runEvent(event, eventType, List.of(), List.of(), null));
    }
    if (isTransformOfAPipelineRun(ctx) && !endedPipelineRuns.contains(pipelineLogChannelId(ctx))) {
      // Its datasets are only complete once the pipeline ends; see pipelineRuns.
      pipelineRun(ctx).deferredTerminals.add(event);
      return List.of();
    }

    // A terminal event carries the datasets the run touched. OpenLineage has no event of its own
    // for a dataset appearing on its own, and a collector may reject one: DataHub answers 500 to a
    // run event whose datasets hang off no run. Collectors take dataset lineage from a run's inputs
    // and outputs, so that is where the observations go.
    String runId = resolveRunId(ctx, event.getEventId());
    String key = runKey(resolveJobName(ctx), runId);
    PendingRun run = pendingRuns.remove(key);
    endedRuns.add(key);
    List<RunEvent> events = new ArrayList<>();
    JobIdentity identity = JobIdentity.of(ctx);
    if (ctx != null && ctx.getSubjectType() == LineageSubjectType.ACTION) {
      List<Transition> graph =
          workflowGraphs.get(ctx.getAttributes().get(ATTR_WORKFLOW_LOG_CHANNEL_ID));
      if (graph != null) {
        identity = identity.with(UPSTREAM_ACTIONS, leadingTo(graph, ctx.getActionName()));
      }
    }
    if (ctx != null && ctx.getSubjectType() == LineageSubjectType.WORKFLOW) {
      List<Transition> graph = workflowGraphs.remove(runId);
      if (graph != null) {
        identity =
            identity
                .with(ROOT_ACTIONS, roots(graph, List.of()))
                .with(ACTIONS, nodes(graph, List.of()));
      }
    }
    if (ctx != null && ctx.getSubjectType() == LineageSubjectType.PIPELINE) {
      endedPipelineRuns.add(runId);
      PipelineRunState state = pipelineRuns.remove(runId);
      if (state != null) {
        if (run == null) {
          run = new PendingRun(resolveJobName(ctx), runId, identity);
        }
        events.addAll(finishPipelineRun(state, run));
        if (state.graph != null) {
          identity =
              identity
                  .with(ROOT_TRANSFORMS, rootTransforms(state))
                  .with(TRANSFORMS, allTransforms(state));
        }
      }
    }
    events.add(
        run == null
            ? runEvent(event, eventType, List.of(), List.of(), null, identity)
            : runEvent(
                event,
                eventType,
                run.getInputs(),
                run.getOutputs(),
                run.getSoleSqlText(),
                identity));
    return events;
  }

  /**
   * The OpenLineage event for one lifecycle event of a run, carrying the given datasets. Every run
   * event says what produced it ({@code processing_engine}) and what its job is in Hop terms (job
   * type and the {@value #HOP_JOB_FACET} job facet), so a collector does not have to guess either
   * from the job name.
   */
  private RunEvent runEvent(
      LineageEvent event,
      EventType eventType,
      List<InputDataset> inputs,
      List<OutputDataset> outputs,
      String sqlText) {
    return runEvent(event, eventType, inputs, outputs, sqlText, JobIdentity.of(event.getContext()));
  }

  private RunEvent runEvent(
      LineageEvent event,
      EventType eventType,
      List<InputDataset> inputs,
      List<OutputDataset> outputs,
      String sqlText,
      JobIdentity identity) {
    LineageContext ctx = event.getContext();
    ZonedDateTime eventTime = toUtc(event.getTimestampMillis());
    return ol.newRunEventBuilder()
        .eventType(eventType)
        .eventTime(eventTime)
        .run(
            runNode(
                resolveRunId(ctx, event.getEventId()),
                eventTime,
                parentRuns ? parentFacet(ctx) : null))
        .job(job(resolveJobName(ctx), identity, sqlText))
        .inputs(inputs.isEmpty() ? null : inputs)
        .outputs(outputs.isEmpty() ? null : outputs)
        .build();
  }

  /**
   * A job with its type, its Hop identity and — when the run executed exactly one statement (Table
   * Input, Execute SQL) — that statement. With several statements the datasets keep their column
   * lineage and no statement is claimed for the job.
   */
  private OpenLineage.Job job(String jobName, JobIdentity identity, String sqlText) {
    OpenLineage.JobFacetsBuilder facets = ol.newJobFacetsBuilder();
    if (identity.subjectType() != null) {
      facets.jobType(ol.newJobTypeJobFacet("BATCH", "HOP", identity.subjectType().name(), null));
      facets.put(HOP_JOB_FACET, hopJobFacet(identity));
    }
    if (!Utils.isEmpty(sqlText)) {
      facets.sql(ol.newSQLJobFacet(sqlText, null));
    }
    if (!Utils.isEmpty(identity.description())) {
      facets.documentation(ol.newDocumentationJobFacet(identity.description(), null));
    }
    return ol.newJobBuilder().namespace(namespace).name(jobName).facets(facets.build()).build();
  }

  /**
   * What a job is in Hop terms: the subject type and the names it is made of. A job name such as
   * {@code nightly/Load orders} cannot be split back reliably (names may contain the separator), so
   * a sink that models pipelines and transforms separately (DataHub) reads them from here.
   */
  private OpenLineage.JobFacet hopJobFacet(JobIdentity identity) {
    OpenLineage.DefaultJobFacet facet = new OpenLineage.DefaultJobFacet(producer, null);
    Map<String, Object> properties = facet.getAdditionalProperties();
    properties.put("subjectType", identity.subjectType().name());
    putIfPresent(properties, "pipelineName", identity.pipelineName());
    putIfPresent(properties, "workflowName", identity.workflowName());
    putIfPresent(properties, "transformName", identity.transformName());
    putIfPresent(properties, "actionName", identity.actionName());
    putIfPresent(properties, "filename", identity.filename());
    properties.putAll(identity.extra());
    return facet;
  }

  private static void putIfPresent(Map<String, Object> properties, String key, String value) {
    if (!Utils.isEmpty(value)) {
      properties.put(key, value);
    }
  }

  /** A run reports COMPLETE or FAIL once it is over; those two carry its datasets. */
  private static boolean isTerminal(EventType eventType) {
    return eventType == EventType.COMPLETE || eventType == EventType.FAIL;
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
      case PIPELINE, WORKFLOW -> {
        // The engine puts the parent's job name next to its run id when this run was started by an
        // action or a parent pipeline; see LineageRunLifecycleEmitter.
        parentRunId = ctx.getAttributes().get(ATTR_PARENT_RUN_ID);
        parentJobName = ctx.getAttributes().get(ATTR_PARENT_JOB_NAME);
      }
      default -> parentRunId = null;
    }
    if (Utils.isEmpty(parentRunId) || Utils.isEmpty(parentJobName)) {
      return null;
    }
    return ol.newParentRunFacet(
        ol.newParentRunFacetRun(toRunUuid(parentRunId), null),
        ol.newParentRunFacetJob(namespace, parentJobName, null),
        null);
  }

  private Optional<RunEvent> mapFileIo(LineageEvent event) {
    LineagePayload payload = event.getPayload();
    if (!(payload instanceof FileIoLineagePayload fileIo)) {
      return Optional.empty();
    }

    List<InputDataset> inputs = new ArrayList<>();
    List<OutputDataset> outputs = new ArrayList<>();
    addFileIoDatasets(fileIo, inputs, outputs);
    return observe(event, inputs, outputs, null);
  }

  /**
   * Records the datasets an I/O observation found against the run it belongs to, so that run's
   * terminal event can carry them. Returns an event only when there is no run left to carry them:
   * an observation that arrives after its run reported COMPLETE or FAIL has nothing ahead of it,
   * and is sent as a standalone dataset event.
   */
  private Optional<RunEvent> observe(
      LineageEvent event, List<InputDataset> inputs, List<OutputDataset> outputs, String sqlText) {
    if (inputs.isEmpty() && outputs.isEmpty()) {
      return Optional.empty();
    }
    LineageContext ctx = event.getContext();
    boolean perTransform = isTransformOfAPipelineRun(ctx);
    if (perTransform && !endedPipelineRuns.contains(pipelineLogChannelId(ctx))) {
      // Copies of a transform share one job, so they share one record of what it touched.
      pipelineRun(ctx)
          .transformIo
          .computeIfAbsent(
              ctx.getTransformName(),
              name ->
                  new PendingRun(
                      resolveJobName(ctx),
                      resolveRunId(ctx, event.getEventId()),
                      JobIdentity.of(ctx)))
          .add(inputs, outputs, sqlText);
      return Optional.empty();
    }
    String jobName = perTransform ? resolveJobName(ctx) : resolveFileIoJobName(ctx);
    String runId =
        perTransform
            ? resolveRunId(ctx, event.getEventId())
            : resolveFileIoRunId(ctx, event.getEventId());
    JobIdentity identity = perTransform ? JobIdentity.of(ctx) : JobIdentity.ioOwner(ctx);
    String key = runKey(jobName, runId);
    if (perTransform || endedRuns.contains(key)) {
      PendingRun late = new PendingRun(jobName, runId, identity);
      late.add(inputs, outputs, sqlText);
      return Optional.of(standaloneEvent(late, inputs, event.getTimestampMillis()));
    }
    pendingRuns
        .computeIfAbsent(key, k -> new PendingRun(jobName, runId, identity))
        .add(inputs, outputs, sqlText);
    return Optional.empty();
  }

  /**
   * Whether this event belongs to a transform job of its own rather than to its pipeline's: only
   * when the mapping is switched on and the transform's pipeline run is known.
   */
  private boolean isTransformOfAPipelineRun(LineageContext ctx) {
    return transformJobs
        && ctx != null
        && ctx.getSubjectType() == LineageSubjectType.TRANSFORM
        && !Utils.isEmpty(ctx.getTransformName())
        && !Utils.isEmpty(pipelineLogChannelId(ctx));
  }

  private PipelineRunState pipelineRun(LineageContext transformCtx) {
    return pipelineRun(
        pipelineLogChannelId(transformCtx),
        resolveFileIoJobName(transformCtx),
        JobIdentity.ioOwner(transformCtx));
  }

  private PipelineRunState pipelineRun(String pipelineRunId, String jobName, JobIdentity identity) {
    return pipelineRuns.computeIfAbsent(
        pipelineRunId, id -> new PipelineRunState(id, jobName, identity));
  }

  /**
   * Gives each held transform terminal event its datasets, now that the pipeline is over.
   *
   * <p>With the run's hop graph, a transform's job gets what it read plus what its upstream
   * transforms produced, so each of its dataset pairs is a real one. Without a graph (an engine
   * that predates the graph event) a transform job could hold only one end of a pair and no edge
   * would be derived, so the datasets go to the pipeline's run instead, as with transform jobs off.
   *
   * @param pipelineRun the pipeline run's own observations; receives the datasets without a graph
   * @return the transform events, to be sent before the pipeline's own terminal event
   */
  private List<RunEvent> finishPipelineRun(PipelineRunState state, PendingRun pipelineRun) {
    List<RunEvent> events = new ArrayList<>();
    boolean propagate = state.graph != null;
    Set<String> finished = new LinkedHashSet<>();
    for (LineageEvent terminal : state.deferredTerminals) {
      LineageContext ctx = terminal.getContext();
      String transform = ctx.getTransformName();
      finished.add(transform);
      endedRuns.add(runKey(resolveJobName(ctx), resolveRunId(ctx, terminal.getEventId())));
      PendingRun own = state.transformIo.get(transform);
      if (propagate) {
        List<InputDataset> ownInputs = own == null ? List.of() : own.getInputs();
        events.add(
            runEvent(
                terminal,
                eventTypeOf(terminal),
                propagateInputs ? withUpstreamInputs(ownInputs, transform, state) : ownInputs,
                own == null ? List.of() : own.getOutputs(),
                own == null ? null : own.getSoleSqlText(),
                JobIdentity.of(ctx)
                    .with(UPSTREAM_TRANSFORMS, previousTransforms(state, transform))));
      } else {
        events.add(runEvent(terminal, eventTypeOf(terminal), List.of(), List.of(), null));
      }
    }
    for (Map.Entry<String, PendingRun> entry : state.transformIo.entrySet()) {
      PendingRun own = entry.getValue();
      if (!propagate) {
        pipelineRun.absorb(own);
      } else if (!finished.contains(entry.getKey()) && own.hasDatasets()) {
        // A transform that touched datasets but never reported its end.
        events.add(
            standaloneEvent(
                own,
                propagateInputs
                    ? withUpstreamInputs(own.getInputs(), entry.getKey(), state)
                    : own.getInputs(),
                System.currentTimeMillis()));
      }
    }
    return events;
  }

  /**
   * The datasets a transform's job consumes: its own reads plus everything its upstream transforms
   * produced. That is what makes a dataset pair true — without it the job holds either the input or
   * the output of an edge, never both.
   */
  private List<InputDataset> withUpstreamInputs(
      List<InputDataset> own, String transformName, PipelineRunState state) {
    List<InputDataset> combined = new ArrayList<>(own);
    Set<DatasetRef> seen = new LinkedHashSet<>();
    for (InputDataset input : own) {
      seen.add(new DatasetRef(input.getNamespace(), input.getName()));
    }
    for (String upstream : previousTransforms(state, transformName)) {
      for (DatasetRef ref : produced(state, upstream, new LinkedHashSet<>())) {
        if (seen.add(ref)) {
          combined.add(
              ol.newInputDatasetBuilder()
                  .namespace(ref.namespace())
                  .name(ref.name())
                  .facets(ol.newDatasetFacetsBuilder().build())
                  .build());
        }
      }
    }
    return combined;
  }

  /**
   * What a transform hands to its downstream transforms, in terms of datasets: what it wrote if it
   * wrote anything (the stream carries the written content), otherwise what it read (a reader's
   * product is the data it read), otherwise — a transform that does no I/O of its own, a filter or
   * a select — whatever its own upstream transforms produced. That last case is what carries
   * lineage through pass-through transforms.
   */
  private List<DatasetRef> produced(
      PipelineRunState state, String transformName, Set<String> visited) {
    if (Utils.isEmpty(transformName) || !visited.add(transformName)) {
      return List.of();
    }
    PendingRun io = state.transformIo.get(transformName);
    if (io != null && !io.getOutputs().isEmpty()) {
      List<DatasetRef> produced = new ArrayList<>(io.getOutputs().size());
      for (OutputDataset output : io.getOutputs()) {
        produced.add(new DatasetRef(output.getNamespace(), output.getName()));
      }
      return produced;
    }
    if (io != null && !io.getInputs().isEmpty()) {
      List<DatasetRef> produced = new ArrayList<>(io.getInputs().size());
      for (InputDataset input : io.getInputs()) {
        produced.add(new DatasetRef(input.getNamespace(), input.getName()));
      }
      return produced;
    }
    List<DatasetRef> produced = new ArrayList<>();
    for (String upstream : previousTransforms(state, transformName)) {
      produced.addAll(produced(state, upstream, visited));
    }
    return produced;
  }

  /**
   * The transforms of a pipeline run that nothing in the run feeds: where its data enters. A
   * collector that links the job that started the pipeline to the pipeline's first jobs uses them.
   */
  private static List<String> rootTransforms(PipelineRunState state) {
    List<String> reported = new ArrayList<>();
    for (LineageEvent terminal : state.deferredTerminals) {
      reported.add(terminal.getContext().getTransformName());
    }
    return roots(state.graph, reported);
  }

  /** Every transform of a pipeline run: those in its hop graph and those that reported. */
  private static List<String> allTransforms(PipelineRunState state) {
    List<String> reported = new ArrayList<>();
    for (LineageEvent terminal : state.deferredTerminals) {
      reported.add(terminal.getContext().getTransformName());
    }
    return nodes(state.graph, reported);
  }

  /** The nodes of a hop graph, plus {@code others}, in order and once each. */
  private static List<String> nodes(List<Transition> graph, List<String> others) {
    Set<String> all = new LinkedHashSet<>();
    for (Transition transition : graph) {
      all.add(transition.getFrom());
      all.add(transition.getTo());
    }
    all.addAll(others);
    return new ArrayList<>(all);
  }

  /** The transforms that feed {@code transformName} in the run's hop graph. */
  private static List<String> previousTransforms(PipelineRunState state, String transformName) {
    return state.graph == null ? List.of() : leadingTo(state.graph, transformName);
  }

  /** The nodes of a hop graph (plus {@code others}) that no hop leads to. */
  private static List<String> roots(List<Transition> graph, List<String> others) {
    Set<String> led = new LinkedHashSet<>();
    Set<String> all = new LinkedHashSet<>();
    for (Transition transition : graph) {
      all.add(transition.getFrom());
      all.add(transition.getTo());
      led.add(transition.getTo());
    }
    all.addAll(others);
    all.removeAll(led);
    return new ArrayList<>(all);
  }

  /** The nodes a hop leads from to {@code name}. */
  private static List<String> leadingTo(List<Transition> graph, String name) {
    List<String> previous = new ArrayList<>();
    if (Utils.isEmpty(name)) {
      return previous;
    }
    for (Transition transition : graph) {
      if (name.equals(transition.getTo()) && !previous.contains(transition.getFrom())) {
        previous.add(transition.getFrom());
      }
    }
    return previous;
  }

  /** A dataset identity: what lineage edges are made of; facets are enrichment, not identity. */
  private record DatasetRef(String namespace, String name) {}

  /**
   * What a run pushed out of the bounded state still had, as standalone events. A run is only
   * pushed out when more than {@link #MAX_PENDING_RUNS} are in flight at once; its datasets are
   * sent rather than dropped.
   */
  private List<RunEvent> drainEvicted() {
    List<RunEvent> events = new ArrayList<>();
    PendingRun run;
    while ((run = evictedRuns.poll()) != null) {
      if (run.hasDatasets()) {
        events.add(standaloneEvent(run, run.getInputs(), System.currentTimeMillis()));
      }
    }
    PipelineRunState state;
    while ((state = evictedPipelineRuns.poll()) != null) {
      events.addAll(abandon(state));
    }
    return events;
  }

  /** Everything a pipeline run that never reported its end collected, as events. */
  private List<RunEvent> abandon(PipelineRunState state) {
    PendingRun pipelineRun = new PendingRun(state.jobName, state.runId, state.identity);
    List<RunEvent> events = finishPipelineRun(state, pipelineRun);
    if (pipelineRun.hasDatasets()) {
      events.add(standaloneEvent(pipelineRun, pipelineRun.getInputs(), System.currentTimeMillis()));
    }
    return events;
  }

  /**
   * The observations still waiting for a run to end, as events, clearing them. The sink calls this
   * when it shuts down: a run that never reported its end - an execution that crashed, or one that
   * ran where the engine had no chance to say so - leaves nothing to carry its datasets, and
   * dropping them would lose lineage.
   */
  public List<RunEvent> flushObservations() {
    List<RunEvent> events = new ArrayList<>();
    List<PipelineRunState> states;
    synchronized (pipelineRuns) {
      states = new ArrayList<>(pipelineRuns.values());
      pipelineRuns.clear();
    }
    for (PipelineRunState state : states) {
      events.addAll(abandon(state));
    }
    synchronized (pendingRuns) {
      for (PendingRun run : pendingRuns.values()) {
        if (run.hasDatasets()) {
          events.add(standaloneEvent(run, run.getInputs(), System.currentTimeMillis()));
        }
      }
      pendingRuns.clear();
    }
    events.addAll(drainEvicted());
    return events;
  }

  /**
   * A dataset observation as its own event, for the cases where no run event is left to carry it.
   */
  private RunEvent standaloneEvent(
      PendingRun run, List<InputDataset> inputs, long timestampMillis) {
    ZonedDateTime eventTime = toUtc(timestampMillis);
    List<OutputDataset> outputs = run.getOutputs();
    return ol.newRunEventBuilder()
        .eventType(EventType.OTHER)
        .eventTime(eventTime)
        .run(runNode(run.getRunId(), eventTime, null))
        .job(job(run.getJobName(), run.getIdentity(), run.getSoleSqlText()))
        .inputs(inputs.isEmpty() ? null : inputs)
        .outputs(outputs.isEmpty() ? null : outputs)
        .build();
  }

  /**
   * Identity of the run an observation belongs to, as the pair of names a collector sees for it:
   * the job the sink names for the observation and the run id it uses for it. File and relational
   * I/O is correlated to the pipeline run it happened in (see {@link #resolveFileIoRunId}), so a
   * pipeline carries its transforms' datasets.
   */
  private static String runKey(String jobName, String runId) {
    return jobName + '\u0000' + runId;
  }

  /** What a job is in Hop terms; see {@link #hopJobFacet}. */
  record JobIdentity(
      LineageSubjectType subjectType,
      String pipelineName,
      String workflowName,
      String transformName,
      String actionName,
      String filename,
      String description,
      Map<String, Object> extra) {

    /** The engine attributes that name who started a pipeline or workflow, copied to the facet. */
    private static final List<String> PARENT_ATTRIBUTES =
        List.of(
            LineageRunLifecycleEmitter.ATTR_PARENT_WORKFLOW_NAME,
            LineageRunLifecycleEmitter.ATTR_PARENT_ACTION_NAME,
            LineageRunLifecycleEmitter.ATTR_PARENT_PIPELINE_NAME,
            LineageRunLifecycleEmitter.ATTR_PARENT_TRANSFORM_NAME);

    /** The subject the context describes. */
    static JobIdentity of(LineageContext ctx) {
      if (ctx == null) {
        return new JobIdentity(null, null, null, null, null, null, null, Map.of());
      }
      Map<String, Object> extra = new LinkedHashMap<>();
      if (ctx.getSubjectType() == LineageSubjectType.PIPELINE
          || ctx.getSubjectType() == LineageSubjectType.WORKFLOW) {
        for (String attribute : PARENT_ATTRIBUTES) {
          String value = ctx.getAttributes().get(attribute);
          if (!Utils.isEmpty(value)) {
            extra.put(attribute, value);
          }
        }
      }
      return new JobIdentity(
          ctx.getSubjectType(),
          ctx.getPipelineName(),
          ctx.getWorkflowName(),
          ctx.getSubjectType() == LineageSubjectType.TRANSFORM ? ctx.getTransformName() : null,
          ctx.getSubjectType() == LineageSubjectType.ACTION ? ctx.getActionName() : null,
          filenameOf(ctx),
          ctx.getAttributes().get(LineageRunLifecycleEmitter.ATTR_DESCRIPTION),
          Collections.unmodifiableMap(extra));
    }

    /**
     * The job a transform's I/O is filed under in the pipeline-granular mapping: its pipeline (see
     * {@link #resolveFileIoJobName}).
     */
    static JobIdentity ioOwner(LineageContext ctx) {
      if (ctx != null && !Utils.isEmpty(ctx.getPipelineName())) {
        return new JobIdentity(
            LineageSubjectType.PIPELINE,
            ctx.getPipelineName(),
            null,
            null,
            null,
            filenameOf(ctx),
            null,
            Map.of());
      }
      return of(ctx);
    }

    /** This identity with one more facet property. */
    JobIdentity with(String key, Object value) {
      Map<String, Object> more = new LinkedHashMap<>(extra);
      more.put(key, value);
      return new JobIdentity(
          subjectType,
          pipelineName,
          workflowName,
          transformName,
          actionName,
          filename,
          description,
          Collections.unmodifiableMap(more));
    }

    private static String filenameOf(LineageContext ctx) {
      return !Utils.isEmpty(ctx.getHopFilenamePortableKey())
          ? ctx.getHopFilenamePortableKey()
          : ctx.getHopFilename();
    }
  }

  /**
   * What a running pipeline run has collected with transform jobs on; see {@link #pipelineRuns}.
   */
  private static final class PipelineRunState {
    private final String runId;
    private final String jobName;
    private final JobIdentity identity;
    private List<Transition> graph;
    private final Map<String, PendingRun> transformIo = new LinkedHashMap<>();
    private final List<LineageEvent> deferredTerminals = new ArrayList<>();

    PipelineRunState(String runId, String jobName, JobIdentity identity) {
      this.runId = runId;
      this.jobName = jobName;
      this.identity = identity;
    }
  }

  /**
   * Datasets observed for one run, held until that run reports its terminal event. Each dataset is
   * kept once: a run that reads the same table per row, or writes the same file in a loop, holds
   * one entry for it rather than one per observation. A later observation of a dataset replaces the
   * earlier one in place, as a collector would when it upserts the event's datasets in order.
   */
  private static final class PendingRun {
    private final String jobName;
    private final String runId;
    private final JobIdentity identity;
    private final Map<DatasetRef, InputDataset> inputs = new LinkedHashMap<>();
    private final Map<DatasetRef, OutputDataset> outputs = new LinkedHashMap<>();
    private final Set<String> sqlTexts = new LinkedHashSet<>();

    PendingRun(String jobName, String runId, JobIdentity identity) {
      this.jobName = jobName;
      this.runId = runId;
      this.identity = identity;
    }

    String getJobName() {
      return jobName;
    }

    String getRunId() {
      return runId;
    }

    JobIdentity getIdentity() {
      return identity;
    }

    List<InputDataset> getInputs() {
      return new ArrayList<>(inputs.values());
    }

    List<OutputDataset> getOutputs() {
      return new ArrayList<>(outputs.values());
    }

    /**
     * The single statement a run executed, or {@code null} when it ran none or several: with
     * several there is no one statement to claim the job ran.
     */
    String getSoleSqlText() {
      return sqlTexts.size() == 1 ? sqlTexts.iterator().next() : null;
    }

    void add(List<InputDataset> newInputs, List<OutputDataset> newOutputs, String sqlText) {
      for (InputDataset input : newInputs) {
        inputs.put(new DatasetRef(input.getNamespace(), input.getName()), input);
      }
      for (OutputDataset output : newOutputs) {
        outputs.put(new DatasetRef(output.getNamespace(), output.getName()), output);
      }
      if (!Utils.isEmpty(sqlText)) {
        sqlTexts.add(sqlText);
      }
    }

    /** Takes over another run's observations (a transform's, folded into its pipeline). */
    void absorb(PendingRun other) {
      inputs.putAll(other.inputs);
      outputs.putAll(other.outputs);
      sqlTexts.addAll(other.sqlTexts);
    }

    boolean hasDatasets() {
      return !inputs.isEmpty() || !outputs.isEmpty();
    }
  }

  /**
   * Least-recently-used map, oldest entry evicted once {@code maxEntries} is exceeded and handed to
   * {@code onEvict} (when given). Eviction by age, like {@link RelationalColumnLineageCorrelator},
   * does not depend on the order events happen to arrive in.
   */
  private static final class BoundedMap<K, V> extends LinkedHashMap<K, V> {
    private final int maxEntries;
    private final transient Consumer<V> onEvict;

    BoundedMap(int maxEntries, Consumer<V> onEvict) {
      super(16, 0.75f, true);
      this.maxEntries = maxEntries;
      this.onEvict = onEvict;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      boolean evict = size() > maxEntries;
      if (evict && onEvict != null) {
        onEvict.accept(eldest.getValue());
      }
      return evict;
    }
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
        LineageFileIdentity.Identity previous = null;
        if (!Utils.isEmpty(sourceUri)) {
          inputs.add(inputDataset(sourceUri, schema));
          previous = fileDatasetId(sourceUri);
        }
        if (!Utils.isEmpty(targetUri)) {
          outputs.add(outputDataset(targetUri, schema, op, previous));
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
        .facets(fileDatasetFacets(uri, contentSchema, null, null, null))
        .build();
  }

  private OutputDataset outputDataset(
      String uri, FileIoContentSchema contentSchema, FileIoOperation operation) {
    return outputDataset(uri, contentSchema, operation, null);
  }

  private OutputDataset outputDataset(
      String uri,
      FileIoContentSchema contentSchema,
      FileIoOperation operation,
      LineageFileIdentity.Identity previous) {
    LineageFileIdentity.Identity id = fileDatasetId(uri);
    // The content came from another dataset: a copy, or a move (a rename). A move onto the same
    // identity is a rewrite.
    boolean fromElsewhere = previous != null && !previous.equals(id);
    return ol.newOutputDatasetBuilder()
        .namespace(id.namespace())
        .name(id.name())
        .facets(
            fileDatasetFacets(
                uri,
                contentSchema,
                operation == FileIoOperation.MOVE && !fromElsewhere
                    ? OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE
                    : fileLifecycle(operation),
                fromElsewhere ? previous : null,
                fromElsewhere ? previous : null))
        .build();
  }

  /**
   * Lifecycle state of a written file dataset. File outputs replace their target, so a write or a
   * copy is {@code OVERWRITE}; a move renames the source, so its target is {@code RENAME} (the
   * source travels as its previous identifier); a delete is {@code DROP}. Append-mode outputs are
   * not distinguished yet — that would need the mode carried on the event. Reads have no state.
   */
  private OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange fileLifecycle(
      FileIoOperation operation) {
    if (operation == null) {
      return null;
    }
    return switch (operation) {
      case WRITE, COPY ->
          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
      case MOVE -> OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.RENAME;
      case DELETE -> OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP;
      default -> null;
    };
  }

  /**
   * The OpenLineage {@code (namespace, name)} for a file/object-store URI. With spec naming this
   * follows the OpenLineage naming spec (VFS scheme → namespace, path → name via {@link
   * LineageFileIdentity}) so a file reconciles across transforms and engines; otherwise, and for a
   * URI that cannot be parsed, it is the legacy job namespace + file name.
   */
  private LineageFileIdentity.Identity fileDatasetId(String uri) {
    LineageFileIdentity.Identity id = fileSpecNaming ? LineageFileIdentity.of(uri) : null;
    return id != null ? id : new LineageFileIdentity.Identity(namespace, nameFromUri(uri));
  }

  private DatasetFacets fileDatasetFacets(
      String uri,
      FileIoContentSchema contentSchema,
      OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange lifecycle,
      LineageFileIdentity.Identity previous,
      LineageFileIdentity.Identity copiedFrom) {
    OpenLineage.DatasetFacetsBuilder facets = ol.newDatasetFacetsBuilder();
    if (copiedFrom != null) {
      // Which file this one is a copy of. A run that copies several files lists them all as its
      // inputs and outputs, which pairs every source with every copy; this keeps the real pairs.
      OpenLineage.DefaultDatasetFacet source = new OpenLineage.DefaultDatasetFacet(producer, null);
      source.getAdditionalProperties().put("namespace", copiedFrom.namespace());
      source.getAdditionalProperties().put("name", copiedFrom.name());
      facets.put(COPIED_FROM_FACET, source);
    }
    URI parsedUri = toUriOrNull(uri);
    if (parsedUri != null) {
      facets.dataSource(ol.newDatasourceDatasetFacet(nameFromUri(uri), parsedUri));
    }
    List<SchemaDatasetFacetFields> fields = schemaFields(contentSchema);
    if (!fields.isEmpty()) {
      facets.schema(ol.newSchemaDatasetFacet(fields));
    }
    if (lifecycle != null) {
      OpenLineage.LifecycleStateChangeDatasetFacetPreviousIdentifier previousIdentifier =
          lifecycle == OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.RENAME
                  && previous != null
              ? ol.newLifecycleStateChangeDatasetFacetPreviousIdentifierBuilder()
                  .namespace(previous.namespace())
                  .name(previous.name())
                  .build()
              : null;
      facets.lifecycleStateChange(
          ol.newLifecycleStateChangeDatasetFacet(lifecycle, previousIdentifier));
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
      String type = OpenLineageFieldTypes.of(column.getHopTypeName());
      if (type != null) {
        field.type(type);
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

    // A read's row shape is the query's result, not the table: aliases, expressions and a subset of
    // the columns. Declaring it as the source table's schema would overwrite the real one in a
    // collector, so a source only declares the columns the statement provably reads from it.
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
                      datasetNamespace,
                      columnsReadFrom(name, columnEdges, defaultCatalog, defaultSchema),
                      null,
                      null))
              .build());
    }

    if (inputs.isEmpty() && outputs.isEmpty()) {
      // Read events carry only SQL until a sink parses it into tables — nothing to observe yet.
      return Optional.empty();
    }
    return observe(event, inputs, outputs, relational.getSqlText());
  }

  /**
   * Job for a run an observation is correlated to - the pipeline (or workflow) the I/O happened in,
   * named the way that run's own events name it - with the statement it ran when a single one is
   * known (Table Input, Execute SQL).
   */
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

  /**
   * The columns of table {@code datasetName} that the parsed statement reads, by name only: the
   * statement shows which columns it reads, not their types. {@code null} when it names none.
   */
  private static FileIoContentSchema columnsReadFrom(
      String datasetName,
      List<RelationalSqlParser.ColumnEdge> edges,
      String defaultCatalog,
      String defaultSchema) {
    List<FileIoTabularColumn> columns = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();
    for (RelationalSqlParser.ColumnEdge edge : edges) {
      for (RelationalSqlParser.FieldRef ref : edge.inputs()) {
        if (ref.table() == null || Utils.isEmpty(ref.field())) {
          continue;
        }
        RelationalTable table =
            LineageRelationalIdentity.withDefaults(ref.table(), defaultCatalog, defaultSchema);
        if (datasetName.equals(LineageRelationalIdentity.datasetName(table))
            && seen.add(ref.field())) {
          columns.add(new FileIoTabularColumn(ref.field(), null, 0, 0, null, null, false));
        }
      }
    }
    return columns.isEmpty() ? null : new FileIoContentSchema("jdbc", columns, List.of());
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
        ol.newRunFacetsBuilder()
            .nominalTime(ol.newNominalTimeRunFacet(eventTime, null))
            .processing_engine(
                ol.newProcessingEngineRunFacetBuilder()
                    .name(PROCESSING_ENGINE_NAME)
                    .version(engineVersion)
                    .openlineageAdapterVersion(engineVersion)
                    .build());
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
  public static UUID toRunUuid(String runId) {
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

  /**
   * The run id an event maps to: the subject's log-channel id when Hop provides one, a stable
   * synthetic id otherwise.
   *
   * <p>Public because sinks that wrap this one have to derive the same run identity for the same
   * event; duplicating the rule is how the two drift apart.
   */
  public static String resolveRunId(LineageContext ctx, String eventId) {
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

  /**
   * The job name an event maps to ({@code pipeline}, {@code pipeline/transform}, workflow, action).
   *
   * <p>Public because it is also the label a DataHub user sees on a run: sinks that wrap this one
   * must not invent a second naming rule.
   */
  public static String resolveJobName(LineageContext ctx) {
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
    if (ctx.getSubjectType() == LineageSubjectType.ACTION) {
      // An action is a job inside its workflow, the same way a transform is a job inside its
      // pipeline. Without this an action run would be filed under the workflow name and every
      // action would collapse onto one job.
      if (!Utils.isEmpty(ctx.getWorkflowName()) && !Utils.isEmpty(ctx.getActionName())) {
        return ctx.getWorkflowName() + "/" + ctx.getActionName();
      }
      if (!Utils.isEmpty(ctx.getActionName())) {
        return ctx.getActionName();
      }
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
