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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.lineage.LineageRunLifecycleEmitter;
import org.apache.hop.lineage.openlineage.OpenLineageEventMapper;

/**
 * Turns the OpenLineage events the mapper produces into DataHub metadata change proposals.
 *
 * <p>The model is DataHub's own for an orchestrator:
 *
 * <ul>
 *   <li>a pipeline or workflow is a <em>DataFlow</em>;
 *   <li>a transform or action is a <em>DataJob</em> of that flow, with the datasets it read and
 *       wrote and, for a transform, the transforms that feed it as upstream jobs (the hop graph);
 *   <li>every run is a <em>DataProcessInstance</em> of its flow or job, named for humans ({@code
 *       nightly 2026-10-18T07:34Z}), with its parent run and its outcome.
 * </ul>
 *
 * <p>A pipeline started by an action (or by a Pipeline Executor transform) is linked to it through
 * its first transforms, which list that action as an upstream job, so the lineage graph runs from
 * the workflow into the pipeline.
 *
 * <p>Everything here is a pure function of the event: an event replayed from the spool by a later
 * process is translated exactly like a live one. What depends on the state of DataHub — whether a
 * dataset's schema may be written, whether a deprecation may be lifted — is returned as a decision
 * for {@link DataHubEmitter} to make.
 */
final class DataHubTranslator {
  private static final String CONST_STATUS = "status";
  private static final String CONST_DATASET = "dataset";
  private static final String CONST_DATA_JOB = "dataJob";
  private static final String CONST_DATA_JOB_INPUT_OUTPUT = "dataJobInputOutput";
  private static final String CONST_CUSTOM_PROPERTIES = "customProperties";
  private static final String CONST_DATA_FLOW = "dataFlow";
  private static final String CONST_VALUE = "value";
  private static final String CONST_DESTINATION_URN = "destinationUrn";
  private static final String CONST_DATA_PROCESS_INSTANCE = "dataProcessInstance";
  private static final String CONST_WORKFLOW = "WORKFLOW";
  private static final String CONST_PIPELINE = "Pipeline";
  private static final String CONST_WORKFLOW_2 = "Workflow";

  static final String HOP_PLATFORM_LOGO = "https://hop.apache.org/img/hop-logo.svg";

  /** Marks the schemas Hop writes, so a later write only replaces Hop's own. */
  static final String HOP_SCHEMA_NAME = "hop";

  /** Custom property naming the Hop file a flow or task comes from. */
  static final String FILENAME_PROPERTY = "filename";

  /** Marks the deprecations Hop writes, so a later write only lifts Hop's own. */
  static final String HOP_NOTE_PREFIX = "Hop: ";

  private static final double COLUMN_LINEAGE_CONFIDENCE = 0.5;
  private static final DateTimeFormatter RUN_NAME_TIME =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** One proposal: an aspect for an entity, as {@code UPSERT} or as a JSON {@code PATCH}. */
  record Mcp(
      String entityType, String entityUrn, String changeType, String aspectName, JsonNode value) {
    static Mcp upsert(String entityType, String urn, String aspectName, JsonNode value) {
      return new Mcp(entityType, urn, "UPSERT", aspectName, value);
    }

    static Mcp patch(String entityType, String urn, String aspectName, ArrayNode operations) {
      return new Mcp(entityType, urn, "PATCH", aspectName, operations);
    }
  }

  /** A dataset schema Hop knows; whether it is written depends on what DataHub already has. */
  record SchemaCandidate(String datasetUrn, Mcp schemaMetadata) {}

  /** A dataset an event deleted or moved, or wrote again after Hop had retired it. */
  record LifecycleChange(
      Kind kind, String datasetUrn, String replacementUrn, String note, long timeMs) {
    enum Kind {
      DEPRECATE,
      CLEAR
    }
  }

  /**
   * The tasks a flow is made of now, after a successful run: the emitter retires the tasks DataHub
   * still has for the same file that are not among them (a transform or action removed or renamed),
   * unless they ran after {@code timeMs}, which makes this check the older one.
   */
  record StaleCheck(String flowUrn, Set<String> currentJobUrns, String filename, long timeMs) {}

  /**
   * What one event comes down to.
   *
   * @param jobUrn the task the event is about, or {@code null} for a pipeline's or workflow's own
   * @param timeMs when the event happened
   */
  record Translation(
      List<Mcp> proposals,
      List<SchemaCandidate> schemas,
      List<LifecycleChange> lifecycle,
      List<StaleCheck> staleChecks,
      String jobUrn,
      long timeMs) {}

  private final String env;
  private final String cluster;
  private final String platformInstance;
  private final String actor;
  private final boolean lifecycleEnabled;

  DataHubTranslator(DataHubSinkConfig config) {
    this(
        config.getEnv(),
        config.getCluster(),
        config.getPlatformInstance(),
        config.getActor(),
        config.isLifecycleEnabled());
  }

  DataHubTranslator(
      String env, String cluster, String platformInstance, String actor, boolean lifecycleEnabled) {
    this.env = env;
    this.cluster = cluster;
    this.platformInstance = platformInstance;
    this.actor = actor;
    this.lifecycleEnabled = lifecycleEnabled;
  }

  /** The {@code hop} data platform, so Hop's pipelines show under their own name and logo. */
  static Mcp hopPlatform() {
    ObjectNode info = MAPPER.createObjectNode();
    info.put("name", DataHubUrns.HOP_PLATFORM);
    info.put("displayName", "Apache Hop");
    info.put("type", "OTHERS");
    info.put("datasetNameDelimiter", "/");
    info.put("logoUrl", HOP_PLATFORM_LOGO);
    return Mcp.upsert("dataPlatform", DataHubUrns.HOP_PLATFORM_URN, "dataPlatformInfo", info);
  }

  Translation translate(RunEvent event) {
    Subject subject = Subject.of(event);
    List<Mcp> proposals = new ArrayList<>();
    List<SchemaCandidate> schemas = new ArrayList<>();
    List<LifecycleChange> lifecycle = new ArrayList<>();
    List<StaleCheck> staleChecks = new ArrayList<>();
    long time = eventTime(event);
    if (subject == null) {
      return new Translation(proposals, schemas, lifecycle, staleChecks, null, time);
    }
    String flowUrn = flowUrn(subject.flowFilename(), subject.flowName());
    String jobUrn =
        subject.jobName() == null ? null : DataHubUrns.dataJob(flowUrn, subject.jobName());
    EventType type = event.getEventType();

    if (type == EventType.START) {
      proposals.addAll(flowAspects(flowUrn, subject, jobUrn == null ? description(event) : null));
      if (jobUrn != null) {
        proposals.addAll(jobAspects(jobUrn, flowUrn, subject, description(event)));
      }
      proposals.addAll(runStarted(event, subject, jobUrn != null ? jobUrn : flowUrn, time));
      return new Translation(proposals, schemas, lifecycle, staleChecks, jobUrn, time);
    }
    if (type == EventType.ABORT) {
      // A run the engine never reported the end of, ended by the sink when it shut down: there is
      // nothing to say about its task or datasets, only that the run is over.
      proposals.addAll(runEnded(event, List.of(), List.of(), time));
      return new Translation(proposals, schemas, lifecycle, staleChecks, jobUrn, time);
    }

    List<String> inputs = datasetUrns(event.getInputs());
    List<String> outputs = datasetUrns(event.getOutputs());
    // A copied or moved file is linked to its source directly. Through the job, a run that copied
    // several files would pair every source with every copy; the run itself still lists them all.
    Map<String, String> copies = copies(event);
    List<String> jobInputs = new ArrayList<>(inputs);
    jobInputs.removeAll(copies.values());
    List<String> jobOutputs = new ArrayList<>(outputs);
    jobOutputs.removeAll(copies.keySet());
    // A pipeline that carried datasets itself (an engine without the hop graph) needs a job to hold
    // them: DataHub lineage runs through jobs, not through flows.
    String ioJobUrn = jobUrn;
    if (ioJobUrn == null && (!jobInputs.isEmpty() || !jobOutputs.isEmpty())) {
      ioJobUrn = DataHubUrns.dataJob(flowUrn, subject.flowName());
      proposals.addAll(flowAspects(flowUrn, subject, description(event)));
      proposals.addAll(jobAspects(ioJobUrn, flowUrn, subject, null));
    }
    for (String dataset : distinct(inputs, outputs)) {
      proposals.add(Mcp.upsert(CONST_DATASET, dataset, CONST_STATUS, status()));
    }
    proposals.addAll(localFileNames(event));
    schemas.addAll(schemaCandidates(event));
    if (lifecycleEnabled) {
      lifecycle.addAll(lifecycleChanges(event, time));
    }
    List<FieldEdge> fieldEdges = fieldEdges(event, event.getJob().getName());
    proposals.addAll(datasetColumnLineage(fieldEdges));
    proposals.addAll(copyLineage(copies, time));

    if (type == EventType.OTHER) {
      // Datasets seen after their run reported its end: add them, do not replace what the job has.
      if (ioJobUrn != null) {
        proposals.add(addDatasetEdges(ioJobUrn, jobInputs, jobOutputs));
      }
      return new Translation(proposals, schemas, lifecycle, staleChecks, jobUrn, time);
    }

    if (ioJobUrn != null) {
      proposals.add(
          Mcp.upsert(
              CONST_DATA_JOB,
              ioJobUrn,
              CONST_DATA_JOB_INPUT_OUTPUT,
              jobInputOutput(jobInputs, jobOutputs, upstreamJobs(subject, flowUrn), fieldEdges)));
    }
    proposals.addAll(runEnded(event, inputs, outputs, time));
    proposals.addAll(linkToParentJob(subject, flowUrn));
    // Only a run that went through describes the flow completely; a failed one may have stopped
    // before some of its transforms reported.
    if (type == EventType.COMPLETE
        && jobUrn == null
        && !subject.members().isEmpty()
        && subject.flowFilename() != null) {
      Set<String> current = new LinkedHashSet<>();
      for (String member : subject.members()) {
        current.add(DataHubUrns.dataJob(flowUrn, member));
      }
      staleChecks.add(new StaleCheck(flowUrn, current, subject.flowFilename(), time));
    }
    return new Translation(proposals, schemas, lifecycle, staleChecks, jobUrn, time);
  }

  /** The flow of a pipeline or workflow, by its file (see {@link DataHubUrns#flowId}). */
  private String flowUrn(String filename, String name) {
    return DataHubUrns.dataFlow(platformInstance, DataHubUrns.flowId(filename, name), cluster);
  }

  // ---------------------------------------------------------------------------------------------
  // Flows and jobs
  // ---------------------------------------------------------------------------------------------

  /**
   * The flow's aspects. Its info (name, description, file) comes only from the flow's own events:
   * written from a transform's event as well, it would replace the pipeline's description with
   * none.
   */
  private List<Mcp> flowAspects(String flowUrn, Subject subject, String description) {
    List<Mcp> proposals = new ArrayList<>();
    if (subject.jobName() == null) {
      ObjectNode info = MAPPER.createObjectNode();
      info.put("name", subject.flowName());
      if (description != null) {
        info.put("description", description);
      }
      ObjectNode properties = info.putObject(CONST_CUSTOM_PROPERTIES);
      if (subject.flowFilename() != null) {
        properties.put(FILENAME_PROPERTY, subject.flowFilename());
      }
      proposals.add(Mcp.upsert(CONST_DATA_FLOW, flowUrn, "dataFlowInfo", info));
    }
    proposals.add(Mcp.upsert(CONST_DATA_FLOW, flowUrn, "subTypes", subTypes(subject.flowType())));
    proposals.add(Mcp.upsert(CONST_DATA_FLOW, flowUrn, CONST_STATUS, status()));
    if (!StringUtils.isBlank(platformInstance)) {
      proposals.add(
          Mcp.upsert(CONST_DATA_FLOW, flowUrn, "dataPlatformInstance", platformInstance()));
    }
    return proposals;
  }

  private List<Mcp> jobAspects(String jobUrn, String flowUrn, Subject subject, String description) {
    List<Mcp> proposals = new ArrayList<>();
    ObjectNode info = MAPPER.createObjectNode();
    info.put("name", subject.jobName() != null ? subject.jobName() : subject.flowName());
    if (description != null) {
      info.put("description", description);
    }
    info.putObject("type").put("string", subject.jobType().toUpperCase(Locale.ROOT));
    info.put("flowUrn", flowUrn);
    ObjectNode properties = info.putObject(CONST_CUSTOM_PROPERTIES);
    if (subject.flowFilename() != null) {
      // Which file the task belongs to: only that file's runs may retire it (see StaleCheck).
      properties.put(FILENAME_PROPERTY, subject.flowFilename());
    }
    proposals.add(Mcp.upsert(CONST_DATA_JOB, jobUrn, "dataJobInfo", info));
    proposals.add(Mcp.upsert(CONST_DATA_JOB, jobUrn, "subTypes", subTypes(subject.jobType())));
    proposals.add(Mcp.upsert(CONST_DATA_JOB, jobUrn, CONST_STATUS, status()));
    if (!StringUtils.isBlank(platformInstance)) {
      proposals.add(Mcp.upsert(CONST_DATA_JOB, jobUrn, "dataPlatformInstance", platformInstance()));
    }
    return proposals;
  }

  /** The description the Hop metadata gives the event's job, from the documentation facet. */
  private static String description(RunEvent event) {
    OpenLineage.JobFacets facets = event.getJob().getFacets();
    return facets == null
            || facets.getDocumentation() == null
            || StringUtils.isBlank(facets.getDocumentation().getDescription())
        ? null
        : facets.getDocumentation().getDescription();
  }

  /**
   * The jobs of the same flow that lead to this one: the transforms that feed a transform, the
   * actions a workflow hop leads from to an action.
   */
  private static List<String> upstreamJobs(Subject subject, String flowUrn) {
    List<String> jobs = new ArrayList<>();
    for (String upstream : subject.upstreamJobs()) {
      jobs.add(DataHubUrns.dataJob(flowUrn, upstream));
    }
    return jobs;
  }

  /**
   * Links a pipeline or workflow run by an action, or by a transform of another pipeline, to that
   * job: its first transforms or actions list the job as upstream. A {@code PATCH}, because each of
   * those transforms wrote its own job I/O a moment earlier and this only adds to it.
   */
  private List<Mcp> linkToParentJob(Subject subject, String flowUrn) {
    String parentJob = parentJobUrn(subject);
    if (parentJob == null || subject.rootJobs().isEmpty()) {
      return List.of();
    }
    List<Mcp> proposals = new ArrayList<>();
    for (String root : subject.rootJobs()) {
      ArrayNode patch = MAPPER.createArrayNode();
      ObjectNode add = patch.addObject();
      add.put("op", "add");
      add.put("path", "/inputDatajobEdges/" + escapeJsonPointer(parentJob));
      add.putObject(CONST_VALUE).put(CONST_DESTINATION_URN, parentJob);
      proposals.add(
          Mcp.patch(
              CONST_DATA_JOB,
              DataHubUrns.dataJob(flowUrn, root),
              CONST_DATA_JOB_INPUT_OUTPUT,
              patch));
    }
    return proposals;
  }

  private String parentJobUrn(Subject subject) {
    String filename = subject.facet(LineageRunLifecycleEmitter.ATTR_PARENT_FILENAME);
    String workflow = subject.facet(LineageRunLifecycleEmitter.ATTR_PARENT_WORKFLOW_NAME);
    String action = subject.facet(LineageRunLifecycleEmitter.ATTR_PARENT_ACTION_NAME);
    if (workflow != null && action != null) {
      return DataHubUrns.dataJob(flowUrn(filename, workflow), action);
    }
    String pipeline = subject.facet(LineageRunLifecycleEmitter.ATTR_PARENT_PIPELINE_NAME);
    String transform = subject.facet(LineageRunLifecycleEmitter.ATTR_PARENT_TRANSFORM_NAME);
    if (pipeline != null && transform != null) {
      return DataHubUrns.dataJob(flowUrn(filename, pipeline), transform);
    }
    return null;
  }

  private ObjectNode jobInputOutput(
      List<String> inputs,
      List<String> outputs,
      List<String> upstreamJobs,
      List<FieldEdge> fieldEdges) {
    ObjectNode io = MAPPER.createObjectNode();
    // The deprecated plain lists are still required fields of the aspect.
    io.set("inputDatasets", urnArray(inputs));
    io.set("outputDatasets", urnArray(outputs));
    io.set("inputDatasetEdges", edges(inputs));
    io.set("outputDatasetEdges", edges(outputs));
    io.set("inputDatajobs", urnArray(upstreamJobs));
    io.set("inputDatajobEdges", edges(upstreamJobs));
    ArrayNode fineGrained = io.putArray("fineGrainedLineages");
    for (FieldEdge edge : fieldEdges) {
      ObjectNode lineage = fineGrained.addObject();
      lineage.put("upstreamType", "FIELD_SET");
      lineage.putArray("upstreams").add(edge.upstreamFieldUrn());
      lineage.put("downstreamType", "FIELD");
      lineage.putArray("downstreams").add(edge.downstreamFieldUrn());
      lineage.put("confidenceScore", COLUMN_LINEAGE_CONFIDENCE);
    }
    return io;
  }

  private static Mcp addDatasetEdges(String jobUrn, List<String> inputs, List<String> outputs) {
    ArrayNode patch = MAPPER.createArrayNode();
    for (String input : inputs) {
      ObjectNode add = patch.addObject();
      add.put("op", "add");
      add.put("path", "/inputDatasetEdges/" + escapeJsonPointer(input));
      add.putObject(CONST_VALUE).put(CONST_DESTINATION_URN, input);
    }
    for (String output : outputs) {
      ObjectNode add = patch.addObject();
      add.put("op", "add");
      add.put("path", "/outputDatasetEdges/" + escapeJsonPointer(output));
      add.putObject(CONST_VALUE).put(CONST_DESTINATION_URN, output);
    }
    return Mcp.patch(CONST_DATA_JOB, jobUrn, CONST_DATA_JOB_INPUT_OUTPUT, patch);
  }

  /** Copy and move targets with their source, from the mapper's copied-from facet. */
  private Map<String, String> copies(RunEvent event) {
    Map<String, String> copies = new LinkedHashMap<>();
    if (event.getOutputs() == null) {
      return copies;
    }
    for (OpenLineage.OutputDataset output : event.getOutputs()) {
      if (output == null
          || output.getFacets() == null
          || output.getFacets().getAdditionalProperties() == null) {
        continue;
      }
      OpenLineage.DatasetFacet source =
          output
              .getFacets()
              .getAdditionalProperties()
              .get(OpenLineageEventMapper.COPIED_FROM_FACET);
      if (source == null || source.getAdditionalProperties() == null) {
        continue;
      }
      Object namespace = source.getAdditionalProperties().get("namespace");
      Object name = source.getAdditionalProperties().get("name");
      String target = datasetUrn(output);
      String sourceUrn =
          namespace instanceof String ns && name instanceof String n
              ? DataHubUrns.datasetForOpenLineage(ns, n, env)
              : null;
      if (target != null && sourceUrn != null && !sourceUrn.equals(target)) {
        copies.put(target, sourceUrn);
      }
    }
    return copies;
  }

  /** Each copy as a {@code COPY} upstream of its target: additive, like DataHub's patch builder. */
  private List<Mcp> copyLineage(Map<String, String> copies, long time) {
    List<Mcp> proposals = new ArrayList<>();
    for (Map.Entry<String, String> copy : copies.entrySet()) {
      ArrayNode patch = MAPPER.createArrayNode();
      ObjectNode add = patch.addObject();
      add.put("op", "add");
      add.put("path", "/upstreams/" + escapeJsonPointer(copy.getValue()));
      ObjectNode upstream = add.putObject(CONST_VALUE);
      upstream.put(CONST_DATASET, copy.getValue());
      upstream.put("type", "COPY");
      ObjectNode stamp = upstream.putObject("auditStamp");
      stamp.put("time", time);
      stamp.put("actor", actor);
      proposals.add(Mcp.patch(CONST_DATASET, copy.getKey(), "upstreamLineage", patch));
    }
    return proposals;
  }

  // ---------------------------------------------------------------------------------------------
  // Runs
  // ---------------------------------------------------------------------------------------------

  private List<Mcp> runStarted(RunEvent event, Subject subject, String templateUrn, long time) {
    String runUrn = DataHubUrns.dataProcessInstance(event.getRun().getRunId());
    List<Mcp> proposals = new ArrayList<>();
    ObjectNode properties = MAPPER.createObjectNode();
    properties.put("name", runName(event.getJob().getName(), time));
    ObjectNode created = properties.putObject("created");
    created.put("time", time);
    created.put("actor", actor);
    properties
        .putObject(CONST_CUSTOM_PROPERTIES)
        .put("hopRunId", event.getRun().getRunId().toString());
    proposals.add(
        Mcp.upsert(
            CONST_DATA_PROCESS_INSTANCE, runUrn, "dataProcessInstanceProperties", properties));

    ObjectNode relationships = MAPPER.createObjectNode();
    relationships.put("parentTemplate", templateUrn);
    String parentRun = parentRunUrn(event);
    if (parentRun != null) {
      relationships.put("parentInstance", parentRun);
    }
    relationships.putArray("upstreamInstances");
    proposals.add(
        Mcp.upsert(
            CONST_DATA_PROCESS_INSTANCE,
            runUrn,
            "dataProcessInstanceRelationships",
            relationships));

    ObjectNode started = MAPPER.createObjectNode();
    started.put("timestampMillis", time);
    started.put(CONST_STATUS, "STARTED");
    started.put("attempt", 1);
    proposals.add(
        Mcp.upsert(CONST_DATA_PROCESS_INSTANCE, runUrn, "dataProcessInstanceRunEvent", started));
    return proposals;
  }

  private List<Mcp> runEnded(RunEvent event, List<String> inputs, List<String> outputs, long time) {
    String runUrn = DataHubUrns.dataProcessInstance(event.getRun().getRunId());
    List<Mcp> proposals = new ArrayList<>();
    ObjectNode ended = MAPPER.createObjectNode();
    ended.put("timestampMillis", time);
    ended.put(CONST_STATUS, "COMPLETE");
    ended.put("attempt", 1);
    ObjectNode result = ended.putObject("result");
    // Anything but a completion (a failure, a run the sink ended at shutdown) did not succeed.
    result.put("type", event.getEventType() == EventType.COMPLETE ? "SUCCESS" : "FAILURE");
    result.put("nativeResultType", DataHubUrns.HOP_PLATFORM);
    proposals.add(
        Mcp.upsert(CONST_DATA_PROCESS_INSTANCE, runUrn, "dataProcessInstanceRunEvent", ended));
    if (!inputs.isEmpty()) {
      ObjectNode in = MAPPER.createObjectNode();
      in.set("inputs", urnArray(inputs));
      proposals.add(
          Mcp.upsert(CONST_DATA_PROCESS_INSTANCE, runUrn, "dataProcessInstanceInput", in));
    }
    if (!outputs.isEmpty()) {
      ObjectNode out = MAPPER.createObjectNode();
      out.set("outputs", urnArray(outputs));
      proposals.add(
          Mcp.upsert(CONST_DATA_PROCESS_INSTANCE, runUrn, "dataProcessInstanceOutput", out));
    }
    return proposals;
  }

  private static String parentRunUrn(RunEvent event) {
    OpenLineage.RunFacets facets = event.getRun().getFacets();
    if (facets == null || facets.getParent() == null || facets.getParent().getRun() == null) {
      return null;
    }
    return DataHubUrns.dataProcessInstance(facets.getParent().getRun().getRunId());
  }

  /** {@code nightly/Read orders 2026-10-18T07:34Z}: what a person sees on a run, in UTC. */
  static String runName(String jobName, long time) {
    return jobName + " " + RUN_NAME_TIME.format(Instant.ofEpochMilli(time));
  }

  // ---------------------------------------------------------------------------------------------
  // Datasets
  // ---------------------------------------------------------------------------------------------

  private List<String> datasetUrns(List<? extends OpenLineage.Dataset> datasets) {
    List<String> urns = new ArrayList<>();
    if (datasets == null) {
      return urns;
    }
    Set<String> seen = new LinkedHashSet<>();
    for (OpenLineage.Dataset dataset : datasets) {
      String urn = datasetUrn(dataset);
      if (urn != null && seen.add(urn)) {
        urns.add(urn);
      }
    }
    return urns;
  }

  private String datasetUrn(OpenLineage.Dataset dataset) {
    return dataset == null
        ? null
        : DataHubUrns.datasetForOpenLineage(dataset.getNamespace(), dataset.getName(), env);
  }

  /**
   * A local file's name as its display name: its identity is the absolute path, which is long to
   * read in a list. Only for the {@code file} platform, which nothing but Hop describes; datasets
   * of other platforms keep the properties their own sources give them.
   */
  private List<Mcp> localFileNames(RunEvent event) {
    Map<String, Mcp> proposals = new LinkedHashMap<>();
    for (OpenLineage.Dataset dataset : union(event.getInputs(), event.getOutputs())) {
      if (dataset == null
          || !"file".equals(dataset.getNamespace())
          || StringUtils.isBlank(dataset.getName())) {
        continue;
      }
      String urn = datasetUrn(dataset);
      String path = dataset.getName();
      ObjectNode properties = MAPPER.createObjectNode();
      properties.put("name", path.substring(path.lastIndexOf('/') + 1));
      properties.put("qualifiedName", path);
      properties.putObject(CONST_CUSTOM_PROPERTIES);
      proposals.put(urn, Mcp.upsert(CONST_DATASET, urn, "datasetProperties", properties));
    }
    return new ArrayList<>(proposals.values());
  }

  /**
   * The schemas an event can offer: those of the datasets it wrote and of the files it read, on any
   * file system. A table read declares only the columns its query reads; as {@code schemaMetadata}
   * that subset would replace the table's full schema, so it is not offered.
   */
  private List<SchemaCandidate> schemaCandidates(RunEvent event) {
    Map<String, SchemaCandidate> candidates = new LinkedHashMap<>();
    if (event.getInputs() != null) {
      for (OpenLineage.InputDataset input : event.getInputs()) {
        addSchemaCandidate(candidates, input, true);
      }
    }
    // After the inputs, so what a transform wrote wins over what it read of the same dataset.
    if (event.getOutputs() != null) {
      for (OpenLineage.OutputDataset output : event.getOutputs()) {
        addSchemaCandidate(candidates, output, false);
      }
    }
    return new ArrayList<>(candidates.values());
  }

  private void addSchemaCandidate(
      Map<String, SchemaCandidate> candidates, OpenLineage.Dataset dataset, boolean read) {
    String urn = datasetUrn(dataset);
    if (urn == null
        || dataset.getFacets() == null
        || dataset.getFacets().getSchema() == null
        || dataset.getFacets().getSchema().getFields() == null
        || dataset.getFacets().getSchema().getFields().isEmpty()) {
      return;
    }
    if (read && isTable(dataset)) {
      return;
    }
    String platformUrn = urn.substring("urn:li:dataset:(".length(), urn.indexOf(','));
    candidates.put(
        urn,
        new SchemaCandidate(
            urn,
            Mcp.upsert(
                CONST_DATASET,
                urn,
                "schemaMetadata",
                schemaMetadata(platformUrn, dataset.getFacets().getSchema().getFields()))));
  }

  /**
   * Whether a dataset is a database table rather than a file. The mapper gives a table the database
   * it lives in as its data source ({@code dataSource.name} is the dataset's namespace, such as
   * {@code postgres://db:5432}) and a file the file itself, whatever its VFS scheme.
   */
  static boolean isTable(OpenLineage.Dataset dataset) {
    OpenLineage.DatasourceDatasetFacet source =
        dataset.getFacets() == null ? null : dataset.getFacets().getDataSource();
    return source != null
        && dataset.getNamespace() != null
        && dataset.getNamespace().equals(source.getName());
  }

  /**
   * The {@code schemaMetadata} aspect: each field typed with the DataHub scalar its OpenLineage
   * type stands for, the OpenLineage type kept as the native type, and {@value #HOP_SCHEMA_NAME} as
   * the schema name so a later run recognises the schema as Hop's own.
   */
  static ObjectNode schemaMetadata(
      String platformUrn, List<OpenLineage.SchemaDatasetFacetFields> fields) {
    ObjectNode schema = MAPPER.createObjectNode();
    schema.put("schemaName", HOP_SCHEMA_NAME);
    schema.put("platform", platformUrn);
    schema.put("version", 0);
    schema.put("hash", "");
    schema
        .putObject("platformSchema")
        .putObject("com.linkedin.schema.OtherSchema")
        .put("rawSchema", "");
    ArrayNode out = schema.putArray("fields");
    Set<String> seen = new LinkedHashSet<>();
    for (OpenLineage.SchemaDatasetFacetFields field : fields) {
      if (field == null || StringUtils.isBlank(field.getName()) || !seen.add(field.getName())) {
        continue;
      }
      ObjectNode node = out.addObject();
      node.put("fieldPath", field.getName());
      node.put("nativeDataType", field.getType() == null ? "" : field.getType());
      node.put("nullable", true);
      node.putObject("type").putObject("type").putObject(dataHubType(field.getType()));
    }
    return schema;
  }

  /** The DataHub schema type for an OpenLineage type name. */
  static String dataHubType(String openLineageType) {
    if (StringUtils.isBlank(openLineageType)) {
      return "com.linkedin.schema.NullType";
    }
    return switch (openLineageType.trim().toLowerCase(Locale.ROOT)) {
      case "string" -> "com.linkedin.schema.StringType";
      case "int", "long", "double", "float", "decimal", "number" ->
          "com.linkedin.schema.NumberType";
      case "boolean" -> "com.linkedin.schema.BooleanType";
      case "date" -> "com.linkedin.schema.DateType";
      case "timestamp", "time", "datetime" -> "com.linkedin.schema.TimeType";
      case "binary", "bytes" -> "com.linkedin.schema.BytesType";
      case "array" -> "com.linkedin.schema.ArrayType";
      case "struct", "map", "record", "json" -> "com.linkedin.schema.MapType";
      default -> "com.linkedin.schema.NullType";
    };
  }

  /** A column edge from an output's {@code columnLineage} facet. */
  record FieldEdge(
      String upstreamFieldUrn,
      String downstreamFieldUrn,
      String downstreamDatasetUrn,
      String operation) {}

  private List<FieldEdge> fieldEdges(RunEvent event, String operation) {
    List<FieldEdge> edges = new ArrayList<>();
    if (event.getOutputs() == null) {
      return edges;
    }
    for (OpenLineage.OutputDataset output : event.getOutputs()) {
      String downstreamDataset = datasetUrn(output);
      if (downstreamDataset == null
          || output.getFacets() == null
          || output.getFacets().getColumnLineage() == null
          || output.getFacets().getColumnLineage().getFields() == null) {
        continue;
      }
      Map<String, OpenLineage.ColumnLineageDatasetFacetFieldsAdditional> fields =
          output.getFacets().getColumnLineage().getFields().getAdditionalProperties();
      if (fields == null) {
        continue;
      }
      for (Map.Entry<String, OpenLineage.ColumnLineageDatasetFacetFieldsAdditional> field :
          fields.entrySet()) {
        if (field.getValue() == null || field.getValue().getInputFields() == null) {
          continue;
        }
        String downstream = DataHubUrns.schemaField(downstreamDataset, field.getKey());
        for (OpenLineage.InputField source : field.getValue().getInputFields()) {
          if (source == null || StringUtils.isBlank(source.getField())) {
            continue;
          }
          String upstreamDataset =
              DataHubUrns.datasetForOpenLineage(source.getNamespace(), source.getName(), env);
          if (upstreamDataset != null) {
            edges.add(
                new FieldEdge(
                    DataHubUrns.schemaField(upstreamDataset, source.getField()),
                    downstream,
                    downstreamDataset,
                    operation));
          }
        }
      }
    }
    return edges;
  }

  /**
   * Column lineage on the dataset itself, which is what the column view of a dataset page reads. A
   * {@code PATCH} in the shape of DataHub's {@code UpstreamLineagePatchBuilder}: additive, so edges
   * another tool wrote for the same dataset survive, and a rerun re-adds the same path.
   */
  private static List<Mcp> datasetColumnLineage(List<FieldEdge> edges) {
    Map<String, ArrayNode> patches = new LinkedHashMap<>();
    for (FieldEdge edge : edges) {
      ArrayNode patch =
          patches.computeIfAbsent(edge.downstreamDatasetUrn(), k -> MAPPER.createArrayNode());
      ObjectNode add = patch.addObject();
      add.put("op", "add");
      add.put(
          "path",
          "/fineGrainedLineages/"
              + escapeJsonPointer(edge.operation())
              + "/"
              + escapeJsonPointer(edge.downstreamFieldUrn())
              + "/NONE/"
              + escapeJsonPointer(edge.upstreamFieldUrn()));
      add.putObject(CONST_VALUE).put("confidenceScore", COLUMN_LINEAGE_CONFIDENCE);
    }
    List<Mcp> proposals = new ArrayList<>();
    for (Map.Entry<String, ArrayNode> patch : patches.entrySet()) {
      proposals.add(Mcp.patch(CONST_DATASET, patch.getKey(), "upstreamLineage", patch.getValue()));
    }
    return proposals;
  }

  private List<LifecycleChange> lifecycleChanges(RunEvent event, long time) {
    List<LifecycleChange> changes = new ArrayList<>();
    if (event.getOutputs() == null) {
      return changes;
    }
    String by = event.getJob().getName();
    for (OpenLineage.OutputDataset output : event.getOutputs()) {
      String urn = datasetUrn(output);
      if (urn == null || output.getFacets() == null) {
        continue;
      }
      OpenLineage.LifecycleStateChangeDatasetFacet facet =
          output.getFacets().getLifecycleStateChange();
      if (facet == null || facet.getLifecycleStateChange() == null) {
        continue;
      }
      switch (facet.getLifecycleStateChange()) {
        case DROP ->
            changes.add(
                new LifecycleChange(
                    LifecycleChange.Kind.DEPRECATE,
                    urn,
                    null,
                    HOP_NOTE_PREFIX + "deleted by " + by + ".",
                    time));
        case RENAME -> {
          changes.add(
              new LifecycleChange(LifecycleChange.Kind.CLEAR, urn, null, writtenAgain(by), time));
          OpenLineage.LifecycleStateChangeDatasetFacetPreviousIdentifier previous =
              facet.getPreviousIdentifier();
          String previousUrn =
              previous == null
                  ? null
                  : DataHubUrns.datasetForOpenLineage(
                      previous.getNamespace(), previous.getName(), env);
          if (previousUrn != null && !previousUrn.equals(urn)) {
            changes.add(
                new LifecycleChange(
                    LifecycleChange.Kind.DEPRECATE,
                    previousUrn,
                    urn,
                    HOP_NOTE_PREFIX + "moved to " + urn + " by " + by + ".",
                    time));
          }
        }
        default ->
            changes.add(
                new LifecycleChange(LifecycleChange.Kind.CLEAR, urn, null, writtenAgain(by), time));
      }
    }
    return changes;
  }

  private static String writtenAgain(String by) {
    return HOP_NOTE_PREFIX + "written again by " + by + ".";
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private ObjectNode platformInstance() {
    ObjectNode instance = MAPPER.createObjectNode();
    instance.put("platform", DataHubUrns.HOP_PLATFORM_URN);
    instance.put("instance", DataHubUrns.dataPlatformInstance(platformInstance));
    return instance;
  }

  private static ObjectNode status() {
    ObjectNode status = MAPPER.createObjectNode();
    status.put("removed", false);
    return status;
  }

  private static ObjectNode subTypes(String type) {
    ObjectNode subTypes = MAPPER.createObjectNode();
    subTypes.putArray("typeNames").add(type);
    return subTypes;
  }

  private static ArrayNode urnArray(List<String> urns) {
    ArrayNode array = MAPPER.createArrayNode();
    urns.forEach(array::add);
    return array;
  }

  /**
   * Edges without a creation stamp: the edges of a job are the same from one run to the next, and
   * so is the aspect, which lets the emitter skip writing it again.
   */
  private static ArrayNode edges(List<String> urns) {
    ArrayNode edges = MAPPER.createArrayNode();
    for (String urn : urns) {
      edges.addObject().put(CONST_DESTINATION_URN, urn);
    }
    return edges;
  }

  private static <T> List<T> union(List<? extends T> first, List<? extends T> second) {
    List<T> all = new ArrayList<>();
    if (first != null) {
      all.addAll(first);
    }
    if (second != null) {
      all.addAll(second);
    }
    return all;
  }

  private static Set<String> distinct(List<String> first, List<String> second) {
    Set<String> all = new LinkedHashSet<>(first);
    all.addAll(second);
    return all;
  }

  private static long eventTime(RunEvent event) {
    return event.getEventTime() != null
        ? event.getEventTime().toInstant().toEpochMilli()
        : System.currentTimeMillis();
  }

  static String escapeJsonPointer(String value) {
    return value.replace("~", "~0").replace("/", "~1");
  }

  /**
   * What an event's job is, from the mapper's {@value OpenLineageEventMapper#HOP_JOB_FACET} facet:
   * the flow (pipeline or workflow) and, for a transform or action, the job inside it.
   */
  record Subject(
      String flowName,
      String flowType,
      String flowFilename,
      String jobName,
      String jobType,
      List<String> upstreamJobs,
      List<String> rootJobs,
      List<String> members,
      Map<String, Object> facet) {

    static Subject of(RunEvent event) {
      if (event == null || event.getJob() == null || event.getRun() == null) {
        return null;
      }
      Map<String, Object> hop = hopFacet(event);
      String subjectType = string(hop, "subjectType");
      String pipeline = string(hop, "pipelineName");
      String workflow = string(hop, "workflowName");
      String filename = string(hop, FILENAME_PROPERTY);
      List<String> upstream =
          "ACTION".equals(subjectType)
              ? strings(hop, OpenLineageEventMapper.UPSTREAM_ACTIONS)
              : strings(hop, OpenLineageEventMapper.UPSTREAM_TRANSFORMS);
      List<String> roots =
          CONST_WORKFLOW.equals(subjectType)
              ? strings(hop, OpenLineageEventMapper.ROOT_ACTIONS)
              : strings(hop, OpenLineageEventMapper.ROOT_TRANSFORMS);
      List<String> members =
          CONST_WORKFLOW.equals(subjectType)
              ? strings(hop, OpenLineageEventMapper.ACTIONS)
              : strings(hop, OpenLineageEventMapper.TRANSFORMS);
      if ("TRANSFORM".equals(subjectType) && pipeline != null) {
        return new Subject(
            pipeline,
            CONST_PIPELINE,
            filename,
            string(hop, "transformName"),
            "Transform",
            upstream,
            roots,
            members,
            hop);
      }
      if ("ACTION".equals(subjectType) && workflow != null) {
        return new Subject(
            workflow,
            CONST_WORKFLOW_2,
            filename,
            string(hop, "actionName"),
            "Action",
            upstream,
            roots,
            members,
            hop);
      }
      if (CONST_WORKFLOW.equals(subjectType) && workflow != null) {
        return new Subject(
            workflow,
            CONST_WORKFLOW_2,
            filename,
            null,
            CONST_WORKFLOW_2,
            upstream,
            roots,
            members,
            hop);
      }
      if ("PIPELINE".equals(subjectType) && pipeline != null) {
        return new Subject(
            pipeline,
            CONST_PIPELINE,
            filename,
            null,
            CONST_PIPELINE,
            upstream,
            roots,
            members,
            hop);
      }
      // An event without the Hop facet (written by an older sink and replayed from its spool): the
      // job name is all there is, so it becomes a flow of its own.
      String name = event.getJob().getName();
      return StringUtils.isBlank(name)
          ? null
          : new Subject(
              name,
              CONST_PIPELINE,
              null,
              null,
              CONST_PIPELINE,
              List.of(),
              List.of(),
              List.of(),
              hop);
    }

    String facet(String key) {
      return string(facet, key);
    }

    private static Map<String, Object> hopFacet(RunEvent event) {
      OpenLineage.JobFacets facets = event.getJob().getFacets();
      if (facets == null || facets.getAdditionalProperties() == null) {
        return Map.of();
      }
      OpenLineage.JobFacet hop =
          facets.getAdditionalProperties().get(OpenLineageEventMapper.HOP_JOB_FACET);
      return hop == null || hop.getAdditionalProperties() == null
          ? Map.of()
          : hop.getAdditionalProperties();
    }

    private static String string(Map<String, Object> facet, String key) {
      Object value = facet.get(key);
      return value instanceof String s && !s.isBlank() ? s : null;
    }

    private static List<String> strings(Map<String, Object> facet, String key) {
      if (!(facet.get(key) instanceof List<?> list)) {
        return List.of();
      }
      List<String> values = new ArrayList<>();
      for (Object item : list) {
        if (item instanceof String s && !s.isBlank()) {
          values.add(s);
        }
      }
      return values;
    }
  }
}
