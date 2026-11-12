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

import io.openlineage.client.OpenLineage.RunEvent;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hop.lineage.LineageRunLifecycleEmitter;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.model.FileIoContentSchema;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.FileIoTabularColumn;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload.Transition;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.lineage.openlineage.OpenLineageEventMapper;

/**
 * Hop lineage events for one pipeline run, as the engine emits them, and the mapper the DataHub
 * sink runs them through. Tests start from these so they exercise the same mapping as the sink.
 */
final class HopEvents {

  /** When the events of a run happen, unless shifted with {@link #later}. */
  static final long STARTED_AT = 1_760_781_600_000L;

  private final String pipeline;
  private final String run;
  private final String pipelineRun;
  private final List<String[]> parent = new ArrayList<>();
  private String folder = "";
  private long offsetMs;

  HopEvents(String pipeline) {
    this(pipeline, "");
  }

  /** Another run of the same pipeline. */
  HopEvents(String pipeline, String run) {
    this.pipeline = pipeline;
    this.run = run;
    this.pipelineRun = uuid("pipeline:" + pipeline + run);
  }

  /** The pipeline was run by an action of a workflow, stored as {@code <workflow>.hwf}. */
  HopEvents startedBy(String workflow, String action) {
    parent.add(new String[] {workflow, action});
    return this;
  }

  /** The pipeline's file lies in this folder of the project. */
  HopEvents in(String folder) {
    this.folder = folder + "/";
    return this;
  }

  /** Every event of this run happens this much later. */
  HopEvents later(long offsetMs) {
    this.offsetMs = offsetMs;
    return this;
  }

  /** The pipeline's file, as the engine reports it inside a project. */
  String filename() {
    return "${PROJECT_HOME}/" + folder + pipeline + ".hpl";
  }

  static OpenLineageEventMapper mapper() {
    return OpenLineageEventMapper.builder("hop", "https://hop.apache.org/test")
        .transformJobs(true)
        .propagateInputs(false)
        .build();
  }

  static String uuid(String seed) {
    return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
  }

  String pipelineRunId() {
    return pipelineRun;
  }

  String transformRunId(String transform) {
    return uuid("transform:" + pipeline + run + "/" + transform);
  }

  LineageContext pipelineContext() {
    LineageContext.Builder ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .pipelineName(pipeline)
            .logChannelId(pipelineRun)
            .hopFilename("/projects/demo/" + folder + pipeline + ".hpl")
            .hopFilenamePortableKey(filename())
            .putAttribute(LineageRunLifecycleEmitter.ATTR_DESCRIPTION, "Pipeline " + pipeline);
    for (String[] workflowAction : parent) {
      ctx.putAttribute(
          LineageRunLifecycleEmitter.ATTR_PARENT_JOB_NAME,
          workflowAction[0] + "/" + workflowAction[1]);
      ctx.putAttribute(LineageRunLifecycleEmitter.ATTR_PARENT_RUN_ID, uuid("action-run"));
      ctx.putAttribute(LineageRunLifecycleEmitter.ATTR_PARENT_WORKFLOW_NAME, workflowAction[0]);
      ctx.putAttribute(LineageRunLifecycleEmitter.ATTR_PARENT_ACTION_NAME, workflowAction[1]);
      ctx.putAttribute(
          LineageRunLifecycleEmitter.ATTR_PARENT_FILENAME,
          "${PROJECT_HOME}/" + workflowAction[0] + ".hwf");
    }
    return ctx.build();
  }

  LineageContext transformContext(String transform) {
    return LineageContext.builder()
        .subjectType(LineageSubjectType.TRANSFORM)
        .pipelineName(pipeline)
        .transformName(transform)
        .logChannelId(transformRunId(transform))
        .hopFilename("/projects/demo/" + folder + pipeline + ".hpl")
        .hopFilenamePortableKey(filename())
        .putAttribute("pipelineLogChannelId", pipelineRun)
        .putAttribute(LineageRunLifecycleEmitter.ATTR_DESCRIPTION, "Transform " + transform)
        .build();
  }

  LineageEvent pipeline(RunLifecyclePhase phase) {
    return lifecycle(pipelineContext(), phase);
  }

  LineageEvent transform(String transform, RunLifecyclePhase phase) {
    return lifecycle(transformContext(transform), phase);
  }

  LineageEvent graph(String... fromTo) {
    List<Transition> transitions = new ArrayList<>();
    for (String pair : fromTo) {
      String[] ends = pair.split(">");
      transitions.add(new Transition(ends[0], ends[1]));
    }
    return new LineageEvent(
        UUID.randomUUID().toString(),
        STARTED_AT + offsetMs,
        LineageEventKind.PIPELINE_GRAPH,
        pipelineContext(),
        new PipelineGraphLineagePayload(transitions));
  }

  LineageEvent read(String transform, String uri, FileIoTabularColumn... columns) {
    return file(transform, FileIoOperation.READ, uri, null, columns);
  }

  LineageEvent write(String transform, String uri, FileIoTabularColumn... columns) {
    return file(transform, FileIoOperation.WRITE, null, uri, columns);
  }

  LineageEvent delete(String transform, String uri) {
    return file(transform, FileIoOperation.DELETE, null, uri);
  }

  LineageEvent move(String transform, String from, String to) {
    return file(transform, FileIoOperation.MOVE, from, to);
  }

  static FileIoTabularColumn column(String name, String hopType) {
    return new FileIoTabularColumn(name, hopType, 0, 0, null, null, false);
  }

  private LineageEvent file(
      String transform,
      FileIoOperation operation,
      String source,
      String target,
      FileIoTabularColumn... columns) {
    FileIoContentSchema schema =
        columns.length == 0 ? null : new FileIoContentSchema("csv", List.of(columns), List.of());
    return new LineageEvent(
        UUID.randomUUID().toString(),
        STARTED_AT + 1_000L + offsetMs,
        LineageEventKind.FILE_IO,
        transformContext(transform),
        new FileIoLineagePayload(operation, source, target, 10L, true, null, schema));
  }

  private LineageEvent lifecycle(LineageContext ctx, RunLifecyclePhase phase) {
    return new LineageEvent(
        UUID.randomUUID().toString(),
        (phase == RunLifecyclePhase.STARTED ? STARTED_AT : STARTED_AT + 5_000L) + offsetMs,
        LineageEventKind.RUN_LIFECYCLE,
        ctx,
        new RunLifecycleLineagePayload(phase, null));
  }

  /** A read → write pipeline run, start to finish, mapped as the sink maps it. */
  List<RunEvent> readThenWrite(String in, String out) {
    return mapper()
        .mapBatch(
            List.of(
                pipeline(RunLifecyclePhase.STARTED),
                graph("Read>Write"),
                transform("Read", RunLifecyclePhase.STARTED),
                transform("Write", RunLifecyclePhase.STARTED),
                read("Read", in, column("id", "Integer"), column("name", "String")),
                write("Write", out, column("id", "Integer"), column("name", "String")),
                transform("Read", RunLifecyclePhase.FINISHED),
                transform("Write", RunLifecyclePhase.FINISHED),
                pipeline(RunLifecyclePhase.FINISHED)));
  }
}
