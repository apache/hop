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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage.RunEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload.Transition;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.junit.jupiter.api.Test;

/**
 * Forward propagation: with one job per transform, a transform's job carries what it read plus what
 * its upstream transforms produced, so a dataset edge is a real pair instead of every input paired
 * with every output of the pipeline.
 */
class OpenLineageTransformJobsTest {

  private static final String PIPELINE = "demo";
  private static final String PIPELINE_RUN = uuid("pipeline");

  private static String uuid(String seed) {
    return UUID.nameUUIDFromBytes(seed.getBytes(java.nio.charset.StandardCharsets.UTF_8))
        .toString();
  }

  private static OpenLineageEventMapper mapper(boolean transformJobs) {
    return OpenLineageEventMapper.builder("hop", "https://example.test/producer")
        .transformJobs(transformJobs)
        .build();
  }

  private static LineageContext pipelineContext() {
    return LineageContext.builder()
        .subjectType(LineageSubjectType.PIPELINE)
        .pipelineName(PIPELINE)
        .logChannelId(PIPELINE_RUN)
        .build();
  }

  private static LineageContext transformContext(String transformName) {
    return LineageContext.builder()
        .subjectType(LineageSubjectType.TRANSFORM)
        .pipelineName(PIPELINE)
        .transformName(transformName)
        .logChannelId(uuid(transformName))
        .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN)
        .build();
  }

  private static LineageEvent lifecycle(
      LineageContext ctx, RunLifecyclePhase phase, String eventId) {
    return new LineageEvent(
        eventId,
        System.currentTimeMillis(),
        LineageEventKind.RUN_LIFECYCLE,
        ctx,
        new RunLifecycleLineagePayload(phase, null));
  }

  private static LineageEvent read(LineageContext ctx, String uri) {
    return new LineageEvent(
        UUID.randomUUID().toString(),
        System.currentTimeMillis(),
        LineageEventKind.FILE_IO,
        ctx,
        new FileIoLineagePayload(FileIoOperation.READ, uri, null, null, true, null, null));
  }

  private static LineageEvent write(LineageContext ctx, String uri) {
    return new LineageEvent(
        UUID.randomUUID().toString(),
        System.currentTimeMillis(),
        LineageEventKind.FILE_IO,
        ctx,
        new FileIoLineagePayload(FileIoOperation.WRITE, null, uri, null, true, null, null));
  }

  private static LineageEvent graph(String... pairs) {
    List<Transition> transitions = new ArrayList<>();
    for (String pair : pairs) {
      String[] ends = pair.split(">");
      transitions.add(new Transition(ends[0], ends[1]));
    }
    return new LineageEvent(
        UUID.randomUUID().toString(),
        System.currentTimeMillis(),
        LineageEventKind.PIPELINE_GRAPH,
        pipelineContext(),
        new PipelineGraphLineagePayload(transitions));
  }

  private static List<String> names(List<? extends io.openlineage.client.OpenLineage.Dataset> ds) {
    if (ds == null) {
      return List.of();
    }
    List<String> names = new ArrayList<>();
    for (io.openlineage.client.OpenLineage.Dataset dataset : ds) {
      names.add(dataset.getNamespace() + ":" + dataset.getName());
    }
    return names;
  }

  private static RunEvent eventFor(List<RunEvent> events, String jobName) {
    for (RunEvent event : events) {
      if (jobName.equals(event.getJob().getName())) {
        return event;
      }
    }
    throw new AssertionError("No event for job " + jobName + " in " + events);
  }

  private static List<RunEvent> finish(
      OpenLineageEventMapper mapper, List<LineageEvent> observed, String... transforms) {
    List<LineageEvent> events = new ArrayList<>(observed);
    for (String transform : transforms) {
      events.add(
          lifecycle(transformContext(transform), RunLifecyclePhase.FINISHED, uuid(transform)));
    }
    events.add(lifecycle(pipelineContext(), RunLifecyclePhase.FINISHED, uuid("pipeline-end")));
    return mapper.mapBatch(events);
  }

  /** Two independent input→output chains must not be paired across each other. */
  @Test
  void twoChainsProduceTwoEdgesNotTheCrossProduct() {
    OpenLineageEventMapper mapper = mapper(true);
    List<LineageEvent> observed =
        List.of(
            graph("Read A>Write X", "Read B>Write Y"),
            read(transformContext("Read A"), "file:///data/a.csv"),
            read(transformContext("Read B"), "file:///data/b.csv"),
            write(transformContext("Write X"), "file:///out/x.csv"),
            write(transformContext("Write Y"), "file:///out/y.csv"));

    List<RunEvent> events = finish(mapper, observed, "Read A", "Read B", "Write X", "Write Y");

    RunEvent writeX = eventFor(events, PIPELINE + "/Write X");
    assertEquals(List.of("file:/data/a.csv"), names(writeX.getInputs()));
    assertEquals(List.of("file:/out/x.csv"), names(writeX.getOutputs()));
    RunEvent writeY = eventFor(events, PIPELINE + "/Write Y");
    assertEquals(List.of("file:/data/b.csv"), names(writeY.getInputs()));
    assertEquals(List.of("file:/out/y.csv"), names(writeY.getOutputs()));

    // The pipeline job stays as the run-level entry point, without datasets: pairing every dataset
    // there is exactly the cross-product this avoids.
    RunEvent pipeline = eventFor(events, PIPELINE);
    assertNull(pipeline.getInputs());
    assertNull(pipeline.getOutputs());
  }

  /** A transform that only filters has no I/O of its own; lineage still has to cross it. */
  @Test
  void passThroughTransformForwardsItsUpstreamDatasets() {
    OpenLineageEventMapper mapper = mapper(true);
    List<LineageEvent> observed =
        List.of(
            graph("Read>Filter", "Filter>Write"),
            read(transformContext("Read"), "file:///data/a.csv"),
            write(transformContext("Write"), "file:///out/x.csv"));

    List<RunEvent> events = finish(mapper, observed, "Read", "Filter", "Write");

    RunEvent filter = eventFor(events, PIPELINE + "/Filter");
    assertEquals(List.of("file:/data/a.csv"), names(filter.getInputs()));
    assertNull(filter.getOutputs());

    RunEvent write = eventFor(events, PIPELINE + "/Write");
    assertEquals(List.of("file:/data/a.csv"), names(write.getInputs()));
    assertEquals(List.of("file:/out/x.csv"), names(write.getOutputs()));
  }

  /** One reader feeding two writers: both writers carry the same input, each its own output. */
  @Test
  void fanOutGivesEachWriterTheSharedInput() {
    OpenLineageEventMapper mapper = mapper(true);
    List<LineageEvent> observed =
        List.of(
            graph("Read>Write X", "Read>Write Y"),
            read(transformContext("Read"), "file:///data/a.csv"),
            write(transformContext("Write X"), "file:///out/x.csv"),
            write(transformContext("Write Y"), "file:///out/y.csv"));

    List<RunEvent> events = finish(mapper, observed, "Read", "Write X", "Write Y");

    assertEquals(
        List.of("file:/data/a.csv"), names(eventFor(events, PIPELINE + "/Write X").getInputs()));
    assertEquals(
        List.of("file:/data/a.csv"), names(eventFor(events, PIPELINE + "/Write Y").getInputs()));
  }

  /** An upstream transform that writes hands its target down, not the file it read. */
  @Test
  void aWriterHandsItsOutputDownstream() {
    OpenLineageEventMapper mapper = mapper(true);
    List<LineageEvent> observed =
        List.of(
            graph("Read>Copy", "Copy>Stitch"),
            read(transformContext("Read"), "file:///data/a.csv"),
            write(transformContext("Copy"), "file:///tmp/b.csv"),
            read(transformContext("Stitch"), "file:///tmp/b.csv"),
            write(transformContext("Stitch"), "file:///out/c.csv"));

    List<RunEvent> events = finish(mapper, observed, "Read", "Copy", "Stitch");

    RunEvent stitch = eventFor(events, PIPELINE + "/Stitch");
    assertEquals(List.of("file:/tmp/b.csv"), names(stitch.getInputs()));
    assertEquals(List.of("file:/out/c.csv"), names(stitch.getOutputs()));
  }

  /**
   * The graph is emitted next to the pipeline's start, after the transform threads were started: a
   * reader can report its file before the graph arrives. The attribution waits for the pipeline's
   * end, so that order does not matter.
   */
  @Test
  void graphArrivingAfterTheFirstIoStillGivesTransformsTheirJobs() {
    OpenLineageEventMapper mapper = mapper(true);
    List<LineageEvent> observed =
        List.of(
            read(transformContext("Read"), "file:///data/a.csv"),
            graph("Read>Write"),
            write(transformContext("Write"), "file:///out/x.csv"));

    List<RunEvent> events = finish(mapper, observed, "Read", "Write");

    RunEvent write = eventFor(events, PIPELINE + "/Write");
    assertEquals(List.of("file:/data/a.csv"), names(write.getInputs()));
    assertEquals(List.of("file:/out/x.csv"), names(write.getOutputs()));
    assertNull(eventFor(events, PIPELINE).getInputs());
  }

  /**
   * A relational writer reports its table when it finishes, after it closed its connection; the
   * transform downstream of it can finish first. Its upstream table must still be forwarded.
   */
  @Test
  void anUpstreamWriterThatReportsLateIsStillForwarded() {
    OpenLineageEventMapper mapper = mapper(true);
    List<LineageEvent> events = new ArrayList<>();
    events.add(graph("Read>Copy", "Copy>Stitch"));
    events.add(read(transformContext("Read"), "file:///data/a.csv"));
    events.add(write(transformContext("Stitch"), "file:///out/c.csv"));
    events.add(lifecycle(transformContext("Stitch"), RunLifecyclePhase.FINISHED, uuid("Stitch")));
    events.add(write(transformContext("Copy"), "file:///tmp/b.csv"));
    events.add(lifecycle(transformContext("Copy"), RunLifecyclePhase.FINISHED, uuid("Copy")));
    events.add(lifecycle(transformContext("Read"), RunLifecyclePhase.FINISHED, uuid("Read")));
    events.add(lifecycle(pipelineContext(), RunLifecyclePhase.FINISHED, uuid("pipeline-end")));

    List<RunEvent> mapped = mapper.mapBatch(events);

    assertEquals(
        List.of("file:/tmp/b.csv"), names(eventFor(mapped, PIPELINE + "/Stitch").getInputs()));
    // The transform events go out before the pipeline's own, which closes the run.
    assertEquals(PIPELINE, mapped.get(mapped.size() - 1).getJob().getName());
  }

  /**
   * More pipeline runs in flight than the mapper keeps state for: the oldest is pushed out, and
   * what it had collected is sent instead of dropped.
   */
  @Test
  void aPipelineRunPushedOutOfTheWindowIsFlushedNotDropped() {
    OpenLineageEventMapper mapper = mapper(true);
    List<LineageEvent> events = new ArrayList<>();
    events.add(read(transformContext("Read"), "file:///data/a.csv"));
    for (int i = 0; i < OpenLineageEventMapper.MAX_PENDING_RUNS; i++) {
      LineageContext other =
          LineageContext.builder()
              .subjectType(LineageSubjectType.TRANSFORM)
              .pipelineName("other-" + i)
              .transformName("Read")
              .logChannelId(uuid("other-read-" + i))
              .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, uuid("other-" + i))
              .build();
      events.add(read(other, "file:///data/other-" + i + ".csv"));
    }

    List<RunEvent> mapped = mapper.mapBatch(events);

    RunEvent flushed = eventFor(mapped, PIPELINE);
    assertEquals(List.of("file:/data/a.csv"), names(flushed.getInputs()));
  }

  /**
   * A collector that links jobs to each other (DataHub) wants each transform's own datasets and the
   * hop graph, not the propagated inputs: those would draw a second, direct edge next to the path
   * through the transforms.
   */
  @Test
  void withoutPropagationEachTransformKeepsItsOwnDatasetsAndNamesItsUpstream() {
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://example.test/producer")
            .transformJobs(true)
            .propagateInputs(false)
            .build();
    List<LineageEvent> observed =
        List.of(
            graph("Read>Filter", "Filter>Write"),
            read(transformContext("Read"), "file:///data/a.csv"),
            write(transformContext("Write"), "file:///out/x.csv"));

    List<RunEvent> events = finish(mapper, observed, "Read", "Filter", "Write");

    RunEvent write = eventFor(events, PIPELINE + "/Write");
    assertTrue(names(write.getInputs()).isEmpty());
    assertEquals(List.of("file:/out/x.csv"), names(write.getOutputs()));
    assertEquals(
        List.of("Filter"), hopFacet(write).get(OpenLineageEventMapper.UPSTREAM_TRANSFORMS));
    assertEquals(
        List.of("Read"),
        hopFacet(eventFor(events, PIPELINE)).get(OpenLineageEventMapper.ROOT_TRANSFORMS));
  }

  private static java.util.Map<String, Object> hopFacet(RunEvent event) {
    return event
        .getJob()
        .getFacets()
        .getAdditionalProperties()
        .get(OpenLineageEventMapper.HOP_JOB_FACET)
        .getAdditionalProperties();
  }

  /** Without the graph (an engine that predates it) the mapping must stay pipeline-granular. */
  @Test
  void noGraphFallsBackToThePipelineJob() {
    OpenLineageEventMapper mapper = mapper(true);
    List<LineageEvent> observed =
        List.of(
            read(transformContext("Read"), "file:///data/a.csv"),
            write(transformContext("Write"), "file:///out/x.csv"));

    List<RunEvent> events = finish(mapper, observed, "Read", "Write");

    RunEvent pipeline = eventFor(events, PIPELINE);
    assertEquals(List.of("file:/data/a.csv"), names(pipeline.getInputs()));
    assertEquals(List.of("file:/out/x.csv"), names(pipeline.getOutputs()));
    assertTrue(
        names(eventFor(events, PIPELINE + "/Write").getInputs()).isEmpty(),
        "the transform job must not hold datasets it cannot pair");
  }

  /** The flag is off by default: the pipeline job carries everything, as before. */
  @Test
  void flagOffKeepsThePipelineGranularMapping() {
    OpenLineageEventMapper mapper = mapper(false);
    List<LineageEvent> observed =
        List.of(
            graph("Read>Write"),
            read(transformContext("Read"), "file:///data/a.csv"),
            write(transformContext("Write"), "file:///out/x.csv"));

    List<RunEvent> events = finish(mapper, observed, "Read", "Write");

    RunEvent pipeline = eventFor(events, PIPELINE);
    assertEquals(List.of("file:/data/a.csv"), names(pipeline.getInputs()));
    assertEquals(List.of("file:/out/x.csv"), names(pipeline.getOutputs()));
  }
}
