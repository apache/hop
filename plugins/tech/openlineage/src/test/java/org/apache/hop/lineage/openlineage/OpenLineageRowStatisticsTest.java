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

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hop.lineage.LineageRowCounts;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload;
import org.apache.hop.lineage.model.PipelineGraphLineagePayload.Transition;
import org.apache.hop.lineage.model.RelationalIoLineagePayload;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.lineage.model.RelationalTable;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.junit.jupiter.api.Test;

/** The statistics facets a run's datasets get from the row counters of its transforms. */
class OpenLineageRowStatisticsTest {

  private static final String PIPELINE = "load";
  private static final String PIPELINE_RUN = uuid("pipeline");
  private static final String IN = "file:///data/in.csv";
  private static final String OUT_X = "file:///out/x.csv";
  private static final String OUT_Y = "file:///out/y.csv";

  private static String uuid(String seed) {
    return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
  }

  private static OpenLineageEventMapper mapper(boolean transformJobs) {
    return OpenLineageEventMapper.builder("hop", "https://example.test/producer")
        .transformJobs(transformJobs)
        .build();
  }

  @Test
  void eachDatasetGetsTheRowsOfTheTransformsThatTouchedOnlyIt() {
    List<RunEvent> events = mapper(false).mapBatch(oneReaderTwoWriters());

    RunEvent pipeline = eventFor(events, PIPELINE);
    assertEquals(100L, inputRows(pipeline, "/data/in.csv"));
    assertEquals(60L, outputRows(pipeline, "/out/x.csv"));
    // Two copies of Write Y: 15 + 25 rows, and 5 updated
    assertEquals(45L, outputRows(pipeline, "/out/y.csv"));
  }

  @Test
  void eachCopyOfATransformJobCountsItsOwnRows() {
    List<RunEvent> events = mapper(true).mapBatch(oneReaderTwoWriters());

    List<RunEvent> writeY = eventsFor(events, PIPELINE + "/Write Y");
    assertEquals(2, writeY.size());
    List<Long> rows = new ArrayList<>();
    for (RunEvent copy : writeY) {
      rows.add(outputRows(copy, "/out/y.csv"));
    }
    assertEquals(List.of(15L, 30L), rows);

    RunEvent read = eventFor(events, PIPELINE + "/Read");
    assertEquals(100L, inputRows(read, "/data/in.csv"));
    // Write X consumes the file Read read, but read none of it itself
    RunEvent writeX = eventFor(events, PIPELINE + "/Write X");
    assertNull(inputRows(writeX, "/data/in.csv"));
    assertEquals(60L, outputRows(writeX, "/out/x.csv"));
  }

  @Test
  void aTransformThatWritesTwoDatasetsGivesThemNoCount() {
    List<LineageEvent> observed = new ArrayList<>();
    observed.add(write(transform("Split", "0"), OUT_X));
    observed.add(write(transform("Split", "0"), OUT_Y));
    observed.add(finished(transform("Split", "0"), 0, 80, 0));
    observed.add(pipelineFinished());

    RunEvent pipeline = eventFor(mapper(false).mapBatch(observed), PIPELINE);

    assertEquals(2, pipeline.getOutputs().size());
    assertNull(outputRows(pipeline, "/out/x.csv"));
    assertNull(outputRows(pipeline, "/out/y.csv"));
  }

  @Test
  void anEngineWithoutCountersGivesNoStatistics() {
    List<LineageEvent> observed = new ArrayList<>();
    observed.add(write(transform("Write X", "0"), OUT_X));
    observed.add(
        lifecycle(transform("Write X", "0").build(), RunLifecyclePhase.FINISHED, uuid("x")));
    observed.add(pipelineFinished());

    RunEvent pipeline = eventFor(mapper(false).mapBatch(observed), PIPELINE);

    assertNull(pipeline.getOutputs().get(0).getOutputFacets());
  }

  @Test
  void theStatisticsAreInTheJsonOfTheEvent() {
    RunEvent pipeline = eventFor(mapper(false).mapBatch(oneReaderTwoWriters()), PIPELINE);

    String json = OpenLineageClientUtils.toJson(pipeline);

    assertTrue(json.contains("\"outputStatistics\""), json);
    assertTrue(json.contains("\"rowCount\":60"), json);
    assertTrue(json.contains("\"inputStatistics\""), json);
  }

  @Test
  void thePipelineListsEachDatasetItWroteOnce() {
    RunEvent pipeline = eventFor(mapper(true).mapBatch(oneReaderTwoWriters()), PIPELINE);

    List<Map<String, Object>> writes = writes(pipeline);
    assertEquals(2, writes.size());
    assertEquals("INSERT", writes.get(0).get("operationType"));
    assertEquals(60L, ((Number) writes.get(0).get("rows")).longValue());
    assertTrue(((String) writes.get(1).get("name")).endsWith("/out/y.csv"));
    assertEquals(45L, ((Number) writes.get(1).get("rows")).longValue());
  }

  @Test
  void aStoppedPipelineListsNoWrites() {
    List<LineageEvent> observed = new ArrayList<>(oneReaderTwoWriters());
    observed.set(
        observed.size() - 1,
        lifecycle(
            LineageContext.builder()
                .subjectType(LineageSubjectType.PIPELINE)
                .pipelineName(PIPELINE)
                .logChannelId(PIPELINE_RUN)
                .putAttribute(LineageRowCounts.STOPPED, "true")
                .putAttribute(LineageRowCounts.LINES_OUTPUT, "45")
                .build(),
            RunLifecyclePhase.FINISHED,
            uuid("pipeline-stopped")));

    RunEvent pipeline = eventFor(mapper(false).mapBatch(observed), PIPELINE);

    assertNull(
        pipeline
            .getRun()
            .getFacets()
            .getAdditionalProperties()
            .get(OpenLineageEventMapper.HOP_WRITES_RUN_FACET));
    assertEquals(
        true,
        pipeline
            .getRun()
            .getFacets()
            .getAdditionalProperties()
            .get(OpenLineageEventMapper.HOP_ROW_COUNTS_RUN_FACET)
            .getAdditionalProperties()
            .get(LineageRowCounts.STOPPED));
  }

  @Test
  void deletedRowsAreNotRowsWritten() {
    List<LineageEvent> observed = new ArrayList<>();
    observed.add(
        new LineageEvent(
            UUID.randomUUID().toString(),
            System.currentTimeMillis(),
            LineageEventKind.RELATIONAL_IO,
            transform("Delete", "0").build(),
            new RelationalIoLineagePayload(
                RelationalIoOperation.DELETE,
                "postgres://db:5432",
                List.of(),
                List.of(new RelationalTable("dwh", "public", "orders")),
                null,
                null,
                true,
                null)));
    observed.add(
        lifecycle(
            transform("Delete", "0")
                .putAttribute(LineageRowCounts.LINES_READ, "500")
                .putAttribute(LineageRowCounts.LINES_OUTPUT, "0")
                .putAttribute(LineageRowCounts.LINES_UPDATED, "500")
                .build(),
            RunLifecyclePhase.FINISHED,
            uuid("delete")));
    observed.add(pipelineFinished());

    RunEvent pipeline = eventFor(mapper(false).mapBatch(observed), PIPELINE);

    assertNull(outputRows(pipeline, "orders"));
    Map<String, Object> write = writes(pipeline).get(0);
    assertEquals("DELETE", write.get("operationType"));
    assertEquals(500L, ((Number) write.get("rows")).longValue());
  }

  @Test
  void aTableEmptiedAndReloadedInOneRunWasLoaded() {
    List<LineageEvent> observed = new ArrayList<>();
    observed.add(
        relational(transform("Clean", "0"), RelationalIoOperation.EXEC, "DELETE FROM orders"));
    observed.add(relational(transform("Load", "0"), RelationalIoOperation.WRITE, null));
    observed.add(counted(transform("Clean", "0"), 0, 0, 500, "clean"));
    observed.add(counted(transform("Load", "0"), 10_000, 10_000, 0, "load"));
    observed.add(pipelineFinished());

    RunEvent pipeline = eventFor(mapper(false).mapBatch(observed), PIPELINE);

    assertEquals(10_000L, outputRows(pipeline, "orders"));
    Map<String, Object> write = writes(pipeline).get(0);
    assertEquals("INSERT", write.get("operationType"));
    assertEquals(10_000L, ((Number) write.get("rows")).longValue());
  }

  @Test
  void aScriptOfStatementsGivesNoCount() {
    assertEquals(0, OpenLineageEventMapper.statementCount(null));
    assertEquals(1, OpenLineageEventMapper.statementCount("DELETE FROM a"));
    assertEquals(1, OpenLineageEventMapper.statementCount(" DELETE FROM a ; "));
    assertEquals(1, OpenLineageEventMapper.statementCount("UPDATE a SET b = ';'"));
    assertEquals(
        2, OpenLineageEventMapper.statementCount("DELETE FROM a; INSERT INTO a SELECT * FROM b"));
  }

  private static LineageEvent relational(
      LineageContext.Builder ctx, RelationalIoOperation operation, String sql) {
    return new LineageEvent(
        UUID.randomUUID().toString(),
        System.currentTimeMillis(),
        LineageEventKind.RELATIONAL_IO,
        ctx.build(),
        new RelationalIoLineagePayload(
            operation,
            "postgres://db:5432",
            List.of(),
            List.of(new RelationalTable("dwh", "public", "orders")),
            sql,
            null,
            true,
            null));
  }

  private static LineageEvent counted(
      LineageContext.Builder ctx, long read, long output, long updated, String id) {
    return lifecycle(
        ctx.putAttribute(LineageRowCounts.LINES_READ, Long.toString(read))
            .putAttribute(LineageRowCounts.LINES_OUTPUT, Long.toString(output))
            .putAttribute(LineageRowCounts.LINES_UPDATED, Long.toString(updated))
            .build(),
        RunLifecyclePhase.FINISHED,
        uuid(id));
  }

  @Test
  void aTransformThatDoesNotCountWhatItWritesGivesNoCount() {
    List<LineageEvent> observed = new ArrayList<>();
    observed.add(write(transform("Avro", "0"), OUT_X));
    observed.add(
        lifecycle(
            transform("Avro", "0")
                .putAttribute(LineageRowCounts.LINES_READ, "80")
                .putAttribute(LineageRowCounts.LINES_OUTPUT, "0")
                .build(),
            RunLifecyclePhase.FINISHED,
            uuid("avro")));
    observed.add(pipelineFinished());

    RunEvent pipeline = eventFor(mapper(false).mapBatch(observed), PIPELINE);

    assertNull(outputRows(pipeline, "/out/x.csv"));
    assertNull(writes(pipeline).get(0).get("rows"));
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> writes(RunEvent event) {
    return (List<Map<String, Object>>)
        event
            .getRun()
            .getFacets()
            .getAdditionalProperties()
            .get(OpenLineageEventMapper.HOP_WRITES_RUN_FACET)
            .getAdditionalProperties()
            .get("writes");
  }

  /** Read reads 100 rows; Write X writes 60; two copies of Write Y write 15 and 25, update 5. */
  private static List<LineageEvent> oneReaderTwoWriters() {
    List<LineageEvent> events = new ArrayList<>();
    events.add(graph("Read>Write X", "Read>Write Y"));
    events.add(read(transform("Read", "0"), IN));
    events.add(write(transform("Write X", "0"), OUT_X));
    events.add(write(transform("Write Y", "0"), OUT_Y));
    events.add(write(transform("Write Y", "1"), OUT_Y));
    events.add(finished(transform("Read", "0"), 100, 0, 0));
    events.add(finished(transform("Write X", "0"), 0, 60, 0));
    events.add(finished(transform("Write Y", "0"), 0, 15, 0));
    events.add(finished(transform("Write Y", "1"), 0, 25, 5));
    events.add(pipelineFinished());
    return events;
  }

  private static LineageContext.Builder transform(String name, String copy) {
    return LineageContext.builder()
        .subjectType(LineageSubjectType.TRANSFORM)
        .pipelineName(PIPELINE)
        .transformName(name)
        .copyNr(copy)
        .logChannelId(uuid(name + "." + copy))
        .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN);
  }

  private static LineageEvent finished(
      LineageContext.Builder ctx, long input, long output, long updated) {
    LineageContext context =
        ctx.putAttribute(LineageRowCounts.LINES_INPUT, Long.toString(input))
            .putAttribute(LineageRowCounts.LINES_OUTPUT, Long.toString(output))
            .putAttribute(LineageRowCounts.LINES_UPDATED, Long.toString(updated))
            .build();
    return lifecycle(
        context,
        RunLifecyclePhase.FINISHED,
        uuid(context.getTransformName() + context.getCopyNr()));
  }

  private static LineageEvent pipelineFinished() {
    return lifecycle(
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .pipelineName(PIPELINE)
            .logChannelId(PIPELINE_RUN)
            .build(),
        RunLifecyclePhase.FINISHED,
        uuid("pipeline-end"));
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

  private static LineageEvent read(LineageContext.Builder ctx, String uri) {
    return new LineageEvent(
        UUID.randomUUID().toString(),
        System.currentTimeMillis(),
        LineageEventKind.FILE_IO,
        ctx.build(),
        new FileIoLineagePayload(FileIoOperation.READ, uri, null, null, true, null, null));
  }

  private static LineageEvent write(LineageContext.Builder ctx, String uri) {
    return new LineageEvent(
        UUID.randomUUID().toString(),
        System.currentTimeMillis(),
        LineageEventKind.FILE_IO,
        ctx.build(),
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
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .pipelineName(PIPELINE)
            .logChannelId(PIPELINE_RUN)
            .build(),
        new PipelineGraphLineagePayload(transitions));
  }

  private static RunEvent eventFor(List<RunEvent> events, String jobName) {
    List<RunEvent> found = eventsFor(events, jobName);
    assertEquals(1, found.size(), "events of " + jobName);
    return found.get(0);
  }

  private static List<RunEvent> eventsFor(List<RunEvent> events, String jobName) {
    List<RunEvent> found = new ArrayList<>();
    for (RunEvent event : events) {
      if (jobName.equals(event.getJob().getName())) {
        found.add(event);
      }
    }
    return found;
  }

  private static Long inputRows(RunEvent event, String path) {
    if (event.getInputs() == null) {
      return null;
    }
    for (InputDataset input : event.getInputs()) {
      if (input.getName().endsWith(path)) {
        return input.getInputFacets() == null || input.getInputFacets().getInputStatistics() == null
            ? null
            : input.getInputFacets().getInputStatistics().getRowCount();
      }
    }
    throw new AssertionError("No input " + path);
  }

  private static Long outputRows(RunEvent event, String path) {
    for (OutputDataset output : event.getOutputs()) {
      if (output.getName().endsWith(path)) {
        return output.getOutputFacets() == null
                || output.getOutputFacets().getOutputStatistics() == null
            ? null
            : output.getOutputFacets().getOutputStatistics().getRowCount();
      }
    }
    throw new AssertionError("No output " + path);
  }
}
