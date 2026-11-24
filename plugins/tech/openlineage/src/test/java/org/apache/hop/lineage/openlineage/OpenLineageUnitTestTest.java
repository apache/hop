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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.hop.lineage.model.RelationalIoLineagePayload;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.lineage.model.RelationalTable;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.lineage.model.UnitTestLineagePayload;
import org.junit.jupiter.api.Test;

/** A Hop unit test result as a run facet of the tested transform's run, or its pipeline's. */
class OpenLineageUnitTestTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String PIPELINE_RUN = UUID.randomUUID().toString();
  private static final String TRANSFORM_RUN = UUID.randomUUID().toString();

  private static LineageEvent unitTest(UnitTestLineagePayload payload) {
    return unitTest("Write", TRANSFORM_RUN, payload);
  }

  private static LineageContext transform(String name, String run) {
    return LineageContext.builder()
        .subjectType(LineageSubjectType.TRANSFORM)
        .pipelineName("orders")
        .transformName(name)
        .logChannelId(run)
        .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN)
        .build();
  }

  private static LineageEvent unitTest(
      String transform, String run, UnitTestLineagePayload payload) {
    return LineageEvent.of(LineageEventKind.UNIT_TEST, transform(transform, run), payload);
  }

  /**
   * The start of the tested pipeline's run, which a sink set by the HOP_LINEAGE_* variables gets.
   */
  private static LineageEvent pipelineStarted() {
    return pipelineStarted(PIPELINE_RUN);
  }

  private static LineageEvent pipelineStarted(String run) {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .pipelineName("orders")
            .logChannelId(run)
            .build();
    return LineageEvent.of(
        LineageEventKind.RUN_LIFECYCLE,
        ctx,
        new RunLifecycleLineagePayload(RunLifecyclePhase.STARTED, null));
  }

  private static UnitTestLineagePayload passed() {
    return result("golden", UnitTestLineagePayload.Result.PASSED);
  }

  private static UnitTestLineagePayload result(String dataSet, UnitTestLineagePayload.Result r) {
    return new UnitTestLineagePayload("orders-test", dataSet, r, null, List.of());
  }

  private static List<RunEvent.EventType> types(OpenLineageEventMapper mapper, LineageEvent... in) {
    List<RunEvent.EventType> types = new ArrayList<>();
    for (LineageEvent event : in) {
      mapper.toRunEvents(event).forEach(e -> types.add(e.getEventType()));
    }
    return types;
  }

  // One result per golden data set: the run ends once, with the first, and the others are OTHER.
  @Test
  void aRunWithSeveralResultsEndsOnce() {
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org").build();

    List<RunEvent.EventType> types =
        types(
            mapper,
            unitTest("Write", TRANSFORM_RUN, result("g1", UnitTestLineagePayload.Result.FAILED)),
            unitTest("Load", UUID.randomUUID().toString(), passed()),
            unitTest("Write", TRANSFORM_RUN, result("g2", UnitTestLineagePayload.Result.PASSED)));

    assertEquals(
        List.of(RunEvent.EventType.FAIL, RunEvent.EventType.OTHER, RunEvent.EventType.OTHER),
        types,
        "without jobs for transforms all results are on the pipeline's run");
  }

  @Test
  void withJobsForTransformsEachTransformsRunEndsOnce() {
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org").transformJobs(true).build();

    List<RunEvent.EventType> types =
        types(
            mapper,
            unitTest("Write", TRANSFORM_RUN, result("g1", UnitTestLineagePayload.Result.ERROR)),
            unitTest("Load", UUID.randomUUID().toString(), passed()),
            unitTest("Write", TRANSFORM_RUN, result("g2", UnitTestLineagePayload.Result.PASSED)));

    assertEquals(
        List.of(RunEvent.EventType.FAIL, RunEvent.EventType.COMPLETE, RunEvent.EventType.OTHER),
        types);
  }

  // A test that failed, or couldn't compare its rows, ends the run as failed.
  @Test
  void aResultThatDidNotPassEndsTheRunAsFailed() {
    for (UnitTestLineagePayload.Result r :
        List.of(UnitTestLineagePayload.Result.FAILED, UnitTestLineagePayload.Result.ERROR)) {
      OpenLineageEventMapper mapper =
          OpenLineageEventMapper.builder("hop", "https://hop.apache.org").build();

      assertEquals(
          List.of(RunEvent.EventType.FAIL), types(mapper, unitTest(result("g", r))), r.name());
    }
  }

  // A busy server starts many runs while a test pipeline runs: the test run's own events keep it
  // known, so its result still leaves the end to them.
  @Test
  void aRunStaysKnownWhileManyOtherRunsStart() {
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org").build();
    mapper.toRunEvents(pipelineStarted());
    for (int i = 0; i < OpenLineageEventMapper.MAX_PENDING_RUNS + 100; i++) {
      mapper.toRunEvents(pipelineStarted(UUID.randomUUID().toString()));
    }
    // The tested transform ends, then its result comes
    mapper.toRunEvents(
        LineageEvent.of(
            LineageEventKind.RUN_LIFECYCLE,
            transform("Write", TRANSFORM_RUN),
            new RunLifecycleLineagePayload(RunLifecyclePhase.FINISHED, null)));

    assertEquals(List.of(RunEvent.EventType.OTHER), types(mapper, unitTest(passed())));
  }

  @Test
  void theResultAndTheDatasetsItVouchesForAreOnTheTransformsRun() {
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org").transformJobs(true).build();
    mapper.toRunEvents(pipelineStarted());
    UnitTestLineagePayload payload =
        new UnitTestLineagePayload(
            "orders-test",
            "golden-orders",
            UnitTestLineagePayload.Result.FAILED,
            "row 3: amount 10 is not 12",
            List.of(
                new FileIoLineagePayload(
                    FileIoOperation.WRITE, null, "file:///out/orders.csv", null, true, null, null),
                new RelationalIoLineagePayload(
                    RelationalIoOperation.WRITE,
                    "postgres://db:5432",
                    List.of(),
                    List.of(new RelationalTable("sales", "public", "orders")),
                    null,
                    null,
                    "sales",
                    "public",
                    List.of(),
                    null,
                    true,
                    null)));

    List<RunEvent> events = mapper.toRunEvents(unitTest(payload));

    assertEquals(1, events.size());
    RunEvent event = events.get(0);
    assertEquals(RunEvent.EventType.OTHER, event.getEventType());
    assertEquals(UUID.fromString(TRANSFORM_RUN), event.getRun().getRunId());
    assertTrue(event.getJob().getName().endsWith("Write"), event.getJob().getName());
    assertNull(event.getOutputs(), "a test run doesn't write the datasets it vouches for");
    JsonNode facet =
        MAPPER.valueToTree(
            event
                .getRun()
                .getFacets()
                .getAdditionalProperties()
                .get(OpenLineageEventMapper.UNIT_TEST_FACET));
    assertEquals("orders-test", facet.path("unitTest").asText());
    assertEquals("golden-orders", facet.path("dataSet").asText());
    assertEquals(false, facet.path("passed").asBoolean(true));
    assertEquals("row 3: amount 10 is not 12", facet.path("comment").asText());
    assertEquals(2, facet.path("datasets").size(), facet.toString());
    assertEquals("postgres://db:5432", facet.path("datasets").get(1).path("namespace").asText());
    assertEquals("sales.public.orders", facet.path("datasets").get(1).path("name").asText());
  }

  @Test
  void withoutJobsForTransformsTheResultIsOnThePipelinesRun() {
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org").build();

    List<RunEvent> events =
        mapper.toRunEvents(
            unitTest(
                new UnitTestLineagePayload(
                    "orders-test",
                    "golden",
                    UnitTestLineagePayload.Result.ERROR,
                    null,
                    List.of())));

    assertEquals(1, events.size());
    RunEvent event = events.get(0);
    assertEquals(UUID.fromString(PIPELINE_RUN), event.getRun().getRunId());
    assertEquals("orders", event.getJob().getName(), "no job of its own for the transform");
    JsonNode facet =
        MAPPER.valueToTree(
            event
                .getRun()
                .getFacets()
                .getAdditionalProperties()
                .get(OpenLineageEventMapper.UNIT_TEST_FACET));
    assertEquals(false, facet.path("passed").asBoolean(true));
    assertEquals(true, facet.path("error").asBoolean(false), "not compared: " + facet);
  }

  /**
   * A sink selected in a run configuration gets the results of a unit test run but not its start
   * and end: the result ends the run, which Marquez would list as running for good otherwise.
   */
  @Test
  void withoutTheRunsOwnEventsTheResultEndsThePipelinesRun() {
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org").build();

    List<RunEvent> events = mapper.toRunEvents(unitTest(passed()));

    assertEquals(1, events.size());
    RunEvent event = events.get(0);
    assertEquals(RunEvent.EventType.COMPLETE, event.getEventType());
    assertEquals(UUID.fromString(PIPELINE_RUN), event.getRun().getRunId());
    assertEquals("orders", event.getJob().getName());
  }

  @Test
  void withoutTheRunsOwnEventsTheResultEndsTheTransformsRun() {
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org").transformJobs(true).build();

    List<RunEvent> events = mapper.toRunEvents(unitTest(passed()));

    assertEquals(1, events.size());
    assertEquals(RunEvent.EventType.COMPLETE, events.get(0).getEventType());
    assertEquals(UUID.fromString(TRANSFORM_RUN), events.get(0).getRun().getRunId());
  }

  @Test
  void withTheRunsOwnEventsTheResultLeavesTheEndToThem() {
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org").build();
    mapper.toRunEvents(pipelineStarted());

    List<RunEvent> events = mapper.toRunEvents(unitTest(passed()));

    assertEquals(1, events.size());
    assertEquals(RunEvent.EventType.OTHER, events.get(0).getEventType());
    assertEquals(UUID.fromString(PIPELINE_RUN), events.get(0).getRun().getRunId());
  }

  @Test
  void eachSinkOnlyKnowsTheRunsThatReachedIt() {
    OpenLineageEventMapper variablesSink =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org").build();
    OpenLineageEventMapper runConfigurationSink =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org").build();
    variablesSink.toRunEvents(pipelineStarted());

    assertEquals(
        RunEvent.EventType.OTHER,
        variablesSink.toRunEvents(unitTest(passed())).get(0).getEventType());
    assertEquals(
        RunEvent.EventType.COMPLETE,
        runConfigurationSink.toRunEvents(unitTest(passed())).get(0).getEventType());
  }
}
