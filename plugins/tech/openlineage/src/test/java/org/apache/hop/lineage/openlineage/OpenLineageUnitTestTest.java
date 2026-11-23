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
import org.apache.hop.lineage.model.UnitTestLineagePayload;
import org.junit.jupiter.api.Test;

/** A Hop unit test result as a run facet of the tested transform's run, or its pipeline's. */
class OpenLineageUnitTestTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String PIPELINE_RUN = UUID.randomUUID().toString();
  private static final String TRANSFORM_RUN = UUID.randomUUID().toString();

  private static LineageEvent unitTest(UnitTestLineagePayload payload) {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.TRANSFORM)
            .pipelineName("orders")
            .transformName("Write")
            .logChannelId(TRANSFORM_RUN)
            .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN)
            .build();
    return LineageEvent.of(LineageEventKind.UNIT_TEST, ctx, payload);
  }

  @Test
  void theResultAndTheDatasetsItVouchesForAreOnTheTransformsRun() {
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org").transformJobs(true).build();
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
}
