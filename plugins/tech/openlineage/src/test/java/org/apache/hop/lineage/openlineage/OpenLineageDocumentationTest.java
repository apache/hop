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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage.RunEvent;
import java.util.List;
import java.util.UUID;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.model.DocumentationLineagePayload;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.junit.jupiter.api.Test;

/** The documentation of a pipeline as a run facet of its (made-up) run. */
class OpenLineageDocumentationTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void theDocumentationIsAFacetOfThePipelinesRun() {
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org").transformJobs(true).build();
    String run = UUID.randomUUID().toString();
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .pipelineName("orders")
            .putAttribute("description", "Loads the orders")
            .logChannelId(run)
            .build();

    List<RunEvent> events =
        mapper.toRunEvents(
            LineageEvent.of(
                LineageEventKind.DOCUMENTATION,
                ctx,
                new DocumentationLineagePayload(
                    "Pipeline orders",
                    "* Description : Loads the orders",
                    "sales",
                    List.of("Read", "Write"))));

    assertEquals(1, events.size());
    RunEvent event = events.get(0);
    assertEquals(RunEvent.EventType.OTHER, event.getEventType());
    assertEquals(UUID.fromString(run), event.getRun().getRunId());
    assertEquals("orders", event.getJob().getName());
    // The job keeps its own description: the documentation doesn't replace it
    assertEquals(
        "Loads the orders", event.getJob().getFacets().getDocumentation().getDescription());
    JsonNode facet =
        MAPPER.valueToTree(
            event
                .getRun()
                .getFacets()
                .getAdditionalProperties()
                .get(OpenLineageEventMapper.DOCUMENTATION_FACET));
    assertEquals("Pipeline orders", facet.path("title").asText());
    assertEquals("* Description : Loads the orders", facet.path("markdown").asText());
    assertEquals("sales", facet.path("project").asText());
    assertEquals(List.of("Read", "Write"), MAPPER.convertValue(facet.path("parts"), List.class));
    assertTrue(event.getOutputs() == null || event.getOutputs().isEmpty());
  }
}
