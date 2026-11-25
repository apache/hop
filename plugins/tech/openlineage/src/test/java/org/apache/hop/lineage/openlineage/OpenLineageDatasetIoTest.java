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

import io.openlineage.client.OpenLineage.RunEvent;
import java.util.List;
import java.util.UUID;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.model.DatasetIoLineagePayload;
import org.apache.hop.lineage.model.DatasetIoOperation;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.junit.jupiter.api.Test;

/** A dataset that is neither a table nor a file, such as a Kafka topic. */
class OpenLineageDatasetIoTest {
  private static final String PIPELINE_RUN = UUID.randomUUID().toString();
  private static final String TRANSFORM_RUN = UUID.randomUUID().toString();
  private static final String KAFKA = "kafka://kafka:9092";

  private final OpenLineageEventMapper mapper =
      OpenLineageEventMapper.builder("hop", "https://hop.apache.org").build();

  private static LineageEvent pipeline(RunLifecyclePhase phase) {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .pipelineName("orders")
            .logChannelId(PIPELINE_RUN)
            .build();
    return LineageEvent.of(
        LineageEventKind.RUN_LIFECYCLE, ctx, new RunLifecycleLineagePayload(phase, null));
  }

  private static LineageEvent topic(DatasetIoOperation operation, String topic) {
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.TRANSFORM)
            .pipelineName("orders")
            .transformName("Kafka")
            .logChannelId(TRANSFORM_RUN)
            .putAttribute(OpenLineageEventMapper.ATTR_PIPELINE_LOG_CHANNEL_ID, PIPELINE_RUN)
            .build();
    return LineageEvent.of(
        LineageEventKind.DATASET_IO,
        ctx,
        new DatasetIoLineagePayload(operation, KAFKA, topic, true, null));
  }

  /** A consumer runs until it is stopped: what it reads is sent while the run goes on. */
  @Test
  void aTopicIsSentWhileTheRunGoesOn() {
    mapper.toRunEvents(pipeline(RunLifecyclePhase.STARTED));

    List<RunEvent> events = mapper.toRunEvents(topic(DatasetIoOperation.READ, "orders"));

    assertEquals(1, events.size());
    RunEvent running = events.get(0);
    assertEquals(RunEvent.EventType.RUNNING, running.getEventType());
    assertEquals(UUID.fromString(PIPELINE_RUN), running.getRun().getRunId());
    assertEquals("orders", running.getJob().getName());
    assertEquals(1, running.getInputs().size());
    assertEquals(KAFKA, running.getInputs().get(0).getNamespace());
    assertEquals("orders", running.getInputs().get(0).getName());
    assertNull(running.getOutputs());
  }

  @Test
  void theRunsEndCarriesItsTopicsToo() {
    mapper.toRunEvents(pipeline(RunLifecyclePhase.STARTED));
    mapper.toRunEvents(topic(DatasetIoOperation.WRITE, "orders-enriched"));

    List<RunEvent> end = mapper.toRunEvents(pipeline(RunLifecyclePhase.FINISHED));

    RunEvent complete = end.get(end.size() - 1);
    assertEquals(RunEvent.EventType.COMPLETE, complete.getEventType());
    assertEquals(1, complete.getOutputs().size());
    assertEquals(KAFKA, complete.getOutputs().get(0).getNamespace());
    assertEquals("orders-enriched", complete.getOutputs().get(0).getName());
  }

  @Test
  void aTopicSeenAfterTheRunEndedIsSentOnItsOwn() {
    mapper.toRunEvents(pipeline(RunLifecyclePhase.STARTED));
    mapper.toRunEvents(pipeline(RunLifecyclePhase.FINISHED));

    List<RunEvent> events = mapper.toRunEvents(topic(DatasetIoOperation.READ, "orders"));

    assertEquals(1, events.size());
    assertEquals(RunEvent.EventType.OTHER, events.get(0).getEventType());
    assertEquals("orders", events.get(0).getInputs().get(0).getName());
  }
}
