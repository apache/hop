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
package org.apache.hop.pipeline.transforms.kafka.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.DatasetIoLineagePayload;
import org.apache.hop.lineage.model.DatasetIoOperation;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** The topics a Kafka transform reports, named as DataHub's Kafka source and OpenLineage do. */
class KafkaLineageTest {

  @Test
  void theNamespaceIsTheFirstBootstrapServer() {
    assertEquals("kafka://kafka:9092", KafkaLineage.namespace("kafka:9092"));
    assertEquals("kafka://a:9093", KafkaLineage.namespace(" a:9093 , b:9093"));
    assertEquals("kafka://broker:9093", KafkaLineage.namespace("SASL_SSL://broker:9093"));
    assertEquals("kafka://broker:9092", KafkaLineage.namespace("broker"), "Kafka's default port");
    assertNull(KafkaLineage.namespace(""));
    assertNull(KafkaLineage.namespace(null));
  }

  private static ITransform transform() {
    ITransform transform = mock(ITransform.class);
    ILogChannel log = mock(ILogChannel.class);
    when(log.getLogChannelId()).thenReturn("kafka-run");
    when(transform.getLogChannel()).thenReturn(log);
    when(transform.getTransformName()).thenReturn("Kafka producer");
    when(transform.resolve(anyString())).thenAnswer(inv -> inv.getArgument(0));
    PipelineMeta meta = new PipelineMeta();
    meta.setName("orders");
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.getPipelineMeta()).thenReturn(meta);
    when(pipeline.getLogChannelId()).thenReturn("pipeline-run");
    when(transform.getPipeline()).thenReturn(pipeline);
    return transform;
  }

  private static LineageHub hub(boolean enabled) {
    LineageHub hub = mock(LineageHub.class);
    when(hub.isEnabled()).thenReturn(enabled);
    return hub;
  }

  @Test
  void eachTopicIsReportedTheFirstTime() {
    LineageHub hub = hub(true);
    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      KafkaLineage lineage =
          new KafkaLineage(transform(), DatasetIoOperation.WRITE, "kafka:9092,other:9092");
      lineage.topic("orders");
      lineage.topic("orders");
      lineage.topic("payments");
      lineage.topic("");
    }

    ArgumentCaptor<LineageEvent> events = ArgumentCaptor.forClass(LineageEvent.class);
    verify(hub, times(2)).emit(events.capture());
    List<LineageEvent> emitted = events.getAllValues();
    assertEquals(LineageEventKind.DATASET_IO, emitted.get(0).getKind());
    DatasetIoLineagePayload orders = (DatasetIoLineagePayload) emitted.get(0).getPayload();
    assertEquals(DatasetIoOperation.WRITE, orders.getOperation());
    assertEquals("kafka://kafka:9092", orders.getNamespace());
    assertEquals("orders", orders.getName());
    assertEquals("payments", ((DatasetIoLineagePayload) emitted.get(1).getPayload()).getName());
  }

  /** A topic taken from a field can have any number of values: the set stops growing. */
  @Test
  void theTopicsReportedStopAtTheCap() {
    LineageHub hub = hub(true);
    KafkaLineage lineage;
    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      lineage = new KafkaLineage(transform(), DatasetIoOperation.WRITE, "kafka:9092");
      for (int i = 0; i < KafkaLineage.MAX_TOPICS + 50; i++) {
        lineage.topic("topic-" + i);
      }
    }

    assertEquals(KafkaLineage.MAX_TOPICS, lineage.reported().size());
  }

  @Test
  void withLineageOffNothingIsReportedOrKept() {
    LineageHub hub = hub(false);
    KafkaLineage lineage;
    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      lineage = new KafkaLineage(transform(), DatasetIoOperation.READ, "kafka:9092");
      lineage.topic("orders");
    }

    verify(hub, never()).emit(Mockito.any());
    assertTrue(lineage.reported().isEmpty());
  }
}
