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

package org.apache.hop.pipeline.transforms.kafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;

class KafkaConsumerInputTest {

  @Test
  void maxConsumeDurationReachedWhenElapsed() {
    assertFalse(KafkaConsumerInput.maxConsumeDurationReached(1_000L, 0L, 0L));
    assertFalse(KafkaConsumerInput.maxConsumeDurationReached(1_000L, -5L, 0L));
    assertFalse(KafkaConsumerInput.maxConsumeDurationReached(999L, 1_000L, 0L));
    assertTrue(KafkaConsumerInput.maxConsumeDurationReached(1_000L, 1_000L, 0L));
    assertTrue(KafkaConsumerInput.maxConsumeDurationReached(1_500L, 1_000L, 0L));
    assertFalse(KafkaConsumerInput.maxConsumeDurationReached(1_500L, 1_000L, 600L));
  }

  @Test
  void pollTimeoutUsesBatchDurationWhenNoDeadline() {
    assertEquals(2_000L, KafkaConsumerInput.pollTimeoutMs(false, 2_000L, 0L, 0L, 0L));
    // A duration of 0 used to wait forever inside one poll. Stop has to be rechecked.
    assertEquals(
        KafkaConsumerInput.DEFAULT_POLL_MS,
        KafkaConsumerInput.pollTimeoutMs(false, 0L, 0L, 0L, 0L));
  }

  @Test
  void closeConsumerDrainsPendingWakeupBeforeClose() {
    AtomicBoolean wakeupPending = new AtomicBoolean(true);
    AtomicBoolean closed = new AtomicBoolean(false);
    @SuppressWarnings("unchecked")
    Consumer<Object, Object> consumer = mock(Consumer.class);
    when(consumer.poll(any()))
        .thenAnswer(
            invocation -> {
              if (wakeupPending.getAndSet(false)) {
                throw new WakeupException();
              }
              return ConsumerRecords.empty();
            });
    doAnswer(
            invocation -> {
              if (wakeupPending.get()) {
                throw new WakeupException();
              }
              Duration timeout = invocation.getArgument(0);
              assertEquals(KafkaConsumerInput.CONSUMER_CLOSE_TIMEOUT, timeout);
              closed.set(true);
              return null;
            })
        .when(consumer)
        .close(any());

    KafkaConsumerInput.closeConsumer(consumer);

    assertTrue(closed.get());
    verify(consumer, never()).wakeup();
    verify(consumer).close(KafkaConsumerInput.CONSUMER_CLOSE_TIMEOUT);
  }

  @Test
  void stopRunningToleratesNullConsumerAndStopsSubPipeline() throws Exception {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("kafka");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(transformMeta);
    KafkaConsumerInputData data = new KafkaConsumerInputData();
    KafkaConsumerInput transform =
        new KafkaConsumerInput(
            transformMeta,
            new KafkaConsumerInputMeta(),
            data,
            0,
            pipelineMeta,
            new LocalPipelineEngine());

    transform.stopRunning();

    @SuppressWarnings("unchecked")
    Consumer<Object, Object> consumer = mock(Consumer.class);
    data.consumer = consumer;
    Pipeline subPipeline = mock(Pipeline.class);
    SingleThreadedPipelineExecutor executor = mock(SingleThreadedPipelineExecutor.class);
    when(executor.getPipeline()).thenReturn(subPipeline);
    data.executor = executor;

    transform.stopRunning();

    assertTrue(data.isKafkaConsumerClosing);
    verify(consumer).wakeup();
    verify(subPipeline).stopAll();
    verify(consumer, never()).close(any());
  }

  @Test
  void pollTimeoutUsesShortPollWhenStopWhenIdle() {
    assertEquals(100L, KafkaConsumerInput.pollTimeoutMs(true, 2_000L, 0L, 0L, 0L));
  }

  @Test
  void batchLogReportsBatchSizeAndCumulativeInput() {
    String message = KafkaConsumerInput.batchLogMessage(12, 40L);
    assertTrue(message.contains("12"));
    assertTrue(message.contains("40"));
    assertFalse(message.toLowerCase().contains("finished processing"));
    assertFalse(message.toLowerCase().contains("rows read"));
  }

  @Test
  void pollTimeoutIsCappedToRemainingConsumeDuration() {
    // A deadline uses a short poll so empty topics re-check the clock instead of blocking in
    // poll() for batchDuration (or forever when batchDuration is 0).
    assertEquals(100L, KafkaConsumerInput.pollTimeoutMs(false, 2_000L, 1_000L, 0L, 0L));
    assertEquals(100L, KafkaConsumerInput.pollTimeoutMs(false, 0L, 10_000L, 0L, 0L));
    assertEquals(50L, KafkaConsumerInput.pollTimeoutMs(false, 2_000L, 1_000L, 0L, 950L));
    assertEquals(0L, KafkaConsumerInput.pollTimeoutMs(false, 2_000L, 1_000L, 0L, 1_000L));
    assertEquals(100L, KafkaConsumerInput.pollTimeoutMs(true, 2_000L, 5_000L, 0L, 0L));
    assertEquals(50L, KafkaConsumerInput.pollTimeoutMs(true, 2_000L, 1_000L, 0L, 950L));
  }
}
