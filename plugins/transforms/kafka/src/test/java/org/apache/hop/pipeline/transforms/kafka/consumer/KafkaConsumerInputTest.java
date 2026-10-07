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
    assertEquals(Long.MAX_VALUE, KafkaConsumerInput.pollTimeoutMs(false, 0L, 0L, 0L, 0L));
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
