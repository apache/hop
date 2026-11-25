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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

/** When the Kafka dialogs list the topics of the cluster. */
class KafkaDialogHelperTest {

  private static final KafkaOption SECURITY = new KafkaOption("security.protocol", "SASL_SSL");

  @Test
  void theBootstrapServersFieldIsEnough() {
    assertTrue(KafkaDialogHelper.hasBootstrapServers("broker:9092", List.of()));
  }

  /** The servers set only on the Options tab, as the Kafka Consumer runs with them. */
  @Test
  void aBootstrapServersOptionIsEnough() {
    assertTrue(
        KafkaDialogHelper.hasBootstrapServers(
            "",
            List.of(
                SECURITY, new KafkaOption(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "${BROKERS}"))));
  }

  @Test
  void withoutServersTheTopicsAreNotListed() {
    assertFalse(KafkaDialogHelper.hasBootstrapServers("", List.of(SECURITY)));
    assertFalse(
        KafkaDialogHelper.hasBootstrapServers(
            null, List.of(new KafkaOption(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ""))));
  }
}
