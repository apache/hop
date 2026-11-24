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
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.pipeline.transforms.kafka.consumer.KafkaConsumerInputMeta;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

/** The consumer the Kafka Consumer dialog lists topics with. */
class KafkaFactoryTest {

  private static Map<String, Object> consumerConfig(String bootstrapServersField) {
    Map<String, Object> config = new HashMap<>();
    KafkaFactory factory =
        new KafkaFactory(
            c -> {
              config.putAll(c);
              return null;
            },
            c -> null);
    KafkaConsumerInputMeta meta = new KafkaConsumerInputMeta();
    meta.setDirectBootstrapServers(bootstrapServersField);
    meta.setOptions(
        List.of(
            new KafkaOption(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "option-broker:9092"),
            new KafkaOption("security.protocol", "SASL_SSL"),
            new KafkaOption("sasl.mechanism", "")));
    factory.consumer(meta, s -> s);
    return config;
  }

  @Test
  void theTopicListUsesTheOptions() {
    Map<String, Object> config = consumerConfig("");

    assertEquals("option-broker:9092", config.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("SASL_SSL", config.get("security.protocol"));
    assertFalse(config.containsKey("sasl.mechanism"), "an empty option is left out");
  }

  @Test
  void theBootstrapServersFieldGoesOverTheOption() {
    Map<String, Object> config = consumerConfig("field-broker:9092");

    assertEquals("field-broker:9092", config.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("SASL_SSL", config.get("security.protocol"));
  }
}
