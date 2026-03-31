/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.kafka.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class KafkaProducerOutputMetaTest {
  @Test
  void testSerializationRoundTrip() throws Exception {
    KafkaProducerOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/kafka-producer-output.xml", KafkaProducerOutputMeta.class);

    assertEquals("${KAFKA_SERVER}", meta.getDirectBootstrapServers());
    assertEquals("${KAFKA_TOPIC}", meta.getTopic());
    assertEquals("Hop", meta.getClientId());
    assertEquals("id", meta.getKeyField());
    assertEquals("json", meta.getMessageField());
    assertEquals(6, meta.getOptions().size());
    assertEquals("compression.type", meta.getOptions().getFirst().getProperty());
    assertEquals("none", meta.getOptions().getFirst().getValue());
    assertEquals("ssl.truststore.password", meta.getOptions().getLast().getProperty());
  }
}
