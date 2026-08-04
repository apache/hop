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

package org.apache.hop.pipeline.transforms.kafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class KafkaConsumerHeadersTest {

  private static Headers headers(String... namesAndValues) {
    RecordHeaders recordHeaders = new RecordHeaders();
    for (int i = 0; i < namesAndValues.length; i += 2) {
      String value = namesAndValues[i + 1];
      recordHeaders.add(
          namesAndValues[i], value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }
    return recordHeaders;
  }

  @Test
  void testNoHeadersRendersEmptyArray() {
    assertEquals("[]", KafkaConsumerInput.headersAsJson(new RecordHeaders()));
  }

  @Test
  void testNullHeadersRendersEmptyArray() {
    // A hand-built ConsumerRecord in a test, or a broker that sends none, can leave this null.
    assertEquals("[]", KafkaConsumerInput.headersAsJson(null));
  }

  @Test
  void testSingleHeader() {
    assertEquals(
        "[{\"name\":\"traceparent\",\"value\":\"00-abc\"}]",
        KafkaConsumerInput.headersAsJson(headers("traceparent", "00-abc")));
  }

  @Test
  void testHeaderOrderIsPreserved() {
    // Kafka headers are an ordered list, so the rendering has to keep that order rather than
    // sorting or de-duplicating.
    assertEquals(
        "[{\"name\":\"b\",\"value\":\"1\"},{\"name\":\"a\",\"value\":\"2\"}]",
        KafkaConsumerInput.headersAsJson(headers("b", "1", "a", "2")));
  }

  @Test
  void testRepeatedHeaderNamesAreBothKept() {
    // The reason for an array of objects rather than a JSON object: Kafka permits the same header
    // name more than once, and a JSON object would silently drop one of them.
    assertEquals(
        "[{\"name\":\"tag\",\"value\":\"one\"},{\"name\":\"tag\",\"value\":\"two\"}]",
        KafkaConsumerInput.headersAsJson(headers("tag", "one", "tag", "two")));
  }

  @Test
  void testNullHeaderValueRendersJsonNull() {
    assertEquals(
        "[{\"name\":\"tombstone\",\"value\":null}]",
        KafkaConsumerInput.headersAsJson(headers("tombstone", null)));
  }

  @Test
  void testValuesAreJsonEscaped() {
    assertEquals(
        "[{\"name\":\"quoted\",\"value\":\"he said \\\"hi\\\"\"}]",
        KafkaConsumerInput.headersAsJson(headers("quoted", "he said \"hi\"")));
  }

  @Test
  void testNonAsciiValuesRoundTripAsUtf8() {
    assertEquals(
        "[{\"name\":\"city\",\"value\":\"München\"}]",
        KafkaConsumerInput.headersAsJson(headers("city", "München")));
  }

  @Test
  void testHeadersFieldIsOffByDefault() {
    // Pipelines saved before headers existed must produce exactly the row they did before, so the
    // field ships with an empty output name and getRowMeta leaves it off the row.
    KafkaConsumerInputMeta meta = new KafkaConsumerInputMeta();
    assertEquals("", meta.getHeadersField().getOutputName());
  }
}
