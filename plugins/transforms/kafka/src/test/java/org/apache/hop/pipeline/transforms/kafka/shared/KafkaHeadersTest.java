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

package org.apache.hop.pipeline.transforms.kafka.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class KafkaHeadersTest {

  private static Headers headers(String... namesAndValues) {
    RecordHeaders recordHeaders = new RecordHeaders();
    for (int i = 0; i < namesAndValues.length; i += 2) {
      String value = namesAndValues[i + 1];
      recordHeaders.add(
          namesAndValues[i], value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }
    return recordHeaders;
  }

  private static String valueOf(Header header) {
    return header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
  }

  // --- rendering -------------------------------------------------------------------------------

  @Test
  void testNoHeadersRenderEmptyArray() {
    assertEquals("[]", KafkaHeaders.toJson(new RecordHeaders()));
  }

  @Test
  void testNullHeadersRenderEmptyArray() {
    assertEquals("[]", KafkaHeaders.toJson(null));
  }

  @Test
  void testSingleHeaderIsRendered() {
    assertEquals(
        "[{\"name\":\"traceparent\",\"value\":\"00-abc\"}]",
        KafkaHeaders.toJson(headers("traceparent", "00-abc")));
  }

  @Test
  void testRenderingPreservesOrder() {
    assertEquals(
        "[{\"name\":\"b\",\"value\":\"1\"},{\"name\":\"a\",\"value\":\"2\"}]",
        KafkaHeaders.toJson(headers("b", "1", "a", "2")));
  }

  @Test
  void testRenderingKeepsRepeatedNames() {
    // The reason the format is an array rather than an object: Kafka allows the same header name
    // more than once and an object would silently drop one.
    assertEquals(
        "[{\"name\":\"tag\",\"value\":\"one\"},{\"name\":\"tag\",\"value\":\"two\"}]",
        KafkaHeaders.toJson(headers("tag", "one", "tag", "two")));
  }

  @Test
  void testNullHeaderValueRendersJsonNull() {
    assertEquals(
        "[{\"name\":\"tombstone\",\"value\":null}]",
        KafkaHeaders.toJson(headers("tombstone", null)));
  }

  @Test
  void testRenderingEscapesJson() {
    assertEquals(
        "[{\"name\":\"quoted\",\"value\":\"he said \\\"hi\\\"\"}]",
        KafkaHeaders.toJson(headers("quoted", "he said \"hi\"")));
  }

  // --- parsing ---------------------------------------------------------------------------------

  @Test
  void testEmptyArrayParsesToNoHeaders() throws HopException {
    assertTrue(KafkaHeaders.fromJson("[]").isEmpty());
  }

  @Test
  void testParsingKeepsOrderAndRepeatedNames() throws HopException {
    List<Header> parsed =
        KafkaHeaders.fromJson(
            "[{\"name\":\"tag\",\"value\":\"one\"},{\"name\":\"a\",\"value\":\"x\"},"
                + "{\"name\":\"tag\",\"value\":\"two\"}]");

    assertEquals(3, parsed.size());
    assertEquals("tag", parsed.get(0).key());
    assertEquals("one", valueOf(parsed.get(0)));
    assertEquals("a", parsed.get(1).key());
    assertEquals("tag", parsed.get(2).key());
    assertEquals("two", valueOf(parsed.get(2)));
  }

  @Test
  void testJsonNullParsesToNullValue() throws HopException {
    // A null header value is distinct from an empty one on the wire.
    assertNull(KafkaHeaders.fromJson("[{\"name\":\"tombstone\",\"value\":null}]").get(0).value());
  }

  @Test
  void testMissingValueParsesToNullValue() throws HopException {
    assertNull(KafkaHeaders.fromJson("[{\"name\":\"bare\"}]").get(0).value());
  }

  @Test
  void testNonJsonIsRejected() {
    assertThrows(HopException.class, () -> KafkaHeaders.fromJson("not json at all"));
  }

  @Test
  void testJsonObjectIsRejected() {
    // An object cannot express ordering or repeats, so it is rejected even though it is the more
    // obvious shape to hand-write.
    assertThrows(HopException.class, () -> KafkaHeaders.fromJson("{\"traceparent\":\"00-abc\"}"));
  }

  @Test
  void testEntryWithoutNameIsRejected() {
    assertThrows(HopException.class, () -> KafkaHeaders.fromJson("[{\"value\":\"orphan\"}]"));
  }

  // --- round trip ------------------------------------------------------------------------------

  @Test
  void testRoundTripPreservesEverything() throws HopException {
    // The point of keeping both directions together: what the Kafka Consumer writes into a field
    // can be handed straight back to the Kafka Producer and yield the same headers.
    Headers original =
        headers("tag", "one", "tag", "two", "city", "München", "tombstone", null, "empty", "");

    List<Header> parsed = KafkaHeaders.fromJson(KafkaHeaders.toJson(original));

    assertEquals(5, parsed.size());
    assertEquals("tag", parsed.get(0).key());
    assertEquals("one", valueOf(parsed.get(0)));
    assertEquals("two", valueOf(parsed.get(1)));
    assertEquals("München", valueOf(parsed.get(2)));
    assertNull(parsed.get(3).value(), "a null value must not become an empty one");
    assertEquals("", valueOf(parsed.get(4)), "an empty value must not become null");
  }
}
