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

package org.apache.hop.pipeline.transforms.jsoninput.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link RowOutputConverter}. */
class RowOutputConverterTest {

  /**
   * Regression test for Apache Hop #7990. A JSON {@code null} value is parsed by Jackson into a
   * {@link NullNode}, a non-null Java reference whose {@code toString()} returns the string literal
   * "null". Before the fix, this fell through to the generic {@code jo.toString()} branch and
   * downstream String output fields contained "null" instead of a Hop null.
   */
  @Test
  void getStringValue_nullNode_returnsNull() {
    assertNull(RowOutputConverter.getStringValue(NullNode.getInstance()));
  }

  @Test
  void getStringValue_missingNode_returnsNull() {
    assertNull(RowOutputConverter.getStringValue(MissingNode.getInstance()));
  }

  @Test
  void getStringValue_javaNull_returnsNull() {
    assertNull(RowOutputConverter.getStringValue(null));
  }

  @Test
  void getStringValue_textNode_returnsUnquotedText() {
    // Ensures the pre-existing TextNode branch still returns the unquoted string
    // rather than the JSON-encoded "\"foo\"" that toString() would produce.
    assertEquals("foo", RowOutputConverter.getStringValue(TextNode.valueOf("foo")));
  }

  @Test
  void getStringValue_plainString_isPassedThrough() {
    assertEquals("plain", RowOutputConverter.getStringValue("plain"));
  }

  @Test
  void getStringValue_intNode_fallsThroughToToString() {
    // Non-Text, non-null Jackson nodes intentionally fall through to toString().
    assertEquals("42", RowOutputConverter.getStringValue(IntNode.valueOf(42)));
  }

  @Test
  void getStringValue_map_isSerializedAsJson() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("a", 1);
    map.put("b", "two");
    assertEquals("{\"a\":1,\"b\":\"two\"}", RowOutputConverter.getStringValue(map));
  }
}
