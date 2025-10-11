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

package org.apache.hop.core.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class JsonUtilTest {

  // -------------------------
  // Singletons
  // -------------------------

  @Test
  public void testSingletons() {
    assertNotNull(JsonUtil.jsonReader());
    assertNotNull(JsonUtil.jsonMapper());
    // same instance each call
    assertSame(JsonUtil.jsonReader(), JsonUtil.jsonReader());
    assertSame(JsonUtil.jsonMapper(), JsonUtil.jsonMapper());
  }

  // -------------------------
  // Parse
  // -------------------------

  @Test
  public void testParseCharSequence() throws Exception {
    JsonNode n = JsonUtil.parse("{a:1, b:\"x\"}");
    assertEquals(1, n.get("a").intValue());
    assertEquals("x", n.get("b").textValue());
  }

  @Test
  public void testParseBytes() throws Exception {
    byte[] bytes = "{\"k\":true}".getBytes(StandardCharsets.UTF_8);
    JsonNode n = JsonUtil.parse(bytes);
    assertTrue(n.get("k").booleanValue());
  }

  @Test
  public void testParseStream() throws Exception {
    InputStream in = new ByteArrayInputStream("{\"n\":42}".getBytes(StandardCharsets.UTF_8));
    JsonNode n = JsonUtil.parse(in);
    assertEquals(42, n.get("n").intValue());
  }

  @Test
  public void testParseNulls() throws Exception {
    assertNull(JsonUtil.parse((CharSequence) null));
    assertNull(JsonUtil.parse((byte[]) null));
    assertNull(JsonUtil.parse((InputStream) null));
  }

  @Test
  public void testParseInvalid() {
    assertThrows(JsonProcessingException.class, () -> JsonUtil.parse("{oops"));
  }

  @Test
  public void testParseTextValue() throws Exception {
    var bytes = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
    assertEquals(1, JsonUtil.parseTextValue(bytes).get("a").intValue());

    assertEquals(2, JsonUtil.parseTextValue("{\"b\":2}").get("b").intValue());

    InputStream in = new ByteArrayInputStream("{\"c\":3}".getBytes(StandardCharsets.UTF_8));
    assertEquals(3, JsonUtil.parseTextValue(in).get("c").intValue());

    // generic object
    Object custom =
        new Object() {
          @Override
          public String toString() {
            return "{\"d\":4}";
          }
        };
    assertEquals(4, JsonUtil.parseTextValue(custom).get("d").intValue());

    assertNull(JsonUtil.parseTextValue(null));
  }

  // -------------------------
  // Mapping
  // -------------------------

  @Test
  public void testMapObjectToJson() {
    assertNull(JsonUtil.mapObjectToJson(null));

    ObjectNode node = JsonUtil.jsonMapper().createObjectNode().put("x", 1);
    assertSame(node, JsonUtil.mapObjectToJson(node));

    JsonNode mappedMap = JsonUtil.mapObjectToJson(Map.of("a", 1, "b", "x"));
    assertEquals(1, mappedMap.get("a").intValue());
    assertEquals("x", mappedMap.get("b").textValue());

    JsonNode mappedList = JsonUtil.mapObjectToJson(List.of(1, 2, 3));
    assertEquals(3, mappedList.size());
    assertEquals(2, mappedList.get(1).intValue());
  }

  @Test
  public void testMapJsonToString() throws Exception {
    ObjectNode o = JsonUtil.jsonMapper().createObjectNode().put("a", 1).put("b", "x");

    String compact = JsonUtil.mapJsonToString(o, false);
    assertEquals("{\"a\":1,\"b\":\"x\"}", compact);

    String pretty = JsonUtil.mapJsonToString(o, true);
    assertTrue(pretty.contains("\n"));
    assertTrue(pretty.contains("  \"a\""));

    assertNull(JsonUtil.mapJsonToString(null, true));
  }

  @Test
  public void testMapJsonToBytes() throws Exception {
    ObjectNode o = JsonUtil.jsonMapper().createObjectNode().put("a", "u");
    byte[] bytes = JsonUtil.mapJsonToBytes(o);
    assertNotNull(bytes);

    JsonNode n = JsonUtil.parse(new ByteArrayInputStream(bytes));
    assertEquals("u", n.get("a").textValue());

    assertNull(JsonUtil.mapJsonToBytes(null));
  }
}
