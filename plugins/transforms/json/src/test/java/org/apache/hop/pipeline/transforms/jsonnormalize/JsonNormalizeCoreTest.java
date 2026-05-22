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

package org.apache.hop.pipeline.transforms.jsonnormalize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class JsonNormalizeCoreTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void extractRecords_storeBook() throws Exception {
    String json = """
        {"store":{"book":[{"category":"fiction","price":8.99}]}}
        """;
    List<JsonNode> rows =
        JsonNormalizeRecords.extractRecordsFromStream(
            new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), "$.store.book[*]");
    assertEquals(1, rows.size());
    assertEquals("fiction", rows.get(0).path("category").asText());
  }

  @Test
  void flatten_nested() throws Exception {
    JsonNode rec = MAPPER.readTree("{\"a\":1,\"b\":{\"c\":2}}");
    Map<String, JsonNode> m =
        JsonNodeFlattener.flatten(
            rec,
            ".",
            -1,
            JsonNodeFlattener.CODE_ARRAY_STRINGIFY,
            JsonNodeFlattener.CODE_BEYOND_STRINGIFY);
    assertEquals(1, m.get("a").asInt());
    assertEquals(2, m.get("b.c").asInt());
  }

  @Test
  void flatten_maxDepth_zero_stringifies_nested() throws Exception {
    JsonNode rec = MAPPER.readTree("{\"b\":{\"c\":2}}");
    Map<String, JsonNode> m =
        JsonNodeFlattener.flatten(
            rec,
            ".",
            0,
            JsonNodeFlattener.CODE_ARRAY_STRINGIFY,
            JsonNodeFlattener.CODE_BEYOND_STRINGIFY);
    assertTrue(m.containsKey("b"));
    assertTrue(m.get("b").isObject());
  }

  /**
   * Hop's {@link org.apache.hop.core.variables.Variables#resolve(String)} applies hex decoding to
   * {@code $[...]} segments, breaking JsonPath such as {@code $[*]}. {@link JsonNormalizeInput}
   * uses {@code resolveJsonPath} (Windows + Unix substitution only) for record paths and field
   * paths.
   */
  @Test
  void environmentSubstituteManglesDollarBracketJsonPath() throws HopException {
    HopEnvironment.init();
    Variables variables = new Variables();
    assertNotEquals("$[*]", variables.resolve("$[*]"));
  }

  @Test
  void array_error_mode_throws() throws Exception {
    JsonNode rec = MAPPER.readTree("{\"tags\":[\"x\",\"y\"]}");
    assertThrows(
        HopException.class,
        () ->
            JsonNodeFlattener.flatten(
                rec,
                ".",
                -1,
                JsonNodeFlattener.CODE_ARRAY_ERROR,
                JsonNodeFlattener.CODE_BEYOND_STRINGIFY));
  }
}
