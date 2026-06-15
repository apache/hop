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

package org.apache.hop.pipeline.transforms.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import org.junit.jupiter.api.Test;

class PagingBodyMergeTest {

  @Test
  void mergeFormUrlEncodedAddsCursorAndPreservesExistingFields() {
    LinkedHashMap<String, String> paging = new LinkedHashMap<>();
    paging.put("limit", "5");
    paging.put("cursor", "abc123");

    String merged =
        PagingBodyMerge.merge(
            "token=xoxb-test&types=public_channel", paging, "application/x-www-form-urlencoded");

    assertTrue(merged.contains("token=xoxb-test"));
    assertTrue(merged.contains("types=public_channel"));
    assertTrue(merged.contains("limit=5"));
    assertTrue(merged.contains("cursor=abc123"));
  }

  @Test
  void mergeJsonAddsNumericLimitAndCursor() {
    LinkedHashMap<String, String> paging = new LinkedHashMap<>();
    paging.put("MaxResults", "25");
    paging.put("NextToken", "token-abc");

    String merged = PagingBodyMerge.merge("{\"Action\":\"ListUsers\"}", paging, "application/json");

    assertTrue(merged.contains("\"Action\":\"ListUsers\""));
    assertTrue(merged.contains("\"MaxResults\":25"));
    assertTrue(merged.contains("\"NextToken\":\"token-abc\""));
  }

  @Test
  void parseFormUrlEncodedRoundTrip() {
    LinkedHashMap<String, String> parsed = PagingBodyMerge.parseFormUrlEncoded("a=1&b=two%262");
    assertEquals("1", parsed.get("a"));
    assertEquals("two&2", parsed.get("b"));
  }
}
