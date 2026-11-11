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

package org.apache.hop.execution.opensearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hop.core.failure.FailureCategory;
import org.apache.hop.execution.caching.CacheEntry;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.jupiter.api.Test;

class OpenSearchExecutionInfoLocationFailureTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void newIndexStoresTheFailureWithoutIndexingIt() throws Exception {
    JsonNode failure =
        MAPPER
            .readTree(OpenSearchExecutionInfoLocation.createIndexBody())
            .path("mappings")
            .path("properties")
            .path("executionState")
            .path("properties")
            .path("failure");
    assertEquals("object", failure.path("type").asText());
    assertFalse(failure.path("enabled").asBoolean(true));
  }

  @Test
  void existingIndexGetsTheSameFailureMapping() throws Exception {
    JsonNode executionState =
        MAPPER
            .readTree(OpenSearchExecutionInfoLocation.failureMappingBody())
            .path("properties")
            .path("executionState");
    // Has to match the nested type of the existing mapping
    assertEquals("nested", executionState.path("type").asText());
    assertFalse(executionState.path("properties").path("failure").path("enabled").asBoolean(true));
  }

  /** An older Hop reading what a newer one wrote: unknown fields are skipped. */
  @Test
  void documentWithUnknownFieldsIsRead() throws Exception {
    String json =
        """
        { "_id": "doc-1",
          "_source": {
            "id": "abc",
            "aFieldFromTheFuture": 1,
            "executionState": {
              "name": "orders",
              "failed": true,
              "failure": { "category": "SCHEMA_MISMATCH", "notKnownYet": true }
            }
          }
        }
        """;
    JSONObject hit = (JSONObject) new JSONParser().parse(json);
    CacheEntry entry = OpenSearchExecutionInfoLocation.cacheEntryFromSearchHit(hit);
    assertEquals("abc", entry.getId());
    assertTrue(entry.getExecutionState().isFailed());
    assertEquals(
        FailureCategory.SCHEMA_MISMATCH, entry.getExecutionState().getFailure().getCategory());
  }
}
