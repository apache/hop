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

package org.apache.hop.execution.elastic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hop.core.failure.FailureCategory;
import org.apache.hop.execution.caching.CacheEntry;
import org.junit.jupiter.api.Test;

class ElasticExecutionInfoLocationFailureTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void newIndexStoresTheFailureWithoutIndexingIt() throws Exception {
    JsonNode properties =
        MAPPER
            .readTree(ElasticExecutionInfoLocation.createIndexBody())
            .path("mappings")
            .path("properties");
    JsonNode failure = properties.path("executionState.failure");
    assertEquals("object", failure.path("type").asText());
    assertFalse(failure.path("enabled").asBoolean(true));
  }

  @Test
  void existingIndexGetsTheSameFailureMapping() throws Exception {
    JsonNode failure =
        MAPPER
            .readTree(ElasticExecutionInfoLocation.failureMappingBody())
            .path("properties")
            .path("executionState")
            .path("properties")
            .path("failure");
    assertFalse(failure.path("enabled").asBoolean(true));
  }

  /** An older Hop reading what a newer one wrote: unknown fields are skipped. */
  @Test
  void documentWithUnknownFieldsIsRead() throws Exception {
    String json =
        """
        { "id": "abc",
          "aFieldFromTheFuture": 1,
          "executionState": {
            "name": "orders",
            "failed": true,
            "anotherFieldFromTheFuture": "x",
            "failure": { "category": "CONNECTIVITY", "notKnownYet": true }
          }
        }
        """;
    CacheEntry entry = ElasticExecutionInfoLocation.cacheEntryFromJson(json);
    assertEquals("abc", entry.getId());
    assertTrue(entry.getExecutionState().isFailed());
    assertEquals(
        FailureCategory.CONNECTIVITY, entry.getExecutionState().getFailure().getCategory());
  }
}
