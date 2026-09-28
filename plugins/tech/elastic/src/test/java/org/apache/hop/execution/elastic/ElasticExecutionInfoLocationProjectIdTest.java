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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ElasticExecutionInfoLocationProjectIdTest {

  @Test
  void listQueryIsMatchAllWhenProjectIdIsEmpty() {
    String query = ElasticExecutionInfoLocation.listQuery(null, 50);
    assertTrue(query.contains("match_all"));
    assertFalse(query.contains("projectId"));

    String unlimited = ElasticExecutionInfoLocation.listQuery("", 0);
    assertTrue(unlimited.contains("match_all"));
    assertFalse(unlimited.contains("\"size\""));
  }

  @Test
  void listQueryFiltersByProjectIdAndKeepsLegacyDocuments() {
    String query = ElasticExecutionInfoLocation.listQuery("sales", 50);
    assertTrue(query.contains("\"term\""));
    assertTrue(query.contains("\"projectId\": \"sales\""));
    assertTrue(query.contains("must_not"));
    assertTrue(query.contains("exists"));
    assertFalse(query.contains("match_all"));
  }

  @Test
  void listQueryEscapesQuotesInTheProjectId() {
    String query = ElasticExecutionInfoLocation.listQuery("sa\"les", 10);
    assertTrue(query.contains("sa\\\"les"));
  }

  @Test
  void createIndexDeclaresProjectIdAsKeyword() {
    String body = ElasticExecutionInfoLocation.createIndexBody();
    assertTrue(body.contains("\"projectId\": { \"type\": \"keyword\" }"));
    assertTrue(ElasticExecutionInfoLocation.projectIdMappingBody().contains("keyword"));
  }

  @Test
  void alreadyExistsIsOnlyTheElastic400() {
    assertTrue(
        ElasticExecutionInfoLocation.isIndexAlreadyExists(
            400, "{\"error\":{\"type\":\"resource_already_exists_exception\"}}"));
    assertFalse(ElasticExecutionInfoLocation.isIndexAlreadyExists(400, "other"));
    assertFalse(
        ElasticExecutionInfoLocation.isIndexAlreadyExists(
            200, "resource_already_exists_exception"));
  }
}
