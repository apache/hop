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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class OpenSearchExecutionInfoLocationProjectIdTest {

  @Test
  void whereClauseIsEmptyWhenProjectIdIsEmpty() {
    assertEquals("", OpenSearchExecutionInfoLocation.projectIdWhereClause(null));
    assertEquals("", OpenSearchExecutionInfoLocation.projectIdWhereClause(""));
  }

  @Test
  void whereClauseMatchesTheProjectAndLegacyRows() {
    assertEquals(
        "(projectId = 'sales' OR projectId IS NULL OR projectId = '')",
        OpenSearchExecutionInfoLocation.projectIdWhereClause("sales"));
  }

  @Test
  void whereClauseEscapesQuotes() {
    assertEquals(
        "(projectId = 'o''brien' OR projectId IS NULL OR projectId = '')",
        OpenSearchExecutionInfoLocation.projectIdWhereClause("o'brien"));
  }

  @Test
  void createIndexDeclaresRootProjectIdAsKeyword() {
    String body = OpenSearchExecutionInfoLocation.createIndexBody();
    assertTrue(body.contains("\"projectId\": { \"type\": \"keyword\" }"));
    assertTrue(OpenSearchExecutionInfoLocation.projectIdMappingBody().contains("keyword"));
  }
}
