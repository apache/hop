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
package org.apache.hop.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hop.core.failure.ExecutionFailure;
import org.apache.hop.core.failure.FailurePhase;
import org.apache.hop.execution.caching.CacheEntry;
import org.junit.jupiter.api.Test;

class FailureJsonCompatibilityTest {

  /**
   * The Elastic and OpenSearch locations of 2.20 and earlier read their documents with a strict
   * ObjectMapper: a property they don't know fails the whole document.
   */
  private final ObjectMapper strictMapper = new ObjectMapper();

  @Test
  void omitsTheFailureOfAnExecutionThatDidNotFail() throws Exception {
    ExecutionState state = new ExecutionState();
    state.setName("pipeline");
    assertFalse(strictMapper.writeValueAsString(state).contains("\"failure\""));

    ExecutionState transformState = new ExecutionState();
    transformState.setId("transform-id");
    transformState.setName("transform");

    CacheEntry entry = new CacheEntry();
    entry.setId("id");
    entry.setName("pipeline");
    entry.setExecutionState(state);
    entry.addChildExecutionState(transformState);
    String json = strictMapper.writeValueAsString(entry);
    assertFalse(json.contains("\"failure\""));

    CacheEntry read = strictMapper.readValue(json, CacheEntry.class);
    assertNull(read.getExecutionState().getFailure());
  }

  @Test
  void writesTheFailureOfAnExecutionThatFailed() throws Exception {
    ExecutionFailure failure = new ExecutionFailure();
    failure.setPhase(FailurePhase.INIT);
    ExecutionState state = new ExecutionState();
    state.setName("pipeline");
    state.setFailed(true);
    state.setFailure(failure);

    String json = strictMapper.writeValueAsString(state);
    assertTrue(json.contains("\"failure\""));
    assertEquals(
        FailurePhase.INIT,
        strictMapper.readValue(json, ExecutionState.class).getFailure().getPhase());
  }
}
