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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hop.execution.caching.CacheEntry;
import org.junit.jupiter.api.Test;

class ProjectIdJsonCompatibilityTest {

  /** 2.19 execution locations read and write with a strict ObjectMapper. */
  private final ObjectMapper strictMapper = new ObjectMapper();

  @Test
  void omitsProjectIdWhenEmpty() throws Exception {
    Execution execution = new Execution();
    execution.setName("pipeline");
    String json = strictMapper.writeValueAsString(execution);
    assertFalse(json.contains("projectId"));
    assertNull(strictMapper.readValue(json, Execution.class).getProjectId());

    CacheEntry entry = new CacheEntry();
    entry.setId("id");
    entry.setName("pipeline");
    entry.setExecution(execution);
    String entryJson = strictMapper.writeValueAsString(entry);
    assertFalse(entryJson.contains("projectId"));
    assertNull(strictMapper.readValue(entryJson, CacheEntry.class).getProjectId());
  }

  @Test
  void writesProjectIdWhenSet() throws Exception {
    Execution execution = new Execution();
    execution.setName("pipeline");
    execution.setProjectId("sales");
    String json = strictMapper.writeValueAsString(execution);
    assertTrue(json.contains("\"projectId\":\"sales\""));

    CacheEntry entry = new CacheEntry();
    entry.setId("id");
    entry.setName("pipeline");
    entry.setExecution(execution);
    entry.setProjectId("sales");
    String entryJson = strictMapper.writeValueAsString(entry);
    assertTrue(entryJson.contains("\"projectId\":\"sales\""));
    CacheEntry read = strictMapper.readValue(entryJson, CacheEntry.class);
    assertEquals("sales", read.getProjectId());
    assertEquals("sales", read.getExecution().getProjectId());
  }

  @Test
  void strictMapperWithoutThePropertyRejectsATaggedDocument() {
    assertThrows(
        JsonMappingException.class,
        () -> strictMapper.readValue("{\"projectId\":\"sales\"}", UntaggedExecution.class));
  }

  /** Stand-in for the 2.19 Execution class, which has no projectId property. */
  static class UntaggedExecution {
    public String name;
  }
}
