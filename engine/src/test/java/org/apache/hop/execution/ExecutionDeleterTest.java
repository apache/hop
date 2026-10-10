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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExecutionDeleterTest {

  @Test
  void deleteAllDrainsAListThatReturnsThreeIdsAtATime() throws Exception {
    Map<String, Execution> store = new LinkedHashMap<>();
    for (int i = 0; i < 5; i++) {
      store.put("id-" + i, execution("id-" + i, null, new Date(i)));
    }
    IExecutionInfoLocation location = location(store, 3);

    int deleted = ExecutionDeleter.delete(location, null, null);

    assertEquals(5, deleted);
    assertTrue(store.isEmpty());
  }

  @Test
  void deleteOlderThanKeepsExecutionsThatStartedLater() throws Exception {
    Map<String, Execution> store = new LinkedHashMap<>();
    store.put("old", execution("old", null, new Date(1_000L)));
    store.put("missing", execution("missing", null, null));
    store.put("recent", execution("recent", null, new Date(50_000L)));
    IExecutionInfoLocation location = location(store, 3);

    int deleted = ExecutionDeleter.delete(location, new Date(10_000L), null);

    assertEquals(2, deleted);
    assertFalse(store.containsKey("old"));
    assertFalse(store.containsKey("missing"));
    assertTrue(store.containsKey("recent"));
  }

  private static IExecutionInfoLocation location(Map<String, Execution> store, int pageCap)
      throws Exception {
    IExecutionInfoLocation location = mock(IExecutionInfoLocation.class);
    when(location.getExecutionIds(anyBoolean(), anyInt()))
        .thenAnswer(
            invocation -> {
              boolean includeChildren = invocation.getArgument(0);
              int limit = invocation.getArgument(1);
              int cap = limit <= 0 ? Integer.MAX_VALUE : pageCap;
              List<String> ids = new ArrayList<>();
              for (Execution execution : store.values()) {
                if (!includeChildren && execution.getParentId() != null) {
                  continue;
                }
                ids.add(execution.getId());
                if (ids.size() >= cap) {
                  break;
                }
              }
              return ids;
            });
    when(location.getExecution(anyString()))
        .thenAnswer(invocation -> store.get(invocation.getArgument(0, String.class)));
    when(location.deleteExecution(anyString()))
        .thenAnswer(invocation -> store.remove(invocation.getArgument(0, String.class)) != null);
    return location;
  }

  private static Execution execution(String id, String parentId, Date start) {
    Execution execution = new Execution();
    execution.setId(id);
    execution.setName(id);
    execution.setParentId(parentId);
    execution.setExecutionStartDate(start);
    return execution;
  }
}
