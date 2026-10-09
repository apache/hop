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

package org.apache.hop.ui.hopgui.perspective.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.junit.jupiter.api.Test;

class ExecutionDeleteSelectionTest {

  @Test
  void aSelectedLocationCoversItsExecutionsAndKeepsTheOtherLocation() {
    ExecutionInfoLocation locationA = location("A");
    ExecutionInfoLocation locationB = location("B");
    Execution inA = execution("a1");
    Execution inB = execution("b1");

    List<ExecutionDeleteSelection.Target> targets =
        ExecutionDeleteSelection.targets(
            List.of(
                new ExecutionDeleteSelection.Item(locationA, null),
                new ExecutionDeleteSelection.Item(locationA, inA),
                new ExecutionDeleteSelection.Item(locationB, inB)));

    assertEquals(2, targets.size());
    assertTrue(targets.get(0).locationWipe());
    assertEquals("A", targets.get(0).location().getName());
    assertNull(targets.get(0).execution());
    assertEquals("b1", targets.get(1).execution().getId());
    assertEquals("B", targets.get(1).location().getName());
  }

  @Test
  void theSameLocationIsWipedOnce() {
    ExecutionInfoLocation location = location("A");
    List<ExecutionDeleteSelection.Target> targets =
        ExecutionDeleteSelection.targets(
            List.of(
                new ExecutionDeleteSelection.Item(location, null),
                new ExecutionDeleteSelection.Item(location, null)));

    assertEquals(1, targets.size());
    assertTrue(targets.get(0).locationWipe());
  }

  private static ExecutionInfoLocation location(String name) {
    ExecutionInfoLocation location = new ExecutionInfoLocation();
    location.setName(name);
    return location;
  }

  private static Execution execution(String id) {
    Execution execution = new Execution();
    execution.setId(id);
    execution.setName(id);
    return execution;
  }
}
