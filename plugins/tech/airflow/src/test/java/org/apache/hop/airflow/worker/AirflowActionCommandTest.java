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

package org.apache.hop.airflow.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AirflowActionCommandTest {

  @Test
  void passesOnWhatCameInAndWhatTheActionSet() {
    Map<String, String> carried = Map.of("LABEL", "from-a");
    Map<String, Long> carriedTimes = Map.of("LABEL", 10L);
    Map<String, String> atStart = new LinkedHashMap<>();
    atStart.put("LABEL", "from-a");
    atStart.put("PRM_DAY", "2026-11-23");
    atStart.put("Internal.Workflow.Name", "w");
    Map<String, String> atEnd = new LinkedHashMap<>(atStart);
    atEnd.put("ROWS", "3");
    atEnd.put("Internal.Action.Current.Directory", "/tmp");
    atEnd.put("PRM_DAY", "2026-11-24");

    Map<String, Object> state =
        AirflowActionCommand.variablesToPassOn(carried, carriedTimes, atStart, atEnd, 99L);

    assertEquals(
        Map.of("LABEL", "from-a", "ROWS", "3", "PRM_DAY", "2026-11-24"),
        state.get(AirflowActionCommand.STATE_VARIABLES));
    assertEquals(
        Map.of("LABEL", 10L, "ROWS", 99L, "PRM_DAY", 99L),
        state.get(AirflowActionCommand.STATE_VARIABLE_TIMES));
  }

  @Test
  void givesAChangedCarriedVariableTheNewTime() {
    Map<String, Object> state =
        AirflowActionCommand.variablesToPassOn(
            Map.of("LABEL", "old"),
            Map.of("LABEL", 10L),
            Map.of("LABEL", "old"),
            Map.of("LABEL", "new"),
            99L);

    assertEquals(Map.of("LABEL", "new"), state.get(AirflowActionCommand.STATE_VARIABLES));
    assertEquals(Map.of("LABEL", 99L), state.get(AirflowActionCommand.STATE_VARIABLE_TIMES));
  }

  @Test
  void readsTheStateItWrites(@TempDir Path folder) throws Exception {
    String file = folder.resolve("state.json").toString();
    Map<String, Object> state = new LinkedHashMap<>();
    state.put(AirflowActionCommand.STATE_RESULT, "<result><result>Y</result></result>");
    state.put(AirflowActionCommand.STATE_VARIABLES, Map.of("A", "1,2;3"));
    state.put("list", List.of(1, 2));

    AirflowActionCommand.writeState(file, state);

    assertEquals(state, AirflowActionCommand.readState(file));
  }
}
