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

package org.apache.hop.airflow.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.airflow.launcher.ActionTasks.ActionTask;
import org.apache.hop.airflow.launcher.ActionTasks.HopKind;
import org.apache.hop.airflow.launcher.ActionTasks.Role;
import org.apache.hop.airflow.launcher.ActionTasks.Upstream;
import org.junit.jupiter.api.Test;

class TaskStateTest {

  private final Map<String, ActionTask> tasks = new LinkedHashMap<>();
  private final Map<String, Object> outcomes = new HashMap<>();

  private ActionTask task(String id, Upstream... upstreams) {
    return task(id, Role.ACTION, "all_success", upstreams);
  }

  private ActionTask task(String id, Role role, String triggerRule, Upstream... upstreams) {
    ActionTask task =
        new ActionTask(id, id, "X", role, List.of(upstreams), triggerRule, null, null);
    tasks.put(id, task);
    return task;
  }

  private void outcome(String taskId, boolean success, String result, Map<String, Object> vars) {
    Map<String, Object> variables = new LinkedHashMap<>();
    Map<String, Object> times = new LinkedHashMap<>();
    vars.forEach(
        (name, value) -> {
          Object[] pair = (Object[]) value;
          variables.put(name, pair[0]);
          times.put(name, pair[1]);
        });
    Map<String, Object> state = new LinkedHashMap<>();
    if (result != null) {
      state.put(TaskState.RESULT, result);
    }
    state.put(TaskState.VARIABLES, variables);
    state.put(TaskState.VARIABLE_TIMES, times);
    Map<String, Object> outcome = new LinkedHashMap<>();
    outcome.put("success", success);
    outcome.put(TaskState.KEY, state);
    outcomes.put(taskId, outcome);
  }

  private static Object[] value(String value, long time) {
    return new Object[] {value, time};
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> before(ActionTask task) {
    return TaskState.before(task, tasks, outcomes::get);
  }

  @Test
  void passesOnTheResultAndTheVariablesOfTheTaskBefore() {
    task("A");
    ActionTask b = task("B", new Upstream("A", HopKind.SUCCESS, false));
    outcome("A", true, "<result/>", Map.of("LABEL", value("x", 10)));

    Map<String, Object> state = before(b);

    assertEquals("<result/>", state.get(TaskState.RESULT));
    assertEquals(Map.of("LABEL", "x"), state.get(TaskState.VARIABLES));
    assertNull(state.get(TaskState.WITHOUT_ROWS));
  }

  @Test
  void keepsTheValueSetLastWhereBranchesMeet() {
    task("A");
    task("B");
    ActionTask join =
        task(
            "Join",
            new Upstream("A", HopKind.SUCCESS, false),
            new Upstream("B", HopKind.SUCCESS, false));
    // B only carried the value A's branch started with; A changed it later
    outcome("A", true, "<a/>", Map.of("X", value("new", 20), "Y", value("from A", 5)));
    outcome("B", true, "<b/>", Map.of("X", value("old", 1), "Z", value("from B", 7)));

    Map<String, Object> state = before(join);

    assertEquals(Map.of("X", "new", "Y", "from A", "Z", "from B"), state.get(TaskState.VARIABLES));
    assertEquals("<a/>", state.get(TaskState.RESULT));
  }

  @Test
  void takesTheResultFromTheTaskWhoseHopWasFollowed() {
    task("A");
    task("B");
    ActionTask abort =
        task(
            "Abort",
            new Upstream("A", HopKind.FAILURE, false),
            new Upstream("B", HopKind.FAILURE, false));
    outcome("A", true, "<a/>", Map.of());
    outcome("B", false, "<b/>", Map.of());

    assertEquals("<b/>", before(abort).get(TaskState.RESULT));
  }

  @Test
  void passesOnTheStateBeforeATaskThatDidNotRun() {
    task("A");
    task("Gate", new Upstream("A", HopKind.FAILURE, false));
    ActionTask next = task("Next", new Upstream("Gate", HopKind.SUCCESS, false));
    outcome("A", true, "<a/>", Map.of("LABEL", value("x", 3)));

    Map<String, Object> state = before(next);

    assertEquals(Map.of("LABEL", "x"), state.get(TaskState.VARIABLES));
    // A succeeded: its result goes with a success hop, not with the failure hop to the gate
    assertNull(state.get(TaskState.RESULT));
  }

  @Test
  void passesTheResultWithoutRowsToAParallelBranch() {
    task("Fan out");
    ActionTask branch = task("Branch", new Upstream("Fan out", HopKind.ALWAYS, true));
    outcome("Fan out", true, "<result/>", Map.of());

    assertEquals(true, before(branch).get(TaskState.WITHOUT_ROWS));
  }

  @Test
  void hasNoStateWhenNoTaskBeforeLeftOne() {
    task("A");
    ActionTask b = task("B", new Upstream("A", HopKind.SUCCESS, false));

    assertNull(before(b));
    assertNull(before(task("First")));
  }

  @Test
  void failsATaskAfterAFailureHopWhenTheTaskBeforeDidNotRun() {
    task("Load");
    task("Transform", new Upstream("Load", HopKind.SUCCESS, false));
    ActionTask mail =
        task("Mail", Role.ACTION, "one_failed", new Upstream("Transform", HopKind.FAILURE, false));

    // Load failed, so Airflow marks Transform upstream_failed and still runs Mail
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> TaskState.checkTheTaskBeforeRan(mail, outcomes::get));
    assertTrue(e.getMessage().contains("Transform"), e.getMessage());

    outcome("Transform", false, "<result/>", Map.of());
    TaskState.checkTheTaskBeforeRan(mail, outcomes::get);
  }

  @Test
  void letsATaskAfterASuccessHopRunWithoutLooking() {
    task("Load");
    ActionTask next = task("Next", new Upstream("Load", HopKind.SUCCESS, false));

    TaskState.checkTheTaskBeforeRan(next, outcomes::get);
  }

  @Test
  void failsAJoinWhenAnActionBeforeItEndedFalseOnASuccessHop() {
    task("A", Role.CONDITION, "all_success");
    task("A__then", Role.GATE, "all_success", new Upstream("A", HopKind.SUCCESS, false));
    task("B");
    ActionTask join =
        task(
            "Join",
            Role.JOIN,
            "none_failed_min_one_success",
            new Upstream("A__then", HopKind.SUCCESS, false),
            new Upstream("B", HopKind.SUCCESS, false));
    outcome("B", true, "<b/>", Map.of());

    outcome("A", true, "<a/>", Map.of());
    assertTrue(TaskState.joinResult(join, tasks, outcomes::get));

    outcome("A", false, "<a/>", Map.of());
    assertFalse(TaskState.joinResult(join, tasks, outcomes::get));
  }
}
