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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * What an action passes on to the actions after it: its result (rows, files, counters) as Hop's
 * result XML, and the variables set so far. Each task keeps it in its outcome (XCom {@link
 * #OUTCOME_KEY}) under {@link #KEY}, as {@code hop airflow-action} writes it.
 */
public final class TaskState {

  /** The key of the state in a task's outcome. */
  public static final String KEY = "state";

  /**
   * The XCom key of a task's outcome. Not the return value: a condition's return value is its
   * decision.
   */
  public static final String OUTCOME_KEY = "hop_outcome";

  static final String RESULT = "result";
  static final String VARIABLES = "variables";

  /** The next action gets the result without its rows, as a parallel branch does in Hop. */
  static final String WITHOUT_ROWS = "without_rows";

  /** When each variable was set, in milliseconds since the epoch. */
  static final String VARIABLE_TIMES = "variable_times";

  /**
   * More than this in XCom fails the task: Airflow keeps XCom in its database, and big values slow
   * down every task that reads them.
   */
  static final int MAX_BYTES = 1024 * 1024;

  private TaskState() {}

  /**
   * The result of a Join, as Hop's Join works it out: false when an action before it ended false
   * and its hop into the Join is a success hop. Airflow only runs the Join when no task before it
   * failed, so this only finds a condition whose success side, a gate, was skipped.
   */
  public static boolean joinResult(
      ActionTasks.ActionTask join,
      Map<String, ActionTasks.ActionTask> tasks,
      Function<String, Object> outcomes) {
    for (ActionTasks.Upstream upstream : join.upstreams()) {
      ActionTasks.Upstream hop = upstream;
      ActionTasks.ActionTask before = tasks.get(upstream.taskId());
      if (before != null && before.role() == ActionTasks.Role.GATE) {
        hop = before.upstreams().get(0);
      }
      if (hop.kind() == ActionTasks.HopKind.SUCCESS
          && outcomes.apply(hop.taskId()) instanceof Map<?, ?> outcome
          && !Boolean.TRUE.equals(outcome.get("success"))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Fails when the task follows a failure or unconditional hop from a task that did not run: in Hop
   * the workflow stopped before that task.
   *
   * @throws IllegalStateException when a task before it left no outcome
   */
  public static void checkTheTaskBeforeRan(
      ActionTasks.ActionTask task, Function<String, Object> outcomes) {
    if (!task.needsTheTaskBeforeToHaveRun()) {
      return;
    }
    for (ActionTasks.Upstream upstream : task.upstreams()) {
      if (outcomes.apply(upstream.taskId()) == null) {
        throw new IllegalStateException(
            "["
                + task.actionName()
                + "] does not run: task "
                + upstream.taskId()
                + " before it did not run, because a task before that failed");
      }
    }
  }

  /**
   * The state a task starts with: the variables of all the tasks before it, each with the value
   * that was set last, as when Hop runs actions in parallel, and the result of the first one whose
   * outcome matches the hop from it. A task before it that did not run (skipped) passes on the
   * state of the tasks before that one.
   *
   * @param task the task
   * @param tasks the tasks of the DAG, by task id
   * @param outcomes the outcome of a task of this DAG run, or null when it has none
   * @return the state, or null when no task before it left one
   */
  public static Map<String, Object> before(
      ActionTasks.ActionTask task,
      Map<String, ActionTasks.ActionTask> tasks,
      Function<String, Object> outcomes) {
    Map<String, Object> variables = new LinkedHashMap<>();
    Map<String, Long> times = new LinkedHashMap<>();
    // The result XML, and whether to pass it on without the rows
    Object[] result = new Object[2];
    collect(task, tasks, outcomes, variables, times, result, new HashSet<>());
    if (variables.isEmpty() && result[0] == null) {
      return null;
    }
    Map<String, Object> state = new LinkedHashMap<>();
    if (result[0] != null) {
      state.put(RESULT, result[0]);
      if (Boolean.TRUE.equals(result[1])) {
        state.put(WITHOUT_ROWS, true);
      }
    }
    state.put(VARIABLES, variables);
    state.put(VARIABLE_TIMES, times);
    return state;
  }

  private static void collect(
      ActionTasks.ActionTask task,
      Map<String, ActionTasks.ActionTask> tasks,
      Function<String, Object> outcomes,
      Map<String, Object> variables,
      Map<String, Long> times,
      Object[] result,
      Set<String> seen) {
    for (ActionTasks.Upstream upstream : task.upstreams()) {
      if (!seen.add(upstream.taskId())) {
        continue;
      }
      Map<?, ?> outcome = outcomes.apply(upstream.taskId()) instanceof Map<?, ?> map ? map : null;
      Map<?, ?> state = outcome != null && outcome.get(KEY) instanceof Map<?, ?> s ? s : null;
      if (state == null) {
        ActionTasks.ActionTask before = tasks.get(upstream.taskId());
        if (before != null) {
          collect(before, tasks, outcomes, variables, times, result, seen);
        }
        continue;
      }
      if (state.get(VARIABLES) instanceof Map<?, ?> values) {
        Map<?, ?> setAt = state.get(VARIABLE_TIMES) instanceof Map<?, ?> t ? t : Map.of();
        for (Map.Entry<?, ?> entry : values.entrySet()) {
          String name = String.valueOf(entry.getKey());
          long time = setAt.get(name) instanceof Number number ? number.longValue() : 0L;
          if (!times.containsKey(name) || time > times.get(name)) {
            variables.put(name, entry.getValue());
            times.put(name, time);
          }
        }
      }
      if (result[0] == null && state.get(RESULT) != null && matches(upstream.kind(), outcome)) {
        result[0] = state.get(RESULT);
        result[1] = upstream.withoutRows() || Boolean.TRUE.equals(state.get(WITHOUT_ROWS));
      }
    }
  }

  private static boolean matches(ActionTasks.HopKind kind, Map<?, ?> outcome) {
    boolean success = Boolean.TRUE.equals(outcome.get("success"));
    return switch (kind) {
      case SUCCESS -> success;
      case FAILURE -> !success;
      case ALWAYS -> true;
    };
  }
}
