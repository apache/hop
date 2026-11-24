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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.airflow.sdk.Client;
import org.apache.airflow.sdk.Context;
import org.apache.airflow.sdk.Task;

/**
 * A task of a workflow DAG: runs one action of the workflow, or the whole workflow, as {@code hop
 * run} would. The DAG run conf becomes parameters; the outcome becomes the task's return value
 * (XCom).
 */
public class RunWorkflowTask implements Task {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Override
  public void execute(Context context, Client client) throws Exception {
    Map<String, Object> outcome = run(context, client);
    // The return value without the state: rows and variables go to the next tasks only
    Map<String, Object> returnValue = new LinkedHashMap<>(outcome);
    returnValue.remove(TaskState.KEY);
    client.setXCom(returnValue);
    failOnError(context, outcome);
  }

  /**
   * Runs the workflow, or the action of the task, and keeps the outcome as XCom {@link
   * TaskState#OUTCOME_KEY}.
   *
   * @return the outcome, with the exit code of Hop
   */
  @SuppressWarnings("unchecked")
  static Map<String, Object> run(Context context, Client client) throws Exception {
    BundleManifest bundle = HopBundleMain.bundle();
    HopWorkflowFile workflow = HopBundleMain.workflow(context.ti.dagId);
    if (bundle == null || workflow == null) {
      throw new IllegalStateException("No Hop workflow for DAG " + context.ti.dagId);
    }
    ActionTasks.ActionTask task = workflow.task(context.ti.taskId);
    if (task == null && !HopBundleMain.TASK_ID.equals(context.ti.taskId)) {
      // A DAG that Airflow parsed from an earlier version of the workflow
      throw new IllegalStateException(
          "Workflow "
              + workflow.relativePath()
              + " has no action for task "
              + context.ti.taskId
              + " anymore: it changed after Airflow read the DAG. Start a new run once Airflow"
              + " shows the new DAG.");
    }
    Object startedFor =
        context.dagRun.conf == null
            ? null
            : context.dagRun.conf.get(HopAirflowBundle.CONF_WORKFLOW_VERSION);
    if (startedFor != null && !String.valueOf(startedFor).equals(workflow.version())) {
      throw new IllegalStateException(
          "This DAG run was started for version "
              + startedFor
              + " of workflow "
              + workflow.relativePath()
              + ", the bundle now holds version "
              + workflow.version()
              + ": it was published again during the run. Start a new run.");
    }
    if (task != null) {
      TaskState.checkTheTaskBeforeRan(task, workflow.tasksById(), outcomes(client));
    }
    Path resultFile = Files.createTempFile("hop-airflow-result-", ".json");
    Path stateIn = null;
    Path stateOut = null;
    try {
      HopCommandLine.ActionRun action = null;
      if (task != null) {
        Map<String, Object> state = TaskState.before(task, workflow.tasksById(), outcomes(client));
        if (state != null) {
          stateIn = Files.createTempFile("hop-airflow-state-in-", ".json");
          JSON.writeValue(stateIn.toFile(), state);
        }
        stateOut = Files.createTempFile("hop-airflow-state-out-", ".json");
        action = new HopCommandLine.ActionRun(task.actionName(), stateIn, stateOut);
      }
      Path hopHome = HopRuntime.findHopHome();
      HopRuntime runtime = new HopRuntime(hopHome);
      runtime.applySystemProperties(System.getenv());
      List<String> workerProjects =
          WorkerProjects.relativeProjectLocations(
              hopHome,
              hopHome.resolve(System.getProperty("HOP_CONFIG_FOLDER")),
              bundle.projectName());
      List<String> args =
          HopCommandLine.build(
              bundle, workflow, action, context.dagRun.conf, resultFile, workerProjects);
      int exitCode = runtime.execute(args);

      Map<String, Object> outcome = outcome(resultFile, exitCode);
      if (task != null) {
        // hop airflow-action exits on the result of the action, and writes the state only when it
        // ran the action: without one, Hop failed before (no such action, a bad run
        // configuration, an unreadable state)
        boolean ran = Files.isRegularFile(stateOut) && Files.size(stateOut) > 0;
        outcome.put("success", ran && exitCode == 0);
        outcome.put(TaskState.ACTION_RAN, ran);
        if (ran) {
          try {
            addState(outcome, stateOut, task);
          } catch (Exception e) {
            // Keep an outcome, so a failure hop after this task can still run
            outcome.put("success", false);
            outcome.put("error", e.getMessage());
            client.setXCom(TaskState.OUTCOME_KEY, outcome);
            throw e;
          }
        }
      }
      client.setXCom(TaskState.OUTCOME_KEY, outcome);
      return outcome;
    } finally {
      Files.deleteIfExists(resultFile);
      if (stateIn != null) {
        Files.deleteIfExists(stateIn);
      }
      if (stateOut != null) {
        Files.deleteIfExists(stateOut);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void addState(
      Map<String, Object> outcome, Path stateOut, ActionTasks.ActionTask task) throws Exception {
    long size = Files.size(stateOut);
    if (size > TaskState.MAX_BYTES) {
      throw new IllegalStateException(
          "Action ["
              + task.actionName()
              + "] passes on "
              + size
              + " bytes of result rows, files and variables, more than the "
              + TaskState.MAX_BYTES
              + " that fit in an Airflow XCom. Pass less to the next action, for example by"
              + " writing the rows to a file or a table.");
    }
    outcome.put(TaskState.KEY, JSON.readValue(stateOut.toFile(), Map.class));
  }

  /** The outcome other tasks of this DAG run kept, by task id; null for a task without one. */
  static Function<String, Object> outcomes(Client client) {
    return taskId -> client.getXCom(TaskState.OUTCOME_KEY, taskId);
  }

  /** Fails the task when Hop did not finish without errors. */
  static void failOnError(Context context, Map<String, Object> outcome) {
    Object exitCode = outcome.get("exitCode");
    if (exitCode instanceof Number number && number.intValue() == 0) {
      return;
    }
    HopWorkflowFile workflow = HopBundleMain.workflow(context.ti.dagId);
    ActionTasks.ActionTask task = workflow.task(context.ti.taskId);
    throw new IllegalStateException(
        (task == null ? "Hop workflow " : "Action [" + task.actionName() + "] of Hop workflow ")
            + workflow.relativePath()
            + " failed (exit code "
            + exitCode
            + ")"
            + (outcome.get("error") == null ? "" : ": " + outcome.get("error")));
  }

  /**
   * The parts of Hop's result file that are worth passing on: no stack traces, which can be long
   * and hold details of the systems involved.
   */
  @SuppressWarnings("unchecked")
  static Map<String, Object> outcome(Path resultFile, int exitCode) throws Exception {
    Map<String, Object> outcome = new LinkedHashMap<>();
    outcome.put("exitCode", exitCode);
    if (!Files.isRegularFile(resultFile) || Files.size(resultFile) == 0) {
      outcome.put("success", exitCode == 0);
      return outcome;
    }
    Map<String, Object> result = JSON.readValue(resultFile.toFile(), Map.class);
    for (String key :
        List.of(
            "success",
            "stopped",
            "type",
            "name",
            "executionId",
            "runConfiguration",
            "startDate",
            "endDate",
            "counters")) {
      if (result.get(key) != null) {
        outcome.put(key, result.get(key));
      }
    }
    String error = message(result.get("failure"));
    if (error == null) {
      error = message(result.get("error"));
    }
    if (error != null) {
      outcome.put("error", error);
    }
    return outcome;
  }

  /** The message of a failure ({@code cause.message}) or of a cause ({@code message}). */
  @SuppressWarnings("unchecked")
  private static String message(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return null;
    }
    Object cause = map.get("cause");
    if (cause instanceof Map<?, ?> causeMap && causeMap.get("message") != null) {
      return String.valueOf(causeMap.get("message"));
    }
    return map.get("message") == null ? null : String.valueOf(map.get("message"));
  }
}
