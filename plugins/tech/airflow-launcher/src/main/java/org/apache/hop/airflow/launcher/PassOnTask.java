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

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.airflow.sdk.Client;
import org.apache.airflow.sdk.Context;
import org.apache.airflow.sdk.Task;

/**
 * The task of a Join action, or a gate on a hop. Its trigger rule decides whether it runs, so it
 * only passes the state of the tasks before it on: it does not start Hop.
 */
@SuppressWarnings("java:S106") // no Hop logging here: Airflow keeps stdout as the task log
public class PassOnTask implements Task {

  @Override
  public void execute(Context context, Client client) {
    HopWorkflowFile workflow = HopBundleMain.workflow(context.ti.dagId);
    ActionTasks.ActionTask task = workflow == null ? null : workflow.task(context.ti.taskId);
    if (task == null) {
      throw new IllegalStateException(
          "No Join or gate for task " + context.ti.taskId + " of DAG " + context.ti.dagId);
    }
    TaskState.checkTheTaskBeforeRan(task, workflow.tasksById(), RunWorkflowTask.outcomes(client));
    // stdout: Airflow shows a task's stderr as errors
    System.out.println(
        task.role() == ActionTasks.Role.JOIN
            ? "Join [" + task.actionName() + "]: the actions before it are done"
            : "The hop after [" + task.actionName() + "] is followed");
    int errors =
        task.role() == ActionTasks.Role.JOIN
            ? TaskState.joinErrors(task, workflow.tasksById(), RunWorkflowTask.outcomes(client))
            : 0;
    boolean success = errors == 0;
    Map<String, Object> outcome = new LinkedHashMap<>();
    outcome.put("exitCode", success ? 0 : 1);
    outcome.put("success", success);
    client.setXCom(outcome);
    Map<String, Object> state =
        TaskState.before(task, workflow.tasksById(), RunWorkflowTask.outcomes(client));
    if (!success) {
      // As Hop's Join: the next action gets the result with result false and the errors
      state = state == null ? new LinkedHashMap<>() : new LinkedHashMap<>(state);
      state.put(TaskState.JOIN_ERRORS, errors);
    }
    Map<String, Object> withState = new LinkedHashMap<>(outcome);
    if (state != null) {
      withState.put(TaskState.KEY, state);
    }
    client.setXCom(TaskState.OUTCOME_KEY, withState);
    if (!success) {
      // As in Hop: the success hops after the Join are not followed, its failure hops are
      throw new IllegalStateException(
          "Join ["
              + task.actionName()
              + "]: an action before it ended with result false on a success hop into the Join");
    }
  }
}
