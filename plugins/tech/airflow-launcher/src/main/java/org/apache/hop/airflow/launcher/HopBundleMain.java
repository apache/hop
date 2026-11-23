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

import io.ktor.network.sockets.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.airflow.sdk.Bundle;
import org.apache.airflow.sdk.ConditionRef;
import org.apache.airflow.sdk.CoordinatorServer;
import org.apache.airflow.sdk.DagDef;
import org.apache.airflow.sdk.Server;
import org.apache.airflow.sdk.Task;
import org.apache.airflow.sdk.TaskRef;

/**
 * What the Airflow Java coordinator runs for a Hop DAG bundle: to list the DAGs (DAG processor) and
 * to run a task (worker). Every workflow of the project becomes a DAG with one task per action, or
 * with one task that runs the whole workflow when its hops don't fit Airflow (see {@link
 * ActionTasks}).
 */
public final class HopBundleMain {

  /** The name of the one task of a workflow DAG. */
  public static final String TASK_ID = "run_workflow";

  /** The queue the DAGs use; the Airflow configuration maps it to the Java coordinator. */
  public static final String QUEUE = "java";

  private static final Map<String, HopWorkflowFile> WORKFLOWS = new ConcurrentHashMap<>();
  private static volatile BundleManifest bundle;

  private HopBundleMain() {}

  public static void main(String[] args) throws Exception {
    long start = System.currentTimeMillis();
    BundleManifest manifest = BundleManifest.locate();
    manifest.checkSchemaVersion(BundleManifest.sdkSchemaVersion());
    bundle = manifest;

    List<HopWorkflowFile> workflows =
        HopWorkflowFile.scan(
            manifest.folder(), manifest.workflowPatterns(), manifest.dagIdPrefix());
    Bundle dags = new Bundle();
    Map<String, HopWorkflowFile> listed = new LinkedHashMap<>();
    for (HopWorkflowFile workflow : workflows) {
      HopWorkflowFile other = listed.putIfAbsent(workflow.dagId(), workflow);
      if (other != null) {
        // Airflow then shows an import error with this message
        throw new IllegalStateException(
            "Workflows "
                + other.relativePath()
                + " and "
                + workflow.relativePath()
                + " would both become DAG "
                + workflow.dagId()
                + ": rename one of them");
      }
      WORKFLOWS.put(workflow.dagId(), workflow);
      DagDef dag =
          new DagDef(workflow.dagId())
              .config("queue", QUEUE)
              .config("description", workflow.description())
              .config("tags", List.of("hop", manifest.projectName()))
              .config("doc_md", dagDoc(workflow));
      if (workflow.tasks().isEmpty()) {
        // stdout: Airflow shows a task's stderr as errors
        System.out.println(
            workflow.relativePath()
                + " runs as one task, "
                + TASK_ID
                + ": "
                + workflow.singleTaskReason());
        dag.task(TASK_ID, RunWorkflowTask.class);
      } else {
        addTasks(dag, workflow);
      }
      dags.register(dag);
    }
    // stdout: Airflow shows a task's stderr as errors
    System.out.println(
        "Hop project "
            + manifest.projectName()
            + ": "
            + workflows.size()
            + " workflows in "
            + manifest.folder()
            + " listed in "
            + (System.currentTimeMillis() - start)
            + " ms");
    server(args).serve(dags);
  }

  /** One task per action, plus the gates; see {@link ActionTasks}. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static void addTasks(DagDef dag, HopWorkflowFile workflow) {
    Map<String, TaskRef<Object>> refs = new LinkedHashMap<>();
    Map<String, ConditionRef> conditions = new LinkedHashMap<>();
    for (ActionTasks.ActionTask task : workflow.tasks()) {
      Class<? extends Task> type =
          switch (task.role()) {
            case ACTION -> RunWorkflowTask.class;
            case CONDITION -> RunActionCondition.class;
            case JOIN, GATE -> PassOnTask.class;
          };
      TaskRef<Object> ref =
          dag.<Object>task(task.taskId(), type).config("doc_md", doc(task, workflow));
      if (!ActionTasks.ALL_SUCCESS.equals(task.triggerRule())) {
        ref.config("trigger_rule", task.triggerRule());
      }
      refs.put(task.taskId(), ref);
      if (task.role() == ActionTasks.Role.CONDITION) {
        conditions.put(task.taskId(), ConditionRef.of((TaskRef) ref));
      }
    }
    for (ActionTasks.ActionTask task : workflow.tasks()) {
      List<TaskRef<Object>> before = new ArrayList<>();
      for (ActionTasks.Upstream upstream : task.upstreams()) {
        ActionTasks.ActionTask from = workflow.task(upstream.taskId());
        boolean side =
            from.role() == ActionTasks.Role.CONDITION
                && (task.taskId().equals(from.thenTaskId())
                    || task.taskId().equals(from.elseTaskId()));
        // Then and Else put the condition before its sides
        if (!side) {
          before.add(refs.get(upstream.taskId()));
        }
      }
      if (!before.isEmpty()) {
        refs.get(task.taskId()).after(before.toArray(new TaskRef[0]));
      }
    }
    for (ActionTasks.ActionTask task : workflow.tasks()) {
      ConditionRef condition = conditions.get(task.taskId());
      if (condition != null) {
        condition.Then(refs.get(task.thenTaskId()));
        if (task.elseTaskId() != null) {
          condition.Else(refs.get(task.elseTaskId()));
        }
      }
    }
  }

  /** Names the workflow, and its version for the Airflow run configuration. */
  static String dagDoc(HopWorkflowFile workflow) {
    String doc = "Hop workflow `" + workflow.relativePath() + "`";
    if (workflow.version() != null) {
      doc += "\n\n" + HopAirflowBundle.DAG_DOC_VERSION + workflow.version();
    }
    return doc;
  }

  private static String doc(ActionTasks.ActionTask task, HopWorkflowFile workflow) {
    return switch (task.role()) {
      case GATE ->
          "Passes the state of action ["
              + task.actionName()
              + "] of "
              + workflow.relativePath()
              + " on, when the hop is followed";
      case JOIN -> "Join action [" + task.actionName() + "] of " + workflow.relativePath();
      default -> "Action [" + task.actionName() + "] of " + workflow.relativePath();
    };
  }

  static BundleManifest bundle() {
    return bundle;
  }

  static HopWorkflowFile workflow(String dagId) {
    return WORKFLOWS.get(dagId);
  }

  /**
   * The coordinator appends {@code --comm=host:port} and {@code --logs=host:port}. {@code
   * Server.create(args)} would parse them with an LGPL library that Hop cannot ship, so parse them
   * here.
   */
  static Server server(String[] args) {
    String comm = null;
    String logs = null;
    for (String arg : args) {
      if (arg.startsWith("--comm=")) {
        comm = arg.substring("--comm=".length());
      } else if (arg.startsWith("--logs=")) {
        logs = arg.substring("--logs=".length());
      }
    }
    if (comm == null || logs == null) {
      throw new IllegalArgumentException(
          "Expected --comm=host:port and --logs=host:port from the Airflow coordinator, got: "
              + String.join(" ", args));
    }
    return new CoordinatorServer(address(comm), address(logs));
  }

  static InetSocketAddress address(String hostAndPort) {
    int colon = hostAndPort.lastIndexOf(':');
    if (colon <= 0 || colon == hostAndPort.length() - 1) {
      throw new IllegalArgumentException("Not a host:port address: " + hostAndPort);
    }
    String host = hostAndPort.substring(0, colon);
    if (host.startsWith("[") && host.endsWith("]")) {
      host = host.substring(1, host.length() - 1);
    }
    return new InetSocketAddress(host, Integer.parseInt(hostAndPort.substring(colon + 1)));
  }
}
