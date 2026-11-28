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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntConsumer;
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
 * ActionTasks}) or when the bundle asks for it ({@link HopAirflowBundle#ATTRIBUTE_TASKS}).
 */
@SuppressWarnings("java:S106") // no Hop logging here: Airflow keeps stdout as the task log
public final class HopBundleMain {

  /** The name of the one task of a workflow DAG. */
  public static final String TASK_ID = "run_workflow";

  /** The queue the DAGs use; the Airflow configuration maps it to the Java coordinator. */
  public static final String QUEUE = "java";

  private static final Map<String, HopWorkflowFile> WORKFLOWS = new ConcurrentHashMap<>();
  private static volatile BundleManifest bundle;

  private HopBundleMain() {}

  /** What {@link #main(String[])} serves. */
  @FunctionalInterface
  interface Served {
    void serve() throws Exception;
  }

  public static void main(String[] args) {
    exitWhenServed(() -> serve(args), System::exit);
  }

  /**
   * The SDK returns once the coordinator has the outcome, but does not end the JVM, and Hop runs in
   * this JVM without the {@code System.exit} of the hop script. A thread that Hop or a plugin
   * leaves running (a connection pool, a timer) would keep the task running until Airflow's {@code
   * task_success_overtime} kills it. Exit as {@code hop run} does: 0, or 1 when serving failed.
   */
  static void exitWhenServed(Served served, IntConsumer exit) {
    int exitCode = 0;
    try {
      served.serve();
    } catch (Throwable e) {
      // As the JVM reports an exception thrown from main: Airflow shows it
      e.printStackTrace();
      exitCode = 1;
    }
    System.out.flush();
    System.err.flush();
    exit.accept(exitCode);
  }

  private static void serve(String[] args) throws Exception {
    long start = System.currentTimeMillis();
    BundleManifest manifest = BundleManifest.locate();
    manifest.checkSchemaVersion(BundleManifest.sdkSchemaVersion());
    bundle = manifest;

    List<HopWorkflowFile> workflows =
        HopWorkflowFile.scan(
            manifest.folder(),
            manifest.workflowPatterns(),
            manifest.dagIdPrefix(),
            manifest.notEvaluatingActions());
    workflows = unique(dags(workflows, HopDagSettings.read(manifest.folder()), manifest));
    Bundle dags = new Bundle();
    for (HopWorkflowFile workflow : workflows) {
      WORKFLOWS.put(workflow.dagId(), workflow);
      DagDef dag = dagDef(workflow, manifest);
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
            + " DAGs in "
            + manifest.folder()
            + " listed in "
            + (System.currentTimeMillis() - start)
            + " ms");
    server(args).serve(dags);
  }

  /**
   * The DAGs without a second DAG of the same id. The SDK would refuse the bundle, and Airflow
   * would then list none of its DAGs, so one DAG keeps the id and the others are left out with a
   * message. The DAG of the default id of its workflow keeps it, so an object's own DAG id never
   * takes the DAG and the run history of another workflow; else the first one. A publish from Hop
   * refuses such a bundle before this.
   */
  static List<HopWorkflowFile> unique(List<HopWorkflowFile> dags) {
    List<HopWorkflowFile> byClaim = new ArrayList<>(dags);
    byClaim.sort(Comparator.comparing(dag -> hasDefaultDagId(dag) ? 0 : 1));
    Map<String, HopWorkflowFile> kept = new LinkedHashMap<>();
    for (HopWorkflowFile dag : byClaim) {
      HopWorkflowFile other = kept.putIfAbsent(dag.dagId(), dag);
      if (other != null) {
        // stdout: Airflow shows a task's stderr as errors
        System.out.println(
            source(dag)
                + " is not listed: "
                + source(other)
                + " is DAG "
                + dag.dagId()
                + " already. "
                + (other.settings() == null && dag.settings() == null
                    ? "Rename one of the workflows."
                    : "Give the Airflow DAG its own DAG id."));
      }
    }
    return dags.stream().filter(dag -> kept.get(dag.dagId()) == dag).toList();
  }

  /** True for the DAG of a workflow, or of an object of a workflow without a DAG id of its own. */
  private static boolean hasDefaultDagId(HopWorkflowFile dag) {
    return dag.settings() == null
        || (dag.settings().dagId() == null && dag.settings().workflow() != null);
  }

  /** What a DAG is made of, for messages: the workflow, or the Airflow DAG object. */
  private static String source(HopWorkflowFile dag) {
    return dag.settings() == null
        ? "Workflow " + dag.relativePath()
        : "Airflow DAG " + dag.settings().name() + " (" + dag.settings().file() + ")";
  }

  /**
   * The DAGs of the workflows. A workflow without a {@value HopAirflowBundle#DAG_METADATA_KEY}
   * object is one DAG, as the bundle describes it. A workflow with objects is one DAG per object;
   * an object without a DAG id keeps the DAG id of the workflow. An object Hop can't use, or whose
   * workflow is not in the project, is a DAG that fails with the reason.
   */
  static List<HopWorkflowFile> dags(
      List<HopWorkflowFile> workflows, List<HopDagSettings> allSettings, BundleManifest manifest) {
    Map<String, List<HopDagSettings>> byWorkflow = new LinkedHashMap<>();
    for (HopDagSettings settings : allSettings) {
      if (settings.workflow() != null) {
        byWorkflow.computeIfAbsent(settings.workflow(), k -> new ArrayList<>()).add(settings);
      }
    }
    List<HopWorkflowFile> dags = new ArrayList<>();
    Map<String, HopWorkflowFile> scanned = new LinkedHashMap<>();
    for (HopWorkflowFile workflow : workflows) {
      scanned.put(workflow.relativePath(), workflow);
      List<HopDagSettings> settingsOfWorkflow = byWorkflow.get(workflow.relativePath());
      if (settingsOfWorkflow == null) {
        dags.add(manifest.wholeWorkflowTasks() ? asOneTask(workflow, bundleReason()) : workflow);
        continue;
      }
      for (HopDagSettings settings : settingsOfWorkflow) {
        dags.add(dag(workflow, settings, manifest));
      }
    }
    for (HopDagSettings settings : allSettings) {
      if (settings.workflow() != null && scanned.containsKey(settings.workflow())) {
        continue;
      }
      if (settings.workflow() != null
          && Files.isRegularFile(manifest.folder().resolve(settings.workflow()))) {
        // stdout: Airflow shows a task's stderr as errors
        System.out.println(
            "Airflow DAG "
                + settings.name()
                + " is not listed: workflow "
                + settings.workflow()
                + " is not one of the workflows of the bundle ("
                + HopAirflowBundle.ATTRIBUTE_WORKFLOWS
                + ")");
        continue;
      }
      String error =
          settings.error() != null
              ? settings.error()
              : "Airflow DAG "
                  + settings.name()
                  + " ("
                  + settings.file()
                  + "): workflow "
                  + settings.workflow()
                  + " is not in the project";
      String dagId =
          settings.dagId() != null
              ? HopWorkflowFile.objectDagId(manifest.dagIdPrefix(), settings.dagId())
              : settings.workflow() != null
                  ? HopWorkflowFile.dagId(manifest.dagIdPrefix(), settings.workflow())
                  : HopWorkflowFile.objectDagId(manifest.dagIdPrefix(), settings.name());
      String path = settings.workflow() != null ? settings.workflow() : settings.file();
      dags.add(
          new HopWorkflowFile(
              path, dagId, error, List.of(), error, null, settings.withError(error)));
    }
    return dags;
  }

  /** The DAG of one Airflow DAG object of a workflow. */
  static HopWorkflowFile dag(
      HopWorkflowFile workflow, HopDagSettings settings, BundleManifest manifest) {
    String dagId =
        settings.dagId() == null
            ? workflow.dagId()
            : HopWorkflowFile.objectDagId(manifest.dagIdPrefix(), settings.dagId());
    if (settings.error() != null) {
      return failing(workflow, dagId, settings);
    }
    HopWorkflowFile dag;
    if (settings.tasks() == null
        ? manifest.wholeWorkflowTasks()
        : HopAirflowBundle.TASKS_WHOLE_WORKFLOW.equals(settings.tasks())) {
      String reason =
          settings.tasks() == null
              ? bundleReason()
              : "Airflow DAG " + settings.name() + " runs the workflow as one task";
      dag = asOneTask(workflow, reason).withSettings(dagId, settings);
    } else {
      dag = workflow.withSettings(dagId, settings);
    }
    if (settings.failFast()) {
      List<String> otherRules =
          dag.tasks().stream()
              .map(ActionTasks.ActionTask::triggerRule)
              .filter(rule -> !HopDagSettings.FAIL_FAST_TRIGGER_RULES.contains(rule))
              .distinct()
              .toList();
      if (!otherRules.isEmpty()) {
        String error =
            "Airflow DAG "
                + settings.name()
                + " fails fast, which Airflow allows only when every task waits for all tasks"
                + " before it to succeed. The tasks of workflow "
                + workflow.relativePath()
                + " also use trigger rules "
                + String.join(", ", otherRules)
                + ", for joins or hops that are followed on failure. Turn off fail fast, or run"
                + " the workflow as one task.";
        return failing(workflow, dagId, settings.withError(error));
      }
    }
    return dag;
  }

  /** A DAG of the workflow that only fails, with the error of the settings as its description. */
  private static HopWorkflowFile failing(
      HopWorkflowFile workflow, String dagId, HopDagSettings settings) {
    return new HopWorkflowFile(
        workflow.relativePath(),
        dagId,
        settings.error(),
        List.of(),
        settings.error(),
        workflow.version(),
        settings);
  }

  private static String bundleReason() {
    return "the bundle runs every workflow as one task ("
        + HopAirflowBundle.ATTRIBUTE_TASKS
        + ": "
        + HopAirflowBundle.TASKS_WHOLE_WORKFLOW
        + ")";
  }

  /** The DAG with its settings, without the tasks. */
  static DagDef dagDef(HopWorkflowFile workflow, BundleManifest manifest) {
    List<String> tags = new ArrayList<>(List.of("hop", manifest.projectName()));
    HopDagSettings settings = workflow.settings();
    String tasks = manifest.tasks();
    if (settings != null) {
      settings.tags().stream().filter(tag -> !tags.contains(tag)).forEach(tags::add);
      if (settings.tasks() != null) {
        tasks = settings.tasks();
      }
    }
    DagDef dag =
        new DagDef(workflow.dagId())
            .config("queue", QUEUE)
            .config("description", workflow.description())
            .config("tags", tags);
    if (settings != null) {
      settings.config().forEach(dag::config);
    }
    return dag.config("doc_md", dagDoc(workflow, tasks));
  }

  /** The workflow as one task, {@link #TASK_ID}, for this reason. */
  static HopWorkflowFile asOneTask(HopWorkflowFile workflow, String reason) {
    if (workflow.tasks().isEmpty() && workflow.singleTaskReason() != null) {
      // Keep the reason it already runs as one task for, such as a file Hop could not read
      return workflow;
    }
    return new HopWorkflowFile(
        workflow.relativePath(),
        workflow.dagId(),
        workflow.description(),
        List.of(),
        reason,
        workflow.version(),
        workflow.settings());
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

  /**
   * The workflow, its version, the tasks and the version of the Airflow DAG object, for the Airflow
   * run configuration, then the documentation of the object, if any. The documentation comes last:
   * the run configuration reads the first {@link HopAirflowBundle#DAG_DOC_TASKS} line, which the
   * documentation could hold as well.
   */
  static String dagDoc(HopWorkflowFile workflow, String tasks) {
    HopDagSettings settings = workflow.settings();
    String doc = "Hop workflow `" + workflow.relativePath() + "`";
    if (workflow.version() != null) {
      doc += "\n\n" + HopAirflowBundle.DAG_DOC_VERSION + workflow.version();
    }
    doc += "\n\n" + HopAirflowBundle.DAG_DOC_TASKS + tasks;
    if (settings != null && settings.version() != null) {
      doc +=
          "\n\n"
              + HopAirflowBundle.DAG_DOC_SETTINGS
              + settings.version()
              + " (`"
              + settings.file()
              + "`)";
    }
    if (settings != null && settings.documentation() != null) {
      doc += "\n\n" + settings.documentation();
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
