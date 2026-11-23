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

package org.apache.hop.airflow.engine;

import java.io.InputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.NameScope;
import org.apache.hop.airflow.bundle.AirflowBundleWriter;
import org.apache.hop.airflow.client.AirflowClient;
import org.apache.hop.airflow.client.AirflowDag;
import org.apache.hop.airflow.client.AirflowDagRun;
import org.apache.hop.airflow.client.AirflowTaskInstance;
import org.apache.hop.airflow.client.AirflowTaskLog;
import org.apache.hop.airflow.launcher.HopAirflowBundle;
import org.apache.hop.airflow.metadata.AirflowConnection;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.engine.WorkflowEnginePlugin;
import org.apache.hop.workflow.engines.remote.RemoteWorkflowEngine;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Runs a workflow as an Airflow DAG: publishes the project as a DAG bundle, starts a run of the
 * workflow's DAG and follows it until it ends. The run state, the log and the outcome come from the
 * Airflow REST API. Stopping the workflow fails the DAG run, which makes Airflow stop the task.
 *
 * <p>It reuses the remote engine for what every engine that runs a workflow elsewhere has: the
 * listeners, the variables and the parameters.
 */
@WorkflowEnginePlugin(
    id = AirflowWorkflowEngine.ID,
    name = AirflowWorkflowEngine.NAME,
    description = "Runs the workflow as an Apache Airflow DAG")
public class AirflowWorkflowEngine extends RemoteWorkflowEngine {

  private static final Class<?> PKG = AirflowWorkflowEngine.class;

  public static final String ID = "Airflow";
  public static final String NAME = "Apache Airflow";

  /** The task of a workflow DAG and the key of its outcome, as the launcher names them. */
  static final String TASK_ID = "run_workflow";

  static final String XCOM_KEY = "return_value";

  /** Failed status polls in a row before the engine stops following a DAG run. */
  static final int MAX_FAILED_POLLS = 5;

  /** The XCom key of a task's outcome; a condition's return value is its decision. */
  static final String OUTCOME_XCOM_KEY = "hop_outcome";

  private AirflowWorkflowRunConfiguration airflowRunConfiguration;
  private AirflowClient client;
  private String dagId;
  private AirflowActionLog actionLog;
  private String publishedVersion;

  /** How far the log of each task has been read. */
  private final Map<String, TaskLog> taskLogs = new LinkedHashMap<>();

  private static final class TaskLog {
    private int tryNumber;
    private String token;
    private int linesAfterToken;
    private boolean complete;
  }

  @Override
  public IWorkflowEngineRunConfiguration createDefaultWorkflowEngineRunConfiguration() {
    return new AirflowWorkflowRunConfiguration();
  }

  /** Publishes the bundle and starts the DAG run. The DAG run id becomes the container id. */
  @Override
  protected void submitToRemoteServer() throws HopException {
    if (!(workflowRunConfiguration.getEngineRunConfiguration()
        instanceof AirflowWorkflowRunConfiguration config)) {
      throw new HopException(
          BaseMessages.getString(PKG, "AirflowWorkflowEngine.Error.WrongConfig"));
    }
    airflowRunConfiguration = config;
    if (workflowMeta.hasChanged()) {
      throw new HopException(BaseMessages.getString(PKG, "AirflowWorkflowEngine.Error.Unsaved"));
    }
    client = AirflowClient.create(loadConnection(), this);

    String projectName = getVariable("HOP_PROJECT_NAME");
    String projectHome = getVariable("PROJECT_HOME");
    if (StringUtils.isBlank(projectName) || StringUtils.isBlank(projectHome)) {
      throw new HopException(BaseMessages.getString(PKG, "AirflowWorkflowEngine.Error.NoProject"));
    }
    String relativePath = relativeWorkflowPath(projectHome, workflowMeta.getFilename());
    dagId = AirflowDagIds.dagId(resolve(config.getDagIdPrefix()), projectName, relativePath);
    actionLog = new AirflowActionLog(workflowMeta.getName());

    String bundleFolder = publish(projectName, projectHome);
    checkNotStopped();
    publishedVersion = workflowVersion(bundleFolder, relativePath);
    AirflowDag dag = waitForDag(publishedVersion);
    checkNotStopped();
    if (dag.paused()) {
      if (!config.isUnpausingDag()) {
        throw new HopException(
            BaseMessages.getString(PKG, "AirflowWorkflowEngine.Error.DagPaused", dagId));
      }
      client.setDagPaused(dagId, false);
      logChannel.logBasic(BaseMessages.getString(PKG, "AirflowWorkflowEngine.Log.Unpaused", dagId));
    }

    // Stop can come while the DAG is unpaused: never start a run after it
    checkNotStopped();
    AirflowDagRun run = client.triggerDagRun(dagId, null, null, parameters());
    containerId = run.dagRunId();
    running = true;
    active = true;
    statusDescription = run.state();
    logChannel.logBasic(
        BaseMessages.getString(
            PKG,
            "AirflowWorkflowEngine.Log.Started",
            dagId,
            containerId,
            client.getBaseUrl() + "/dags/" + dagId + "/runs/" + containerId));
  }

  private void checkNotStopped() throws HopException {
    if (stopped) {
      throw new HopException(
          BaseMessages.getString(PKG, "AirflowWorkflowEngine.Error.StoppedBeforeStart", dagId));
    }
  }

  private AirflowConnection loadConnection() throws HopException {
    String name = resolve(airflowRunConfiguration.getConnectionName());
    if (StringUtils.isBlank(name)) {
      throw new HopException(
          BaseMessages.getString(PKG, "AirflowWorkflowEngine.Error.NoConnection"));
    }
    AirflowConnection connection =
        metadataProvider.getSerializer(AirflowConnection.class).load(name);
    if (connection == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "AirflowWorkflowEngine.Error.ConnectionNotFound", name));
    }
    return connection;
  }

  /** The workflow file relative to the project, which is part of its DAG id. */
  static String relativeWorkflowPath(String projectHome, String filename) throws HopException {
    if (StringUtils.isBlank(filename)) {
      throw new HopException(BaseMessages.getString(PKG, "AirflowWorkflowEngine.Error.Unsaved"));
    }
    FileObject project = HopVfs.getFileObject(projectHome);
    FileObject file = HopVfs.getFileObject(filename);
    if (!project.getName().isDescendent(file.getName(), NameScope.DESCENDENT)) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "AirflowWorkflowEngine.Error.OutsideProject", filename, projectHome));
    }
    try {
      return project.getName().getRelativeName(file.getName());
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /** Publishes the project, and returns the bundle folder. */
  private String publish(String projectName, String projectHome) throws HopException {
    AirflowBundleWriter writer = newBundleWriter();
    writer.setProjectName(projectName);
    writer.setProjectHome(projectHome);
    writer.setTargetFolder(resolve(airflowRunConfiguration.getBundleFolder()));
    writer.setWorkerHopHome(
        StringUtils.defaultIfBlank(
            resolve(airflowRunConfiguration.getWorkerHopHome()),
            AirflowBundleWriter.DEFAULT_WORKER_HOP_HOME));
    writer.setRunConfiguration(resolve(airflowRunConfiguration.getWorkerRunConfiguration()));
    if (getLogLevel() != null) {
      writer.setLogLevel(getLogLevel().getCode());
    }
    String environmentFiles = resolve(airflowRunConfiguration.getEnvironmentFiles());
    if (StringUtils.isNotBlank(environmentFiles)) {
      writer.setEnvironmentFiles(
          new ArrayList<>(
              Arrays.stream(environmentFiles.split(";"))
                  .map(String::trim)
                  .filter(StringUtils::isNotEmpty)
                  .toList()));
    }
    writer.setDagIdPrefix(resolve(airflowRunConfiguration.getDagIdPrefix()));
    writer.setHopVersion(HopEnvironment.class.getPackage().getImplementationVersion());
    AirflowBundleWriter.Result written = writer.write();
    logChannel.logBasic(
        BaseMessages.getString(
            PKG, "AirflowWorkflowEngine.Log.Published", projectName, written.bundleFolder()));
    return written.bundleFolder();
  }

  /**
   * The version of the published workflow, as the launcher puts it in the documentation of the DAG:
   * see {@link HopAirflowBundle#DAG_DOC_VERSION}.
   */
  static String workflowVersion(String bundleFolder, String relativePath) throws HopException {
    try (InputStream in =
        HopVfs.getInputStream(HopVfs.getFileObject(bundleFolder).resolveFile(relativePath))) {
      byte[] hash = MessageDigest.getInstance("SHA-256").digest(in.readAllBytes());
      return HexFormat.of().formatHex(hash).substring(0, HopAirflowBundle.VERSION_LENGTH);
    } catch (Exception e) {
      throw new HopException("Unable to read the published workflow " + relativePath, e);
    }
  }

  /** The writer of the bundle. Tests override it, to write without an installed launcher. */
  protected AirflowBundleWriter newBundleWriter() {
    return new AirflowBundleWriter();
  }

  /**
   * A new or changed workflow becomes a DAG once Airflow parses the bundle again, which can take a
   * while. Until then Airflow runs the DAG of the earlier version: wait for the version that was
   * published.
   *
   * @param version the version of the published workflow
   * @return the DAG
   */
  private AirflowDag waitForDag(String version) throws HopException {
    long waitMillis =
        Const.toLong(
                resolve(airflowRunConfiguration.getDagWaitSeconds()),
                Long.parseLong(AirflowWorkflowRunConfiguration.DEFAULT_DAG_WAIT_SECONDS))
            * 1000L;
    long deadline = System.currentTimeMillis() + waitMillis;
    boolean logged = false;
    while (true) {
      AirflowDag dag = client.findDag(dagId);
      boolean current =
          dag != null
              && dag.docMd() != null
              && dag.docMd().contains(HopAirflowBundle.DAG_DOC_VERSION + version);
      if (current) {
        return dag;
      }
      if (stopped) {
        throw new HopException(
            BaseMessages.getString(PKG, "AirflowWorkflowEngine.Error.StoppedBeforeStart", dagId));
      }
      String what = dag == null ? "NoDag" : "OldDag";
      if (System.currentTimeMillis() >= deadline) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "AirflowWorkflowEngine.Error." + what, dagId, waitMillis / 1000L));
      }
      if (!logged) {
        logChannel.logBasic(
            BaseMessages.getString(
                PKG,
                dag == null
                    ? "AirflowWorkflowEngine.Log.WaitingForDag"
                    : "AirflowWorkflowEngine.Log.WaitingForNewDag",
                dagId,
                waitMillis / 1000L));
        logged = true;
      }
      sleep(pollMillis());
    }
  }

  /** The workflow parameters become the DAG run conf, which the launcher passes on to Hop. */
  private Map<String, Object> parameters() {
    Map<String, Object> conf = new LinkedHashMap<>();
    // Tasks of another version of the workflow fail instead of running it
    conf.put(HopAirflowBundle.CONF_WORKFLOW_VERSION, publishedVersion);
    for (String parameter : listParameters()) {
      String value = getVariable(parameter);
      if (value != null) {
        conf.put(parameter, value);
      }
    }
    return conf;
  }

  @Override
  public void monitorRemoteWorkflowUntilFinished() {
    int failedPolls = 0;
    try {
      while (!finished) {
        try {
          getWorkflowStatus();
          failedPolls = 0;
        } catch (HopException e) {
          // A proxy that answers 503 once, or a token that expires, must not end the follow-up
          // while the DAG run goes on in Airflow
          failedPolls++;
          if (failedPolls >= MAX_FAILED_POLLS) {
            throw e;
          }
          logChannel.logBasic(
              BaseMessages.getString(
                  PKG, "AirflowWorkflowEngine.Log.PollFailed", failedPolls, e.getMessage()));
        }
        if (!finished) {
          sleep(pollMillis());
        }
      }
    } catch (Exception e) {
      logChannel.logError(BaseMessages.getString(PKG, "AirflowWorkflowEngine.Error.Monitor"), e);
      result.setNrErrors(result.getNrErrors() + 1);
      result.setResult(false);
      finished = true;
    } finally {
      running = false;
      active = false;
      activeActions.clear();
    }
  }

  /** Reads the DAG run state and the new log lines, and the outcome once the run has ended. */
  @Override
  public synchronized void getWorkflowStatus() throws HopException {
    if (containerId == null) {
      return;
    }
    AirflowDagRun run = client.getDagRun(dagId, containerId);
    statusDescription = run.state();
    readLog();
    if (run.isFinished()) {
      // The last lines can be written after the state changes
      readLog();
      result = outcome(run);
      finished = true;
      running = false;
      active = false;
    }
  }

  /** The new lines of the task logs, in the order the tasks started. */
  private void readLog() throws HopException {
    for (AirflowTaskInstance task : client.getTaskInstances(dagId, containerId)) {
      if (task.tryNumber() >= 1) {
        readLog(task);
      }
    }
  }

  private void readLog(AirflowTaskInstance task) throws HopException {
    TaskLog taskLog = taskLogs.computeIfAbsent(task.taskId(), id -> new TaskLog());
    if (task.tryNumber() != taskLog.tryNumber) {
      // A retry writes a new log. A task that runs one action shows it starting again.
      if (taskLog.tryNumber > 0 && TASK_ID.equals(task.taskId())) {
        getActionResults().clear();
        activeActions.clear();
      }
      taskLog.tryNumber = task.tryNumber();
      taskLog.token = null;
      taskLog.linesAfterToken = 0;
      taskLog.complete = false;
    }
    if (taskLog.complete) {
      return;
    }
    AirflowTaskLog log =
        client.getTaskLog(dagId, containerId, task.taskId(), taskLog.tryNumber, taskLog.token);
    // Without a new token, the next read starts at the same place again: skip what was shown
    List<String> lines = log.lines();
    for (String line :
        lines.subList(Math.min(taskLog.linesAfterToken, lines.size()), lines.size())) {
      logChannel.logBasic(line);
      showOnCanvas(actionLog.parse(line));
    }
    String token = log.continuationToken();
    if (token != null && !token.equals(taskLog.token)) {
      taskLog.token = token;
      taskLog.linesAfterToken = 0;
    } else {
      taskLog.linesAfterToken = Math.max(taskLog.linesAfterToken, lines.size());
      taskLog.complete = token == null && isEnded(task.state());
    }
  }

  private static boolean isEnded(String state) {
    return state != null && List.of("success", "failed", "skipped", "removed").contains(state);
  }

  private void showOnCanvas(AirflowActionLog.Event event) {
    if (event == null) {
      return;
    }
    ActionMeta actionMeta = workflowMeta.findAction(event.actionName());
    if (actionMeta == null) {
      return;
    }
    if (!event.finished()) {
      // A retry runs the action again
      getActionResults().removeIf(entry -> actionMeta.getName().equals(entry.getActionName()));
      activeActions.add(actionMeta);
      return;
    }
    activeActions.remove(actionMeta);
    Result actionResult = new Result();
    actionResult.setResult(event.success());
    if (!event.success()) {
      actionResult.setNrErrors(1);
    }
    ActionResult entry = new ActionResult();
    entry.setActionName(actionMeta.getName());
    entry.setResult(actionResult);
    getActionResults().add(entry);
  }

  /**
   * The result of the run. As in Hop, where each action passes the counters on to the next one, the
   * counters are those of the task that ended last; the errors of every task are logged.
   */
  private Result outcome(AirflowDagRun run) throws HopException {
    Result outcome = new Result();
    Instant lastEnd = null;
    for (AirflowTaskInstance task : client.getTaskInstances(dagId, containerId)) {
      if (task.tryNumber() < 1 || !isEnded(task.state())) {
        continue;
      }
      Object value = client.findXComValue(dagId, containerId, task.taskId(), OUTCOME_XCOM_KEY);
      if (value == null) {
        value = client.findXComValue(dagId, containerId, task.taskId(), XCOM_KEY);
      }
      if (value instanceof String text) {
        // An XCom backend can hand the value back as JSON text
        value = parseJson(text);
      }
      if (!(value instanceof Map<?, ?> map)) {
        continue;
      }
      if (map.get("error") != null) {
        logChannel.logError(String.valueOf(map.get("error")));
      }
      Instant end = task.ended() == null ? Instant.EPOCH : task.ended();
      if (map.get("counters") instanceof Map<?, ?> counters
          && (lastEnd == null || !end.isBefore(lastEnd))) {
        lastEnd = end;
        outcome.setNrErrors(counter(counters, "errors"));
        outcome.setNrLinesRead(counter(counters, "read"));
        outcome.setNrLinesWritten(counter(counters, "written"));
        outcome.setNrLinesInput(counter(counters, "input"));
        outcome.setNrLinesOutput(counter(counters, "output"));
        outcome.setNrLinesUpdated(counter(counters, "updated"));
        outcome.setNrLinesRejected(counter(counters, "rejected"));
      }
    }
    outcome.setResult(run.isSuccess());
    outcome.setStopped(stopped);
    if (!run.isSuccess() && outcome.getNrErrors() == 0) {
      outcome.setNrErrors(1);
    }
    logChannel.logBasic(
        BaseMessages.getString(PKG, "AirflowWorkflowEngine.Log.Finished", dagId, run.state()));
    return outcome;
  }

  private static Object parseJson(String text) {
    try {
      return new JSONParser().parse(text);
    } catch (ParseException e) {
      return null;
    }
  }

  private static long counter(Map<?, ?> counters, String key) {
    return Const.toLong(String.valueOf(counters.get(key)), 0L);
  }

  /** Fails the DAG run: Airflow then stops the task, and the workflow with it. */
  @Override
  public void stopExecution() {
    stopped = true;
    try {
      if (client != null && containerId != null && !finished) {
        client.setDagRunState(dagId, containerId, AirflowDagRun.STATE_FAILED);
        logChannel.logBasic(
            BaseMessages.getString(PKG, "AirflowWorkflowEngine.Log.Stopped", dagId, containerId));
      }
      fireExecutionStoppedListeners();
    } catch (HopException e) {
      logChannel.logError(
          BaseMessages.getString(PKG, "AirflowWorkflowEngine.Error.Stop", dagId, containerId), e);
    }
  }

  private long pollMillis() {
    return Math.max(
            1L,
            Const.toLong(
                resolve(airflowRunConfiguration.getPollSeconds()),
                Long.parseLong(AirflowWorkflowRunConfiguration.DEFAULT_POLL_SECONDS)))
        * 1000L;
  }

  private static void sleep(long millis) throws HopException {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException("Interrupted while waiting for Airflow", e);
    }
  }

  String getDagId() {
    return dagId;
  }
}
