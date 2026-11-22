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

package org.apache.hop.workflow.actions.airflow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.airflow.client.AirflowClient;
import org.apache.hop.airflow.client.AirflowDagRun;
import org.apache.hop.airflow.metadata.AirflowConnection;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Starts a run of an Apache Airflow DAG through the Airflow REST API and, optionally, waits until
 * the run finishes.
 */
@Action(
    id = "AIRFLOW_TRIGGER_DAG_RUN",
    name = "i18n::ActionAirflowTriggerDagRun.Name",
    description = "i18n::ActionAirflowTriggerDagRun.Description",
    image = "airflow-trigger-dag-run.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionAirflowTriggerDagRun.Keywords",
    documentationUrl = "/workflow/actions/airflow-trigger-dag-run.html")
@GuiPlugin
@Getter
@Setter
public class ActionAirflowTriggerDagRun extends ActionBase implements IAction {
  private static final Class<?> PKG = ActionAirflowTriggerDagRun.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "AirflowTriggerDagRun.Dialog.Options";

  private static final String GROUP_DAG_RUN = "i18n::ActionAirflowTriggerDagRun.Group.DagRun";
  private static final String GROUP_CONF = "i18n::ActionAirflowTriggerDagRun.Group.Conf";
  private static final String GROUP_WAIT = "i18n::ActionAirflowTriggerDagRun.Group.Wait";
  private static final String GROUP_RESULT = "i18n::ActionAirflowTriggerDagRun.Group.Result";

  private static final Set<String> FAILED_TASK_STATES = Set.of("failed", "upstream_failed");
  private static final long SLEEP_SLICE_MS = 250L;

  /** How many status checks in a row may fail before the action gives up waiting. */
  static final int MAX_FAILED_POLLS = 5;

  @GuiWidgetElement(
      id = "connection",
      order = "0100",
      type = GuiElementType.METADATA,
      metadata = AirflowConnection.class,
      label = "i18n::ActionAirflowTriggerDagRun.Connection.Label",
      toolTip = "i18n::ActionAirflowTriggerDagRun.Connection.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DAG_RUN,
      groupOrder = "10")
  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.AIRFLOW_CONNECTION)
  private String connectionName;

  @GuiWidgetElement(
      id = "dagId",
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::ActionAirflowTriggerDagRun.DagId.Label",
      toolTip = "i18n::ActionAirflowTriggerDagRun.DagId.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DAG_RUN,
      groupOrder = "10")
  @HopMetadataProperty(key = "dag_id")
  private String dagId;

  @GuiWidgetElement(
      id = "dagRunId",
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::ActionAirflowTriggerDagRun.DagRunId.Label",
      toolTip = "i18n::ActionAirflowTriggerDagRun.DagRunId.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DAG_RUN,
      groupOrder = "10")
  @HopMetadataProperty(key = "dag_run_id")
  private String dagRunId;

  @GuiWidgetElement(
      id = "logicalDate",
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::ActionAirflowTriggerDagRun.LogicalDate.Label",
      toolTip = "i18n::ActionAirflowTriggerDagRun.LogicalDate.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_DAG_RUN,
      groupOrder = "10")
  @HopMetadataProperty(key = "logical_date")
  private String logicalDate;

  @GuiWidgetElement(
      id = "confJson",
      order = "1000",
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 5,
      label = "i18n::ActionAirflowTriggerDagRun.ConfJson.Label",
      toolTip = "i18n::ActionAirflowTriggerDagRun.ConfJson.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CONF,
      groupOrder = "20")
  @HopMetadataProperty(key = "conf_json")
  private String confJson;

  @GuiWidgetElement(
      id = "confEntries",
      order = "1100",
      type = GuiElementType.TABLE,
      toolTip = "i18n::ActionAirflowTriggerDagRun.Conf.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_CONF,
      groupOrder = "20",
      tableRows = 6)
  @HopMetadataProperty(groupKey = "conf", key = "entry")
  private List<AirflowConfEntry> confEntries = new ArrayList<>();

  @GuiWidgetElement(
      id = "waitingForCompletion",
      order = "2000",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ActionAirflowTriggerDagRun.Wait.Label",
      toolTip = "i18n::ActionAirflowTriggerDagRun.Wait.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_WAIT,
      groupOrder = "30")
  @HopMetadataProperty(key = "wait_for_completion")
  private boolean waitingForCompletion;

  @GuiWidgetElement(
      id = "pollIntervalSeconds",
      order = "2100",
      type = GuiElementType.TEXT,
      label = "i18n::ActionAirflowTriggerDagRun.PollInterval.Label",
      toolTip = "i18n::ActionAirflowTriggerDagRun.PollInterval.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_WAIT,
      groupOrder = "30")
  @HopMetadataProperty(key = "poll_interval_seconds")
  private String pollIntervalSeconds;

  @GuiWidgetElement(
      id = "timeoutSeconds",
      order = "2200",
      type = GuiElementType.TEXT,
      label = "i18n::ActionAirflowTriggerDagRun.Timeout.Label",
      toolTip = "i18n::ActionAirflowTriggerDagRun.Timeout.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_WAIT,
      groupOrder = "30")
  @HopMetadataProperty(key = "timeout_seconds")
  private String timeoutSeconds;

  @GuiWidgetElement(
      id = "dagRunIdVariable",
      order = "3000",
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::ActionAirflowTriggerDagRun.DagRunIdVariable.Label",
      toolTip = "i18n::ActionAirflowTriggerDagRun.DagRunIdVariable.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_RESULT,
      groupOrder = "40")
  @HopMetadataProperty(key = "dag_run_id_variable")
  private String dagRunIdVariable;

  @GuiWidgetElement(
      id = "stateVariable",
      order = "3100",
      type = GuiElementType.TEXT,
      variables = false,
      label = "i18n::ActionAirflowTriggerDagRun.StateVariable.Label",
      toolTip = "i18n::ActionAirflowTriggerDagRun.StateVariable.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_RESULT,
      groupOrder = "40")
  @HopMetadataProperty(key = "state_variable")
  private String stateVariable;

  public ActionAirflowTriggerDagRun() {
    this("");
  }

  public ActionAirflowTriggerDagRun(String name) {
    super(name, "");
    waitingForCompletion = true;
    pollIntervalSeconds = "10";
    timeoutSeconds = "0";
    dagRunIdVariable = "AIRFLOW_DAG_RUN_ID";
    stateVariable = "AIRFLOW_DAG_RUN_STATE";
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);
    result.setNrErrors(1);
    setResultVariable(dagRunIdVariable, "");
    setResultVariable(stateVariable, "");

    try {
      String dag = resolve(dagId);
      if (StringUtils.isBlank(dag)) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Error.NoDagId"));
      }
      AirflowClient client = AirflowClient.create(loadConnection(), this);

      if (client.isDagPaused(dag)) {
        if (waitingForCompletion) {
          // The run would stay queued, and the action would wait for it forever.
          throw new HopException(
              BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Error.DagPaused", dag));
        }
        logBasic(BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Log.DagPaused", dag));
      }

      AirflowDagRun run =
          client.triggerDagRun(dag, resolve(dagRunId), resolve(logicalDate), buildConf());
      setResultVariable(dagRunIdVariable, run.dagRunId());
      setResultVariable(stateVariable, run.state());
      logBasic(
          BaseMessages.getString(
              PKG, "ActionAirflowTriggerDagRun.Log.Triggered", run.dagRunId(), dag));

      if (!waitingForCompletion) {
        result.setResult(true);
        result.setNrErrors(0);
        return result;
      }

      run = waitForRun(client, run);
      if (run == null) {
        return result;
      }
      if (run.isSuccess()) {
        logBasic(
            BaseMessages.getString(
                PKG, "ActionAirflowTriggerDagRun.Log.Succeeded", run.dagRunId(), dag));
        result.setResult(true);
        result.setNrErrors(0);
      } else {
        logError(
            BaseMessages.getString(
                PKG,
                "ActionAirflowTriggerDagRun.Error.RunFailed",
                run.dagRunId(),
                dag,
                failedTasks(client, run)));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logError(BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Error.Execute"), e);
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Error.Execute"), e);
    }
    return result;
  }

  private AirflowConnection loadConnection() throws HopException {
    String name = resolve(connectionName);
    if (StringUtils.isBlank(name)) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Error.NoConnection"));
    }
    AirflowConnection connection =
        getMetadataProvider().getSerializer(AirflowConnection.class).load(name);
    if (connection == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Error.ConnectionNotFound", name));
    }
    return connection;
  }

  /**
   * The JSON object first, then the table: a key in the table replaces the same key in the JSON.
   */
  @SuppressWarnings("unchecked")
  Map<String, Object> buildConf() throws HopException {
    Map<String, Object> conf = new LinkedHashMap<>();
    String json = resolve(confJson);
    if (StringUtils.isNotBlank(json)) {
      Object parsed;
      try {
        parsed = new JSONParser().parse(json);
      } catch (ParseException e) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Error.ConfJson", e.toString()),
            e);
      }
      if (!(parsed instanceof JSONObject object)) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Error.ConfJsonNotObject"));
      }
      conf.putAll(object);
    }
    for (AirflowConfEntry entry : confEntries) {
      String key = resolve(entry.getName());
      if (StringUtils.isNotBlank(key)) {
        conf.put(key, Const.NVL(resolve(entry.getValue()), ""));
      }
    }
    return conf;
  }

  /**
   * Polls the DAG run until it finishes.
   *
   * @return the finished run, or null when the workflow was stopped or the wait timed out
   */
  private AirflowDagRun waitForRun(AirflowClient client, AirflowDagRun run)
      throws HopException, InterruptedException {
    long pollMs = Math.max(1, Const.toInt(resolve(pollIntervalSeconds), 10)) * 1000L;
    int timeout = Math.max(0, Const.toInt(resolve(timeoutSeconds), 0));
    long deadline = timeout == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeout * 1000L;

    AirflowDagRun current = run;
    int failedPolls = 0;
    while (!current.isFinished()) {
      if (System.currentTimeMillis() >= deadline) {
        logError(
            BaseMessages.getString(
                PKG,
                "ActionAirflowTriggerDagRun.Error.Timeout",
                Integer.toString(timeout),
                current.dagRunId(),
                current.dagId(),
                current.state()));
        return null;
      }
      if (!sleep(Math.min(pollMs, Math.max(0, deadline - System.currentTimeMillis())))) {
        logBasic(
            BaseMessages.getString(
                PKG, "ActionAirflowTriggerDagRun.Log.Stopped", current.dagRunId()));
        return null;
      }
      try {
        current = client.getDagRun(current.dagId(), current.dagRunId());
        failedPolls = 0;
      } catch (HopException e) {
        // A restarting API server or a proxy error should not fail a long wait.
        failedPolls++;
        if (failedPolls >= MAX_FAILED_POLLS) {
          throw e;
        }
        logBasic(
            BaseMessages.getString(
                PKG,
                "ActionAirflowTriggerDagRun.Log.PollFailed",
                current.dagRunId(),
                Integer.toString(failedPolls),
                Integer.toString(MAX_FAILED_POLLS),
                e.getMessage()));
        continue;
      }
      setResultVariable(stateVariable, current.state());
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionAirflowTriggerDagRun.Log.State", current.dagRunId(), current.state()));
      }
    }
    return current;
  }

  /**
   * Sleeps in short slices so that stopping the workflow does not wait for a whole poll interval.
   *
   * @return false when the workflow was stopped
   */
  private boolean sleep(long millis) throws InterruptedException {
    long end = System.currentTimeMillis() + millis;
    while (System.currentTimeMillis() < end) {
      if (isWorkflowStopped()) {
        return false;
      }
      Thread.sleep(Math.min(SLEEP_SLICE_MS, Math.max(1, end - System.currentTimeMillis())));
    }
    return !isWorkflowStopped();
  }

  private boolean isWorkflowStopped() {
    IWorkflowEngine<WorkflowMeta> workflow = getParentWorkflow();
    return workflow != null && workflow.isStopped();
  }

  private String failedTasks(AirflowClient client, AirflowDagRun run) {
    try {
      List<String> taskIds = client.getTaskIds(run.dagId(), run.dagRunId(), FAILED_TASK_STATES);
      return taskIds.isEmpty() ? "-" : String.join(", ", taskIds);
    } catch (HopException e) {
      if (isDetailed()) {
        logDetailed("Unable to list the failed tasks of DAG run " + run.dagRunId(), e);
      }
      return "-";
    }
  }

  /** Sets the variable on this action and on the workflow, for the actions that follow. */
  private void setResultVariable(String variableName, String value) {
    if (StringUtils.isBlank(variableName)) {
      return;
    }
    String name = resolve(variableName);
    String text = Const.NVL(value, "");
    setVariable(name, text);
    IWorkflowEngine<WorkflowMeta> workflow = getParentWorkflow();
    if (workflow != null) {
      workflow.setVariable(name, text);
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (StringUtils.isBlank(connectionName)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Error.NoConnection"),
              this));
    }
    if (StringUtils.isBlank(dagId)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Error.NoDagId"),
              this));
    }
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  @Override
  public ActionAirflowTriggerDagRun clone() {
    ActionAirflowTriggerDagRun copy = (ActionAirflowTriggerDagRun) super.clone();
    copy.confEntries = new ArrayList<>();
    for (AirflowConfEntry entry : confEntries) {
      copy.confEntries.add(new AirflowConfEntry(entry));
    }
    return copy;
  }
}
