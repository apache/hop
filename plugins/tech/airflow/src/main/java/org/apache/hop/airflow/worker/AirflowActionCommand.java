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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.run.HopCommandRun;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import picocli.CommandLine;

/**
 * {@code hop airflow-action}: what an Airflow task of a Hop DAG runs. It takes the options of
 * {@code hop run} and runs only the action given with {@code --startaction}.
 *
 * <p>In Hop, an action gets the result of the action before it (rows, files, counters) and sees the
 * variables the actions before it set. On Airflow every action runs in its own JVM, so the task
 * before it writes them to a state file ({@code --state-out}), Airflow keeps them as XCom, and the
 * next task reads them back ({@code --state-in}).
 */
@Getter
@Setter
@CommandLine.Command(
    description =
        "Run one action of a workflow, with the result and variables of the actions before it."
            + " Used by the Airflow tasks of a Hop DAG bundle.")
@HopCommand(
    id = "airflow-action",
    description = "Run one action of a workflow, for an Airflow task of a Hop DAG bundle")
public class AirflowActionCommand extends HopCommandRun {

  /** The keys of a state file. */
  public static final String STATE_RESULT = "result";

  public static final String STATE_VARIABLES = "variables";

  /**
   * The action follows a Join that ended false: it gets the result with result false and this many
   * errors, as Hop's Join passes it on.
   */
  public static final String STATE_JOIN_ERRORS = "join_errors";

  /** The action gets the result without the rows, as a parallel branch does in Hop. */
  public static final String STATE_WITHOUT_ROWS = "without_rows";

  /**
   * When each variable was set, in milliseconds since the epoch: where two branches meet, the value
   * set last wins, as when Hop runs actions in parallel.
   */
  public static final String STATE_VARIABLE_TIMES = "variable_times";

  private static final ObjectMapper JSON = new ObjectMapper();

  /** Hop sets these itself for every run: never passed on. */
  private static final String INTERNAL_PREFIX = "Internal.";

  @CommandLine.Option(
      names = {"--state-in"},
      description = "The state file of the action before this one")
  private String stateIn;

  @CommandLine.Option(
      names = {"--state-out"},
      description = "Where to write the result of the action and the variables set so far")
  private String stateOut;

  /** The variables the state brought in, and every variable as it was when the action started. */
  private final Map<String, String> carriedVariables = new LinkedHashMap<>();

  private final Map<String, Long> carriedTimes = new LinkedHashMap<>();

  private final Map<String, String> variablesAtStart = new HashMap<>();

  /** No enabled hop leaves the action: the workflow ends with it. */
  private boolean lastAction;

  @Override
  protected void beforeWorkflowStart(IWorkflowEngine<WorkflowMeta> workflow) throws HopException {
    if (StringUtils.isEmpty(startActionName)) {
      throw new HopException("hop airflow-action needs the action to run, with --startaction");
    }
    ActionMeta action = workflow.getWorkflowMeta().findAction(startActionName);
    if (action == null) {
      throw new HopException("The workflow has no action '" + startActionName + "'");
    }
    // Without hops leaving it, the workflow ends after this action
    WorkflowMeta workflowMeta = workflow.getWorkflowMeta();
    lastAction = true;
    for (int i = workflowMeta.nrWorkflowHops() - 1; i >= 0; i--) {
      WorkflowHopMeta hop = workflowMeta.getWorkflowHop(i);
      ActionMeta from = hop.getFromAction();
      if (from != null && action.getName().equals(from.getName())) {
        lastAction &= !hop.isEnabled();
        workflowMeta.removeWorkflowHop(i);
      }
    }

    if (StringUtils.isNotEmpty(stateIn)) {
      Map<String, Object> state = readState(stateIn);
      if (state.get(STATE_VARIABLE_TIMES) instanceof Map<?, ?> times) {
        times.forEach(
            (name, time) -> {
              if (time instanceof Number number) {
                carriedTimes.put(String.valueOf(name), number.longValue());
              }
            });
      }
      if (state.get(STATE_VARIABLES) instanceof Map<?, ?> variables) {
        for (Map.Entry<?, ?> entry : variables.entrySet()) {
          String name = String.valueOf(entry.getKey());
          String value = entry.getValue() == null ? null : String.valueOf(entry.getValue());
          carriedVariables.put(name, value);
          workflow.setVariable(name, value);
        }
      }
      if (state.get(STATE_RESULT) instanceof String xml && StringUtils.isNotBlank(xml)) {
        if (!(workflow instanceof Workflow localWorkflow)) {
          throw new HopException(
              "The result of the action before '"
                  + startActionName
                  + "' can only be passed on to the local workflow engine, not to "
                  + workflow.getClass().getSimpleName());
        }
        Result previous = new Result(XmlHandler.loadXmlString(xml, "result"));
        if (Boolean.TRUE.equals(state.get(STATE_WITHOUT_ROWS))) {
          previous = previous.lightClone();
        }
        if (state.get(STATE_JOIN_ERRORS) instanceof Number joinErrors) {
          previous.setResult(false);
          previous.setNrErrors(joinErrors.longValue());
        }
        // As Hop does before it runs the next action
        if (action.getAction().resetErrorsBeforeExecution()) {
          previous.setNrErrors(0);
        }
        localWorkflow.setStartActionResult(previous);
      }
    }
    for (String name : workflow.getVariableNames()) {
      variablesAtStart.put(name, workflow.getVariable(name));
    }
  }

  @Override
  protected void afterWorkflowFinished(IWorkflowEngine<WorkflowMeta> workflow) throws HopException {
    // Hop follows the next hop on the result of the action. The result of this one-action
    // workflow can be false where the action's is true: an action on a failure hop, Write to log
    // for one, keeps the error count it was given.
    Result actionResult = null;
    for (ActionResult entry : workflow.getActionResults()) {
      if (startActionName.equals(entry.getActionName())) {
        actionResult = entry.getResult();
      }
    }
    // The last action of the workflow ends it as hop run would: errors it keeps make the run fail
    if (actionResult != null && !lastAction) {
      setFinishedWithoutError(actionResult.isResult());
    }
    if (StringUtils.isEmpty(stateOut)) {
      return;
    }
    Map<String, String> now = new LinkedHashMap<>();
    for (String name : workflow.getVariableNames()) {
      now.put(name, workflow.getVariable(name));
    }
    Map<String, Object> state =
        variablesToPassOn(
            carriedVariables, carriedTimes, variablesAtStart, now, System.currentTimeMillis());
    // The workflow's result is the one the action returned, with its rows; the copy among the
    // action
    // results has no rows
    Result result = workflow.getResult() == null ? new Result() : workflow.getResult().clone();
    if (actionResult != null) {
      result.setResult(actionResult.isResult());
    }
    // The log of the action is in the task log already
    result.setLogText(null);

    state.put(STATE_RESULT, result.getXml());
    writeState(stateOut, state);
  }

  /**
   * The variables the next action sees: those the actions before passed on, and those this action
   * set or changed, with when each was set.
   *
   * @param carried the variables the state brought in
   * @param carriedTimes when those were set
   * @param atStart every variable when the action started
   * @param atEnd every variable when it ended
   * @param now the time to give the variables this action set
   * @return a state with {@link #STATE_VARIABLES} and {@link #STATE_VARIABLE_TIMES}
   */
  static Map<String, Object> variablesToPassOn(
      Map<String, String> carried,
      Map<String, Long> carriedTimes,
      Map<String, String> atStart,
      Map<String, String> atEnd,
      long now) {
    Map<String, String> variables = new LinkedHashMap<>(carried);
    Map<String, Long> times = new LinkedHashMap<>();
    carried.keySet().forEach(name -> times.put(name, carriedTimes.getOrDefault(name, 0L)));
    atEnd.forEach(
        (name, value) -> {
          if (!name.startsWith(INTERNAL_PREFIX)
              && (!atStart.containsKey(name) || !Objects.equals(value, atStart.get(name)))) {
            variables.put(name, value);
            times.put(name, now);
          }
        });
    Map<String, Object> state = new LinkedHashMap<>();
    state.put(STATE_VARIABLES, variables);
    state.put(STATE_VARIABLE_TIMES, times);
    return state;
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> readState(String filename) throws HopException {
    try (InputStream in = HopVfs.getInputStream(filename)) {
      return JSON.readValue(in, Map.class);
    } catch (Exception e) {
      throw new HopException("Unable to read the state of the action before from " + filename, e);
    }
  }

  static void writeState(String filename, Map<String, Object> state) throws HopException {
    try (OutputStream out = HopVfs.getOutputStream(filename, false)) {
      JSON.writeValue(out, state);
    } catch (Exception e) {
      throw new HopException("Unable to write the state of the action to " + filename, e);
    }
  }
}
