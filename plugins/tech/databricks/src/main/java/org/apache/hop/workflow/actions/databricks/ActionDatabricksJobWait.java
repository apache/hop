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

package org.apache.hop.workflow.actions.databricks;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databricks.client.DatabricksJobsClient;
import org.apache.hop.databricks.client.DatabricksRunLifeCycleState;
import org.apache.hop.databricks.client.DatabricksRunStatus;
import org.apache.hop.databricks.client.DatabricksRunWaiter;
import org.apache.hop.databricks.client.RestDatabricksJobsClient;
import org.apache.hop.databricks.metadata.DatabricksConnection;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * Poll a Databricks job run until terminal state. Typically used after Job Run in fire-and-forget
 * mode, with Run ID defaulting to {@code ${DatabricksRunId}}.
 */
@Action(
    id = "DATABRICKS_JOB_WAIT",
    name = "i18n::ActionDatabricksJobWait.Name",
    description = "i18n::ActionDatabricksJobWait.Description",
    image = "databricks-wait.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.BigData",
    keywords = "i18n::ActionDatabricksJobWait.keyword",
    documentationUrl = "/workflow/actions/databricks-job-wait.html")
@Getter
@Setter
public class ActionDatabricksJobWait extends ActionBase implements Cloneable, IAction {

  private static final Class<?> PKG = ActionDatabricksJobWait.class;

  @HopMetadataProperty(key = "connection")
  private String connectionName;

  /** Run id to poll; often {@code ${DatabricksRunId}}. */
  @HopMetadataProperty(key = "run_id")
  private String runId = "${DatabricksRunId}";

  @HopMetadataProperty(key = "timeout_seconds")
  private String timeoutSeconds = "3600";

  @HopMetadataProperty(key = "poll_seconds")
  private String pollIntervalSeconds = "15";

  @HopMetadataProperty(key = "cancel_on_stop")
  private boolean cancelOnWorkflowStop = true;

  @HopMetadataProperty(key = "var_job_id")
  private String resultVariableJobId = "DatabricksJobId";

  @HopMetadataProperty(key = "var_run_id")
  private String resultVariableRunId = "DatabricksRunId";

  @HopMetadataProperty(key = "var_status")
  private String resultVariableStatus = "DatabricksStatus";

  @HopMetadataProperty(key = "var_page_url")
  private String resultVariablePageUrl = "DatabricksRunPageUrl";

  @HopMetadataProperty(key = "var_error")
  private String resultVariableError = "DatabricksError";

  private transient ActionDatabricksJobRun.ClientFactory clientFactory =
      RestDatabricksJobsClient::create;

  public ActionDatabricksJobWait(String name) {
    super(name, "");
  }

  public ActionDatabricksJobWait() {
    this("");
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  public void setClientFactory(ActionDatabricksJobRun.ClientFactory clientFactory) {
    this.clientFactory = clientFactory != null ? clientFactory : RestDatabricksJobsClient::create;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);
    result.setNrErrors(1);
    setResultVariable(resultVariableError, "");

    try {
      String connName = resolve(connectionName);
      if (Utils.isEmpty(connName)) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionDatabricksJobWait.Error.NoConnection"));
      }
      IHopMetadataProvider metadataProvider = getMetadataProvider();
      if (metadataProvider == null) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionDatabricksJobWait.Error.NoMetadataProvider"));
      }
      DatabricksConnection connection =
          metadataProvider.getSerializer(DatabricksConnection.class).load(connName);
      if (connection == null) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "ActionDatabricksJobWait.Error.ConnectionNotFound", connName));
      }

      String runIdStr = resolve(runId);
      if (Utils.isEmpty(runIdStr)) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionDatabricksJobWait.Error.NoRunId"));
      }
      long rid = Long.parseLong(runIdStr.trim());

      int timeoutSec = Const.toInt(resolve(timeoutSeconds), 3600);
      int pollSec = Const.toInt(resolve(pollIntervalSeconds), 15);

      try (DatabricksJobsClient client = clientFactory.create(connection, this)) {
        DatabricksRunStatus status =
            DatabricksRunWaiter.waitFor(
                client,
                rid,
                timeoutSec,
                pollSec,
                cancelOnWorkflowStop,
                new DatabricksRunWaiter.Hooks() {
                  @Override
                  public boolean isStopped() {
                    return parentWorkflow != null && parentWorkflow.isStopped();
                  }

                  @Override
                  public void onStatus(DatabricksRunStatus s) {
                    applyStatusVariables(s);
                  }

                  @Override
                  public void logDetailed(String message) {
                    if (isDetailed()) {
                      ActionDatabricksJobWait.this.logDetailed(message);
                    }
                  }

                  @Override
                  public void logError(String message) {
                    ActionDatabricksJobWait.this.logError(message);
                  }
                });

        applyStatusVariables(status);

        if (status.isSuccess()) {
          result.setNrErrors(0);
          result.setResult(true);
        } else if (status.getLifeCycleState() == DatabricksRunLifeCycleState.TERMINATED
            && "CANCELED".equalsIgnoreCase(status.getResultState())) {
          result.setNrErrors(1);
          result.setResult(false);
          logError(
              BaseMessages.getString(
                  PKG,
                  "ActionDatabricksJobWait.Error.Canceled",
                  Const.NVL(status.getStateMessage(), "")));
        } else {
          result.setNrErrors(1);
          result.setResult(false);
          String msg =
              Const.NVL(
                  status.getStateMessage(),
                  "Databricks run ended with status " + status.toStatusVariable());
          setResultVariable(resultVariableError, msg);
          logError(msg);
        }
      }
    } catch (Exception e) {
      result.setNrErrors(1);
      result.setResult(false);
      setResultVariable(
          resultVariableError, e.getMessage() != null ? e.getMessage() : e.toString());
      logError(BaseMessages.getString(PKG, "ActionDatabricksJobWait.Error.Execute"), e);
    }

    return result;
  }

  private void applyStatusVariables(DatabricksRunStatus status) {
    if (status.getJobId() != null) {
      setResultVariable(resultVariableJobId, Long.toString(status.getJobId()));
    }
    setResultVariable(resultVariableRunId, Long.toString(status.getRunId()));
    setResultVariable(resultVariableStatus, status.toStatusVariable());
    if (status.getRunPageUrl() != null) {
      setResultVariable(resultVariablePageUrl, status.getRunPageUrl());
    }
  }

  private void setResultVariable(String name, String value) {
    if (Utils.isEmpty(name)) {
      return;
    }
    String varName = resolve(name);
    String varValue = value == null ? "" : value;
    setVariable(varName, varValue);
    IWorkflowEngine<WorkflowMeta> parent = getParentWorkflow();
    if (parent != null) {
      parent.setVariable(varName, varValue);
      IWorkflowEngine<WorkflowMeta> p = parent.getParentWorkflow();
      while (p != null) {
        p.setVariable(varName, varValue);
        p = p.getParentWorkflow();
      }
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
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "connectionName",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }
}
