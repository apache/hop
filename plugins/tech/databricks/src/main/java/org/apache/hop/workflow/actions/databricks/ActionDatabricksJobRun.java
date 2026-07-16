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
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.hop.databricks.deploy.DatabricksJobSpecFactory;
import org.apache.hop.databricks.deploy.HopSparkDeployHelper;
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
 * Run or submit a Databricks job and optionally wait for completion. Sets result variables for Job
 * ID, Run ID, and status.
 */
@Action(
    id = "DATABRICKS_JOB_RUN",
    name = "i18n::ActionDatabricksJobRun.Name",
    description = "i18n::ActionDatabricksJobRun.Description",
    image = "databricks-job-run.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.BigData",
    keywords = "i18n::ActionDatabricksJobRun.keyword",
    documentationUrl = "/workflow/actions/databricks-job-run.html")
@Getter
@Setter
public class ActionDatabricksJobRun extends ActionBase implements Cloneable, IAction {

  private static final Class<?> PKG = ActionDatabricksJobRun.class;

  public static final String MODE_RUN_EXISTING = "RUN_EXISTING";
  public static final String MODE_SUBMIT_ONCE = "SUBMIT_ONCE";
  public static final String MODE_DEPLOY_AND_RUN = "DEPLOY_AND_RUN";

  public static final String WAIT_WAIT = "WAIT";
  public static final String WAIT_FIRE_AND_FORGET = "FIRE_AND_FORGET";

  @HopMetadataProperty(key = "connection")
  private String connectionName;

  /** {@link #MODE_RUN_EXISTING}, {@link #MODE_SUBMIT_ONCE}, or {@link #MODE_DEPLOY_AND_RUN}. */
  @HopMetadataProperty(key = "run_mode")
  private String runMode = MODE_RUN_EXISTING;

  @HopMetadataProperty(key = "job_id")
  private String jobId;

  /** Raw Jobs API JSON for runs/submit (one-time). */
  @HopMetadataProperty(key = "submit_json")
  private String submitRunJson;

  /** Local fat jar path for deploy mode. */
  @HopMetadataProperty(key = "fat_jar")
  private String fatJarPath;

  /** Pipeline .hpl path for deploy mode. */
  @HopMetadataProperty(key = "pipeline_file")
  private String pipelineFilename;

  /** Native Spark pipeline run configuration name (inside exported metadata). */
  @HopMetadataProperty(key = "run_configuration")
  private String runConfigurationName;

  /** DBFS directory for uploads, e.g. dbfs:/FileStore/hop/my-job */
  @HopMetadataProperty(key = "dbfs_base")
  private String dbfsBasePath = "dbfs:/FileStore/hop";

  /** Existing Databricks cluster id for the JAR task. */
  @HopMetadataProperty(key = "cluster_id")
  private String existingClusterId;

  /** Job name when creating a new job (deploy mode). */
  @HopMetadataProperty(key = "job_name")
  private String jobName;

  /**
   * When true and job_id is set, reset the existing job after upload; otherwise create a new job
   * (job_id empty) or run-now only if create fails... actually: update if job_id non-empty.
   */
  @HopMetadataProperty(key = "update_existing_job")
  private boolean updateExistingJob;

  /** {@link #WAIT_WAIT} or {@link #WAIT_FIRE_AND_FORGET}. */
  @HopMetadataProperty(key = "wait_mode")
  private String waitMode = WAIT_WAIT;

  /** Seconds; 0 = no timeout. */
  @HopMetadataProperty(key = "timeout_seconds")
  private String timeoutSeconds = "3600";

  @HopMetadataProperty(key = "poll_seconds")
  private String pollIntervalSeconds = "15";

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

  /** Optional factory for tests. */
  private transient ClientFactory clientFactory = RestDatabricksJobsClient::create;

  @FunctionalInterface
  public interface ClientFactory {
    DatabricksJobsClient create(DatabricksConnection connection, IVariables variables)
        throws HopException;
  }

  public ActionDatabricksJobRun(String name) {
    super(name, "");
  }

  public ActionDatabricksJobRun() {
    this("");
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  public void setClientFactory(ClientFactory clientFactory) {
    this.clientFactory = clientFactory != null ? clientFactory : RestDatabricksJobsClient::create;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);
    result.setNrErrors(1);
    clearResultVariables();

    try {
      String connName = resolve(connectionName);
      if (Utils.isEmpty(connName)) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionDatabricksJobRun.Error.NoConnection"));
      }
      IHopMetadataProvider metadataProvider = getMetadataProvider();
      if (metadataProvider == null) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionDatabricksJobRun.Error.NoMetadataProvider"));
      }
      DatabricksConnection connection =
          metadataProvider.getSerializer(DatabricksConnection.class).load(connName);
      if (connection == null) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "ActionDatabricksJobRun.Error.ConnectionNotFound", connName));
      }

      try (DatabricksJobsClient client = clientFactory.create(connection, this)) {
        long runId;
        Long jobIdValue = null;
        String mode = StringUtils.defaultIfBlank(runMode, MODE_RUN_EXISTING).trim();

        if (MODE_DEPLOY_AND_RUN.equalsIgnoreCase(mode)) {
          DeployOutcome outcome = deployAndRun(client, metadataProvider);
          jobIdValue = outcome.jobId();
          runId = outcome.runId();
        } else if (MODE_SUBMIT_ONCE.equalsIgnoreCase(mode)) {
          String json = resolve(submitRunJson);
          if (Utils.isEmpty(json)) {
            throw new HopException(
                BaseMessages.getString(PKG, "ActionDatabricksJobRun.Error.NoSubmitJson"));
          }
          runId = client.submitRun(json);
          if (isDetailed()) {
            logDetailed("Submitted one-time Databricks run, run_id=" + runId);
          }
        } else {
          String jobIdStr = resolve(jobId);
          if (Utils.isEmpty(jobIdStr)) {
            throw new HopException(
                BaseMessages.getString(PKG, "ActionDatabricksJobRun.Error.NoJobId"));
          }
          long jid = Long.parseLong(jobIdStr.trim());
          jobIdValue = jid;
          runId = client.runNow(jid, Map.of());
          if (isDetailed()) {
            logDetailed("Triggered Databricks job_id=" + jid + ", run_id=" + runId);
          }
        }

        setResultVariable(resultVariableJobId, jobIdValue != null ? Long.toString(jobIdValue) : "");
        setResultVariable(resultVariableRunId, Long.toString(runId));
        setResultVariable(resultVariableStatus, DatabricksRunLifeCycleState.PENDING.name());
        setResultVariable(resultVariableError, "");

        String wait = StringUtils.defaultIfBlank(waitMode, WAIT_WAIT).trim();
        if (WAIT_FIRE_AND_FORGET.equalsIgnoreCase(wait)) {
          result.setNrErrors(0);
          result.setResult(true);
          return result;
        }

        DatabricksRunStatus status = waitForRun(client, runId);
        applyStatusVariables(status, jobIdValue);

        if (status.isSuccess()) {
          result.setNrErrors(0);
          result.setResult(true);
        } else if (status.getLifeCycleState() == DatabricksRunLifeCycleState.TERMINATED
            && "CANCELED".equalsIgnoreCase(status.getResultState())) {
          result.setNrErrors(1);
          result.setResult(false);
          logError("Databricks run canceled: " + Const.NVL(status.getStateMessage(), ""));
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
      logError(BaseMessages.getString(PKG, "ActionDatabricksJobRun.Error.Execute"), e);
    }

    return result;
  }

  private record DeployOutcome(long jobId, long runId) {}

  private DeployOutcome deployAndRun(
      DatabricksJobsClient client, IHopMetadataProvider metadataProvider) throws HopException {
    String cluster = resolve(existingClusterId);
    if (Utils.isEmpty(cluster)) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionDatabricksJobRun.Error.NoClusterId"));
    }
    String name = resolve(jobName);
    if (Utils.isEmpty(name)) {
      name = "hop-" + resolve(runConfigurationName);
    }

    HopSparkDeployHelper.DeployedArtifacts artifacts =
        HopSparkDeployHelper.deploy(
            client,
            metadataProvider,
            this,
            getLogChannel(),
            fatJarPath,
            pipelineFilename,
            runConfigurationName,
            dbfsBasePath);

    long jid;
    String jobIdStr = resolve(jobId);
    if (updateExistingJob && StringUtils.isNotBlank(jobIdStr)) {
      jid = Long.parseLong(jobIdStr.trim());
      String resetJson =
          DatabricksJobSpecFactory.buildResetJobJson(
              jid,
              name,
              cluster,
              artifacts.jarDbfs(),
              artifacts.pipelineDbfs(),
              artifacts.metadataDbfs(),
              artifacts.runConfigName());
      client.resetJob(resetJson);
      if (isBasic()) {
        logBasic("Updated Databricks job_id=" + jid);
      }
    } else {
      String createJson =
          DatabricksJobSpecFactory.buildCreateJobJson(
              name,
              cluster,
              artifacts.jarDbfs(),
              artifacts.pipelineDbfs(),
              artifacts.metadataDbfs(),
              artifacts.runConfigName());
      jid = client.createJob(createJson);
      if (isBasic()) {
        logBasic("Created Databricks job_id=" + jid);
      }
    }

    long rid = client.runNow(jid, Map.of());
    if (isBasic()) {
      logBasic("Started Databricks job_id=" + jid + " run_id=" + rid);
    }
    return new DeployOutcome(jid, rid);
  }

  private DatabricksRunStatus waitForRun(DatabricksJobsClient client, long runId)
      throws HopException, InterruptedException {
    int timeoutSec = Const.toInt(resolve(timeoutSeconds), 3600);
    int pollSec = Const.toInt(resolve(pollIntervalSeconds), 15);
    return DatabricksRunWaiter.waitFor(
        client,
        runId,
        timeoutSec,
        pollSec,
        true,
        new DatabricksRunWaiter.Hooks() {
          @Override
          public boolean isStopped() {
            return parentWorkflow != null && parentWorkflow.isStopped();
          }

          @Override
          public void onStatus(DatabricksRunStatus status) {
            applyStatusVariables(status, status.getJobId());
          }

          @Override
          public void logDetailed(String message) {
            if (isDetailed()) {
              ActionDatabricksJobRun.this.logDetailed(message);
            }
          }

          @Override
          public void logError(String message) {
            ActionDatabricksJobRun.this.logError(message);
          }
        });
  }

  private void applyStatusVariables(DatabricksRunStatus status, Long jobIdFallback) {
    if (status.getJobId() != null) {
      setResultVariable(resultVariableJobId, Long.toString(status.getJobId()));
    } else if (jobIdFallback != null) {
      setResultVariable(resultVariableJobId, Long.toString(jobIdFallback));
    }
    setResultVariable(resultVariableRunId, Long.toString(status.getRunId()));
    setResultVariable(resultVariableStatus, status.toStatusVariable());
    if (status.getRunPageUrl() != null) {
      setResultVariable(resultVariablePageUrl, status.getRunPageUrl());
    }
  }

  private void clearResultVariables() {
    setResultVariable(resultVariableJobId, "");
    setResultVariable(resultVariableRunId, "");
    setResultVariable(resultVariableStatus, "");
    setResultVariable(resultVariablePageUrl, "");
    setResultVariable(resultVariableError, "");
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
