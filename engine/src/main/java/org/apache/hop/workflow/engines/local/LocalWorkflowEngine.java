/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.engines.local;

import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.IExtensionData;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.map.DatabaseConnectionMap;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.ExecutionBuilder;
import org.apache.hop.execution.ExecutionDataBuilder;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionStateBuilder;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.workflow.IActionListener;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEnginePlugin;

@WorkflowEnginePlugin(
    id = "Local",
    name = "Hop local workflow engine",
    description = "Executes your workflow locally")
public class LocalWorkflowEngine extends Workflow implements IWorkflowEngine<WorkflowMeta> {

  private ExecutionInfoLocation executionInfoLocation;
  private Timer executionInfoTimer;

  public LocalWorkflowEngine() {
    super();
    setDefaultRunConfiguration();
  }

  public LocalWorkflowEngine(WorkflowMeta workflowMeta) {
    super(workflowMeta);
    setDefaultRunConfiguration();
  }

  public LocalWorkflowEngine(WorkflowMeta workflowMeta, ILoggingObject parent) {
    super(workflowMeta, parent);
    setDefaultRunConfiguration();
  }

  @Override
  public IWorkflowEngineRunConfiguration createDefaultWorkflowEngineRunConfiguration() {
    return new LocalWorkflowRunConfiguration();
  }

  private void setDefaultRunConfiguration() {
    setWorkflowRunConfiguration(
        new WorkflowRunConfiguration(
            "local", "", "", createDefaultWorkflowEngineRunConfiguration(), false));
  }

  @Override
  public Result startExecution() {

    if (!(workflowRunConfiguration.getEngineRunConfiguration()
        instanceof LocalWorkflowRunConfiguration)) {
      log.logError(
          "Error starting workflow",
          new HopException(
              "A local workflow execution expects a local workflow configuration, not an instance of class "
                  + workflowRunConfiguration.getEngineRunConfiguration().getClass().getName()));
      result = new Result();
      result.setNrErrors(1L);
      return result;
    }

    LocalWorkflowRunConfiguration config =
        (LocalWorkflowRunConfiguration) workflowRunConfiguration.getEngineRunConfiguration();

    // See if we need to enable transactions...
    //
    IExtensionData parentExtensionData = getParentPipeline();
    if (parentExtensionData == null) {
      parentExtensionData = getParentWorkflow();
    }
    String connectionGroup = null;
    if (parentExtensionData != null && parentExtensionData.getExtensionDataMap() != null) {
      connectionGroup =
          (String) parentExtensionData.getExtensionDataMap().get(Const.CONNECTION_GROUP);
    }

    // Create a new transaction group?
    //
    if (config.isTransactional() && connectionGroup == null) {
      // Store a value in the parent...
      //
      connectionGroup = getWorkflowMeta().getName() + " - " + UUID.randomUUID();

      // We also need to commit/rollback at the end of this workflow...
      //
      addExecutionFinishedListener(
          workflow -> {
            String group = (String) workflow.getExtensionDataMap().get(Const.CONNECTION_GROUP);
            List<Database> databases = DatabaseConnectionMap.getInstance().getDatabases(group);
            Result result = workflow.getResult();

            for (Database database : databases) {
              // All fine?  Commit!
              //
              try {
                if (result.isResult() && !result.isStopped() && result.getNrErrors() == 0) {
                  try {
                    database.commit(true);
                    workflow
                        .getLogChannel()
                        .logBasic(
                            "All transactions of database connection '"
                                + database.getDatabaseMeta().getName()
                                + "' were committed at the end of the workflow!");
                  } catch (HopDatabaseException e) {
                    workflow
                        .getLogChannel()
                        .logError(
                            "Error committing database connection "
                                + database.getDatabaseMeta().getName(),
                            e);
                    result.setNrErrors(result.getNrErrors() + 1);
                  }
                } else {
                  // Error? Rollback!
                  try {
                    database.rollback(true);
                    workflow
                        .getLogChannel()
                        .logBasic(
                            "All transactions of database connection '"
                                + database.getDatabaseMeta().getName()
                                + "' were rolled back at the end of the workflow!");
                  } catch (HopDatabaseException e) {
                    workflow
                        .getLogChannel()
                        .logError(
                            "Error rolling back database connection "
                                + database.getDatabaseMeta().getName(),
                            e);
                    result.setNrErrors(result.getNrErrors() + 1);
                  }
                }
              } finally {
                // Always close connection!
                try {
                  database.closeConnectionOnly();
                  workflow
                      .getLogChannel()
                      .logDebug(
                          "Database connection '"
                              + database.getDatabaseMeta().getName()
                              + "' closed successfully!");
                } catch (HopDatabaseException hde) {
                  workflow
                      .getLogChannel()
                      .logError(
                          "Error disconnecting from database - closeConnectionOnly failed:"
                              + Const.CR
                              + hde.getMessage());
                  workflow.getLogChannel().logError(Const.getStackTracker(hde));
                }
                // Definitely remove the connection reference the connections map
                DatabaseConnectionMap.getInstance().removeConnection(group, null, database);
              }
            }
          });
    }

    // Signal that we're dealing with a connection group
    // We'll have extension points to pass the connection group down the hierarchy: workflow -
    // action - pipeline - transform - database
    //
    if (connectionGroup != null && getExtensionDataMap() != null) {
      // Set the connection group for this workflow
      //
      getExtensionDataMap().put(Const.CONNECTION_GROUP, connectionGroup);
    }

    // Pass down the value of the connection group value to actions before they're executed...
    //
    addActionListener(
        new IActionListener() {
          @Override
          public void beforeExecution(
              IWorkflowEngine workflow, ActionMeta actionMeta, IAction action) {
            String connectionGroup =
                (String) workflow.getExtensionDataMap().get(Const.CONNECTION_GROUP);
            if (connectionGroup != null) {
              action.getExtensionDataMap().put(Const.CONNECTION_GROUP, connectionGroup);
            }
          }

          @Override
          public void afterExecution(
              IWorkflowEngine workflow, ActionMeta actionMeta, IAction action, Result result) {
            // Nothing
          }
        });

    // Do the lookup of the execution information only once
    lookupExecutionInformationLocation();

    addExecutionStartedListener(
        l -> {
          // Register the pipeline after start
          //
          registerWorkflowExecutionInformation();

          // Start the update timer for the execution information
          //
          startExecutionInfoTimer();
        });

    return super.startExecution();
  }

  /** This method looks up the execution information location specified in the run configuration. */
  public void lookupExecutionInformationLocation() {
    try {
      String locationName = resolve(workflowRunConfiguration.getExecutionInfoLocationName());
      if (StringUtils.isNotEmpty(locationName)) {
        ExecutionInfoLocation location =
            metadataProvider.getSerializer(ExecutionInfoLocation.class).load(locationName);
        if (location != null) {
          executionInfoLocation = location;

          IExecutionInfoLocation iLocation = executionInfoLocation.getExecutionInfoLocation();
          // Initialize the location.
          // This location is closed when nothing else needs to be done.  This is when the timer is
          // stopped in
          // stopExecutionInfoTimer().
          //
          iLocation.initialize(this, metadataProvider);
        } else {
          log.logError(
              "Execution information location '"
                  + locationName
                  + "' could not be found in the metadata (non-fatal)");
        }
      }
    } catch (Exception e) {
      log.logError("Error looking up execution information location (non-fatal error)", e);
    }
  }

  /**
   * If needed, register this workflow at the specified execution information location. The name of
   * the location is specified in the run configuration.
   */
  public void registerWorkflowExecutionInformation() {
    try {
      if (executionInfoLocation != null) {
        // Register the execution at this location
        // This adds metadata, variables, parameters, ...
        executionInfoLocation
            .getExecutionInfoLocation()
            .registerExecution(ExecutionBuilder.fromExecutor(this).build());
      }
    } catch (Exception e) {
      log.logError("Error registering workflow execution information (non-fatal)", e);
    }
  }

  public void startExecutionInfoTimer() {
    if (executionInfoLocation == null) {
      return;
    }

    long delay = Const.toLong(resolve(executionInfoLocation.getDataLoggingDelay()), 2000L);
    long interval = Const.toLong(resolve(executionInfoLocation.getDataLoggingInterval()), 5000L);
    final AtomicInteger lastLogLineNr = new AtomicInteger(0);

    final IExecutionInfoLocation iLocation = executionInfoLocation.getExecutionInfoLocation();

    final IVariables referenceVariables = new Variables();
    referenceVariables.copyFrom(this);

    //
    TimerTask sampleTask =
        new TimerTask() {
          @Override
          public void run() {
            try {
              // Update the workflow execution state regularly
              //
              ExecutionState executionState =
                  ExecutionStateBuilder.fromExecutor(LocalWorkflowEngine.this, lastLogLineNr.get())
                      .build();
              iLocation.updateExecutionState(executionState);
              if (executionState.getLastLogLineNr() != null) {
                lastLogLineNr.set(executionState.getLastLogLineNr());
              }
            } catch (Exception e) {
              throw new RuntimeException(
                  "Error registering execution info data from transforms at location "
                      + executionInfoLocation.getName(),
                  e);
            }
          }
        };

    // After every action is finished, grab the information...
    //
    addActionListener(new ExecutionInfoActionListener(iLocation, referenceVariables, log));

    // Schedule the task to run regularly
    //
    executionInfoTimer = new Timer("LocalWorkflowEngine execution timer: " + getWorkflowName());
    executionInfoTimer.schedule(sampleTask, delay, interval);

    // When the workflow is done, register one more time and stop the timer
    //
    addExecutionFinishedListener(
        listener -> {
          stopExecutionInfoTimer();
        });
  }

  private static class ExecutionInfoActionListener implements IActionListener<WorkflowMeta> {
    private final IExecutionInfoLocation iLocation;
    private final IVariables referenceVariables;
    private final ILogChannel log;

    private IVariables beforeVariables;

    public ExecutionInfoActionListener(
        IExecutionInfoLocation iLocation, IVariables referenceVariables, ILogChannel log) {
      this.iLocation = iLocation;
      this.referenceVariables = referenceVariables;
      this.log = log;
      this.beforeVariables = null;
    }

    @Override
    public void beforeExecution(
        IWorkflowEngine<WorkflowMeta> workflow, ActionMeta actionMeta, IAction action) {

      // Keep the variables from before the action started...
      //
      beforeVariables = new Variables();
      beforeVariables.copyFrom(workflow);

      try {
        // Register the execution of the action
        //
        iLocation.registerExecution(
            ExecutionBuilder.of()
                .withId(action.getLogChannel().getLogChannelId())
                .withParentId(workflow.getLogChannelId())
                .withExecutionStartDate(new Date())
                .withRegistrationDate(new Date())
                .withExecutorType(ExecutionType.Action)
                .withLogLevel(action.getLogChannel().getLogLevel())
                .build());

        // Register all the data we can for this action
        iLocation.registerData(
            ExecutionDataBuilder.beforeActionExecution(
                    workflow, actionMeta, action, referenceVariables)
                .build());

        // Also register the execution node itself
        //
        iLocation.registerExecution(
            ExecutionBuilder.fromAction(workflow, actionMeta, action, new Date()).build());
      } catch (Exception e) {
        log.logError(
            "Error registering execution data before action "
                + actionMeta.getName()
                + " (non-fatal)",
            e);
      }
    }

    @Override
    public void afterExecution(
        IWorkflowEngine<WorkflowMeta> workflow,
        ActionMeta actionMeta,
        IAction action,
        Result result) {
      ExecutionDataBuilder dataBuilder =
          ExecutionDataBuilder.afterActionExecution(
              workflow, actionMeta, action, result, referenceVariables, beforeVariables);
      try {
        iLocation.registerData(dataBuilder.build());
      } catch (Exception e) {
        log.logError(
            "Error registering execution data after action "
                + actionMeta.getName()
                + " (non-fatal)",
            e);
      }
    }
  }

  public void stopExecutionInfoTimer() throws HopException {
    ExecutorUtil.cleanup(executionInfoTimer);

    if (executionInfoLocation == null) {
      return;
    }

    try {
      IExecutionInfoLocation iLocation = executionInfoLocation.getExecutionInfoLocation();

      // Register one final last state of the workflow
      //
      ExecutionState executionState =
          ExecutionStateBuilder.fromExecutor(LocalWorkflowEngine.this, -1).build();
      iLocation.updateExecutionState(executionState);
    } finally {
      // Nothing more needs to be done. We can now close the location.
      //
      executionInfoLocation.getExecutionInfoLocation().close();
    }
  }
}
