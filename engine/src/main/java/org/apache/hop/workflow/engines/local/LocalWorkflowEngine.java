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

import org.apache.hop.core.Const;
import org.apache.hop.core.IExtensionData;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.map.DatabaseConnectionMap;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.workflow.IActionListener;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEnginePlugin;

import java.util.List;
import java.util.UUID;

@WorkflowEnginePlugin(
    id = "Local",
    name = "Hop local workflow engine",
    description = "Executes your workflow locally")
public class LocalWorkflowEngine extends Workflow implements IWorkflowEngine<WorkflowMeta> {

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
        new WorkflowRunConfiguration("local", "", createDefaultWorkflowEngineRunConfiguration()));
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
      addWorkflowFinishedListener(
          workflow -> {
            String group = (String) workflow.getExtensionDataMap().get(Const.CONNECTION_GROUP);
            List<Database> databases = DatabaseConnectionMap.getInstance().getDatabases(group);
            Result result = workflow.getResult();

            for (Database database : databases) {
              // All fine?  Commit!
              //
              try {
                if (result.getResult() && !result.isStopped() && result.getNrErrors() == 0) {
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
                //Definitely remove the connection reference the connections map
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

    return super.startExecution();
  }
}
