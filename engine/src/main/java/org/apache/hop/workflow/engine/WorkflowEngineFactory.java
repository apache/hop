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

package org.apache.hop.workflow.engine;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;

public class WorkflowEngineFactory {

  public static final <T extends WorkflowMeta> IWorkflowEngine<T> createWorkflowEngine(
      IVariables variables,
      String runConfigurationName,
      IHopMetadataProvider metadataProvider,
      T workflowMeta,
      ILoggingObject parentLogging)
      throws HopException {

    if (StringUtils.isEmpty(runConfigurationName)) {
      throw new HopException(
          "You need to specify a workflow run configuration to execute this workflow");
    }
    WorkflowRunConfiguration runConfiguration;
    try {
      runConfiguration =
          metadataProvider.getSerializer(WorkflowRunConfiguration.class).load(runConfigurationName);
    } catch (Exception e) {
      throw new HopException(
          "Error loading workflow run configuration '" + runConfigurationName + "'", e);
    }
    if (runConfiguration == null) {
      throw new HopException(
          "Workflow run configuration '" + runConfigurationName + "' could not be found");
    }
    IWorkflowEngine<T> workflowEngine =
        createWorkflowEngine(runConfiguration, workflowMeta, parentLogging);

    // Copy the variables from the metadata
    //
    workflowEngine.initializeFrom(variables);

    workflowEngine.setInternalHopVariables();

    // Copy the parameters from the metadata...
    //
    workflowEngine.copyParametersFromDefinitions( workflowMeta );

    // Pass the metadata providers around to make sure
    //
    workflowEngine.setMetadataProvider(metadataProvider);
    workflowMeta.setMetadataProvider(metadataProvider);

    return workflowEngine;
  }

  private static final <T extends WorkflowMeta> IWorkflowEngine<T> createWorkflowEngine(
      WorkflowRunConfiguration workflowRunConfiguration,
      T workflowMeta,
      ILoggingObject parentLogging)
      throws HopException {
    IWorkflowEngineRunConfiguration engineRunConfiguration =
        workflowRunConfiguration.getEngineRunConfiguration();
    if (engineRunConfiguration == null) {
      throw new HopException(
          "There is no pipeline execution engine specified in run configuration '"
              + workflowRunConfiguration.getName()
              + "'");
    }
    String enginePluginId = engineRunConfiguration.getEnginePluginId();

    // Load this engine from the plugin registry
    //
    PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    IPlugin plugin =
        pluginRegistry.findPluginWithId(WorkflowEnginePluginType.class, enginePluginId);
    if (plugin == null) {
      throw new HopException(
          "Unable to find pipeline engine plugin type with ID '" + enginePluginId + "'");
    }

    IWorkflowEngine<T> workflowEngine = pluginRegistry.loadClass(plugin, IWorkflowEngine.class);
    workflowEngine.setWorkflowRunConfiguration(workflowRunConfiguration);

    if (workflowEngine instanceof LocalWorkflowEngine) {
      ((LocalWorkflowEngine) workflowEngine).setParentLoggingObject(parentLogging);
    }
    workflowEngine.setWorkflowMeta(workflowMeta);

    return workflowEngine;
  }
}
