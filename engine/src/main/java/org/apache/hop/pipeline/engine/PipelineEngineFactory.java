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

package org.apache.hop.pipeline.engine;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.VariableValueDescription;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;

import java.util.List;

public class PipelineEngineFactory {

  /**
   * Create a new pipeline engine
   *
   * @param parentVariables The parent variables to use and pass on to the pipeline engine. They
   *     will not be changed.
   * @param runConfigurationName The run configuration to use
   * @param metadataProvider
   * @param pipelineMeta
   * @param <T>
   * @return
   * @throws HopException
   */
  public static final <T extends PipelineMeta> IPipelineEngine<T> createPipelineEngine(
      IVariables parentVariables,
      String runConfigurationName,
      IHopMetadataProvider metadataProvider,
      T pipelineMeta)
      throws HopException {

    if (StringUtils.isEmpty(runConfigurationName)) {
      throw new HopException("Please specify a run configuration to execute the pipeline with");
    }
    PipelineRunConfiguration pipelineRunConfiguration;
    try {
      pipelineRunConfiguration =
          metadataProvider.getSerializer(PipelineRunConfiguration.class).load(runConfigurationName);
    } catch (HopException e) {
      throw new HopException(
          "Error loading the pipeline run configuration '" + runConfigurationName + "'", e);
    }
    if (pipelineRunConfiguration == null) {
      throw new HopException(
          "Unable to find the specified pipeline run configuration '"
              + runConfigurationName
              + "' in metadata provider: "
              + metadataProvider.getDescription());
    }

    // Apply the variables from the run configuration
    //
    IVariables variables = new Variables();
    variables.copyFrom(parentVariables);
    pipelineRunConfiguration.applyToVariables(variables);

    IPipelineEngine<T> pipelineEngine =
        createPipelineEngine(pipelineRunConfiguration, pipelineMeta);

    // inherit variables from the metadata
    //
    pipelineEngine.initializeFrom(variables);

    // Set internal variables...
    //
    pipelineMeta.setInternalHopVariables(pipelineEngine);

    // Copy over the parameter definitions
    //
    pipelineEngine.copyParametersFromDefinitions(pipelineMeta);

    // Apply the variables in the pipeline run configuration
    //
    applyVariableDefinitions(pipelineEngine, pipelineRunConfiguration.getConfigurationVariables());

    // Pass the metadata to make sure
    //
    pipelineEngine.setMetadataProvider(metadataProvider);
    pipelineMeta.setMetadataProvider(metadataProvider);

    return pipelineEngine;
  }

  /**
   * Apply all the variables in the pipeline run configuration... //
   *
   * @param pipelineEngine
   * @param configurationVariables
   * @param <T>
   */
  private static <T extends PipelineMeta> void applyVariableDefinitions(
      IPipelineEngine<T> pipelineEngine, List<VariableValueDescription> configurationVariables) {

    for (VariableValueDescription cv : configurationVariables) {
      if (StringUtils.isNotEmpty(cv.getValue()) && StringUtils.isNotEmpty(cv.getName())) {
        String realValue = pipelineEngine.resolve(cv.getValue());
        pipelineEngine.setVariable(cv.getName(), realValue);
      }
    }
  }

  public static final <T extends PipelineMeta> IPipelineEngine<T> createPipelineEngine(
      PipelineRunConfiguration pipelineRunConfiguration, T pipelineMeta) throws HopException {
    IPipelineEngineRunConfiguration engineRunConfiguration =
        pipelineRunConfiguration.getEngineRunConfiguration();
    if (engineRunConfiguration == null) {
      throw new HopException(
          "There is no pipeline execution engine specified in run configuration '"
              + pipelineRunConfiguration.getName()
              + "'");
    }
    String enginePluginId = engineRunConfiguration.getEnginePluginId();

    // Load this engine from the plugin registry
    //
    PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    IPlugin plugin =
        pluginRegistry.findPluginWithId(PipelineEnginePluginType.class, enginePluginId);
    if (plugin == null) {
      throw new HopException(
          "Unable to find pipeline engine plugin type with ID '" + enginePluginId + "'");
    }

    IPipelineEngine<T> pipelineEngine = pluginRegistry.loadClass(plugin, IPipelineEngine.class);
    pipelineEngine.setPipelineRunConfiguration(pipelineRunConfiguration);

    // Apply the variables in the pipeline run configuration
    //
    applyVariableDefinitions(pipelineEngine, pipelineRunConfiguration.getConfigurationVariables());

    pipelineEngine.setPipelineMeta(pipelineMeta);

    return pipelineEngine;
  }
}
