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

package org.apache.hop.pipeline.config;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.profiling.ExecutionDataProfile;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;

@HopMetadata(
    key = "pipeline-run-configuration",
    name = "i18n::PipelineRunConfiguration.name",
    description = "i18n::PipelineRunConfiguration.description",
    image = "ui/images/pipeline_run_config.svg",
    documentationUrl = "/metadata-types/pipeline-run-config.html",
    hopMetadataPropertyType = HopMetadataPropertyType.PIPELINE_RUN_CONFIG)
public class PipelineRunConfiguration extends HopMetadataBase implements Cloneable, IHopMetadata {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID =
      "PipelineRunConfiguration-PluginSpecific-Options";

  @HopMetadataProperty private String description;

  /** The name of the location to send execution information to */
  @HopMetadataProperty private String executionInfoLocationName;

  @HopMetadataProperty private List<DescribedVariable> configurationVariables;

  @HopMetadataProperty private IPipelineEngineRunConfiguration engineRunConfiguration;

  /** The name of an {@link ExecutionDataProfile} */
  @HopMetadataProperty(key = "dataProfile")
  protected String executionDataProfileName;

  @HopMetadataProperty protected boolean defaultSelection;

  public PipelineRunConfiguration() {
    configurationVariables = new ArrayList<>();
  }

  public PipelineRunConfiguration(
      String name,
      String description,
      String executionInfoLocationName,
      List<DescribedVariable> configurationVariables,
      IPipelineEngineRunConfiguration engineRunConfiguration,
      String executionDataProfileName,
      boolean defaultSelection) {
    this.name = name;
    this.description = description;
    this.executionInfoLocationName = executionInfoLocationName;
    this.configurationVariables = configurationVariables;
    this.engineRunConfiguration = engineRunConfiguration;
    this.executionDataProfileName = executionDataProfileName;
    this.defaultSelection = defaultSelection;
  }

  public PipelineRunConfiguration(PipelineRunConfiguration runConfiguration) {
    this();
    this.name = runConfiguration.name;
    this.description = runConfiguration.description;
    this.executionInfoLocationName = runConfiguration.executionInfoLocationName;
    this.configurationVariables.addAll(runConfiguration.getConfigurationVariables());
    if (runConfiguration.getEngineRunConfiguration() != null) {
      this.engineRunConfiguration = runConfiguration.engineRunConfiguration.clone();
    }
    this.executionDataProfileName = runConfiguration.executionDataProfileName;
    this.defaultSelection = runConfiguration.defaultSelection;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Gets executionInfoLocationName
   *
   * @return value of executionInfoLocationName
   */
  public String getExecutionInfoLocationName() {
    return executionInfoLocationName;
  }

  /**
   * Sets executionInfoLocationName
   *
   * @param executionInfoLocationName value of executionInfoLocationName
   */
  public void setExecutionInfoLocationName(String executionInfoLocationName) {
    this.executionInfoLocationName = executionInfoLocationName;
  }

  /**
   * Gets configurationVariables
   *
   * @return value of configurationVariables
   */
  public List<DescribedVariable> getConfigurationVariables() {
    return configurationVariables;
  }

  /**
   * @param configurationVariables The configurationVariables to set
   */
  public void setConfigurationVariables(List<DescribedVariable> configurationVariables) {
    this.configurationVariables = configurationVariables;
  }

  /**
   * Gets engineRunConfiguration
   *
   * @return value of engineRunConfiguration
   */
  public IPipelineEngineRunConfiguration getEngineRunConfiguration() {
    return engineRunConfiguration;
  }

  /**
   * @param engineRunConfiguration The engineRunConfiguration to set
   */
  public void setEngineRunConfiguration(IPipelineEngineRunConfiguration engineRunConfiguration) {
    this.engineRunConfiguration = engineRunConfiguration;
  }

  /**
   * Gets executionDataProfileName
   *
   * @return value of executionDataProfileName
   */
  public String getExecutionDataProfileName() {
    return executionDataProfileName;
  }

  /**
   * Sets executionDataProfileName
   *
   * @param executionDataProfileName value of executionDataProfileName
   */
  public void setExecutionDataProfileName(String executionDataProfileName) {
    this.executionDataProfileName = executionDataProfileName;
  }

  /**
   * Gets defaultSelection
   *
   * @return value of defaultSelection
   */
  public boolean isDefaultSelection() {
    return defaultSelection;
  }

  /**
   * Sets defaultSelection
   *
   * @param defaultSelection value of defaultSelection
   */
  public void setDefaultSelection(boolean defaultSelection) {
    this.defaultSelection = defaultSelection;
  }

  public void applyToVariables(IVariables variables) {
    for (DescribedVariable vvd : configurationVariables) {
      if (StringUtils.isNotEmpty(vvd.getName())) {
        variables.setVariable(vvd.getName(), variables.resolve(vvd.getValue()));
      }
    }
  }

  /**
   * Find the first default run configuration in the metadata and return it. Return null if there's
   * no default.
   *
   * @param metadataProvider
   * @return The default run configuration or null if none is specified.
   * @throws HopException
   */
  public static final PipelineRunConfiguration findDefault(IHopMetadataProvider metadataProvider)
      throws HopException {
    for (PipelineRunConfiguration runConfiguration :
        metadataProvider.getSerializer(PipelineRunConfiguration.class).loadAll()) {
      if (runConfiguration.isDefaultSelection()) {
        return runConfiguration;
      }
    }

    return null;
  }
}
