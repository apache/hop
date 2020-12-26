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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.VariableValueDescription;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

import java.util.ArrayList;
import java.util.List;

@HopMetadata(
  key = "pipeline-run-configuration",
  name = "Pipeline Run Configuration",
  description = "Describes how and with which engine a pipeline is to be executed",
  image = "ui/images/pipeline_run_config.svg"
)
public class PipelineRunConfiguration extends HopMetadataBase implements Cloneable, IHopMetadata {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "PipelineRunConfiguration-PluginSpecific-Options";

  @HopMetadataProperty
  private String description;

  @HopMetadataProperty
  private List<VariableValueDescription> configurationVariables;

  @HopMetadataProperty
  private IPipelineEngineRunConfiguration engineRunConfiguration;

  public PipelineRunConfiguration() {
    configurationVariables = new ArrayList<>();
  }

  public PipelineRunConfiguration( String name, String description, List<VariableValueDescription> configurationVariables,
                                   IPipelineEngineRunConfiguration engineRunConfiguration ) {
    this.name = name;
    this.description = description;
    this.configurationVariables = configurationVariables;
    this.engineRunConfiguration = engineRunConfiguration;
  }

  public PipelineRunConfiguration( PipelineRunConfiguration runConfiguration ) {
    this();
    this.name = runConfiguration.name;
    this.description = runConfiguration.description;
    this.configurationVariables.addAll( runConfiguration.getConfigurationVariables() );
    if ( runConfiguration.getEngineRunConfiguration() != null ) {
      this.engineRunConfiguration = runConfiguration.engineRunConfiguration.clone();
    }
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
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets configurationVariables
   *
   * @return value of configurationVariables
   */
  public List<VariableValueDescription> getConfigurationVariables() {
    return configurationVariables;
  }

  /**
   * @param configurationVariables The configurationVariables to set
   */
  public void setConfigurationVariables( List<VariableValueDescription> configurationVariables ) {
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
  public void setEngineRunConfiguration( IPipelineEngineRunConfiguration engineRunConfiguration ) {
    this.engineRunConfiguration = engineRunConfiguration;
  }

  public void applyToVariables( IVariables variables ) {
    for ( VariableValueDescription vvd : configurationVariables ) {
      if ( StringUtils.isNotEmpty( vvd.getName() ) ) {
        variables.setVariable( vvd.getName(), variables.resolve( vvd.getValue() ) );
      }
    }
  }
}
