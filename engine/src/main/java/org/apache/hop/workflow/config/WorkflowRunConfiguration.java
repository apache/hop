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

package org.apache.hop.workflow.config;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;

@HopMetadata(
    key = "workflow-run-configuration",
    name = "i18n::WorkflowRunConfiguration.name",
    description = "i18n::WorkflowRunConfiguration.description",
    image = "ui/images/workflow_run_config.svg",
    documentationUrl = "/metadata-types/workflow-run-config.html",
    hopMetadataPropertyType = HopMetadataPropertyType.WORKFLOW_RUN_CONFIG)
public class WorkflowRunConfiguration extends HopMetadataBase implements Cloneable, IHopMetadata {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID =
      "WorkflowRunConfiguration-PluginSpecific-Options";

  @HopMetadataProperty private String description;

  @HopMetadataProperty private IWorkflowEngineRunConfiguration engineRunConfiguration;

  @HopMetadataProperty private String executionInfoLocationName;

  @HopMetadataProperty protected boolean defaultSelection;

  public WorkflowRunConfiguration() {}

  public WorkflowRunConfiguration(
      String name,
      String description,
      String executionInfoLocationName,
      IWorkflowEngineRunConfiguration engineRunConfiguration,
      boolean defaultSelection) {
    this();
    this.name = name;
    this.description = description;
    this.executionInfoLocationName = executionInfoLocationName;
    this.engineRunConfiguration = engineRunConfiguration;
    this.defaultSelection = defaultSelection;
  }

  public WorkflowRunConfiguration(WorkflowRunConfiguration c) {
    this.name = c.name;
    this.description = c.description;
    this.executionInfoLocationName = c.executionInfoLocationName;
    if (c.engineRunConfiguration != null) {
      this.engineRunConfiguration = c.engineRunConfiguration.clone();
    }
    this.defaultSelection = c.defaultSelection;
  }

  @Override
  protected WorkflowRunConfiguration clone() {
    return new WorkflowRunConfiguration(this);
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
   * Gets engineRunConfiguration
   *
   * @return value of engineRunConfiguration
   */
  public IWorkflowEngineRunConfiguration getEngineRunConfiguration() {
    return engineRunConfiguration;
  }

  /**
   * @param engineRunConfiguration The engineRunConfiguration to set
   */
  public void setEngineRunConfiguration(IWorkflowEngineRunConfiguration engineRunConfiguration) {
    this.engineRunConfiguration = engineRunConfiguration;
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

  /**
   * Find the first default run configuration in the metadata and return it. Return null if there's
   * no default.
   *
   * @param metadataProvider
   * @return The default run configuration or null if none is specified.
   * @throws HopException
   */
  public static final WorkflowRunConfiguration findDefault(IHopMetadataProvider metadataProvider)
      throws HopException {
    for (WorkflowRunConfiguration runConfiguration :
        metadataProvider.getSerializer(WorkflowRunConfiguration.class).loadAll()) {
      if (runConfiguration.isDefaultSelection()) {
        return runConfiguration;
      }
    }

    return null;
  }
}
