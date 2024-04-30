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

package org.apache.hop.workflow.engines.remote;

import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.empty.EmptyWorkflowRunConfiguration;

@GuiPlugin(description = "Remote workflow run configuration widgets")
public class RemoteWorkflowRunConfiguration extends EmptyWorkflowRunConfiguration
    implements IWorkflowEngineRunConfiguration {

  @GuiWidgetElement(
      order = "10",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.METADATA,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.HopServer.Label",
      typeMetadata = HopServerTypeMetadata.class)
  @HopMetadataProperty(key = "hop_server")
  protected String hopServerName;

  @GuiWidgetElement(
      order = "20",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.METADATA,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.RunConfiguration.Label",
      typeMetadata = WorkflowRunConfigurationTypeMetadata.class)
  @HopMetadataProperty(key = "run_config")
  protected String runConfigurationName;

  @GuiWidgetElement(
      order = "30",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.ServerPollDelay.Label")
  @HopMetadataProperty(key = "server_poll_delay")
  protected String serverPollDelay;

  @GuiWidgetElement(
      order = "40",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.ServerPollInterval.Label")
  @HopMetadataProperty(key = "server_poll_interval")
  protected String serverPollInterval;

  @GuiWidgetElement(
      order = "50",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.ExportResources.Label",
      toolTip =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.ExportResources.ToolTip")
  @HopMetadataProperty(key = "export_resources")
  protected boolean exportingResources;

  @GuiWidgetElement(
      order = "60",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FOLDER,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.NamedResourceSourceFolder.Label",
      toolTip =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.NamedResourceSourceFolder.ToolTip")
  @HopMetadataProperty(key = "resources_source_folder")
  protected String namedResourcesSourceFolder;

  @GuiWidgetElement(
      order = "70",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.NamedResourceTargetFolder.Label",
      toolTip =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.NamedResourceTargetFolder.ToolTip")
  @HopMetadataProperty(key = "resources_target_folder")
  protected String namedResourcesTargetFolder;

  public RemoteWorkflowRunConfiguration() {
    super();
  }

  public RemoteWorkflowRunConfiguration(RemoteWorkflowRunConfiguration config) {
    super(config);
    this.hopServerName = config.hopServerName;
    this.runConfigurationName = config.runConfigurationName;
    this.serverPollDelay = config.serverPollDelay;
    this.serverPollInterval = config.serverPollInterval;
    this.exportingResources = config.exportingResources;
    this.namedResourcesSourceFolder = config.namedResourcesSourceFolder;
    this.namedResourcesTargetFolder = config.namedResourcesTargetFolder;
  }

  @Override
  public RemoteWorkflowRunConfiguration clone() {
    return new RemoteWorkflowRunConfiguration(this);
  }

  /**
   * Gets hopServerName
   *
   * @return value of hopServerName
   */
  public String getHopServerName() {
    return hopServerName;
  }

  /**
   * @param hopServerName The hopServerName to set
   */
  public void setHopServerName(String hopServerName) {
    this.hopServerName = hopServerName;
  }

  /**
   * Gets runConfigurationName
   *
   * @return value of runConfigurationName
   */
  public String getRunConfigurationName() {
    return runConfigurationName;
  }

  /**
   * @param runConfigurationName The runConfigurationName to set
   */
  public void setRunConfigurationName(String runConfigurationName) {
    this.runConfigurationName = runConfigurationName;
  }

  /**
   * Gets serverPollDelay
   *
   * @return value of serverPollDelay
   */
  public String getServerPollDelay() {
    return serverPollDelay;
  }

  /**
   * @param serverPollDelay The serverPollDelay to set
   */
  public void setServerPollDelay(String serverPollDelay) {
    this.serverPollDelay = serverPollDelay;
  }

  /**
   * Gets serverPollInterval
   *
   * @return value of serverPollInterval
   */
  public String getServerPollInterval() {
    return serverPollInterval;
  }

  /**
   * @param serverPollInterval The serverPollInterval to set
   */
  public void setServerPollInterval(String serverPollInterval) {
    this.serverPollInterval = serverPollInterval;
  }

  /**
   * Gets exportingResources
   *
   * @return value of exportingResources
   */
  public boolean isExportingResources() {
    return exportingResources;
  }

  /**
   * @param exportingResources The exportingResources to set
   */
  public void setExportingResources(boolean exportingResources) {
    this.exportingResources = exportingResources;
  }

  /**
   * Gets namedResourcesSourceFolder
   *
   * @return value of namedResourcesSourceFolder
   */
  public String getNamedResourcesSourceFolder() {
    return namedResourcesSourceFolder;
  }

  /**
   * @param namedResourcesSourceFolder The namedResourcesSourceFolder to set
   */
  public void setNamedResourcesSourceFolder(String namedResourcesSourceFolder) {
    this.namedResourcesSourceFolder = namedResourcesSourceFolder;
  }

  /**
   * Gets namedResourcesTargetFolder
   *
   * @return value of namedResourcesTargetFolder
   */
  public String getNamedResourcesTargetFolder() {
    return namedResourcesTargetFolder;
  }

  /**
   * @param namedResourcesTargetFolder The namedResourcesTargetFolder to set
   */
  public void setNamedResourcesTargetFolder(String namedResourcesTargetFolder) {
    this.namedResourcesTargetFolder = namedResourcesTargetFolder;
  }
}
