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

package org.apache.hop.workflow.engines.loadbalance;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.server.loadbalance.ILoadBalancingRunConfiguration;
import org.apache.hop.server.loadbalance.LoadBalancingAlgorithm;
import org.apache.hop.server.loadbalance.LoadBalancingServerEntry;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.empty.EmptyWorkflowRunConfiguration;

@GuiPlugin(description = "Load-balancing workflow run configuration widgets")
@Getter
@Setter
public class LoadBalancingWorkflowRunConfiguration extends EmptyWorkflowRunConfiguration
    implements IWorkflowEngineRunConfiguration, ILoadBalancingRunConfiguration {

  @GuiWidgetElement(
      order = "20",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.METADATA,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.RunConfiguration.Label",
      metadata = WorkflowRunConfiguration.class)
  @HopMetadataProperty(
      key = "run_config",
      hopMetadataPropertyType = HopMetadataPropertyType.WORKFLOW_RUN_CONFIG)
  protected String runConfigurationName;

  @GuiWidgetElement(
      order = "30",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingAlgorithm.Label",
      toolTip =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingAlgorithm.ToolTip",
      comboValuesMethod = "getAlgorithms")
  @HopMetadataProperty(key = "algorithm")
  protected String algorithm;

  @GuiWidgetElement(
      order = "40",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingMaxRetries.Label",
      toolTip =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingMaxRetries.ToolTip")
  @HopMetadataProperty(key = "max_retries")
  protected String maxRetries;

  @GuiWidgetElement(
      order = "50",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingRetryWindow.Label",
      toolTip =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingRetryWindow.ToolTip")
  @HopMetadataProperty(key = "retry_window_ms")
  protected String retryWindowMs;

  @GuiWidgetElement(
      order = "60",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingRetryOnFailure.Label",
      toolTip =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingRetryOnFailure.ToolTip")
  @HopMetadataProperty(key = "retry_on_execution_failure")
  protected boolean retryOnExecutionFailure;

  @GuiWidgetElement(
      order = "70",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingProbeTimeout.Label",
      toolTip =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingProbeTimeout.ToolTip")
  @HopMetadataProperty(key = "probe_timeout_ms")
  protected String probeTimeoutMs;

  @GuiWidgetElement(
      order = "80",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingConfigRefresh.Label",
      toolTip =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingConfigRefresh.ToolTip")
  @HopMetadataProperty(key = "config_refresh_ms")
  protected String configRefreshIntervalMs;

  @GuiWidgetElement(
      order = "90",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FOLDER,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingStateFolder.Label",
      toolTip =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.LoadBalancingStateFolder.ToolTip")
  @HopMetadataProperty(key = "state_folder")
  protected String stateFolder;

  @GuiWidgetElement(
      order = "100",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.ServerPollDelay.Label")
  @HopMetadataProperty(key = "server_poll_delay")
  protected String serverPollDelay;

  @GuiWidgetElement(
      order = "110",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.ServerPollInterval.Label")
  @HopMetadataProperty(key = "server_poll_interval")
  protected String serverPollInterval;

  @GuiWidgetElement(
      order = "120",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.ExportResources.Label",
      toolTip =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.ExportResources.ToolTip")
  @HopMetadataProperty(key = "export_resources")
  protected boolean exportingResources;

  @GuiWidgetElement(
      order = "130",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FOLDER,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.NamedResourceSourceFolder.Label",
      toolTip =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.NamedResourceSourceFolder.ToolTip")
  @HopMetadataProperty(key = "resources_source_folder")
  protected String namedResourcesSourceFolder;

  @GuiWidgetElement(
      order = "140",
      parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.NamedResourceTargetFolder.Label",
      toolTip =
          "i18n:org.apache.hop.ui.workflow.config:WorkflowRunConfigurationDialog.NamedResourceTargetFolder.ToolTip")
  @HopMetadataProperty(key = "resources_target_folder")
  protected String namedResourcesTargetFolder;

  @HopMetadataProperty(key = "server", groupKey = "servers")
  protected List<LoadBalancingServerEntry> servers;

  protected String hopServerName;

  public LoadBalancingWorkflowRunConfiguration() {
    super();
    this.servers = new ArrayList<>();
    this.algorithm = LoadBalancingAlgorithm.EVEN_LOAD.getDescription();
    this.maxRetries = "2";
    this.retryWindowMs = "0";
    this.probeTimeoutMs = "3000";
    this.configRefreshIntervalMs = "10000";
  }

  public LoadBalancingWorkflowRunConfiguration(LoadBalancingWorkflowRunConfiguration config) {
    super(config);
    this.runConfigurationName = config.runConfigurationName;
    this.algorithm = config.algorithm;
    this.maxRetries = config.maxRetries;
    this.retryWindowMs = config.retryWindowMs;
    this.retryOnExecutionFailure = config.retryOnExecutionFailure;
    this.probeTimeoutMs = config.probeTimeoutMs;
    this.configRefreshIntervalMs = config.configRefreshIntervalMs;
    this.stateFolder = config.stateFolder;
    this.serverPollDelay = config.serverPollDelay;
    this.serverPollInterval = config.serverPollInterval;
    this.exportingResources = config.exportingResources;
    this.namedResourcesSourceFolder = config.namedResourcesSourceFolder;
    this.namedResourcesTargetFolder = config.namedResourcesTargetFolder;
    this.hopServerName = config.hopServerName;
    this.servers = new ArrayList<>();
    if (config.servers != null) {
      for (LoadBalancingServerEntry entry : config.servers) {
        this.servers.add(new LoadBalancingServerEntry(entry));
      }
    }
  }

  @Override
  public LoadBalancingWorkflowRunConfiguration clone() {
    return new LoadBalancingWorkflowRunConfiguration(this);
  }

  public List<String> getAlgorithms(ILogChannel log, IHopMetadataProvider metadataProvider) {
    List<String> list = new ArrayList<>();
    for (LoadBalancingAlgorithm value : LoadBalancingAlgorithm.values()) {
      list.add(value.getDescription());
    }
    return list;
  }
}
