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

package org.apache.hop.pipeline.engines.loadbalance;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.EmptyPipelineRunConfiguration;
import org.apache.hop.server.loadbalance.ILoadBalancingRunConfiguration;
import org.apache.hop.server.loadbalance.LoadBalancingAlgorithm;
import org.apache.hop.server.loadbalance.LoadBalancingServerEntry;

@GuiPlugin(description = "Load-balancing pipeline run configuration widgets")
@Getter
@Setter
public class LoadBalancingPipelineRunConfiguration extends EmptyPipelineRunConfiguration
    implements IPipelineEngineRunConfiguration, ILoadBalancingRunConfiguration {

  @GuiWidgetElement(
      order = "20",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      group =
          "i18n:org.apache.hop.ui.server.loadbalance:LoadBalancingRunConfiguration.Tab.LoadBalancing",
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS,
      type = GuiElementType.METADATA,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.RunConfiguration.Label",
      metadata = PipelineRunConfiguration.class)
  @HopMetadataProperty(
      key = "run_config",
      hopMetadataPropertyType = HopMetadataPropertyType.PIPELINE_RUN_CONFIG)
  protected String runConfigurationName;

  @GuiWidgetElement(
      order = "30",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      group =
          "i18n:org.apache.hop.ui.server.loadbalance:LoadBalancingRunConfiguration.Tab.LoadBalancing",
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS,
      type = GuiElementType.COMBO,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingAlgorithm.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingAlgorithm.ToolTip",
      comboValuesMethod = "getAlgorithms")
  @HopMetadataProperty(key = "algorithm")
  protected String algorithm;

  @GuiWidgetElement(
      order = "40",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      group =
          "i18n:org.apache.hop.ui.server.loadbalance:LoadBalancingRunConfiguration.Tab.LoadBalancing",
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingMaxRetries.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingMaxRetries.ToolTip")
  @HopMetadataProperty(key = "max_retries")
  protected String maxRetries;

  @GuiWidgetElement(
      order = "50",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      group =
          "i18n:org.apache.hop.ui.server.loadbalance:LoadBalancingRunConfiguration.Tab.LoadBalancing",
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingRetryWindow.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingRetryWindow.ToolTip")
  @HopMetadataProperty(key = "retry_window_ms")
  protected String retryWindowMs;

  @GuiWidgetElement(
      order = "60",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      group =
          "i18n:org.apache.hop.ui.server.loadbalance:LoadBalancingRunConfiguration.Tab.LoadBalancing",
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS,
      type = GuiElementType.CHECKBOX,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingRetryOnFailure.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingRetryOnFailure.ToolTip")
  @HopMetadataProperty(key = "retry_on_execution_failure")
  protected boolean retryOnExecutionFailure;

  @GuiWidgetElement(
      order = "70",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      group =
          "i18n:org.apache.hop.ui.server.loadbalance:LoadBalancingRunConfiguration.Tab.LoadBalancing",
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingProbeTimeout.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingProbeTimeout.ToolTip")
  @HopMetadataProperty(key = "probe_timeout_ms")
  protected String probeTimeoutMs;

  @GuiWidgetElement(
      order = "80",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      group =
          "i18n:org.apache.hop.ui.server.loadbalance:LoadBalancingRunConfiguration.Tab.LoadBalancing",
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingConfigRefresh.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingConfigRefresh.ToolTip")
  @HopMetadataProperty(key = "config_refresh_ms")
  protected String configRefreshIntervalMs;

  @GuiWidgetElement(
      order = "90",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      group =
          "i18n:org.apache.hop.ui.server.loadbalance:LoadBalancingRunConfiguration.Tab.LoadBalancing",
      groupOrder = "20",
      groupType = GuiWidgetGroupType.TABS,
      type = GuiElementType.FOLDER,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingStateFolder.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.LoadBalancingStateFolder.ToolTip")
  @HopMetadataProperty(key = "state_folder")
  protected String stateFolder;

  @GuiWidgetElement(
      order = "100",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      group =
          "i18n:org.apache.hop.ui.server.loadbalance:LoadBalancingRunConfiguration.Tab.Management",
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.ServerPollDelay.Label")
  @HopMetadataProperty(key = "server_poll_delay")
  protected String serverPollDelay;

  @GuiWidgetElement(
      order = "110",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      group =
          "i18n:org.apache.hop.ui.server.loadbalance:LoadBalancingRunConfiguration.Tab.Management",
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.ServerPollInterval.Label")
  @HopMetadataProperty(key = "server_poll_interval")
  protected String serverPollInterval;

  @GuiWidgetElement(
      order = "120",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      group =
          "i18n:org.apache.hop.ui.server.loadbalance:LoadBalancingRunConfiguration.Tab.Management",
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS,
      type = GuiElementType.CHECKBOX,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.ExportResources.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.ExportResources.ToolTip")
  @HopMetadataProperty(key = "export_resources")
  protected boolean exportingResources;

  @GuiWidgetElement(
      order = "130",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      group =
          "i18n:org.apache.hop.ui.server.loadbalance:LoadBalancingRunConfiguration.Tab.Management",
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.NamedResourceSourceFolder.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.NamedResourceSourceFolder.ToolTip")
  @HopMetadataProperty(key = "resources_source_folder")
  protected String namedResourcesSourceFolder;

  @GuiWidgetElement(
      order = "140",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      group =
          "i18n:org.apache.hop.ui.server.loadbalance:LoadBalancingRunConfiguration.Tab.Management",
      groupOrder = "10",
      groupType = GuiWidgetGroupType.TABS,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.NamedResourceTargetFolder.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.NamedResourceTargetFolder.ToolTip")
  @HopMetadataProperty(key = "resources_target_folder")
  protected String namedResourcesTargetFolder;

  @HopMetadataProperty(key = "server", groupKey = "servers")
  protected List<LoadBalancingServerEntry> servers;

  /** Runtime-only: last selected server. Not stored in metadata. */
  protected String hopServerName;

  public LoadBalancingPipelineRunConfiguration() {
    super();
    this.servers = new ArrayList<>();
    this.algorithm = LoadBalancingAlgorithm.EVEN_LOAD.getDescription();
    this.maxRetries = "2";
    this.retryWindowMs = "0";
    this.probeTimeoutMs = "3000";
    this.configRefreshIntervalMs = "10000";
  }

  public LoadBalancingPipelineRunConfiguration(LoadBalancingPipelineRunConfiguration config) {
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
  public LoadBalancingPipelineRunConfiguration clone() {
    return new LoadBalancingPipelineRunConfiguration(this);
  }

  public List<String> getAlgorithms(ILogChannel log, IHopMetadataProvider metadataProvider) {
    List<String> list = new ArrayList<>();
    for (LoadBalancingAlgorithm value : LoadBalancingAlgorithm.values()) {
      list.add(value.getDescription());
    }
    return list;
  }
}
