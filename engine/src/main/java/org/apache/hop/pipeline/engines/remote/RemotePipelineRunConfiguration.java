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

package org.apache.hop.pipeline.engines.remote;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.EmptyPipelineRunConfiguration;
import org.apache.hop.server.HopServerMeta;

@GuiPlugin(description = "Remote pipeline run configuration widgets")
@Getter
@Setter
public class RemotePipelineRunConfiguration extends EmptyPipelineRunConfiguration
    implements IPipelineEngineRunConfiguration {

  @GuiWidgetElement(
      order = "10",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.METADATA,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.HopServer.Label",
      metadata = HopServerMeta.class)
  @HopMetadataProperty(
      key = "hop_server",
      hopMetadataPropertyType = HopMetadataPropertyType.SERVER_DEFINITION)
  protected String hopServerName;

  @GuiWidgetElement(
      order = "20",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
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
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.ServerPollDelay.Label")
  @HopMetadataProperty(key = "server_poll_delay")
  protected String serverPollDelay;

  @GuiWidgetElement(
      order = "40",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.ServerPollInterval.Label")
  @HopMetadataProperty(key = "server_poll_interval")
  protected String serverPollInterval;

  @GuiWidgetElement(
      order = "50",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.ExportResources.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.ExportResources.ToolTip")
  @HopMetadataProperty(key = "export_resources")
  protected boolean exportingResources;

  @GuiWidgetElement(
      order = "60",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.NamedResourceSourceFolder.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.NamedResourceSourceFolder.ToolTip")
  @HopMetadataProperty(key = "resources_source_folder")
  protected String namedResourcesSourceFolder;

  @GuiWidgetElement(
      order = "70",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.NamedResourceTargetFolder.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.NamedResourceTargetFolder.ToolTip")
  @HopMetadataProperty(key = "resources_target_folder")
  protected String namedResourcesTargetFolder;

  public RemotePipelineRunConfiguration() {
    super();
  }

  public RemotePipelineRunConfiguration(RemotePipelineRunConfiguration config) {
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
  public RemotePipelineRunConfiguration clone() {
    return new RemotePipelineRunConfiguration(this);
  }
}
