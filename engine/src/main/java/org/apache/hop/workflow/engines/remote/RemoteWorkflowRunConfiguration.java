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

package org.apache.hop.workflow.engines.remote;

import org.apache.hop.server.HopServer;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.empty.EmptyWorkflowRunConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@GuiPlugin(description = "Remote workflow run configuration widgets")
public class RemoteWorkflowRunConfiguration extends EmptyWorkflowRunConfiguration implements IWorkflowEngineRunConfiguration {

  @GuiWidgetElement(
    order = "10",
    parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.COMBO,
    comboValuesMethod = "getHopServerNames",
    label = "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.HopServer.Label"
  )
  @HopMetadataProperty( key = "hop_server" )
  protected String hopServerName;

  @GuiWidgetElement(
    order = "20",
    parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.COMBO,
    comboValuesMethod = "getRunConfigurationNames",
    label = "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.RunConfiguration.Label"
  )
  @HopMetadataProperty( key = "safe_mode" )
  protected String runConfigurationName;

  @GuiWidgetElement(
    order = "30",
    parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.ServerPollDelay.Label"
  )
  @HopMetadataProperty( key = "server_poll_delay" )
  protected String serverPollDelay;

  @GuiWidgetElement(
    order = "40",
    parentId = WorkflowRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.ServerPollInterval.Label"
  )
  @HopMetadataProperty( key = "server_poll_interval" )
  protected String serverPollInterval;


  public RemoteWorkflowRunConfiguration() {
    super();
  }

  public RemoteWorkflowRunConfiguration( RemoteWorkflowRunConfiguration config ) {
    super( config );
    this.hopServerName = config.hopServerName;
    this.runConfigurationName = config.runConfigurationName;
    this.serverPollDelay = config.serverPollDelay;
    this.serverPollInterval = config.serverPollInterval;
  }

  public List<String> getHopServerNames( ILogChannel log, IHopMetadataProvider metadataProvider ) {
    List<String> names = new ArrayList<>();
    try {
      names.addAll( metadataProvider.getSerializer( HopServer.class ).listObjectNames() );
      Collections.sort( names );
    } catch ( Exception e ) {
      log.logError( "Error getting hop server names from the metadata", e );
    }
    return names;
  }

  public List<String> getRunConfigurationNames( ILogChannel log, IHopMetadataProvider metadataProvider ) {
    List<String> names = new ArrayList<>();
    try {
      names.addAll( metadataProvider.getSerializer( WorkflowRunConfiguration.class ).listObjectNames() );
      Collections.sort( names );
    } catch ( Exception e ) {
      log.logError( "Error getting the workflow run configuration names from the metadata", e );
    }
    return names;
  }

  public RemoteWorkflowRunConfiguration clone() {
    return new RemoteWorkflowRunConfiguration( this );
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
  public void setHopServerName( String hopServerName ) {
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
  public void setRunConfigurationName( String runConfigurationName ) {
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
  public void setServerPollDelay( String serverPollDelay ) {
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
  public void setServerPollInterval( String serverPollInterval ) {
    this.serverPollInterval = serverPollInterval;
  }
}
