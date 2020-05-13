/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.engines.remote;

import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.EmptyPipelineRunConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@GuiPlugin
public class RemotePipelineRunConfiguration extends EmptyPipelineRunConfiguration implements IPipelineEngineRunConfiguration {

  @GuiWidgetElement(
    order = "10",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.COMBO,
    comboValuesMethod = "getSlaveServerNames",
    i18nPackage = "org.apache.hop.ui.pipeline.config",
    label = "PipelineRunConfigurationDialog.SlaveServer.Label"
  )
  @MetaStoreAttribute(key="slave_server")
  protected String slaveServerName;

  @GuiWidgetElement(
    order = "20",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.COMBO,
    comboValuesMethod = "getRunConfigurationNames",
    i18nPackage = "org.apache.hop.ui.pipeline.config",
    label = "PipelineRunConfigurationDialog.RunConfiguration.Label"
  )
  @MetaStoreAttribute(key="safe_mode")
  protected String runConfigurationName;

  @GuiWidgetElement(
    order = "30",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    i18nPackage = "org.apache.hop.ui.pipeline.config",
    label = "PipelineRunConfigurationDialog.ServerPollDelay.Label"
  )
  @MetaStoreAttribute(key="server_poll_delay")
  protected String serverPollDelay;

  @GuiWidgetElement(
    order = "40",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    i18nPackage = "org.apache.hop.ui.pipeline.config",
    label = "PipelineRunConfigurationDialog.ServerPollInterval.Label"
  )
  @MetaStoreAttribute(key="server_poll_interval")
  protected String serverPollInterval;


  public RemotePipelineRunConfiguration() {
    super();
  }

  public RemotePipelineRunConfiguration( RemotePipelineRunConfiguration config ) {
    super( config );
    this.slaveServerName = config.slaveServerName;
    this.runConfigurationName = config.runConfigurationName;
    this.serverPollDelay = config.serverPollDelay;
    this.serverPollInterval = config.serverPollInterval;
  }

  public List<String> getSlaveServerNames( ILogChannel log, IMetaStore metaStore ) {
    List<String> names = new ArrayList<>(  );
    try {
      MetaStoreFactory<SlaveServer> factory = new MetaStoreFactory<>( SlaveServer.class, metaStore );
      names.addAll(factory.getElementNames());
      Collections.sort( names );
    } catch(Exception e) {
      log.logError("Error getting slave server names from the metastore", e);
    }
    return names;
  }

  public List<String> getRunConfigurationNames( ILogChannel log, IMetaStore metaStore ) {
    List<String> names = new ArrayList<>(  );
    try {
      MetaStoreFactory<PipelineRunConfiguration> factory = new MetaStoreFactory<>( PipelineRunConfiguration.class, metaStore );
      names.addAll(factory.getElementNames());
      Collections.sort( names );
    } catch(Exception e) {
      log.logError("Error getting the pipeline run configuration names from the metastore", e);
    }
    return names;
  }

  public RemotePipelineRunConfiguration clone() {
    return new RemotePipelineRunConfiguration( this );
  }

  /**
   * Gets slaveServerName
   *
   * @return value of slaveServerName
   */
  public String getSlaveServerName() {
    return slaveServerName;
  }

  /**
   * @param slaveServerName The slaveServerName to set
   */
  public void setSlaveServerName( String slaveServerName ) {
    this.slaveServerName = slaveServerName;
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
