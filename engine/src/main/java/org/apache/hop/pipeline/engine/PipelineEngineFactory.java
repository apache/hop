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

package org.apache.hop.pipeline.engine;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;

public class PipelineEngineFactory {

  public static final <T extends PipelineMeta> IPipelineEngine<T> createPipelineEngine( PipelineRunConfiguration pipelineRunConfiguration, T pipelineMeta ) throws HopException {
    IPipelineEngineRunConfiguration engineRunConfiguration = pipelineRunConfiguration.getEngineRunConfiguration();
    if (engineRunConfiguration==null) {
      throw new HopException( "There is no pipeline execution engine specified in run configuration '"+pipelineRunConfiguration.getName()+"'" );
    }
    String enginePluginId = engineRunConfiguration.getEnginePluginId();

    // Load this engine from the plugin registry
    //
    PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    IPlugin plugin = pluginRegistry.findPluginWithId( PipelineEnginePluginType.class, enginePluginId );
    if (plugin==null) {
      throw new HopException( "Unable to find pipeline engine plugin type with ID '"+enginePluginId+"'" );
    }

    IPipelineEngine<T> pipelineEngine = pluginRegistry.loadClass( plugin, IPipelineEngine.class );
    pipelineEngine.setPipelineRunConfiguration( pipelineRunConfiguration );

    pipelineEngine.setSubject( pipelineMeta );

    return pipelineEngine;
  }
}
