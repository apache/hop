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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;

public class PipelineEngineFactory {

  public static final <T extends PipelineMeta> IPipelineEngine<T> createPipelineEngine( String runConfigurationName, IMetaStore metaStore, T pipelineMeta ) throws HopException {

    if ( StringUtils.isEmpty(runConfigurationName)) {
      throw new HopException( "Please specify a run configuration to execute the pipeline with" );
    }
    PipelineRunConfiguration pipelineRunConfiguration = null;
    try {
      pipelineRunConfiguration = PipelineRunConfiguration.createFactory( metaStore ).loadElement( runConfigurationName );
    } catch ( MetaStoreException e ) {
      throw new HopException( "Error loading the pipeline run configuration '"+runConfigurationName+"'", e);
    }
    if (pipelineRunConfiguration==null) {
      throw new HopException( "Unable to find the specified pipeline run configuration '"+runConfigurationName+"'" );
    }

    // Apply the variables from the run configuration
    //
    pipelineRunConfiguration.applyToVariables(pipelineMeta);

    IPipelineEngine<T> pipelineEngine = createPipelineEngine( pipelineRunConfiguration, pipelineMeta );

    // inherit variables from the metadata
    //
    pipelineEngine.initializeVariablesFrom( pipelineMeta );

    // Pass the metastore to make sure
    //
    pipelineEngine.setMetaStore( metaStore );
    pipelineMeta.setMetaStore( metaStore );

    return pipelineEngine;
  }

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

    pipelineEngine.setPipelineMeta( pipelineMeta );

    return pipelineEngine;
  }
}
