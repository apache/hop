package org.apache.hop.pipeline.engine;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;

public class PipelineEngineFactory {

  public static final IPipelineEngine createPipelineEngine( PipelineRunConfiguration pipelineRunConfiguration, PipelineMeta pipelineMeta ) throws HopException {
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

    IPipelineEngine pipelineEngine = pluginRegistry.loadClass( plugin, IPipelineEngine.class );
    pipelineEngine.setPipelineEngineRunConfiguration( pipelineRunConfiguration.getEngineRunConfiguration() );

    pipelineEngine.setSubject( pipelineMeta );

    return pipelineEngine;
  }

}
