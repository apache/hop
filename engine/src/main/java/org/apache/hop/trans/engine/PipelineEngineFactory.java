package org.apache.hop.trans.engine;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.config.IPipelineEngineRunConfiguration;
import org.apache.hop.trans.config.PipelineRunConfiguration;

public class PipelineEngineFactory {

  public static final IPipelineEngine createPipelineEngine( PipelineRunConfiguration pipelineRunConfiguration, TransMeta transMeta ) throws HopException {
    IPipelineEngineRunConfiguration engineRunConfiguration = pipelineRunConfiguration.getEngineRunConfiguration();
    if (engineRunConfiguration==null) {
      throw new HopException( "There is no pipeline execution engine specified in run configuration '"+pipelineRunConfiguration.getName()+"'" );
    }
    String enginePluginId = engineRunConfiguration.getEnginePluginId();

    // Load this engine from the plugin registry
    //
    PluginRegistry pluginRegistry = PluginRegistry.getInstance();
    PluginInterface plugin = pluginRegistry.findPluginWithId( PipelineEnginePluginType.class, enginePluginId );
    if (plugin==null) {
      throw new HopException( "Unable to find pipeline engine plugin type with ID '"+enginePluginId+"'" );
    }

    IPipelineEngine pipelineEngine = pluginRegistry.loadClass( plugin, IPipelineEngine.class );
    pipelineEngine.setPipelineEngineRunConfiguration( pipelineRunConfiguration.getEngineRunConfiguration() );

    pipelineEngine.setSubject(transMeta);

    return pipelineEngine;
  }

}
