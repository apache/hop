package org.apache.hop.pipeline.config;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.IMetaStoreObjectFactory;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEnginePluginType;

import java.util.HashMap;
import java.util.Map;

public class PipelineRunConfigurationMetaStoreObjectFactory implements IMetaStoreObjectFactory {

  public static final String PLUGIN_ID_KEY = "pluginId";

  @Override public Object instantiateClass( String className, Map<String, String> context ) throws MetaStoreException {
    PluginRegistry registry = PluginRegistry.getInstance();

    String pluginId = context.get( PLUGIN_ID_KEY );
    if ( pluginId == null ) {
      throw new MetaStoreException( "Unable to find plugin ID of the pipeline engine plugin in the metadata when instantiating class '"+className+"'" );
    }
    IPlugin plugin = registry.findPluginWithId( PipelineEnginePluginType.class, pluginId );
    if ( plugin == null ) {
      throw new MetaStoreException( "Unable to find the plugin in the context of a pipeline engine plugin, classname: " + className + ", plugin id: " + pluginId );
    }

    try {
      // We don't return the engine but the corresponding engine configuration
      //
      IPipelineEngine engine = registry.loadClass( plugin, IPipelineEngine.class );

      IPipelineEngineRunConfiguration engineRunConfiguration = engine.createDefaultPipelineEngineRunConfiguration();
      engineRunConfiguration.setEnginePluginId( plugin.getIds()[0] );
      engineRunConfiguration.setEnginePluginName( plugin.getName() );
      return engineRunConfiguration;
    } catch ( HopPluginException e ) {
      throw new MetaStoreException( "Unable to load the pipeline engine plugin class: " + className + ", plugin id: " + pluginId, e );
    }
  }

  @Override public Map<String, String> getContext( Object pluginObject ) throws MetaStoreException {
    Map<String, String> context = new HashMap<>();
    if ( pluginObject instanceof IPipelineEngineRunConfiguration ) {
      context.put( PLUGIN_ID_KEY, ( (IPipelineEngineRunConfiguration) pluginObject ).getEnginePluginId() );
    }
    return context;
  }
}
