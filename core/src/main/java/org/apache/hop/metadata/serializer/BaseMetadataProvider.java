package org.apache.hop.metadata.serializer;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.plugin.MetadataPluginType;

import java.util.ArrayList;
import java.util.List;

public class BaseMetadataProvider {

  public <T extends IHopMetadata> List<Class<T>> getMetadataClasses() {
    try {
    PluginRegistry registry = PluginRegistry.getInstance();
    List<Class<T>> classes = new ArrayList<>();
    for ( IPlugin plugin : registry.getPlugins( MetadataPluginType.class )) {
      String className = plugin.getClassMap().get( plugin.getMainType() );
      Class<?> pluginClass = registry.getClassLoader( plugin ).loadClass( className );
      classes.add( (Class<T>) pluginClass );
    }
    return classes;
    } catch(Exception e) {
      throw new RuntimeException("Error listing metadata plugin classes (setup issue?)", e);
    }
  }

  public <T extends IHopMetadata> Class<T> getMetadataClassForKey(String key) throws HopException {
    try {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin plugin = registry.findPluginWithId( MetadataPluginType.class, key );
      if ( plugin == null ) {
        throw new HopException( "The metadata plugin for key " + key + " could not be found in the plugin registry" );
      }
      String className = plugin.getClassMap().get( plugin.getMainType() );
      Class<?> pluginClass = registry.getClassLoader( plugin ).loadClass( className );

      return (Class<T>) pluginClass;
    } catch(Exception e) {
      throw new HopException("Error find metadata class for key "+key, e);
    }
  }

}
