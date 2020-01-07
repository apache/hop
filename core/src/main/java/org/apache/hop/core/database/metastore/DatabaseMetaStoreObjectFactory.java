package org.apache.hop.core.database.metastore;

import org.apache.hop.core.database.DatabaseInterface;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.DatabasePluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.IMetaStoreObjectFactory;

import java.util.HashMap;
import java.util.Map;

public class DatabaseMetaStoreObjectFactory implements IMetaStoreObjectFactory {

  public static final String PLUGIN_ID_KEY = "pluginId";

  @Override public Object instantiateClass( String className, Map<String, String> context ) throws MetaStoreException {
    PluginRegistry registry = PluginRegistry.getInstance();

    String pluginId = context.get(PLUGIN_ID_KEY);
    if (pluginId==null) {
      // Find using the class name...
      //
      pluginId = registry.findPluginIdWithMainClassName( DatabasePluginType.class, className );
    }
    if (pluginId==null) {
      try {
        Class<?> clazz =  Class.forName( className );
        return clazz.newInstance();
      } catch(ClassNotFoundException e) {
        throw new MetaStoreException( "Unable to find class '"+className+"'", e );
      } catch ( IllegalAccessException e ) {
        throw new MetaStoreException( "Unable to access class '"+className+"'", e );
      } catch ( InstantiationException e ) {
        throw new MetaStoreException( "Unable to instantiate class '"+className+"'", e );
      }

    }
    PluginInterface plugin = registry.findPluginWithId( DatabasePluginType.class, pluginId );
    if (plugin==null) {
      throw new MetaStoreException( "Unable to find the plugin in the context of a database meta plugin, classname: "+className+", plugin id: "+pluginId );
    }

    try {
      return registry.loadClass( plugin );
    } catch ( HopPluginException e ) {
      throw new MetaStoreException( "Unable to load the database plugin class: "+className+", plugin id: "+pluginId, e);
    }
  }

  @Override public Map<String, String> getContext( Object pluginObject ) throws MetaStoreException {
    Map<String, String> context = new HashMap<>();
    if (pluginObject instanceof DatabaseInterface) {
      context.put( PLUGIN_ID_KEY, ( (DatabaseInterface) pluginObject ).getPluginId() );
    }
    return context;
  }
}
