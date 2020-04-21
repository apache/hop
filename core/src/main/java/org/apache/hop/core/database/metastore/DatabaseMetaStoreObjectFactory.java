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

package org.apache.hop.core.database.metastore;

import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.IMetaStoreObjectFactory;

import java.util.HashMap;
import java.util.Map;

public class DatabaseMetaStoreObjectFactory implements IMetaStoreObjectFactory {

  public static final String PLUGIN_ID_KEY = "pluginId";

  @Override public Object instantiateClass( String className, Map<String, String> context ) throws MetaStoreException {
    PluginRegistry registry = PluginRegistry.getInstance();

    String pluginId = context.get( PLUGIN_ID_KEY );
    if ( pluginId == null ) {
      // Find using the class name...
      //
      pluginId = registry.findPluginIdWithMainClassName( DatabasePluginType.class, className );
    }
    if ( pluginId == null ) {
      try {
        Class<?> clazz = Class.forName( className );
        return clazz.newInstance();
      } catch ( ClassNotFoundException e ) {
        throw new MetaStoreException( "Unable to find class '" + className + "'", e );
      } catch ( IllegalAccessException e ) {
        throw new MetaStoreException( "Unable to access class '" + className + "'", e );
      } catch ( InstantiationException e ) {
        throw new MetaStoreException( "Unable to instantiate class '" + className + "'", e );
      }

    }
    IPlugin plugin = registry.findPluginWithId( DatabasePluginType.class, pluginId );
    if ( plugin == null ) {
      throw new MetaStoreException( "Unable to find the plugin in the context of a database meta plugin, classname: " + className + ", plugin id: " + pluginId );
    }

    try {
      return registry.loadClass( plugin );
    } catch ( HopPluginException e ) {
      throw new MetaStoreException( "Unable to load the database plugin class: " + className + ", plugin id: " + pluginId, e );
    }
  }

  @Override public Map<String, String> getContext( Object pluginObject ) throws MetaStoreException {
    Map<String, String> context = new HashMap<>();
    if ( pluginObject instanceof IDatabase ) {
      context.put( PLUGIN_ID_KEY, ( (IDatabase) pluginObject ).getPluginId() );
    }
    return context;
  }
}
