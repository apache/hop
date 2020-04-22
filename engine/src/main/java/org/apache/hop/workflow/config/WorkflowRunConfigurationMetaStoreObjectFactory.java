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

package org.apache.hop.workflow.config;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.IMetaStoreObjectFactory;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEnginePluginType;

import java.util.HashMap;
import java.util.Map;

public class WorkflowRunConfigurationMetaStoreObjectFactory implements IMetaStoreObjectFactory {

  public static final String PLUGIN_ID_KEY = "pluginId";

  @Override public Object instantiateClass( String className, Map<String, String> context ) throws MetaStoreException {
    PluginRegistry registry = PluginRegistry.getInstance();

    String pluginId = context.get( PLUGIN_ID_KEY );
    if ( pluginId == null ) {
      throw new MetaStoreException( "Unable to find plugin ID of the workflow engine plugin in the metadata when instantiating class '" + className + "'" );
    }
    IPlugin plugin = registry.findPluginWithId( WorkflowEnginePluginType.class, pluginId );
    if ( plugin == null ) {
      throw new MetaStoreException( "Unable to find the plugin in the context of a workflow engine plugin, classname: " + className + ", plugin id: " + pluginId );
    }

    try {
      // We don't return the engine but the corresponding engine configuration
      //
      IWorkflowEngine engine = registry.loadClass( plugin, IWorkflowEngine.class );

      IWorkflowEngineRunConfiguration engineRunConfiguration = engine.createDefaultWorkflowEngineRunConfiguration();
      engineRunConfiguration.setEnginePluginId( plugin.getIds()[ 0 ] );
      engineRunConfiguration.setEnginePluginName( plugin.getName() );
      return engineRunConfiguration;
    } catch ( HopPluginException e ) {
      throw new MetaStoreException( "Unable to load the workflow engine plugin class: " + className + ", plugin id: " + pluginId, e );
    }
  }

  @Override public Map<String, String> getContext( Object pluginObject ) throws MetaStoreException {
    Map<String, String> context = new HashMap<>();
    if ( pluginObject instanceof IWorkflowEngineRunConfiguration ) {
      context.put( PLUGIN_ID_KEY, ( (IWorkflowEngineRunConfiguration) pluginObject ).getEnginePluginId() );
    }
    return context;
  }
}
