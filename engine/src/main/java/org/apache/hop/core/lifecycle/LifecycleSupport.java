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

package org.apache.hop.core.lifecycle;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.LifecyclePluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.IPluginTypeListener;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LifecycleSupport {
  @VisibleForTesting protected static PluginRegistry registry = PluginRegistry.getInstance();
  private Set<ILifecycleListener> lifeListeners;
  private boolean started;
  private ILifeEventHandler handler;

  public LifecycleSupport() {
    lifeListeners =
      Collections.synchronizedSet( loadPlugins( LifecyclePluginType.class, ILifecycleListener.class ) );

    final PluginRegistry registry = PluginRegistry.getInstance();
    registry.addPluginListener( LifecyclePluginType.class, new IPluginTypeListener() {
      public void pluginAdded( Object serviceObject ) {
        ILifecycleListener listener = null;
        try {
          listener = (ILifecycleListener) PluginRegistry.getInstance().loadClass( (IPlugin) serviceObject );
        } catch ( HopPluginException e ) {
          e.printStackTrace();
          return;
        }

        lifeListeners.add( listener );
        if ( started ) {
          try {
            listener.onStart( handler );
          } catch ( LifecycleException e ) {
            e.printStackTrace();
          }
        }

      }

      public void pluginRemoved( Object serviceObject ) {
        lifeListeners.remove( serviceObject );
      }

      public void pluginChanged( Object serviceObject ) {
      }
    } );
  }

  /**
   * Instantiate the main plugin class types for the plugin type provided from the set of registered plugins via
   * {@link PluginRegistry}.
   *
   * @param pluginType Type of plugin whose main class types should be instanticated
   * @return Set of plugin main class instances (a.k.a. plugins)
   */
  static <T> Set<T> loadPlugins( Class<? extends IPluginType> pluginType, Class<T> mainPluginClass ) {
    Set<T> pluginInstances = new HashSet<T>();
    List<IPlugin> plugins = registry.getPlugins( pluginType );
    for ( IPlugin plugin : plugins ) {
      try {
        pluginInstances.add( registry.loadClass( plugin, mainPluginClass ) );
      } catch ( Throwable e ) {
        LogChannel.GENERAL.logError( "Unexpected error loading class for plugin " + plugin.getName(), e );
      }
    }
    return pluginInstances;
  }

  public void onStart( ILifeEventHandler handler ) throws LifecycleException {
    // Caching the last handler and the fact that start has been called. This would cause problems if onStart
    // is called by more than one handler.
    this.handler = handler;
    started = true;
    for ( ILifecycleListener listener : lifeListeners ) {
      listener.onStart( handler );

    }
  }

  public void onExit( ILifeEventHandler handler ) throws LifecycleException {
    for ( ILifecycleListener listener : lifeListeners ) {
      listener.onExit( handler );
    }
  }
}
