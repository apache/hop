/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.hopui;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.PluginTypeListener;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HopUiPluginManager is a singleton class which loads all SpoonPlugins from the SPOON_HOME/plugins/spoon directory.
 * <p/>
 * Spoon Plugins are able to listen for SpoonLifeCycleEvents and can register categorized XUL Overlays to be retrieved
 * later.
 * <p/>
 * Spoon Plugins are deployed as directories under the SPOON_HOME/plugins/spoon directory. Each plugin must provide a
 * build.xml as the root of it's directory and have any required jars under a "lib" directory.
 * <p/>
 * The plugin.xml format is Spring-based e.g. <beans xmlns="http://www.springframework.org/schema/beans"
 * xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation=
 * "http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-4.1.xsd">
 * <p/>
 * <bean id="PLUGIN_ID" class="org.foo.SpoonPluginClassName"></bean> </beans>
 *
 * @author nbaker
 */
public class HopUiPluginManager implements PluginTypeListener {

  private static HopUiPluginManager instance = new HopUiPluginManager();
  private Map<Object, HopUiPluginInterface> plugins = new HashMap<>();
  private Map<String, List<HopUiPluginInterface>> pluginCategoryMap = new HashMap<>();

  @Override public void pluginAdded( final Object serviceObject ) {
    try {
      HopUiPluginInterface hopUiPluginInterface =
        (HopUiPluginInterface) getPluginRegistry().loadClass( (PluginInterface) serviceObject );

      if ( plugins.get( serviceObject ) != null ) {
        return;
      }
      HopUiPluginCategories categories = hopUiPluginInterface.getClass().getAnnotation( HopUiPluginCategories.class );
      if ( categories != null ) {
        for ( String cat : categories.value() ) {
          List<HopUiPluginInterface> categoryList = pluginCategoryMap.get( cat );
          if ( categoryList == null ) {
            categoryList = new ArrayList<>();
            pluginCategoryMap.put( cat, categoryList );
          }
          categoryList.add( hopUiPluginInterface );
        }
      }

      if ( hopUiPluginInterface.getPerspective() != null ) {
        getSpoonPerspectiveManager().addPerspective( hopUiPluginInterface.getPerspective() );
      }

      plugins.put( serviceObject, hopUiPluginInterface );

    } catch ( HopPluginException e ) {
      e.printStackTrace();
    }
  }

  @Override public void pluginRemoved( Object serviceObject ) {
    HopUiPluginInterface hopUiPluginInterface = plugins.get( serviceObject );
    if ( hopUiPluginInterface == null ) {
      return;
    }

    HopUiPluginCategories categories = hopUiPluginInterface.getClass().getAnnotation( HopUiPluginCategories.class );
    if ( categories != null ) {
      for ( String cat : categories.value() ) {
        List<HopUiPluginInterface> categoryList = pluginCategoryMap.get( cat );
        categoryList.remove( hopUiPluginInterface );
      }
    }

    if ( hopUiPluginInterface.getPerspective() != null ) {
      getSpoonPerspectiveManager().removePerspective( hopUiPluginInterface.getPerspective() );
    }

    plugins.remove( serviceObject );
  }

  @Override public void pluginChanged( Object serviceObject ) {
    // Not implemented yet
  }

  /**
   * Return the single instance of this class
   *
   * @return HopUiPerspectiveManager
   */
  public static HopUiPluginManager getInstance() {
    return instance;
  }

  public void applyPluginsForContainer( final String category, final XulDomContainer container ) throws XulException {
    List<HopUiPluginInterface> plugins = pluginCategoryMap.get( category );
    if ( plugins != null ) {
      for ( HopUiPluginInterface sp : plugins ) {
        sp.applyToContainer( category, container );
      }
    }
  }

  /**
   * Returns an unmodifiable list of all Spoon Plugins.
   *
   * @return list of plugins
   */
  public List<HopUiPluginInterface> getPlugins() {
    return Collections.unmodifiableList( Arrays.asList( plugins.values().toArray( new HopUiPluginInterface[] {} ) ) );
  }

  /**
   * Notifies all registered SpoonLifecycleListeners of the given SpoonLifeCycleEvent.
   *
   * @param evt event to notify listeners about
   */
  public void notifyLifecycleListeners( HopUiLifecycleListener.SpoonLifeCycleEvent evt ) {
    for ( HopUiPluginInterface p : plugins.values() ) {
      HopUiLifecycleListener listener = p.getLifecycleListener();
      if ( listener != null ) {
        listener.onEvent( evt );
      }
    }
  }

  PluginRegistry getPluginRegistry() {
    return PluginRegistry.getInstance();
  }

  HopUiPerspectiveManager getSpoonPerspectiveManager() {
    return HopUiPerspectiveManager.getInstance();
  }

  private HopUiPluginManager() {
    PluginRegistry pluginRegistry = getPluginRegistry();
    pluginRegistry.addPluginListener( HopUiPluginType.class, this );

    List<PluginInterface> plugins = pluginRegistry.getPlugins( HopUiPluginType.class );
    for ( PluginInterface plug : plugins ) {
      pluginAdded( plug );
    }
  }
}
