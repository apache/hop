/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.plugins;

import java.lang.annotation.Annotation;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

public abstract class BaseFragmentType<T extends Annotation> extends BasePluginType<T> {

  BaseFragmentType( Class<T> pluginType, String id, String name, Class<? extends IPluginType> typeToTrack ) {
    super( pluginType, id, name );
    initListeners( this.getClass(), typeToTrack );
  }

  protected void initListeners( Class<? extends IPluginType> aClass, Class<? extends IPluginType> typeToTrack ) {
    // keep track of new fragments
    registry.addPluginListener( aClass, new FragmentTypeListener( registry, typeToTrack ) {
      /**
       * Keep track of new Fragments, keep note of the method signature's order
       * @param fragment The plugin fragment to merge
       * @param plugin The plugin to be merged
       */
      @Override
      void mergePlugin( IPlugin fragment, IPlugin plugin ) {
        if ( plugin != null ) {
          plugin.merge( fragment );
        }
      }
    } );

    // start listening to interested parties
    registry.addPluginListener( typeToTrack, new FragmentTypeListener( registry, aClass ) {
      /**
       * Keep track of new Fragments, keep note of the method signature's order
       * @param plugin The plugin to be merged
       * @param fragment The plugin fragment to merge
       */
      @Override
      void mergePlugin( IPlugin plugin, IPlugin fragment ) {
        if ( plugin != null ) {
          plugin.merge( fragment );
        }
      }
    } );
  }

  @Override
  public boolean isFragment() {
    return true;
  }

  @Override protected URLClassLoader createUrlClassLoader( URL jarFileUrl, ClassLoader classLoader ) {
    return new HopURLClassLoader( new URL[] { jarFileUrl }, classLoader );
  }

  @Override
  protected String extractName( T annotation ) {
    return null;
  }

  @Override
  protected String extractDesc( T annotation ) {
    return null;
  }

  @Override
  protected String extractCategory( T annotation ) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader( T annotation ) {
    return false;
  }

  @Override
  @Deprecated
  protected String extractI18nPackageName( T annotation ) {
    return null;
  }

  @Override
  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, T annotation ) {
  }

  protected abstract class FragmentTypeListener implements IPluginTypeListener {
    private final PluginRegistry registry;
    private final Class<? extends IPluginType> typeToTrack;

    FragmentTypeListener( PluginRegistry registry, Class<? extends IPluginType> typeToTrack ) {
      this.registry = registry;
      this.typeToTrack = typeToTrack;
    }

    abstract void mergePlugin( IPlugin left, IPlugin right );

    @Override
    public void pluginAdded( Object serviceObject ) {
      IPlugin left = (IPlugin) serviceObject;
      IPlugin right = registry.findPluginWithId( typeToTrack, left.getIds()[ 0 ] );
      mergePlugin( left, right );
    }

    @Override
    public void pluginRemoved( Object serviceObject ) {
    }

    @Override
    public void pluginChanged( Object serviceObject ) {
      pluginAdded( serviceObject );
    }
  }
}
