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

package org.apache.hop.beam.core;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.Plugin;
import org.apache.hop.core.plugins.PluginMainClassType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.xml.XmlHandlerCache;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BeamHop {

  private static final Logger LOG = LoggerFactory.getLogger( BeamHop.class );

  public static final boolean isInitialized() {
    return HopEnvironment.isInitialized();
  }

  public static final void init( List<String> transformPluginClasses, List<String> xpPluginClasses ) throws HopException {
    PluginRegistry registry = PluginRegistry.getInstance();
    synchronized ( registry ) {

      // Don't create hop config files everywhere...
      //
      System.setProperty( Const.HOP_AUTO_CREATE_CONFIG, "N" );

      // Load Hop base plugins
      //
      HopEnvironment.init();

      XmlHandlerCache.getInstance();

      // LOG.info( "Registering " + transformPluginClasses.size() + " extra transform plugins, and " + xpPluginClasses.size() + " XP plugins" );

      // Register extra classes from the plugins...
      // If they're already in the classpath, this should be fast.
      //
      TransformPluginType transformPluginType = (TransformPluginType) registry.getPluginType( TransformPluginType.class );
      for ( String transformPluginClassName : transformPluginClasses ) {
        try {
          // Only register if it doesn't exist yet.  This is not ideal if we want to replace old transforms with bug fixed new ones.
          //
          IPlugin exists = findPlugin( registry, TransformPluginType.class, transformPluginClassName );
          if ( exists == null ) {
            // Class should be in the classpath since we put it there
            //
            Class<?> transformPluginClass = Class.forName( transformPluginClassName );
            Transform annotation = transformPluginClass.getAnnotation( Transform.class );

            // The plugin class is already in the classpath so we simply call Class.forName() on it.
            //
            // LOG.info( "Registering transform plugin class: " + transformPluginClass );
            transformPluginType.handlePluginAnnotation( transformPluginClass, annotation, new ArrayList<>(), true, null );
          } else {
            LOG.debug( "Plugin " + transformPluginClassName + " is already registered" );
          }
        } catch ( Exception e ) {
          LOG.error( "Error registering transform plugin class : " + transformPluginClassName, e );
        }
      }

      ExtensionPointPluginType xpPluginType = (ExtensionPointPluginType) registry.getPluginType( ExtensionPointPluginType.class );
      for ( String xpPluginClassName : xpPluginClasses ) {
        try {
          IPlugin exists = findPlugin( registry, ExtensionPointPluginType.class, xpPluginClassName );
          // Only register if it doesn't exist yet. This is not ideal if we want to replace old transforms with bug fixed new ones.
          //
          if ( exists == null ) {
            // Class should be in the classpath since we put it there
            //
            Class<?> xpPluginClass = Class.forName( xpPluginClassName );
            ExtensionPoint annotation = xpPluginClass.getAnnotation( ExtensionPoint.class );

            // The plugin class is already in the classpath so we simply call Class.forName() on it.
            //
            // LOG.info( "Registering transform plugin class: " + xpPluginClass );
            xpPluginType.handlePluginAnnotation( xpPluginClass, annotation, new ArrayList<>(), true, null );
          } else {
            LOG.debug( "Plugin " + xpPluginClassName + " is already registered" );
          }
        } catch ( Exception e ) {
          LOG.error( "Error registering transform plugin class : " + xpPluginClassName, e );

        }
      }
    }
  }

  private static IPlugin findPlugin( PluginRegistry registry, Class<? extends IPluginType> pluginTypeClass, String pluginClassName ) {
    PluginMainClassType classType = pluginTypeClass.getAnnotation( PluginMainClassType.class );
    // System.out.println("Found class type : "+classType+" as main plugin class");
    List<IPlugin> plugins = registry.getPlugins( pluginTypeClass );
    // System.out.println("Found "+plugins.size()+" plugins of type "+pluginTypeClass);
    for ( IPlugin plugin : plugins ) {
      String mainClassName = plugin.getClassMap().get( classType.value() );
      if ( mainClassName != null && pluginClassName.equals( mainClassName ) ) {
        return plugin;
      }
    }
    return null;
  }


  public static IPlugin getTransformPluginForClass( Class<? extends ITransformMeta> metaClass ) {
    Transform transformAnnotation = metaClass.getAnnotation( Transform.class );

    return new Plugin(
      new String[] { transformAnnotation.id() },
      TransformPluginType.class,
      metaClass,
      transformAnnotation.categoryDescription(),
      transformAnnotation.name(),
      transformAnnotation.description(),
      transformAnnotation.image(),
      transformAnnotation.isSeparateClassLoaderNeeded(),
      false,
      new HashMap<>(),
      new ArrayList<>(),
      transformAnnotation.documentationUrl(),
      transformAnnotation.keywords(),
      null, false
    );
  }
}
