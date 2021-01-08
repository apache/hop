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

package org.apache.hop.core.gui;

import org.apache.commons.io.IOUtils;
import org.apache.hop.core.SwingUniversalImageSvg;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.svg.SvgImage;
import org.apache.hop.core.svg.SvgSupport;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class SwingGUIResource {
  private static ILogChannel log = LogChannel.GENERAL;

  private static SwingGUIResource instance;

  private Map<String, SwingUniversalImageSvg> transformImages;
  private Map<String, SwingUniversalImageSvg> actionImages;

  private SwingGUIResource() {
    this.transformImages = loadTransformImages();
    this.actionImages = loadActionImages();
  }

  public static SwingGUIResource getInstance() {
    if ( instance == null ) {
      instance = new SwingGUIResource();
    }
    return instance;
  }

  private Map<String, SwingUniversalImageSvg> loadTransformImages() {
    Map<String, SwingUniversalImageSvg> map = new HashMap<>();

    for ( IPlugin plugin : PluginRegistry.getInstance().getPlugins( TransformPluginType.class ) ) {
      try {
        SwingUniversalImageSvg image = getUniversalImageIcon( plugin );
        for ( String id : plugin.getIds() ) {
          map.put( id, image );
        }
      } catch ( Exception e ) {
        log.logError( "Unable to load transform icon image for plugin: " + plugin.getName() + " (id=" + plugin.getIds()[ 0 ] + ")", e );
      }
    }

    return map;
  }

  private Map<String, SwingUniversalImageSvg> loadActionImages() {
    Map<String, SwingUniversalImageSvg> map = new HashMap<>();

    for ( IPlugin plugin : PluginRegistry.getInstance().getPlugins( ActionPluginType.class ) ) {
      try {
        SwingUniversalImageSvg image = getUniversalImageIcon( plugin );
        if ( image == null ) {
          throw new HopException( "Unable to find image file: " + plugin.getImageFile() + " for plugin: " + plugin );
        }

        map.put( plugin.getIds()[ 0 ], image );
      } catch ( Exception e ) {
        log.logError( "Unable to load action icon image for plugin: "
          + plugin.getName() + " (id=" + plugin.getIds()[ 0 ] + ")", e );
      }
    }

    return map;
  }

  private SwingUniversalImageSvg getUniversalImageIcon( IPlugin plugin ) throws HopException {
    try {
      PluginRegistry registry = PluginRegistry.getInstance();
      String filename = plugin.getImageFile();

      ClassLoader classLoader = registry.getClassLoader( plugin );

      SwingUniversalImageSvg image = null;

      if ( SvgSupport.isSvgEnabled() && SvgSupport.isSvgName( filename ) ) {
        // Try to use the plugin class loader to get access to the icon
        //
        InputStream inputStream = classLoader.getResourceAsStream( filename );
        if ( inputStream == null ) {
          inputStream = classLoader.getResourceAsStream( "/" + filename );
        }
        // Try to use the Hop class loader to get access to the icon
        //
        if ( inputStream == null ) {
          inputStream = registry.getClass().getResourceAsStream( filename );
        }
        if ( inputStream == null ) {
          inputStream = registry.getClass().getResourceAsStream( "/" + filename );
        }
        // As a last resort, try to use the standard file-system
        //
        if ( inputStream == null ) {
          try {
            inputStream = new FileInputStream( filename );
          } catch ( FileNotFoundException e ) {
            // Ignore, throws error below
          }
        }
        if ( inputStream != null ) {
          try {
            SvgImage svg = SvgSupport.loadSvgImage( inputStream );
            image = new SwingUniversalImageSvg( svg );
          } finally {
            IOUtils.closeQuietly( inputStream );
          }
        }
      }

      if ( image == null ) {
        throw new HopException( "Unable to find file: " + plugin.getImageFile() + " for plugin: " + plugin );
      }

      return image;
    } catch ( Throwable e ) {
      throw new HopException( "Unable to load image from file : '" + plugin.getImageFile() + "' for plugin: " + plugin, e );
    }
  }

  public Map<String, SwingUniversalImageSvg> getActionImages() {
    return actionImages;
  }

  public Map<String, SwingUniversalImageSvg> getTransformImages() {
    return transformImages;
  }
}
