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

package org.apache.hop.core.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.SwingUniversalImage;
import org.apache.hop.core.SwingUniversalImageSvg;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.svg.SvgSupport;
import org.apache.hop.core.vfs.HopVfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Class for loading images from SVG, PNG, or other bitmap formats.
 * <p>
 * Logic is: if SVG is enabled, then SVG icon loaded if exist. Otherwise, class trying to change name into PNG and try
 * to load. If initial name is PNG, then PNG icon will be loaded.
 */
public class SwingSvgImageUtil {

  private static FileObject base;
  private static final String NO_IMAGE = "ui/images/no_image.svg";

  static {
    try {
      base = HopVfs.getFileSystemManager().resolveFile( System.getProperty( "user.dir" ) );
    } catch ( FileSystemException e ) {
      e.printStackTrace();
      base = null;
    }
  }

  /**
   * Load image from several sources.
   */
  private static SwingUniversalImageSvg getImageAsResourceInternal( String location ) {
    SwingUniversalImageSvg result = null;
    if ( result == null ) {
      result = loadFromCurrentClasspath( location );
    }
    if ( result == null ) {
      result = loadFromBasedVFS( location );
    }
    if ( result == null ) {
      result = loadFromSimpleVFS( location );
    }
    return result;
  }

  /**
   * Load image from several sources.
   */
  public static SwingUniversalImageSvg getImageAsResource( String location ) {
    SwingUniversalImageSvg result = null;
    if ( result == null && SvgSupport.isSvgEnabled() ) {
      result = getImageAsResourceInternal( SvgSupport.toSvgName( location ) );
    }
    if ( result == null && !location.equals( NO_IMAGE ) ) {
      result = getImageAsResource( NO_IMAGE );
    }
    return result;
  }

  private static SwingUniversalImageSvg getUniversalImageInternal( ClassLoader classLoader, String filename ) {
    SwingUniversalImageSvg result = loadFromClassLoader( classLoader, filename );
    if ( result == null ) {
      result = loadFromClassLoader( classLoader, "/" + filename );
      if ( result == null ) {
        result = loadFromClassLoader( classLoader, "ui/images/" + filename );
        if ( result == null ) {
          result = getImageAsResourceInternal( filename );
        }
      }
    }
    return result;
  }

  /**
   * Load image from several sources.
   */
  public static SwingUniversalImageSvg getUniversalImage( ClassLoader classLoader, String filename ) {

    if ( StringUtils.isBlank( filename ) ) {
      throw new RuntimeException( "Filename not provided" );
    }

    SwingUniversalImageSvg result = null;
    if ( SvgSupport.isSvgEnabled() ) {
      result = getUniversalImageInternal( classLoader, SvgSupport.toSvgName( filename ) );
    }

    // if we haven't loaded SVG attempt to use PNG
    if ( result == null ) {
      result = getUniversalImageInternal( classLoader, SvgSupport.toPngName( filename ) );
    }

    // if we can't load PNG, use default "no_image" graphic
    if ( result == null ) {
      result = getImageAsResource( NO_IMAGE );
    }
    return result;
  }

  /**
   * Load image from several sources.
   */
  public static SwingUniversalImage getImage( String location ) {
    return getImageAsResource( location );
  }

  /**
   * Internal image loading by ClassLoader.getResourceAsStream.
   */
  private static SwingUniversalImageSvg loadFromClassLoader( ClassLoader classLoader, String location ) {
    InputStream s = classLoader.getResourceAsStream( location );
    if ( s == null ) {
      return null;
    }
    try {
      return loadImage( s, location );
    } finally {
      IOUtils.closeQuietly( s );
    }
  }

  /**
   * Internal image loading by Thread.currentThread.getContextClassLoader.getResource.
   */
  private static SwingUniversalImageSvg loadFromCurrentClasspath( String location ) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    URL res = cl.getResource( location );
    if ( res == null ) {
      return null;
    }
    InputStream s;
    try {
      s = res.openStream();
    } catch ( IOException ex ) {
      return null;
    }
    if ( s == null ) {
      return null;
    }
    try {
      return loadImage( s, location );
    } finally {
      IOUtils.closeQuietly( s );
    }
  }

  /**
   * Internal image loading from Hop's user.dir VFS.
   */
  private static SwingUniversalImageSvg loadFromBasedVFS( String location ) {
    try {
      FileObject imageFileObject = HopVfs.getFileSystemManager().resolveFile( base, location );
      InputStream s = HopVfs.getInputStream( imageFileObject );
      if ( s == null ) {
        return null;
      }
      try {
        return loadImage( s, location );
      } finally {
        IOUtils.closeQuietly( s );
      }
    } catch ( FileSystemException ex ) {
      return null;
    }
  }

  /**
   * Internal image loading from Hop's VFS.
   */
  private static SwingUniversalImageSvg loadFromSimpleVFS( String location ) {
    try {
      InputStream s = HopVfs.getInputStream( location );
      if ( s == null ) {
        return null;
      }
      try {
        return loadImage( s, location );
      } finally {
        IOUtils.closeQuietly( s );
      }
    } catch ( HopFileException e ) {
      // do nothing. try to load next
    }
    return null;
  }

  /**
   * Load image from InputStream as bitmap image, or SVG image conversion to bitmap image.
   */
  private static SwingUniversalImageSvg loadImage( InputStream in, String filename ) {
    if ( !SvgSupport.isSvgName( filename ) ) {
     throw new RuntimeException("Only SVG images are supported, not : '"+filename+"'");
    } else {
      // svg image
      try {
        return new SwingUniversalImageSvg( SvgSupport.loadSvgImage( in ) );
      } catch ( Exception ex ) {
        throw new RuntimeException( ex );
      }
    }
  }
}
