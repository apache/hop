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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.jar.JarFile;

public class HopURLClassLoader extends URLClassLoader {
  private static final Class<?> PKG = HopURLClassLoader.class; // For Translator

  private String name;

  public HopURLClassLoader( URL[] url, ClassLoader classLoader ) {
    super( url, classLoader );
  }

  public HopURLClassLoader( URL[] url, ClassLoader classLoader, String name ) {
    this( url, classLoader );
    this.name = name;
  }

  @Override
  protected void addURL( URL url ) {
    super.addURL( url );
  }

  @Override
  public String toString() {
    return super.toString() + " : " + name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  protected Class<?> loadClassFromThisLoader( String arg0, boolean arg1 ) throws ClassNotFoundException {
    Class<?> clz;
    if ( ( clz = findLoadedClass( arg0 ) ) != null ) {
      if ( arg1 ) {
        resolveClass( clz );
      }
      return clz;
    }

    if ( ( clz = findClass( arg0 ) ) != null ) {
      if ( arg1 ) {
        resolveClass( clz );
      }
      return clz;
    }
    return clz;
  }

  protected Class<?> loadClassFromParent( String name, boolean resolve ) throws ClassNotFoundException {
    Class<?> clz;
    if ( ( clz = getParent().loadClass( name ) ) != null ) {
      if ( resolve ) {
        resolveClass( clz );
      }
      return clz;
    }
    throw new ClassNotFoundException( "Could not find :" + name );
  }

  @Override
  protected synchronized Class<?> loadClass( String name, boolean resolve ) throws ClassNotFoundException {
    Throwable thisLoaderException = null;

    try {
      return loadClassFromThisLoader( name, resolve );
    } catch ( ClassNotFoundException | NoClassDefFoundError e ) {
      thisLoaderException = e;
    } catch ( SecurityException e ) {
      thisLoaderException = e;
    }

    try {
      return loadClassFromParent( name, resolve );
    } catch ( Exception e ) {
      if ( thisLoaderException != null ) {
        throw new ClassNotFoundException( "Unable to load class '" + name + "' in this classloader or in the parent", thisLoaderException );
      } else {
        throw e;
      }
    }
  }

  /*
   * Cglib doe's not creates custom class loader (to access package methods and classes ) it uses reflection to invoke
   * "defineClass", but you can call protected method in subclass without problems:
   */
  public Class<?> loadClass( String name, ProtectionDomain protectionDomain ) {
    Class<?> loaded = findLoadedClass( name );
    if ( loaded == null ) {
      // Get the jar, load the bytes from the jar file, construct class from scratch as in snippet below...

      /*
       *
       * loaded = super.findClass(name);
       *
       * URL url = super.findResource(newName);
       *
       * InputStream clis = getResourceAsStream(newName);
       */

      String newName = name.replace( '.', '/' );
      InputStream is = getResourceAsStream( newName );
      byte[] driverBytes = toBytes( is );

      loaded = super.defineClass( name, driverBytes, 0, driverBytes.length, protectionDomain );

    }
    return loaded;
  }

  private byte[] toBytes( InputStream is ) {
    byte[] retval = new byte[ 0 ];
    try {
      int a = is.available();
      while ( a > 0 ) {
        byte[] buffer = new byte[ a ];
        is.read( buffer );

        byte[] newretval = new byte[ retval.length + a ];

        for ( int i = 0; i < retval.length; i++ ) {
          newretval[ i ] = retval[ i ]; // old part
        }
        for ( int i = 0; i < a; i++ ) {
          newretval[ retval.length + i ] = buffer[ i ]; // new part
        }

        retval = newretval;

        a = is.available(); // see what's left
      }
      return retval;
    } catch ( Exception e ) {
      // TODO: Throw error here!!
      return null;
    }
  }

  private static Object getFieldObject( Class<?> clazz, String name, Object obj ) throws Exception {
    Field field = clazz.getDeclaredField( name );
    field.setAccessible( true );
    return field.get( obj );
  }

  /**
   * This method is designed to shutdown out classloader file locks in windows.
   */
  public void closeClassLoader() {
    HashSet<String> closedFiles = new HashSet<>();
    try {
      Object obj = getFieldObject( URLClassLoader.class, "ucp", this );
      ArrayList<?> loaders = (ArrayList<?>) getFieldObject( obj.getClass(), "loaders", obj );
      for ( Object ldr : loaders ) {
        try {
          JarFile file = (JarFile) getFieldObject( ldr.getClass(), "jar", ldr );
          closedFiles.add( file.getName() );
          file.close();
        } catch ( Exception e ) {
          // skip
        }
      }
    } catch ( Exception e ) {
      // skip
    }

    try {
      Vector<?> nativeLibArr = (Vector<?>) getFieldObject( ClassLoader.class, "nativeLibraries", this );
      for ( Object lib : nativeLibArr ) {
        try {
          Method fMethod = lib.getClass().getDeclaredMethod( "finalize" );
          fMethod.setAccessible( true );
          fMethod.invoke( lib );
        } catch ( Exception e ) {
          // skip
        }
      }
    } catch ( Exception e ) {
      // skip
    }

    HashMap<?, ?> uCache = null;
    HashMap<?, ?> fCache = null;

    try {
      Class<?> jarUrlConnClass = null;
      try {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        jarUrlConnClass = contextClassLoader.loadClass( "sun.net.www.protocol.jar.JarURLConnection" );
      } catch ( Throwable skip ) {
        // skip
      }
      if ( jarUrlConnClass == null ) {
        jarUrlConnClass = Class.forName( "sun.net.www.protocol.jar.JarURLConnection" );
      }
      Class<?> factory = getFieldObject( jarUrlConnClass, "factory", null ).getClass();
      try {
        fCache = (HashMap<?, ?>) getFieldObject( factory, "fileCache", null );
      } catch ( Exception e ) {
        // skip
      }
      try {
        uCache = (HashMap<?, ?>) getFieldObject( factory, "urlCache", null );
      } catch ( Exception e ) {
        // skip
      }
      if ( uCache != null ) {
        Set<?> set = null;
        while ( set == null ) {
          try {
            set = ( (HashMap<?, ?>) uCache.clone() ).keySet();
          } catch ( ConcurrentModificationException e ) {
            //Fix for BACKLOG-2149 - Do nothing - while loop will try again.
          }
        }

        for ( Object file : set ) {
          if ( file instanceof JarFile ) {
            JarFile jar = (JarFile) file;
            if ( !closedFiles.contains( jar.getName() ) ) {
              continue;
            }
            try {
              jar.close();
            } catch ( IOException e ) {
              // skip
            }
            if ( fCache != null ) {
              fCache.remove( uCache.get( jar ) );
            }
            uCache.remove( jar );
          }
        }
      } else if ( fCache != null ) {
        for ( Object key : ( (HashMap<?, ?>) fCache.clone() ).keySet() ) {
          Object file = fCache.get( key );
          if ( file instanceof JarFile ) {
            JarFile jar = (JarFile) file;
            if ( !closedFiles.contains( jar.getName() ) ) {
              continue;
            }
            try {
              jar.close();
            } catch ( IOException e ) {
              // ignore
            }
            fCache.remove( key );
          }
        }
      }
    } catch ( Exception e ) {
      // skip
      e.printStackTrace();
    }
  }

  @Override
  public URL getResource( String name ) {
    URL url;
    url = findResource( name );
    if ( url == null && getParent() != null ) {
      url = getParent().getResource( name );
    }
    return url;
  }
}
