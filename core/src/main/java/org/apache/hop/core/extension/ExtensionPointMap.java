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
package org.apache.hop.core.extension;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.IPluginTypeListener;
import org.apache.hop.core.variables.IVariables;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class maintains a map of IExtensionPoint object to its name.
 */
public class ExtensionPointMap {

  private static ILogChannel log = new LogChannel( "ExtensionPointMap" );
  private static ExtensionPointMap INSTANCE = new ExtensionPointMap( PluginRegistry.getInstance() );

  private final PluginRegistry registry;
  private Table<String, String, Supplier<IExtensionPoint>> extensionPointPluginMap;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private ExtensionPointMap( PluginRegistry pluginRegistry ) {
    this.registry = pluginRegistry;
    extensionPointPluginMap = HashBasedTable.create();
    registry.addPluginListener( ExtensionPointPluginType.class, new IPluginTypeListener() {

      @Override
      public void pluginAdded( Object serviceObject ) {
        addExtensionPoint( (IPlugin) serviceObject );
      }

      @Override
      public void pluginRemoved( Object serviceObject ) {
        removeExtensionPoint( (IPlugin) serviceObject );
      }

      @Override
      public void pluginChanged( Object serviceObject ) {
        removeExtensionPoint( (IPlugin) serviceObject );
        addExtensionPoint( (IPlugin) serviceObject );
      }

    } );

    List<IPlugin> extensionPointPlugins = registry.getPlugins( ExtensionPointPluginType.class );
    for ( IPlugin extensionPointPlugin : extensionPointPlugins ) {
      addExtensionPoint( extensionPointPlugin );
    }
  }

  public static ExtensionPointMap getInstance() {
    return INSTANCE;
  }

  /**
   * Add the extension point plugin to the map
   *
   * @param extensionPointPlugin
   */
  public void addExtensionPoint( IPlugin extensionPointPlugin ) {
    lock.writeLock().lock();
    try {
      for ( String id : extensionPointPlugin.getIds() ) {
        extensionPointPluginMap.put( extensionPointPlugin.getName(), id, createLazyLoader( extensionPointPlugin ) );
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Remove the extension point plugin from the map
   *
   * @param extensionPointPlugin
   */
  public void removeExtensionPoint( IPlugin extensionPointPlugin ) {
    lock.writeLock().lock();
    try {
      for ( String id : extensionPointPlugin.getIds() ) {
        extensionPointPluginMap.remove( extensionPointPlugin.getName(), id );
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Reinitialize the extension point plugins map
   */
  public void reInitialize() {
    lock.writeLock().lock();
    try {
      extensionPointPluginMap = HashBasedTable.create();
      final PluginRegistry registry = PluginRegistry.getInstance();
      List<IPlugin> extensionPointPlugins = registry.getPlugins( ExtensionPointPluginType.class );
      for ( IPlugin extensionPointPlugin : extensionPointPlugins ) {
        addExtensionPoint( extensionPointPlugin );
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  Supplier<IExtensionPoint> createLazyLoader( IPlugin extensionPointPlugin ) {
    return Suppliers.memoize( new ExtensionPointLoader( extensionPointPlugin ) );
  }

  /**
   * Call the extension point(s) corresponding to the given id
   * <p>
   * This iteration was isolated here to protect against ConcurrentModificationException using PluginRegistry's lock
   *  @param log    log channel to pass to extension point call
   * @param variables
   * @param id     the id of the extension point interface
   * @param object object to pass to extension point call
   */
  public void callExtensionPoint( ILogChannel log, IVariables variables, String id, Object object ) throws HopException {
    lock.readLock().lock();
    try {
      if ( extensionPointPluginMap.containsRow( id ) && !extensionPointPluginMap.rowMap().get( id ).values().isEmpty() ) {
        for ( Supplier<IExtensionPoint> extensionPoint : extensionPointPluginMap.row( id ).values() ) {
          extensionPoint.get().callExtensionPoint( log, variables, object );
        }
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the element in the position (rowId,columnId) of the table
   * <p>
   * Useful for Unit Testing
   *
   * @param rowId    the key of the row to be accessed
   * @param columnId the key of the column to be accessed
   */
  IExtensionPoint getTableValue( String rowId, String columnId ) {
    lock.readLock().lock();
    try {
      return extensionPointPluginMap.contains( rowId, columnId )
        ? extensionPointPluginMap.get( rowId, columnId ).get() : null;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the number of rows of the table
   * <p>
   * Useful for Unit Testing
   */
  int getNumberOfRows() {
    lock.readLock().lock();
    try {
      return extensionPointPluginMap.rowMap().size();
    } finally {
      lock.readLock().unlock();
    }
  }

  private class ExtensionPointLoader implements Supplier<IExtensionPoint> {
    private final IPlugin extensionPointPlugin;

    private ExtensionPointLoader( IPlugin extensionPointPlugin ) {
      this.extensionPointPlugin = extensionPointPlugin;
    }

    @Override public IExtensionPoint get() {
      try {
        return registry.loadClass( extensionPointPlugin, IExtensionPoint.class );
      } catch ( Exception e ) {
        getLog().logError( "Unable to load extension point for name = ["
          + ( extensionPointPlugin != null ? extensionPointPlugin.getName() : "null" ) + "]", e );
        return null;
      }
    }
  }

  public static ILogChannel getLog() {
    if ( log == null ) {
      log = new LogChannel( "ExtensionPointMap" );
    }
    return log;
  }

  public void reset() {
    lock.writeLock().lock();
    try {
      extensionPointPluginMap.clear();
      registry.addPluginListener( ExtensionPointPluginType.class, new IPluginTypeListener() {

        @Override
        public void pluginAdded( Object serviceObject ) {
          addExtensionPoint( (IPlugin) serviceObject );
        }

        @Override
        public void pluginRemoved( Object serviceObject ) {
          removeExtensionPoint( (IPlugin) serviceObject );
        }

        @Override
        public void pluginChanged( Object serviceObject ) {
          removeExtensionPoint( (IPlugin) serviceObject );
          addExtensionPoint( (IPlugin) serviceObject );
        }

      } );
    } finally {
      lock.writeLock().unlock();
    }
  }
}
