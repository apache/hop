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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginClassMapException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * This singleton provides access to all the plugins in the Hop universe.<br> It allows you to register types and
 * plugins, query plugin lists per category, list plugins per type, etc.<br>
 *
 * @author matt
 */
public class PluginRegistry {

  private static final Class<?> PKG = PluginRegistry.class; // For Translator

  private static final PluginRegistry pluginRegistry = new PluginRegistry();

  private static final List<IPluginType> pluginTypes = new ArrayList<>();
  private static final List<IPluginRegistryExtension> extensions = new ArrayList<>();
  private static final String SUPPLEMENTALS_SUFFIX = "-supplementals";

  public static final ILogChannel log = new LogChannel( "PluginRegistry", true );

  // the list of plugins
  private final Map<Class<? extends IPluginType>, Set<IPlugin>> pluginMap = new HashMap<>();

  private final Map<Class<? extends IPluginType>, Map<IPlugin, URLClassLoader>> classLoaderMap = new HashMap<>();
  private final Map<URLClassLoader, Set<IPlugin>> inverseClassLoaderLookup = new HashMap<>();
  private final Map<String, URLClassLoader> classLoaderGroupsMap = new HashMap<>();
  private final Map<String, URLClassLoader> folderBasedClassLoaderMap = new HashMap<>();

  private final Map<IPlugin, String[]> parentClassloaderPatternMap = new HashMap<>();

  private final Map<Class<? extends IPluginType>, Set<IPluginTypeListener>> listeners = new HashMap<>();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private static final int WAIT_FOR_PLUGIN_TO_BE_AVAILABLE_LIMIT = 5;


  /**
   * Initialize the registry, keep private to keep this a singleton
   */
  private PluginRegistry() {
  }

  /**
   * @return The one and only PluginRegistry instance
   */
  public static PluginRegistry getInstance() {
    return pluginRegistry;
  }

  public void registerPluginType( Class<? extends IPluginType> pluginType ) {
    lock.writeLock().lock();
    try {
      pluginMap.computeIfAbsent( pluginType, k -> new TreeSet<>( Plugin.nullStringComparator ) );
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void removePlugin( Class<? extends IPluginType> pluginType, IPlugin plugin ) {
    lock.writeLock().lock();
    try {
      URLClassLoader ucl;
      Set<IPlugin> list = pluginMap.get( pluginType );
      if ( list != null ) {
        list.remove( plugin );
      }

      Map<IPlugin, URLClassLoader> classLoaders = classLoaderMap.get( plugin.getPluginType() );
      if ( classLoaders != null ) {
        classLoaders.remove( plugin );
      }

      if ( !StringUtils.isEmpty( plugin.getClassLoaderGroup() ) ) {
        // Straight away remove the class loader for the whole group...
        //
        ucl = classLoaderGroupsMap.remove( plugin.getClassLoaderGroup() );
        if ( ucl != null && classLoaders != null ) {
          for ( IPlugin p : inverseClassLoaderLookup.remove( ucl ) ) {
            classLoaders.remove( p );
          }
          try {
            ucl.close();
          } catch ( IOException e ) {
            e.printStackTrace();
          }
        }
      }

      if ( plugin.getPluginDirectory() != null ) {
        folderBasedClassLoaderMap.remove( plugin.getPluginDirectory().toString() );
      }
    } finally {
      lock.writeLock().unlock();
      Set<IPluginTypeListener> listeners = this.listeners.get( pluginType );
      if ( listeners != null ) {
        for ( IPluginTypeListener listener : listeners ) {
          listener.pluginRemoved( plugin );
        }
      }
      synchronized ( this ) {
        notifyAll();
      }
    }
  }

  public void addParentClassLoaderPatterns( IPlugin plugin, String[] patterns ) {
    lock.writeLock().lock();
    try {
      parentClassloaderPatternMap.put( plugin, patterns );
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void registerPlugin( Class<? extends IPluginType> pluginType, IPlugin plugin )
    throws HopPluginException {
    boolean changed = false; // Is this an add or an update?
    lock.writeLock().lock();
    try {
      if ( plugin.getIds()[ 0 ] == null ) {
        throw new HopPluginException( "Not a valid id specified in plugin :" + plugin );
      }

      // Keep the list of plugins sorted by name...
      //
      Set<IPlugin> list = pluginMap.computeIfAbsent( pluginType, k -> new TreeSet<>( Plugin.nullStringComparator ) );

      if ( !list.add( plugin ) ) {
        list.remove( plugin );
        list.add( plugin );
        changed = true;
      }
    } finally {
      lock.writeLock().unlock();
      Set<IPluginTypeListener> listeners = this.listeners.get( pluginType );
      if ( listeners != null ) {
        for ( IPluginTypeListener listener : listeners ) {
          // Changed or added?
          if ( changed ) {
            listener.pluginChanged( plugin );
          } else {
            listener.pluginAdded( plugin );
          }
        }
      }
      synchronized ( this ) {
        notifyAll();
      }
    }
  }

  /**
   * @return An unmodifiable list of plugin types
   */
  public List<Class<? extends IPluginType>> getPluginTypes() {
    lock.readLock().lock();
    try {
      return Collections.unmodifiableList( new ArrayList<>( pluginMap.keySet() ) );
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @param type The plugin type to query
   * @return The list of plugins
   */
  public <T extends IPlugin, K extends IPluginType> List<T> getPlugins( Class<K> type ) {
    List<T> result;
    lock.readLock().lock();
    try {
      result = pluginMap.keySet().stream()
        .filter( pi -> Const.classIsOrExtends( pi, type ) )
        .flatMap( pi -> pluginMap.get( pi ).stream() )
        .map( p -> (T) p )
        .collect( Collectors.toList() );
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  /**
   * Get a plugin from the registry
   *
   * @param pluginType The type of plugin to look for
   * @param id         The ID to scan for
   * @return the plugin or null if nothing was found.
   */
  public IPlugin getPlugin( Class<? extends IPluginType> pluginType, String id ) {
    if ( Utils.isEmpty( id ) ) {
      return null;
    }

    // getPlugins() never returns null, see his method above
    return getPlugins( pluginType ).stream()
      .filter( plugin -> plugin.matches( id ) )
      .findFirst()
      .orElse( null );
  }

  /**
   * Retrieve a list of plugins per category.
   *
   * @param pluginType     The type of plugins to search
   * @param pluginCategory The category to look in
   * @return An unmodifiable list of plugins that belong to the specified type and category.
   */
  public <T extends IPluginType> List<IPlugin> getPluginsByCategory( Class<T> pluginType,
                                                                     String pluginCategory ) {
    List<IPlugin> plugins = getPlugins( pluginType ).stream()
      .filter( plugin -> plugin.getCategory() != null && plugin.getCategory().equals( pluginCategory ) )
      .collect( Collectors.toList() );

    return Collections.unmodifiableList( plugins );
  }

  /**
   * List all the main types (classes) for the specified plugin type.
   *
   * @param pluginType
   * @return The list of plugin classes
   */
  public List<Class<?>> listMainTypes( Class<? extends IPluginType> pluginType ) {
    List<Class<?>> classes = new ArrayList<>();
    for ( IPlugin plugin : getPlugins( pluginType ) ) {
      classes.add( plugin.getMainType() );
    }
    return classes;
  }

  /**
   * Load and instantiate the main class of the plugin specified.
   *
   * @param plugin The plugin to load the main class for.
   * @return The instantiated class
   * @throws HopPluginException In case there was a loading problem.
   */
  public Object loadClass( IPlugin plugin ) throws HopPluginException {
    return loadClass( plugin, plugin.getMainType() );
  }

  /**
   * Load the class of the type specified for the plugin that owns the class of the specified object.
   *
   * @param pluginType the type of plugin
   * @param object     The object for which we want to search the class to find the plugin
   * @param classType  The type of class to load
   * @return the instantiated class.
   * @throws HopPluginException
   */
  public <T> T loadClass( Class<? extends IPluginType> pluginType, Object object, Class<T> classType )
    throws HopPluginException {
    IPlugin plugin = getPlugin( pluginType, object );
    if ( plugin == null ) {
      return null;
    }
    return loadClass( plugin, classType );
  }

  /**
   * Load the class of the type specified for the plugin with the ID specified.
   *
   * @param pluginType the type of plugin
   * @param pluginId   The plugin id to use
   * @param classType  The type of class to load
   * @return the instantiated class.
   * @throws HopPluginException
   */
  public <T> T loadClass( Class<? extends IPluginType> pluginType, String pluginId, Class<T> classType )
    throws HopPluginException {
    IPlugin plugin = getPlugin( pluginType, pluginId );
    if ( plugin == null ) {
      return null;
    }
    return loadClass( plugin, classType );
  }

  private HopURLClassLoader createClassLoader( IPlugin plugin ) throws MalformedURLException, UnsupportedEncodingException {

    List<String> jarFiles = plugin.getLibraries();
    URL[] urls = new URL[ jarFiles.size() ];
    for ( int i = 0; i < jarFiles.size(); i++ ) {
      File jarFile = new File( jarFiles.get( i ) );
      urls[ i ] = new URL( URLDecoder.decode( jarFile.toURI().toURL().toString(), "UTF-8" ) );
    }
    ClassLoader classLoader = getClass().getClassLoader();
    String[] patterns = parentClassloaderPatternMap.get( plugin );
    if ( patterns != null ) {
      return new HopSelectiveParentFirstClassLoader( urls, classLoader, plugin.getDescription(), patterns );
    } else {
      return new HopURLClassLoader( urls, classLoader, plugin.getDescription() );
    }
  }

  private void addToClassLoader( IPlugin plugin, HopURLClassLoader ucl ) throws MalformedURLException,
    UnsupportedEncodingException {
    String[] patterns = parentClassloaderPatternMap.get( plugin );

    if ( ucl instanceof HopSelectiveParentFirstClassLoader ) {
      ( (HopSelectiveParentFirstClassLoader) ucl ).addPatterns( patterns );
    }

    for ( String jarFile : plugin.getLibraries() ) {
      File jarfile = new File( jarFile );
      ucl.addURL( new URL( URLDecoder.decode( jarfile.toURI().toURL().toString(), "UTF-8" ) ) );
    }
  }

  /**
   * Add a Class Mapping + factory for a plugin. This allows extra classes to be added to existing plugins.
   *
   * @param pluginType Type of plugin
   * @param tClass     Class to factory
   * @param id         ID of the plugin to extend
   * @param callable   Factory Callable
   * @param <T>        Type of the object factoried
   * @throws HopPluginException
   */
  public <T> void addClassFactory( Class<? extends IPluginType> pluginType, Class<T> tClass, String id,
                                   Callable<T> callable ) throws HopPluginException {

    String key = createSupplemantalKey( pluginType.getName(), id );
    SupplementalPlugin supplementalPlugin = (SupplementalPlugin) getPlugin( pluginType, key );

    if ( supplementalPlugin == null ) {
      supplementalPlugin = new SupplementalPlugin( pluginType, key );
      registerPlugin( pluginType, supplementalPlugin );
    }
    supplementalPlugin.addFactory( tClass, callable );
  }

  private String createSupplemantalKey( String pluginName, String id ) {
    return pluginName + "-" + id + SUPPLEMENTALS_SUFFIX;
  }

  /**
   * Load and instantiate the plugin class specified
   *
   * @param plugin      the plugin to load
   * @param pluginClass the class to be loaded
   * @return The instantiated class
   * @throws HopPluginException In case there was a class loading problem somehow
   */
  @SuppressWarnings( "unchecked" )
  public <T> T loadClass( IPlugin plugin, Class<T> pluginClass ) throws HopPluginException {
    if ( plugin == null ) {
      throw new HopPluginException( BaseMessages.getString(
        PKG, "PluginRegistry.RuntimeError.NoValidTransformOrPlugin.PLUGINREGISTRY001" ) );
    }

    if ( plugin instanceof IClassLoadingPlugin ) {
      T aClass = ( (IClassLoadingPlugin) plugin ).loadClass( pluginClass );
      if ( aClass == null ) {
        throw new HopPluginClassMapException( BaseMessages
          .getString( PKG, "PluginRegistry.RuntimeError.NoValidClassRequested.PLUGINREGISTRY002", plugin.getName(),
            pluginClass.getName() ) );
      } else {
        return aClass;
      }
    } else {
      String className = plugin.getClassMap().get( pluginClass );
      if ( className == null ) {
        // Look for supplemental plugin supplying extra classes
        for ( String id : plugin.getIds() ) {
          try {
            T aClass = loadClass( plugin.getPluginType(), createSupplemantalKey( plugin.getPluginType().getName(), id ), pluginClass );
            if ( aClass != null ) {
              return aClass;
            }
          } catch ( HopPluginException exception ) {
            // ignore. we'll fall through to the other exception if this loop doesn't produce a return
          }
        }
        throw new HopPluginClassMapException( BaseMessages.getString( PKG,
          "PluginRegistry.RuntimeError.NoValidClassRequested.PLUGINREGISTRY002", plugin.getName(),
          pluginClass.getName() ) );
      }

      try {
        Class<? extends T> cl;
        if ( plugin.isNativePlugin() ) {
          cl = (Class<? extends T>) Class.forName( className );
        } else {
          ClassLoader ucl = getClassLoader( plugin );

          // Load the class.
          cl = (Class<? extends T>) ucl.loadClass( className );
        }

        return cl.newInstance();
      } catch ( ClassNotFoundException e ) {
        throw new HopPluginException( BaseMessages.getString(
          PKG, "PluginRegistry.RuntimeError.ClassNotFound.PLUGINREGISTRY003" ), e );
      } catch ( InstantiationException e ) {
        throw new HopPluginException( BaseMessages.getString(
          PKG, "PluginRegistry.RuntimeError.UnableToInstantiateClass.PLUGINREGISTRY004" ), e );
      } catch ( IllegalAccessException e ) {
        throw new HopPluginException( BaseMessages.getString(
          PKG, "PluginRegistry.RuntimeError.IllegalAccessToClass.PLUGINREGISTRY005" ), e );
      } catch ( Throwable e ) {
        e.printStackTrace();
        throw new HopPluginException( BaseMessages.getString(
          PKG, "PluginRegistry.RuntimeError.UnExpectedErrorLoadingClass.PLUGINREGISTRY007" ), e );
      }
    }
  }

  /**
   * Add a PluginType to be managed by the registry
   *
   * @param type
   */
  public static synchronized void addPluginType( IPluginType type ) {
    pluginTypes.add( type );
  }

  /**
   * Added so we can tell when types have been added (but not necessarily registered)
   *
   * @return the list of added plugin types
   */
  public static List<IPluginType> getAddedPluginTypes() {
    return Collections.unmodifiableList( pluginTypes );
  }

  public static synchronized void init() throws HopPluginException {
    init( true );
  }

  /**
   * This method registers plugin types and loads their respective plugins
   *
   * @throws HopPluginException
   */
  public static synchronized void init( boolean keepCache ) throws HopPluginException {
    final PluginRegistry registry = getInstance();

    for ( final IPluginType pluginType : pluginTypes ) {
      registry.registerType( pluginType );
    }

    // Clear the jar file cache so that we don't waste memory...
    //
    if ( !keepCache ) {
      JarCache.getInstance().clear();
    }
  }

  public void registerType( IPluginType pluginType ) throws HopPluginException {

    // Don't register the same type twice...
    //
    if (pluginMap.get( pluginType.getClass() )!=null) {
      return;
    }

    registerPluginType( pluginType.getClass() );

    // Search plugins for this type...
    //
    long startScan = System.currentTimeMillis();
    pluginType.searchPlugins();

    for ( IPluginRegistryExtension ext : extensions ) {
      ext.searchForType( pluginType );
    }

    Set<String> pluginClassNames = new HashSet<>();

    // Scan for plugin classes to facilitate debugging etc.
    //
    String pluginClasses = EnvUtil.getSystemProperty( Const.HOP_PLUGIN_CLASSES );
    if ( !Utils.isEmpty( pluginClasses ) ) {
      String[] classNames = pluginClasses.split( "," );
      Collections.addAll( pluginClassNames, classNames );
    }

    for ( String className : pluginClassNames ) {
      try {
        // What annotation does the plugin type have?
        //
        PluginAnnotationType annotationType = pluginType.getClass().getAnnotation( PluginAnnotationType.class );
        if ( annotationType != null ) {
          Class<? extends Annotation> annotationClass = annotationType.value();

          Class<?> clazz = Class.forName( className );
          Annotation annotation = clazz.getAnnotation( annotationClass );

          if ( annotation != null ) {
            // Register this one!
            //
            pluginType.handlePluginAnnotation( clazz, annotation, new ArrayList<>(), true, null );
            LogChannel.GENERAL.logBasic( "Plugin class "
              + className + " registered for plugin type '" + pluginType.getName() + "'" );
          } else {
            if ( HopLogStore.isInitialized() && LogChannel.GENERAL.isDebug() ) {
              LogChannel.GENERAL.logDebug( "Plugin class "
                + className + " doesn't contain annotation for plugin type '" + pluginType.getName() + "'" );
            }
          }
        } else {
          if ( HopLogStore.isInitialized() && LogChannel.GENERAL.isDebug() ) {
            LogChannel.GENERAL.logDebug( "Plugin class "
              + className + " doesn't contain valid class for plugin type '" + pluginType.getName() + "'" );
          }
        }
      } catch ( Exception e ) {
        if ( HopLogStore.isInitialized() ) {
          LogChannel.GENERAL.logError( "Error registring plugin class from HOP_PLUGIN_CLASSES: "
            + className + Const.CR + Const.getSimpleStackTrace( e ) + Const.CR + Const.getStackTracker( e ) );
        }
      }
    }

    if ( LogChannel.GENERAL.isDetailed() ) {
      LogChannel.GENERAL.logDetailed( "Registered "
        + getPlugins( pluginType.getClass() ).size() + " plugins of type '" + pluginType.getName() + "' in "
        + ( System.currentTimeMillis() - startScan ) + "ms." );
    }

  }

  /**
   * Find the plugin ID based on the class
   *
   * @param pluginClass
   * @return The ID of the plugin to which this class belongs (checks the plugin class maps)
   */
  public String getPluginId( Object pluginClass ) {
    return getPluginTypes().stream()
      .map( pluginType -> getPluginId( pluginType, pluginClass ) )
      .filter( Objects::nonNull )
      .findFirst()
      .orElse( null );
  }

  /**
   * Find the plugin ID based on the class
   *
   * @param pluginType  the type of plugin
   * @param pluginClass The class to look for
   * @return The ID of the plugin to which this class belongs (checks the plugin class maps) or null if nothing was
   * found.
   */
  public String getPluginId( Class<? extends IPluginType> pluginType, Object pluginClass ) {
    String className = pluginClass.getClass().getName();

    IPlugin plugin = getPlugins( pluginType ).stream()
      .filter( p -> p.getClassMap().values().contains( className ) )
      .findFirst()
      .orElse( null );

    if ( plugin != null ) {
      return plugin.getIds()[ 0 ];
    }

    return extensions.stream()
      .map( ext -> ext.getPluginId( pluginType, pluginClass ) )
      .filter( Objects::nonNull )
      .findFirst()
      .orElse( null );
  }

  /**
   * Retrieve the Plugin for a given class
   *
   * @param pluginType  The type of plugin to search for
   * @param pluginClass The class of this object is used to look around
   * @return the plugin or null if nothing could be found
   */
  public IPlugin getPlugin( Class<? extends IPluginType> pluginType, Object pluginClass ) {
    String pluginId = getPluginId( pluginType, pluginClass );
    if ( pluginId == null ) {
      return null;
    }
    return getPlugin( pluginType, pluginId );
  }

  /**
   * Find the plugin ID based on the name of the plugin
   *
   * @param pluginType the type of plugin
   * @param pluginName The name to look for
   * @return The plugin with the specified name or null if nothing was found.
   */
  public IPlugin findPluginWithName( Class<? extends IPluginType> pluginType, String pluginName ) {
    return getPlugins( pluginType ).stream()
      .filter( plugin -> plugin.getName().equals( pluginName ) )
      .findFirst()
      .orElse( null );
  }

  /**
   * Find the plugin ID based on the description of the plugin
   *
   * @param pluginType        the type of plugin
   * @param pluginDescription The description to look for
   * @return The plugin with the specified description or null if nothing was found.
   */
  public IPlugin findPluginWithDescription( Class<? extends IPluginType> pluginType,
                                            String pluginDescription ) {
    return getPlugins( pluginType ).stream()
      .filter( plugin -> plugin.getDescription().equals( pluginDescription ) )
      .findFirst()
      .orElse( null );
  }

  /**
   * Find the plugin ID based on the name of the plugin
   *
   * @param pluginType the type of plugin
   * @param pluginId   The name to look for
   * @return The plugin with the specified name or null if nothing was found.
   */
  public IPlugin findPluginWithId( Class<? extends IPluginType> pluginType, String pluginId ) {
    return getPlugin( pluginType, pluginId );
  }

  /**
   * @return a unique list of all the transform plugin package names
   */
  public List<String> getPluginPackages( Class<? extends IPluginType> pluginType ) {
    Set<String> list = new TreeSet<>();
    for ( IPlugin plugin : getPlugins( pluginType ) ) {
      for ( String className : plugin.getClassMap().values() ) {
        int lastIndex = className.lastIndexOf( "." );
        if ( lastIndex > -1 ) {
          list.add( className.substring( 0, lastIndex ) );
        }
      }
    }
    return new ArrayList<>( list );
  }

  private IRowMeta getPluginInformationRowMeta() {
    IRowMeta row = new RowMeta();

    row.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "PluginRegistry.Information.Type.Label" ) ) );
    row.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "PluginRegistry.Information.ID.Label" ) ) );
    row.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "PluginRegistry.Information.Name.Label" ) ) );
    row.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "PluginRegistry.Information.Description.Label" ) ) );
    row.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "PluginRegistry.Information.ImageFile.Label" ) ) );
    row.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "PluginRegistry.Information.Category.Label" ) ) );
    row.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "PluginRegistry.Information.Keywords.Label" ) ) );
    row.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "PluginRegistry.Information.ClassName.Label" ) ) );
    row.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "PluginRegistry.Information.Libraries.Label" ) ) );

    return row;
  }

  /**
   * @param pluginType the type of plugin to get information for
   * @return a row buffer containing plugin information for the given plugin type
   * @throws HopPluginException
   */
  public RowBuffer getPluginInformation( Class<? extends IPluginType> pluginType )
    throws HopPluginException {
    RowBuffer rowBuffer = new RowBuffer( getPluginInformationRowMeta() );
    for ( IPlugin plugin : getPlugins( pluginType ) ) {

      Object[] row = new Object[ getPluginInformationRowMeta().size() ];
      int rowIndex = 0;

      row[ rowIndex++ ] = getPluginType( plugin.getPluginType() ).getName();
      row[ rowIndex++ ] = plugin.getIds()[ 0 ];
      row[ rowIndex++ ] = plugin.getName();
      row[ rowIndex++ ] = Const.NVL( plugin.getDescription(), "" );
      row[ rowIndex++ ] = Const.NVL( plugin.getImageFile(), "" );
      row[ rowIndex++ ] = Const.NVL( plugin.getCategory(), "" );
      row[ rowIndex++ ] = String.join( ",", plugin.getKeywords() );
      row[ rowIndex++ ] = plugin.getClassMap().values().toString();
      row[ rowIndex++ ] = String.join( ",", plugin.getLibraries() );

      rowBuffer.getBuffer().add( row );
    }
    return rowBuffer;
  }

  /**
   * Load the class with a certain name using the class loader of certain plugin.
   *
   * @param plugin    The plugin for which we want to use the class loader
   * @param className The name of the class to load
   * @return the name of the class
   * @throws HopPluginException In case there is something wrong
   */
  @SuppressWarnings( "unchecked" )
  public <T> T getClass( IPlugin plugin, String className ) throws HopPluginException {
    try {
      if ( plugin.isNativePlugin() ) {
        return (T) Class.forName( className );
      } else if ( ( plugin instanceof IClassLoadingPlugin ) && ( (IClassLoadingPlugin) plugin ).getClassLoader() != null ) {
        return (T) ( (IClassLoadingPlugin) plugin ).getClassLoader().loadClass( className );
      } else {
        URLClassLoader ucl;
        lock.writeLock().lock();
        try {
          Map<IPlugin, URLClassLoader> classLoaders = classLoaderMap.computeIfAbsent( plugin.getPluginType(), k -> new HashMap<>() );
          ucl = classLoaders.get( plugin );

          if ( ucl == null ) {
            if ( !Utils.isEmpty( plugin.getClassLoaderGroup() ) ) {
              ucl = classLoaderGroupsMap.get( plugin.getClassLoaderGroup() );
            } else {
              ucl = folderBasedClassLoaderMap.get( plugin.getPluginDirectory().toString() );
            }
          }
          if ( ucl != null ) {
            classLoaders.put( plugin, ucl ); // save for later use...
          }
        } finally {
          lock.writeLock().unlock();
        }

        if ( ucl == null ) {
          throw new HopPluginException( "Unable to find class loader for plugin: " + plugin );
        }
        return (T) ucl.loadClass( className );

      }
    } catch ( Exception e ) {
      throw new HopPluginException( "Unexpected error loading class with name: " + className, e );
    }
  }

  /**
   * Load the class with a certain name using the class loader of certain plugin.
   *
   * @param plugin    The plugin for which we want to use the class loader
   * @param classType The type of class to load
   * @return the name of the class
   * @throws HopPluginException In case there is something wrong
   */
  @SuppressWarnings( "unchecked" )
  public <T> T getClass( IPlugin plugin, T classType ) throws HopPluginException {
    String className = plugin.getClassMap().get( classType );
    if ( className == null ) {
      throw new HopPluginException( "Unable to find the classname for class type " + classType.getClass().getName() );
    }
    return (T) getClass( plugin, className );
  }

  /**
   * Create or retrieve the class loader for the specified plugin
   *
   * @param plugin the plugin to use
   * @return The class loader
   * @throws HopPluginException In case there was a problem
   *                            <p/>
   *                            getClassLoader();
   */
  public ClassLoader getClassLoader( IPlugin plugin ) throws HopPluginException {

    if ( plugin == null ) {
      throw new HopPluginException( BaseMessages.getString(
        PKG, "PluginRegistry.RuntimeError.NoValidTransformOrPlugin.PLUGINREGISTRY001" ) );
    }

    try {
      if ( plugin.isNativePlugin() ) {
        return this.getClass().getClassLoader();
      } else {
        URLClassLoader ucl;

        lock.writeLock().lock();
        try {
          // If the plugin needs to have a separate class loader for each instance of the plugin.
          // This is not the default. By default we cache the class loader for each plugin ID.
          if ( plugin.isSeparateClassLoaderNeeded() ) {
            // Create a new one each time
            ucl = createClassLoader( plugin );
          } else {
            // See if we can find a class loader to re-use.
            Map<IPlugin, URLClassLoader> classLoaders = classLoaderMap.computeIfAbsent( plugin.getPluginType(), k -> new HashMap<>() );
            ucl = classLoaders.get( plugin );

            if ( ucl == null ) {
              // check if plugin belongs to a group we can use
              if ( !Utils.isEmpty( plugin.getClassLoaderGroup() ) ) {
                ucl = classLoaderGroupsMap.get( plugin.getClassLoaderGroup() );
                if ( ucl == null ) {
                  ucl = createClassLoader( plugin );
                  classLoaders.put( plugin, ucl ); // save for later use...
                  inverseClassLoaderLookup.computeIfAbsent( ucl, k -> new HashSet<>() ).add( plugin );
                  classLoaderGroupsMap.put( plugin.getClassLoaderGroup(), ucl );
                } else {
                  // we have a classloader, but does it have this plugin?
                  try {
                    ucl.loadClass( plugin.getClassMap().values().iterator().next() );
                  } catch ( ClassNotFoundException ignored ) {
                    // missed, add it to the classloader
                    addToClassLoader( plugin, (HopURLClassLoader) ucl );
                  }
                }
              } else {
                // fallthrough folder based plugin
                if ( !plugin.isUsingLibrariesOutsidePluginFolder() && plugin.getPluginDirectory() != null ) {
                  ucl = folderBasedClassLoaderMap.get( plugin.getPluginDirectory().toString() );
                  if ( ucl == null ) {
                    ucl = createClassLoader( plugin );
                    classLoaders.put( plugin, ucl ); // save for later use...
                    inverseClassLoaderLookup.computeIfAbsent( ucl, k -> new HashSet<>() ).add( plugin );
                    folderBasedClassLoaderMap.put( plugin.getPluginDirectory().toString(), ucl );
                  } else {
                    // we have a classloader, but does it have this plugin?
                    try {
                      ucl.loadClass( plugin.getClassMap().values().iterator().next() );
                    } catch ( ClassNotFoundException ignored ) {
                      // missed, add it to the classloader
                      addToClassLoader( plugin, (HopURLClassLoader) ucl );
                    }
                  }
                } else {
                  ucl = classLoaders.get( plugin );
                  if ( ucl == null ) {
                    if ( plugin.getLibraries().size() == 0 ) {
                      if ( plugin instanceof IClassLoadingPlugin ) {
                        return ( (IClassLoadingPlugin) plugin ).getClassLoader();
                      }
                    }
                    ucl = createClassLoader( plugin );
                    classLoaders.put( plugin, ucl ); // save for later use...
                    inverseClassLoaderLookup.computeIfAbsent( ucl, k -> new HashSet<>() ).add( plugin );
                  }
                }
              }
            }
          }
        } finally {
          lock.writeLock().unlock();
        }

        // Load the class.
        return ucl;
      }
    } catch ( MalformedURLException e ) {
      throw new HopPluginException( BaseMessages.getString(
        PKG, "PluginRegistry.RuntimeError.MalformedURL.PLUGINREGISTRY006" ), e );
    } catch ( Throwable e ) {
      e.printStackTrace();
      throw new HopPluginException( BaseMessages.getString(
        PKG, "PluginRegistry.RuntimeError.UnExpectedCreatingClassLoader.PLUGINREGISTRY008" ), e );
    }
  }

  /**
   * Allows the tracking of plugins as they come and go.
   *
   * @param typeToTrack extension of IPluginType to track.
   * @param listener    receives notification when a plugin of the specified type is added/removed/modified
   * @param <T>         extension of IPluginType
   */
  public <T extends IPluginType> void addPluginListener( Class<T> typeToTrack, IPluginTypeListener listener ) {
    lock.writeLock().lock();
    try {
      Set<IPluginTypeListener> list = listeners.computeIfAbsent( typeToTrack, k -> new HashSet<>() );
      list.add( listener );
    } finally {
      lock.writeLock().unlock();
    }
  }

  public IPluginType getPluginType( Class<? extends IPluginType> pluginTypeClass )
    throws HopPluginException {
    try {
      // All these plugin type interfaces are singletons...
      // So we should call a static getInstance() method...
      //
      Method method = pluginTypeClass.getMethod( "getInstance", new Class<?>[ 0 ] );

      return (IPluginType) method.invoke( null, new Object[ 0 ] );
    } catch ( Exception e ) {
      throw new HopPluginException( "Unable to get instance of plugin type: " + pluginTypeClass.getName(), e );
    }
  }

  public List<IPlugin> findPluginsByFolder( URL folder ) {
    String path = folder.getPath();
    try {
      path = folder.toURI().normalize().getPath();
    } catch ( URISyntaxException e ) {
      log.logError( e.getLocalizedMessage(), e );
    }
    if ( path.endsWith( "/" ) ) {
      path = path.substring( 0, path.length() - 1 );
    }
    List<IPlugin> result = new ArrayList<>();
    lock.readLock().lock();
    try {
      for ( Set<IPlugin> typeInterfaces : pluginMap.values() ) {
        for ( IPlugin plugin : typeInterfaces ) {
          URL pluginFolder = plugin.getPluginDirectory();
          try {
            if ( pluginFolder != null && pluginFolder.toURI().normalize().getPath().startsWith( path ) ) {
              result.add( plugin );
            }
          } catch ( URISyntaxException e ) {
            log.logError( e.getLocalizedMessage(), e );
          }
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  public void reset() {
    lock.writeLock().lock();
    try {
      pluginTypes.clear();
      extensions.clear();
      pluginMap.clear();
      classLoaderMap.clear();
      classLoaderGroupsMap.clear();
      folderBasedClassLoaderMap.clear();
      inverseClassLoaderLookup.forEach( ( key, value ) -> {
        try {
          key.close();
        } catch ( IOException e ) {
          e.printStackTrace();
        }
      } );
      inverseClassLoaderLookup.clear();
      parentClassloaderPatternMap.clear();
      listeners.clear();
      JarCache.getInstance().clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public IPlugin findPluginWithId( Class<? extends IPluginType> pluginType, String pluginId, boolean waitForPluginToBeAvailable ) {
    IPlugin pluginInterface = findPluginWithId( pluginType, pluginId );
    return waitForPluginToBeAvailable && pluginInterface == null
      ? waitForPluginToBeAvailable( pluginType, pluginId, WAIT_FOR_PLUGIN_TO_BE_AVAILABLE_LIMIT )
      : pluginInterface;
  }

  private IPlugin waitForPluginToBeAvailable( Class<? extends IPluginType> pluginType, String pluginId, int waitLimit ) {
    int timeToSleep = 50;
    try {
      Thread.sleep( timeToSleep );
      waitLimit -= timeToSleep;
    } catch ( InterruptedException e ) {
      log.logError( e.getLocalizedMessage(), e );
      Thread.currentThread().interrupt();
      return null;
    }
    IPlugin pluginInterface = findPluginWithId( pluginType, pluginId );
    return waitLimit <= 0 && pluginInterface == null
      ? null
      : pluginInterface != null
      ? pluginInterface
      : waitForPluginToBeAvailable( pluginType, pluginId, waitLimit );
  }

  /**
   * Try to register the given annotated class, present in the classpath, as a plugin
   *
   * @param pluginClassName The name of the class to register
   * @param pluginTypeClass The type of plugin to register
   * @param annotationClass The type of annotation to consider
   */
  public void registerPluginClass( String pluginClassName, Class<? extends IPluginType> pluginTypeClass, Class<? extends Annotation> annotationClass ) throws HopPluginException {
    registerPluginClass(getClass().getClassLoader(), Collections.emptyList(), null, pluginClassName, pluginTypeClass, annotationClass, true);
  }

    /**
     * Try to register the given annotated class, present in the classpath, as a plugin
     *
     * @param classLoader the classloader to find the class with
     * @param pluginClassName The name of the class to register
     * @param pluginTypeClass The type of plugin to register
     * @param annotationClass The type of annotation to consider
     * @param isNative if this is a native plugin (in the classpath) or an external plugin
     */
  public void registerPluginClass( ClassLoader classLoader, List<String> libraries, URL pluginUrl, String pluginClassName, Class<? extends IPluginType> pluginTypeClass, Class<? extends Annotation> annotationClass, boolean isNative ) throws HopPluginException {
    IPluginType pluginType = getPluginType( pluginTypeClass );
    try {
      Class<?> pluginClass = classLoader.loadClass( pluginClassName );
      Annotation annotation = pluginClass.getAnnotation( annotationClass );
      if ( annotation == null ) {
        throw new HopPluginException( "The requested annotation '" + annotationClass.getName() + " couldn't be found in the plugin class" );
      }

      // Register the plugin using the metadata in the annotation
      //
      pluginType.handlePluginAnnotation( pluginClass, annotation, libraries, isNative, pluginUrl );

    } catch ( ClassNotFoundException e ) {
      throw new HopPluginException( "Sorry, the plugin class you want to register '" + pluginClassName + "' can't be found in the classpath", e );
    }
  }

  /**
   * Try to find the plugin ID using the main class of the plugin.
   *
   * @param pluginTypeClass The plugin type
   * @param className       The main classname to search for
   * @return The plugin ID or null if nothing was found.
   */
  public String findPluginIdWithMainClassName( Class<? extends IPluginType> pluginTypeClass, String className ) {
    List<IPlugin> plugins = getPlugins( pluginTypeClass );
    for ( IPlugin plugin : plugins ) {
      String mainClassName = plugin.getClassMap().get( plugin.getMainType() );
      if ( mainClassName != null && mainClassName.equals( className ) ) {
        return plugin.getIds()[ 0 ];
      }
    }
    return null;
  }
}
