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

package org.apache.hop.core;

import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.logging.ConsoleLoggingEventListener;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILoggingPlugin;
import org.apache.hop.core.logging.LoggingPluginType;
import org.apache.hop.core.logging.Slf4jLoggingEventListener;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.vfs.plugin.VfsPluginType;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This singleton is responsible for initializing the Hop client environment and remembering if it is initialized.
 * More specifically it loads client plugins like value meta plugins and other core Hop functionality.
 *
 * @author matt
 */
public class HopClientEnvironment {
  private static final Class<?> PKG = Const.class; // For Translator

  private static HopClientEnvironment instance = null;

  private static Boolean initialized;

  public enum ClientType {
    HOP_GUI, CLI, SERVER, OTHER;

    public String getID() {
      if ( this != OTHER ) {
        return this.name();
      }
      return instance.clientID;
    }
  }

  private ClientType client;
  // used when type is OTHER
  private String clientID = null;


  public static synchronized void init() throws HopException {
    init( Arrays.asList(
      LoggingPluginType.getInstance(),
      ValueMetaPluginType.getInstance(),
      DatabasePluginType.getInstance(),
      ExtensionPointPluginType.getInstance(),
      TwoWayPasswordEncoderPluginType.getInstance(),
      VfsPluginType.getInstance()
      )
    );
  }

  public static synchronized void init( List<IPluginType> pluginsToLoad ) throws HopException {
    if ( initialized != null ) {
      return;
    }

    if ( HopClientEnvironment.instance == null ) {
      HopClientEnvironment.instance = new HopClientEnvironment();
    }

    // Check the Hop Configuration backend
    //


    // Initialize the logging back-end.
    //
    HopLogStore.init();

    // Add console output so that folks see what's going on...
    //
    if ( !"Y".equalsIgnoreCase( System.getProperty( Const.HOP_DISABLE_CONSOLE_LOGGING, "N" ) ) ) {
      HopLogStore.getAppender().addLoggingEventListener( new ConsoleLoggingEventListener() );
    }
    HopLogStore.getAppender().addLoggingEventListener( new Slf4jLoggingEventListener() );

    // Load plugins
    //
    pluginsToLoad.forEach( PluginRegistry::addPluginType );
    PluginRegistry.init();

    List<IPlugin> loggingPlugins = PluginRegistry.getInstance().getPlugins( LoggingPluginType.class );
    initLogginPlugins( loggingPlugins );

    String passwordEncoderPluginID = Const.NVL( EnvUtil.getSystemProperty( Const.HOP_PASSWORD_ENCODER_PLUGIN ), "Hop" );

    Encr.init( passwordEncoderPluginID );

    initialized = new Boolean( true );
  }

  /**
   * Get all declared fields from the given class, also the ones from all super classes
   *
   * @param parentClass
   * @return A unique list of fields.
   */
  protected static final List<Field> findDeclaredFields( Class<?> parentClass ) {
    Set<Field> fields = new HashSet<>();

    for ( Field field : parentClass.getDeclaredFields() ) {
      fields.add( field );
    }
    Class<?> superClass = parentClass.getSuperclass();
    while ( superClass != null ) {
      for ( Field field : superClass.getDeclaredFields() ) {
        fields.add( field );
      }

      superClass = superClass.getSuperclass();
    }

    return new ArrayList<>( fields );
  }

  /**
   * Get all declared methods from the given class, also the ones from all super classes
   *
   * @param parentClass
   * @return A unique list of methods.
   */
  protected static final List<Method> findDeclaredMethods( Class<?> parentClass ) {
    Set<Method> methods = new HashSet<>();

    for ( Method method : parentClass.getDeclaredMethods() ) {
      methods.add( method );
    }
    Class<?> superClass = parentClass.getSuperclass();
    while ( superClass != null ) {
      for ( Method method : superClass.getDeclaredMethods() ) {
        methods.add( method );
      }

      superClass = superClass.getSuperclass();
    }

    return new ArrayList<>( methods );
  }

  public static boolean isInitialized() {
    return initialized != null;
  }

  private static void initLogginPlugins( List<IPlugin> logginPlugins ) throws HopPluginException {
    for ( IPlugin plugin : logginPlugins ) {
      ILoggingPlugin loggingPlugin = (ILoggingPlugin) PluginRegistry.getInstance().loadClass( plugin );
      loggingPlugin.init();
    }
  }


  public void setClient( ClientType client ) {
    this.client = client;
  }

  /**
   * Set the Client ID which has significance when the ClientType == OTHER
   *
   * @param id
   */
  public void setClientID( String id ) {
    this.clientID = id;
  }

  public ClientType getClient() {
    return this.client;
  }

  /**
   * Return this singleton. Create it if it hasn't been.
   *
   * @return
   */
  public static HopClientEnvironment getInstance() {

    if ( HopClientEnvironment.instance == null ) {
      HopClientEnvironment.instance = new HopClientEnvironment();
    }

    return HopClientEnvironment.instance;
  }

  public static void reset() {
    if ( HopLogStore.isInitialized() ) {
      HopLogStore.getInstance().reset();
    }
    PluginRegistry.getInstance().reset();
    initialized = null;
  }
}
