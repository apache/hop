/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.hop.core.auth.AuthenticationConsumerPluginType;
import org.apache.hop.core.auth.AuthenticationProviderPluginType;
import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.lifecycle.HopLifecycleSupport;
import org.apache.hop.core.logging.LogTablePluginType;
import org.apache.hop.core.plugins.HopServerPluginType;
import org.apache.hop.core.plugins.ImportRulePluginType;
import org.apache.hop.core.plugins.JobEntryDialogFragmentType;
import org.apache.hop.core.plugins.JobEntryPluginType;
import org.apache.hop.core.plugins.HopLifecyclePluginType;
import org.apache.hop.core.plugins.LifecyclePluginType;
import org.apache.hop.core.plugins.PartitionerPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.PluginTypeInterface;
import org.apache.hop.core.plugins.RepositoryPluginType;
import org.apache.hop.core.plugins.StepDialogFragmentType;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.repository.IUser;
import org.apache.hop.repository.Repository;
import org.apache.hop.trans.step.RowDistributionPluginType;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The HopEnvironment class contains settings and properties for all of Hop. Initialization of the environment is
 * done by calling the init() method, which reads in properties file(s), registers plugins, etc. Initialization should
 * be performed once at application startup; for example, HopUi's main() method calls HopEnvironment.init() in order
 * to prepare the environment for usage by HopUi.
 */
public class HopEnvironment {

  private static Class<?> PKG = Const.class; // for i18n purposes, needed by Translator2!!

  /**
   * Indicates whether the Hop environment has been initialized.
   */
  private static AtomicReference<SettableFuture<Boolean>> initialized =
    new AtomicReference<>( null );
  private static HopLifecycleSupport kettleLifecycleSupport;

  /**
   * Initializes the Hop environment. This method will attempt to configure Simple JNDI, by simply calling
   * init(true).
   *
   * @throws HopException Any errors that occur during initialization will throw a HopException.
   * @see HopEnvironment#init(boolean)
   */
  public static void init() throws HopException {
    init( true );
  }

  public static void init( Class<? extends PluginTypeInterface> pluginClasses ) {

  }

  /**
   * Initializes the Hop environment. This method performs the following operations:
   * <p/>
   * - Creates a Hop "home" directory if it does not already exist - Reads in the kettle.properties file -
   * Initializes the logging back-end - Sets the console log level to debug - If specified by parameter, configures
   * Simple JNDI - Registers the native types and the plugins for the various plugin types - Reads the list of variables
   * - Initializes the Lifecycle listeners
   *
   * @param simpleJndi true to configure Simple JNDI, false otherwise
   * @throws HopException Any errors that occur during initialization will throw a HopException.
   */
  public static void init( boolean simpleJndi ) throws HopException {
    init( Arrays.asList(
      RowDistributionPluginType.getInstance(),
      StepPluginType.getInstance(),
      StepDialogFragmentType.getInstance(),
      PartitionerPluginType.getInstance(),
      JobEntryPluginType.getInstance(),
      JobEntryDialogFragmentType.getInstance(),
      LogTablePluginType.getInstance(),
      RepositoryPluginType.getInstance(),
      LifecyclePluginType.getInstance(),
      HopLifecyclePluginType.getInstance(),
      ImportRulePluginType.getInstance(),
      HopServerPluginType.getInstance(),
      CompressionPluginType.getInstance(),
      AuthenticationProviderPluginType.getInstance(),
      AuthenticationConsumerPluginType.getInstance()
    ), simpleJndi );
  }

  public static void init( List<PluginTypeInterface> pluginClasses, boolean simpleJndi ) throws HopException {

    SettableFuture<Boolean> ready;
    if ( initialized.compareAndSet( null, ready = SettableFuture.create() ) ) {

      // Swaps out System Properties for a thread safe version.  This is needed so Karaf can spawn multiple instances.
      // See https://jira.pentaho.com/browse/PDI-17496
      System.setProperties( ConcurrentMapProperties.convertProperties( System.getProperties() ) );

      try {
        // This creates .kettle and kettle.properties...
        //
        if ( !HopClientEnvironment.isInitialized() ) {
          HopClientEnvironment.init();
        }

        // Configure Simple JNDI when we run in stand-alone mode (spoon, pan, kitchen, carte, ... NOT on the platform
        //
        if ( simpleJndi ) {
          JndiUtil.initJNDI();
        }

        // Register the native types and the plugins for the various plugin types...
        //
        pluginClasses.forEach( PluginRegistry::addPluginType );
        PluginRegistry.init();

        // Also read the list of variables.
        //
        HopVariablesList.init();

        // Initialize the Lifecycle Listeners
        //
        initLifecycleListeners();
        ready.set( true );
      } catch ( Throwable t ) {
        ready.setException( t );
        // If it's a HopException, throw it, otherwise wrap it in a HopException
        throw ( ( t instanceof HopException ) ? (HopException) t : new HopException( t ) );
      }

    } else {
      // A different thread is initializing
      ready = initialized.get();
      // Block until environment is initialized
      try {
        ready.get();
      } catch ( Throwable t ) {
        throw new HopException( t );
      }
    }
  }

  /**
   * Alert all Lifecycle plugins that the Hop environment is being initialized.
   *
   * @throws HopException when a lifecycle listener throws an exception
   */
  private static void initLifecycleListeners() throws HopException {
    kettleLifecycleSupport = new HopLifecycleSupport();
    kettleLifecycleSupport.onEnvironmentInit();
    final HopLifecycleSupport s = kettleLifecycleSupport;

    // Register a shutdown hook to invoke the listener's onExit() methods
    Runtime.getRuntime().addShutdownHook( new Thread() {
      public void run() {
        shutdown( s );
      }
    } );

  }

  // Shutdown the Hop environment programmatically
  public static void shutdown() {
    shutdown( kettleLifecycleSupport );
  }

  private static void shutdown( HopLifecycleSupport kettleLifecycleSupport ) {
    if ( isInitialized() ) {
      try {
        kettleLifecycleSupport.onEnvironmentShutdown();
      } catch ( Throwable t ) {
        System.err.println( BaseMessages.getString( PKG,
          "LifecycleSupport.ErrorInvokingHopEnvironmentShutdownListeners" ) );
        t.printStackTrace();
      }
    }
  }

  /**
   * Checks if the Hop environment has been initialized.
   *
   * @return true if initialized, false otherwise
   */
  public static boolean isInitialized() {
    Future<Boolean> future = initialized.get();
    try {
      return future != null && future.get();
    } catch ( Throwable e ) {
      return false;
    }
  }

  /**
   * Loads the plugin registry.
   *
   * @throws HopPluginException if any errors are encountered while loading the plugin registry.
   */
  public void loadPluginRegistry() throws HopPluginException {

  }

  /**
   * Sets the executor's user and Server information
   */
  public static void setExecutionInformation( ExecutorInterface executor, Repository repository ) {
    // Capture the executing user and server name...
    executor.setExecutingUser( System.getProperty( "user.name" ) );
    if ( repository != null ) {
      IUser userInfo = repository.getUserInfo();
      if ( userInfo != null ) {
        executor.setExecutingUser( userInfo.getLogin() );
      }
    }
  }

  public static void reset() {
    HopClientEnvironment.reset();
    initialized.set( null );
  }
}
