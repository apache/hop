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

import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.gui.plugin.GuiElement;
import org.apache.hop.core.gui.plugin.GuiElements;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiPluginType;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.logging.ConsoleLoggingEventListener;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LoggingPluginInterface;
import org.apache.hop.core.logging.LoggingPluginType;
import org.apache.hop.core.logging.Slf4jLoggingEventListener;
import org.apache.hop.core.plugins.DatabasePluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.PluginTypeInterface;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.i18n.BaseMessages;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This singleton is responsible for initializing the Hop client environment and remembering if it is initialized.
 * More specifically it loads client plugins like value meta plugins and other core Hop functionality.
 *
 * @author matt
 */
public class HopClientEnvironment {
  /**
   * For i18n purposes, needed by Translator2!!
   */
  private static Class<?> PKG = Const.class;

  private static HopClientEnvironment instance = null;

  private static Boolean initialized;

  public enum ClientType {
    SPOON, PAN, KITCHEN, CARTE, DI_SERVER, SCALE, OTHER;

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
      GuiPluginType.getInstance()
      )
    );
  }

  public static synchronized void init( List<PluginTypeInterface> pluginsToLoad ) throws HopException {
    if ( initialized != null ) {
      return;
    }

    if ( HopClientEnvironment.instance == null ) {
      HopClientEnvironment.instance = new HopClientEnvironment();
    }

    createHopHome();

    // Read the kettle.properties file before anything else
    //
    EnvUtil.environmentInit();

    // Initialize the logging back-end.
    //
    HopLogStore.init();

    // Add console output so that folks see what's going on...
    // TODO: make this configurable...
    //
    if ( !"Y".equalsIgnoreCase( System.getProperty( Const.HOP_DISABLE_CONSOLE_LOGGING, "N" ) ) ) {
      HopLogStore.getAppender().addLoggingEventListener( new ConsoleLoggingEventListener() );
    }
    HopLogStore.getAppender().addLoggingEventListener( new Slf4jLoggingEventListener() );

    // Load plugins
    //
    pluginsToLoad.forEach( PluginRegistry::addPluginType );
    PluginRegistry.init();

    List<PluginInterface> logginPlugins = PluginRegistry.getInstance().getPlugins( LoggingPluginType.class );
    initLogginPlugins( logginPlugins );

    String passwordEncoderPluginID = Const.NVL( EnvUtil.getSystemProperty( Const.HOP_PASSWORD_ENCODER_PLUGIN ), "Hop" );

    Encr.init( passwordEncoderPluginID );

    initGuiPlugins();

    initialized = new Boolean( true );
  }

  /**
   * Look for GuiElement annotated fields in all the GuiPlugins.
   * Put them in the Gui registry
   * @throws HopException
   */
  public static void initGuiPlugins() throws HopException {

    try {
      GuiRegistry guiRegistry = GuiRegistry.getInstance();
      PluginRegistry pluginRegistry = PluginRegistry.getInstance();

      List<PluginInterface> guiPlugins = pluginRegistry.getPlugins( GuiPluginType.class );
      for ( PluginInterface guiPlugin : guiPlugins ) {
        ClassLoader classLoader = pluginRegistry.getClassLoader( guiPlugin );
        Class<?>[] typeClasses = guiPlugin.getClassMap().keySet().toArray( new Class<?>[ 0 ] );
        String mainClassname = guiPlugin.getClassMap().get( typeClasses[ 0 ] );
        Class<?> mainClass = classLoader.loadClass( mainClassname );
        List<Field> fields = findDeclaredFields(mainClass);

        for ( Field field : fields ) {
          GuiElement guiElement = field.getAnnotation( GuiElement.class );
          if ( guiElement != null ) {
            // Add the GUI Element to the registry...
            //
            guiRegistry.addGuiElement(mainClassname, guiElement, field.getName(), field.getType());
          }
        }
      }
      // Sort all GUI elements once.
      //
      guiRegistry.sortAllElements();

    } catch(Exception e) {
      throw new HopException( "Error looking for Elements in GUI Plugins ", e );
    }
  }

  /**
   * Get all declared fields from the given class, also the ones from all super classes
   *
   * @param parentClass
   * @return A unqiue list of fields.
   */
  private static final List<Field> findDeclaredFields(Class<?> parentClass) {
    Set<Field> fields = new HashSet<>(  );

    for (Field field : parentClass.getDeclaredFields()) {
      fields.add(field);
    }
    Class<?> superClass = parentClass.getSuperclass();
    while (superClass!=null) {
      for (Field field : superClass.getDeclaredFields()) {
        fields.add(field);
      }

      superClass = superClass.getSuperclass();
    }

    return new ArrayList<>( fields );
  }

  public static boolean isInitialized() {
    return initialized != null;
  }

  /**
   * Creates the kettle home area, which is a directory containing a default kettle.properties file
   */
  public static void createHopHome() {

    // Try to create the directory...
    //
    String directory = Const.getHopDirectory();
    File dir = new File( directory );
    try {
      dir.mkdirs();

      // Also create a file called kettle.properties
      //
      createDefaultHopProperties( directory );
    } catch ( Exception e ) {
      // ignore - should likely propagate the error

    }
  }

  private static void initLogginPlugins( List<PluginInterface> logginPlugins ) throws HopPluginException {
    for ( PluginInterface plugin : logginPlugins ) {
      LoggingPluginInterface loggingPlugin = (LoggingPluginInterface) PluginRegistry.getInstance().loadClass( plugin );
      loggingPlugin.init();
    }
  }

  /**
   * Creates the default kettle properties file, containing the standard header.
   *
   * @param directory the directory
   */
  private static void createDefaultHopProperties( String directory ) {

    String kpFile = directory + Const.FILE_SEPARATOR + Const.HOP_PROPERTIES;
    File file = new File( kpFile );
    if ( !file.exists() ) {
      FileOutputStream out = null;
      try {
        out = new FileOutputStream( file );
        out.write( Const.getHopPropertiesFileHeader().getBytes() );
      } catch ( IOException e ) {
        System.err
          .println( BaseMessages.getString(
            PKG, "Props.Log.Error.UnableToCreateDefaultHopProperties.Message", Const.HOP_PROPERTIES,
            kpFile ) );
        System.err.println( e.getStackTrace() );
      } finally {
        if ( out != null ) {
          try {
            out.close();
          } catch ( IOException e ) {
            System.err.println( BaseMessages.getString(
              PKG, "Props.Log.Error.UnableToCreateDefaultHopProperties.Message", Const.HOP_PROPERTIES,
              kpFile ) );
            System.err.println( e.getStackTrace() );
          }
        }
      }
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
