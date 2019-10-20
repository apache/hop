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

package org.apache.hop.core.util;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.version.BuildVersion;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TimeZone;

public class EnvUtil {
  private static Properties env = null;

  /**
   * Returns the properties from the users kettle home directory.
   *
   * @param fileName
   *          the relative name of the properties file in the users kettle directory.
   * @return the map of properties.
   */
  public static Properties readProperties( final String fileName ) throws HopException {
    if ( !new File( fileName ).exists() ) {
      return readPropertiesByFullPath( Const.getHopDirectory() + Const.FILE_SEPARATOR + fileName );
    }
    return readPropertiesByFullPath( fileName );
  }

  private static Properties readPropertiesByFullPath( final String fileName ) throws HopException {
    Properties props = new Properties();
    InputStream is = null;
    try {
      is = new FileInputStream( fileName );
      props.load( is );
    } catch ( IOException ioe ) {
      throw new HopException( "Unable to read file '" + fileName + "'", ioe );
    } finally {
      if ( is != null ) {
        try {
          is.close();
        } catch ( IOException e ) {
          // ignore
        }
      }
    }
    return props;
  }

  /**
   * Adds the kettle properties the the global system properties.
   *
   * @throws HopException
   *           in case the properties file can't be read.
   */
  public static void environmentInit() throws HopException {
    // Workaround for a Mac OS/X Leopard issue where getContextClassLoader() is returning
    // null when run from the eclipse IDE
    // http://lists.apple.com/archives/java-dev/2007/Nov/msg00385.html - DM
    // Moving this hack to the first place where the NPE is triggered so all entrypoints can be debugged in Mac Eclipse
    if ( Thread.currentThread().getContextClassLoader() == null ) {
      Thread.currentThread().setContextClassLoader( ClassLoader.getSystemClassLoader() );
    }

    Map<Object, Object> kettleProperties = EnvUtil.readProperties( Const.HOP_PROPERTIES );
    insertDefaultValues( kettleProperties );
    applyHopProperties( kettleProperties );

    // Also put some default values for obscure environment variables in there...
    // Place-holders if you will.
    //
    System.getProperties().put( Const.INTERNAL_VARIABLE_CLUSTER_SIZE, "1" );
    System.getProperties().put( Const.INTERNAL_VARIABLE_SLAVE_SERVER_NUMBER, "0" );
    System.getProperties().put( Const.INTERNAL_VARIABLE_SLAVE_SERVER_NAME, "slave-trans-name" );

    System.getProperties().put( Const.INTERNAL_VARIABLE_STEP_COPYNR, "0" );
    System.getProperties().put( Const.INTERNAL_VARIABLE_STEP_NAME, "step-name" );
    System.getProperties().put( Const.INTERNAL_VARIABLE_STEP_PARTITION_ID, "partition-id" );
    System.getProperties().put( Const.INTERNAL_VARIABLE_STEP_PARTITION_NR, "0" );
    System.getProperties().put( Const.INTERNAL_VARIABLE_STEP_UNIQUE_COUNT, "1" );
    System.getProperties().put( Const.INTERNAL_VARIABLE_STEP_UNIQUE_NUMBER, "0" );
  }

  private static void insertDefaultValues( Map<Object, Object> kettleProperties ) {
    // If user didn't set value for USER_DIR_IS_ROOT set it to "false".
    // See PDI-14522, PDI-14821
    if ( !kettleProperties.containsKey( Const.VFS_USER_DIR_IS_ROOT ) ) {
      kettleProperties.put( Const.VFS_USER_DIR_IS_ROOT, "false" );
    }
  }

  public static void applyHopProperties( Map<?, ?> kettleProperties ) {
    applyHopProperties( kettleProperties, false );
  }

  public static void applyHopProperties( Map<?, ?> kettleProperties, boolean override ) {
    Variables variables = new Variables();
    for ( Object key : kettleProperties.keySet() ) {
      String variable = (String) key;
      String value = variables.environmentSubstitute( (String) kettleProperties.get( key ) );
      variables.setVariable( variable, value );
    }

    Properties systemProperties = System.getProperties();

    // Copy the data over to the system properties...
    //
    for ( String variable : variables.listVariables() ) {
      String value = variables.getVariable( variable );

      // Too many developers bump into the issue of the kettle.properties editor setting
      // an empty string in the kettle.properties file...
      //
      if ( variable.equals( Const.HOP_PLUGIN_CLASSES ) || variable.equals( Const.HOP_PLUGIN_PACKAGES ) ) {
        String jvmValue = System.getProperty( variable );
        if ( !Utils.isEmpty( jvmValue ) ) {
          if ( !Utils.isEmpty( value ) ) {
            value += "," + jvmValue;
          } else {
            value = jvmValue;
          }
        }
      } else {
        if ( !override && systemProperties.containsKey( variable ) ) {
          continue;
        }
      }
      System.setProperty( variable, value );
    }
  }

  /**
   * Add a number of internal variables to the Hop Variables at the root.
   *
   * @param prop
   */
  public static void addInternalVariables( Properties prop ) {
    // Add a bunch of internal variables

    // The Hop version
    prop.put( Const.INTERNAL_VARIABLE_HOP_VERSION, BuildVersion.getInstance().getVersion() );

    // The Hop build version
    prop.put( Const.INTERNAL_VARIABLE_HOP_BUILD_VERSION, BuildVersion.getInstance().getVersion() );

    // The Hop build date
    prop.put( Const.INTERNAL_VARIABLE_HOP_BUILD_DATE, BuildVersion.getInstance().getBuildDate() );
  }

  /**
   * Get System.getenv() in a reflection kind of way. The problem is that System.getenv() was deprecated in Java 1.4
   * while reinstated in 1.5 This method will get at getenv() using reflection and will return empty properties when
   * used in 1.4
   *
   * @return Properties containing the environment. You're not meant to change any value in the returned Properties!
   *
   */
  @SuppressWarnings( { "unchecked" } )
  private static final Properties getEnv() {
    Class<?> system = System.class;
    if ( env == null ) {
      Map<String, String> returnMap = null;
      try {
        Method method = system.getMethod( "getenv" );

        returnMap = (Map<String, String>) method.invoke( system );
      } catch ( Exception ex ) {
        returnMap = null;
      }

      env = new Properties();
      if ( returnMap != null ) {
        // We're on a VM with getenv() defined.
        ArrayList<String> list = new ArrayList<String>( returnMap.keySet() );
        for ( int i = 0; i < list.size(); i++ ) {
          String var = list.get( i );
          String val = returnMap.get( var );

          env.setProperty( var, val );
        }
      }
    }
    return env;
  }

  /**
   * @return an array of strings, made up of all the environment variables available in the VM, format var=value. To be
   *         used for Runtime.exec(cmd, envp)
   */
  public static final String[] getEnvironmentVariablesForRuntimeExec() {
    Properties sysprops = new Properties();
    sysprops.putAll( getEnv() );
    sysprops.putAll( System.getProperties() );
    addInternalVariables( sysprops );

    String[] envp = new String[sysprops.size()];
    List<Object> list = new ArrayList<Object>( sysprops.keySet() );
    for ( int i = 0; i < list.size(); i++ ) {
      String var = (String) list.get( i );
      String val = sysprops.getProperty( var );

      envp[i] = var + "=" + val;
    }

    return envp;
  }

  /**
   * This method is written especially for weird JVM's like IBM's on AIX and OS/400. On these platforms, we notice that
   * environment variables have an extra double quote around it... This is messing up the ability to specify things.
   *
   * @param key
   *          The key, the name of the environment variable to return
   * @param def
   *          The default value to return in case the key can't be found
   * @return The value of a System environment variable in the java virtual machine. If the key is not present, the
   *         variable is not defined and the default value is returned.
   */
  public static final String getSystemPropertyStripQuotes( String key, String def ) {
    String value = System.getProperty( key, def );
    if ( value.startsWith( "\"" ) && value.endsWith( "\"" ) && value.length() > 1 ) {
      return value.substring( 1, value.length() - 2 );
    }
    return value;
  }

  /**
   * This method is written especially for weird JVM's like
   *
   * @param key
   *          The key, the name of the environment variable to return
   * @param def
   *          The default value to return in case the key can't be found
   * @return The value of a System environment variable in the java virtual machine. If the key is not present, the
   *         variable is not defined and the default value is returned.
   */
  public static final String getSystemProperty( String key, String def ) {
    String value = System.getProperty( key, def );
    return value;
  }

  /**
   * @param key
   *          The key, the name of the environment variable to return
   * @return The value of a System environment variable in the java virtual machine. If the key is not present, the
   *         variable is not defined and null returned.
   */
  public static final String getSystemProperty( String key ) {
    return getSystemProperty( key, null );
  }

  /**
   * @param key
   *          The key, the name of the environment variable to clear
   * @return The value of a System environment variable in the java virtual machine. If the key is not present, the
   *         variable is not defined and null returned.
   */
  public static final String clearSystemProperty( String key ) {
    return System.clearProperty( key );
  }

  /**
   * Returns an available java.util.Locale object for the given localeCode.
   *
   * The localeCode code can be case insensitive, if it is available the method will find it and return it.
   *
   * @param localeCode
   * @return java.util.Locale.
   */
  public static Locale createLocale( String localeCode ) {
    if ( Utils.isEmpty( localeCode ) ) {
      return null;
    }
    StringTokenizer parser = new StringTokenizer( localeCode, "_" );
    if ( parser.countTokens() == 2 ) {
      return new Locale( parser.nextToken(), parser.nextToken() );
    }
    if ( parser.countTokens() == 3 ) {
      return new Locale( parser.nextToken(), parser.nextToken(), parser.nextToken() );
    }
    return new Locale( localeCode );
  }

  public static TimeZone createTimeZone( String timeZoneId ) {

    TimeZone resultTimeZone = null;
    if ( !Utils.isEmpty( timeZoneId ) ) {
      return TimeZone.getTimeZone( timeZoneId );
    } else {
      resultTimeZone = TimeZone.getDefault();
    }
    return resultTimeZone;
  }

  public static String[] getTimeZones() {
    String[] timeZones = TimeZone.getAvailableIDs();
    Arrays.sort( timeZones );
    return timeZones;
  }

  public static String[] getLocaleList() {
    Locale[] locales = Locale.getAvailableLocales();
    String[] strings = new String[locales.length];
    for ( int i = 0; i < strings.length; i++ ) {
      strings[i] = locales[i].toString();
    }
    Arrays.sort( strings );
    return strings;
  }
}
