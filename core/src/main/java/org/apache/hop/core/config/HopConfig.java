package org.apache.hop.core.config;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;

import java.util.Map;

/**
 * This class keeps track of storing and retrieving all the configuration options in Hop.
 * This includes all options of the various plugins in the Hop ecosystem.
 */
public class HopConfig {
  private static HopConfig hopConfig;

  private Map<String, Object> configMap;

  private String filename;
  private IHopConfigSerializer serializer;

  private HopConfig( String filename, IHopConfigSerializer serializer ) throws HopException {
    this.filename = filename;
    this.serializer = serializer;

    configMap = serializer.readFromFile( filename );
  }

  public static HopConfig getInstance() {
    if ( hopConfig == null ) {
      throw new RuntimeException( "The Hop configuration system needs to be initialized, call HopConfig.initialize(<filename>)" );
    }
    return hopConfig;
  }

  /**
   * Provide the name of the Hop configuration file
   *
   * @param filename The filename of the Hop config file
   * @throws HopException in case something goes wrong, the file doesn't exist and so on.
   */
  public static void initialize( String filename, IHopConfigSerializer serializer ) throws HopException {
    if ( hopConfig != null ) {
      throw new RuntimeException( "The hop configuration system can only be initialized once!" );
    }
    hopConfig = new HopConfig( filename, serializer );
  }

  public static void saveOption( String optionKey, Object optionValue ) throws HopException {
    HopConfig hopConfig = getInstance();
    hopConfig.configMap.put( optionKey, optionValue );
    hopConfig.serializer.writeToFile( hopConfig.filename, hopConfig.configMap );
  }

  public static void saveOptions( Map<String, Object> extraOptions ) throws HopException {
    HopConfig hopConfig = getInstance();
    hopConfig.configMap.putAll( extraOptions );
    hopConfig.serializer.writeToFile( hopConfig.filename, hopConfig.configMap );
  }

  public static Object readOption( String optionKey ) {
    HopConfig hopConfig = getInstance();
    return hopConfig.configMap.get( optionKey );
  }

  public static String readOptionString( String optionKey, String defaultValue ) {
    Object value = readOption( optionKey );
    if ( value == null ) {
      return defaultValue;
    }
    if ( Utils.isEmpty( value.toString() ) ) {
      return defaultValue;
    }
    return value.toString();
  }

  public static int readOptionInteger( String optionKey, int defaultValue ) {
    Object value = readOption( optionKey );
    if ( value == null ) {
      return defaultValue;
    }
    if ( Utils.isEmpty( value.toString() ) ) {
      return defaultValue;
    }
    return Integer.valueOf( value.toString() ).intValue();
  }

  public static boolean readOptionBoolean( String optionKey, boolean defaultValue ) {
    Object value = readOption( optionKey );
    if ( value == null ) {
      return defaultValue;
    }
    if ( Utils.isEmpty( value.toString() ) ) {
      return defaultValue;
    }
    if (value instanceof Boolean) {
      return (Boolean)value;
    }
    if (value instanceof Integer) {
      return ((Integer)value)!=0;
    }
    String str = value.toString();
    return "true".equalsIgnoreCase( str ) || "y".equalsIgnoreCase( str );
  }
}
