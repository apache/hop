package org.apache.hop.core.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class keeps track of storing and retrieving all the configuration options in Hop.
 * This includes all options of the various plugins in the Hop ecosystem.
 */
public class HopConfig {

  private static final String HOP_CONFIG_KEY = "systemProperties";

  @JsonIgnore
  private static HopConfig hopConfig;

  private Map<String, Object> configMap;

  @JsonIgnore
  private String filename;

  @JsonIgnore
  private IHopConfigSerializer serializer;

  private HopConfig() {
    try {
      this.filename = Const.HOP_CONFIG_DIRECTORY + Const.FILE_SEPARATOR + Const.HOP_CONFIG;
      this.serializer = new ConfigFileSerializer();
      Thread.sleep(5000);
      configMap = serializer.readFromFile( filename );
    } catch ( Exception e ) {
      throw new RuntimeException( "Error reading the hop config file '" + filename + "'", e );
    }
  }

  public static HopConfig getInstance() {
    if ( hopConfig == null ) {
      hopConfig = new HopConfig();
    }
    return hopConfig;
  }

  public synchronized static void saveOption( String optionKey, Object optionValue ) {
    try {
      HopConfig hopConfig = getInstance();
      hopConfig.configMap.put( optionKey, optionValue );
      synchronized ( hopConfig.serializer ) {
        hopConfig.serializer.writeToFile( hopConfig.filename, hopConfig.configMap );
      }
    } catch ( Exception e ) {
      throw new RuntimeException( "Error saving configuration option '" + optionKey + "'", e );
    }
  }

  public synchronized static void saveOptions( Map<String, Object> extraOptions ) {
    try {
      HopConfig hopConfig = getInstance();
      hopConfig.configMap.putAll( extraOptions );
      synchronized ( hopConfig.serializer ) {
        hopConfig.serializer.writeToFile( hopConfig.filename, hopConfig.configMap );
      }
    } catch ( Exception e ) {
      throw new RuntimeException( "Error saving configuration options", e );
    }
  }

  public static Object readOption( String optionKey ) {
    try {
      HopConfig hopConfig = getInstance();
      return hopConfig.configMap.get( optionKey );
    } catch ( Exception e ) {
      throw new RuntimeException( "Error reading option '" + optionKey + "'", e );
    }
  }

  public static String readOptionString( String optionKey, String defaultValue ) {
    try {
      Object value = readOption( optionKey );
      if ( value == null ) {
        return defaultValue;
      }
      if ( Utils.isEmpty( value.toString() ) ) {
        return defaultValue;
      }
      return value.toString();
    } catch ( Exception e ) {
      throw new RuntimeException( "Error reading option '" + optionKey + "'", e );
    }
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
    if ( value instanceof Boolean ) {
      return (Boolean) value;
    }
    if ( value instanceof Integer ) {
      return ( (Integer) value ) != 0;
    }
    String str = value.toString();
    return "true".equalsIgnoreCase( str ) || "y".equalsIgnoreCase( str );
  }

  /**
   * Gets configMap
   *
   * @return value of configMap
   */
  public static Map<String, Object> getConfigMap() {
    return getInstance().configMap;
  }

  public static List<String> getSortedKeys() {
    try {
      List<String> keys = new ArrayList<>( getInstance().configMap.keySet() );
      Collections.sort( keys );
      return keys;
    } catch ( Exception e ) {
      throw new RuntimeException( "Error getting a list of sorted configuration keys", e );
    }
  }

  public static Map<String, String> readSystemProperties() {
    try {
      Object propertiesObject = getInstance().configMap.get(HOP_CONFIG_KEY);
      if (propertiesObject==null) {
        Map<String, String> map = new HashMap<>();
        getInstance().configMap.put(HOP_CONFIG_KEY, map);
        return map;
      } else {
        return (Map<String, String>) propertiesObject;
      }
    } catch ( Exception e ) {
      throw new RuntimeException( "Error getting system properties from the Hop configuration" );
    }
  }

  public static void saveSystemProperty(String key, String value) {
    try {
      readSystemProperties().put(key, value);
      synchronized ( hopConfig.serializer ) {
        hopConfig.serializer.writeToFile( hopConfig.filename, hopConfig.configMap );
      }
    } catch ( HopException e ) {
      throw new RuntimeException("Error adding system property key '"+key+"' with value '"+value+"'", e);
    }
  }

  public static void saveSystemProperties( Map<String, String> map ) {
    try {
      readSystemProperties().putAll(map);
      synchronized ( hopConfig.serializer ) {
        hopConfig.serializer.writeToFile( hopConfig.filename, hopConfig.configMap );
      }
    } catch ( HopException e ) {
      throw new RuntimeException("Error adding system properties map with "+map.size()+" entries", e);
    }
  }
}
