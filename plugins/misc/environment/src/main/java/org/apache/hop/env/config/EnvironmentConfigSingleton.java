package org.apache.hop.env.config;

import org.apache.hop.core.config.HopConfig;

import java.util.Map;

public class EnvironmentConfigSingleton {

  private static EnvironmentConfigSingleton configSingleton;

  private EnvironmentConfig config;

  private EnvironmentConfigSingleton() {
    // Load from the HopConfig store
    //
    Object configObject = HopConfig.getConfigMap().get( EnvironmentConfig.HOP_CONFIG_ENVIRONMENT_CONFIG );
    if ( configObject == null ) {
      config = new EnvironmentConfig();
      HopConfig.saveOption( EnvironmentConfig.HOP_CONFIG_ENVIRONMENT_CONFIG, config );
    } else {
      // The way Jackson stores these simple POJO is with a map per default...
      // So we don't really need to mess around with Deserializer and so on.
      // This way we can keep the class name out of the JSON as well.
      //
      Map<String, Object> map = (Map<String, Object>) configObject;
      config = new EnvironmentConfig();
      config.setEnabled( (Boolean) map.get("enabled") );
      config.setOpeningLastEnvironmentAtStartup( (Boolean) map.get("openingLastEnvironmentAtStartup") );
    }
  }

  public static EnvironmentConfig getConfig() {
    if (configSingleton==null) {
      configSingleton = new EnvironmentConfigSingleton();
    }
    return configSingleton.config;
  }

}
