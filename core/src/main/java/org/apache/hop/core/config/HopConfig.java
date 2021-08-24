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

package org.apache.hop.core.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.plugin.ConfigFile;
import org.apache.hop.core.util.Utils;

import java.util.*;

/**
 * This class keeps track of storing and retrieving all the configuration options in Hop. This
 * includes all options of the various plugins in the Hop ecosystem.
 */
public class HopConfig extends ConfigFile implements IConfigFile {

  private static final String HOP_GUI_PROPERTIES_KEY = "guiProperties";

  private String configFilename;

  @JsonIgnore private static HopConfig instance;

  private HopConfig() {
    try {
      this.configFilename = Const.HOP_CONFIG_FOLDER + Const.FILE_SEPARATOR + Const.HOP_CONFIG;
      readFromFile();
    } catch (Exception e) {
      throw new RuntimeException("Error reading the hop config file '" + configFilename + "'", e);
    }
  }

  public static HopConfig getInstance() {
    if (instance == null) {
      instance = new HopConfig();
    }
    return instance;
  }

  public synchronized void saveOption(String optionKey, Object optionValue) {
    try {
      HopConfig hopConfig = getInstance();
      hopConfig.configMap.put(optionKey, optionValue);
      saveToFile();
    } catch (Exception e) {
      throw new RuntimeException("Error saving configuration option '" + optionKey + "'", e);
    }
  }

  public static synchronized void saveOptions(Map<String, Object> extraOptions) {
    try {
      HopConfig hopConfig = getInstance();
      hopConfig.configMap.putAll(extraOptions);
      hopConfig.saveToFile();
    } catch (Exception e) {
      throw new RuntimeException("Error saving configuration options", e);
    }
  }

  public static Object readOption(String optionKey) {
    try {
      HopConfig hopConfig = getInstance();
      return hopConfig.configMap.get(optionKey);
    } catch (Exception e) {
      throw new RuntimeException("Error reading option '" + optionKey + "'", e);
    }
  }

  public static String readOptionString(String optionKey, String defaultValue) {
    try {
      Object value = readOption(optionKey);
      if (value == null) {
        return defaultValue;
      }
      if (Utils.isEmpty(value.toString())) {
        return defaultValue;
      }
      return value.toString();
    } catch (Exception e) {
      throw new RuntimeException("Error reading option '" + optionKey + "'", e);
    }
  }

  public static int readOptionInteger(String optionKey, int defaultValue) {
    Object value = readOption(optionKey);
    if (value == null) {
      return defaultValue;
    }
    if (Utils.isEmpty(value.toString())) {
      return defaultValue;
    }
    return Integer.parseInt(value.toString());
  }

  public static boolean readOptionBoolean(String optionKey, boolean defaultValue) {
    Object value = readOption(optionKey);
    if (value == null) {
      return defaultValue;
    }
    if (Utils.isEmpty(value.toString())) {
      return defaultValue;
    }
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    if (value instanceof Integer) {
      return ((Integer) value) != 0;
    }
    String str = value.toString();
    return "true".equalsIgnoreCase(str) || "y".equalsIgnoreCase(str);
  }

  public static List<String> getSortedKeys() {
    try {
      List<String> keys = new ArrayList<>(getInstance().configMap.keySet());
      Collections.sort(keys);
      return keys;
    } catch (Exception e) {
      throw new RuntimeException("Error getting a list of sorted configuration keys", e);
    }
  }

  public static Map<String, String> readGuiProperties() {
    try {
      Object propertiesObject = getInstance().configMap.get(HOP_GUI_PROPERTIES_KEY);
      if (propertiesObject == null) {
        Map<String, String> map = new HashMap<>();
        getInstance().configMap.put(HOP_GUI_PROPERTIES_KEY, map);
        return map;
      } else {
        return (Map<String, String>) propertiesObject;
      }
    } catch (Exception e) {
      throw new RuntimeException("Error getting GUI properties from the Hop configuration");
    }
  }

  /**
   * This method returns the value of a Hop named string variable.
   *
   * @param key - variable name
   * @param defaultValue - default value
   * @return the value associated to the variable
   */
  public static String readStringVariable(String key, String defaultValue) {
    String value = null;

    ArrayList<DescribedVariable> variables =
        (ArrayList<DescribedVariable>) getInstance().configMap.get(HOP_VARIABLES_KEY);
    if (variables != null) {
      Iterator<DescribedVariable> i = variables.iterator();

      while (i.hasNext() && value == null) {
        DescribedVariable v = i.next();
        if (v.getName().equals(key)) value = v.getValue();
      }
    }

    return value == null ? defaultValue : value;
  }

  public static void setGuiProperty(String key, String value) {
    readGuiProperties().put(key, value);
  }

  public static String getGuiProperty(String key) {
    return readGuiProperties().get(key);
  }

  public static void setGuiProperties(Map<String, String> map) {
    readGuiProperties().putAll(map);
  }

  /**
   * Gets configFilename
   *
   * @return value of configFilename
   */
  @Override
  public String getConfigFilename() {
    return configFilename;
  }

  /** @param configFilename The configFilename to set */
  public void setConfigFilename(String configFilename) {
    this.configFilename = configFilename;
  }
}
