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

package org.apache.hop.ui.hopgui.search.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;

public class SearchConfigSingleton {

  private static SearchConfigSingleton configSingleton;

  private SearchConfig searchConfig;

  private SearchConfigSingleton() {
    Object configObject =
        HopConfig.getInstance().getConfigMap().get(SearchConfig.HOP_CONFIG_SEARCH_KEY);
    if (configObject == null) {
      searchConfig = new SearchConfig();
    } else {
      try {
        ObjectMapper mapper = HopJson.newMapper();
        searchConfig = mapper.readValue(new Gson().toJson(configObject), SearchConfig.class);
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            "Error reading search configuration, please check property '"
                + SearchConfig.HOP_CONFIG_SEARCH_KEY
                + "' in the Hop config json file",
            e);
        searchConfig = new SearchConfig();
      }
    }
    HopConfig.getInstance().getConfigMap().put(SearchConfig.HOP_CONFIG_SEARCH_KEY, searchConfig);
  }

  public static SearchConfig getConfig() {
    if (configSingleton == null) {
      configSingleton = new SearchConfigSingleton();
    }
    return configSingleton.searchConfig;
  }

  public static void saveConfig() throws HopException {
    if (configSingleton == null) {
      configSingleton = new SearchConfigSingleton();
    }
    HopConfig.getInstance()
        .saveOption(SearchConfig.HOP_CONFIG_SEARCH_KEY, configSingleton.searchConfig);
    HopConfig.getInstance().saveToFile();
  }

  /**
   * Replace the in-memory config without writing hop-config. Intended for unit tests; also used
   * carefully when applying settings that must take effect immediately.
   */
  public static void setConfigForTesting(SearchConfig config) {
    if (configSingleton == null) {
      // Avoid loading from disk when HopConfig is not initialized in unit tests.
      configSingleton = new SearchConfigSingleton(config == null ? new SearchConfig() : config);
      return;
    }
    configSingleton.searchConfig = config == null ? new SearchConfig() : config;
  }

  private SearchConfigSingleton(SearchConfig config) {
    this.searchConfig = config;
  }
}
