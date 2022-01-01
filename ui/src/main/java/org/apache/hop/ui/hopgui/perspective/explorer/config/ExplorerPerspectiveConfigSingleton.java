/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.perspective.explorer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;

public class ExplorerPerspectiveConfigSingleton {

  private static ExplorerPerspectiveConfigSingleton configSingleton;

  private ExplorerPerspectiveConfig explorerPerspectiveConfig;

  private ExplorerPerspectiveConfigSingleton() {
    // Load from the HopConfig store
    //
    Object configObject =
        HopConfig.getInstance()
            .getConfigMap()
            .get(ExplorerPerspectiveConfig.HOP_CONFIG_EXPLORER_PERSPECTIVE_CONFIG_KEY);
    if (configObject == null) {
      explorerPerspectiveConfig = new ExplorerPerspectiveConfig();
    } else {
      // The way Jackson stores these simple POJO is with a map per default...
      // So we don't really need to mess around with Deserializer and so on.
      // This way we can keep the class name out of the JSON as well.
      //
      try {
        ObjectMapper mapper = new ObjectMapper();
        explorerPerspectiveConfig =
            mapper.readValue(new Gson().toJson(configObject), ExplorerPerspectiveConfig.class);
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            "Error reading explorer perspective configuration, please check property '"
                + ExplorerPerspectiveConfig.HOP_CONFIG_EXPLORER_PERSPECTIVE_CONFIG_KEY
                + "' in the Hop config json file",
            e);
        explorerPerspectiveConfig = new ExplorerPerspectiveConfig();
      }
    }
    HopConfig.getInstance()
        .getConfigMap()
        .put(
            ExplorerPerspectiveConfig.HOP_CONFIG_EXPLORER_PERSPECTIVE_CONFIG_KEY,
            explorerPerspectiveConfig);
  }

  public static ExplorerPerspectiveConfigSingleton getInstance() {
    return configSingleton;
  }

  public static ExplorerPerspectiveConfig getConfig() {
    if (configSingleton == null) {
      configSingleton = new ExplorerPerspectiveConfigSingleton();
    }
    return configSingleton.explorerPerspectiveConfig;
  }

  public static void saveConfig() throws HopException {
    HopConfig.getInstance()
        .saveOption(
            ExplorerPerspectiveConfig.HOP_CONFIG_EXPLORER_PERSPECTIVE_CONFIG_KEY,
            configSingleton.explorerPerspectiveConfig);
    HopConfig.getInstance().saveToFile();
  }
}
