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

package org.apache.hop.ui.hopgui.perspective.database.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;

public class DatabasePerspectiveConfigSingleton {

  private static DatabasePerspectiveConfigSingleton configSingleton;

  private DatabasePerspectiveConfig config;

  private DatabasePerspectiveConfigSingleton() {
    Object configObject =
        HopConfig.getInstance().getConfigMap().get(DatabasePerspectiveConfig.HOP_CONFIG_KEY);
    if (configObject == null) {
      config = new DatabasePerspectiveConfig();
    } else {
      try {
        ObjectMapper mapper = HopJson.newMapper();
        config = mapper.readValue(new Gson().toJson(configObject), DatabasePerspectiveConfig.class);
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            "Error reading Database perspective configuration, check property '"
                + DatabasePerspectiveConfig.HOP_CONFIG_KEY
                + "' in the Hop config json file",
            e);
        config = new DatabasePerspectiveConfig();
      }
    }
    HopConfig.getInstance().getConfigMap().put(DatabasePerspectiveConfig.HOP_CONFIG_KEY, config);
  }

  public static DatabasePerspectiveConfig getConfig() {
    if (configSingleton == null) {
      configSingleton = new DatabasePerspectiveConfigSingleton();
    }
    return configSingleton.config;
  }

  public static void saveConfig() throws HopException {
    HopConfig.getInstance()
        .saveOption(DatabasePerspectiveConfig.HOP_CONFIG_KEY, configSingleton.config);
    HopConfig.getInstance().saveToFile();
  }
}
