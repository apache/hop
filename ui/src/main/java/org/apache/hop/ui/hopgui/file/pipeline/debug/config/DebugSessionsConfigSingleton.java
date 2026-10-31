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

package org.apache.hop.ui.hopgui.file.pipeline.debug.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;

public class DebugSessionsConfigSingleton {

  private static DebugSessionsConfigSingleton singleton;
  private DebugSessionsConfig config;

  private DebugSessionsConfigSingleton() {
    Object stored = HopConfig.getInstance().getConfigMap().get(DebugSessionsConfig.HOP_CONFIG_KEY);
    if (stored == null) {
      config = new DebugSessionsConfig();
    } else {
      try {
        ObjectMapper mapper = HopJson.newMapper();
        config = mapper.readValue(new Gson().toJson(stored), DebugSessionsConfig.class);
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            "Error reading the debug sessions folder from the Hop config", e);
        config = new DebugSessionsConfig();
      }
    }
    if (config == null) {
      config = new DebugSessionsConfig();
    }
    HopConfig.getInstance().getConfigMap().put(DebugSessionsConfig.HOP_CONFIG_KEY, config);
  }

  public static DebugSessionsConfig getConfig() {
    if (singleton == null) {
      singleton = new DebugSessionsConfigSingleton();
    }
    return singleton.config;
  }

  public static void saveConfig() throws HopException {
    if (singleton == null) {
      singleton = new DebugSessionsConfigSingleton();
    }
    HopConfig.getInstance().saveOption(DebugSessionsConfig.HOP_CONFIG_KEY, singleton.config);
    HopConfig.getInstance().saveToFile();
  }
}
