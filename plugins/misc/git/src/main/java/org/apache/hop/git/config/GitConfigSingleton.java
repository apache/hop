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

package org.apache.hop.git.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;

public class GitConfigSingleton {

  private static GitConfigSingleton configSingleton;

  private GitConfig gitConfig;

  private GitConfigSingleton() {
    // Load from the HopConfig store
    //
    Object configObject =
        HopConfig.getInstance().getConfigMap().get(GitConfig.HOP_CONFIG_GIT_CONFIG_KEY);
    if (configObject == null) {
      gitConfig = new GitConfig();
    } else {
      // The way Jackson stores these simple POJO is with a map per default...
      // So we don't really need to mess around with Deserializer and so on.
      // This way we can keep the class name out of the JSON as well.
      //
      try {
        ObjectMapper mapper = HopJson.newMapper();
        gitConfig = mapper.readValue(new Gson().toJson(configObject), GitConfig.class);
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            "Error reading git configuration, check property '"
                + GitConfig.HOP_CONFIG_GIT_CONFIG_KEY
                + "' in the Hop config json file",
            e);
        gitConfig = new GitConfig();
      }
    }
    HopConfig.getInstance().getConfigMap().put(GitConfig.HOP_CONFIG_GIT_CONFIG_KEY, gitConfig);
  }

  public static GitConfigSingleton getInstance() {
    return configSingleton;
  }

  public static GitConfig getConfig() {
    if (configSingleton == null) {
      configSingleton = new GitConfigSingleton();
    }
    return configSingleton.gitConfig;
  }

  public static void saveConfig() throws HopException {
    HopConfig.getInstance()
        .saveOption(GitConfig.HOP_CONFIG_GIT_CONFIG_KEY, configSingleton.gitConfig);
    HopConfig.getInstance().saveToFile();
  }
}
