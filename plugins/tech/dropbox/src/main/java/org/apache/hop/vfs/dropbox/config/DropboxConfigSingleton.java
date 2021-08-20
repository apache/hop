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
 *
 */

package org.apache.hop.vfs.dropbox.config;

import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

public class DropboxConfigSingleton {

  private static DropboxConfigSingleton configSingleton;

  private DropboxConfig config;

  private DropboxConfigSingleton() {
    // Load from the HopConfig store
    //
    Object configObject =
        HopConfig.getInstance().getConfigMap().get(DropboxConfig.HOP_CONFIG_DROPBOX_CONFIG_KEY);
    if (configObject == null) {
      config = new DropboxConfig();
    } else {

      try {
        ObjectMapper mapper = new ObjectMapper();
        config = mapper.readValue(new Gson().toJson(configObject), DropboxConfig.class);
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            "Error reading Dropbox configuration, check property '"
                + DropboxConfig.HOP_CONFIG_DROPBOX_CONFIG_KEY
                + "' in the Hop config json file",
            e);
        config = new DropboxConfig();
      }
    }
    HopConfig.getInstance()
        .getConfigMap()
        .put(DropboxConfig.HOP_CONFIG_DROPBOX_CONFIG_KEY, config);
  }

  public static DropboxConfigSingleton getInstance() {
    return configSingleton;
  }

  public static DropboxConfig getConfig() {
    if (configSingleton == null) {
      configSingleton = new DropboxConfigSingleton();
    }
    return configSingleton.config;
  }

  public static void saveConfig() throws HopException {
    HopConfig.getInstance()
        .saveOption(DropboxConfig.HOP_CONFIG_DROPBOX_CONFIG_KEY, configSingleton.config);
    HopConfig.getInstance().saveToFile();
  }
}
