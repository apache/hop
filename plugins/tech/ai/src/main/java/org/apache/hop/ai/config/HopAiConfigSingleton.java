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

package org.apache.hop.ai.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;

public class HopAiConfigSingleton {

  private static HopAiConfigSingleton configSingleton;
  private static Map<String, Object> legacySnapshot;

  private HopAiConfig config;

  private HopAiConfigSingleton() {
    Object configObject = HopConfig.getInstance().getConfigMap().get(HopAiConfig.HOP_CONFIG_KEY);
    Map<String, Object> raw = HopAiLegacyConfigMigrator.asStringObjectMap(configObject);
    if (HopAiLegacyConfigMigrator.hasLegacyKeys(raw)) {
      legacySnapshot = raw;
    }
    if (configObject == null) {
      config = new HopAiConfig();
      HopConfig.getInstance().getConfigMap().put(HopAiConfig.HOP_CONFIG_KEY, config);
      return;
    }
    try {
      ObjectMapper mapper = HopJson.newMapper();
      config = mapper.readValue(new Gson().toJson(configObject), HopAiConfig.class);
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Error reading AI configuration, check property '"
              + HopAiConfig.HOP_CONFIG_KEY
              + "' in the Hop config json file",
          e);
      config = new HopAiConfig();
    }
    // Keep the raw hopAiConfig map until migrate() writes an AiProvider so CLI
    // saveToFile() does not drop aiApiKey without a destination.
    if (legacySnapshot == null) {
      HopConfig.getInstance().getConfigMap().put(HopAiConfig.HOP_CONFIG_KEY, config);
    }
  }

  public static HopAiConfig getConfig() {
    if (configSingleton == null) {
      configSingleton = new HopAiConfigSingleton();
    }
    return configSingleton.config;
  }

  public static void saveConfig() throws HopException {
    if (legacySnapshot != null) {
      HopConfig.getInstance()
          .saveOption(HopAiConfig.HOP_CONFIG_KEY, mergedLegacyMap(legacySnapshot, getConfig()));
    } else {
      HopConfig.getInstance().saveOption(HopAiConfig.HOP_CONFIG_KEY, getConfig());
    }
    HopConfig.getInstance().saveToFile();
  }

  static Map<String, Object> peekLegacySnapshot() {
    return legacySnapshot;
  }

  static void clearLegacySnapshot() {
    legacySnapshot = null;
  }

  static Map<String, Object> mergedLegacyMap(Map<String, Object> legacy, HopAiConfig config) {
    Map<String, Object> raw = HopAiLegacyConfigMigrator.asStringObjectMap(legacy);
    if (raw == null) {
      raw = new LinkedHashMap<>();
    }
    if (config != null) {
      raw.put("aiEnabled", config.isAiEnabled());
      raw.put("defaultProviderName", config.getDefaultProviderName());
      raw.put("allowSendFullXml", config.isAllowSendFullXml());
      raw.put("extraContext", config.getExtraContext());
      raw.put("extraContextFiles", config.getExtraContextFiles());
    }
    return raw;
  }
}
