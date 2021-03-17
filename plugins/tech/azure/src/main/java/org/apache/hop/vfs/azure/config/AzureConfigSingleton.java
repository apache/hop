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

package org.apache.hop.vfs.azure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;

public class AzureConfigSingleton {

  private static AzureConfigSingleton configSingleton;

  private AzureConfig azureConfig;

  private AzureConfigSingleton() {
    // Load from the HopConfig store
    //
    Object configObject = HopConfig.getInstance().getConfigMap().get( AzureConfig.HOP_CONFIG_AZURE_CONFIG_KEY );
    if ( configObject == null ) {
      azureConfig = new AzureConfig();
    } else {
      // The way Jackson stores these simple POJO is with a map per default...
      // So we don't really need to mess around with Deserializer and so on.
      // This way we can keep the class name out of the JSON as well.
      //
      try {
        ObjectMapper mapper = new ObjectMapper();
        azureConfig = mapper.readValue( new Gson().toJson( configObject ), AzureConfig.class );
      } catch ( Exception e ) {
        LogChannel.GENERAL.logError( "Error reading Azure configuration, check property '" + AzureConfig.HOP_CONFIG_AZURE_CONFIG_KEY + "' in the Hop config json file", e );
        azureConfig = new AzureConfig();
      }
    }
    HopConfig.getInstance().getConfigMap().put( AzureConfig.HOP_CONFIG_AZURE_CONFIG_KEY, azureConfig );
  }

  public static AzureConfigSingleton getInstance() {
    return configSingleton;
  }

  public static AzureConfig getConfig() {
    if ( configSingleton == null ) {
      configSingleton = new AzureConfigSingleton();
    }
    return configSingleton.azureConfig;
  }

  public static void saveConfig() throws HopException {
    HopConfig.getInstance().saveOption( AzureConfig.HOP_CONFIG_AZURE_CONFIG_KEY, configSingleton.azureConfig );
    HopConfig.getInstance().saveToFile();
  }
}

