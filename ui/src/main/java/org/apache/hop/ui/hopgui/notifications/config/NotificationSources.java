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
package org.apache.hop.ui.hopgui.notifications.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.JsonUtil;
import org.apache.hop.core.util.Utils;

/**
 * Reads the configured notification sources.
 *
 * <p>The service, the startup extension point and the settings tab all need this list, and each had
 * grown its own copy of the same fifteen lines. Reading it is also on the path that draws the
 * panel, where the alternative was building a whole settings tab object, {@link
 * org.apache.hop.ui.core.PropsUi} and all, twice for every notification on screen.
 */
public final class NotificationSources {

  public static final String CONFIG_KEY = "notification.sources";

  private NotificationSources() {
    // Utility class
  }

  /**
   * @return The configured sources, or an empty list when there are none or they cannot be read
   */
  public static List<NotificationSourceConfig> load() {
    try {
      String json = HopConfig.readOptionString(CONFIG_KEY, null);
      if (!Utils.isEmpty(json)) {
        ObjectMapper mapper = JsonUtil.jsonMapper();
        return mapper.readValue(json, new TypeReference<List<NotificationSourceConfig>>() {});
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error reading the notification sources from the configuration", e);
    }
    return new ArrayList<>();
  }
}
