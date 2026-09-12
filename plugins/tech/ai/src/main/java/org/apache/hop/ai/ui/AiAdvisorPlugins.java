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

package org.apache.hop.ai.ui;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hop.ai.advisor.AiAdvisorPluginType;
import org.apache.hop.ai.advisor.IAiAdvisor;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;

public final class AiAdvisorPlugins {

  private AiAdvisorPlugins() {}

  public static List<IPlugin> list() {
    List<IPlugin> plugins =
        new ArrayList<>(PluginRegistry.getInstance().getPlugins(AiAdvisorPluginType.class));
    plugins.sort(Comparator.comparing(IPlugin::getName, String.CASE_INSENSITIVE_ORDER));
    return plugins;
  }

  public static IAiAdvisor load(String pluginId) throws HopException {
    if (Utils.isEmpty(pluginId)) {
      return null;
    }
    try {
      IAiAdvisor advisor =
          PluginRegistry.getInstance()
              .loadClass(AiAdvisorPluginType.class, pluginId, IAiAdvisor.class);
      return advisor;
    } catch (HopPluginException e) {
      throw new HopException("Unable to load AI advisor plugin " + pluginId, e);
    }
  }

  public static IPlugin find(String pluginId) {
    if (Utils.isEmpty(pluginId)) {
      return null;
    }
    return PluginRegistry.getInstance().findPluginWithId(AiAdvisorPluginType.class, pluginId);
  }
}
