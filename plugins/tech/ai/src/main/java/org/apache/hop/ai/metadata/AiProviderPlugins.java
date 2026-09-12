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

package org.apache.hop.ai.metadata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hop.ai.provider.AiProviderPluginType;
import org.apache.hop.ai.provider.IAiProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;

/** Lookup {@link IAiProvider} plugins by id or display name. */
public final class AiProviderPlugins {

  private AiProviderPlugins() {}

  public static List<IPlugin> list() {
    List<IPlugin> plugins =
        new ArrayList<>(PluginRegistry.getInstance().getPlugins(AiProviderPluginType.class));
    plugins.sort(Comparator.comparing(IPlugin::getName, String.CASE_INSENSITIVE_ORDER));
    return plugins;
  }

  public static String[] names() {
    List<IPlugin> plugins = list();
    String[] names = new String[plugins.size()];
    for (int i = 0; i < plugins.size(); i++) {
      names[i] = plugins.get(i).getName();
    }
    return names;
  }

  public static IAiProvider load(String pluginNameOrId) throws HopException {
    if (Utils.isEmpty(pluginNameOrId)) {
      throw new HopException("AI provider type is empty");
    }
    IPlugin plugin = find(pluginNameOrId);
    if (plugin == null) {
      throw new HopException("AI provider plugin not found: " + pluginNameOrId);
    }
    try {
      IAiProvider provider = PluginRegistry.getInstance().loadClass(plugin, IAiProvider.class);
      provider.setPluginId(plugin.getIds()[0]);
      provider.setPluginName(plugin.getName());
      return provider;
    } catch (HopPluginException e) {
      throw new HopException("Unable to load AI provider plugin " + pluginNameOrId, e);
    }
  }

  public static IPlugin find(String pluginNameOrId) {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin byId = registry.findPluginWithId(AiProviderPluginType.class, pluginNameOrId);
    if (byId != null) {
      return byId;
    }
    for (IPlugin plugin : registry.getPlugins(AiProviderPluginType.class)) {
      if (pluginNameOrId.equalsIgnoreCase(plugin.getName())) {
        return plugin;
      }
    }
    return null;
  }
}
