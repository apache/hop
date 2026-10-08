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
package org.apache.hop.ai.engine;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;

/**
 * The installed transform or action plugins, as compact text for a prompt.
 *
 * <p>One line per category with {@code id (name)} pairs. Every plugin is listed, so a proposal can
 * use any installed plugin id, at roughly half the tokens of one JSON object per plugin.
 */
public final class AiPluginCatalog {

  /** A safety net for very large plugin sets; Hop's own plugins take far less. */
  static final int MAX_CHARS = 40_000;

  private AiPluginCatalog() {}

  public static String compact(Class<? extends IPluginType> pluginType) {
    return compact(PluginRegistry.getInstance().getPlugins(pluginType));
  }

  static String compact(List<IPlugin> plugins) {
    Map<String, List<IPlugin>> byCategory = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (IPlugin plugin : plugins) {
      if (plugin.getIds() == null || plugin.getIds().length == 0) {
        continue;
      }
      String category = Utils.isEmpty(plugin.getCategory()) ? "Other" : plugin.getCategory();
      byCategory.computeIfAbsent(category, key -> new ArrayList<>()).add(plugin);
    }
    StringBuilder text = new StringBuilder();
    for (Map.Entry<String, List<IPlugin>> entry : byCategory.entrySet()) {
      List<IPlugin> inCategory = entry.getValue();
      inCategory.sort(
          Comparator.comparing(
              IPlugin::getName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)));
      StringBuilder line = new StringBuilder(entry.getKey()).append(": ");
      for (int i = 0; i < inCategory.size(); i++) {
        IPlugin plugin = inCategory.get(i);
        if (i > 0) {
          line.append("; ");
        }
        line.append(plugin.getIds()[0]);
        if (!Utils.isEmpty(plugin.getName()) && !plugin.getName().equals(plugin.getIds()[0])) {
          line.append(" (").append(plugin.getName()).append(')');
        }
      }
      if (text.length() + line.length() > MAX_CHARS) {
        text.append("... (more plugins are installed than fit here)\n");
        break;
      }
      text.append(line).append('\n');
    }
    return text.toString();
  }
}
