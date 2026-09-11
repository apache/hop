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
package org.apache.hop.pipeline.transform;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Checked-in inventory of transform plugin IDs that do not consume main input at default settings.
 *
 * <p>Plugins that are not on the test classpath are skipped. A loaded plugin that returns {@code
 * consumesMainInput() == false} after {@code setDefault()} must be listed, or it is an unmarked
 * exception.
 */
class TransformInputConsumptionInventoryTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  static final String INVENTORY_RESOURCE = "never-consume-at-default-plugins.txt";

  @Test
  void inventoryIsNonEmpty() throws Exception {
    Set<String> inventory = loadInventory();
    assertFalse(inventory.isEmpty(), "never-consume inventory must list known exceptions");
  }

  @Test
  void registeredPluginsMatchInventory() throws Exception {
    Set<String> inventory = loadInventory();
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = registry.getPlugins(TransformPluginType.class);
    assertNotNull(plugins);

    for (IPlugin plugin : plugins) {
      ITransformMeta meta;
      try {
        meta = registry.loadClass(plugin, ITransformMeta.class);
      } catch (Exception e) {
        continue;
      }
      if (meta == null) {
        continue;
      }
      try {
        meta.setDefault();
      } catch (Exception e) {
        if (isListed(plugin, inventory)) {
          fail(
              "Listed never-consume plugin "
                  + ids(plugin)
                  + " failed setDefault(): "
                  + e.getMessage());
        }
        continue;
      }

      boolean neverConsumes = !meta.consumesMainInput();
      boolean listed = isListed(plugin, inventory);
      if (neverConsumes && !listed) {
        fail(
            "Transform plugin "
                + ids(plugin)
                + " does not consume main input at default settings but is not in "
                + INVENTORY_RESOURCE);
      }
      if (listed && !neverConsumes) {
        fail(
            "Transform plugin "
                + ids(plugin)
                + " is in "
                + INVENTORY_RESOURCE
                + " but consumesMainInput() is true after setDefault()");
      }
      if (listed && !meta.canStartWithoutInput()) {
        fail(
            "Transform plugin "
                + ids(plugin)
                + " is in "
                + INVENTORY_RESOURCE
                + " but canStartWithoutInput() is false after setDefault()");
      }
    }
  }

  private static boolean isListed(IPlugin plugin, Set<String> inventory) {
    String[] ids = plugin.getIds();
    if (ids == null) {
      return false;
    }
    for (String id : ids) {
      if (inventory.contains(id)) {
        return true;
      }
    }
    return false;
  }

  private static String ids(IPlugin plugin) {
    String[] ids = plugin.getIds();
    if (ids == null || ids.length == 0) {
      return plugin.getName();
    }
    return String.join(",", ids);
  }

  static Set<String> loadInventory() throws Exception {
    InputStream in =
        TransformInputConsumptionInventoryTest.class.getResourceAsStream(INVENTORY_RESOURCE);
    assertNotNull(in, "missing classpath resource " + INVENTORY_RESOURCE);
    Set<String> ids = new HashSet<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        ids.add(line);
      }
    }
    return ids;
  }
}
