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
 */

package org.apache.hop.driver;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.DriverDownload;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;

/**
 * The catalog of downloadable JDBC drivers. There is no separate catalog file: this aggregates the
 * {@link DriverDownload} descriptors that the database plugins declare from {@link
 * IDatabase#getDriverDownload()}, so the driver definition lives next to the database metadata and
 * external plugins automatically contribute their own.
 */
public class DriverCatalog {

  private final Map<String, DriverDefinition> driversById = new LinkedHashMap<>();

  private DriverCatalog() {
    // use load()
  }

  /** Build the catalog by scanning all registered database plugins for a driver download. */
  public static DriverCatalog load() {
    DriverCatalog catalog = new DriverCatalog();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (IPlugin plugin : registry.getPlugins(DatabasePluginType.class)) {
      try {
        Object loaded = registry.loadClass(plugin);
        if (loaded instanceof IDatabase database) {
          DriverDownload download = database.getDriverDownload();
          if (download != null) {
            DriverDefinition definition =
                new DriverDefinition(
                    plugin.getIds()[0], plugin.getName(), driverClass(database), download);
            catalog.driversById.put(definition.getId(), definition);
          }
        }
      } catch (Exception e) {
        // Skip any plugin that fails to load; it simply has no downloadable driver here.
      }
    }
    return catalog;
  }

  private static String driverClass(IDatabase database) {
    try {
      return database.getDriverClass();
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * @return all downloadable drivers, in plugin-registration order.
   */
  public List<DriverDefinition> list() {
    return new ArrayList<>(driversById.values());
  }

  /**
   * @return the entry for the given id/database type (case-insensitive), or null if not found.
   */
  public DriverDefinition get(String id) {
    return id == null ? null : driversById.get(id.toLowerCase(Locale.ROOT));
  }

  /**
   * Find a catalog entry by Hop database plugin type, e.g. {@code ORACLE} (matches {@code
   * DatabaseMeta.getPluginId()}). The id and the database type share the same key space.
   */
  public DriverDefinition getByDatabaseType(String databaseType) {
    return get(databaseType);
  }

  /**
   * Best-effort check whether a jar for this driver is already present in the given lib/jdbc
   * folder. Heuristic: a file name in the folder starts with the driver's Maven artifactId.
   */
  public static boolean isInstalled(DriverDefinition driver, File libJdbcFolder) {
    String artifactId = driver.getDownload() == null ? null : driver.getDownload().getArtifactId();
    if (artifactId == null || libJdbcFolder == null || !libJdbcFolder.isDirectory()) {
      return false;
    }
    File[] jars = libJdbcFolder.listFiles((dir, fileName) -> fileName.endsWith(".jar"));
    if (jars == null) {
      return false;
    }
    for (File jar : jars) {
      if (jar.getName().startsWith(artifactId)) {
        return true;
      }
    }
    return false;
  }
}
