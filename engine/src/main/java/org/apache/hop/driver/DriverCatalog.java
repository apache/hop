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
import java.util.stream.Stream;
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
  private final Map<String, KnownDatabase> databasesById = new LinkedHashMap<>();

  private DriverCatalog() {
    // use load()
  }

  /**
   * A database plugin as the driver commands see it. Every database type ends up here; only the
   * ones declaring a {@link DriverDownload} also become a {@link DriverDefinition}. It is what lets
   * "no download for this database" be told apart from "no such database type".
   *
   * @param id the command-line id: the database type lowercased, e.g. {@code databricks}
   * @param name the plugin's display name, e.g. {@code Databricks}
   * @param driverClass the JDBC driver class the plugin expects, null when it could not be asked
   * @param classLoader the plugin's classloader, used to see whether that driver class is there
   */
  public record KnownDatabase(String id, String name, String driverClass, ClassLoader classLoader) {

    /**
     * @return true when the JDBC driver class is already loadable, i.e. the driver ships with this
     *     Hop installation (bundled with the plugin, or installed earlier) and there is nothing
     *     left to download.
     */
    public boolean isDriverAvailable() {
      if (driverClass == null || driverClass.isBlank() || classLoader == null) {
        return false;
      }
      try {
        classLoader.loadClass(driverClass);
        return true;
      } catch (Exception | LinkageError e) {
        return false;
      }
    }
  }

  /** Build the catalog by scanning all registered database plugins for a driver download. */
  public static DriverCatalog load() {
    DriverCatalog catalog = new DriverCatalog();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (IPlugin plugin : registry.getPlugins(DatabasePluginType.class)) {
      String databaseType = plugin.getIds()[0];
      String id = databaseType.toLowerCase(Locale.ROOT);
      try {
        Object loaded = registry.loadClass(plugin);
        if (loaded instanceof IDatabase database) {
          String driverClass = driverClass(database);
          catalog.databasesById.put(
              id,
              new KnownDatabase(
                  id, plugin.getName(), driverClass, database.getClass().getClassLoader()));
          DriverDownload download = database.getDriverDownload();
          if (download != null) {
            DriverDefinition definition =
                new DriverDefinition(databaseType, plugin.getName(), driverClass, download);
            catalog.driversById.put(definition.getId(), definition);
          }
        }
      } catch (Exception e) {
        // The plugin failed to load, so it has no downloadable driver here - but the id is still a
        // database type Hop knows, which is worth saying instead of "unknown driver id".
        catalog.databasesById.putIfAbsent(id, new KnownDatabase(id, plugin.getName(), null, null));
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
   * @return the database plugin known under this id/database type (case-insensitive), whether or
   *     not it declares a driver download, or null when no such database type is installed.
   */
  public KnownDatabase getDatabaseType(String id) {
    return id == null ? null : databasesById.get(id.toLowerCase(Locale.ROOT));
  }

  /**
   * Ids close enough to an unknown one to be worth offering as a "did you mean", downloadable ones
   * first - a typo or an id that only differs in its suffix ({@code postgres} for {@code
   * postgresql}) is the common case.
   *
   * @param id the id the user typed
   * @return at most 5 candidate ids, closest first, empty when nothing looks similar
   */
  public List<String> suggestIds(String id) {
    if (id == null || id.isBlank()) {
      return List.of();
    }
    String needle = id.toLowerCase(Locale.ROOT);
    return Stream.concat(
            databasesById.keySet().stream().filter(driversById::containsKey),
            databasesById.keySet().stream().filter(known -> !driversById.containsKey(known)))
        .filter(known -> known.contains(needle) || needle.contains(known))
        .limit(5)
        .toList();
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
