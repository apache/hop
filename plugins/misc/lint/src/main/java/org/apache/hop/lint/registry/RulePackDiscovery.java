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
package org.apache.hop.lint.registry;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import org.apache.hop.core.logging.LogChannel;

/**
 * Discovers rule packs from built-ins, classpath, ServiceLoader, sibling plugin folders, and
 * $HOP_HOME/plugins.
 *
 * <p>Third-party (vendor) rule packs live in their own folder under {@code plugins/misc/}; they are
 * not Hop transform/action plugins, but their YAML is discovered by scanning misc plugin
 * directories. Such a pack declares its own identity (id, owner, priority) in the {@code pack:}
 * block of its YAML file.
 */
public class RulePackDiscovery {

  private static final String[] PLUGIN_YAML_NAMES = {"hop-lint-core.yml", "hop-lint-pack.yml"};

  public List<IHopLintRulePack> discoverAll() {
    Map<String, IHopLintRulePack> packsById = new LinkedHashMap<>();

    registerBuiltInPacks(packsById);
    registerServiceLoaderPacks(packsById);
    registerInstalledPluginYamlPacks(packsById);
    registerHopHomePluginPacks(packsById);

    List<IHopLintRulePack> packs = new ArrayList<>(packsById.values());
    packs.sort(Comparator.comparingInt(IHopLintRulePack::getPriority));

    if (packs.isEmpty()) {
      LogChannel.GENERAL.logError("No lint rule packs discovered");
    } else {
      LogChannel.GENERAL.logBasic("Discovered " + packs.size() + " lint rule pack(s)");
    }
    return packs;
  }

  private void registerBuiltInPacks(Map<String, IHopLintRulePack> packsById) {
    packsById.putIfAbsent(RulePackIds.HOP_CORE, new HopCoreRulePack());
  }

  private void registerServiceLoaderPacks(Map<String, IHopLintRulePack> packsById) {
    for (ClassLoader loader : classLoadersToTry()) {
      registerServiceLoaderPacks(packsById, loader);
    }
  }

  /**
   * Collect packs advertised through {@code META-INF/services}, skipping any that cannot be
   * instantiated rather than letting one bad provider end discovery.
   *
   * <p>Two situations make this necessary. A third-party pack built against a different version of
   * the interface throws {@link ServiceConfigurationError}, and there is no reason for that to stop
   * every other pack from loading. The same error also appears in a healthy installation: the
   * engine jar sits on the JVM classpath so its main class can start, while Hop's plugin registry
   * loads the very same jar through its own classloader, and the two copies of the interface are
   * not assignable to each other. Those duplicates are already registered as built-ins, so skipping
   * them costs nothing.
   */
  private void registerServiceLoaderPacks(
      Map<String, IHopLintRulePack> packsById, ClassLoader loader) {
    Iterator<IHopLintRulePack> providers =
        ServiceLoader.load(IHopLintRulePack.class, loader).iterator();
    while (true) {
      IHopLintRulePack pack;
      try {
        if (!providers.hasNext()) {
          break;
        }
        pack = providers.next();
      } catch (ServiceConfigurationError e) {
        LogChannel.GENERAL.logDetailed(
            "Skipping an unusable lint rule pack provider: " + e.getMessage());
        continue;
      }
      packsById.putIfAbsent(pack.getPackId(), pack);
    }
  }

  /**
   * Load pack YAML from the engine plugin folder and sibling folders under {@code plugins/misc/}.
   * On-disk YAML wins over built-in classpath registrars (Hop GUI often cannot read resources from
   * the plugin jar).
   */
  private void registerInstalledPluginYamlPacks(Map<String, IHopLintRulePack> packsById) {
    File engineDir = PluginDirectoryResolver.locateEnginePluginDirectory(HopCoreRulePack.class);
    if (engineDir != null) {
      registerInstalledPluginYamlPacksFromEngineDir(engineDir, packsById);
    }
  }

  void registerInstalledPluginYamlPacksFromEngineDir(
      File engineDir, Map<String, IHopLintRulePack> packsById) {
    registerYamlPacksInFolder(engineDir, packsById, true);
    registerSiblingMiscPluginFolders(engineDir, packsById);
  }

  private void registerSiblingMiscPluginFolders(
      File engineDir, Map<String, IHopLintRulePack> packsById) {
    File miscDir = engineDir.getParentFile();
    if (miscDir == null || !miscDir.isDirectory()) {
      return;
    }
    File[] children = miscDir.listFiles(File::isDirectory);
    if (children == null) {
      return;
    }
    for (File sibling : children) {
      if (!sibling.equals(engineDir)) {
        registerYamlPacksInFolder(sibling, packsById, true);
      }
    }
  }

  private void registerYamlPacksInFolder(
      File folder, Map<String, IHopLintRulePack> packsById, boolean overrideExisting) {
    for (String yamlName : PLUGIN_YAML_NAMES) {
      File yamlFile = new File(folder, yamlName);
      if (!yamlFile.isFile()) {
        continue;
      }
      IHopLintRulePack pack = toFilePack(yamlFile);
      if (overrideExisting) {
        packsById.put(pack.getPackId(), pack);
      } else {
        packsById.putIfAbsent(pack.getPackId(), pack);
      }
      LogChannel.GENERAL.logDetailed(
          "Registered rule pack " + pack.getPackId() + " from " + yamlFile.getAbsolutePath());
    }
  }

  private void registerHopHomePluginPacks(Map<String, IHopLintRulePack> packsById) {
    File pluginsDir = PluginDirectoryResolver.resolvePluginsRoot();
    if (pluginsDir == null || !pluginsDir.isDirectory()) {
      return;
    }
    List<IHopLintRulePack> discovered = new ArrayList<>();
    discoverYamlPacksInDirectory(pluginsDir, discovered);
    discoverJarPacksInDirectory(pluginsDir, discovered);
    for (IHopLintRulePack pack : discovered) {
      packsById.putIfAbsent(pack.getPackId(), pack);
    }
  }

  private ClassLoader[] classLoadersToTry() {
    ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader packLoader = HopCoreRulePack.class.getClassLoader();
    ClassLoader discoveryLoader = RulePackDiscovery.class.getClassLoader();
    return new ClassLoader[] {packLoader, discoveryLoader, contextLoader};
  }

  private void discoverYamlPacksInDirectory(File pluginsDir, List<IHopLintRulePack> packs) {
    for (String yamlName : PLUGIN_YAML_NAMES) {
      collectYamlFiles(pluginsDir, yamlName, packs);
    }
  }

  private void collectYamlFiles(File directory, String yamlName, List<IHopLintRulePack> packs) {
    File[] children = directory.listFiles();
    if (children == null) {
      return;
    }
    for (File child : children) {
      if (child.isDirectory()) {
        File yamlFile = new File(child, yamlName);
        if (yamlFile.isFile()) {
          packs.add(toFilePack(yamlFile));
        }
        collectYamlFiles(child, yamlName, packs);
      }
    }
  }

  private IHopLintRulePack toFilePack(File yamlFile) {
    String fileName = yamlFile.getName();
    if ("hop-lint-core.yml".equals(fileName)) {
      return new FileYamlRulePack(
          yamlFile, RulePackIds.HOP_CORE, "Apache Hop Core Lint Rules", RulePackOwner.APACHE, 100);
    }
    // Any other discovered pack declares its own id/owner/priority in its YAML pack: block.
    // Defaults: pack id from the containing folder name, vendor-owned, priority 300.
    String defaultPackId =
        yamlFile.getParentFile() != null ? yamlFile.getParentFile().getName() : "plugin-pack";
    try {
      YamlRulePackParser.PackMetadata metadata =
          YamlRulePackParser.readPackMetadata(yamlFile, defaultPackId, RulePackOwner.VENDOR, 300);
      return new FileYamlRulePack(
          yamlFile,
          metadata.packId(),
          metadata.displayName(),
          metadata.owner(),
          metadata.priority(),
          metadata.overrides());
    } catch (IOException e) {
      LogChannel.GENERAL.logError(
          "Failed to read pack metadata from "
              + yamlFile.getAbsolutePath()
              + ": "
              + e.getMessage());
      return new FileYamlRulePack(
          yamlFile, defaultPackId, defaultPackId, RulePackOwner.VENDOR, 300);
    }
  }

  private void discoverJarPacksInDirectory(File pluginsDir, List<IHopLintRulePack> packs) {
    List<File> jars = new ArrayList<>();
    collectJars(pluginsDir, jars);
    for (File jar : jars) {
      // try-with-resources: the loader used to be left open, leaking a file handle per jar on
      // every discovery pass, and discovery runs on every rule resolution.
      try (URLClassLoader classLoader =
          new URLClassLoader(
              new URL[] {jar.toURI().toURL()}, HopCoreRulePack.class.getClassLoader())) {
        Map<String, IHopLintRulePack> fromJar = new LinkedHashMap<>();
        registerServiceLoaderPacks(fromJar, classLoader);
        // Rules are read eagerly below, so nothing needs the loader after this block.
        for (IHopLintRulePack pack : fromJar.values()) {
          packs.add(EagerRulePack.of(pack));
        }
      } catch (Exception e) {
        LogChannel.GENERAL.logDetailed(
            "Skipping rule pack discovery for jar "
                + jar.getAbsolutePath()
                + ": "
                + e.getMessage());
      }
    }
  }

  private void collectJars(File directory, List<File> jars) {
    File[] children = directory.listFiles();
    if (children == null) {
      return;
    }
    for (File child : children) {
      if (child.isDirectory()) {
        collectJars(child, jars);
      } else if (child.getName().endsWith(".jar")
          && !child.getName().contains("jackson")
          && child.getName().contains("lint")) {
        jars.add(child);
      }
    }
  }
}
