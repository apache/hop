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
package org.apache.hop.core.plugins.manifest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Finds the manifests of everything installed in a Hop home: {@code plugins/**}{@code
 * /plugin-manifest.json} for plugins and {@code lib/core/*-manifest.json} for engine modules.
 */
public class PluginManifestScanner {

  private static final int PLUGIN_FOLDER_DEPTH =
      4; // plugins/<category>/<name>/plugin-manifest.json

  private PluginManifestScanner() {}

  /** All manifests found under the Hop home, sorted by manifest path. */
  public static List<PluginManifest> scan(Path hopHome) throws IOException {
    Map<String, PluginManifest> found = new TreeMap<>();
    Path plugins = hopHome.resolve("plugins");
    if (Files.isDirectory(plugins)) {
      try (Stream<Path> stream = Files.walk(plugins, PLUGIN_FOLDER_DEPTH)) {
        for (Path file :
            stream
                .filter(p -> p.getFileName().toString().equals(PluginManifest.PLUGIN_FILE_NAME))
                .toList()) {
          PluginManifest manifest = PluginManifest.read(file);
          found.put(relative(hopHome, file), manifest);
        }
      }
    }
    Path libCore = hopHome.resolve("lib").resolve("core");
    if (Files.isDirectory(libCore)) {
      try (Stream<Path> stream = Files.list(libCore)) {
        for (Path file :
            stream
                .filter(p -> p.getFileName().toString().endsWith(PluginManifest.ENGINE_FILE_SUFFIX))
                .toList()) {
          found.put(relative(hopHome, file), PluginManifest.read(file));
        }
      }
    }
    return new ArrayList<>(found.values());
  }

  /** Every file path claimed by at least one manifest, mapped to the manifests claiming it. */
  public static Map<String, List<PluginManifest>> owners(List<PluginManifest> manifests) {
    Map<String, List<PluginManifest>> owners = new TreeMap<>();
    for (PluginManifest manifest : manifests) {
      for (ManifestFile file : manifest.getFiles()) {
        owners.computeIfAbsent(file.getPath(), k -> new ArrayList<>()).add(manifest);
      }
    }
    return owners;
  }

  private static String relative(Path hopHome, Path file) {
    return hopHome.relativize(file).toString().replace('\\', '/');
  }
}
