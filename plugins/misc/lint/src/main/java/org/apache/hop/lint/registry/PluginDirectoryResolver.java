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
import java.net.URL;
import org.apache.hop.core.plugins.JarCache;

/** Resolves Hop plugin directories for rule pack YAML discovery. */
public final class PluginDirectoryResolver {

  private PluginDirectoryResolver() {}

  /** Directory containing the lint engine plugin jar (e.g. {@code .../plugins/misc/lint}). */
  public static File locateEnginePluginDirectory(Class<?> anchor) {
    try {
      URL location = anchor.getProtectionDomain().getCodeSource().getLocation();
      if (location == null) {
        return null;
      }
      File codeSource = new File(location.toURI());
      return codeSource.isFile() ? codeSource.getParentFile() : codeSource;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Hop {@code plugins/} root derived from the engine plugin path ({@code .../plugins/misc/lint} →
   * {@code .../plugins}).
   */
  public static File resolvePluginsRootFromEngine(Class<?> anchor) {
    File engineDir = locateEnginePluginDirectory(anchor);
    if (engineDir == null) {
      return null;
    }
    File miscDir = engineDir.getParentFile();
    if (miscDir == null) {
      return null;
    }
    File pluginsDir = miscDir.getParentFile();
    if (pluginsDir != null && pluginsDir.isDirectory() && "plugins".equals(pluginsDir.getName())) {
      return pluginsDir;
    }
    return null;
  }

  public static File resolvePluginsRoot() {
    File fromEngine = resolvePluginsRootFromEngine(HopCoreRulePack.class);
    if (fromEngine != null) {
      return fromEngine;
    }

    String hopHome = System.getenv("HOP_HOME");
    if (isEmpty(hopHome)) {
      hopHome = System.getProperty("HOP_HOME");
    }
    if (isEmpty(hopHome)) {
      hopHome = System.getProperty("hop.installation.path");
    }
    if (!isEmpty(hopHome)) {
      File pluginsDir = new File(hopHome, "plugins");
      if (pluginsDir.isDirectory()) {
        return pluginsDir;
      }
    }

    try {
      for (String folder : JarCache.getInstance().getPluginFolders()) {
        if (isEmpty(folder)) {
          continue;
        }
        File candidate = new File(folder);
        if (candidate.isDirectory()) {
          return candidate;
        }
      }
    } catch (Exception ignored) {
      // JarCache may not be initialized in unit tests.
    }
    return null;
  }

  private static boolean isEmpty(String value) {
    return value == null || value.trim().isEmpty();
  }
}
