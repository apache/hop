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

package org.apache.hop.marketplace.install;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;

/**
 * Removes a marketplace-installed plugin using its receipt. Shared {@code lib/core} jars stay in
 * place so other plugins (and the system classpath) keep working.
 */
public class PluginUninstaller {

  private final ILogChannel log;
  private final Path hopHome;

  public PluginUninstaller(ILogChannel log, Path hopHome) {
    this.log = log;
    this.hopHome = hopHome;
  }

  public void uninstall(String artifactId) throws HopException {
    InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, artifactId);
    if (receipt == null) {
      throw new HopException(
          "No marketplace install receipt for '"
              + artifactId
              + "'. Only plugins installed via the marketplace can be uninstalled this way.");
    }

    // Delete files deepest-first; leave shared lib/core jars (sticky, may be used by others).
    // Files of a previous version that an update could not remove yet go too, unless another
    // marketplace artifact has installed the same file since.
    Set<String> paths = new LinkedHashSet<>(PluginInstaller.nullSafe(receipt.getPaths()));
    if (!PluginInstaller.nullSafe(receipt.getObsoletePaths()).isEmpty()) {
      Set<String> otherReceiptPaths =
          PluginInstaller.pathsOfOtherReceipts(hopHome, artifactId, log);
      for (String obsolete : receipt.getObsoletePaths()) {
        if (StringUtils.isNotBlank(obsolete)
            && !otherReceiptPaths.contains(PluginInstaller.normalizeRelative(obsolete))) {
          paths.add(obsolete);
        }
      }
    }
    Set<Path> dirs = new HashSet<>();
    for (String relative : paths) {
      if (PluginInstaller.isSharedCorePath(relative)) {
        log.logBasic("Leaving shared lib/core path in place: " + relative);
        continue;
      }
      Path path = hopHome.resolve(relative);
      try {
        if (Files.isRegularFile(path)) {
          Files.deleteIfExists(path);
          Path parent = path.getParent();
          if (parent != null) {
            dirs.add(parent);
          }
        } else if (Files.isDirectory(path)) {
          dirs.add(path);
        }
      } catch (IOException e) {
        throw new HopException("Failed to remove " + path, e);
      }
    }

    removeEmptyPluginDirectories(hopHome, dirs, log);

    Path receiptFile = hopHome.resolve(PluginInstaller.RECEIPTS_DIR).resolve(artifactId + ".json");
    try {
      Files.deleteIfExists(receiptFile);
    } catch (IOException e) {
      throw new HopException("Failed to remove receipt for " + artifactId, e);
    }
    log.logBasic(
        "Uninstalled "
            + artifactId
            + ". Restart Hop so the plugin registry drops the removed classes.");
  }

  /** Remove the given directories, deepest first, when they are empty and under plugins/. */
  static void removeEmptyPluginDirectories(Path hopHome, Set<Path> dirs, ILogChannel log) {
    Path plugins = hopHome.resolve("plugins").toAbsolutePath().normalize();
    dirs.stream()
        .sorted(Comparator.comparingInt(p -> -p.getNameCount()))
        .forEach(
            dir -> {
              try {
                if (Files.isDirectory(dir)
                    && isEmpty(dir)
                    && dir.toAbsolutePath().normalize().startsWith(plugins)) {
                  Files.deleteIfExists(dir);
                }
              } catch (IOException e) {
                log.logError("Could not remove directory " + dir, e);
              }
            });
  }

  private static boolean isEmpty(Path dir) throws IOException {
    try (var stream = Files.list(dir)) {
      return stream.findAny().isEmpty();
    }
  }
}
