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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;

/**
 * Removes a marketplace-installed plugin using its receipt. Shared {@code lib/core} jars stay in
 * place so other plugins (and the system classpath) keep working.
 */
public class PluginUninstaller {

  private final ILogChannel log;
  private final Path hopHome;
  private final PendingChanges pending;

  /** See {@code PluginInstaller#filesLockedWhileInUse}. */
  private final boolean filesLockedWhileInUse;

  public PluginUninstaller(ILogChannel log, Path hopHome) {
    this(log, hopHome, Const.isWindows());
  }

  PluginUninstaller(ILogChannel log, Path hopHome, boolean filesLockedWhileInUse) {
    this.log = log;
    this.hopHome = hopHome;
    this.pending = new PendingChanges(hopHome);
    this.filesLockedWhileInUse = filesLockedWhileInUse;
  }

  public void uninstall(String artifactId) throws HopException {
    InstallReceipt receipt = PluginInstaller.readReceipt(hopHome, artifactId);
    if (receipt == null) {
      throw new HopException(
          "No marketplace install receipt for '"
              + artifactId
              + "'. Only plugins installed via the marketplace can be uninstalled this way.");
    }

    // Leave shared lib/core jars in place (sticky, may be used by others). Files of a previous
    // version that an update could not remove yet go too, unless another marketplace artifact has
    // installed the same file since. Those are the files that were in use, so a failed delete is
    // logged and kept in the receipt instead of stopping the uninstall half way. On Windows, where
    // a plugin's own files are in use as well while Hop runs, Hop removes them the next time it
    // starts.
    Set<String> obsolete = new LinkedHashSet<>();
    if (!PluginInstaller.nullSafe(receipt.getObsoletePaths()).isEmpty()) {
      Set<String> otherReceiptPaths =
          PluginInstaller.pathsOfOtherReceipts(hopHome, artifactId, log);
      for (String path : receipt.getObsoletePaths()) {
        if (StringUtils.isNotBlank(path)
            && !otherReceiptPaths.contains(PluginInstaller.normalizeRelative(path))) {
          obsolete.add(path);
        }
      }
    }
    Set<Path> dirs = new HashSet<>();
    List<String> leftovers = new ArrayList<>();
    List<String> removedAtNextStart = new ArrayList<>();
    for (String relative : obsolete) {
      try {
        removePath(relative, dirs);
      } catch (IOException e) {
        if (removeAtNextStart(relative)) {
          removedAtNextStart.add(relative);
        } else {
          log.logError(
              "Could not remove "
                  + relative
                  + " of a previous version of "
                  + artifactId
                  + " (file in use?). Close Hop and uninstall the plugin again to remove it. "
                  + e.getMessage());
        }
        leftovers.add(relative);
      }
    }
    for (String relative : PluginInstaller.nullSafe(receipt.getPaths())) {
      if (obsolete.contains(relative)) {
        continue;
      }
      try {
        removePath(relative, dirs);
      } catch (IOException e) {
        if (!removeAtNextStart(relative)) {
          throw new HopException("Failed to remove " + hopHome.resolve(relative), e);
        }
        removedAtNextStart.add(relative);
        leftovers.add(relative);
      }
    }

    removeEmptyPluginDirectories(hopHome, dirs, log);

    // A plugin that is still staged would otherwise be activated again at the next start.
    Path staged = hopHome.resolve(PluginInstaller.STAGING_DIR).resolve(artifactId);
    try {
      PluginInstaller.deleteRecursive(staged);
    } catch (IOException e) {
      throw new HopException("Failed to remove the staged files of " + artifactId, e);
    }

    if (!leftovers.isEmpty()) {
      // The receipt now lists only what is left, the files and the plugin folders that hold
      // them, so the next uninstall, install or apply retries it.
      List<String> remainingDirs = new ArrayList<>();
      for (Path dir : dirs) {
        if (Files.isDirectory(dir)) {
          remainingDirs.add(hopHome.relativize(dir).toString().replace('\\', '/'));
        }
      }
      Collections.sort(remainingDirs);
      // The plugin is gone: nothing may read this receipt as an install of some version, or
      // reuse what it recorded about the shared lib/core jars.
      receipt.setArtifactId(artifactId);
      receipt.setUninstallPending(true);
      receipt.setPendingActivation(false);
      receipt.setVersion(null);
      receipt.setPreviousVersion(null);
      receipt.setChecksums(new LinkedHashMap<>());
      receipt.setSharedFiles(new LinkedHashMap<>());
      receipt.setPaths(remainingDirs);
      receipt.setObsoletePaths(leftovers);
      PluginInstaller.writeReceipt(hopHome, receipt);
      if (removedAtNextStart.size() == leftovers.size()) {
        log.logBasic(
            "Uninstalled "
                + artifactId
                + ". Hop removes the files that are in use the next time it starts: "
                + String.join(", ", leftovers));
      } else {
        log.logError(
            "Uninstalled "
                + artifactId
                + " except for files that could not be removed: "
                + String.join(", ", leftovers));
      }
      return;
    }

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

  /** Delete one receipt path. Shared lib/core paths stay; folders go when they end up empty. */
  private void removePath(String relative, Set<Path> dirs) throws IOException {
    if (StringUtils.isBlank(relative)) {
      return;
    }
    if (PluginInstaller.isSharedCorePath(relative)) {
      log.logBasic("Leaving shared lib/core path in place: " + relative);
      return;
    }
    Path path = hopHome.resolve(relative);
    if (Files.isDirectory(path)) {
      dirs.add(path);
      return;
    }
    // Also when the file is gone already (removed by the Hop launcher, for example), or cannot
    // be removed yet: the receipt then lists its folder, so the folder goes once it is empty.
    Path parent = path.getParent();
    if (parent != null) {
      dirs.add(parent);
    }
    if (Files.isRegularFile(path)) {
      Files.deleteIfExists(path);
      // A change an install left for the next start would put the file back.
      PluginInstaller.forgetPendingChange(pending, log, path);
    }
  }

  /**
   * On Windows a file that cannot be removed is in use: Hop removes it the next time it starts.
   * Shared lib/core paths never get here.
   */
  private boolean removeAtNextStart(String relative) {
    return filesLockedWhileInUse
        && PluginInstaller.queueDelete(pending, log, hopHome.resolve(relative));
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
