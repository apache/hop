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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.DriverDownload;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.HopURLClassLoader;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.driver.DriverResolver.ResolvedArtifact;

/**
 * Downloads a driver (and its transitive runtime dependencies) and installs the resulting jars into
 * a JDBC folder Hop scans, where the existing {@code DatabasePluginType} scan picks them up. It can
 * also hot-load them into the database plugin's classloader so they can be used without a restart.
 * This is the single install path the CLI, the Studio dialog and the Docker entrypoint all call.
 */
public class DriverInstaller {

  /**
   * The folder where Hop's own (bundled) JDBC drivers live. Downloaded drivers should not be mixed
   * in here, so they survive upgrades and don't touch the shipped distribution.
   */
  public static final String BUNDLED_JDBC_FOLDER = "lib/jdbc";

  /** A driver jar found in one of the scanned JDBC folders. */
  public record InstalledDriver(File folder, File jar, String version) {}

  /**
   * All folders Hop scans for JDBC drivers: the entries of {@code HOP_SHARED_JDBC_FOLDERS}, or just
   * {@code lib/jdbc} when it is not set.
   */
  public static List<File> configuredJdbcFolders() {
    List<File> folders = new ArrayList<>();
    String shared = System.getProperty(Const.HOP_SHARED_JDBC_FOLDERS);
    if (shared != null && !shared.isBlank()) {
      for (String entry : shared.split(",")) {
        String folder = entry.trim();
        if (!folder.isEmpty()) {
          folders.add(new File(folder).getAbsoluteFile());
        }
      }
    }
    if (folders.isEmpty()) {
      folders.add(new File(BUNDLED_JDBC_FOLDER).getAbsoluteFile());
    }
    return folders;
  }

  /**
   * The folder a downloaded driver should be installed into. To keep downloaded (and especially
   * proprietary) drivers out of Hop's own bundled {@code lib/jdbc} folder, this returns the first
   * entry of {@code HOP_SHARED_JDBC_FOLDERS} that is NOT {@code lib/jdbc}. It only falls back to
   * {@code lib/jdbc} when that is the only configured folder, or nothing is configured at all.
   */
  public static File defaultInstallFolder() {
    String shared = System.getProperty(Const.HOP_SHARED_JDBC_FOLDERS);
    if (shared == null || shared.isBlank()) {
      return new File(BUNDLED_JDBC_FOLDER).getAbsoluteFile();
    }
    String fallback = null;
    for (String entry : shared.split(",")) {
      String folder = entry.trim();
      if (folder.isEmpty()) {
        continue;
      }
      if (fallback == null) {
        fallback = folder;
      }
      if (!isBundledFolder(folder)) {
        return new File(folder).getAbsoluteFile();
      }
    }
    return new File(fallback != null ? fallback : BUNDLED_JDBC_FOLDER).getAbsoluteFile();
  }

  private static boolean isBundledFolder(String folder) {
    String normalized = folder.replace('\\', '/');
    return normalized.equals(BUNDLED_JDBC_FOLDER) || normalized.endsWith("/" + BUNDLED_JDBC_FOLDER);
  }

  /** Local Aether cache (a Maven-style local repository). Kept on the user's machine only. */
  public static File defaultCacheFolder() {
    return new File(System.getProperty("user.home"), ".hop" + File.separator + "driver-cache");
  }

  /**
   * Find every installed jar of the given Maven artifact across all scanned JDBC folders.
   *
   * @param artifactId the Maven artifactId, e.g. {@code ojdbc11}
   * @return the matching jars with their version and folder, possibly empty
   */
  public static List<InstalledDriver> findInstalled(String artifactId) {
    List<InstalledDriver> found = new ArrayList<>();
    if (artifactId == null || artifactId.isBlank()) {
      return found;
    }
    String prefix = artifactId + "-";
    for (File folder : configuredJdbcFolders()) {
      File[] jars = folder.listFiles((dir, name) -> name.endsWith(".jar"));
      if (jars == null) {
        continue;
      }
      for (File jar : jars) {
        String version = versionOf(prefix, jar.getName());
        if (version != null) {
          found.add(new InstalledDriver(folder, jar, version));
        }
      }
    }
    return found;
  }

  /**
   * Extract the version from a jar name of the form {@code <artifactId>-<version>.jar}, where the
   * version starts with a digit. Returns null when the name does not match this artifact.
   */
  private static String versionOf(String prefix, String jarName) {
    if (!jarName.startsWith(prefix)
        || !jarName.endsWith(".jar")
        || jarName.length() <= prefix.length()
        || !Character.isDigit(jarName.charAt(prefix.length()))) {
      return null;
    }
    return jarName.substring(prefix.length(), jarName.length() - ".jar".length());
  }

  /**
   * Resolve + download the driver and copy every resolved jar into {@code targetFolder}. Any
   * existing jar of the same artifact with a different version is removed from the target folder
   * first, so the folder never ends up with two versions of the same driver (or dependency).
   *
   * @param download the driver download descriptor (from the database plugin)
   * @param version optional version override (null uses the descriptor default)
   * @param repoUrl optional Maven repository URL (null uses Maven Central)
   * @param targetFolder the folder to install into
   * @return the list of jar files written into the target folder
   */
  public List<File> install(
      DriverDownload download, String version, String repoUrl, File targetFolder)
      throws HopException {

    File cache = defaultCacheFolder();
    if (!cache.exists() && !cache.mkdirs()) {
      throw new HopException("Unable to create driver cache folder " + cache.getAbsolutePath());
    }

    DriverResolver resolver = new DriverResolver(cache);
    List<ResolvedArtifact> resolved =
        resolver.resolve(
            download.toCoordinate(version),
            repoUrl,
            download.getExcludes(),
            download.getRepositoryUrl());

    if (!targetFolder.exists() && !targetFolder.mkdirs()) {
      throw new HopException("Unable to create target folder " + targetFolder.getAbsolutePath());
    }

    List<File> installed = new ArrayList<>();
    for (ResolvedArtifact artifact : resolved) {
      removeOtherVersions(targetFolder, artifact);
      File destination = new File(targetFolder, artifact.file().getName());
      try {
        Files.copy(
            artifact.file().toPath(), destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
      } catch (Exception e) {
        throw new HopException("Unable to copy " + artifact.file() + " to " + destination, e);
      }
      installed.add(destination);
    }
    return installed;
  }

  /** Remove jars of the same artifact but a different version from the target folder. */
  static void removeOtherVersions(File targetFolder, ResolvedArtifact artifact) {
    File[] existing = targetFolder.listFiles((dir, name) -> name.endsWith(".jar"));
    if (existing == null) {
      return;
    }
    String prefix = artifact.artifactId() + "-";
    String keep = artifact.file().getName();
    for (File jar : existing) {
      if (!jar.getName().equals(keep) && versionOf(prefix, jar.getName()) != null) {
        // Same artifact, different version: remove it so we don't keep two versions.
        try {
          Files.deleteIfExists(jar.toPath());
        } catch (Exception e) {
          // Best effort; leave the old jar in place if it cannot be removed.
        }
      }
    }
  }

  /**
   * Make freshly-installed jars usable without a restart by injecting them into the database
   * plugin's classloader. The database plugin classloaders are otherwise fixed at startup, so a
   * driver dropped into a JDBC folder would only be picked up on the next launch.
   *
   * @param databaseType the database plugin type, e.g. {@code ORACLE}
   * @param jars the jar files to add (typically the result of {@link #install})
   * @return true if the jars were hot-loaded into the plugin classloader
   */
  public static boolean hotLoad(String databaseType, List<File> jars) {
    try {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin plugin = registry.getPlugin(DatabasePluginType.class, databaseType);
      if (plugin == null) {
        return false;
      }
      ClassLoader classLoader = registry.getClassLoader(plugin);
      if (classLoader instanceof HopURLClassLoader hopClassLoader) {
        for (File jar : jars) {
          hopClassLoader.addJar(jar.toURI().toURL());
        }
        return true;
      }
      return false;
    } catch (Exception e) {
      return false;
    }
  }
}
