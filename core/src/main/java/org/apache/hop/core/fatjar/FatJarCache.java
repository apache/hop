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

package org.apache.hop.core.fatjar;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;

/**
 * A local cache of fat jars, so a run does not rebuild a fat jar the installation already has.
 *
 * <p>A cached jar is named after its flavor and a fingerprint of its input jars (path, size and
 * modification time): {@code <flavor>-<fingerprint>.jar}. Installing or upgrading a plugin changes
 * the fingerprint, and the next run builds a new jar. Next to the jar a {@code .sha256} sidecar
 * holds the SHA-256 of the jar contents, so a deployment can name and compare the jar it uploads
 * without hashing a few hundred megabytes again.
 *
 * <p>The cache folder is {@value #VARIABLE_CACHE_FOLDER} (variable, system property or environment
 * variable), or {@code .hop/fat-jars} in the user's home folder. Jars of the same flavor that were
 * not used for {@link #RETENTION} are removed after a build.
 */
public final class FatJarCache {

  /** Folder of the fat jar cache. */
  public static final String VARIABLE_CACHE_FOLDER = "HOP_FAT_JAR_CACHE_FOLDER";

  public static final String SHA256_SIDECAR_SUFFIX = ".sha256";

  /** Number of fingerprint hex characters in a cached jar name. */
  static final int FINGERPRINT_LENGTH = 16;

  /** Unused jars of a flavor older than this are removed after a new build. */
  static final Duration RETENTION = Duration.ofDays(7);

  private static final Object BUILD_LOCK = new Object();

  private static final Class<?> PKG = FatJarCache.class;

  private FatJarCache() {}

  /**
   * @param log log channel for the build
   * @param variables resolves {@value #VARIABLE_CACHE_FOLDER}
   * @param flavor jar name prefix, for example {@link NativeSparkFatJar#FLAVOR_PROVIDED}
   * @param jarFilenames the jars to merge
   * @return the cached fat jar for exactly these input jars
   */
  public static Path getOrBuild(
      ILogChannel log, IVariables variables, String flavor, List<String> jarFilenames)
      throws HopException {
    return getOrBuild(log, variables, flavor, jarFilenames, null);
  }

  /**
   * @param progressMonitor optional: follows a build and can cancel it; a cached jar is returned
   *     without using it
   * @see #getOrBuild(ILogChannel, IVariables, String, List)
   */
  public static Path getOrBuild(
      ILogChannel log,
      IVariables variables,
      String flavor,
      List<String> jarFilenames,
      IProgressMonitor progressMonitor)
      throws HopException {
    Path folder = cacheFolder(variables);
    String fingerprint = fingerprint(flavor, jarFilenames);
    Path jar = folder.resolve(flavor + "-" + fingerprint + ".jar");

    synchronized (BUILD_LOCK) {
      if (Files.isRegularFile(jar) && Files.isRegularFile(sidecar(jar))) {
        touch(jar);
        log.logBasic("Using cached fat jar " + jar);
        return jar;
      }

      long start = System.currentTimeMillis();
      log.logBasic(
          "Building fat jar "
              + jar
              + " from "
              + jarFilenames.size()
              + " jar files. This happens once per installation change.");
      try {
        Files.createDirectories(folder);
      } catch (IOException e) {
        throw new HopException("Unable to create the fat jar cache folder " + folder, e);
      }
      // Build next to the final name and move it in place, so a reader never sees half a jar.
      Path temp = folder.resolve(jar.getFileName() + ".tmp-" + UUID.randomUUID());
      try {
        FatJarBuilder builder = new FatJarBuilder(log, variables, temp.toString(), jarFilenames);
        builder.setProgressMonitor(progressMonitor);
        builder.buildTargetJar();
        if (progressMonitor != null) {
          progressMonitor.subTask(BaseMessages.getString(PKG, "FatJarCache.Progress.Checksum"));
        }
        String sha = sha256Hex(temp);
        Path tempSidecar = folder.resolve(temp.getFileName() + SHA256_SIDECAR_SUFFIX);
        Files.writeString(tempSidecar, sha + "\n", StandardCharsets.UTF_8);
        Files.move(temp, jar, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        Files.move(
            tempSidecar,
            sidecar(jar),
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE);
      } catch (IOException e) {
        throw new HopException("Unable to store fat jar " + jar, e);
      } finally {
        deleteQuietly(temp);
        deleteQuietly(folder.resolve(temp.getFileName() + SHA256_SIDECAR_SUFFIX));
      }
      log.logBasic(
          "Built fat jar "
              + jar
              + " in "
              + (System.currentTimeMillis() - start) / 1000
              + "s (sha256="
              + sha256Of(jar)
              + ")");
      removeStale(log, folder, flavor, jar);
      return jar;
    }
  }

  /**
   * SHA-256 of a jar's contents: read from the {@code .sha256} sidecar when present, computed
   * otherwise.
   */
  public static String sha256Of(Path jar) throws HopException {
    Path sidecar = sidecar(jar);
    if (Files.isRegularFile(sidecar)) {
      try {
        String sha = Files.readString(sidecar, StandardCharsets.UTF_8).trim();
        if (sha.length() == 64) {
          return sha.toLowerCase();
        }
      } catch (IOException e) {
        // compute below
      }
    }
    return sha256Hex(jar);
  }

  /** SHA-256 hex (lowercase) of a file. */
  public static String sha256Hex(Path file) throws HopException {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      try (InputStream in = Files.newInputStream(file);
          DigestInputStream din = new DigestInputStream(in, digest)) {
        din.transferTo(OutputStream.nullOutputStream());
      }
      return HexFormat.of().formatHex(digest.digest());
    } catch (Exception e) {
      throw new HopException("Unable to compute SHA-256 of " + file, e);
    }
  }

  static Path cacheFolder(IVariables variables) {
    String folder = variables != null ? variables.getVariable(VARIABLE_CACHE_FOLDER) : null;
    if (StringUtils.isBlank(folder)) {
      folder = System.getProperty(VARIABLE_CACHE_FOLDER);
    }
    if (StringUtils.isBlank(folder)) {
      folder = System.getenv(VARIABLE_CACHE_FOLDER);
    }
    if (StringUtils.isBlank(folder)) {
      return defaultCacheFolder();
    }
    return Paths.get(variables != null ? variables.resolve(folder) : folder);
  }

  /**
   * {@code .hop/fat-jars} in the user's home folder. Per user, so on a shared machine such as an
   * edge node one user's cache folder never blocks another user's build, and on disk rather than in
   * a temporary folder that may be a tmpfs holding a few hundred megabytes in memory.
   */
  static Path defaultCacheFolder() {
    return Paths.get(System.getProperty("user.home"), ".hop", "fat-jars");
  }

  /** Fingerprint of the flavor and the path, size and modification time of every input jar. */
  static String fingerprint(String flavor, List<String> jarFilenames) throws HopException {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(flavor.getBytes(StandardCharsets.UTF_8));
      for (String filename : jarFilenames.stream().sorted().toList()) {
        File file = new File(filename);
        String line =
            "\n" + file.getAbsolutePath() + "|" + file.length() + "|" + file.lastModified();
        digest.update(line.getBytes(StandardCharsets.UTF_8));
      }
      return HexFormat.of().formatHex(digest.digest()).substring(0, FINGERPRINT_LENGTH);
    } catch (Exception e) {
      throw new HopException("Unable to fingerprint the fat jar inputs", e);
    }
  }

  static Path sidecar(Path jar) {
    return jar.resolveSibling(jar.getFileName() + SHA256_SIDECAR_SUFFIX);
  }

  private static void touch(Path jar) {
    try {
      Files.setLastModifiedTime(jar, FileTime.from(Instant.now()));
    } catch (IOException e) {
      // Only affects retention.
    }
  }

  /** Remove jars of the same flavor that were not used for {@link #RETENTION}. */
  private static void removeStale(ILogChannel log, Path folder, String flavor, Path keep) {
    Instant cutoff = Instant.now().minus(RETENTION);
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(folder, flavor + "-*.jar")) {
      for (Path entry : entries) {
        String suffix = entry.getFileName().toString().substring(flavor.length() + 1);
        // hop-native-* also matches hop-native-provided-*: only touch this flavor's fingerprints
        if (entry.equals(keep) || !suffix.matches("[0-9a-f]{" + FINGERPRINT_LENGTH + "}\\.jar")) {
          continue;
        }
        if (Files.getLastModifiedTime(entry).toInstant().isBefore(cutoff)) {
          log.logDetailed("Removing unused fat jar " + entry);
          deleteQuietly(entry);
          deleteQuietly(sidecar(entry));
        }
      }
    } catch (IOException e) {
      log.logDetailed("Unable to clean up the fat jar cache " + folder + ": " + e.getMessage());
    }
  }

  private static void deleteQuietly(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      // best effort
    }
  }
}
