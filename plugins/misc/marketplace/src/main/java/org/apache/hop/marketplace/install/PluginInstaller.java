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
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.apache.hop.marketplace.resolve.MavenRepositoryClient;

/**
 * Downloads a plugin zip, stages it under {@code plugins/.staging/}, writes a receipt, and
 * activates into the live plugins tree. Uses stage-then-activate (restart-friendly) rather than
 * hot-reload.
 */
public class PluginInstaller {

  public static final String STAGING_DIR = "plugins/.staging";
  public static final String RECEIPTS_DIR = "plugins/.marketplace";

  private final ILogChannel log;
  private final Path hopHome;
  private final MarketplaceConfig config;
  private final MavenRepositoryClient client;

  public PluginInstaller(ILogChannel log, Path hopHome, MarketplaceConfig config) {
    this.log = log;
    this.hopHome = hopHome;
    this.config = config;
    this.client = new MavenRepositoryClient(log);
  }

  PluginInstaller(
      ILogChannel log, Path hopHome, MarketplaceConfig config, MavenRepositoryClient client) {
    this.log = log;
    this.hopHome = hopHome;
    this.config = config;
    this.client = client;
  }

  /**
   * Download, stage, and activate a plugin. Returns the receipt after activation.
   *
   * @param activateImmediately if true, move staged files into place now (CLI default). GUI may
   *     stage and ask for restart later; for CLI we activate immediately then ask the user to
   *     restart so classloaders pick up the new jars.
   */
  public InstallReceipt install(MavenCoordinates coordinates, boolean activateImmediately)
      throws HopException {
    return install(coordinates, activateImmediately, null);
  }

  /**
   * @param forceRepoId when non-blank, only that repository is used (no fallback chain)
   */
  public InstallReceipt install(
      MavenCoordinates coordinates, boolean activateImmediately, String forceRepoId)
      throws HopException {
    Path downloadDir = hopHome.resolve(STAGING_DIR).resolve(".download");
    Path zipFile =
        downloadDir.resolve(coordinates.artifactId() + "-" + coordinates.version() + ".zip");
    try {
      List<MarketplaceRepository> repos = resolveRepositories(forceRepoId);
      MarketplaceRepository used = null;
      List<String> errors = new ArrayList<>();
      for (MarketplaceRepository repo : repos) {
        try {
          client.downloadZip(repo, coordinates, zipFile);
          used = repo;
          break;
        } catch (HopException e) {
          errors.add(
              repo.getId()
                  + " @ "
                  + repo.normalizedUrl()
                  + " → "
                  + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
          log.logBasic("Repository attempt failed: " + errors.get(errors.size() - 1));
        }
      }
      if (used == null) {
        throw new HopException(
            "Could not download "
                + coordinates.gav()
                + " from any configured repository:\n  - "
                + String.join("\n  - ", errors));
      }

      Path stageRoot = hopHome.resolve(STAGING_DIR).resolve(coordinates.artifactId());
      deleteRecursive(stageRoot);
      Files.createDirectories(stageRoot);
      List<String> relativePaths = unzipAllowedEntries(zipFile, stageRoot);

      InstallReceipt receipt = new InstallReceipt();
      receipt.setGroupId(coordinates.groupId());
      receipt.setArtifactId(coordinates.artifactId());
      receipt.setVersion(coordinates.version());
      receipt.setInstalledAt(Instant.now().toString());
      receipt.setRepositoryId(used.getId());
      receipt.setRepositoryUrl(used.normalizedUrl());
      receipt.setPaths(relativePaths);
      receipt.setPendingActivation(!activateImmediately);
      writeReceipt(receipt);

      if (activateImmediately) {
        activateStaged(coordinates.artifactId(), relativePaths);
        receipt.setPendingActivation(false);
        writeReceipt(receipt);
        deleteRecursive(stageRoot);
      }

      Files.deleteIfExists(zipFile);
      log.logBasic(
          "Installed "
              + coordinates.gav()
              + (activateImmediately
                  ? ". Restart Hop to load the plugin."
                  : ". Staged for activation on next restart."));
      return receipt;
    } catch (IOException e) {
      throw new HopException("Failed to install plugin " + coordinates.gav(), e);
    }
  }

  private List<MarketplaceRepository> resolveRepositories(String forceRepoId) throws HopException {
    if (StringUtils.isNotBlank(forceRepoId)) {
      MarketplaceRepository forced = config.findRepository(forceRepoId);
      if (forced == null) {
        throw new HopException("Unknown marketplace repository id: " + forceRepoId);
      }
      if (!forced.isEnabled()) {
        throw new HopException("Marketplace repository is disabled: " + forceRepoId);
      }
      return List.of(forced);
    }
    return config.orderedRepositories();
  }

  /** Activate all staged plugins (startup hook). */
  public void activateAllPending() throws HopException {
    Path staging = hopHome.resolve(STAGING_DIR);
    if (!Files.isDirectory(staging)) {
      return;
    }
    try {
      try (var stream = Files.list(staging)) {
        for (Path staged : stream.filter(Files::isDirectory).toList()) {
          if (staged.getFileName().toString().startsWith(".")) {
            continue;
          }
          List<String> paths = collectRelativePaths(staged);
          activateStaged(staged.getFileName().toString(), paths);
          deleteRecursive(staged);
          log.logBasic("Activated staged plugin: " + staged.getFileName());
        }
      }
    } catch (IOException e) {
      throw new HopException("Failed to activate staged plugins", e);
    }
  }

  private void activateStaged(String artifactId, List<String> relativePaths) throws HopException {
    Path stageRoot = hopHome.resolve(STAGING_DIR).resolve(artifactId);
    try {
      for (String relative : relativePaths) {
        // Never overwrite core beam libraries from a marketplace zip
        if (isProtectedPath(relative)) {
          log.logBasic("Skipping protected path from marketplace package: " + relative);
          continue;
        }
        Path from = stageRoot.resolve(relative);
        Path to = hopHome.resolve(relative);
        if (Files.isDirectory(from)) {
          Files.createDirectories(to);
        } else if (Files.isRegularFile(from)) {
          Files.createDirectories(to.getParent());
          Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
        }
      }
    } catch (IOException e) {
      throw new HopException("Failed to activate staged plugin " + artifactId, e);
    }
  }

  static boolean isProtectedPath(String relative) {
    String normalized = relative.replace('\\', '/');
    return normalized.equals("lib/beam")
        || normalized.startsWith("lib/beam/")
        || normalized.equals("lib/core")
        || normalized.startsWith("lib/core/");
  }

  private List<String> unzipAllowedEntries(Path zipFile, Path stageRoot)
      throws IOException, HopException {
    List<String> paths = new ArrayList<>();
    try (ZipFile zip = new ZipFile(zipFile.toFile())) {
      Enumeration<? extends ZipEntry> entries = zip.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        String name = entry.getName();
        if (name == null || name.isBlank()) {
          continue;
        }
        // Normalize and reject path traversal
        Path resolved = stageRoot.resolve(name).normalize();
        if (!resolved.startsWith(stageRoot)) {
          throw new HopException("Zip entry escapes target directory: " + name);
        }
        if (entry.isDirectory()) {
          Files.createDirectories(resolved);
        } else {
          Files.createDirectories(resolved.getParent());
          try (InputStream in = zip.getInputStream(entry)) {
            Files.copy(in, resolved, StandardCopyOption.REPLACE_EXISTING);
          }
        }
        String relative = stageRoot.relativize(resolved).toString().replace('\\', '/');
        if (!relative.isBlank()) {
          paths.add(relative);
        }
      }
    }
    return paths;
  }

  private void writeReceipt(InstallReceipt receipt) throws HopException {
    try {
      Path dir = hopHome.resolve(RECEIPTS_DIR);
      Files.createDirectories(dir);
      Path file = dir.resolve(receipt.getArtifactId() + ".json");
      HopJson.newMapper().writerWithDefaultPrettyPrinter().writeValue(file.toFile(), receipt);
    } catch (IOException e) {
      throw new HopException("Unable to write install receipt for " + receipt.getArtifactId(), e);
    }
  }

  public static InstallReceipt readReceipt(Path hopHome, String artifactId) throws HopException {
    Path file = hopHome.resolve(RECEIPTS_DIR).resolve(artifactId + ".json");
    if (!Files.isRegularFile(file)) {
      return null;
    }
    try {
      return HopJson.newMapper().readValue(file.toFile(), InstallReceipt.class);
    } catch (IOException e) {
      throw new HopException("Unable to read install receipt for " + artifactId, e);
    }
  }

  private static List<String> collectRelativePaths(Path stageRoot) throws IOException {
    List<String> paths = new ArrayList<>();
    Files.walkFileTree(
        stageRoot,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            paths.add(stageRoot.relativize(file).toString().replace('\\', '/'));
            return FileVisitResult.CONTINUE;
          }
        });
    return paths;
  }

  static void deleteRecursive(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    Files.walkFileTree(
        root,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }
}
