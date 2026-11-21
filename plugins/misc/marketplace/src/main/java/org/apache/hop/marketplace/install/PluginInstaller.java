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
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
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
  private static final String CONST_PLUGIN_INSTALLER = "PluginInstaller";

  public static final String STAGING_DIR = "plugins/.staging";
  public static final String RECEIPTS_DIR = "plugins/.marketplace";

  private final ILogChannel log;
  private final Path hopHome;
  private final MarketplaceConfig config;
  private final MavenRepositoryClient client;

  public PluginInstaller(ILogChannel log, Path hopHome, MarketplaceConfig config) {
    this(
        log != null ? log : new LogChannel(CONST_PLUGIN_INSTALLER),
        hopHome,
        config,
        new MavenRepositoryClient(log != null ? log : new LogChannel(CONST_PLUGIN_INSTALLER)));
  }

  PluginInstaller(
      ILogChannel log, Path hopHome, MarketplaceConfig config, MavenRepositoryClient client) {
    this.log = log != null ? log : new LogChannel(CONST_PLUGIN_INSTALLER);
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
    return install(coordinates, activateImmediately, null, null);
  }

  /**
   * @param forceRepoId when non-blank, only that repository is used (no fallback chain)
   */
  public InstallReceipt install(
      MavenCoordinates coordinates, boolean activateImmediately, String forceRepoId)
      throws HopException {
    return install(coordinates, activateImmediately, forceRepoId, null);
  }

  /**
   * @param forceRepoId when non-blank, only that repository is used (no fallback chain)
   * @param preferredRepoId when non-blank and force is blank, try this repository first then the
   *     normal fallback chain (used when discovery knows the source repo)
   */
  public InstallReceipt install(
      MavenCoordinates coordinates,
      boolean activateImmediately,
      String forceRepoId,
      String preferredRepoId)
      throws HopException {
    return install(
        coordinates, activateImmediately, forceRepoId, preferredRepoId, IInstallListener.NONE);
  }

  /**
   * @param listener receives phase changes and download byte counts, and can cancel the install
   *     between chunks. Pass {@link IInstallListener#NONE} for headless callers.
   */
  public InstallReceipt install(
      MavenCoordinates coordinates,
      boolean activateImmediately,
      String forceRepoId,
      String preferredRepoId,
      IInstallListener listener)
      throws HopException {
    IInstallListener progress = listener == null ? IInstallListener.NONE : listener;
    InstallReceipt already = satisfiedInstall(coordinates);
    if (already != null) {
      // An earlier update may have left files of the previous version that were in use.
      if (!nullSafe(already.getObsoletePaths()).isEmpty()) {
        removeObsoleteFiles(already);
        writeReceipt(already);
      }
      already.setAlreadyPresent(true);
      log.logBasic(
          "Plugin "
              + coordinates.gav()
              + " is already installed (receipt "
              + already.getVersion()
              + "); skipping download.");
      return already;
    }
    Path downloadDir = hopHome.resolve(STAGING_DIR).resolve(".download");
    Path zipFile =
        downloadDir.resolve(coordinates.artifactId() + "-" + coordinates.version() + ".zip");
    try {
      progress.phase(IInstallListener.Phase.RESOLVE, coordinates.gav());
      List<MarketplaceRepository> repos = resolveRepositories(forceRepoId, preferredRepoId);
      MarketplaceRepository used = null;
      List<String> errors = new ArrayList<>();
      for (MarketplaceRepository repo : repos) {
        if (progress.isCancelled()) {
          throw new HopException("Install of " + coordinates.gav() + " was cancelled");
        }
        try {
          progress.phase(IInstallListener.Phase.DOWNLOAD, repo.displayName());
          client.downloadZip(repo, coordinates, zipFile, progress);
          used = repo;
          break;
        } catch (HopException e) {
          // A cancel is a user decision, not a repository failure — do not fall through to the
          // next repository and start the whole download again.
          if (progress.isCancelled()) {
            throw e;
          }
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

      progress.phase(IInstallListener.Phase.UNZIP, coordinates.artifactId());
      Path stageRoot = hopHome.resolve(STAGING_DIR).resolve(coordinates.artifactId());
      deleteRecursive(stageRoot);
      Files.createDirectories(stageRoot);
      List<String> relativePaths = unzipAllowedEntries(zipFile, stageRoot);

      // Read the receipt of the version installed now before the new receipt replaces it: the
      // files only that version has must go, or both versions end up on the classpath.
      InstallReceipt previous = readReceipt(hopHome, coordinates.artifactId());

      InstallReceipt receipt = new InstallReceipt();
      receipt.setGroupId(coordinates.groupId());
      receipt.setArtifactId(coordinates.artifactId());
      receipt.setVersion(coordinates.version());
      receipt.setInstalledAt(Instant.now().toString());
      receipt.setRepositoryId(used.getId());
      receipt.setRepositoryUrl(used.normalizedUrl());
      receipt.setPaths(relativePaths);
      receipt.setObsoletePaths(
          obsoletePaths(
              previous,
              relativePaths,
              pathsOfOtherReceipts(hopHome, coordinates.artifactId(), log)));
      receipt.setPendingActivation(!activateImmediately);
      writeReceipt(receipt);

      if (activateImmediately) {
        progress.phase(IInstallListener.Phase.ACTIVATE, coordinates.artifactId());
        activateStaged(coordinates.artifactId(), relativePaths);
        receipt.setPendingActivation(false);
        removeObsoleteFiles(receipt);
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

  /**
   * A repeated install can skip the download when the receipt names this version and every file it
   * recorded is still on disk.
   */
  private InstallReceipt satisfiedInstall(MavenCoordinates coordinates) throws HopException {
    InstallReceipt receipt = readReceipt(hopHome, coordinates.artifactId());
    if (receipt == null || receipt.isPendingActivation()) {
      return null;
    }
    if (!coordinates.version().equals(receipt.getVersion())) {
      return null;
    }
    if (StringUtils.isNotBlank(receipt.getGroupId())
        && !receipt.getGroupId().equals(coordinates.groupId())) {
      return null;
    }
    if (!receiptFilesPresent(receipt)) {
      return null;
    }
    return receipt;
  }

  private boolean receiptFilesPresent(InstallReceipt receipt) {
    List<String> paths = receipt.getPaths();
    if (paths == null || paths.isEmpty()) {
      return false;
    }
    boolean file = false;
    for (String relative : paths) {
      if (StringUtils.isBlank(relative)) {
        continue;
      }
      Path path = activationTarget(relative);
      if (!Files.exists(path)) {
        return false;
      }
      if (Files.isRegularFile(path)) {
        file = true;
      }
    }
    return file;
  }

  private List<MarketplaceRepository> resolveRepositories(
      String forceRepoId, String preferredRepoId) throws HopException {
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
    List<MarketplaceRepository> ordered = new ArrayList<>(config.orderedRepositories());
    if (StringUtils.isNotBlank(preferredRepoId)) {
      MarketplaceRepository preferred = null;
      for (int i = 0; i < ordered.size(); i++) {
        MarketplaceRepository r = ordered.get(i);
        if (r != null && preferredRepoId.equals(r.getId())) {
          preferred = ordered.remove(i);
          break;
        }
      }
      if (preferred != null) {
        ordered.add(0, preferred);
      }
    }
    return ordered;
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
          String artifactId = staged.getFileName().toString();
          List<String> paths = collectRelativePaths(staged);
          activateStaged(artifactId, paths);
          // The files are in place now; a receipt problem must not stop the other plugins.
          try {
            InstallReceipt receipt = readReceipt(hopHome, artifactId);
            if (receipt != null) {
              receipt.setPendingActivation(false);
              removeObsoleteFiles(receipt);
              writeReceipt(receipt);
            }
          } catch (HopException e) {
            log.logError(
                "Activated staged plugin "
                    + artifactId
                    + " but could not update its install receipt; files of a previous version"
                    + " may remain",
                e);
          }
          deleteRecursive(staged);
          log.logBasic("Activated staged plugin: " + artifactId);
        }
      }
      removeLeftoverObsoleteFiles();
    } catch (IOException e) {
      throw new HopException("Failed to activate staged plugins", e);
    }
  }

  /**
   * Files a previous install wrote that the new version does not have. Shared {@code lib/core} jars
   * stay (they are sticky, see {@link #isSharedCorePath}), and so does every path another
   * marketplace receipt lists.
   *
   * @param previous receipt of the version installed until now, or null for a first install
   * @param newPaths the paths the new version installs
   * @param otherReceiptPaths paths listed by the receipts of other marketplace artifacts
   */
  static List<String> obsoletePaths(
      InstallReceipt previous, Collection<String> newPaths, Set<String> otherReceiptPaths) {
    if (previous == null) {
      return new ArrayList<>();
    }
    Set<String> keep = new HashSet<>();
    for (String path : newPaths) {
      keep.add(normalizeRelative(path));
    }
    for (String path : otherReceiptPaths) {
      keep.add(normalizeRelative(path));
    }
    // A previous version that was itself never activated still lists the files of the version
    // before it as obsolete: those are the ones on disk.
    Set<String> candidates = new LinkedHashSet<>();
    candidates.addAll(nullSafe(previous.getPaths()));
    candidates.addAll(nullSafe(previous.getObsoletePaths()));
    List<String> obsolete = new ArrayList<>();
    for (String path : candidates) {
      if (StringUtils.isBlank(path)) {
        continue;
      }
      String normalized = normalizeRelative(path);
      if (isSharedCorePath(normalized) || keep.contains(normalized)) {
        continue;
      }
      obsolete.add(normalized);
    }
    return obsolete;
  }

  /**
   * Delete the receipt's obsolete files and the plugin folders that leaves empty. A file that
   * cannot be deleted (in use, typically on Windows) stays in the receipt so a later install, apply
   * or uninstall removes it. A file that another receipt lists by now, or that is one of the
   * current version's files under a differently cased name, is no longer obsolete and is kept.
   */
  private void removeObsoleteFiles(InstallReceipt receipt) throws HopException {
    if (nullSafe(receipt.getObsoletePaths()).isEmpty()) {
      return;
    }
    Set<String> otherReceiptPaths = pathsOfOtherReceipts(hopHome, receipt.getArtifactId(), log);
    List<String> remaining = new ArrayList<>();
    Set<Path> dirs = new HashSet<>();
    Path home = hopHome.toAbsolutePath().normalize();
    for (String relative : nullSafe(receipt.getObsoletePaths())) {
      if (StringUtils.isBlank(relative)
          || isSharedCorePath(relative)
          || otherReceiptPaths.contains(normalizeRelative(relative))) {
        continue;
      }
      Path path = hopHome.resolve(relative).toAbsolutePath().normalize();
      if (!path.startsWith(home)) {
        log.logError("Ignoring obsolete path outside the Hop installation: " + relative);
        continue;
      }
      if (isCurrentFileUnderAnotherCase(receipt, relative, path)) {
        continue;
      }
      if (Files.isDirectory(path)) {
        dirs.add(path);
      } else if (Files.exists(path)) {
        try {
          Files.delete(path);
          log.logBasic(
              "Removed " + relative + " of the previous version of " + receipt.getArtifactId());
          if (path.getParent() != null) {
            dirs.add(path.getParent());
          }
        } catch (IOException e) {
          log.logError(
              "Could not remove "
                  + relative
                  + " of the previous version of "
                  + receipt.getArtifactId()
                  + " (file in use?). Close Hop and run 'hop marketplace install' or"
                  + " 'hop marketplace apply', or uninstall the plugin, to remove it. "
                  + e.getMessage());
          remaining.add(relative);
        }
      }
    }
    PluginUninstaller.removeEmptyPluginDirectories(hopHome, dirs, log);
    receipt.setObsoletePaths(remaining);
  }

  /**
   * On a case-insensitive file system, {@code plugins/tech/Foo/a.jar} of the previous version and
   * {@code plugins/tech/foo/a.jar} of the current one are the same file.
   */
  private boolean isCurrentFileUnderAnotherCase(
      InstallReceipt receipt, String relative, Path path) {
    String normalized = normalizeRelative(relative);
    for (String current : nullSafe(receipt.getPaths())) {
      String currentNormalized = normalizeRelative(current);
      if (!currentNormalized.equals(normalized) && currentNormalized.equalsIgnoreCase(normalized)) {
        Path currentPath = hopHome.resolve(currentNormalized);
        try {
          if (Files.exists(path)
              && Files.exists(currentPath)
              && Files.isSameFile(path, currentPath)) {
            return true;
          }
        } catch (IOException e) {
          // Cannot tell: keep the file rather than risk deleting the current version's copy.
          return true;
        }
      }
    }
    return false;
  }

  /** Retry the obsolete files that earlier installs could not remove. */
  private void removeLeftoverObsoleteFiles() throws HopException {
    for (InstallReceipt receipt : readAllReceipts()) {
      if (!receipt.isPendingActivation() && !nullSafe(receipt.getObsoletePaths()).isEmpty()) {
        removeObsoleteFiles(receipt);
        writeReceipt(receipt);
      }
    }
  }

  /** Paths (normalised) that the receipts of every other marketplace artifact list. */
  static Set<String> pathsOfOtherReceipts(Path hopHome, String artifactId, ILogChannel log)
      throws HopException {
    Set<String> paths = new HashSet<>();
    for (InstallReceipt receipt : readAllReceipts(hopHome, log)) {
      if (!receipt.getArtifactId().equals(artifactId)) {
        for (String path : nullSafe(receipt.getPaths())) {
          if (StringUtils.isNotBlank(path)) {
            paths.add(normalizeRelative(path));
          }
        }
      }
    }
    return paths;
  }

  private List<InstallReceipt> readAllReceipts() throws HopException {
    return readAllReceipts(hopHome, log);
  }

  private static List<InstallReceipt> readAllReceipts(Path hopHome, ILogChannel log)
      throws HopException {
    Path dir = hopHome.resolve(RECEIPTS_DIR);
    List<InstallReceipt> receipts = new ArrayList<>();
    if (!Files.isDirectory(dir)) {
      return receipts;
    }
    try (var stream = Files.list(dir)) {
      for (Path file : stream.filter(Files::isRegularFile).toList()) {
        String name = file.getFileName().toString();
        if (!name.endsWith(".json")) {
          continue;
        }
        // One unreadable receipt must not block installing or updating other plugins.
        try {
          InstallReceipt receipt =
              readReceipt(hopHome, name.substring(0, name.length() - ".json".length()));
          if (receipt != null && receipt.getArtifactId() != null) {
            receipts.add(receipt);
          }
        } catch (HopException e) {
          log.logError("Skipping unreadable marketplace install receipt " + file, e);
        }
      }
    } catch (IOException e) {
      throw new HopException("Unable to list marketplace install receipts in " + dir, e);
    }
    return receipts;
  }

  static String normalizeRelative(String relative) {
    String normalized = relative.replace('\\', '/');
    while (normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    return normalized;
  }

  static <T> List<T> nullSafe(List<T> list) {
    return list == null ? List.of() : list;
  }

  private void activateStaged(String artifactId, List<String> relativePaths) throws HopException {
    Path stageRoot = hopHome.resolve(STAGING_DIR).resolve(artifactId);
    try {
      for (String relative : relativePaths) {
        Path from = stageRoot.resolve(relative);
        Path to = activationTarget(relative);
        if (Files.isDirectory(from)) {
          Files.createDirectories(to);
        } else if (Files.isRegularFile(from)) {
          Files.createDirectories(to.getParent());
          if (isSharedCorePath(relative)) {
            activateSharedCoreJar(artifactId, relative, from, to);
          } else {
            Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
          }
        }
      }
    } catch (IOException e) {
      throw new HopException("Failed to activate staged plugin " + artifactId, e);
    }
  }

  /**
   * Install shared classpath jars (Maven provided → {@code lib/core}). Missing jars are always
   * copied. Existing jars that match the staged content are skipped. On Windows, existing jars are
   * never overwritten (the running JVM locks {@code lib/core/*} on the system classpath). On other
   * platforms, different content is replaced when possible; copy failures are logged and skipped so
   * the rest of the plugin still activates.
   */
  private void activateSharedCoreJar(String artifactId, String relative, Path from, Path to)
      throws IOException {
    boolean targetExists = Files.isRegularFile(to);
    Long mismatch = targetExists ? Files.mismatch(from, to) : null;

    if (skipSharedCoreCopy(targetExists, mismatch, Const.isWindows())) {
      if (targetExists && mismatch != null && mismatch != -1L) {
        log.logBasic(
            "Leaving existing shared lib/core jar in place (Windows locks system classpath jars): "
                + relative
                + " from "
                + artifactId
                + ". Content differs from the package; stop Hop and replace manually if needed.");
      }
      return;
    }

    if (targetExists && mismatch != null && mismatch != -1L) {
      log.logBasic(
          "Replacing shared lib/core jar with different content from "
              + artifactId
              + ": "
              + relative);
    }
    try {
      Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      // File in use (Windows), antivirus, etc. — do not fail the whole plugin install.
      log.logError(
          "Could not write shared lib/core jar (file in use?): "
              + relative
              + " — leaving existing file. "
              + e.getMessage());
    }
  }

  /**
   * Paths under {@code lib/core/} hold shared system-classpath jars (plugin assembly {@code
   * provided} scope). Marketplace <strong>installs</strong> these jars so optional plugins work the
   * same as the full client; uninstall never removes them (sticky shared libraries).
   */
  static boolean isSharedCorePath(String relative) {
    String normalized = relative.replace('\\', '/');
    return normalized.equals("lib/core") || normalized.startsWith("lib/core/");
  }

  /** Web layout keeps shared jars in {@code WEB-INF/lib} instead of {@code lib/core}. */
  private Path activationTarget(String relative) {
    Path standard = hopHome.resolve(relative);
    String normalized = relative == null ? "" : relative.replace('\\', '/');
    if (!normalized.startsWith("lib/core/") || normalized.endsWith("/")) {
      return standard;
    }
    Path webLib = webInfLib();
    if (webLib == null) {
      return standard;
    }
    Path name = Path.of(normalized).getFileName();
    return name == null ? standard : webLib.resolve(name);
  }

  private Path webInfLib() {
    Path fromCwd =
        Path.of(System.getProperty("user.dir", "."))
            .resolve("WEB-INF")
            .resolve("lib")
            .toAbsolutePath()
            .normalize();
    if (Files.isDirectory(fromCwd)) {
      return fromCwd;
    }
    Path fromHome = hopHome.resolve("webapps").resolve("ROOT").resolve("WEB-INF").resolve("lib");
    if (Files.isDirectory(fromHome)) {
      return fromHome;
    }
    return null;
  }

  /**
   * Whether activation should skip copying a staged lib/core file onto an existing target.
   *
   * <ul>
   *   <li>Missing target → never skip (install new shared jars; see #7666).
   *   <li>Identical content → always skip (avoids Windows lock failures when re-shipping the same
   *       jar, e.g. avro).
   *   <li>Different content on Windows → skip (cannot overwrite locked classpath jars).
   *   <li>Different content elsewhere → do not skip (try replace).
   * </ul>
   *
   * @param targetExists whether the destination file already exists
   * @param mismatch result of {@link Files#mismatch} when the target exists; ignored when missing
   * @param windows whether the host is Windows
   */
  static boolean skipSharedCoreCopy(boolean targetExists, Long mismatch, boolean windows) {
    if (!targetExists) {
      return false;
    }
    if (mismatch != null && mismatch == -1L) {
      return true;
    }
    return windows;
  }

  private List<String> unzipAllowedEntries(Path zipFile, Path stageRoot)
      throws IOException, HopException {
    List<String> paths = new ArrayList<>();
    try (ZipFile zip = new ZipFile(zipFile.toFile())) {
      Enumeration<? extends ZipEntry> entries = zip.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        String name = entry.getName();
        if (name.isBlank()) {
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
