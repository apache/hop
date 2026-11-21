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
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
import org.apache.hop.marketplace.install.InstallReceipt.SharedFile;
import org.apache.hop.marketplace.install.InstallReceipt.SharedFileAction;
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

  /**
   * Shared {@code lib/core} files an install replaced, under {@code <artifactId>/<version>/<path>}.
   * The plugin scanner skips folders named {@code lib}, so the backed-up jars are not loaded.
   */
  public static final String BACKUP_DIR = RECEIPTS_DIR + "/backup";

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
      Map<String, String> checksums = new LinkedHashMap<>();
      List<String> relativePaths = unzipAllowedEntries(zipFile, stageRoot, checksums);

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
      receipt.setChecksums(checksums);
      receipt.setPreviousVersion(previousVersion(previous, coordinates.version()));
      receipt.setSharedFiles(sharedFilesFromEarlierInstalls(relativePaths, previous, coordinates));
      receipt.setObsoletePaths(
          obsoletePaths(
              previous,
              relativePaths,
              pathsOfOtherReceipts(hopHome, coordinates.artifactId(), log)));
      receipt.setPendingActivation(!activateImmediately);
      writeReceipt(receipt);

      if (activateImmediately) {
        progress.phase(IInstallListener.Phase.ACTIVATE, coordinates.artifactId());
        receipt.setSharedFiles(
            activateStaged(
                coordinates.artifactId(),
                coordinates.version(),
                relativePaths,
                receipt.getSharedFiles()));
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
    if (Files.isDirectory(staging)) {
      activateStagedFolders(staging);
    }
    removeLeftoverObsoleteFiles();
  }

  private void activateStagedFolders(Path staging) throws HopException {
    try {
      try (var stream = Files.list(staging)) {
        for (Path staged : stream.filter(Files::isDirectory).toList()) {
          if (staged.getFileName().toString().startsWith(".")) {
            continue;
          }
          String artifactId = staged.getFileName().toString();
          List<String> paths = collectRelativePaths(staged);
          // A receipt problem must not stop the other plugins from activating.
          InstallReceipt receipt = null;
          boolean unreadable = false;
          try {
            receipt = readReceipt(hopHome, artifactId);
          } catch (HopException e) {
            unreadable = true;
            log.logError(
                "Could not read the install receipt of staged plugin "
                    + artifactId
                    + "; activating it with a new receipt",
                e);
            moveUnreadableReceiptAside(artifactId);
          }
          Map<String, SharedFile> sharedFiles =
              activateStaged(
                  artifactId,
                  receipt == null || StringUtils.isBlank(receipt.getVersion())
                      ? "unknown"
                      : receipt.getVersion(),
                  paths,
                  receipt == null ? Map.of() : receipt.getSharedFiles());
          if (unreadable) {
            // Without a receipt the plugin could not be updated or uninstalled. The version, the
            // repository and the previous version's files were only in the unreadable one.
            receipt = new InstallReceipt();
            receipt.setArtifactId(artifactId);
            receipt.setInstalledAt(Instant.now().toString());
            receipt.setPaths(paths);
            receipt.setChecksums(checksums(staged, paths));
          }
          if (receipt != null) {
            try {
              receipt.setPendingActivation(false);
              receipt.setSharedFiles(sharedFiles);
              removeObsoleteFiles(receipt);
              writeReceipt(receipt);
            } catch (HopException e) {
              log.logError(
                  "Activated staged plugin "
                      + artifactId
                      + " but could not update its install receipt; files of a previous version"
                      + " may remain",
                  e);
            }
          }
          deleteRecursive(staged);
          log.logBasic("Activated staged plugin: " + artifactId);
        }
      }
    } catch (IOException e) {
      throw new HopException("Failed to activate staged plugins", e);
    }
  }

  /**
   * Keep an unreadable receipt for inspection, under a name that is no longer read as a receipt, so
   * it does not block later installs and uninstalls of the plugin.
   */
  private void moveUnreadableReceiptAside(String artifactId) {
    Path file = hopHome.resolve(RECEIPTS_DIR).resolve(artifactId + ".json");
    Path aside = file.resolveSibling(artifactId + ".json.unreadable");
    try {
      Files.move(file, aside, StandardCopyOption.REPLACE_EXISTING);
      log.logError("Moved the unreadable install receipt of " + artifactId + " to " + aside);
    } catch (IOException e) {
      log.logError("Could not move the unreadable install receipt " + file + " aside", e);
    }
  }

  /** SHA-256 of the staged files, keyed by their relative path. */
  private Map<String, String> checksums(Path stageRoot, List<String> paths) {
    Map<String, String> checksums = new LinkedHashMap<>();
    for (String relative : paths) {
      Path file = stageRoot.resolve(relative);
      if (!Files.isRegularFile(file)) {
        continue;
      }
      MessageDigest digest = sha256();
      try (InputStream in = new DigestInputStream(Files.newInputStream(file), digest)) {
        in.transferTo(OutputStream.nullOutputStream());
        checksums.put(relative, HexFormat.of().formatHex(digest.digest()));
      } catch (IOException e) {
        log.logError("Could not compute the checksum of staged file " + file, e);
      }
    }
    return checksums;
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
   * The version an install replaces: the one installed now, or, when that one was staged and never
   * activated or is the same version again, the version it replaced in turn.
   */
  static String previousVersion(InstallReceipt previous, String newVersion) {
    if (previous == null) {
      return null;
    }
    if (previous.isPendingActivation() || StringUtils.equals(previous.getVersion(), newVersion)) {
      return previous.getPreviousVersion();
    }
    return previous.getVersion();
  }

  /**
   * What earlier installs recorded about the new version's shared {@code lib/core} files, for
   * activation to build on:
   *
   * <ul>
   *   <li>whether the marketplace created a file, and where the client's own copy was backed up,
   *       from the receipts of this and every other artifact;
   *   <li>when the same version is installed again, what that install did, so a file it replaced
   *       keeps pointing to its backup;
   *   <li>the shared files the previous version created or replaced that this version no longer
   *       ships, as {@link SharedFileAction#NOT_SHIPPED}, while they are on disk.
   * </ul>
   */
  private Map<String, SharedFile> sharedFilesFromEarlierInstalls(
      List<String> newPaths, InstallReceipt previous, MavenCoordinates coordinates)
      throws HopException {
    Map<String, SharedFile> previousFiles = new HashMap<>();
    if (previous != null) {
      for (Map.Entry<String, SharedFile> entry : nullSafe(previous.getSharedFiles()).entrySet()) {
        if (entry.getValue() != null) {
          previousFiles.put(normalizeRelative(entry.getKey()), entry.getValue());
        }
      }
    }
    boolean sameVersion =
        previous != null
            && !previous.isPendingActivation()
            && coordinates.version().equals(previous.getVersion());
    SharedFileHistory history = SharedFileHistory.of(readAllReceipts());

    Map<String, SharedFile> seeded = new LinkedHashMap<>();
    Set<String> shipped = new HashSet<>();
    for (String path : newPaths) {
      if (!isSharedCorePath(path) || path.endsWith("/")) {
        continue;
      }
      String normalized = normalizeRelative(path);
      shipped.add(normalized);
      SharedFile earlier = previousFiles.get(normalized);
      SharedFileAction action = null;
      String backup = null;
      if (sameVersion && earlier != null && isReusable(earlier.getAction())) {
        action = earlier.getAction();
        backup = earlier.getBackup();
      }
      boolean created = history.created(normalized);
      String original = history.originalBackup(normalized);
      if (action != null || created || original != null) {
        seeded.put(path, new SharedFile(action, backup, created, original));
      }
    }
    for (Map.Entry<String, SharedFile> entry : previousFiles.entrySet()) {
      SharedFile earlier = entry.getValue();
      String path = entry.getKey();
      if (shipped.contains(path)
          || !(earlier.isCreatedByMarketplace() || earlier.getOriginalBackup() != null)
          || !Files.exists(activationTarget(path))) {
        continue;
      }
      seeded.put(
          path,
          new SharedFile(
              SharedFileAction.NOT_SHIPPED,
              null,
              earlier.isCreatedByMarketplace(),
              earlier.getOriginalBackup()));
    }
    return seeded;
  }

  /**
   * Whether a same-version reinstall that finds a file unchanged keeps what the earlier install
   * recorded. Not {@link SharedFileAction#KEPT}: the file now matches the package.
   */
  private static boolean isReusable(SharedFileAction action) {
    return action == SharedFileAction.CREATED
        || action == SharedFileAction.REPLACED
        || action == SharedFileAction.UNCHANGED;
  }

  /** Shared-file facts gathered from a set of receipts. */
  private record SharedFileHistory(Set<String> createdPaths, Map<String, String> originals) {
    static SharedFileHistory of(List<InstallReceipt> receipts) {
      Set<String> created = new HashSet<>();
      Map<String, String> originals = new HashMap<>();
      for (InstallReceipt receipt : receipts) {
        for (Map.Entry<String, SharedFile> entry : nullSafe(receipt.getSharedFiles()).entrySet()) {
          SharedFile file = entry.getValue();
          if (file == null) {
            continue;
          }
          String path = normalizeRelative(entry.getKey());
          if (file.isCreatedByMarketplace()) {
            created.add(path);
          }
          if (file.getOriginalBackup() != null) {
            originals.putIfAbsent(path, file.getOriginalBackup());
          }
        }
      }
      return new SharedFileHistory(created, originals);
    }

    boolean created(String path) {
      return createdPaths.contains(path);
    }

    String originalBackup(String path) {
      return originals.get(path);
    }
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

  /**
   * Retry the obsolete files that earlier installs could not remove. Best effort: a problem with
   * one receipt is logged and does not stop the install or apply that runs this.
   */
  private void removeLeftoverObsoleteFiles() {
    List<InstallReceipt> receipts;
    try {
      receipts = readAllReceipts();
    } catch (HopException e) {
      log.logError("Could not read the marketplace install receipts to remove leftover files", e);
      return;
    }
    for (InstallReceipt receipt : receipts) {
      if (receipt.isPendingActivation() || nullSafe(receipt.getObsoletePaths()).isEmpty()) {
        continue;
      }
      try {
        removeObsoleteFiles(receipt);
        writeReceipt(receipt);
      } catch (HopException e) {
        log.logError(
            "Could not remove the leftover files of the previous version of "
                + receipt.getArtifactId(),
            e);
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

  static <K, V> Map<K, V> nullSafe(Map<K, V> map) {
    return map == null ? Map.of() : map;
  }

  /**
   * Copy the staged files into place.
   *
   * @param earlierInstalls what earlier installs recorded about the shared files (see {@link
   *     #sharedFilesFromEarlierInstalls})
   * @return what activation did to each shared {@code lib/core} file
   */
  private Map<String, SharedFile> activateStaged(
      String artifactId,
      String version,
      List<String> relativePaths,
      Map<String, SharedFile> earlierInstalls)
      throws HopException {
    Path stageRoot = hopHome.resolve(STAGING_DIR).resolve(artifactId);
    Map<String, SharedFile> sharedFiles = new LinkedHashMap<>();
    // Read at activation, not when staging: another plugin activated in between may have created
    // a shared file this one ships too.
    SharedFileHistory history = SharedFileHistory.of(readAllReceipts());
    String installId = newInstallId(artifactId, version);
    try {
      for (String relative : relativePaths) {
        Path from = stageRoot.resolve(relative);
        Path to = activationTarget(relative);
        if (Files.isDirectory(from)) {
          Files.createDirectories(to);
        } else if (Files.isRegularFile(from)) {
          Files.createDirectories(to.getParent());
          if (isSharedCorePath(relative)) {
            String backup = backupPath(artifactId, version, installId, relative);
            SharedFileAction action = activateSharedCoreJar(artifactId, relative, from, to, backup);
            SharedFile earlier = nullSafe(earlierInstalls).get(relative);
            if (action == SharedFileAction.UNCHANGED
                && earlier != null
                && isReusable(earlier.getAction())) {
              // The same version again: the file is as that install left it.
              action = earlier.getAction();
              backup = earlier.getBackup();
            } else if (action != SharedFileAction.REPLACED) {
              backup = null;
            }
            if (action != null) {
              String normalized = normalizeRelative(relative);
              boolean created =
                  action == SharedFileAction.CREATED
                      || (earlier != null && earlier.isCreatedByMarketplace())
                      || history.created(normalized);
              String original =
                  earlier != null && earlier.getOriginalBackup() != null
                      ? earlier.getOriginalBackup()
                      : history.originalBackup(normalized);
              if (original == null && action == SharedFileAction.REPLACED && !created) {
                // This install replaced the copy the client shipped with.
                original = backup;
              }
              sharedFiles.put(relative, new SharedFile(action, backup, created, original));
            }
          } else {
            Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
          }
        }
      }
    } catch (IOException e) {
      throw new HopException("Failed to activate staged plugin " + artifactId, e);
    }
    // Shared files an earlier version created or replaced and this one no longer ships
    for (Map.Entry<String, SharedFile> entry : nullSafe(earlierInstalls).entrySet()) {
      if (entry.getValue() != null
          && entry.getValue().getAction() == SharedFileAction.NOT_SHIPPED
          && !sharedFiles.containsKey(entry.getKey())) {
        sharedFiles.put(entry.getKey(), entry.getValue());
      }
    }
    return sharedFiles;
  }

  /**
   * Where an install of {@code artifactId:version} backs up the shared file it replaces: {@code
   * <artifactId>/<version installed>/<installId>/<path>} under {@link #BACKUP_DIR}.
   */
  static String backupPath(String artifactId, String version, String installId, String relative) {
    return BACKUP_DIR
        + "/"
        + pathSegment(artifactId)
        + "/"
        + pathSegment(version)
        + "/"
        + installId
        + "/"
        + normalizeRelative(relative);
  }

  /**
   * The artifact id and version come from the command line or a receipt and are not validated, so
   * they are reduced to one plain folder name before they go into a backup path.
   */
  static String pathSegment(String value) {
    if (StringUtils.isBlank(value)) {
      return "unknown";
    }
    String segment = value.replaceAll("[^A-Za-z0-9._+-]", "_");
    return segment.matches("\\.+") ? segment.replace('.', '_') : segment;
  }

  /** A backup folder name no earlier install of this version used. */
  private String newInstallId(String artifactId, String version) {
    String base =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssSSS'Z'")
            .withZone(ZoneOffset.UTC)
            .format(Instant.now());
    Path versionDir =
        hopHome.resolve(BACKUP_DIR).resolve(pathSegment(artifactId)).resolve(pathSegment(version));
    String id = base;
    for (int i = 1; Files.exists(versionDir.resolve(id)); i++) {
      id = base + "-" + i;
    }
    return id;
  }

  /**
   * Install shared classpath jars (Maven provided → {@code lib/core}). Missing jars are always
   * copied. Existing jars that match the staged content are skipped. On Windows, existing jars are
   * never overwritten (the running JVM locks {@code lib/core/*} on the system classpath). On other
   * platforms, different content is replaced when possible; copy failures are logged and skipped so
   * the rest of the plugin still activates. A jar that is replaced is first backed up to {@link
   * #backupPath}; when that fails, the existing jar stays.
   *
   * @return what was done, or null when a missing jar could not be written
   */
  private SharedFileAction activateSharedCoreJar(
      String artifactId, String relative, Path from, Path to, String backupRelative)
      throws IOException {
    boolean targetExists = Files.isRegularFile(to);
    Long mismatch = targetExists ? Files.mismatch(from, to) : null;
    boolean differs = targetExists && mismatch != null && mismatch != -1L;

    if (skipSharedCoreCopy(targetExists, mismatch, Const.isWindows())) {
      if (differs) {
        log.logBasic(
            "Leaving existing shared lib/core jar in place (Windows locks system classpath jars): "
                + relative
                + " from "
                + artifactId
                + ". Content differs from the package; stop Hop and replace manually if needed.");
        return SharedFileAction.KEPT;
      }
      return SharedFileAction.UNCHANGED;
    }

    Path backup = null;
    if (differs) {
      log.logBasic(
          "Replacing shared lib/core jar with different content from "
              + artifactId
              + ": "
              + relative);
      backup = hopHome.resolve(backupRelative).toAbsolutePath().normalize();
      if (!backup.startsWith(backupRoot())) {
        log.logError(
            "Not backing up shared lib/core jar "
                + relative
                + " outside "
                + BACKUP_DIR
                + ": "
                + backupRelative
                + " — leaving existing file.");
        return SharedFileAction.KEPT;
      }
      try {
        Files.createDirectories(backup.getParent());
        Files.copy(to, backup, StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException e) {
        log.logError(
            "Could not back up shared lib/core jar "
                + relative
                + " to "
                + backup
                + " — leaving existing file. "
                + e.getMessage());
        removeUnusedBackup(backup);
        return SharedFileAction.KEPT;
      }
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
      if (backup != null) {
        removeUnusedBackup(backup);
      }
      return targetExists ? SharedFileAction.KEPT : null;
    }
    return targetExists ? SharedFileAction.REPLACED : SharedFileAction.CREATED;
  }

  private Path backupRoot() {
    return hopHome.resolve(BACKUP_DIR).toAbsolutePath().normalize();
  }

  /** Remove a backup that was not used, and the folders made for it that are now empty. */
  private void removeUnusedBackup(Path backup) {
    Path root = backupRoot();
    try {
      Files.deleteIfExists(backup);
      for (Path dir = backup.getParent();
          dir != null && dir.startsWith(root) && !dir.equals(root) && Files.isDirectory(dir);
          dir = dir.getParent()) {
        try (var stream = Files.list(dir)) {
          if (stream.findAny().isPresent()) {
            break;
          }
        }
        Files.delete(dir);
      }
    } catch (IOException e) {
      log.logError("Could not remove unused backup " + backup, e);
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

  /**
   * @param checksums receives the SHA-256 of every file entry, keyed by its relative path
   */
  private List<String> unzipAllowedEntries(
      Path zipFile, Path stageRoot, Map<String, String> checksums)
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
        String relative = stageRoot.relativize(resolved).toString().replace('\\', '/');
        if (entry.isDirectory()) {
          Files.createDirectories(resolved);
        } else {
          Files.createDirectories(resolved.getParent());
          MessageDigest digest = sha256();
          try (InputStream in = new DigestInputStream(zip.getInputStream(entry), digest)) {
            Files.copy(in, resolved, StandardCopyOption.REPLACE_EXISTING);
          }
          if (!relative.isBlank()) {
            checksums.put(relative, HexFormat.of().formatHex(digest.digest()));
          }
        }
        if (!relative.isBlank()) {
          paths.add(relative);
        }
      }
    }
    return paths;
  }

  static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      // Every Java platform is required to support SHA-256
      throw new IllegalStateException(e);
    }
  }

  private void writeReceipt(InstallReceipt receipt) throws HopException {
    writeReceipt(hopHome, receipt);
  }

  static void writeReceipt(Path hopHome, InstallReceipt receipt) throws HopException {
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
