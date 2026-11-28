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
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import lombok.AccessLevel;
import lombok.Setter;
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
   * What an install of {@code <artifactId>:<version>} replaced, under {@code
   * <artifactId>/<version>/}: the shared {@code lib/core} files in {@code <installId>/<path>}, and
   * the version installed until then in {@link #ROLLBACK_ZIP}. The plugin scanner skips folders
   * named {@code lib} and only loads {@code .jar} files, so none of it is loaded.
   */
  public static final String BACKUP_DIR = RECEIPTS_DIR + "/backup";

  /**
   * The files of the version an install replaced, as they were on disk, in {@code
   * <BACKUP_DIR>/<artifactId>/<version installed>/}. A failed install puts them back, and so does a
   * rollback. Only the backup of the version that the installed one replaced is kept.
   */
  static final String ROLLBACK_ZIP = "rollback.zip";

  /** The receipt of the version in {@link #ROLLBACK_ZIP}, next to it. */
  static final String ROLLBACK_RECEIPT = "rollback.json";

  private final ILogChannel log;
  private final Path hopHome;
  private final MarketplaceConfig config;
  private final MavenRepositoryClient client;
  private final PendingChanges pending;

  /**
   * Whether a file that cannot be written or removed is in use and changes at the next Hop start
   * instead (see {@link PendingChanges}). Windows keeps the jars of a running JVM locked; elsewhere
   * a failure has another cause (permissions, for example) that a restart does not fix. Tests set
   * it to behave as on Windows, or not.
   */
  @Setter(AccessLevel.PACKAGE)
  private boolean filesLockedWhileInUse = Const.isWindows();

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
    this.pending = new PendingChanges(hopHome);
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
      if (removeLeftovers(already)) {
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

      InstallReceipt receipt =
          installZip(
              coordinates,
              zipFile,
              used.getId(),
              used.normalizedUrl(),
              activateImmediately,
              progress);
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
   * Install a plugin zip that is on disk already: stage it, back up the version installed now when
   * this is another version, then activate it and write the receipt. When activation fails, the
   * backed-up version is put back.
   */
  private InstallReceipt installZip(
      MavenCoordinates coordinates,
      Path zipFile,
      String repositoryId,
      String repositoryUrl,
      boolean activateImmediately,
      IInstallListener progress)
      throws HopException, IOException {
    progress.phase(IInstallListener.Phase.UNZIP, coordinates.artifactId());
    Path stageRoot = hopHome.resolve(STAGING_DIR).resolve(coordinates.artifactId());
    deleteRecursive(stageRoot);
    Files.createDirectories(stageRoot);
    Map<String, String> checksums = new LinkedHashMap<>();
    List<String> relativePaths = unzipAllowedEntries(zipFile, stageRoot, checksums);

    // Read the receipt of the version installed now before the new receipt replaces it: the
    // files only that version has must go, or both versions end up on the classpath.
    InstallReceipt previous = readReceipt(hopHome, coordinates.artifactId());

    Path backup;
    try {
      backup = backUpInstalledVersion(previous, coordinates.version());
    } catch (HopException e) {
      deleteRecursive(stageRoot);
      throw e;
    }

    InstallReceipt receipt = new InstallReceipt();
    receipt.setGroupId(coordinates.groupId());
    receipt.setArtifactId(coordinates.artifactId());
    receipt.setVersion(coordinates.version());
    receipt.setInstalledAt(Instant.now().toString());
    receipt.setRepositoryId(repositoryId);
    receipt.setRepositoryUrl(repositoryUrl);
    receipt.setPaths(relativePaths);
    receipt.setChecksums(checksums);
    receipt.setPreviousVersion(previousVersion(previous, coordinates.version()));
    receipt.setSharedFiles(sharedFilesFromEarlierInstalls(relativePaths, previous, coordinates));
    receipt.setObsoletePaths(
        obsoletePaths(
            previous, relativePaths, pathsOfOtherReceipts(hopHome, coordinates.artifactId(), log)));
    receipt.setPendingActivation(!activateImmediately);

    if (!activateImmediately) {
      writeReceipt(receipt);
      return receipt;
    }
    // What activation did to the shared files, also when it fails half-way.
    Map<String, SharedFile> activated = new LinkedHashMap<>();
    try {
      writeReceipt(receipt);
      progress.phase(IInstallListener.Phase.ACTIVATE, coordinates.artifactId());
      receipt.setSharedFiles(
          activateStaged(
              coordinates.artifactId(),
              coordinates.version(),
              stageRoot,
              relativePaths,
              receipt.getSharedFiles(),
              activated));
      receipt.setPendingActivation(false);
      removeObsoleteFiles(receipt);
      writeReceipt(receipt);
    } catch (HopException | RuntimeException e) {
      if (backup == null) {
        throw e;
      }
      Map<String, SharedFile> attempted = new LinkedHashMap<>(nullSafe(receipt.getSharedFiles()));
      attempted.putAll(activated);
      receipt.setSharedFiles(attempted);
      throw restoreAfterFailedInstall(coordinates, receipt, backup, e);
    }
    deleteRecursive(stageRoot);
    // Only the version this one replaced can be rolled back to.
    pruneRollbackBackups(hopHome, coordinates.artifactId(), coordinates.version(), log);
    return receipt;
  }

  /**
   * Put the backed-up version back after a failed install of another version.
   *
   * @return the exception to throw, which says whether the earlier version is back
   */
  private HopException restoreAfterFailedInstall(
      MavenCoordinates coordinates, InstallReceipt attempted, Path backup, Exception failure) {
    String restoredVersion = "the version installed before";
    try {
      InstallReceipt saved = readRollbackReceipt(backup);
      if (saved != null) {
        restoredVersion = "version " + saved.getVersion();
      }
      restoreBackup(coordinates.artifactId(), backup, attempted);
      deleteRecursive(hopHome.resolve(STAGING_DIR).resolve(coordinates.artifactId()));
      // The restored version is installed again: its copy here is not needed any more.
      try {
        removeRollbackBackup(backup);
      } catch (IOException e) {
        log.logError("Could not remove the backup " + backup + " that is not needed any more", e);
      }
      return new HopException(
          "Failed to install " + coordinates.gav() + ". Hop put " + restoredVersion + " back.",
          failure);
    } catch (Exception restoreFailure) {
      HopException e =
          new HopException(
              "Failed to install "
                  + coordinates.gav()
                  + ", and putting "
                  + restoredVersion
                  + " back failed too. Its files are in "
                  + backup.resolve(ROLLBACK_ZIP)
                  + ".",
              failure);
      e.addSuppressed(restoreFailure);
      return e;
    }
  }

  /**
   * Put back the version the installed one replaced, from the backup its install made. This is an
   * install of that version: the version installed now is backed up in turn, so a second rollback
   * returns to it.
   *
   * @param listener receives the install phases. Pass {@link IInstallListener#NONE} for headless
   *     callers.
   */
  public InstallReceipt rollback(String artifactId, IInstallListener listener) throws HopException {
    IInstallListener progress = listener == null ? IInstallListener.NONE : listener;
    InstallReceipt current = readReceipt(hopHome, artifactId);
    if (current == null
        || current.isUninstallPending()
        || StringUtils.isBlank(current.getVersion())) {
      throw new HopException(
          "No marketplace install of '"
              + artifactId
              + "'. Only plugins installed via the marketplace can be rolled back.");
    }
    if (current.isPendingActivation()) {
      throw new HopException(
          "Version "
              + current.getVersion()
              + " of "
              + artifactId
              + " is staged and not active yet. Restart Hop before rolling it back.");
    }
    Path backup = rollbackDir(hopHome, artifactId, current.getVersion());
    InstallReceipt saved = readRollbackReceipt(backup);
    if (saved == null
        || StringUtils.isBlank(saved.getVersion())
        || !Files.isRegularFile(backup.resolve(ROLLBACK_ZIP))) {
      throw new HopException(
          "There is no earlier version of "
              + artifactId
              + " to roll back to. The marketplace keeps one when it installs another version.");
    }
    String groupId =
        StringUtils.defaultIfBlank(
            StringUtils.firstNonBlank(
                saved.getGroupId(),
                current.getGroupId(),
                config == null ? null : config.getGroupId()),
            "org.apache.hop");
    MavenCoordinates coordinates = new MavenCoordinates(groupId, artifactId, saved.getVersion());
    try {
      InstallReceipt receipt =
          installZip(
              coordinates,
              backup.resolve(ROLLBACK_ZIP),
              saved.getRepositoryId(),
              saved.getRepositoryUrl(),
              true,
              progress);
      log.logBasic(
          "Rolled back "
              + artifactId
              + " from "
              + current.getVersion()
              + " to "
              + saved.getVersion()
              + ". Restart Hop to load it.");
      return receipt;
    } catch (IOException e) {
      throw new HopException("Failed to roll back " + artifactId + " to " + saved.getVersion(), e);
    }
  }

  /**
   * The version a rollback of this install puts back, or null when there is none: the plugin was
   * not installed over another version by the marketplace, or the backup is gone.
   */
  public static String rollbackVersion(Path hopHome, InstallReceipt receipt) {
    if (receipt == null
        || receipt.isPendingActivation()
        || receipt.isUninstallPending()
        || StringUtils.isAnyBlank(receipt.getArtifactId(), receipt.getVersion())) {
      return null;
    }
    Path backup = rollbackDir(hopHome, receipt.getArtifactId(), receipt.getVersion());
    if (!Files.isRegularFile(backup.resolve(ROLLBACK_ZIP))) {
      return null;
    }
    try {
      InstallReceipt saved = readRollbackReceipt(backup);
      return saved == null || StringUtils.isBlank(saved.getVersion()) ? null : saved.getVersion();
    } catch (HopException e) {
      return null;
    }
  }

  /** Where an install of {@code artifactId:version} keeps the version it replaced. */
  static Path rollbackDir(Path hopHome, String artifactId, String version) {
    return hopHome
        .resolve(BACKUP_DIR)
        .resolve(pathSegment(artifactId))
        .resolve(pathSegment(version));
  }

  private static InstallReceipt readRollbackReceipt(Path backup) throws HopException {
    Path file = backup.resolve(ROLLBACK_RECEIPT);
    if (!Files.isRegularFile(file)) {
      return null;
    }
    try (InputStream in = Files.newInputStream(file)) {
      return HopJson.newMapper().readValue(in, InstallReceipt.class);
    } catch (IOException e) {
      throw new HopException("Unable to read the backed-up receipt " + file, e);
    }
  }

  /**
   * Before another version replaces the one installed now, zip that version's files, shared {@code
   * lib/core} jars included, into {@link #ROLLBACK_ZIP}, with its receipt. A file that the next Hop
   * start replaces (it was in use when that version was installed, see {@link PendingChanges}) is
   * taken from the queued copy: the one on disk still belongs to the version before.
   *
   * @return the backup folder, or null when there is no other version to back up: a first install,
   *     the same version again, or a version that was staged and never activated
   */
  private Path backUpInstalledVersion(InstallReceipt installed, String newVersion)
      throws HopException {
    if (installed == null
        || installed.isPendingActivation()
        || installed.isUninstallPending()
        || StringUtils.isBlank(installed.getVersion())
        || installed.getVersion().equals(newVersion)) {
      return null;
    }
    String artifactId = installed.getArtifactId();
    Path dir = rollbackDir(hopHome, artifactId, newVersion);
    Path zip = dir.resolve(ROLLBACK_ZIP);
    Path json = dir.resolve(ROLLBACK_RECEIPT);
    Path zipTmp = dir.resolve(ROLLBACK_ZIP + ".tmp");
    Path jsonTmp = dir.resolve(ROLLBACK_RECEIPT + ".tmp");
    Path home = hopHome.toAbsolutePath().normalize();
    List<String> missing = new ArrayList<>();
    try {
      Map<String, PendingChanges.Change> queued = pendingChangesByTarget();
      Files.createDirectories(dir);
      try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zipTmp))) {
        // Jars are compressed already.
        out.setLevel(Deflater.BEST_SPEED);
        Set<String> written = new HashSet<>();
        for (String path : nullSafe(installed.getPaths())) {
          if (StringUtils.isBlank(path)) {
            continue;
          }
          String relative = normalizeRelative(path);
          if (!hopHome.resolve(relative).toAbsolutePath().normalize().startsWith(home)
              || !written.add(relative)) {
            continue;
          }
          Path source = activationTarget(relative);
          PendingChanges.Change change = queued.get(relativeToHome(source));
          if (change != null) {
            // Removed at the next start, or replaced by the queued copy.
            source =
                change.action() == PendingChanges.Action.COPY
                    ? hopHome.resolve(change.source())
                    : null;
          }
          if (source != null && Files.isDirectory(source)) {
            out.putNextEntry(new ZipEntry(relative + "/"));
            out.closeEntry();
          } else if (source != null && Files.isRegularFile(source)) {
            out.putNextEntry(new ZipEntry(relative));
            Files.copy(source, out);
            out.closeEntry();
          } else {
            missing.add(relative);
          }
        }
      }
      try (OutputStream out = Files.newOutputStream(jsonTmp)) {
        HopJson.newMapper().writerWithDefaultPrettyPrinter().writeValue(out, installed);
      }
      // A zip without its receipt is not a backup, so a crash in between leaves none rather than a
      // receipt of another version next to this zip.
      Files.deleteIfExists(json);
      Files.move(zipTmp, zip, StandardCopyOption.REPLACE_EXISTING);
      Files.move(jsonTmp, json, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      try {
        Files.deleteIfExists(zipTmp);
        Files.deleteIfExists(jsonTmp);
      } catch (IOException cleanup) {
        e.addSuppressed(cleanup);
      }
      throw new HopException(
          "Could not back up version "
              + installed.getVersion()
              + " of "
              + artifactId
              + " before installing "
              + newVersion
              + ". Nothing was changed.",
          e);
    }
    if (!missing.isEmpty()) {
      log.logError(
          "Version "
              + installed.getVersion()
              + " of "
              + artifactId
              + " is missing files, so a rollback cannot put them back: "
              + String.join(", ", missing));
    }
    log.logBasic(
        "Backed up version " + installed.getVersion() + " of " + artifactId + " to " + zip);
    return dir;
  }

  /** The changes the next Hop start makes, by target (relative to the Hop installation). */
  private Map<String, PendingChanges.Change> pendingChangesByTarget() throws HopException {
    Map<String, PendingChanges.Change> changes = new HashMap<>();
    for (PendingChanges.Change change : pending.read()) {
      changes.put(change.target(), change);
    }
    return changes;
  }

  /** {@code path} relative to the Hop installation with {@code /}, or null when outside it. */
  private String relativeToHome(Path path) {
    Path home = hopHome.toAbsolutePath().normalize();
    Path absolute = path.toAbsolutePath().normalize();
    return absolute.startsWith(home)
        ? home.relativize(absolute).toString().replace('\\', '/')
        : null;
  }

  /**
   * Put a backed-up version back: its files and its receipt. The files only the attempted version
   * has are removed.
   */
  private void restoreBackup(String artifactId, Path backup, InstallReceipt attempted)
      throws HopException, IOException {
    InstallReceipt saved = readRollbackReceipt(backup);
    if (saved == null) {
      throw new HopException("The backup in " + backup + " has no " + ROLLBACK_RECEIPT);
    }
    // A folder name starting with a dot is never activated as a staged plugin.
    Path stage = hopHome.resolve(STAGING_DIR).resolve(".restore").resolve(artifactId);
    deleteRecursive(stage);
    Files.createDirectories(stage);
    try {
      List<String> paths =
          unzipAllowedEntries(backup.resolve(ROLLBACK_ZIP), stage, new LinkedHashMap<>());
      // What putting the shared files back did, the backups of the attempted version's copies
      // included: without it a shared file the attempted version created looks like the client's.
      saved.setSharedFiles(
          activateStaged(
              artifactId,
              saved.getVersion(),
              stage,
              paths,
              sharedFilesForRestore(saved, attempted),
              new LinkedHashMap<>()));
      removeUnreferencedSharedBackups(artifactId, saved, attempted);
      Set<String> obsolete = new LinkedHashSet<>(nullSafe(saved.getObsoletePaths()));
      obsolete.addAll(
          obsoletePaths(
              attempted,
              nullSafe(saved.getPaths()),
              pathsOfOtherReceipts(hopHome, artifactId, log)));
      saved.setObsoletePaths(new ArrayList<>(obsolete));
      removeObsoleteFiles(saved);
      writeReceipt(saved);
    } finally {
      deleteRecursive(stage);
      deleteIfEmpty(stage.getParent());
    }
    log.logBasic("Put version " + saved.getVersion() + " of " + artifactId + " back in place.");
  }

  /**
   * The copies of shared files the attempted version replaced, that nothing refers to now that
   * another version is back: the restored version's receipt and every other receipt record their
   * own backups. Best effort.
   */
  private void removeUnreferencedSharedBackups(
      String artifactId, InstallReceipt restored, InstallReceipt attempted) {
    Set<String> referenced = new HashSet<>();
    List<InstallReceipt> receipts = new ArrayList<>(List.of(restored));
    try {
      for (InstallReceipt receipt : readAllReceipts()) {
        if (!artifactId.equals(receipt.getArtifactId())) {
          receipts.add(receipt);
        }
      }
    } catch (HopException e) {
      log.logError("Could not read the install receipts; keeping the shared file backups", e);
      return;
    }
    for (InstallReceipt receipt : receipts) {
      for (SharedFile file : nullSafe(receipt.getSharedFiles()).values()) {
        if (file != null) {
          referenced.add(file.getBackup());
          referenced.add(file.getOriginalBackup());
        }
      }
    }
    for (SharedFile file : nullSafe(attempted.getSharedFiles()).values()) {
      if (file != null && file.getBackup() != null && !referenced.contains(file.getBackup())) {
        Path backup = hopHome.resolve(file.getBackup()).toAbsolutePath().normalize();
        if (backup.startsWith(backupRoot())) {
          removeUnusedBackup(backup);
        }
      }
    }
  }

  /**
   * What the restored version's receipt recorded about its shared files, plus the shared files the
   * attempted version created or replaced that the restored version does not ship, while they are
   * on disk.
   */
  private Map<String, SharedFile> sharedFilesForRestore(
      InstallReceipt saved, InstallReceipt attempted) {
    Map<String, SharedFile> files = new LinkedHashMap<>();
    for (Map.Entry<String, SharedFile> entry : nullSafe(saved.getSharedFiles()).entrySet()) {
      if (entry.getValue() != null) {
        files.put(normalizeRelative(entry.getKey()), entry.getValue());
      }
    }
    for (Map.Entry<String, SharedFile> entry : nullSafe(attempted.getSharedFiles()).entrySet()) {
      SharedFile file = entry.getValue();
      String path = normalizeRelative(entry.getKey());
      if (file == null
          || files.containsKey(path)
          || !(file.isCreatedByMarketplace() || file.getOriginalBackup() != null)
          || !Files.exists(activationTarget(path))) {
        continue;
      }
      files.put(
          path,
          new SharedFile(
              SharedFileAction.NOT_SHIPPED,
              null,
              file.isCreatedByMarketplace(),
              file.getOriginalBackup()));
    }
    return files;
  }

  /**
   * Remove the backed-up versions of {@code artifactId}, except the one that {@code keepVersion}
   * replaced. The shared {@code lib/core} backups stay. Best effort.
   *
   * @param keepVersion the version installed now, or null to remove every backed-up version
   */
  static void pruneRollbackBackups(
      Path hopHome, String artifactId, String keepVersion, ILogChannel log) {
    Path artifactDir = hopHome.resolve(BACKUP_DIR).resolve(pathSegment(artifactId));
    if (!Files.isDirectory(artifactDir)) {
      return;
    }
    String keep = keepVersion == null ? null : pathSegment(keepVersion);
    try (var stream = Files.list(artifactDir)) {
      for (Path versionDir : stream.filter(Files::isDirectory).toList()) {
        if (!versionDir.getFileName().toString().equals(keep)) {
          removeRollbackBackup(versionDir);
        }
      }
      deleteIfEmpty(artifactDir);
    } catch (IOException e) {
      log.logError("Could not remove the older backed-up versions of " + artifactId, e);
    }
  }

  /** Remove one backed-up version, and its folder when nothing else is in it. */
  private static void removeRollbackBackup(Path versionDir) throws IOException {
    // The receipt goes first: a zip without it is no backup.
    for (String name :
        List.of(ROLLBACK_RECEIPT, ROLLBACK_ZIP, ROLLBACK_RECEIPT + ".tmp", ROLLBACK_ZIP + ".tmp")) {
      Files.deleteIfExists(versionDir.resolve(name));
    }
    deleteIfEmpty(versionDir);
  }

  private static void deleteIfEmpty(Path dir) throws IOException {
    try (var stream = Files.list(dir)) {
      if (stream.findAny().isPresent()) {
        return;
      }
    }
    Files.delete(dir);
  }

  /**
   * A repeated install can skip the download when the receipt names this version and every file it
   * recorded is still on disk.
   */
  private InstallReceipt satisfiedInstall(MavenCoordinates coordinates) throws HopException {
    InstallReceipt receipt = readReceipt(hopHome, coordinates.artifactId());
    if (receipt == null || receipt.isPendingActivation() || receipt.isUninstallPending()) {
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
    return receiptFilesPresent(hopHome, receipt);
  }

  /**
   * Whether every file the receipt lists is on disk, so installing the same version again would not
   * download anything. When a file is missing, that install repairs the plugin.
   */
  public static boolean receiptFilesPresent(Path hopHome, InstallReceipt receipt) {
    List<String> paths = receipt == null ? null : receipt.getPaths();
    if (paths == null || paths.isEmpty()) {
      return false;
    }
    boolean file = false;
    for (String relative : paths) {
      if (StringUtils.isBlank(relative)) {
        continue;
      }
      Path path = activationTarget(hopHome, relative);
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
            warnAboutFilesTheStagedVersionDoesNotHave(artifactId, paths);
          }
          Map<String, SharedFile> sharedFiles =
              activateStaged(
                  artifactId,
                  receipt == null || StringUtils.isBlank(receipt.getVersion())
                      ? "unknown"
                      : receipt.getVersion(),
                  staged,
                  paths,
                  receipt == null ? Map.of() : receipt.getSharedFiles(),
                  new LinkedHashMap<>());
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

  /**
   * Without the receipt, the files of the version installed until now are unknown, so they cannot
   * be removed. Plugin folders can also hold files people added by hand (JDBC drivers, for
   * example), so nothing is removed by guessing: the files the staged version does not have are
   * reported instead.
   */
  private void warnAboutFilesTheStagedVersionDoesNotHave(String artifactId, List<String> paths) {
    List<String> unknown = filesTheStagedVersionDoesNotHave(artifactId, paths);
    if (!unknown.isEmpty()) {
      log.logError(
          "The plugin folder of "
              + artifactId
              + " holds files the version being activated does not have. They may be files of"
              + " the previous version, which could then load next to the new one: check and"
              + " remove them by hand, or install the plugin again. "
              + String.join(", ", unknown));
    }
  }

  /**
   * The files in the top-level plugin folders ({@code plugins/<type>/<name>}) of the staged paths
   * that are not staged themselves and that no other receipt lists.
   */
  List<String> filesTheStagedVersionDoesNotHave(String artifactId, List<String> paths) {
    Set<String> staged = new HashSet<>();
    Set<String> pluginFolders = new LinkedHashSet<>();
    for (String path : paths) {
      String normalized = normalizeRelative(path);
      staged.add(normalized);
      String[] segments = normalized.split("/");
      if (segments.length > 3 && segments[0].equals("plugins") && !segments[1].startsWith(".")) {
        pluginFolders.add(String.join("/", segments[0], segments[1], segments[2]));
      }
    }
    Set<String> otherReceiptPaths;
    try {
      otherReceiptPaths = pathsOfOtherReceipts(hopHome, artifactId, log);
    } catch (HopException e) {
      log.logError("Could not read the other marketplace install receipts", e);
      otherReceiptPaths = Set.of();
    }
    List<String> unknown = new ArrayList<>();
    for (String folder : pluginFolders) {
      Path dir = hopHome.resolve(folder);
      if (!Files.isDirectory(dir)) {
        continue;
      }
      try (var stream = Files.walk(dir)) {
        for (Path file : stream.filter(Files::isRegularFile).toList()) {
          String relative = normalizeRelative(hopHome.relativize(file).toString());
          if (!staged.contains(relative) && !otherReceiptPaths.contains(relative)) {
            unknown.add(relative);
          }
        }
      } catch (IOException e) {
        log.logError("Could not list the plugin folder " + dir, e);
      }
    }
    Collections.sort(unknown);
    return unknown;
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
   * are not among them: the receipt lists those as {@link SharedFileAction#NOT_SHIPPED} instead,
   * see {@link #removeUnusedSharedFiles}. Every path another marketplace receipt lists stays too.
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
   * cannot be deleted stays in the receipt so a later install, apply or uninstall removes it; on
   * Windows, where that means the file is in use, Hop also removes it the next time it starts. A
   * file that another receipt lists by now, or that is one of the current version's files under a
   * differently cased name, is no longer obsolete and is kept. Shared jars the receipt's version no
   * longer ships go too when no installed plugin needs them.
   */
  private void removeObsoleteFiles(InstallReceipt receipt) throws HopException {
    removeSharedFilesNoLongerShipped(receipt);
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
      } else if (!Files.exists(path)) {
        // Removed in the meantime, by the Hop launcher for example: its folder may be empty now.
        if (path.getParent() != null) {
          dirs.add(path.getParent());
        }
      } else {
        try {
          Files.delete(path);
          forgetPendingChange(path);
          log.logBasic(
              "Removed " + relative + " of the previous version of " + receipt.getArtifactId());
          if (path.getParent() != null) {
            dirs.add(path.getParent());
          }
        } catch (IOException e) {
          if (filesLockedWhileInUse && queueDelete(pending, log, path)) {
            log.logBasic(
                relative
                    + " of the previous version of "
                    + receipt.getArtifactId()
                    + " is in use: Hop removes it the next time it starts.");
          } else {
            log.logError(
                "Could not remove "
                    + relative
                    + " of the previous version of "
                    + receipt.getArtifactId()
                    + " (file in use?). Close Hop and run 'hop marketplace install' or"
                    + " 'hop marketplace apply', or uninstall the plugin, to remove it. "
                    + e.getMessage());
          }
          remaining.add(relative);
        }
      }
    }
    PluginUninstaller.removeEmptyPluginDirectories(hopHome, dirs, log);
    receipt.setObsoletePaths(remaining);
  }

  /**
   * Retry what an earlier install of an active version could not remove: obsolete files, and shared
   * jars it no longer ships.
   *
   * @return whether the receipt changed and has to be written
   */
  private boolean removeLeftovers(InstallReceipt receipt) throws HopException {
    int obsolete = nullSafe(receipt.getObsoletePaths()).size();
    int notShipped = notShippedSharedFiles(receipt).size();
    if (obsolete == 0 && notShipped == 0) {
      return false;
    }
    removeObsoleteFiles(receipt);
    return nullSafe(receipt.getObsoletePaths()).size() != obsolete
        || notShippedSharedFiles(receipt).size() != notShipped;
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
      if (receipt.isUninstallPending()) {
        try {
          new PluginUninstaller(log, hopHome, filesLockedWhileInUse)
              .uninstall(receipt.getArtifactId());
        } catch (HopException e) {
          log.logError("Could not finish uninstalling " + receipt.getArtifactId(), e);
        }
        continue;
      }
      if (receipt.isPendingActivation()) {
        continue;
      }
      try {
        if (removeLeftovers(receipt)) {
          writeReceipt(receipt);
        }
      } catch (HopException e) {
        log.logError(
            "Could not remove the leftover files of the previous version of "
                + receipt.getArtifactId(),
            e);
      }
    }
  }

  /**
   * Remove the shared jars of the receipt that the version it installs no longer ships ({@link
   * SharedFileAction#NOT_SHIPPED}) when no installed plugin needs them any more, see {@link
   * #removeUnusedSharedFiles}. Their entries leave the receipt; a jar that stays keeps its entry,
   * so a later install, apply or uninstall tries again.
   */
  private void removeSharedFilesNoLongerShipped(InstallReceipt receipt) {
    List<String> notShipped = notShippedSharedFiles(receipt);
    if (notShipped.isEmpty()) {
      return;
    }
    Set<String> removed =
        removeUnusedSharedFiles(hopHome, receipt, notShipped, pending, filesLockedWhileInUse, log)
            .removed();
    if (removed.isEmpty()) {
      return;
    }
    Map<String, SharedFile> kept = new LinkedHashMap<>();
    for (Map.Entry<String, SharedFile> entry : nullSafe(receipt.getSharedFiles()).entrySet()) {
      if (!removed.contains(normalizeRelative(entry.getKey()))) {
        kept.put(entry.getKey(), entry.getValue());
      }
    }
    receipt.setSharedFiles(kept);
  }

  /** The shared files of the receipt that the version it installs does not ship. */
  static List<String> notShippedSharedFiles(InstallReceipt receipt) {
    List<String> paths = new ArrayList<>();
    for (Map.Entry<String, SharedFile> entry : nullSafe(receipt.getSharedFiles()).entrySet()) {
      if (entry.getKey() != null
          && entry.getValue() != null
          && entry.getValue().getAction() == SharedFileAction.NOT_SHIPPED) {
        paths.add(normalizeRelative(entry.getKey()));
      }
    }
    return paths;
  }

  /**
   * Remove the shared {@code lib/core} jars among {@code candidates} that no installed plugin needs
   * any more: a receipt records that the marketplace created the jar, and no receipt other than the
   * one of {@code receipt}'s artifact lists it. Jars the client shipped with, or that only receipts
   * without that record list, stay. When a receipt cannot be read, every jar stays: it may list
   * them. On Windows a jar in use is removed the next time Hop starts.
   *
   * @param receipt the receipt of the plugin that stops using the jars, as it is in memory
   */
  static SharedFileRemoval removeUnusedSharedFiles(
      Path hopHome,
      InstallReceipt receipt,
      Collection<String> candidates,
      PendingChanges pending,
      boolean filesLockedWhileInUse,
      ILogChannel log) {
    SharedFileRemoval result = new SharedFileRemoval(new LinkedHashSet<>(), new LinkedHashSet<>());
    Set<String> removed = result.removed();
    List<InstallReceipt> others = otherReceiptsOrNull(hopHome, receipt.getArtifactId(), log);
    if (others == null) {
      return result;
    }
    Set<String> listedByOthers = new HashSet<>();
    for (InstallReceipt other : others) {
      for (String path : nullSafe(other.getPaths())) {
        if (StringUtils.isNotBlank(path)) {
          listedByOthers.add(normalizeRelative(path));
        }
      }
    }
    List<InstallReceipt> all = new ArrayList<>(others);
    all.add(receipt);
    SharedFileHistory history = SharedFileHistory.of(all);
    for (String candidate : new LinkedHashSet<>(candidates)) {
      if (StringUtils.isBlank(candidate) || candidate.endsWith("/")) {
        continue;
      }
      String relative = normalizeRelative(candidate);
      if (!isSharedCorePath(relative)
          || relative.equals("lib/core")
          || !history.created(relative)
          || listedByOthers.contains(relative)) {
        continue;
      }
      Path target = activationTarget(hopHome, relative);
      if (!Files.exists(target)) {
        // A change left for the next start would create it.
        forgetPendingChange(pending, log, target);
        removed.add(relative);
        continue;
      }
      if (!Files.isRegularFile(target)) {
        continue;
      }
      try {
        Files.delete(target);
        forgetPendingChange(pending, log, target);
        removed.add(relative);
        log.logBasic(
            "Removed shared jar " + relative + ": no installed marketplace plugin needs it");
      } catch (IOException e) {
        if (filesLockedWhileInUse && queueDelete(pending, log, target)) {
          removed.add(relative);
          log.logBasic(
              "Shared jar "
                  + relative
                  + " is no longer needed and in use: Hop removes it the next time it starts.");
        } else {
          result.failed().add(relative);
          log.logError(
              "Could not remove shared jar "
                  + relative
                  + " that no installed marketplace plugin needs any more. The next install,"
                  + " apply or uninstall tries again. "
                  + e.getMessage());
        }
      }
    }
    return result;
  }

  /**
   * What {@link #removeUnusedSharedFiles} did.
   *
   * @param removed the shared jars that are gone, or go the next time Hop starts
   * @param failed the shared jars that no plugin needs but could not be removed
   */
  record SharedFileRemoval(Set<String> removed, Set<String> failed) {}

  /**
   * The receipts of every other marketplace artifact, or null when one of them cannot be read: what
   * it lists is unknown then.
   */
  private static List<InstallReceipt> otherReceiptsOrNull(
      Path hopHome, String artifactId, ILogChannel log) {
    Path dir = hopHome.resolve(RECEIPTS_DIR);
    List<InstallReceipt> receipts = new ArrayList<>();
    if (!Files.isDirectory(dir)) {
      return receipts;
    }
    try (var stream = Files.list(dir)) {
      for (Path file : stream.filter(Files::isRegularFile).toList()) {
        String name = file.getFileName().toString();
        if (!name.endsWith(".json") || name.equals(artifactId + ".json")) {
          continue;
        }
        InstallReceipt other = readReceipt(hopHome, name.substring(0, name.length() - 5));
        if (other != null) {
          receipts.add(other);
        }
      }
    } catch (IOException | HopException e) {
      log.logError(
          "Keeping the shared lib/core jars: not every marketplace install receipt can be read", e);
      return null;
    }
    return receipts;
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

  /** Every readable marketplace install receipt. An unreadable one is logged and skipped. */
  public static List<InstallReceipt> readAllReceipts(Path hopHome, ILogChannel log)
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
   * @param stageRoot the folder the files were staged in
   * @param earlierInstalls what earlier installs recorded about the shared files (see {@link
   *     #sharedFilesFromEarlierInstalls})
   * @param sharedFiles receives what activation did to each shared {@code lib/core} file, as it
   *     goes, so a caller knows what was done when activation fails half-way
   * @return {@code sharedFiles}
   */
  private Map<String, SharedFile> activateStaged(
      String artifactId,
      String version,
      Path stageRoot,
      List<String> relativePaths,
      Map<String, SharedFile> earlierInstalls,
      Map<String, SharedFile> sharedFiles)
      throws HopException {
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
            activatePluginFile(relative, from, to);
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
   * Copy one plugin file into place. On Windows a file in use cannot be overwritten: unless it
   * already has the new content, Hop replaces it the next time it starts.
   */
  private void activatePluginFile(String relative, Path from, Path to) throws IOException {
    try {
      Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      if (!filesLockedWhileInUse || !Files.isRegularFile(to)) {
        throw e;
      }
      if (Files.mismatch(from, to) != -1L) {
        if (!queueCopy(from, to)) {
          throw e;
        }
        log.logBasic(relative + " is in use: Hop replaces it the next time it starts.");
        return;
      }
    }
    // A change queued by an earlier install would undo this one at the next start.
    forgetPendingChange(to);
  }

  /**
   * Install shared classpath jars (Maven provided → {@code lib/core}). Missing jars are always
   * copied. Existing jars that match the staged content are left alone. Different content is
   * replaced; the existing jar is first backed up to {@link #backupPath}, and when that fails it
   * stays. On Windows the running JVM locks the jars on the system classpath, so Hop replaces an
   * existing jar the next time it starts (see {@link PendingChanges}). On other platforms a copy
   * that fails is logged and skipped, so the rest of the plugin still activates.
   *
   * @return what was done, or null when a missing jar could not be written
   */
  private SharedFileAction activateSharedCoreJar(
      String artifactId, String relative, Path from, Path to, String backupRelative)
      throws IOException {
    boolean targetExists = Files.isRegularFile(to);
    if (targetExists && Files.mismatch(from, to) == -1L) {
      // A change queued by an earlier install would undo this one at the next start.
      forgetPendingChange(to);
      return SharedFileAction.UNCHANGED;
    }

    Path backup = null;
    if (targetExists) {
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
    SharedFileAction done = targetExists ? SharedFileAction.REPLACED : SharedFileAction.CREATED;
    // On Windows, overwriting a jar on the system classpath fails: do not even try.
    if (!(targetExists && filesLockedWhileInUse)) {
      try {
        Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING);
        forgetPendingChange(to);
        return done;
      } catch (IOException e) {
        if (!filesLockedWhileInUse) {
          // Permissions, a full disk, antivirus... — do not fail the whole plugin install.
          log.logError(
              "Could not write shared lib/core jar: "
                  + relative
                  + " — leaving existing file. "
                  + e.getMessage());
          if (backup != null) {
            removeUnusedBackup(backup);
          }
          return targetExists ? SharedFileAction.KEPT : null;
        }
      }
    }
    if (queueCopy(from, to)) {
      log.logBasic(
          "Shared lib/core jar "
              + relative
              + " from "
              + artifactId
              + " is in use: Hop "
              + (targetExists ? "replaces" : "adds")
              + " it the next time it starts.");
      return done;
    }
    log.logError("Leaving shared lib/core jar " + relative + " as it is.");
    if (backup != null) {
      removeUnusedBackup(backup);
    }
    return targetExists ? SharedFileAction.KEPT : null;
  }

  /**
   * Copy {@code from} over {@code to} the next time Hop starts. False when that is not possible.
   */
  private boolean queueCopy(Path from, Path to) {
    try {
      pending.queueCopy(from, to);
      return true;
    } catch (HopException e) {
      log.logError("Could not leave the change to " + to + " for the next Hop start", e);
      return false;
    }
  }

  /** Delete {@code path} the next time Hop starts. False when that is not possible. */
  static boolean queueDelete(PendingChanges pending, ILogChannel log, Path path) {
    try {
      pending.queueDelete(path);
      return true;
    } catch (HopException e) {
      log.logError("Could not leave the removal of " + path + " for the next Hop start", e);
      return false;
    }
  }

  private void forgetPendingChange(Path target) {
    forgetPendingChange(pending, log, target);
  }

  /**
   * Drop a change an earlier install left for the next start: the file is as it should be now, and
   * the change would undo that.
   */
  static void forgetPendingChange(PendingChanges pending, ILogChannel log, Path target) {
    try {
      pending.forget(target);
    } catch (HopException e) {
      log.logError(
          "Could not drop the pending change to "
              + target
              + ". Check "
              + PendingChanges.PENDING_FILE
              + " before Hop starts again.",
          e);
    }
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
   * same as the full client. Uninstall and update remove one only when the marketplace created it
   * and no other installed plugin lists it, see {@link #removeUnusedSharedFiles}.
   */
  static boolean isSharedCorePath(String relative) {
    String normalized = relative.replace('\\', '/');
    return normalized.equals("lib/core") || normalized.startsWith("lib/core/");
  }

  private Path activationTarget(String relative) {
    return activationTarget(hopHome, relative);
  }

  /** Web layout keeps shared jars in {@code WEB-INF/lib} instead of {@code lib/core}. */
  private static Path activationTarget(Path hopHome, String relative) {
    Path standard = hopHome.resolve(relative);
    String normalized = relative == null ? "" : relative.replace('\\', '/');
    if (!normalized.startsWith("lib/core/") || normalized.endsWith("/")) {
      return standard;
    }
    Path webLib = webInfLib(hopHome);
    if (webLib == null) {
      return standard;
    }
    Path name = Path.of(normalized).getFileName();
    return name == null ? standard : webLib.resolve(name);
  }

  private static Path webInfLib(Path hopHome) {
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
