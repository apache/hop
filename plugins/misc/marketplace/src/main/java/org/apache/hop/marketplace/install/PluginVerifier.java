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

package org.apache.hop.marketplace.install;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.marketplace.install.InstallReceipt.SharedFile;
import org.apache.hop.marketplace.install.InstallReceipt.SharedFileAction;

/**
 * Compares the files of marketplace installs with their receipts: which files are missing, and
 * which no longer have the content the install wrote. It changes nothing.
 *
 * <p>A shared {@code lib/core} jar is not reported as modified when the difference is expected: the
 * install kept the copy that was there ({@link SharedFileAction#KEPT}), or another marketplace
 * install wrote the jar that is there now. A change the next Hop start makes (see {@link
 * PendingChanges}) counts as made.
 */
public class PluginVerifier {

  /** What verify found out about one file. */
  public enum Finding {
    /** A file or folder of the install is not on disk. */
    MISSING(true),
    /** A file no longer has the content the install wrote. */
    MODIFIED(true),
    /** A shared {@code lib/core} jar that another marketplace install wrote since. */
    SHARED_BY_OTHER(false),
    /** A shared {@code lib/core} jar the install left as it was, as its receipt says. */
    SHARED_KEPT(false),
    /** A file of the version this one replaced, that is removed later (it was in use). */
    OBSOLETE(false),
    /** A file in the plugin's folder that no marketplace receipt lists. */
    UNKNOWN(false);

    private final boolean damage;

    Finding(boolean damage) {
      this.damage = damage;
    }

    /** Whether this finding makes the install damaged: a reinstall puts it right. */
    public boolean isDamage() {
      return damage;
    }
  }

  /**
   * One file verify reports.
   *
   * @param path relative to the Hop installation, as the receipt lists it
   * @param detail for {@link Finding#SHARED_BY_OTHER}, the artifact that wrote the jar; for a file
   *     that cannot be read, why; else null
   */
  public record FileFinding(String path, Finding finding, String detail) {}

  /** The state of one marketplace install. */
  public enum State {
    /** Every file of the install is there, with the content the install wrote. */
    OK,
    /** Files are missing or modified. */
    DAMAGED,
    /** The receipt cannot be read. */
    UNREADABLE,
    /** The install is staged and becomes active the next time Hop starts. Not checked. */
    PENDING_ACTIVATION,
    /** An uninstall could not remove every file; the next install, apply or uninstall does. */
    UNINSTALL_PENDING
  }

  /**
   * What verify found for one install.
   *
   * @param version the version of the receipt, or null when it cannot be read
   * @param withoutChecksums the receipt has no checksums (Hop before 2.21 wrote it), so only the
   *     presence of the files was checked
   * @param error why the receipt cannot be read, for {@link State#UNREADABLE}
   */
  public record Result(
      String artifactId,
      String version,
      State state,
      boolean withoutChecksums,
      List<FileFinding> files,
      String error) {

    /** Whether a reinstall is needed: files are missing or modified. */
    public boolean isDamaged() {
      return state == State.DAMAGED;
    }

    /** The files with this finding. */
    public List<String> paths(Finding finding) {
      return files.stream().filter(f -> f.finding() == finding).map(FileFinding::path).toList();
    }
  }

  private final ILogChannel log;
  private final Path hopHome;

  public PluginVerifier(ILogChannel log, Path hopHome) {
    this.log = log;
    this.hopHome = hopHome;
  }

  /** Verify every marketplace install, unreadable receipts included, by artifact id. */
  public List<Result> verifyAll() throws HopException {
    Path dir = hopHome.resolve(PluginInstaller.RECEIPTS_DIR);
    List<String> artifactIds = new ArrayList<>();
    if (Files.isDirectory(dir)) {
      try (var stream = Files.list(dir)) {
        for (Path file : stream.filter(Files::isRegularFile).toList()) {
          String name = file.getFileName().toString();
          if (name.endsWith(".json")) {
            artifactIds.add(name.substring(0, name.length() - ".json".length()));
          }
        }
      } catch (IOException e) {
        throw new HopException("Unable to list marketplace install receipts in " + dir, e);
      }
    }
    artifactIds.sort(null);
    List<Result> results = new ArrayList<>();
    for (String artifactId : artifactIds) {
      results.add(verify(artifactId));
    }
    return results;
  }

  /**
   * Verify one marketplace install.
   *
   * @throws HopException when the plugin was not installed via the marketplace
   */
  public Result verify(String artifactId) throws HopException {
    InstallReceipt receipt;
    try {
      receipt = PluginInstaller.readReceipt(hopHome, artifactId);
    } catch (HopException e) {
      return new Result(
          artifactId, null, State.UNREADABLE, false, List.of(), rootMessage(e, artifactId));
    }
    if (receipt == null) {
      throw new HopException(
          "No marketplace install of '"
              + artifactId
              + "'. Only plugins installed via the marketplace have a receipt to verify against.");
    }
    if (receipt.isUninstallPending()) {
      return new Result(
          artifactId, null, State.UNINSTALL_PENDING, false, obsoleteFiles(receipt), null);
    }
    if (receipt.isPendingActivation()) {
      return new Result(
          artifactId, receipt.getVersion(), State.PENDING_ACTIVATION, false, List.of(), null);
    }
    List<InstallReceipt> others = otherReceipts(artifactId);
    Map<String, PendingChanges.Change> pending = pendingChangesByTarget();
    Map<String, String> checksums = normalizedKeys(receipt.getChecksums());
    Map<String, SharedFile> sharedFiles = normalizedKeys(receipt.getSharedFiles());
    boolean withoutChecksums = checksums.isEmpty();

    List<FileFinding> files = new ArrayList<>();
    Set<String> listed = new LinkedHashSet<>();
    Path home = hopHome.toAbsolutePath().normalize();
    for (String path : PluginInstaller.nullSafe(receipt.getPaths())) {
      if (StringUtils.isBlank(path)) {
        continue;
      }
      String relative = PluginInstaller.normalizeRelative(path);
      // A path that leaves the installation is not one an install wrote. The target can: with
      // the Hop Web layout, shared jars go to the WEB-INF/lib of the working folder.
      if (!hopHome.resolve(relative).toAbsolutePath().normalize().startsWith(home)
          || !listed.add(relative)) {
        continue;
      }
      Path target =
          PluginInstaller.activationTarget(hopHome, relative).toAbsolutePath().normalize();
      // What is there after the next Hop start, which applies the changes that waited for it
      Path content = target;
      PendingChanges.Change change =
          pending.get(home.relativize(target).toString().replace('\\', '/'));
      if (change != null) {
        content =
            change.action() == PendingChanges.Action.COPY ? hopHome.resolve(change.source()) : null;
      }
      String expected = checksums.get(relative);
      if (expected == null) {
        // A folder, or a file of a receipt without checksums
        if (content == null || !Files.exists(content)) {
          files.add(new FileFinding(relative, Finding.MISSING, null));
        }
        continue;
      }
      if (content == null || !Files.isRegularFile(content)) {
        files.add(new FileFinding(relative, Finding.MISSING, null));
        continue;
      }
      String actual;
      try {
        actual = sha256(content);
      } catch (IOException e) {
        files.add(new FileFinding(relative, Finding.MODIFIED, "cannot be read: " + e.getMessage()));
        continue;
      }
      if (expected.equalsIgnoreCase(actual)) {
        continue;
      }
      if (PluginInstaller.isSharedCorePath(relative)) {
        SharedFile shared = sharedFiles.get(relative);
        if (shared != null && shared.getAction() == SharedFileAction.KEPT) {
          files.add(new FileFinding(relative, Finding.SHARED_KEPT, null));
          continue;
        }
        String writer = otherWriter(others, relative, actual);
        if (writer != null) {
          files.add(new FileFinding(relative, Finding.SHARED_BY_OTHER, writer));
          continue;
        }
      }
      files.add(new FileFinding(relative, Finding.MODIFIED, null));
    }
    files.addAll(obsoleteFiles(receipt));
    Set<String> known = new LinkedHashSet<>(listed);
    for (String path : PluginInstaller.nullSafe(receipt.getObsoletePaths())) {
      if (StringUtils.isNotBlank(path)) {
        known.add(PluginInstaller.normalizeRelative(path));
      }
    }
    for (String path : PluginInstaller.filesNoReceiptLists(hopHome, artifactId, known, log)) {
      files.add(new FileFinding(path, Finding.UNKNOWN, null));
    }
    boolean damaged = files.stream().anyMatch(f -> f.finding().isDamage());
    return new Result(
        artifactId,
        receipt.getVersion(),
        damaged ? State.DAMAGED : State.OK,
        withoutChecksums,
        files,
        null);
  }

  /** The obsolete files of the receipt that are still on disk. */
  private List<FileFinding> obsoleteFiles(InstallReceipt receipt) {
    List<FileFinding> files = new ArrayList<>();
    Path home = hopHome.toAbsolutePath().normalize();
    for (String path : PluginInstaller.nullSafe(receipt.getObsoletePaths())) {
      if (StringUtils.isBlank(path)) {
        continue;
      }
      String relative = PluginInstaller.normalizeRelative(path);
      Path file = hopHome.resolve(relative).toAbsolutePath().normalize();
      if (file.startsWith(home) && Files.exists(file)) {
        files.add(new FileFinding(relative, Finding.OBSOLETE, null));
      }
    }
    return files;
  }

  /**
   * The artifact of another active install whose receipt has this content for the shared file, as
   * written by that install (not kept), or null.
   */
  private static String otherWriter(List<InstallReceipt> others, String relative, String actual) {
    for (InstallReceipt other : others) {
      String checksum = normalizedKeys(other.getChecksums()).get(relative);
      SharedFile shared = normalizedKeys(other.getSharedFiles()).get(relative);
      if (checksum != null
          && checksum.equalsIgnoreCase(actual)
          && (shared == null || shared.getAction() != SharedFileAction.KEPT)) {
        return other.getArtifactId();
      }
    }
    return null;
  }

  /** The readable receipts of the other active marketplace installs. */
  private List<InstallReceipt> otherReceipts(String artifactId) {
    try {
      return PluginInstaller.readAllReceipts(hopHome, log).stream()
          .filter(r -> !artifactId.equals(r.getArtifactId()))
          .filter(r -> !r.isPendingActivation() && !r.isUninstallPending())
          .toList();
    } catch (HopException e) {
      log.logError("Could not read the other marketplace install receipts", e);
      return List.of();
    }
  }

  /** The changes the next Hop start makes, by target (relative to the Hop installation). */
  private Map<String, PendingChanges.Change> pendingChangesByTarget() {
    Map<String, PendingChanges.Change> changes = new HashMap<>();
    try {
      for (PendingChanges.Change change : new PendingChanges(hopHome).read()) {
        changes.put(change.target(), change);
      }
    } catch (HopException e) {
      log.logError("Could not read the changes left for the next Hop start", e);
    }
    return changes;
  }

  private static <V> Map<String, V> normalizedKeys(Map<String, V> map) {
    Map<String, V> normalized = new HashMap<>();
    for (Map.Entry<String, V> entry : PluginInstaller.nullSafe(map).entrySet()) {
      if (entry.getKey() != null && entry.getValue() != null) {
        normalized.put(PluginInstaller.normalizeRelative(entry.getKey()), entry.getValue());
      }
    }
    return normalized;
  }

  private static String sha256(Path file) throws IOException {
    MessageDigest digest = PluginInstaller.sha256();
    try (InputStream in = new DigestInputStream(Files.newInputStream(file), digest)) {
      in.transferTo(OutputStream.nullOutputStream());
      return HexFormat.of().formatHex(digest.digest());
    }
  }

  private static String rootMessage(HopException e, String artifactId) {
    Throwable cause = e;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return "The install receipt of "
        + artifactId
        + " cannot be read: "
        + StringUtils.defaultIfBlank(cause.getMessage(), cause.getClass().getSimpleName());
  }
}
