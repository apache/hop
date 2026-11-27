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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * Removes the files of a staging folder that no run used for a while.
 *
 * <p>Staged files are named after their content, so clients with different Hop versions or other
 * plugins never share or overwrite a file, and a client cannot tell whether a file it did not stage
 * itself is still in use. Use is the only safe signal: each run writes a small marker next to every
 * file it uses ({@code <file>.lastused}, see {@link ContentStaging}), also when the file was
 * already staged, and refreshes it while the run lasts. A file whose newest marker is older than
 * the retention is removed together with its sidecars.
 *
 * <p>Only the files Hop stages are considered (fat jars, project packages, Spark runtime archives),
 * because a staging folder is often a shared bucket. A marker in a format this version does not
 * know, written by a newer Hop, keeps its file, and so does a file without a marker.
 */
public final class StagingCleanup {

  private static final Class<?> PKG = StagingCleanup.class;

  /** Days a staged file is kept after its last use; 0 turns automatic cleanup off. */
  public static final String VARIABLE_RETENTION_DAYS = "HOP_STAGING_RETENTION_DAYS";

  public static final int DEFAULT_RETENTION_DAYS = 14;

  /** Suffix of the marker a run writes next to each staged file it uses. */
  public static final String MARKER_SUFFIX = ".lastused";

  /**
   * Suffix of a sidecar that keeps a staged file for good, such as the fat jar of a Databricks job
   * Hop deployed: the job keeps using it without a run of Hop to mark it. Removing the sidecar lets
   * the cleanup remove the file once it is not used.
   */
  public static final String KEEP_SUFFIX = ".keep";

  /** First line of a marker. */
  static final String MARKER_FORMAT = "hop-staging-marker/1";

  /** The files Hop stages, named after the first 12 hex characters of their SHA-256. */
  static final Pattern STAGED_NAME =
      Pattern.compile("(hop-native|hop-spark-package|spark-runtime)-[0-9a-f]{12}\\.(jar|zip)");

  /**
   * Infix of a file that is being uploaded, {@code <file>.uploading-<uuid>}: it gets the name of
   * the staged file once complete. One left behind by an upload that was killed goes with the
   * staged file.
   */
  public static final String UPLOAD_INFIX = ".uploading-";

  /** Sidecars that go with a staged file. */
  static final List<String> SIDECAR_SUFFIXES =
      List.of(MARKER_SUFFIX, KEEP_SUFFIX, FatJarCache.SHA256_SIDECAR_SUFFIX);

  /** A client cleans a staging folder automatically at most this often. */
  static final Duration AUTOMATIC_INTERVAL = Duration.ofDays(1);

  private StagingCleanup() {}

  /** What happens to a staged file. */
  public enum State implements IEnumHasCodeAndDescription {
    /** Used within the retention. */
    IN_USE,
    /** Not used within the retention: removed by a cleanup. */
    EXPIRED,
    /** Marked by a newer Hop in a format this version does not know: never removed here. */
    UNKNOWN_MARKER,
    /** Kept for good by a {@link #KEEP_SUFFIX} sidecar: never removed here. */
    KEPT,
    /** The store does not say when the file was last used: never removed here. */
    UNKNOWN_TIME,
    /**
     * No run of Hop marked the file as used, such as the fat jar of a Databricks job deployed by a
     * Hop version that wrote no markers: never removed here.
     */
    NOT_MARKED;

    @Override
    public String getCode() {
      return name();
    }

    @Override
    public String getDescription() {
      return BaseMessages.getString(PKG, "StagingCleanup.State." + name());
    }
  }

  /**
   * A staged file and its sidecars.
   *
   * @param name the staged file
   * @param files the staged file (when it is still there) and its sidecars
   * @param size bytes of all these files
   * @param lastUsed newest marker, or the upload time of a file staged without one; null when the
   *     store does not know
   */
  public record Entry(String name, List<String> files, long size, Instant lastUsed, State state) {}

  /**
   * @param removed the entries that were removed
   * @param failed the entries that could not be removed, with the reason
   */
  public record Result(List<Entry> removed, Map<String, String> failed) {
    public long removedBytes() {
      return removed.stream().mapToLong(Entry::size).sum();
    }
  }

  /**
   * @return the staged files of the folder, the least recently used first
   */
  public static List<Entry> inspect(IStagingStore store, Duration retention, Instant now)
      throws HopException {
    Map<String, List<IStagingStore.StagedFile>> groups = new LinkedHashMap<>();
    for (IStagingStore.StagedFile file : store.list()) {
      String staged = stagedName(file.name());
      if (staged != null) {
        groups.computeIfAbsent(staged, k -> new ArrayList<>()).add(file);
      }
    }
    Instant cutoff = now.minus(retention);
    List<Entry> entries = new ArrayList<>();
    for (Map.Entry<String, List<IStagingStore.StagedFile>> group : groups.entrySet()) {
      String name = group.getKey();
      long size = 0;
      long staged = -1;
      long marker = -1;
      boolean hasMarker = false;
      boolean kept = false;
      long newest = -1;
      List<String> files = new ArrayList<>();
      for (IStagingStore.StagedFile file : group.getValue()) {
        files.add(file.name());
        size += Math.max(0, file.size());
        // 0 or less: the store does not know when the file was written
        long modified = file.lastModified() > 0 ? file.lastModified() : -1;
        newest = Math.max(newest, modified);
        if (file.name().equals(name)) {
          staged = modified;
        } else if (file.name().equals(name + MARKER_SUFFIX)) {
          hasMarker = true;
          marker = modified;
        } else if (file.name().equals(name + KEEP_SUFFIX)) {
          kept = true;
        }
      }
      if (hasMarker && marker < 0 && !kept) {
        // The marker itself says when it was written
        marker = markerTime(store, name + MARKER_SUFFIX);
      }
      // The marker says when a run used the file; without one, the upload time of the file, else
      // the newest sidecar, which only shows when it got there
      long lastUsed = hasMarker ? marker : staged >= 0 ? staged : newest;
      Instant used = lastUsed >= 0 ? Instant.ofEpochMilli(lastUsed) : null;
      State state;
      if (kept) {
        state = State.KEPT;
      } else if (used == null) {
        // Never removed on a time nobody knows: it could be in use right now
        state = State.UNKNOWN_TIME;
      } else if (!hasMarker) {
        // Every run of this version marks a file before it uses it. A file without a marker was
        // staged by a version that wrote none, and something may use it every day, such as the
        // fat jar of a Databricks job deployed back then.
        state = State.NOT_MARKED;
      } else {
        state = used.isBefore(cutoff) ? State.EXPIRED : State.IN_USE;
      }
      if (state == State.EXPIRED && hasMarker && !isKnownMarker(store, name + MARKER_SUFFIX)) {
        state = State.UNKNOWN_MARKER;
      }
      // The staged file goes first and the marker last: a run that marks the file meanwhile
      // finds it missing and stages it again
      files.sort(Comparator.comparingInt(file -> deleteOrder(name, file)));
      entries.add(new Entry(name, files, size, used, state));
    }
    entries.sort(
        Comparator.comparing(Entry::lastUsed, Comparator.nullsFirst(Comparator.naturalOrder())));
    return entries;
  }

  /**
   * Remove the staged files that no run used within the retention. Each candidate is checked again
   * just before it is removed, so a run that started using it meanwhile keeps it.
   */
  public static Result cleanUp(
      ILogChannel log, IStagingStore store, Duration retention, Instant now) throws HopException {
    List<Entry> removed = new ArrayList<>();
    Map<String, String> failed = new LinkedHashMap<>();
    if (inspect(store, retention, now).stream().noneMatch(e -> e.state() == State.EXPIRED)) {
      return new Result(removed, failed);
    }
    // Check again right before removing: a run may have marked a file since
    for (Entry entry : inspect(store, retention, Instant.now())) {
      if (entry.state() != State.EXPIRED) {
        continue;
      }
      try {
        for (String file : entry.files()) {
          store.delete(file);
        }
        removed.add(entry);
        log.logBasic(
            "Removed staged "
                + store.location(entry.name())
                + " ("
                + entry.size() / (1024 * 1024)
                + " MB, last used "
                + entry.lastUsed()
                + ")");
      } catch (HopException e) {
        failed.put(entry.name(), e.getMessage());
        log.logBasic("Unable to remove staged " + store.location(entry.name()) + ": " + e);
      }
    }
    return new Result(removed, failed);
  }

  /**
   * Clean the staging folder of a run, at most once a day per client and folder. It never fails the
   * run: a store that cannot list, or an error, is logged and skipped.
   */
  public static void cleanUpIfDue(ILogChannel log, IVariables variables, IStagingStore store) {
    try {
      if (!store.supportsCleanup()) {
        return;
      }
      int days = retentionDays(variables);
      if (days <= 0) {
        return;
      }
      Path stamp =
          FatJarCache.cacheFolder(variables)
              .resolve("staging-cleanup-" + shortHash(store.location("")) + ".stamp");
      Instant now = Instant.now();
      if (Files.exists(stamp)
          && Files.getLastModifiedTime(stamp).toInstant().isAfter(now.minus(AUTOMATIC_INTERVAL))) {
        return;
      }
      Files.createDirectories(stamp.getParent());
      if (!Files.exists(stamp)) {
        Files.createFile(stamp);
      }
      Files.setLastModifiedTime(stamp, FileTime.from(now));
      Result result = cleanUp(log, store, Duration.ofDays(days), now);
      if (!result.removed().isEmpty()) {
        log.logBasic(
            "Staging cleanup removed "
                + result.removed().size()
                + " file(s) not used for "
                + days
                + " days ("
                + result.removedBytes() / (1024 * 1024)
                + " MB)");
      }
    } catch (Exception e) {
      log.logBasic("Skipped the staging cleanup: " + e.getMessage());
    }
  }

  /**
   * @return {@value #VARIABLE_RETENTION_DAYS} as a variable, system property or environment
   *     variable, else {@value #DEFAULT_RETENTION_DAYS}
   */
  public static int retentionDays(IVariables variables) {
    String value = variables != null ? variables.getVariable(VARIABLE_RETENTION_DAYS) : null;
    if (StringUtils.isBlank(value)) {
      value = System.getProperty(VARIABLE_RETENTION_DAYS);
    }
    if (StringUtils.isBlank(value)) {
      value = System.getenv(VARIABLE_RETENTION_DAYS);
    }
    if (StringUtils.isBlank(value)) {
      return DEFAULT_RETENTION_DAYS;
    }
    String resolved = variables != null ? variables.resolve(value) : value;
    try {
      return Integer.parseInt(resolved.trim());
    } catch (NumberFormatException e) {
      return DEFAULT_RETENTION_DAYS;
    }
  }

  /** The text of a marker written now. */
  static String markerText(Instant now) {
    return MARKER_FORMAT + "\n" + now + "\n";
  }

  /**
   * @return the staged file a file in the folder belongs to, or null when it is not Hop's
   */
  static String stagedName(String fileName) {
    String name = fileName;
    int upload = name.indexOf(UPLOAD_INFIX);
    if (upload > 0) {
      return STAGED_NAME.matcher(name.substring(0, upload)).matches()
          ? name.substring(0, upload)
          : null;
    }
    for (String suffix : SIDECAR_SUFFIXES) {
      if (name.endsWith(suffix)) {
        name = name.substring(0, name.length() - suffix.length());
        break;
      }
    }
    return STAGED_NAME.matcher(name).matches() ? name : null;
  }

  /**
   * The time a marker says it was written, for a store that does not know its modification time.
   *
   * @return milliseconds since the epoch, or -1 when the marker can't tell
   */
  static long markerTime(IStagingStore store, String markerName) {
    try {
      String text = store.readText(markerName);
      if (text == null) {
        return -1;
      }
      List<String> lines = text.lines().map(String::trim).toList();
      if (lines.size() < 2 || !lines.get(0).equals(MARKER_FORMAT)) {
        return -1;
      }
      long time = Instant.parse(lines.get(1)).toEpochMilli();
      return time > 0 ? time : -1;
    } catch (Exception e) {
      return -1;
    }
  }

  private static boolean isKnownMarker(IStagingStore store, String markerName) {
    try {
      String text = store.readText(markerName);
      return text == null || text.lines().findFirst().orElse("").trim().equals(MARKER_FORMAT);
    } catch (HopException e) {
      // Unreadable: keep the file
      return false;
    }
  }

  private static int deleteOrder(String name, String file) {
    if (file.equals(name)) {
      return 0;
    }
    return file.endsWith(MARKER_SUFFIX) ? 2 : 1;
  }

  private static String shortHash(String value) throws HopException {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest).substring(0, 16);
    } catch (Exception e) {
      throw new HopException("Unable to hash " + value, e);
    }
  }
}
