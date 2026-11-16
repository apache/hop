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

package org.apache.hop.lineage.openlineage;

import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Local write-ahead spool for outbound OpenLineage events.
 *
 * <p>An event is written to its own file before it is sent, and the file is deleted only once the
 * collector accepted or refused it. Whatever is still on disk when a process stops is sent by the
 * next one, so a collector restart, a network outage or a process that dies between "pipeline
 * finished" and "HTTP 2xx" delays lineage instead of losing it.
 *
 * <p>Every process writes to a folder of its own inside the configured one, and keeps a heartbeat
 * file in it. A process only takes over the files of a folder whose owner is gone: it shut down (no
 * heartbeat left) or stopped beating for {@link #STALE_AFTER_MS}. Taking over moves each file into
 * the process's own folder, so two processes that start together never both send a file, and a Hop
 * Server and a {@code hop-run} that share the configured folder never send each other's events.
 *
 * <p>Delivery is therefore <i>at least once</i>: a process that dies after a successful POST but
 * before the delete sends that one event again. OpenLineage events set aspects on entities rather
 * than appending records, so a replay rewrites the same state; a duplicate is the cheaper problem
 * compared to a permanent gap in the graph.
 *
 * <p>The spool is bounded by {@code maxBytes}; when it is full new events are <i>not</i> spooled
 * (they are still sent) and the refusal is counted, so a collector outage cannot fill the disk. The
 * oldest events are kept: they are the ones no later run will re-emit.
 */
final class OpenLineageSpool {
  private static final String CONST_IN = "' in ";

  static final String FILE_SUFFIX = ".json";

  /** Written first and renamed into place, so a crash never leaves a half event to replay. */
  static final String TEMP_SUFFIX = ".tmp";

  /** Holds the time its process last confirmed it is alive; see {@link #heartbeat()}. */
  static final String HEARTBEAT_FILE = "heartbeat";

  /** How often a process confirms it still owns its spool folder. */
  static final long HEARTBEAT_INTERVAL_MS = 30_000;

  /**
   * How long a heartbeat may go unrenewed before the folder counts as abandoned (its process was
   * killed). Several heartbeats, so a busy or paused JVM does not lose its folder.
   */
  static final long STALE_AFTER_MS = 180_000;

  private static final String UUID_PATTERN =
      "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

  /** The names this spool writes ({@link #nextName()}); nothing else is replayed or deleted. */
  private static final Pattern SPOOL_FILE_NAME =
      Pattern.compile("\\d{13}-\\d{8}-" + UUID_PATTERN + Pattern.quote(FILE_SUFFIX));

  /** The configured folder, which holds one folder per process. */
  private final String rootLocation;

  /** This process's folder; the files of a folder it took over are moved in here. */
  private final String location;

  /**
   * The names of the process folders of this sink. A sink that shares the spool code (DataHub) uses
   * a prefix of its own, so neither replays the other's events from a shared folder.
   */
  private final Pattern ownerFolderName;

  private final long maxBytes;
  private final long staleAfterMs;
  private final ILogChannel log;
  private final AtomicLong storedBytes = new AtomicLong();
  private final AtomicLong storedFiles = new AtomicLong();

  /** Size of every file this instance counts, so the totals stay right whatever removes a file. */
  private final Map<String, Long> storedSizes = new ConcurrentHashMap<>();

  private final AtomicLong sequence = new AtomicLong();
  private final AtomicLong serialisationFailures = new AtomicLong();
  private final AtomicLong writeFailures = new AtomicLong();
  private final AtomicLong fullRefusals = new AtomicLong();
  private final AtomicLong unreadableFiles = new AtomicLong();
  private volatile boolean fullReported;

  /**
   * Opens a spool on {@code directory}, or returns {@code null} when no directory is configured or
   * it cannot be used. A misconfigured spool must not stop lineage from being sent: the caller gets
   * {@code null} and sends without durability, with the problem logged here.
   */
  static OpenLineageSpool open(String directory, long maxBytes, ILogChannel log) {
    return open(directory, "", maxBytes, log);
  }

  /**
   * {@link #open(String, long, ILogChannel)} for a sink whose process folders start with {@code
   * prefix}, so two sinks configured with the same folder do not replay each other's events.
   */
  static OpenLineageSpool open(String directory, String prefix, long maxBytes, ILogChannel log) {
    return open(directory, prefix, maxBytes, STALE_AFTER_MS, log);
  }

  static OpenLineageSpool open(
      String directory, String prefix, long maxBytes, long staleAfterMs, ILogChannel log) {
    if (StringUtils.isBlank(directory)) {
      return null;
    }
    try {
      return new OpenLineageSpool(
          HopVfs.getFileObject(directory.trim()), prefix, maxBytes, staleAfterMs, log);
    } catch (Exception e) {
      log.logError(
          "Lineage spool directory '"
              + directory
              + "' is not usable; lineage will be sent without a spool: "
              + e.getMessage(),
          e);
      return null;
    }
  }

  private OpenLineageSpool(
      FileObject root, String prefix, long maxBytes, long staleAfterMs, ILogChannel log)
      throws Exception {
    this.rootLocation = root.getName().getURI();
    String folderPrefix = StringUtils.defaultString(prefix) + "spool-";
    this.ownerFolderName = Pattern.compile(Pattern.quote(folderPrefix) + UUID_PATTERN);
    this.maxBytes = maxBytes;
    this.staleAfterMs = staleAfterMs;
    this.log = log;
    if (!root.exists()) {
      root.createFolder();
    }
    if (!root.isFolder()) {
      throw new IllegalArgumentException("not a folder: " + rootLocation);
    }
    FileObject own = root.resolveFile(folderPrefix + UUID.randomUUID());
    own.createFolder();
    this.location = own.getName().getURI();
    heartbeat();
  }

  private void track(String name, long size) {
    if (storedSizes.putIfAbsent(name, size) == null) {
      storedBytes.addAndGet(size);
      storedFiles.incrementAndGet();
    }
  }

  private void untrack(String name) {
    Long size = storedSizes.remove(name);
    if (size != null) {
      storedBytes.addAndGet(-size);
      storedFiles.decrementAndGet();
    }
  }

  /**
   * This process's folder, resolved afresh for every operation.
   *
   * <p>Hop replaces its shared VFS file system manager when a project is loaded or closed ({@code
   * HopVfs.reset}), which leaves every {@link FileObject} held from before that point pointing at a
   * manager whose cache has been torn down: the next {@code resolveFile} on it fails with "No
   * files-cache implementation set". A sink outlives those resets, so the folder is re-resolved
   * instead of cached.
   */
  private FileObject folder() throws Exception {
    return HopVfs.getFileObject(location);
  }

  /**
   * Takes over the events of processes that are gone and returns every event in this process's
   * folder, oldest first, by id. Only the names are read: an event is loaded when it is sent (see
   * {@link #load}), so a large backlog costs neither start-up time nor memory.
   */
  List<String> recover() {
    List<String> ids = new ArrayList<>();
    try {
      FileObject own = folder();
      for (FileObject candidate : HopVfs.getFileObject(rootLocation).getChildren()) {
        String name = candidate.getName().getBaseName();
        if (candidate.getType() == FileType.FOLDER
            && ownerFolderName.matcher(name).matches()
            && !candidate.getName().getURI().equals(location)
            && isAbandoned(candidate)) {
          ids.addAll(takeOver(candidate, own));
        }
      }
    } catch (Exception e) {
      log.logError(
          "Could not list the lineage spool at " + rootLocation + ": " + e.getMessage(), e);
    }
    Collections.sort(ids);
    return ids;
  }

  /**
   * Whether no process owns this folder any more: it removed its heartbeat on a clean shutdown, or
   * the heartbeat has not been renewed for {@link #staleAfterMs}.
   */
  private boolean isAbandoned(FileObject folder) throws Exception {
    FileObject heartbeat = folder.resolveFile(HEARTBEAT_FILE);
    if (!heartbeat.exists()) {
      return true;
    }
    long beat;
    try (InputStream in = HopVfs.getInputStream(heartbeat)) {
      beat = Long.parseLong(new String(in.readAllBytes(), StandardCharsets.UTF_8).trim());
    } catch (Exception e) {
      // Being rewritten right now, or unreadable: fall back to when it was last written.
      beat = heartbeat.getContent().getLastModifiedTime();
    }
    return System.currentTimeMillis() - beat > staleAfterMs;
  }

  /**
   * Moves an abandoned folder's events into this process's folder. A move is a claim: when two
   * processes take over the same folder, each file ends up with exactly one of them.
   */
  private List<String> takeOver(FileObject abandoned, FileObject own) throws Exception {
    List<String> taken = new ArrayList<>();
    boolean stale = abandoned.resolveFile(HEARTBEAT_FILE).exists();
    for (FileObject file : abandoned.getChildren()) {
      String name = file.getName().getBaseName();
      if (file.getType() != FileType.FILE) {
        continue;
      }
      if (name.endsWith(TEMP_SUFFIX)) {
        deleteQuietly(file);
      } else if (SPOOL_FILE_NAME.matcher(name).matches()) {
        try {
          long size = file.getContent().getSize();
          file.moveTo(own.resolveFile(name));
          track(name, size);
          taken.add(name);
        } catch (Exception e) {
          // Another process took it over first.
          log.logDebug("Lineage spool file " + name + " was claimed elsewhere: " + e.getMessage());
        }
      }
    }
    // An empty folder without a heartbeat may be one whose process is only just starting; leave it.
    if (stale || !taken.isEmpty()) {
      deleteQuietly(abandoned.resolveFile(HEARTBEAT_FILE));
      deleteQuietly(abandoned);
    }
    if (!taken.isEmpty()) {
      log.logBasic(
          "Lineage spool: "
              + taken.size()
              + " event(s) left by an earlier process in "
              + abandoned.getName().getURI()
              + " will be sent again");
    }
    return taken;
  }

  /** Tells other processes sharing the folder that this one is alive and owns its events. */
  void heartbeat() {
    try {
      FileObject file = folder().resolveFile(HEARTBEAT_FILE);
      if (!file.exists()) {
        file.createFile();
      }
      try (OutputStream out = HopVfs.getOutputStream(file, false)) {
        out.write(Long.toString(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
      }
    } catch (Exception e) {
      log.logError(
          "Could not renew the lineage spool heartbeat in "
              + location
              + "; another process may send its events: "
              + e.getMessage());
    }
  }

  /**
   * Gives up the folder on shutdown: removes it when it is empty, and otherwise removes the
   * heartbeat, so the next process sends what is left without waiting for it to go stale.
   */
  void release() {
    try {
      FileObject own = folder();
      deleteQuietly(own.resolveFile(HEARTBEAT_FILE));
      deleteQuietly(own);
    } catch (Exception e) {
      log.logDebug("Could not release lineage spool folder " + location + ": " + e.getMessage());
    }
  }

  /**
   * Persists an event and returns the id to delete once it has been delivered, or {@code null} when
   * nothing was stored (spool full, serialisation or disk failure). A {@code null} id never fails
   * the delivery: it only means this event is not guaranteed to survive the process.
   */
  String store(RunEvent event) {
    byte[] bytes;
    try {
      bytes = OpenLineageClientUtils.toJson(event).getBytes(StandardCharsets.UTF_8);
    } catch (RuntimeException e) {
      if (serialisationFailures.getAndIncrement() == 0) {
        log.logError(
            "Could not serialise a lineage event for the spool at "
                + location
                + "; the event is sent but not replayable: "
                + e.getMessage(),
            e);
      }
      return null;
    }
    if (isFull(bytes.length)) {
      return null;
    }
    String name = nextName();
    FileObject temp = null;
    try {
      FileObject folder = folder();
      temp = folder.resolveFile(name + TEMP_SUFFIX);
      if (!temp.exists()) {
        temp.createFile();
      }
      try (OutputStream out = HopVfs.getOutputStream(temp, false)) {
        out.write(bytes);
      }
      temp.moveTo(folder.resolveFile(name));
      track(name, bytes.length);
      return name;
    } catch (Exception e) {
      if (writeFailures.getAndIncrement() == 0) {
        log.logError(
            "Could not write a lineage event to the spool at "
                + location
                + "; the event is sent but not replayable: "
                + e.getMessage(),
            e);
      }
      deleteQuietly(temp);
      return null;
    }
  }

  private boolean isFull(int incomingBytes) {
    if (maxBytes <= 0 || storedBytes.get() + incomingBytes <= maxBytes) {
      return false;
    }
    fullRefusals.incrementAndGet();
    if (!fullReported) {
      fullReported = true;
      log.logError(
          "Lineage spool at "
              + location
              + " is full ("
              + storedBytes.get()
              + " of "
              + maxBytes
              + " bytes); new events are sent but not spooled. Raise "
              + OpenLineageSinkConfig.VAR_SPOOL_MAX_BYTES
              + " or drain "
              + location
              + ".");
    }
    return true;
  }

  private String nextName() {
    // Sortable by name: timestamp, then this instance's sequence number, then a random suffix so
    // the files of folders that were taken over never collide.
    return String.format(
        Locale.ROOT,
        "%013d-%08d-%s%s",
        System.currentTimeMillis(),
        sequence.getAndIncrement(),
        UUID.randomUUID(),
        FILE_SUFFIX);
  }

  /**
   * Reads a spooled event, or returns {@code null} when it cannot be read: such a file is discarded
   * and counted, so it cannot hold up the events behind it.
   */
  RunEvent load(String id) {
    try (InputStream in = HopVfs.getInputStream(folder().resolveFile(id))) {
      return OpenLineageClientUtils.runEventFromJson(
          new String(in.readAllBytes(), StandardCharsets.UTF_8));
    } catch (Exception e) {
      unreadableFiles.incrementAndGet();
      log.logError(
          "Discarding unreadable lineage spool file '"
              + id
              + CONST_IN
              + location
              + ": "
              + e.getMessage());
      delete(id);
      return null;
    }
  }

  /**
   * Removes a delivered event. A failed delete costs one duplicate on the next replay, so it is
   * reported but never turned into an error the pipeline could see.
   */
  void delete(String id) {
    if (id == null) {
      return;
    }
    try {
      FileObject file = folder().resolveFile(id);
      if (file.delete() || !file.exists()) {
        untrack(id);
      } else {
        log.logError(
            "Could not delete delivered lineage spool file '"
                + id
                + CONST_IN
                + location
                + "; it will be replayed");
      }
    } catch (Exception e) {
      log.logError(
          "Could not delete delivered lineage spool file '"
              + id
              + CONST_IN
              + location
              + "; it will be replayed: "
              + e.getMessage(),
          e);
    }
  }

  private void deleteQuietly(FileObject file) {
    if (file == null) {
      return;
    }
    try {
      if (file.exists()) {
        // A folder that still has files is left alone (FileObject#delete).
        file.delete();
      }
    } catch (Exception e) {
      // A leftover tmp file or empty folder is not replayed, so it only wastes a little space.
      log.logDebug("Could not remove spool file " + file + ": " + e.getMessage());
    }
  }

  /** This process's spool folder. */
  String getLocation() {
    return location;
  }

  /** Number of events in this process's folder that it has not delivered yet. */
  long getStoredCount() {
    return storedFiles.get();
  }

  long getStoredBytes() {
    return storedBytes.get();
  }

  long getFullRefusals() {
    return fullRefusals.get();
  }

  long getUnreadableFiles() {
    return unreadableFiles.get();
  }

  /**
   * Serialisation plus disk-write failures: events that were sent but could not be made durable.
   */
  long getWriteFailures() {
    return serialisationFailures.get() + writeFailures.get();
  }
}
