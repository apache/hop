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
 * <p>An event is written to its own file before it is handed to the dispatcher, and the file is
 * deleted only once the collector accepted it (HTTP 2xx). Whatever is still on disk when the sink
 * starts is replayed first, so a collector restart, a network outage or a process that dies between
 * "pipeline finished" and "HTTP 2xx" delays lineage instead of losing it.
 *
 * <p>Delivery is therefore <i>at least once</i>: a process that dies after a successful POST but
 * before the delete replays that one event. OpenLineage events set aspects on entities rather than
 * appending records, so a replay rewrites the same state; a duplicate is the cheaper problem
 * compared to a permanent gap in the graph.
 *
 * <p>The spool is bounded by {@code maxBytes}; when it is full new events are <i>not</i> spooled
 * (they are still sent) and the refusal is counted, so a collector outage cannot fill the disk. The
 * oldest events are kept: they are the ones no later run will re-emit.
 */
final class OpenLineageSpool {

  static final String FILE_SUFFIX = ".json";

  /** Written first and renamed into place, so a crash never leaves a half event to replay. */
  static final String TEMP_SUFFIX = ".tmp";

  /**
   * The names this spool writes ({@link #nextName()}). Only these are listed, replayed or deleted:
   * a spool pointed at a folder that holds other files must never touch them.
   */
  private static final Pattern SPOOL_FILE_NAME =
      Pattern.compile(
          "\\d{13}-\\d{8}-"
              + "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
              + Pattern.quote(FILE_SUFFIX));

  /** A spooled event, identified by the name of the file that holds it. */
  record SpooledEvent(String id, RunEvent event) {}

  private final String location;
  private final long maxBytes;
  private final ILogChannel log;
  private final AtomicLong storedBytes = new AtomicLong();
  private final AtomicLong storedFiles = new AtomicLong();

  /**
   * Size of every file this instance counts, so a file that disappears (another process sharing the
   * folder delivered it first) still leaves the totals right.
   */
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
    if (StringUtils.isBlank(directory)) {
      return null;
    }
    try {
      return new OpenLineageSpool(HopVfs.getFileObject(directory.trim()), maxBytes, log);
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

  private OpenLineageSpool(FileObject folder, long maxBytes, ILogChannel log) throws Exception {
    this.location = folder.getName().getURI();
    this.maxBytes = maxBytes;
    this.log = log;
    if (!folder.exists()) {
      folder.createFolder();
    }
    if (!folder.isFolder()) {
      throw new IllegalArgumentException("not a folder: " + location);
    }
    for (String name : spoolFileNames()) {
      track(name, folder().resolveFile(name).getContent().getSize());
    }
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
   * The spool folder, resolved afresh for every operation.
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

  /** The spool files directly in the folder (not in subfolders), oldest first. */
  private List<String> spoolFileNames() throws Exception {
    List<String> names = new ArrayList<>();
    for (FileObject file : folder().getChildren()) {
      String name = file.getName().getBaseName();
      if (SPOOL_FILE_NAME.matcher(name).matches() && file.getType() == FileType.FILE) {
        names.add(name);
      }
    }
    Collections.sort(names);
    return names;
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
    // two processes sharing a spool folder never collide.
    return String.format(
        Locale.ROOT,
        "%013d-%08d-%s%s",
        System.currentTimeMillis(),
        sequence.getAndIncrement(),
        UUID.randomUUID(),
        FILE_SUFFIX);
  }

  /** Events left by an earlier run, oldest first. Unreadable files are discarded and counted. */
  List<SpooledEvent> loadAll() {
    List<SpooledEvent> events = new ArrayList<>();
    try {
      for (String name : spoolFileNames()) {
        try (InputStream in = HopVfs.getInputStream(folder().resolveFile(name))) {
          events.add(
              new SpooledEvent(
                  name,
                  OpenLineageClientUtils.runEventFromJson(
                      new String(in.readAllBytes(), StandardCharsets.UTF_8))));
        } catch (Exception e) {
          unreadableFiles.incrementAndGet();
          log.logError(
              "Discarding unreadable lineage spool file '"
                  + name
                  + "' in "
                  + location
                  + ": "
                  + e.getMessage());
          delete(name);
        }
      }
    } catch (Exception e) {
      log.logError("Could not list the lineage spool at " + location + ": " + e.getMessage(), e);
    }
    return events;
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
        // Gone either way: deleted here, or already delivered by another process sharing the
        // folder.
        untrack(id);
      } else {
        log.logError(
            "Could not delete delivered lineage spool file '"
                + id
                + "' in "
                + location
                + "; it will be replayed");
      }
    } catch (Exception e) {
      log.logError(
          "Could not delete delivered lineage spool file '"
              + id
              + "' in "
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
        file.delete();
      }
    } catch (Exception e) {
      // The tmp file is not listed by loadAll, so leaving it behind only wastes a little space.
      log.logDebug("Could not remove partial spool file " + file + ": " + e.getMessage());
    }
  }

  String getLocation() {
    return location;
  }

  /** Number of events currently on disk (including those this instance has not delivered yet). */
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
