/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.commons.vfs2.provider.UriParser;

/**
 * Single owner: filesystem hints, reconciliation, stability and acknowledgement all run on the Hop
 * transform thread. stop() only sets a flag and closes the blocking watcher.
 */
public class WatchFilesEngine implements AutoCloseable {
  private final VfsFileScanner scanner;
  private final FileWatcher watcher;
  private final FileStateStore store;
  private final FileStabilityTracker stability;
  private final FileReconciler reconciler = new FileReconciler();
  private final Map<String, FileState> observed = new HashMap<>();
  private final long scanInterval;
  private final long checkpointInterval;
  private final long retryInterval;
  private final Consumer<String> log;
  private final Path localRoot;
  private final int maximumEntries;
  private final WatchFilesClock clock;
  private final WatchFilesDiagnostics diagnostics;
  private volatile boolean stopped;
  private boolean initialized;
  private boolean dirty;
  private boolean scanRequired;
  private long nextScan;
  private long nextCheckpoint;
  private long nextProbe;

  public WatchFilesEngine(
      VfsFileScanner scanner,
      FileWatcher watcher,
      FileStateStore store,
      FileStabilityTracker stability,
      long scanInterval,
      long checkpointInterval,
      long retryInterval,
      Path localRoot,
      int maximumEntries,
      Consumer<String> log) {
    this(
        scanner,
        watcher,
        store,
        stability,
        scanInterval,
        checkpointInterval,
        retryInterval,
        localRoot,
        maximumEntries,
        log,
        WatchFilesClock.SYSTEM,
        null);
  }

  public WatchFilesEngine(
      VfsFileScanner scanner,
      FileWatcher watcher,
      FileStateStore store,
      FileStabilityTracker stability,
      long scanInterval,
      long checkpointInterval,
      long retryInterval,
      Path localRoot,
      int maximumEntries,
      Consumer<String> log,
      WatchFilesClock clock,
      WatchFilesDiagnostics diagnostics) {
    this.clock = clock;
    this.diagnostics = diagnostics;
    this.scanner = scanner;
    this.watcher = watcher;
    this.store = store;
    this.stability = stability;
    this.scanInterval = scanInterval;
    this.checkpointInterval = checkpointInterval;
    this.retryInterval = retryInterval;
    this.localRoot = localRoot;
    this.maximumEntries = maximumEntries;
    this.log = log;
  }

  public void initialize(InitialScan initial) throws IOException {
    initialize(initial, clock.elapsedMillis(), clock.wallMillis());
  }

  /** Explicit times support deterministic engine tests without sleeping. */
  public void initialize(InitialScan initial, long now) throws IOException {
    initialize(initial, now, now);
  }

  private void initialize(InitialScan initial, long now, long wallTime) throws IOException {
    long initializationStarted = clock.elapsedMillis();
    Map<String, FileState> saved = store.load();
    Map<String, FileState> snapshot = snapshot();
    if (saved != null) {
      observed.putAll(saved);
      log.accept("Loaded state with " + observed.size() + " entries.");
    } else {
      if (initial != InitialScan.EMIT_EXISTING) {
        // COMPARE_WITH_STATE without a checkpoint establishes an explicit quiet baseline.
        observed.putAll(snapshot);
        log.accept("Initial baseline contains " + observed.size() + " files.");
      }
      save();
    }
    initialized = true;
    reconcile(snapshot, now, wallTime);
    long completedAt = now + Math.max(0, clock.elapsedMillis() - initializationStarted);
    nextScan = completedAt + scanInterval;
    nextCheckpoint = completedAt + checkpointInterval;
    report(false);
  }

  public void tick() throws IOException, InterruptedException {
    tick(clock.elapsedMillis(), clock.wallMillis());
  }

  public void tick(long now) throws IOException, InterruptedException {
    tick(now, now);
  }

  private void tick(long now, long wallTime) throws IOException, InterruptedException {
    if (stopped) {
      return;
    }
    FileWatcher.WatchBatch batch = deferredBatch == null ? watcher.poll(0) : deferredBatch;
    deferredBatch = null;
    if (batch.getOverflowCount() > 0) {
      if (diagnostics != null) diagnostics.overflow(batch.getOverflowCount());
      log.accept("Watch service overflow detected. Scheduling reconciliation scan.");
    }
    if (batch.isReconciliationRequired()) {
      scanRequired = true;
      nextScan = now;
    }
    if (scanRequired || now >= nextScan) {
      // Never advance deletion state after a failed/incomplete scan.
      long scanStarted = clock.elapsedMillis();
      try {
        watcher.reconcileDirectories();
        reconcile(snapshot(), now, wallTime);
        scanRequired = false;
        nextScan = now + Math.max(0, clock.elapsedMillis() - scanStarted) + scanInterval;
      } catch (IOException e) {
        if (e instanceof WatchLimitException) {
          throw e;
        }
        if (stopped) {
          return;
        }
        if (diagnostics != null) diagnostics.failure("scan");
        log.accept("Reconciliation failed; state retained. Retrying: " + e.getMessage());
        scanRequired = false;
        nextScan = now + Math.max(0, clock.elapsedMillis() - scanStarted) + retryInterval;
      }
    } else if (localRoot != null) {
      for (Path path : batch.getPaths()) {
        if (stopped) {
          return;
        }
        if (!path.startsWith(localRoot) || !scanner.matches(path.getFileName().toString())) {
          continue;
        }
        String relative =
            UriParser.encode(
                localRoot
                    .relativize(path)
                    .toString()
                    .replace(localRoot.getFileSystem().getSeparator(), "/"),
                new char[] {'%'});
        try {
          FileState current = scanner.readRelative(relative);
          // A missing file's canonical VFS URI is found through its persisted relative path.
          if (current != null) {
            observe(current.getUri(), current, now, wallTime);
          } else {
            String uri = scanner.uri(relative);
            FileState previous = observed.get(uri);
            stability.remove(uri);
            if (previous != null) {
              deletions.put(
                  uri, new FileChangeEvent(FileEventType.DELETED, null, previous, wallTime));
            }
          }
        } catch (IOException e) {
          scanRequired = true;
        }
      }
    }
    checkpoint(now);
    report(false);
  }

  private void reconcile(Map<String, FileState> current, long now, long wallTime) {
    reconciler.compare(
        observed,
        current,
        wallTime,
        event -> {
          if (event.getType() == FileEventType.DELETED) {
            stability.remove(event.file().getUri());
            // Deletions use the same pending pipeline as writes, but require no stability wait.
            deletions.put(event.file().getUri(), event);
          } else {
            stability.observe(event, now);
          }
        });
    // Files can disappear before their first emission, or return to their observed version.
    for (String uri : stability.uris()) {
      FileState currentFile = current.get(uri);
      if (currentFile == null || currentFile.sameVersion(observed.get(uri))) {
        stability.remove(uri);
      }
    }
    deletions.keySet().removeIf(current::containsKey);
  }

  private final Map<String, FileChangeEvent> deletions = new HashMap<>();

  private void observe(String uri, FileState current, long now, long wallTime) {
    FileState previous = observed.get(uri);
    deletions.remove(uri);
    if (current.sameVersion(previous)) {
      stability.remove(uri);
      return;
    }
    stability.observe(
        new FileChangeEvent(
            previous == null ? FileEventType.CREATED : FileEventType.MODIFIED,
            current,
            previous,
            wallTime),
        now);
  }

  /**
   * One event at a time: Hop rowsets supply downstream backpressure without another event queue.
   */
  public FileChangeEvent next() throws IOException {
    return next(clock.elapsedMillis(), clock.wallMillis());
  }

  public FileChangeEvent next(long now) throws IOException {
    return next(now, now);
  }

  private FileChangeEvent next(long now, long wallTime) throws IOException {
    if (stopped) {
      return null;
    }
    if (!deletions.isEmpty()) {
      return deletions.values().iterator().next();
    }
    int probes = Math.min(stability.size(), 1024);
    if (now < nextProbe) {
      return null;
    }
    for (int index = 0; index < probes; index++) {
      String uri = stability.nextUri();
      // With stability disabled, a fresh stat is still performed immediately before emission.
      if (!stability.due(uri, now, wallTime)) {
        continue;
      }
      FileState current;
      try {
        long start = clock.elapsedMillis();
        current = scanner.readRelative(scanner.relativeUri(uri));
        if (diagnostics != null) diagnostics.probe(start);
      } catch (IOException e) {
        if (stopped) {
          return null;
        }
        if (diagnostics != null) diagnostics.failure("stat");
        log.accept("Stability probe failed; pending state retained. Retrying: " + e.getMessage());
        nextProbe = now + retryInterval;
        return null;
      }
      FileChangeEvent event = stability.check(uri, current, now, wallTime);
      if (event != null) {
        return event;
      }
    }
    return null;
  }

  /**
   * Called only after putRow returned successfully (or a disabled event was intentionally
   * observed).
   */
  public void acknowledge(FileChangeEvent event) throws IOException {
    acknowledge(event, true);
  }

  public void acknowledge(FileChangeEvent event, boolean enabled) throws IOException {
    String uri = event.file().getUri();
    if (event.getType() == FileEventType.DELETED) {
      observed.remove(uri);
      deletions.remove(uri);
    } else {
      if (!observed.containsKey(uri) && observed.size() >= maximumEntries) {
        throw new IOException("Observed state entry limit exceeded. Increase maximum entries.");
      }
      observed.put(uri, event.getCurrent());
      stability.remove(uri);
    }
    dirty = true;
    if (diagnostics != null) diagnostics.acknowledged(enabled);
    report(false);
  }

  public void await(long milliseconds) throws IOException, InterruptedException {
    if (stopped) {
      return;
    }
    // Retain the bounded batch for the next iteration; ordinary hints do not require a full scan.
    FileWatcher.WatchBatch batch = watcher.poll(milliseconds);
    if (!batch.getPaths().isEmpty() || batch.isReconciliationRequired()) {
      deferredBatch = batch;
    }
  }

  private FileWatcher.WatchBatch deferredBatch;

  public void stop() throws IOException {
    stopped = true;
    watcher.close();
  }

  private Map<String, FileState> snapshot() throws IOException {
    long start = clock.elapsedMillis();
    Map<String, FileState> result = scanner.snapshot();
    if (diagnostics != null) diagnostics.scan(start);
    return result;
  }

  private void save() throws IOException {
    long start = clock.elapsedMillis();
    try {
      store.save(observed);
      if (diagnostics != null) diagnostics.checkpoint(start);
    } catch (IOException e) {
      if (diagnostics != null) diagnostics.failure("checkpoint");
      throw e;
    }
  }

  private void report(boolean force) {
    if (diagnostics != null)
      diagnostics.report(observed.size(), stability.size() + deletions.size(), force);
  }

  public void checkpoint() throws IOException {
    checkpoint(clock.elapsedMillis());
  }

  public void checkpoint(long now) throws IOException {
    if (dirty && now >= nextCheckpoint) {
      save();
      dirty = false;
      nextCheckpoint = now + checkpointInterval;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      stop();
      if (initialized && dirty) {
        save();
        dirty = false;
      }
    } finally {
      try {
        report(true);
      } finally {
        // Remove diagnostics before releasing the OS lock, so an immediate restart cannot
        // collide with the previous instance's registry entry.
        if (diagnostics != null) diagnostics.unregister();
        store.close();
      }
    }
  }
}
