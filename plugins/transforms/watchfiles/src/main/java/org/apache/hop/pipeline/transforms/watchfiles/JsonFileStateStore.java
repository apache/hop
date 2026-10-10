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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * Local checkpoints deliberately use NIO: VFS moveFile can copy/delete across mounts, which is
 * unsuitable for a durable checkpoint. The writer holds an OS lock for its entire lifetime.
 */
public class JsonFileStateStore implements FileStateStore {
  private final ObjectMapper mapper =
      new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private final Path state;
  private final Path temporary;
  private final Path backup;
  private final Path journal;
  private final String watchId;
  private String root;
  private String scope;
  private final int maximumEntries;
  private final FileChannel lockChannel;
  private final FileLock lock;
  private boolean loaded;
  private boolean recoveryRequired;
  private int loadedVersion;
  private long generation;

  public JsonFileStateStore(
      Path directory, String watchId, String root, String scope, int maximumEntries)
      throws IOException {
    if (!watchId.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")) {
      throw new IOException("Watch ID must contain 1-128 letters, digits, underscores or hyphens.");
    }
    this.watchId = watchId;
    this.root = root;
    this.scope = scope;
    this.maximumEntries = maximumEntries;
    Files.createDirectories(directory);
    state = directory.resolve(watchId + ".json");
    temporary = directory.resolve(watchId + ".json.tmp");
    backup = directory.resolve(watchId + ".json.bak");
    journal = directory.resolve(watchId + ".json.commit");
    lockChannel =
        FileChannel.open(
            directory.resolve(watchId + ".lock"),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE);
    FileLock acquired;
    try {
      acquired = lockChannel.tryLock();
      if (acquired == null) {
        throw new IOException("Watch ID is already in use: " + watchId);
      }
    } catch (IOException | OverlappingFileLockException e) {
      lockChannel.close();
      throw new IOException("Cannot acquire exclusive state lock for Watch ID " + watchId, e);
    }
    lock = acquired;
  }

  @Override
  public Map<String, FileState> load() throws IOException {
    if (Files.exists(journal)) {
      recoveryRequired = true;
      throw new IOException(
          "Interrupted non-atomic checkpoint for "
              + watchId
              + ". Preserve all state files and restore the .bak checkpoint before removing .commit.");
    }
    if (!Files.exists(state)) {
      loaded = true;
      return null;
    }
    try {
      if (Files.size(state) > 4096L + (long) maximumEntries * 8192L) {
        throw new IOException("Checkpoint exceeds the bounded metadata size limit.");
      }
      JsonNode contents = mapper.readTree(Files.readAllBytes(state));
      Checkpoint checkpoint = mapper.treeToValue(contents, Checkpoint.class);
      if (!contents.path("version").isIntegralNumber()
          || (checkpoint.version != 1 && checkpoint.version != 2)
          || !watchId.equals(checkpoint.watchId)
          || checkpoint.root == null
          || checkpoint.scope == null
          || (checkpoint.version == 2
              && (!"OBSERVED".equals(checkpoint.semantics)
                  || !contents.path("generation").isIntegralNumber()
                  || !contents.path("savedAt").isIntegralNumber()
                  || checkpoint.generation < 1
                  || checkpoint.savedAt < 0))
          || checkpoint.files == null
          || checkpoint.files.size() > maximumEntries) {
        throw new IOException(
            "Unsupported checkpoint version, invalid metadata or entry limit exceeded.");
      }
      if (root != null && !root.equals(checkpoint.root)) {
        throw new IOException(
            "Directory differs from the saved checkpoint. Restore the previous Directory or use a new Watch ID for the new location.");
      }
      if (scope != null && !scope.equals(checkpoint.scope)) {
        throw new IOException(
            "Include/exclude filters or Include subdirectories differ from the saved checkpoint. Restore the previous options or use a new Watch ID for the new scope.");
      }
      for (Map.Entry<String, FileState> entry : checkpoint.files.entrySet()) {
        FileState file = entry.getValue();
        JsonNode fileNode = contents.path("files").path(entry.getKey());
        if (!fileNode.path("size").isIntegralNumber()
            || !fileNode.path("lastModified").isIntegralNumber()
            || file == null
            || !entry.getKey().equals(file.getUri())
            || file.getFilename() == null
            || file.getShortFilename() == null
            || file.getPath() == null
            || file.getScheme() == null
            || file.getSize() < 0
            || file.getLastModified() < 0) {
          throw new IOException("Invalid file entry.");
        }
      }
      loaded = true;
      recoveryRequired = false;
      root = checkpoint.root;
      scope = checkpoint.scope;
      loadedVersion = checkpoint.version;
      generation = checkpoint.generation;
      return new HashMap<>(checkpoint.files);
    } catch (IOException | RuntimeException e) {
      recoveryRequired = true;
      throw new IOException(
          "Cannot load Watch Files state at "
              + state
              + ": "
              + e.getMessage()
              + " State has not been overwritten. Use the Maintenance tab to inspect, restore or reset it, or use a new Watch ID explicitly.",
          e);
    }
  }

  @Override
  public void save(Map<String, FileState> files) throws IOException {
    if (recoveryRequired) {
      throw new IOException("Invalid checkpoint requires explicit recovery before another save.");
    }
    if (!loaded) {
      load();
    }
    if (files.size() > maximumEntries) {
      throw new IOException("State entry limit exceeded.");
    }
    Checkpoint checkpoint = new Checkpoint();
    if (Files.exists(journal)) {
      throw new IOException(
          "Interrupted checkpoint requires explicit recovery before another save.");
    }
    if (root == null || scope == null) {
      throw new IOException("Cannot save a checkpoint without its root and filter identity.");
    }
    if (loadedVersion == 1) {
      WatchFilesStateManager.archive(state.getParent(), watchId);
    }
    checkpoint.version = 2;
    checkpoint.semantics = "OBSERVED";
    checkpoint.generation = Math.addExact(generation, 1);
    checkpoint.savedAt = System.currentTimeMillis();
    checkpoint.watchId = watchId;
    checkpoint.root = root;
    checkpoint.scope = scope;
    checkpoint.files = files;
    writeCheckpoint(temporary, mapper.writeValueAsBytes(checkpoint));
    replaceCheckpoint();
    forceDirectory();
    loadedVersion = 2;
    generation = checkpoint.generation;
  }

  /** Kept overridable so fault tests can simulate ENOSPC without filling the developer's disk. */
  protected void writeCheckpoint(Path target, byte[] contents) throws IOException {
    writeForced(target, contents);
  }

  protected void atomicReplace() throws IOException {
    Files.move(
        temporary, state, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
  }

  protected void nonAtomicReplace() throws IOException {
    Files.move(temporary, state, StandardCopyOption.REPLACE_EXISTING);
  }

  private void replaceCheckpoint() throws IOException {
    try {
      atomicReplaceWithRetry();
    } catch (AtomicMoveNotSupportedException e) {
      // Preserve a forced known-good backup and durable intent before any non-atomic replacement.
      // Recovery is explicit: a leftover intent must never be treated as a fresh first run.
      if (Files.exists(state)) {
        Files.copy(state, backup, StandardCopyOption.REPLACE_EXISTING);
        try (FileChannel channel = FileChannel.open(backup, StandardOpenOption.WRITE)) {
          channel.force(true);
        }
      }
      writeForced(journal, new byte[] {1});
      forceDirectory();
      nonAtomicReplace();
      try (FileChannel channel = FileChannel.open(state, StandardOpenOption.WRITE)) {
        channel.force(true);
      }
      forceDirectory();
      Files.delete(journal);
    }
  }

  private void atomicReplaceWithRetry() throws IOException {
    // Windows readers and antivirus software can briefly deny replacement of an open destination.
    // Retrying the atomic operation preserves the old checkpoint; never downgrade access failures
    // to copy/delete. A persistent permission problem still fails within a bounded 375 ms delay.
    for (int attempt = 0; ; attempt++) {
      try {
        atomicReplace();
        return;
      } catch (AccessDeniedException e) {
        if (attempt == 4) {
          throw e;
        }
        try {
          Thread.sleep(25L << attempt);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          InterruptedIOException failure =
              new InterruptedIOException("Checkpoint replacement interrupted.");
          failure.initCause(interrupted);
          failure.addSuppressed(e);
          throw failure;
        }
      }
    }
  }

  private static void writeForced(Path target, byte[] contents) throws IOException {
    try (FileChannel channel =
        FileChannel.open(
            target,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {
      ByteBuffer buffer = ByteBuffer.wrap(contents);
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
      channel.force(true);
    }
  }

  private void forceDirectory() {
    // Windows and some providers cannot open/force a directory. File forcing is still mandatory.
    try (FileChannel channel = FileChannel.open(state.getParent(), StandardOpenOption.READ)) {
      channel.force(true);
    } catch (IOException | UnsupportedOperationException ignored) {
      // Best effort directory durability; see the documented power-loss limitation.
    }
  }

  @Override
  public void close() throws IOException {
    try {
      lock.release();
    } finally {
      lockChannel.close();
    }
  }

  @Getter
  @Setter
  public static class Checkpoint {
    private int version;
    private String watchId;
    private String root;
    private String scope;
    private Map<String, FileState> files;
    private String semantics;
    private long generation;
    private long savedAt;
  }
}
