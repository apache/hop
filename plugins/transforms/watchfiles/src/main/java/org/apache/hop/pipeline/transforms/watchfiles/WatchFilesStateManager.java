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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/** Local operator actions. Every mutation holds the same lifetime lock as the source. */
public class WatchFilesStateManager {
  private static final List<String> SUFFIXES =
      List.of(".json", ".json.tmp", ".json.bak", ".json.commit");
  private final Path directory;
  private final String watchId;
  private final int maximum;

  public WatchFilesStateManager(Path directory, String watchId, int maximum) throws IOException {
    if (watchId == null || !watchId.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")) {
      throw new IOException("Invalid Watch ID.");
    }
    if (maximum < 1) throw new IOException("Invalid maximum entries.");
    this.directory = directory.toAbsolutePath().normalize();
    this.watchId = watchId;
    this.maximum = maximum;
  }

  public String inspect() throws IOException {
    WatchFilesDiagnostics.Snapshot live =
        WatchFilesDiagnostics.active(directory.resolve(watchId).toString());
    if (live != null) return "Active source\n" + live;
    try (JsonFileStateStore store = open()) {
      Map<String, FileState> state = store.load();
      if (state == null)
        return "No checkpoint. The next start follows First start without saved state.";
      JsonNode header = new ObjectMapper().readTree(Files.readAllBytes(file(".json")));
      return "Stopped source\nWatch ID: "
          + watchId
          + "\nSchema: "
          + header.path("version")
          + "\nObserved files: "
          + state.size()
          + "\nSaved at (epoch ms): "
          + header.path("savedAt")
          + "\nGeneration: "
          + header.path("generation")
          + "\nRoot: "
          + header.path("root").asText()
          + "\nCheckpoint confirms source observation, not destination completion.";
    }
  }

  public Path backup() throws IOException {
    try (JsonFileStateStore ignored = open()) {
      return archive(directory, watchId);
    }
  }

  public int replay(String expression) throws IOException {
    Pattern filter = Pattern.compile(expression);
    try (JsonFileStateStore store = open()) {
      Map<String, FileState> state = store.load();
      if (state == null) throw new IOException("There is no checkpoint to replay.");
      int before = state.size();
      state
          .entrySet()
          .removeIf(entry -> filter.matcher(entry.getValue().getShortFilename()).matches());
      int count = before - state.size();
      if (count != 0) {
        archive(directory, watchId);
        store.save(state);
      }
      return count;
    }
  }

  public Path reset() throws IOException {
    try (JsonFileStateStore ignored = open()) {
      Path history = archive(directory, watchId);
      // Only this Watch ID's reserved files are affected. A backup must complete first.
      for (String suffix : SUFFIXES) Files.deleteIfExists(file(suffix));
      forceDirectory(directory);
      return history;
    }
  }

  public void migrate() throws IOException {
    try (JsonFileStateStore store = open()) {
      Map<String, FileState> state = store.load();
      if (state == null) throw new IOException("There is no checkpoint to migrate.");
      archive(directory, watchId);
      store.save(state);
    }
  }

  public Path restoreBackup() throws IOException {
    return restoreBackup(file(".json.bak"));
  }

  public Path restoreBackup(Path source) throws IOException {
    try (JsonFileStateStore ignored = open()) {
      if (!Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)) {
        throw new IOException("No recovery backup (.json.bak) exists.");
      }
      // Validate the backup using the same reader in a unique scratch directory. Never overwrite
      // an invalid main checkpoint merely to discover that its backup is also invalid.
      Path scratch = Files.createTempDirectory(directory, watchId + ".restore-");
      try {
        Files.copy(source, scratch.resolve(watchId + ".json"));
        try (JsonFileStateStore candidate =
            new JsonFileStateStore(scratch, watchId, null, null, maximum)) {
          candidate.load();
        }
        Path history = archive(directory, watchId);
        copyForced(scratch.resolve(watchId + ".json"), file(".json.tmp"));
        // Keep durable intent until replacement and forcing have completed, including on
        // filesystems
        // without atomic rename. A crash remains recoverable from the validated original backup.
        try (FileChannel marker =
            FileChannel.open(
                file(".json.commit"),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
          marker.force(true);
        }
        forceDirectory(directory);
        try {
          Files.move(
              file(".json.tmp"),
              file(".json"),
              StandardCopyOption.ATOMIC_MOVE,
              StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
          Files.move(file(".json.tmp"), file(".json"), StandardCopyOption.REPLACE_EXISTING);
        }
        force(file(".json"));
        forceDirectory(directory);
        Files.delete(file(".json.commit"));
        forceDirectory(directory);
        return history;
      } finally {
        Files.deleteIfExists(scratch.resolve(watchId + ".json"));
        Files.deleteIfExists(scratch.resolve(watchId + ".lock"));
        Files.delete(scratch);
      }
    }
  }

  private JsonFileStateStore open() throws IOException {
    return new JsonFileStateStore(directory, watchId, null, null, maximum);
  }

  private Path file(String suffix) {
    return directory.resolve(watchId + suffix);
  }

  static Path archive(Path directory, String watchId) throws IOException {
    Path parent = directory.resolve(watchId + ".history");
    if (Files.isSymbolicLink(parent)) throw new IOException("Backup directory must not be a link.");
    Files.createDirectories(parent);
    Path destination =
        Files.createDirectory(parent.resolve(System.currentTimeMillis() + "-" + UUID.randomUUID()));
    for (String suffix : SUFFIXES) {
      Path original = directory.resolve(watchId + suffix);
      if (Files.exists(original, LinkOption.NOFOLLOW_LINKS)) {
        copyForced(original, destination.resolve(watchId + suffix));
      }
    }
    forceDirectory(destination);
    forceDirectory(parent);
    forceDirectory(directory);
    return destination;
  }

  private static void copyForced(Path source, Path target) throws IOException {
    if (!Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("State artifacts must be regular files.");
    }
    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    force(target);
  }

  private static void force(Path file) throws IOException {
    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
      channel.force(true);
    }
  }

  private static void forceDirectory(Path directory) {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    } catch (IOException | UnsupportedOperationException ignored) {
      /* Best effort on Windows. */
    }
  }
}
