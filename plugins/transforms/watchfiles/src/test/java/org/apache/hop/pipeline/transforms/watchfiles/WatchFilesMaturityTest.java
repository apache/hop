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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WatchFilesMaturityTest {
  @TempDir Path temporary;
  private static final String ID = "input";

  private Map<String, FileState> files() {
    return new HashMap<>(
        Map.of(
            "a",
            new FileState("a", "a.csv", "a.csv", "/", "file", 1, 100),
            "b",
            new FileState("b", "b.csv", "b.csv", "/", "file", 2, 200)));
  }

  private JsonFileStateStore store() throws IOException {
    return new JsonFileStateStore(temporary, ID, "root", "scope", 100);
  }

  private WatchFilesStateManager manager() throws IOException {
    return new WatchFilesStateManager(temporary, ID, 100);
  }

  @Test
  void wallClockJumpDoesNotCountAsElapsedStabilityChecks() {
    FileStabilityTracker stability = new FileStabilityTracker(true, 500, 2, 100, 10);
    FileState file = files().get("a");
    stability.observe(new FileChangeEvent(FileEventType.CREATED, file, null, 1000), 10);
    assertFalse(stability.due("a", 20, 10_000_000));
    assertNull(stability.check("a", file, 110, 10_000_000));
    assertFalse(stability.due("a", 210, 50)); // clock moved backwards: minimum age still matters
    assertNotNull(stability.check("a", file, 210, 1000));
  }

  @Test
  void futureMtimeWaitsForAgeEvenWhenElapsedChecksHavePassed() {
    FileStabilityTracker stability = new FileStabilityTracker(true, 100, 2, 10, 10);
    FileState file = files().get("a");
    file.setLastModified(20_000);
    stability.observe(new FileChangeEvent(FileEventType.CREATED, file, null, 1000), 0);
    assertFalse(stability.due("a", 5000, 1000));
    assertFalse(stability.due("a", 5000, 20_099));
    assertTrue(stability.due("a", 5000, 20_100));
  }

  @Test
  void oldTimestampNewFileAndWallClockJumpsDoNotChangeScanSchedule() throws Exception {
    Path input = Files.createDirectory(temporary.resolve("watch"));
    FakeClock clock = new FakeClock();
    clock.wall = System.currentTimeMillis() + 10000;
    AtomicInteger scans = new AtomicInteger();
    ArrayList<String> logs = new ArrayList<>();
    WatchFilesDiagnostics diagnostics =
        new WatchFilesDiagnostics("test", ID, clock, 50, 100, logs::add);
    try (FileObject root = HopVfs.getFileObject(input.toString(), new Variables());
        WatchFilesEngine engine =
            new WatchFilesEngine(
                new VfsFileScanner(root, false, "", "", 100, () -> false) {
                  @Override
                  public Map<String, FileState> snapshot() throws IOException {
                    scans.incrementAndGet();
                    return super.snapshot();
                  }
                },
                new VfsPollingWatcher(),
                store(),
                new FileStabilityTracker(false, 0, 1, 1, 100),
                100,
                100,
                10,
                null,
                100,
                logs::add,
                clock,
                diagnostics)) {
      engine.initialize(InitialScan.IGNORE_EXISTING);
      Files.writeString(input.resolve("old.csv"), "old");
      clock.wall += 86_400_000;
      engine.tick();
      assertEquals(1, scans.get());
      clock.wall -= 172_800_000;
      clock.elapsed = 100;
      engine.tick();
      assertEquals(2, scans.get());
      clock.wall += 86_400_000;
      FileChangeEvent event = engine.next();
      assertNotNull(event);
      assertEquals(clock.wall, event.getDetectedAt());
      engine.acknowledge(event);
      engine.checkpoint();
      assertEquals(1, diagnostics.getSnapshot().emitted());
      assertTrue(logs.stream().anyMatch(line -> line.contains("Diagnostics")));
    }
  }

  @Test
  void slowScansLeaveAnIntervalForEmissionBeforeAnotherScan() throws Exception {
    Path input = Files.createDirectory(temporary.resolve("watch"));
    FakeClock clock = new FakeClock();
    clock.wall = System.currentTimeMillis();
    AtomicInteger scans = new AtomicInteger();
    try (FileObject root = HopVfs.getFileObject(input.toString(), new Variables());
        WatchFilesEngine engine =
            new WatchFilesEngine(
                new VfsFileScanner(root, false, "", "", 100, () -> false) {
                  @Override
                  public Map<String, FileState> snapshot() throws IOException {
                    scans.incrementAndGet();
                    clock.elapsed += 5000;
                    return super.snapshot();
                  }
                },
                new VfsPollingWatcher(),
                store(),
                new FileStabilityTracker(false, 0, 1, 1, 100),
                1000,
                1000,
                100,
                null,
                100,
                message -> {},
                clock,
                null)) {
      engine.initialize(InitialScan.IGNORE_EXISTING);
      engine.tick();
      assertEquals(1, scans.get());
      clock.elapsed = 6000;
      engine.tick();
      assertEquals(2, scans.get());
      engine.tick();
      assertEquals(2, scans.get());
    }
  }

  @Test
  void slowOperationsAndFailureCountersAreObservableAndRegistryIsReleased() {
    FakeClock clock = new FakeClock();
    ArrayList<String> logs = new ArrayList<>();
    WatchFilesDiagnostics diagnostics =
        new WatchFilesDiagnostics("unique-key", ID, clock, 20, 100, logs::add);
    diagnostics.register();
    try {
      clock.elapsed = 25;
      clock.wall = 500;
      diagnostics.scan(0);
      diagnostics.failure("stat");
      diagnostics.overflow(2);
      diagnostics.acknowledged(true);
      diagnostics.acknowledged(false);
      diagnostics.report(3, 4, true);
      var snapshot = WatchFilesDiagnostics.active("unique-key");
      assertEquals(25, snapshot.scanDurationMillis());
      assertEquals(500, snapshot.lastSuccessfulScanAt());
      assertEquals(1, snapshot.retries());
      assertEquals(2, snapshot.overflow());
      assertEquals(1, snapshot.emitted());
      assertEquals(1, snapshot.suppressed());
      assertTrue(logs.stream().anyMatch(line -> line.contains("Slow scan")));
    } finally {
      diagnostics.unregister();
    }
    assertNull(WatchFilesDiagnostics.active("unique-key"));
  }

  @Test
  void replayPersistsSelectionAndRetainsBackupAcrossRestart() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files());
    }
    assertEquals(1, manager().replay("a\\.csv"));
    try (JsonFileStateStore restarted = store()) {
      assertEquals(Map.of("b", files().get("b")), restarted.load());
    }
    try (var history = Files.list(temporary.resolve(ID + ".history"))) {
      Path saved = history.findFirst().orElseThrow().resolve(ID + ".json");
      assertTrue(Files.readString(saved).contains("a.csv"));
    }
    assertEquals(0, manager().replay("missing.*"));
  }

  @Test
  void mutationsRejectAnActiveOwnerAndDoNotChangeCheckpoint() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files());
      String original = Files.readString(temporary.resolve(ID + ".json"));
      assertThrows(IOException.class, () -> manager().replay(".*"));
      assertThrows(IOException.class, () -> manager().reset());
      assertThrows(IOException.class, () -> manager().backup());
      assertEquals(original, Files.readString(temporary.resolve(ID + ".json")));
    }
  }

  @Test
  void resetOfCorruptCheckpointArchivesOnlyThisWatchIdAndArchiveCanBeRestored() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files());
    }
    Path original = manager().backup().resolve(ID + ".json");
    Files.writeString(temporary.resolve(ID + ".json"), "{broken");
    Files.writeString(temporary.resolve("other.json"), "other");
    Path preserved = manager().reset();
    assertEquals("{broken", Files.readString(preserved.resolve(ID + ".json")));
    assertFalse(Files.exists(temporary.resolve(ID + ".json")));
    assertEquals("other", Files.readString(temporary.resolve("other.json")));
    manager().restoreBackup(original);
    try (JsonFileStateStore store = store()) {
      assertEquals(files(), store.load());
    }
  }

  @Test
  void invalidRestoreCannotOverwriteCurrentCheckpoint() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files());
    }
    String original = Files.readString(temporary.resolve(ID + ".json"));
    Path invalid = Files.writeString(temporary.resolve("invalid.json"), "{broken");
    assertThrows(IOException.class, () -> manager().restoreBackup(invalid));
    assertEquals(original, Files.readString(temporary.resolve(ID + ".json")));
  }

  @Test
  void v1MigrationPreservesIdentityFilesAndOriginalBytes() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files());
    }
    Path state = temporary.resolve(ID + ".json");
    String legacy = Files.readString(state).replace("\"version\":2", "\"version\":1");
    Files.writeString(state, legacy);
    manager().migrate();
    assertTrue(Files.readString(state).contains("\"version\":2"));
    assertTrue(manager().inspect().contains("Observed files: 2"));
    try (JsonFileStateStore store = store()) {
      assertEquals(files(), store.load());
    }
    try (var history = Files.walk(temporary.resolve(ID + ".history"))) {
      assertTrue(
          history
              .filter(path -> path.getFileName().toString().equals(ID + ".json"))
              .anyMatch(
                  path -> {
                    try {
                      return legacy.equals(Files.readString(path));
                    } catch (IOException e) {
                      throw new java.io.UncheckedIOException(e);
                    }
                  }));
    }
  }

  @Test
  void partialWriteSimulatingDiskFullPreservesLastValidCheckpoint() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files());
    }
    String original = Files.readString(temporary.resolve(ID + ".json"));
    try (JsonFileStateStore failing =
        new JsonFileStateStore(temporary, ID, "root", "scope", 100) {
          @Override
          protected void writeCheckpoint(Path target, byte[] bytes) throws IOException {
            Files.write(target, java.util.Arrays.copyOf(bytes, 20));
            throw new IOException("No space left on device (injected after partial write)");
          }
        }) {
      assertThrows(IOException.class, () -> failing.save(Map.of()));
    }
    assertEquals(original, Files.readString(temporary.resolve(ID + ".json")));
    try (JsonFileStateStore store = store()) {
      assertEquals(files(), store.load());
    }
  }

  static class FakeClock implements WatchFilesClock {
    long elapsed;
    long wall;

    @Override
    public long elapsedMillis() {
      return elapsed;
    }

    @Override
    public long wallMillis() {
      return wall;
    }
  }
}
