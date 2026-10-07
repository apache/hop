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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WatchFilesEngineTest {
  @TempDir Path temporary;
  private static final long TIME = System.currentTimeMillis() + 100000;

  @Test
  void nativeTargetedModifyAndDeleteAreAcknowledgedOnlyOnce() throws Exception {
    Path input = Files.createDirectory(temporary.resolve("input"));
    Path file = Files.writeString(input.resolve("a.csv"), "a");
    FileWatcher watcher = mock(FileWatcher.class);
    when(watcher.poll(0)).thenReturn(new FileWatcher.WatchBatch(Set.of(file), false, 0));
    try (FileObject root = HopVfs.getFileObject(input.toString(), new Variables());
        WatchFilesEngine engine = engine(root, false, true, watcher, new ArrayList<>())) {
      engine.initialize(InitialScan.IGNORE_EXISTING, TIME);
      Files.writeString(file, "longer");
      assertEquals(FileEventType.MODIFIED, collect(engine, TIME + 1).getType());
      assertNull(collect(engine, TIME + 2));
      Files.delete(file);
      assertEquals(FileEventType.DELETED, collect(engine, TIME + 3).getType());
      assertNull(collect(engine, TIME + 4));
    }
  }

  private WatchFilesEngine engine(
      FileObject root,
      boolean recursive,
      boolean nativeWatch,
      FileWatcher injected,
      List<String> logs)
      throws Exception {
    Path localRoot =
        root.getName().getScheme().equals("file") ? VfsFileScanner.localPath(root) : null;
    return new WatchFilesEngine(
        new VfsFileScanner(root, recursive, ".*\\.csv", "skip.*", 100, () -> false),
        injected != null
            ? injected
            : nativeWatch
                ? new LocalFileWatcher(localRoot, recursive, 100, 100)
                : new VfsPollingWatcher(),
        new JsonFileStateStore(
            temporary.resolve("state"), "input", root.getName().getURI(), "scope", 100),
        new FileStabilityTracker(false, 0, 1, 1, 100),
        nativeWatch ? 60000 : 10,
        5,
        10,
        nativeWatch ? localRoot : null,
        100,
        logs::add,
        new WatchFilesClock() {
          @Override
          public long elapsedMillis() {
            return 0;
          }

          @Override
          public long wallMillis() {
            return TIME;
          }
        },
        null);
  }

  private FileChangeEvent collect(WatchFilesEngine engine, long time) throws Exception {
    engine.tick(time);
    FileChangeEvent event = engine.next(time);
    if (event != null) {
      engine.acknowledge(event);
    }
    return event;
  }

  @Test
  void restartRecoversFileCreatedWhileStoppedWithoutRepeatingBaselineOrEmittedFile()
      throws Exception {
    Path input = Files.createDirectory(temporary.resolve("input"));
    Files.writeString(input.resolve("A.csv"), "a");
    try (FileObject root = HopVfs.getFileObject(input.toString(), new Variables())) {
      try (WatchFilesEngine first = engine(root, false, false, null, new ArrayList<>())) {
        first.initialize(InitialScan.IGNORE_EXISTING, TIME + 100);
        assertNull(first.next(TIME + 100));
        Files.writeString(input.resolve("B.csv"), "b");
        assertEquals("B.csv", collect(first, TIME + 120).file().getShortFilename());
      }
      Files.writeString(input.resolve("C.csv"), "c");
      try (WatchFilesEngine restarted = engine(root, false, false, null, new ArrayList<>())) {
        restarted.initialize(InitialScan.COMPARE_WITH_STATE, TIME + 200);
        FileChangeEvent recovered = restarted.next(TIME + 201);
        assertEquals("C.csv", recovered.file().getShortFilename());
        assertEquals(FileEventType.CREATED, recovered.getType());
        restarted.acknowledge(recovered);
        assertNull(collect(restarted, TIME + 220));
      }
    }
  }

  @Test
  void initialEmitFiltersAndRecursiveScanning() throws Exception {
    Path input = Files.createDirectories(temporary.resolve("input/child")).getParent();
    Files.writeString(input.resolve("child/a.csv"), "a");
    Files.writeString(input.resolve("skip.csv"), "skip");
    Files.writeString(input.resolve("other.tmp"), "tmp");
    try (FileObject root = HopVfs.getFileObject(input.toString(), new Variables());
        WatchFilesEngine engine = engine(root, true, false, null, new ArrayList<>())) {
      engine.initialize(InitialScan.EMIT_EXISTING, TIME + 100);
      FileChangeEvent event = engine.next(TIME + 101);
      assertEquals("a.csv", event.file().getShortFilename());
      engine.acknowledge(event);
      assertNull(collect(engine, TIME + 120));
    }
  }

  @Test
  void compareWithoutCheckpointEstablishesQuietBaseline() throws Exception {
    Path input = Files.createDirectory(temporary.resolve("input"));
    Files.writeString(input.resolve("a.csv"), "a");
    try (FileObject root = HopVfs.getFileObject(input.toString(), new Variables());
        WatchFilesEngine engine = engine(root, false, false, null, new ArrayList<>())) {
      engine.initialize(InitialScan.COMPARE_WITH_STATE, TIME + 100);
      assertNull(engine.next(TIME + 101));
      assertTrue(Files.exists(temporary.resolve("state/input.json")));
    }
  }

  @Test
  void pollingModificationsAndDeletesUsePreviousMetadata() throws Exception {
    Path input = Files.createDirectory(temporary.resolve("input"));
    Path file = Files.writeString(input.resolve("a.csv"), "a");
    try (FileObject root = HopVfs.getFileObject(input.toString(), new Variables());
        WatchFilesEngine engine = engine(root, false, false, null, new ArrayList<>())) {
      engine.initialize(InitialScan.IGNORE_EXISTING, TIME + 100);
      Files.writeString(file, "longer");
      FileChangeEvent modified = collect(engine, TIME + 120);
      assertEquals(FileEventType.MODIFIED, modified.getType());
      assertEquals(1, modified.getPrevious().getSize());
      Files.delete(file);
      FileChangeEvent deleted = collect(engine, TIME + 140);
      assertEquals(FileEventType.DELETED, deleted.getType());
      assertEquals(6, deleted.file().getSize());
      assertNull(collect(engine, TIME + 160));
    }
  }

  @Test
  void overflowPrioritizesScanAndDoesNotDuplicateNativeAndReconciliationCreate() throws Exception {
    Path input = Files.createDirectory(temporary.resolve("input"));
    FileWatcher watcher = mock(FileWatcher.class);
    when(watcher.poll(0)).thenReturn(new FileWatcher.WatchBatch(Set.of(), true, 1));
    List<String> logs = new ArrayList<>();
    try (FileObject root = HopVfs.getFileObject(input.toString(), new Variables());
        WatchFilesEngine engine = engine(root, false, true, watcher, logs)) {
      engine.initialize(InitialScan.IGNORE_EXISTING, TIME + 100);
      Path file = Files.writeString(input.resolve("a.csv"), "a");
      assertEquals("a.csv", collect(engine, TIME + 101).file().getShortFilename());
      when(watcher.poll(0)).thenReturn(new FileWatcher.WatchBatch(Set.of(file), false, 0));
      assertNull(collect(engine, TIME + 102));
      assertTrue(logs.stream().anyMatch(s -> s.contains("overflow")));
    }
  }

  @Test
  void failedListingRetainsStateAndRecoversAfterRootReturns() throws Exception {
    Path input = Files.createDirectory(temporary.resolve("input"));
    Files.writeString(input.resolve("a.csv"), "a");
    List<String> logs = new ArrayList<>();
    try (FileObject root = HopVfs.getFileObject(input.toString(), new Variables());
        WatchFilesEngine engine = engine(root, false, false, null, logs)) {
      engine.initialize(InitialScan.IGNORE_EXISTING, TIME + 100);
      Path moved = temporary.resolve("moved");
      Files.move(input, moved);
      assertNull(collect(engine, TIME + 120));
      assertTrue(logs.stream().anyMatch(s -> s.contains("state retained")));
      Files.move(moved, input);
      assertNull(collect(engine, TIME + 140));
      Files.writeString(input.resolve("b.csv"), "b");
      assertEquals("b.csv", collect(engine, TIME + 160).file().getShortFilename());
    }
  }

  @Test
  void unacknowledgedEventIsRecoveredAfterRestart() throws Exception {
    Path input = Files.createDirectory(temporary.resolve("input"));
    try (FileObject root = HopVfs.getFileObject(input.toString(), new Variables())) {
      try (WatchFilesEngine first = engine(root, false, false, null, new ArrayList<>())) {
        first.initialize(InitialScan.IGNORE_EXISTING, TIME + 100);
        Files.writeString(input.resolve("a.csv"), "a");
        first.tick(TIME + 120);
        assertNotNull(
            first.next(TIME + 121)); // simulate putRow failure: deliberately do not acknowledge
      }
      try (WatchFilesEngine second = engine(root, false, false, null, new ArrayList<>())) {
        second.initialize(InitialScan.COMPARE_WITH_STATE, TIME + 200);
        assertEquals("a.csv", second.next(TIME + 201).file().getShortFilename());
      }
    }
  }

  @Test
  void vfsRamPollingUsesHopProvidersWithoutCustomRemoteClient() throws Exception {
    String uri = "ram:///watchfiles-" + UUID.randomUUID();
    try (FileObject root = HopVfs.getFileObject(uri, new Variables())) {
      root.createFolder();
      try (WatchFilesEngine engine = engine(root, false, false, null, new ArrayList<>())) {
        engine.initialize(InitialScan.IGNORE_EXISTING, TIME + 100);
        try (FileObject file = root.resolveFile("a.csv")) {
          try (var out = HopVfs.getOutputStream(file, false)) {
            out.write(1);
          }
        }
        assertEquals("ram", collect(engine, TIME + 120).file().getScheme());
      } finally {
        root.deleteAll();
      }
    }
  }

  @Test
  void gracefulStopWakesPollingWaitAndReleasesExclusiveStateLock() throws Exception {
    Path input = Files.createDirectory(temporary.resolve("input"));
    try (FileObject root = HopVfs.getFileObject(input.toString(), new Variables())) {
      WatchFilesEngine engine = engine(root, false, false, null, new ArrayList<>());
      engine.initialize(InitialScan.IGNORE_EXISTING, TIME + 100);
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<?> future =
            executor.submit(
                () -> {
                  try {
                    engine.await(60000);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });
        engine.stop();
        future.get(2, TimeUnit.SECONDS);
        engine.close();
        try (WatchFilesEngine next = engine(root, false, false, null, new ArrayList<>())) {
          next.initialize(InitialScan.COMPARE_WITH_STATE, TIME + 200);
        }
      } finally {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
      }
    }
  }
}
