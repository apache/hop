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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@EnabledOnOs(OS.LINUX)
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class WatchFilesUnixTest {
  @TempDir Path temporary;

  @BeforeAll
  static void initializeHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void symbolicLinksAndRecursiveLoopsAreSkippedByNativeAndPolling() throws Exception {
    Path root = Files.createDirectory(temporary.resolve("input"));
    Path nested = Files.createDirectory(root.resolve("nested"));
    Files.writeString(nested.resolve("real.csv"), "real");
    Files.createSymbolicLink(nested.resolve("loop"), root);
    Files.createSymbolicLink(root.resolve("alias.csv"), nested.resolve("real.csv"));
    try (FileObject file = HopVfs.getFileObject(root.toString(), new Variables());
        LocalFileWatcher watcher = new LocalFileWatcher(root, true, 100, 100)) {
      var snapshot = new VfsFileScanner(file, true, ".*\\.csv", "", 100, () -> false).snapshot();
      assertEquals(1, snapshot.size());
      assertEquals("real.csv", snapshot.values().iterator().next().getShortFilename());
      Files.writeString(nested.resolve("new.csv"), "new");
      assertTrue(watcher.poll(2000).getPaths().stream().anyMatch(p -> p.endsWith("new.csv")));
    }
  }

  @Test
  void nonWritableCheckpointDirectoryFailsWithoutCreatingOrOverwritingState() throws Exception {
    Path state = Files.createDirectory(temporary.resolve("state"));
    Files.setPosixFilePermissions(state, PosixFilePermissions.fromString("r-x------"));
    try {
      assertThrows(
          java.io.IOException.class,
          () -> new JsonFileStateStore(state, "input", "root", "scope", 100));
      try (var files = Files.list(state)) {
        assertTrue(files.findAny().isEmpty());
      }
    } finally {
      Files.setPosixFilePermissions(state, PosixFilePermissions.fromString("rwx------"));
    }
  }

  @Test
  void largeSnapshotIsCompleteAndConfiguredLimitDoesNotReturnPartialState() throws Exception {
    Path root = Files.createDirectory(temporary.resolve("input"));
    for (int index = 0; index < 5000; index++) {
      Files.writeString(root.resolve("file-" + index + ".csv"), "x");
    }
    try (FileObject file = HopVfs.getFileObject(root.toString(), new Variables())) {
      assertEquals(
          5000,
          new VfsFileScanner(file, false, ".*\\.csv", "", 5000, () -> false).snapshot().size());
      assertThrows(
          WatchLimitException.class,
          () -> new VfsFileScanner(file, false, ".*\\.csv", "", 4999, () -> false).snapshot());
    }
  }

  @Test
  void realNativeEventBurstSignalsOverflowAndSnapshotRecoversAllFiles() throws Exception {
    Path root = Files.createDirectory(temporary.resolve("burst"));
    try (FileObject file = HopVfs.getFileObject(root.toString(), new Variables());
        LocalFileWatcher watcher = new LocalFileWatcher(root, false, 64, 3000)) {
      // Deliberately do not consume keys during the burst. The JDK Linux watch-key event list
      // overflows, independently of the plugin's bounded hint batch.
      for (int index = 0; index < 2500; index++) {
        Files.writeString(root.resolve("burst-" + index + ".csv"), "x");
      }
      FileWatcher.WatchBatch batch = watcher.poll(2000);
      assertTrue(batch.getOverflowCount() > 0, "Expected a real native/JDK overflow");
      assertTrue(batch.isReconciliationRequired());
      assertTrue(batch.getPaths().size() <= 64);
      var snapshot = new VfsFileScanner(file, false, ".*\\.csv", "", 3000, () -> false).snapshot();
      java.util.concurrent.atomic.AtomicInteger recovered =
          new java.util.concurrent.atomic.AtomicInteger();
      new FileReconciler()
          .compare(
              java.util.Map.of(),
              snapshot,
              System.currentTimeMillis(),
              event -> recovered.incrementAndGet());
      assertEquals(2500, recovered.get());
    }
  }
}
