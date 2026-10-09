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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(40)
class WatchFilesCrashTest {
  @TempDir Path directory;

  static Map<String, FileState> files(long size) {
    return Map.of("a", new FileState("a", "a.csv", "a.csv", "/", "file", size, 100));
  }

  private JsonFileStateStore store() throws IOException {
    return new JsonFileStateStore(directory, "input", "root", "scope", 100);
  }

  private void killAt(String stage) throws Exception {
    Path ready = directory.resolve("ready");
    Files.deleteIfExists(ready);
    String classpath =
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Process child =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                classpath,
                Worker.class.getName(),
                directory.toString(),
                stage)
            .redirectErrorStream(true)
            .redirectOutput(directory.resolve("child.log").toFile())
            .start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      while (!Files.exists(ready) && child.isAlive() && System.nanoTime() < deadline)
        Thread.sleep(10);
      assertTrue(
          Files.exists(ready),
          () -> {
            try {
              return Files.readString(directory.resolve("child.log"));
            } catch (IOException e) {
              return e.toString();
            }
          });
      child.destroyForcibly();
      assertTrue(child.waitFor(5, TimeUnit.SECONDS));
      // Windows can signal process termination before its final kernel file handles are released.
      // Retry only in this crash-restart fixture, never bypass a live owner's production lock.
      long releaseDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (true) {
        try (JsonFileStateStore ignored =
            new JsonFileStateStore(directory, "input", null, null, 100)) {
          break;
        } catch (IOException e) {
          if (System.nanoTime() >= releaseDeadline) throw e;
          Thread.sleep(10);
        }
      }
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        child.waitFor(5, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  void killAfterTemporaryWriteKeepsPreviousCheckpointAndReleasesLock() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files(10));
    }
    killAt("before-rename");
    try (JsonFileStateStore store = store()) {
      assertEquals(files(10), store.load());
    }
  }

  @Test
  void killAfterAtomicReplacementRecoversNewCompleteCheckpoint() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files(10));
    }
    killAt("after-rename");
    try (JsonFileStateStore store = store()) {
      assertEquals(files(20), store.load());
    }
  }

  @Test
  void killDuringEmissionReplaysUnacknowledgedVersion() throws Exception {
    emission("before-ack", true);
  }

  @Test
  void killAfterAcknowledgementBeforeCheckpointCanRepeatVersion() throws Exception {
    emission("after-ack", true);
  }

  @Test
  void killAfterCheckpointDoesNotRepeatObservedVersion() throws Exception {
    emission("after-checkpoint", false);
  }

  @Test
  void killAfterFallbackIntentRequiresExplicitRestore() throws Exception {
    fallback("fallback-before-rename");
  }

  @Test
  void killAfterNonAtomicReplacementRequiresExplicitRestore() throws Exception {
    fallback("fallback-after-rename");
  }

  private void fallback(String stage) throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files(10));
    }
    killAt(stage);
    try (JsonFileStateStore store = store()) {
      assertThrows(IOException.class, store::load);
    }
    new WatchFilesStateManager(directory, "input", 100).restoreBackup();
    try (JsonFileStateStore store = store()) {
      assertEquals(files(10), store.load());
    }
  }

  private void emission(String stage, boolean repeat) throws Exception {
    killAt(stage);
    try (FileObject root =
            HopVfs.getFileObject(directory.resolve("watch").toString(), new Variables());
        WatchFilesEngine engine = Worker.engine(directory, root)) {
      engine.initialize(InitialScan.COMPARE_WITH_STATE);
      assertEquals(repeat, engine.next() != null);
    }
  }

  /** Launched in another JVM; the parent forcibly terminates it at a deterministic boundary. */
  public static class Worker {
    public static void main(String[] args) throws Exception {
      Path directory = Path.of(args[0]);
      String stage = args[1];
      if (stage.contains("rename")) {
        try (JsonFileStateStore store =
            new JsonFileStateStore(directory, "input", "root", "scope", 100) {
              @Override
              protected void atomicReplace() throws IOException {
                if (stage.startsWith("fallback"))
                  throw new java.nio.file.AtomicMoveNotSupportedException(
                      "tmp", "state", "Injected fallback");
                if (stage.equals("before-rename")) pause(directory);
                super.atomicReplace();
                if (stage.equals("after-rename")) pause(directory);
              }

              @Override
              protected void nonAtomicReplace() throws IOException {
                if (stage.equals("fallback-before-rename")) pause(directory);
                super.nonAtomicReplace();
                if (stage.equals("fallback-after-rename")) pause(directory);
              }
            }) {
          store.save(files(20));
        }
      } else {
        Path watch = Files.createDirectories(directory.resolve("watch"));
        try (FileObject root = HopVfs.getFileObject(watch.toString(), new Variables());
            WatchFilesEngine engine = engine(directory, root)) {
          engine.initialize(InitialScan.IGNORE_EXISTING);
          Files.writeString(watch.resolve("a.csv"), "event");
          FileChangeEvent event = null;
          long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
          while (event == null && System.nanoTime() < deadline) {
            engine.tick();
            event = engine.next();
            if (event == null) Thread.sleep(5);
          }
          if (event == null) throw new IOException("No event produced");
          if (stage.equals("before-ack")) pause(directory);
          engine.acknowledge(event);
          if (stage.equals("after-ack")) pause(directory);
          engine.checkpoint();
          pause(directory);
        }
      }
    }

    static WatchFilesEngine engine(Path directory, FileObject root) throws IOException {
      return new WatchFilesEngine(
          new VfsFileScanner(root, false, "", "", 100, () -> false),
          new VfsPollingWatcher(),
          new JsonFileStateStore(directory, "input", root.getName().getURI(), "scope", 100),
          new FileStabilityTracker(false, 0, 1, 1, 100),
          1,
          1,
          1,
          null,
          100,
          message -> {});
    }

    static void pause(Path directory) throws IOException {
      Files.writeString(directory.resolve("ready"), "ready");
      try {
        new java.util.concurrent.CountDownLatch(1).await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }
  }
}
