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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalFileWatcherTest {
  @TempDir Path root;

  private void awaitPath(LocalFileWatcher watcher, Path expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      if (watcher.poll(100).getPaths().contains(expected)) {
        return;
      }
    }
    fail("No native event for " + expected);
  }

  @Test
  void detectCreateModifyDelete() throws Exception {
    Path file = root.resolve("a.csv");
    try (LocalFileWatcher watcher = new LocalFileWatcher(root, false, 100, 100)) {
      Files.writeString(file, "a");
      awaitPath(watcher, file);
      Files.writeString(file, "longer");
      awaitPath(watcher, file);
      Files.delete(file);
      awaitPath(watcher, file);
    }
  }

  @Test
  void existingAndNewSubdirectoriesAreRegistered() throws Exception {
    Path existing = Files.createDirectories(root.resolve("existing/deep"));
    try (LocalFileWatcher watcher = new LocalFileWatcher(root, true, 100, 100)) {
      Path first = Files.writeString(existing.resolve("a.csv"), "a");
      awaitPath(watcher, first);
      Path created = Files.createDirectories(root.resolve("new/deep"));
      // A populated new subtree needs reconciliation as well as registration.
      assertTrue(watcher.poll(1000).isReconciliationRequired());
      watcher.reconcileDirectories();
      Path second = Files.writeString(created.resolve("b.csv"), "b");
      awaitPath(watcher, second);
    }
  }

  @Test
  void nonRecursiveWatcherDoesNotWatchChildren() throws Exception {
    Path child = Files.createDirectory(root.resolve("child"));
    try (LocalFileWatcher watcher = new LocalFileWatcher(root, false, 100, 100)) {
      Files.writeString(child.resolve("a.csv"), "a");
      assertTrue(watcher.poll(200).getPaths().isEmpty());
    }
  }

  @Test
  void closeWakesBlockedPollAndCanBeRepeated() throws Exception {
    LocalFileWatcher watcher = new LocalFileWatcher(root, false, 100, 100);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<FileWatcher.WatchBatch> blocked = executor.submit(() -> watcher.poll(60000));
      watcher.close();
      assertTrue(blocked.get(2, TimeUnit.SECONDS).getPaths().isEmpty());
      watcher.close();
    } finally {
      watcher.close();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
    }
  }

  @Test
  void overflowExplicitlyRequestsReconciliation() throws Exception {
    Path fakeRoot = mock(Path.class);
    FileSystem fileSystem = mock(FileSystem.class);
    WatchService service = mock(WatchService.class);
    WatchKey key = mock(WatchKey.class);
    when(fakeRoot.getFileSystem()).thenReturn(fileSystem);
    when(fileSystem.newWatchService()).thenReturn(service);
    when(fakeRoot.register(
            service,
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_DELETE))
        .thenReturn(key);
    WatchEvent<?> overflow = mock(WatchEvent.class);
    doReturn(StandardWatchEventKinds.OVERFLOW).when(overflow).kind();
    doReturn(List.of(overflow)).when(key).pollEvents();
    when(key.reset()).thenReturn(true);
    when(service.poll(0, TimeUnit.MILLISECONDS)).thenReturn(key);
    try (LocalFileWatcher watcher = new LocalFileWatcher(fakeRoot, false, 2, 10)) {
      FileWatcher.WatchBatch batch = watcher.poll(0);
      assertTrue(batch.isReconciliationRequired());
      assertEquals(1, batch.getOverflowCount());
    }
    verify(service).close();
  }

  @Test
  void hintCapacityRequestsRecoveryInsteadOfUnboundedMemory() throws Exception {
    try (LocalFileWatcher watcher = new LocalFileWatcher(root, false, 2, 10)) {
      for (int i = 0; i < 20; i++) {
        Files.writeString(root.resolve(i + ".csv"), "a");
      }
      FileWatcher.WatchBatch batch = watcher.poll(1000);
      assertTrue(batch.getPaths().size() <= 2);
      assertTrue(batch.isReconciliationRequired());
    }
  }

  @Test
  void cappedDrainRemovesInvalidKeyFromBothRegistriesAndAllowsRegistrationAgain() throws Exception {
    Path fakeRoot = mock(Path.class);
    Path child = mock(Path.class);
    FileSystem fileSystem = mock(FileSystem.class);
    WatchService service = mock(WatchService.class);
    WatchKey first = mock(WatchKey.class);
    WatchKey invalid = mock(WatchKey.class);
    WatchKey replacement = mock(WatchKey.class);
    when(fakeRoot.getFileSystem()).thenReturn(fileSystem);
    when(fileSystem.newWatchService()).thenReturn(service);
    when(fakeRoot.register(
            service,
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_DELETE))
        .thenReturn(first);
    when(child.register(
            service,
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_DELETE))
        .thenReturn(invalid, replacement);
    when(first.reset()).thenReturn(true);
    when(invalid.reset()).thenReturn(false);
    when(service.poll(0, TimeUnit.MILLISECONDS)).thenReturn(first);
    when(service.poll()).thenReturn(invalid, (WatchKey) null);
    var register = LocalFileWatcher.class.getDeclaredMethod("register", Path.class);
    register.setAccessible(true);
    var keysField = LocalFileWatcher.class.getDeclaredField("keys");
    keysField.setAccessible(true);
    var registeredField = LocalFileWatcher.class.getDeclaredField("registered");
    registeredField.setAccessible(true);
    try (LocalFileWatcher watcher = new LocalFileWatcher(fakeRoot, false, 1, 2)) {
      register.invoke(watcher, child);
      java.util.Map<?, ?> keys = (java.util.Map<?, ?>) keysField.get(watcher);
      java.util.Set<?> registered = (java.util.Set<?>) registeredField.get(watcher);
      assertTrue(keys.containsKey(invalid));
      assertTrue(registered.contains(child));
      assertTrue(watcher.poll(0).isReconciliationRequired());
      verify(invalid).pollEvents();
      verify(invalid).reset();
      assertEquals(1, keys.size());
      assertFalse(keys.containsKey(invalid));
      assertFalse(registered.contains(child));
      register.invoke(watcher, child);
      assertEquals(child, keys.get(replacement));
      assertTrue(registered.contains(child));
      assertEquals(2, keys.size());
    }
  }

  @Test
  void deletedDirectoryCanBeRegisteredAgainAndProducesRealNativeHints() throws Exception {
    Path child = Files.createDirectory(root.resolve("child"));
    try (LocalFileWatcher watcher = new LocalFileWatcher(root, true, 2, 2)) {
      var field = LocalFileWatcher.class.getDeclaredField("registered");
      field.setAccessible(true);
      java.util.Set<?> registered = (java.util.Set<?>) field.get(watcher);
      assertTrue(registered.contains(child));
      Files.delete(child);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      boolean reconcile = false;
      while (registered.contains(child) && System.nanoTime() < deadline) {
        reconcile |= watcher.poll(100).isReconciliationRequired();
      }
      assertTrue(reconcile);
      assertFalse(registered.contains(child));
      Files.createDirectory(child);
      deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      boolean recreated = false;
      while (!recreated && System.nanoTime() < deadline) {
        recreated = watcher.poll(100).isReconciliationRequired();
      }
      assertTrue(recreated, "Expected the parent directory's creation notification");
      watcher.reconcileDirectories();
      assertTrue(registered.contains(child));
      Path file = Files.writeString(child.resolve("recreated.csv"), "first");
      awaitPath(watcher, file);
      Files.writeString(file, "modified");
      awaitPath(watcher, file);
    }
  }
}
