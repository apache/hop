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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileStateStoreTest {
  @TempDir Path directory;

  @Test
  void missingMetadataMustNotSilentlyBecomeAnEmptyFileVersion() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files(10));
    }
    Path state = directory.resolve("input.json");
    String invalid = Files.readString(state).replace("\"size\":10,", "");
    Files.writeString(state, invalid);
    try (JsonFileStateStore store = store()) {
      assertThrows(IOException.class, store::load);
      assertThrows(IOException.class, () -> store.save(files(20)));
    }
    assertEquals(invalid, Files.readString(state));
  }

  private JsonFileStateStore store() throws IOException {
    return new JsonFileStateStore(directory, "input", "root", "scope", 100);
  }

  private Map<String, FileState> files(long size) {
    return Map.of("a", new FileState("a", "a", "a", "/", "file", size, 100));
  }

  @Test
  void transientAccessDenialRetriesAtomicReplacementWithoutNonAtomicFallback() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    try (JsonFileStateStore store =
        new JsonFileStateStore(directory, "input", "root", "scope", 100) {
          @Override
          protected void atomicReplace() throws IOException {
            if (attempts.incrementAndGet() < 3) {
              throw new AccessDeniedException("state held open by a reader");
            }
            super.atomicReplace();
          }
        }) {
      store.save(files(20));
    }
    assertEquals(3, attempts.get());
    assertFalse(Files.exists(directory.resolve("input.json.commit")));
    try (JsonFileStateStore store = store()) {
      assertEquals(files(20), store.load());
    }
  }

  @Test
  void persistentAccessDenialFailsAfterBoundedRetriesAndKeepsPreviousCheckpoint() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files(10));
    }
    AtomicInteger attempts = new AtomicInteger();
    try (JsonFileStateStore store =
        new JsonFileStateStore(directory, "input", "root", "scope", 100) {
          @Override
          protected void atomicReplace() throws IOException {
            attempts.incrementAndGet();
            throw new AccessDeniedException("state is not replaceable");
          }
        }) {
      assertThrows(AccessDeniedException.class, () -> store.save(files(20)));
    }
    assertEquals(5, attempts.get());
    assertFalse(Files.exists(directory.resolve("input.json.commit")));
    try (JsonFileStateStore store = store()) {
      assertEquals(files(10), store.load());
    }
  }

  @Test
  void interruptDuringRetryPreservesInterruptAndCheckpoint() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files(10));
    }
    try (JsonFileStateStore store =
        new JsonFileStateStore(directory, "input", "root", "scope", 100) {
          @Override
          protected void atomicReplace() throws IOException {
            Thread.currentThread().interrupt();
            throw new AccessDeniedException("state is held open");
          }
        }) {
      try {
        assertThrows(java.io.InterruptedIOException.class, () -> store.save(files(20)));
        assertTrue(Thread.currentThread().isInterrupted());
      } finally {
        Thread.interrupted();
      }
    }
    try (JsonFileStateStore store = store()) {
      assertEquals(files(10), store.load());
    }
  }

  @Test
  void missingStateIsExplicitAndSaveReloadIsDurable() throws Exception {
    try (JsonFileStateStore store = store()) {
      assertNull(store.load());
      store.save(files(10));
      assertFalse(Files.exists(directory.resolve("input.json.tmp")));
      store.save(files(20));
    }
    try (JsonFileStateStore store = store()) {
      assertEquals(files(20), store.load());
    }
  }

  @Test
  void corruptStateIsNotOverwritten() throws Exception {
    String broken = "{broken";
    Files.writeString(directory.resolve("input.json"), broken);
    try (JsonFileStateStore store = store()) {
      IOException error = assertThrows(IOException.class, store::load);
      assertTrue(error.getMessage().contains("has not been overwritten"));
      assertThrows(IOException.class, () -> store.save(files(20)));
    }
    assertEquals(broken, Files.readString(directory.resolve("input.json")));
  }

  @Test
  void unsupportedVersionAndMismatchedScopeFail() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files(10));
    }
    String saved = Files.readString(directory.resolve("input.json"));
    Files.writeString(
        directory.resolve("input.json"), saved.replace("\"version\":2", "\"version\":999"));
    try (JsonFileStateStore store = store()) {
      assertThrows(IOException.class, store::load);
    }
    Files.writeString(directory.resolve("input.json"), saved);
    try (JsonFileStateStore store =
        new JsonFileStateStore(directory, "input", "other", "scope", 100)) {
      IOException error = assertThrows(IOException.class, store::load);
      assertTrue(error.getMessage().contains("Directory differs"));
      assertEquals(saved, Files.readString(directory.resolve("input.json")));
    }
    try (JsonFileStateStore store =
        new JsonFileStateStore(directory, "input", "root", "different", 100)) {
      IOException error = assertThrows(IOException.class, store::load);
      assertTrue(error.getMessage().contains("filters or Include subdirectories differ"));
      assertTrue(error.getMessage().contains("new Watch ID"));
      assertThrows(IOException.class, () -> store.save(files(20)));
      assertEquals(saved, Files.readString(directory.resolve("input.json")));
    }
    try (JsonFileStateStore store =
        new JsonFileStateStore(directory, "all-files", "root", "different", 100)) {
      assertNull(store.load());
      store.save(files(20));
      assertEquals(files(20), store.load());
    }
    assertEquals(saved, Files.readString(directory.resolve("input.json")));
  }

  @Test
  void simultaneousWritersAreRejectedAndLockCanBeReacquired() throws Exception {
    try (JsonFileStateStore first = store()) {
      assertThrows(IOException.class, this::store);
    }
    try (JsonFileStateStore second = store()) {
      assertNull(second.load());
    }
  }

  @Test
  void nonWritableDirectoryFailsWithoutCreatingCheckpoint() throws Exception {
    Path regularFile = Files.writeString(directory.resolve("not-a-directory"), "content");
    assertThrows(
        IOException.class,
        () -> new JsonFileStateStore(regularFile, "input", "root", "scope", 100));
    assertEquals("content", Files.readString(regularFile));
  }

  @Test
  void failedAtomicReplacementPreservesPreviousCheckpoint() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files(10));
    }
    try (JsonFileStateStore store =
        new JsonFileStateStore(directory, "input", "root", "scope", 100) {
          @Override
          protected void atomicReplace() throws IOException {
            throw new IOException("Injected access denial");
          }
        }) {
      assertThrows(IOException.class, () -> store.save(files(20)));
    }
    try (JsonFileStateStore store = store()) {
      assertEquals(files(10), store.load());
    }
  }

  @Test
  void safeFallbackKeepsBackupAndReloadsReplacement() throws Exception {
    try (JsonFileStateStore store = store()) {
      store.save(files(10));
    }
    try (JsonFileStateStore store =
        new JsonFileStateStore(directory, "input", "root", "scope", 100) {
          @Override
          protected void atomicReplace() throws IOException {
            throw new AtomicMoveNotSupportedException("tmp", "state", "Injected unsupported move");
          }
        }) {
      store.save(files(20));
    }
    assertTrue(Files.exists(directory.resolve("input.json.bak")));
    assertFalse(Files.exists(directory.resolve("input.json.commit")));
    try (JsonFileStateStore store = store()) {
      assertEquals(files(20), store.load());
    }
  }

  @Test
  void interruptedFallbackNeverStartsFreshOrOverwritesJournal() throws Exception {
    Files.writeString(directory.resolve("input.json.commit"), "intent");
    try (JsonFileStateStore store = store()) {
      assertThrows(IOException.class, store::load);
      assertThrows(IOException.class, () -> store.save(files(20)));
    }
    assertEquals("intent", Files.readString(directory.resolve("input.json.commit")));
    assertFalse(Files.exists(directory.resolve("input.json")));
  }

  @Test
  void invalidWatchIdCannotEscapeStateDirectory() {
    assertThrows(
        IOException.class,
        () -> new JsonFileStateStore(directory, "../escape", "root", "scope", 100));
  }
}
