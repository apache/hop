/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.vfs.explorer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.stream.Stream;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VfsFileTransferTest {

  private static final VfsFileTransfer.ConflictResolver REPLACE =
      (source, target) -> VfsFileTransfer.Resolution.REPLACE;
  private static final VfsFileTransfer.ConflictResolver SKIP =
      (source, target) -> VfsFileTransfer.Resolution.SKIP;

  private FileObject root;

  @BeforeEach
  void setUp() throws Exception {
    root = HopVfs.getFileObject("ram:///vfs-transfer/");
    root.delete(org.apache.commons.vfs2.Selectors.SELECT_ALL);
    root.createFolder();
  }

  @AfterEach
  void tearDown() {
    HopVfs.reset();
  }

  @Test
  void copyAndMoveKeepTheBaseNameAndRefuseAFolderIntoItself() throws Exception {
    FileObject sourceDir = folder(root, "source");
    FileObject file = write(sourceDir, "notes.txt", "hello");
    FileObject nested = folder(sourceDir, "nested");
    FileObject dest = folder(root, "dest");

    assertEquals(VfsFileTransfer.Refusal.NONE, VfsFileTransfer.refusal(file, dest));
    VfsFileTransfer.transfer(file, dest, VfsFileTransfer.Mode.COPY, SKIP, null);
    assertTrue(dest.resolveFile("notes.txt").exists());
    assertTrue(file.exists());

    assertEquals(VfsFileTransfer.Refusal.SAME, VfsFileTransfer.refusal(file, sourceDir));
    assertEquals(VfsFileTransfer.Refusal.INTO_ITSELF, VfsFileTransfer.refusal(sourceDir, nested));
    assertEquals(VfsFileTransfer.Refusal.EXISTS, VfsFileTransfer.refusal(file, dest));

    VfsFileTransfer.transfer(file, dest, VfsFileTransfer.Mode.MOVE, REPLACE, null);
    assertFalse(file.exists());
    assertTrue(dest.resolveFile("notes.txt").exists());
  }

  @Test
  void replaceWritesTheNewContentAndLeavesNoTempFile() throws Exception {
    FileObject file = write(folder(root, "a"), "data.csv", "new");
    FileObject dest = folder(root, "b");
    write(dest, "data.csv", "old");

    VfsFileTransfer.transfer(file, dest, VfsFileTransfer.Mode.COPY, REPLACE, null);

    assertEquals("new", read(dest.resolveFile("data.csv")));
    assertEquals(List.of("data.csv"), names(dest));
  }

  @Test
  void aFailedReplaceKeepsTheOriginal() throws Exception {
    FileObject dest = folder(root, "b");
    FileObject target = write(dest, "data.csv", "old");
    FileObject missing = root.resolveFile("a/missing.csv");

    assertThrows(
        FileSystemException.class,
        () -> VfsFileTransfer.replaceFile(missing, target, VfsFileTransfer.Mode.COPY, null));

    assertEquals("old", read(target));
    assertEquals(List.of("data.csv"), names(dest));
  }

  @Test
  void aStoppedReplaceKeepsTheOriginal() throws Exception {
    FileObject source = write(folder(root, "a"), "data.csv", "new");
    FileObject dest = folder(root, "b");
    FileObject target = write(dest, "data.csv", "old");

    assertThrows(
        CancellationException.class,
        () -> VfsFileTransfer.replaceFile(source, target, VfsFileTransfer.Mode.MOVE, () -> true));

    assertEquals("old", read(target));
    assertTrue(source.exists());
    assertEquals(List.of("data.csv"), names(dest));
  }

  @Test
  void aFolderIsMergedNotReplaced() throws Exception {
    FileObject source = folder(root, "data");
    write(source, "new.txt", "new");
    write(source, "both.txt", "from source");
    FileObject dest = folder(root, "dest");
    FileObject existing = folder(dest, "data");
    write(existing, "keep.txt", "keep");
    write(existing, "both.txt", "from dest");

    List<String> asked = new ArrayList<>();
    VfsFileTransfer.transfer(
        source,
        dest,
        VfsFileTransfer.Mode.COPY,
        (from, to) -> {
          asked.add(to.getName().getBaseName());
          return VfsFileTransfer.Resolution.SKIP;
        },
        null);

    assertEquals(List.of("both.txt"), asked);
    assertEquals(List.of("both.txt", "keep.txt", "new.txt"), names(existing));
    assertEquals("from dest", read(existing.resolveFile("both.txt")));
    assertEquals("keep", read(existing.resolveFile("keep.txt")));
  }

  @Test
  void aMergedMoveKeepsSkippedFilesInTheSource() throws Exception {
    FileObject source = folder(root, "data");
    write(source, "new.txt", "new");
    write(source, "both.txt", "from source");
    FileObject dest = folder(root, "dest");
    write(folder(dest, "data"), "both.txt", "from dest");

    VfsFileTransfer.transfer(source, dest, VfsFileTransfer.Mode.MOVE, SKIP, null);

    assertEquals(List.of("both.txt"), names(source));
    assertEquals(List.of("both.txt", "new.txt"), names(dest.resolveFile("data")));

    VfsFileTransfer.transfer(source, dest, VfsFileTransfer.Mode.MOVE, REPLACE, null);
    assertFalse(source.exists());
    assertEquals("from source", read(dest.resolveFile("data/both.txt")));
  }

  @Test
  void cancelInThePromptStopsTheTransfer() throws Exception {
    FileObject file = write(folder(root, "a"), "data.csv", "new");
    FileObject dest = folder(root, "b");
    write(dest, "data.csv", "old");

    assertThrows(
        CancellationException.class,
        () ->
            VfsFileTransfer.transfer(
                file,
                dest,
                VfsFileTransfer.Mode.COPY,
                (source, target) -> VfsFileTransfer.Resolution.CANCEL,
                null));
    assertEquals("old", read(dest.resolveFile("data.csv")));
  }

  @Test
  void aFileDoesNotReplaceAFolder() throws Exception {
    FileObject file = write(folder(root, "a"), "data", "file");
    FileObject dest = folder(root, "b");
    folder(dest, "data");

    assertEquals(VfsFileTransfer.Refusal.OTHER_KIND, VfsFileTransfer.refusal(file, dest));
    VfsFileTransfer.RefusedException refused =
        assertThrows(
            VfsFileTransfer.RefusedException.class,
            () -> VfsFileTransfer.transfer(file, dest, VfsFileTransfer.Mode.COPY, REPLACE, null));
    assertEquals(VfsFileTransfer.Refusal.OTHER_KIND, refused.getRefusal());
    assertTrue(dest.resolveFile("data").isFolder());
  }

  @Test
  void progressCountsFilesAndBytes() throws Exception {
    FileObject source = folder(root, "data");
    write(source, "a.txt", "12345");
    write(folder(source, "sub"), "b.txt", "123");
    FileObject dest = folder(root, "dest");

    VfsExplorerOperation operation = new VfsExplorerOperation("copy", "");
    VfsFileTransfer.transfer(
        source, dest, VfsFileTransfer.Mode.COPY, SKIP, null, operation.progress());

    assertEquals(2, operation.getFilesDone());
    assertEquals(8L, operation.getBytesDone());
    assertEquals("123", read(dest.resolveFile("data/sub/b.txt")));
  }

  @Test
  void aStoppedCopyLeavesNoPartialFile() throws Exception {
    FileObject source = write(folder(root, "a"), "big.bin", "x".repeat(200_000));
    FileObject target = root.resolveFile("b/big.bin");
    folder(root, "b");
    int[] chunks = {0};

    assertThrows(
        CancellationException.class,
        () ->
            VfsFileTransfer.copy(
                source, target, () -> ++chunks[0] > 1, VfsFileTransfer.Progress.NONE));

    assertFalse(target.exists());
  }

  @Test
  void aStoppedFolderCopyRemovesTheFolderItCreated() throws Exception {
    FileObject source = folder(root, "data");
    write(source, "a.txt", "first");
    write(source, "b.txt", "second");
    FileObject dest = folder(root, "dest");
    int[] checks = {0};

    assertThrows(
        CancellationException.class,
        () ->
            VfsFileTransfer.transfer(
                source, dest, VfsFileTransfer.Mode.COPY, SKIP, () -> ++checks[0] > 4, null));

    assertEquals(List.of(), names(dest));
  }

  @Test
  void aCutWithSkipsAndRefusalsMovesOnlyTheRest() throws Exception {
    FileObject source = folder(root, "source");
    FileObject skipped = write(source, "skipped.txt", "new");
    FileObject moved = write(source, "moved.txt", "moved");
    FileObject dest = folder(root, "dest");
    write(dest, "skipped.txt", "old");
    List<VfsFileTransfer.Entry> entries =
        List.of(
            entry(skipped, false),
            entry(dest, true), // into itself: refused
            entry(root.resolveFile("source/missing.txt"), false), // fails
            entry(moved, false));

    VfsFileTransfer.BatchResult result =
        VfsFileTransfer.transferAll(
            entries, dest, VfsFileTransfer.Mode.MOVE, SKIP, null, null, HopVfs::getFileObject);

    assertFalse(result.isCancelled());
    assertEquals(2, result.getFailures().size());
    assertEquals(VfsFileTransfer.Refusal.INTO_ITSELF, result.getFailures().get(0).getRefusal());
    assertEquals("missing.txt", result.getFailures().get(1).getName());
    assertTrue(result.getFailures().get(1).getError() != null);
    // The moved file is gone from the source, and so is the one that was never there. The skipped
    // and refused entries stay on the clipboard, so the cut can be finished.
    assertEquals(
        List.of("skipped.txt", "dest"),
        result.getRemaining().stream().map(VfsFileTransfer.Entry::getName).toList());
    assertEquals("old", read(dest.resolveFile("skipped.txt")));
    assertEquals("moved", read(dest.resolveFile("moved.txt")));
    assertFalse(moved.exists());

    VfsFileClipboard clipboard = VfsFileClipboard.of(null);
    clipboard.set(VfsFileTransfer.Mode.MOVE, entries);
    clipboard.retain(clipboard.entries(), result.getRemaining());
    assertEquals(2, clipboard.entries().size());
    assertEquals(VfsFileTransfer.Mode.MOVE, clipboard.mode());
  }

  @Test
  void aStoppedBatchKeepsWhatWasNotMoved() throws Exception {
    FileObject source = folder(root, "source");
    FileObject first = write(source, "1.txt", "1");
    FileObject second = write(source, "2.txt", "2");
    FileObject dest = folder(root, "dest");
    List<VfsFileTransfer.Entry> entries = List.of(entry(first, false), entry(second, false));
    int[] checks = {0};

    VfsFileTransfer.BatchResult result =
        VfsFileTransfer.transferAll(
            entries,
            dest,
            VfsFileTransfer.Mode.MOVE,
            SKIP,
            () -> ++checks[0] > 3,
            null,
            HopVfs::getFileObject);

    assertTrue(result.isCancelled());
    assertFalse(first.exists());
    assertEquals(
        List.of("2.txt"),
        result.getRemaining().stream().map(VfsFileTransfer.Entry::getName).toList());
  }

  @Test
  void aCaseOnlyLocalNameIsTheSameFile() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-case");
    try {
      Path file = Files.writeString(dir.resolve("data.csv"), "hello");
      Path other = Files.writeString(dir.resolve("other.csv"), "x");
      FileObject source = HopVfs.getFileObject(file.toString());
      FileObject otherCase = source.getParent().resolveFile("Data.csv");
      FileObject different = HopVfs.getFileObject(other.toString());

      assertTrue(VfsFileTransfer.sameFile(source, source));
      assertFalse(VfsFileTransfer.sameFile(source, different));
      if (otherCase.exists()) {
        assertFalse(VfsFileTransfer.same(source, otherCase));
        assertTrue(VfsFileTransfer.sameFile(source, otherCase));
      } else {
        assertFalse(VfsFileTransfer.sameFile(source, otherCase));
      }
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void aSymbolicLinkIsNotTheSameFileAsItsTarget() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-link");
    try {
      Path target = Files.writeString(dir.resolve("a.csv"), "precious");
      Path link = symbolicLink(dir.resolve("link.csv"), target.getFileName());
      assertFalse(
          VfsFileTransfer.sameFile(
              HopVfs.getFileObject(link.toString()), HopVfs.getFileObject(target.toString())));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void aCaseOnlySymbolicLinkIsNotTheSameFile() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-case-link");
    try {
      Path stored = Files.writeString(dir.resolve("data.csv"), "hello");
      Path link;
      try {
        link = Files.createSymbolicLink(dir.resolve("Data.csv"), stored.getFileName());
      } catch (UnsupportedOperationException | IOException e) {
        abort("a distinct case-only symbolic link is not available: " + e.getMessage());
        return;
      }
      assertFalse(
          VfsFileTransfer.sameFile(
              HopVfs.getFileObject(link.toString()), HopVfs.getFileObject(stored.toString())));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void aDroppedPathInsideAnotherIsLeftOut() {
    assertEquals(
        List.of("/data", "/other.txt"),
        VfsFileTransfer.withoutNestedPaths(
            List.of("/data/in/a.txt", "/data", "/other.txt", "file:///data/in", "/other.txt")));
  }

  @Test
  void nestedSelectionKeepsTheParentOnly() {
    VfsFileTransfer.Entry parent = new VfsFileTransfer.Entry("ram:///a", "a", true);
    VfsFileTransfer.Entry child = new VfsFileTransfer.Entry("ram:///a/b", "b", true);
    VfsFileTransfer.Entry other = new VfsFileTransfer.Entry("ram:///c.txt", "c.txt", false);
    List<VfsFileTransfer.Entry> kept = VfsFileTransfer.withoutNested(List.of(child, parent, other));
    assertEquals(List.of("a", "c.txt"), kept.stream().map(VfsFileTransfer.Entry::getName).toList());
  }

  @Test
  void withoutADisplayEachClipboardIsItsOwn() {
    VfsFileClipboard first = VfsFileClipboard.of(null);
    first.set(
        VfsFileTransfer.Mode.MOVE,
        List.of(new VfsFileTransfer.Entry("ram:///a.txt", "a.txt", false)));
    VfsFileClipboard second = VfsFileClipboard.of(null);
    assertNotSame(first, second);
    assertTrue(second.isEmpty());
    assertFalse(first.isEmpty());
  }

  private static VfsFileTransfer.Entry entry(FileObject file, boolean folder) {
    return new VfsFileTransfer.Entry(
        HopVfs.getFilename(file), file.getName().getBaseName(), folder);
  }

  private static FileObject folder(FileObject parent, String name) throws Exception {
    FileObject folder = parent.resolveFile(name);
    folder.createFolder();
    return folder;
  }

  private static FileObject write(FileObject parent, String name, String text) throws Exception {
    FileObject file = parent.resolveFile(name);
    try (OutputStream out = file.getContent().getOutputStream()) {
      out.write(text.getBytes(StandardCharsets.UTF_8));
    }
    return file;
  }

  private static String read(FileObject file) throws Exception {
    try (InputStream in = file.getContent().getInputStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } finally {
      file.close();
    }
  }

  private static Path symbolicLink(Path link, Path relativeTarget) {
    try {
      return Files.createSymbolicLink(link, relativeTarget);
    } catch (UnsupportedOperationException | IOException e) {
      abort("symbolic links are not available: " + e.getMessage());
      return null;
    }
  }

  private static void deleteTree(Path dir) throws Exception {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(dir)) {
      for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
        Files.deleteIfExists(path);
      }
    }
  }

  private static List<String> names(FileObject folder) throws Exception {
    folder.refresh();
    List<String> names = new ArrayList<>();
    for (FileObject child : folder.getChildren()) {
      names.add(child.getName().getBaseName());
    }
    names.sort(String::compareTo);
    return names;
  }
}
