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
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
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
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

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
  void deletingALinkToAFolderKeepsTheFilesItPointsTo() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-delete-link");
    try {
      Path real = Files.createDirectories(dir.resolve("real"));
      Path keep = Files.writeString(real.resolve("keep.txt"), "keep");
      Path view = Files.createDirectories(dir.resolve("view"));
      Path link = symbolicLink(view.resolve("link"), real);

      VfsFileTransfer.delete(HopVfs.getFileObject(link.toString()));

      assertFalse(Files.exists(link, LinkOption.NOFOLLOW_LINKS));
      assertEquals("keep", Files.readString(keep));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void deletingAFolderKeepsTheFilesOfALinkInsideIt() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-delete-folder-link");
    try {
      Path real = Files.createDirectories(dir.resolve("real"));
      Path keep = Files.writeString(real.resolve("keep.txt"), "keep");
      Path view = Files.createDirectories(dir.resolve("view"));
      Files.writeString(view.resolve("own.txt"), "own");
      symbolicLink(view.resolve("link"), real);

      VfsFileTransfer.delete(HopVfs.getFileObject(view.toString()));

      assertFalse(Files.exists(view, LinkOption.NOFOLLOW_LINKS));
      assertEquals("keep", Files.readString(keep));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void deletingABrokenLinkRemovesIt() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-delete-broken-link");
    try {
      Path link = symbolicLink(dir.resolve("link"), dir.resolve("gone"));

      VfsFileTransfer.delete(HopVfs.getFileObject(link.toString()));

      assertFalse(Files.exists(link, LinkOption.NOFOLLOW_LINKS));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void movingALinkToAnotherFileSystemKeepsTheFilesItPointsTo() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-move-link");
    try {
      Path real = Files.createDirectories(dir.resolve("real"));
      Path keep = Files.writeString(real.resolve("keep.txt"), "keep");
      Path link = symbolicLink(Files.createDirectories(dir.resolve("view")).resolve("link"), real);
      FileObject dest = folder(root, "dest");

      VfsFileTransfer.transfer(
          HopVfs.getFileObject(link.toString()), dest, VfsFileTransfer.Mode.MOVE, SKIP, null);

      assertEquals("keep", read(dest.resolveFile("link/keep.txt")));
      assertFalse(Files.exists(link, LinkOption.NOFOLLOW_LINKS));
      assertEquals("keep", Files.readString(keep));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void movingALinkOntoAnExistingFolderKeepsTheFilesItPointsTo() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-merge-link");
    try {
      Path real = Files.createDirectories(dir.resolve("real"));
      Path keep = Files.writeString(real.resolve("keep.txt"), "keep");
      Path link = symbolicLink(Files.createDirectories(dir.resolve("view")).resolve("link"), real);
      FileObject dest = folder(root, "dest");
      folder(dest, "link");

      VfsFileTransfer.transfer(
          HopVfs.getFileObject(link.toString()), dest, VfsFileTransfer.Mode.MOVE, SKIP, null);

      assertEquals("keep", read(dest.resolveFile("link/keep.txt")));
      assertFalse(Files.exists(link, LinkOption.NOFOLLOW_LINKS));
      assertEquals("keep", Files.readString(keep));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void aMergedLinkMoveWithASkippedFileKeepsTheLink() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-merge-link-skip");
    try {
      Path real = Files.createDirectories(dir.resolve("real"));
      Path keep = Files.writeString(real.resolve("keep.txt"), "keep");
      Path other = Files.writeString(real.resolve("other.txt"), "other");
      Path link = symbolicLink(Files.createDirectories(dir.resolve("view")).resolve("link"), real);
      FileObject dest = folder(root, "dest");
      write(folder(dest, "link"), "keep.txt", "existing");

      VfsFileTransfer.transfer(
          HopVfs.getFileObject(link.toString()), dest, VfsFileTransfer.Mode.MOVE, SKIP, null);

      assertEquals("existing", read(dest.resolveFile("link/keep.txt")));
      assertEquals("other", read(dest.resolveFile("link/other.txt")));
      assertTrue(Files.isSymbolicLink(link));
      assertEquals("keep", Files.readString(keep));
      assertEquals("other", Files.readString(other));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void copyingAFolderFollowsALinkToAnotherFolder() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-copy-link");
    try {
      Path real = Files.createDirectories(dir.resolve("real"));
      Files.writeString(real.resolve("keep.txt"), "keep");
      Path view = Files.createDirectories(dir.resolve("view"));
      symbolicLink(view.resolve("link"), real);
      FileObject dest = folder(root, "dest");

      VfsFileTransfer.transfer(
          HopVfs.getFileObject(view.toString()), dest, VfsFileTransfer.Mode.COPY, SKIP, null);

      assertEquals("keep", read(dest.resolveFile("view/link/keep.txt")));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void copyingALinkToItsOwnFolderSkipsItAndCopiesTheRest() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-copy-self");
    try {
      Path source = Files.createDirectories(dir.resolve("source"));
      Files.writeString(source.resolve("a.txt"), "a");
      Path self = symbolicLink(source.resolve("self"), Path.of("."));
      Path dest = Files.createDirectories(dir.resolve("dest"));
      List<String> skipped = new ArrayList<>();

      VfsFileTransfer.transfer(
          HopVfs.getFileObject(source.toString()),
          HopVfs.getFileObject(dest.toString()),
          VfsFileTransfer.Mode.COPY,
          SKIP,
          null,
          skipping(skipped));

      assertEquals(List.of(filename(self)), skipped);
      assertEquals("a", Files.readString(dest.resolve("source/a.txt")));
      assertFalse(Files.exists(dest.resolve("source/self"), LinkOption.NOFOLLOW_LINKS));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void copyingALinkToAParentFolderSkipsTheLoopAndCopiesTheRest() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-copy-parent");
    try {
      Path parent = Files.createDirectories(dir.resolve("parent"));
      Files.writeString(parent.resolve("big.txt"), "big");
      Path sub = Files.createDirectories(parent.resolve("sub"));
      Files.writeString(sub.resolve("own.txt"), "own");
      symbolicLink(sub.resolve("up"), Path.of(".."));
      Path dest = Files.createDirectories(dir.resolve("dest"));
      List<String> skipped = new ArrayList<>();

      VfsFileTransfer.transfer(
          HopVfs.getFileObject(sub.toString()),
          HopVfs.getFileObject(dest.toString()),
          VfsFileTransfer.Mode.COPY,
          SKIP,
          null,
          skipping(skipped));

      assertEquals(List.of(filename(sub.resolve("up").resolve("sub"))), skipped);
      assertEquals("own", Files.readString(dest.resolve("sub/own.txt")));
      assertEquals("big", Files.readString(dest.resolve("sub/up/big.txt")));
      assertFalse(Files.exists(dest.resolve("sub/up/sub")));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void copyingALinkIntoTheFolderItPointsToIsRefused() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-copy-into-target");
    try {
      Path real = Files.createDirectories(dir.resolve("real"));
      Path keep = Files.writeString(real.resolve("keep.txt"), "keep");
      Path link = symbolicLink(Files.createDirectories(dir.resolve("view")).resolve("link"), real);
      FileObject source = HopVfs.getFileObject(link.toString());
      FileObject destination = HopVfs.getFileObject(real.toString());

      assertEquals(
          VfsFileTransfer.Refusal.INTO_ITSELF, VfsFileTransfer.refusal(source, destination));
      VfsFileTransfer.RefusedException e =
          assertThrows(
              VfsFileTransfer.RefusedException.class,
              () ->
                  VfsFileTransfer.transfer(
                      source, destination, VfsFileTransfer.Mode.COPY, SKIP, null));

      assertEquals(VfsFileTransfer.Refusal.INTO_ITSELF, e.getRefusal());
      assertFalse(Files.exists(real.resolve("link")));
      assertEquals("keep", Files.readString(keep));
      assertTrue(Files.isSymbolicLink(link));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void movingAFolderOntoALinkToItselfIsRefused() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-move-onto-self");
    try {
      Path data = Files.createDirectories(dir.resolve("data/x"));
      Path keep = Files.writeString(data.resolve("keep.txt"), "keep");
      Path dest = Files.createDirectories(dir.resolve("dest"));
      symbolicLink(dest.resolve("x"), data);
      FileObject source = HopVfs.getFileObject(data.toString());
      FileObject destination = HopVfs.getFileObject(dest.toString());

      assertEquals(VfsFileTransfer.Refusal.SAME, VfsFileTransfer.refusal(source, destination));
      VfsFileTransfer.RefusedException e =
          assertThrows(
              VfsFileTransfer.RefusedException.class,
              () ->
                  VfsFileTransfer.transfer(
                      source, destination, VfsFileTransfer.Mode.MOVE, REPLACE, null));

      assertEquals(VfsFileTransfer.Refusal.SAME, e.getRefusal());
      assertEquals("keep", Files.readString(keep));
      assertTrue(Files.isSymbolicLink(dest.resolve("x")));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void movingAFolderIsRefusedWhereALinkInTheTargetLeadsBackIntoIt() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-move-nested-self");
    try {
      Path x = Files.createDirectories(dir.resolve("src/p/x"));
      Path keep = Files.writeString(x.resolve("keep.txt"), "keep");
      Path existing = Files.createDirectories(dir.resolve("dest/p"));
      symbolicLink(existing.resolve("x"), x);

      VfsFileTransfer.RefusedException e =
          assertThrows(
              VfsFileTransfer.RefusedException.class,
              () ->
                  VfsFileTransfer.transfer(
                      HopVfs.getFileObject(dir.resolve("src/p").toString()),
                      HopVfs.getFileObject(dir.resolve("dest").toString()),
                      VfsFileTransfer.Mode.MOVE,
                      REPLACE,
                      null));

      assertEquals(VfsFileTransfer.Refusal.SAME, e.getRefusal());
      assertEquals("keep", Files.readString(keep));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void aStoppedLinkMoveIntoAFolderRemovesWhatItCopied() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-merge-link-stop");
    try {
      Path real = Files.createDirectories(dir.resolve("real"));
      Files.writeString(real.resolve("a.txt"), "a");
      Files.writeString(real.resolve("b.txt"), "b");
      Files.writeString(real.resolve("c.txt"), "c");
      Path link = symbolicLink(Files.createDirectories(dir.resolve("view")).resolve("link"), real);
      FileObject dest = folder(root, "dest");
      write(folder(dest, "link"), "own.txt", "own");
      int[] files = {0};
      VfsFileTransfer.Progress counting =
          new VfsFileTransfer.Progress() {
            @Override
            public void bytes(long count) {
              // Not counted.
            }

            @Override
            public void fileDone() {
              files[0]++;
            }
          };

      assertThrows(
          CancellationException.class,
          () ->
              VfsFileTransfer.transfer(
                  HopVfs.getFileObject(link.toString()),
                  dest,
                  VfsFileTransfer.Mode.MOVE,
                  SKIP,
                  () -> files[0] >= 1,
                  counting));

      assertEquals(1, files[0]);
      assertEquals(List.of("own.txt"), names(dest.resolveFile("link")));
      assertTrue(Files.isSymbolicLink(link));
      assertEquals(List.of("a.txt", "b.txt", "c.txt"), sorted(real));
    } finally {
      deleteTree(dir);
    }
  }

  @Test
  void aMoveByCopyCreatesTheLinksAgain() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-move-by-copy");
    try {
      Path real = Files.createDirectories(dir.resolve("real"));
      Path keep = Files.writeString(real.resolve("keep.txt"), "keep");
      Path source = Files.createDirectories(dir.resolve("home/a"));
      Files.writeString(source.resolve("own.txt"), "own");
      symbolicLink(source.resolve("data"), real);
      Path target = Files.createDirectories(dir.resolve("other")).resolve("a");

      VfsFileTransfer.moveByCopy(
          HopVfs.getFileObject(source.toString()),
          HopVfs.getFileObject(target.toString()),
          null,
          null);

      assertFalse(Files.exists(source, LinkOption.NOFOLLOW_LINKS));
      assertEquals("own", Files.readString(target.resolve("own.txt")));
      assertTrue(Files.isSymbolicLink(target.resolve("data")));
      assertEquals(real, Files.readSymbolicLink(target.resolve("data")));
      assertEquals("keep", Files.readString(keep));
      assertEquals(List.of("keep.txt"), sorted(real));
    } finally {
      deleteTree(dir);
    }
  }

  /**
   * Commons VFS lets a rename try to move a folder to another local disk, where it fails. Runs when
   * the temporary folder and the build folder are on different disks.
   */
  @Test
  void aMoveToAnotherDiskKeepsTheLinks() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-move-disk");
    Path other = Files.createTempDirectory(Path.of("target").toAbsolutePath(), "hop-vfs-move-disk");
    try {
      assumeFalse(
          Files.getFileStore(dir).equals(Files.getFileStore(other)),
          "the temporary folder and the build folder are on one disk");
      Path real = Files.createDirectories(dir.resolve("real"));
      Path keep = Files.writeString(real.resolve("keep.txt"), "keep");
      Path source = Files.createDirectories(dir.resolve("a"));
      Files.writeString(source.resolve("own.txt"), "own");
      symbolicLink(source.resolve("data"), real);

      VfsFileTransfer.transfer(
          HopVfs.getFileObject(source.toString()),
          HopVfs.getFileObject(other.toString()),
          VfsFileTransfer.Mode.MOVE,
          SKIP,
          null);

      assertFalse(Files.exists(source, LinkOption.NOFOLLOW_LINKS));
      assertEquals("own", Files.readString(other.resolve("a/own.txt")));
      assertTrue(Files.isSymbolicLink(other.resolve("a/data")));
      assertEquals("keep", Files.readString(keep));
    } finally {
      deleteTree(dir);
      deleteTree(other);
    }
  }

  @Test
  @EnabledOnOs(OS.WINDOWS)
  void deletingAFolderKeepsTheFilesOfAJunctionInsideIt() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-delete-junction");
    try {
      Path real = Files.createDirectories(dir.resolve("real"));
      Path keep = Files.writeString(real.resolve("keep.txt"), "keep");
      Path view = Files.createDirectories(dir.resolve("view"));
      Path junction = view.resolve("junction");
      Process mklink =
          new ProcessBuilder("cmd", "/c", "mklink", "/J", junction.toString(), real.toString())
              .redirectErrorStream(true)
              .start();
      assertEquals(0, mklink.waitFor());
      assertEquals(junction, VfsFileTransfer.localLink(HopVfs.getFileObject(junction.toString())));

      VfsFileTransfer.delete(HopVfs.getFileObject(view.toString()));

      assertFalse(Files.exists(view, LinkOption.NOFOLLOW_LINKS));
      assertEquals("keep", Files.readString(keep));
    } finally {
      deleteTree(dir);
    }
  }

  private static VfsFileTransfer.Progress skipping(List<String> skipped) {
    return new VfsFileTransfer.Progress() {
      @Override
      public void bytes(long count) {
        // Not counted.
      }

      @Override
      public void fileDone() {
        // Not counted.
      }

      @Override
      public void linkLoopSkipped(String folder) {
        skipped.add(folder);
      }
    };
  }

  private static String filename(Path path) throws Exception {
    return HopVfs.getFilename(HopVfs.getFileObject(path.toString()));
  }

  private static List<String> sorted(Path folder) throws Exception {
    try (Stream<Path> paths = Files.list(folder)) {
      return paths.map(path -> path.getFileName().toString()).sorted().toList();
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
