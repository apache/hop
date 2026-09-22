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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class VfsFileTransferTest {

  @AfterEach
  void tearDown() {
    VfsFileClipboard.clear();
    HopVfs.reset();
  }

  @Test
  void copyAndMoveKeepTheBaseNameAndRefuseAFolderIntoItself() throws Exception {
    FileObject root = HopVfs.getFileObject("ram:///vfs-transfer/");
    root.createFolder();
    FileObject sourceDir = root.resolveFile("source");
    sourceDir.createFolder();
    FileObject file = sourceDir.resolveFile("notes.txt");
    try (OutputStream out = file.getContent().getOutputStream()) {
      out.write("hello".getBytes(StandardCharsets.UTF_8));
    }
    FileObject nested = sourceDir.resolveFile("nested");
    nested.createFolder();
    FileObject dest = root.resolveFile("dest");
    dest.createFolder();

    assertEquals(VfsFileTransfer.Refusal.NONE, VfsFileTransfer.refusal(file, dest));
    VfsFileTransfer.transfer(file, dest, VfsFileTransfer.Mode.COPY, false);
    assertTrue(dest.resolveFile("notes.txt").exists());
    assertTrue(file.exists());

    assertEquals(VfsFileTransfer.Refusal.SAME, VfsFileTransfer.refusal(file, sourceDir));
    assertEquals(VfsFileTransfer.Refusal.INTO_ITSELF, VfsFileTransfer.refusal(sourceDir, nested));
    assertEquals(VfsFileTransfer.Refusal.EXISTS, VfsFileTransfer.refusal(file, dest));

    VfsFileTransfer.transfer(file, dest, VfsFileTransfer.Mode.MOVE, true);
    assertFalse(file.exists());
    assertTrue(dest.resolveFile("notes.txt").exists());
  }

  @Test
  void nestedSelectionKeepsTheParentOnly() {
    VfsFileTransfer.Entry parent = new VfsFileTransfer.Entry("ram:///a", "a", true);
    VfsFileTransfer.Entry child = new VfsFileTransfer.Entry("ram:///a/b", "b", true);
    VfsFileTransfer.Entry other = new VfsFileTransfer.Entry("ram:///c.txt", "c.txt", false);
    List<VfsFileTransfer.Entry> kept = VfsFileTransfer.withoutNested(List.of(child, parent, other));
    assertEquals(List.of("a", "c.txt"), kept.stream().map(VfsFileTransfer.Entry::getName).toList());
  }
}
