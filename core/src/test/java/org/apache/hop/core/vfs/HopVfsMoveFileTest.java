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
package org.apache.hop.core.vfs;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.junit.vfs.CrossDeviceFileProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link HopVfs#moveFile(FileObject, FileObject)} is what everything in Hop which moves a file has
 * to use, because {@link FileObject#moveTo(FileObject)} on its own can't move a file out of one
 * mount into another - see <a href="https://github.com/apache/hop/issues/5936">issue #5936</a>.
 *
 * <p>{@link CrossDeviceFileProvider} stands in for those two mounts: one VFS file system, on which
 * a rename never succeeds.
 */
class HopVfsMoveFileTest {

  private static final String CROSS_DEVICE = "xdev";
  private static final String PAYLOAD = "id;amount\n1;42\n";

  @TempDir private Path tempDir;

  @BeforeEach
  void setUp() throws Exception {
    HopVfs.getFileSystemManager().addProvider(CROSS_DEVICE, new CrossDeviceFileProvider());
  }

  @AfterEach
  void tearDown() {
    // The provider above is registered on the one and only file system manager.
    HopVfs.reset();
  }

  @Test
  @DisplayName("a file lands in the destination folder when the rename fails")
  void movesAFileWhenTheRenameFails() throws Exception {
    FileObject source = write("xdev:///work/sales.csv", PAYLOAD);
    FileObject destination = HopVfs.getFileObject("xdev:///archive/sales.csv");

    HopVfs.moveFile(source, destination);

    assertTrue(destination.exists(), "the file was not moved");
    assertFalse(source.exists(), "the file is still in the source folder");
    assertEquals(PAYLOAD, read(destination));
  }

  /** What the file system manager hands out for the destination folder is the folder itself. */
  @Test
  @DisplayName("a folder arrives with its children when the rename fails")
  void movesAFolderWithItsChildren() throws Exception {
    write("xdev:///work/orders/january.csv", PAYLOAD);
    write("xdev:///work/orders/details/lines.csv", PAYLOAD);
    FileObject source = HopVfs.getFileObject("xdev:///work/orders");
    FileObject destination = HopVfs.getFileObject("xdev:///archive/orders");

    HopVfs.moveFile(source, destination);

    assertFalse(source.exists(), "the folder is still in the source folder");
    assertEquals(PAYLOAD, read(HopVfs.getFileObject("xdev:///archive/orders/january.csv")));
    assertEquals(PAYLOAD, read(HopVfs.getFileObject("xdev:///archive/orders/details/lines.csv")));
  }

  /** A rename which does work is left alone: no copy, no second file. */
  @Test
  @DisplayName("a file is renamed when the file system can rename")
  void renamesAFileWhenTheFileSystemCan() throws Exception {
    Path work = Files.createDirectory(tempDir.resolve("work"));
    Path archive = Files.createDirectory(tempDir.resolve("archive"));
    Files.writeString(work.resolve("sales.csv"), PAYLOAD);

    HopVfs.moveFile(
        HopVfs.getFileObject(work.resolve("sales.csv").toString()),
        HopVfs.getFileObject(archive.resolve("sales.csv").toString()));

    assertTrue(Files.exists(archive.resolve("sales.csv")), "the file was not moved");
    assertFalse(Files.exists(work.resolve("sales.csv")), "the file is still in the source folder");
    assertArrayEquals(
        PAYLOAD.getBytes(UTF_8), Files.readAllBytes(archive.resolve("sales.csv")), "content");
  }

  /**
   * When the copy can't save the move either, the caller gets the failure of the move it asked for,
   * with the failure of the copy alongside it.
   */
  @Test
  @DisplayName("the rename failure is what's reported when the copy fails too")
  void reportsTheRenameFailureWhenTheCopyFailsToo() throws Exception {
    FileObject source = HopVfs.getFileObject("xdev:///work/gone.csv");
    FileObject destination = HopVfs.getFileObject("xdev:///archive/gone.csv");

    FileSystemException e =
        assertThrows(FileSystemException.class, () -> HopVfs.moveFile(source, destination));

    assertTrue(e.getMessage().contains("rename"), e.getMessage());
    assertEquals(1, e.getSuppressed().length, "the copy failure should come along");
  }

  private static FileObject write(String uri, String content) throws Exception {
    FileObject fileObject = HopVfs.getFileObject(uri);
    try (OutputStream outputStream = fileObject.getContent().getOutputStream()) {
      outputStream.write(content.getBytes(UTF_8));
    }
    return fileObject;
  }

  private static String read(FileObject fileObject) throws Exception {
    try (InputStream inputStream = fileObject.getContent().getInputStream()) {
      return new String(inputStream.readAllBytes(), UTF_8);
    }
  }
}
