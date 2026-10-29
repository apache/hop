/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopWindowsLocalFileSystemTest {

  @TempDir java.nio.file.Path tempDir;

  @Test
  void permissionShellIsMissingOnlyOnWindowsWithoutWinutilsOrNativeIo() {
    assertFalse(HopWindowsLocalFileSystem.permissionShellMissing(false, false, false));
    assertFalse(HopWindowsLocalFileSystem.permissionShellMissing(false, true, true));
    assertFalse(HopWindowsLocalFileSystem.permissionShellMissing(true, true, false));
    assertFalse(HopWindowsLocalFileSystem.permissionShellMissing(true, false, true));
    assertTrue(HopWindowsLocalFileSystem.permissionShellMissing(true, false, false));
    if (!Shell.WINDOWS) {
      assertFalse(HopWindowsLocalFileSystem.needed());
    }
  }

  @Test
  void createsADirectoryAndFileWithoutASeparatePermissionTool() throws Exception {
    HopWindowsLocalFileSystem fs = new HopWindowsLocalFileSystem();
    try {
      fs.initialize(URI.create("file:///"), new Configuration());
      assertInstanceOf(HopWindowsLocalFileSystem.WinutilsFreeRawLocalFileSystem.class, fs.getRaw());

      Path dir = new Path(tempDir.resolve("spark-out").toUri());
      assertTrue(fs.mkdirs(dir));
      Path file = new Path(dir, "part.txt");
      try (FSDataOutputStream out = fs.create(file, true)) {
        out.write("hello".getBytes(StandardCharsets.UTF_8));
      }
      fs.setPermission(file, FsPermission.getFileDefault());

      assertTrue(fs.exists(file));
      FileStatus status = fs.getFileStatus(file);
      assertEquals(5, status.getLen());
      assertEquals("hello", read(fs, file));
      if (HopWindowsLocalFileSystem.needed()) {
        assertEquals("", status.getOwner());
        assertEquals(FsPermission.getFileDefault(), status.getPermission());
      }
    } finally {
      fs.close();
    }
  }

  @Test
  void listsAFileUriWithJavaInsteadOfTheNativeWindowsAccessCheck() throws Exception {
    java.nio.file.Path localDir = tempDir.resolve("commit-out");
    java.nio.file.Files.createDirectories(localDir);
    java.nio.file.Files.writeString(localDir.resolve("part-00000.csv"), "a,b");
    java.nio.file.Files.writeString(localDir.resolve(".part-00000.csv.crc"), "crc");

    // The three-slash form is what a resolved VFS variable looks like (file:///C:/... on Windows).
    String fileUri = localDir.toUri().toString();
    assertTrue(fileUri.startsWith("file:///"), fileUri);
    Path dir = new Path(fileUri);

    HopWindowsLocalFileSystem fs = new HopWindowsLocalFileSystem(new JavaDirectoryListing());
    try {
      fs.initialize(URI.create("file:///"), new Configuration());
      FileStatus[] listed = fs.listStatus(dir);
      Set<String> names =
          Arrays.stream(listed)
              .map(status -> status.getPath().getName())
              .collect(Collectors.toSet());
      assertEquals(Set.of("part-00000.csv"), names);

      FileStatus[] oneFile = fs.listStatus(new Path(dir, "part-00000.csv"));
      assertEquals(1, oneFile.length);
      assertEquals("part-00000.csv", oneFile[0].getPath().getName());

      assertThrows(
          java.io.FileNotFoundException.class, () -> fs.listStatus(new Path(dir, "missing")));
    } finally {
      fs.close();
    }
  }

  @Test
  void javaDirectoryListingRefusesAnUnreadableDirectory() throws Exception {
    File directory = tempDir.resolve("hidden").toFile();
    assertTrue(directory.mkdir());
    directory.setReadable(false);
    try {
      Assumptions.assumeFalse(directory.canRead(), "this user can still read the directory");
      AccessDeniedException denied =
          assertThrows(
              AccessDeniedException.class,
              () ->
                  HopWindowsLocalFileSystem.WinutilsFreeRawLocalFileSystem.listLocalNames(
                      directory));
      assertTrue(denied.getMessage().contains(directory.toString()));
    } finally {
      directory.setReadable(true);
    }
  }

  /** Raw file system that always takes the Windows listing path, including on Linux. */
  private static final class JavaDirectoryListing
      extends HopWindowsLocalFileSystem.WinutilsFreeRawLocalFileSystem {

    @Override
    boolean skipNativeDirectoryListing() {
      return true;
    }
  }

  private static String read(HopWindowsLocalFileSystem fs, Path file) throws Exception {
    byte[] buf = new byte[5];
    try (FSDataInputStream in = fs.open(file)) {
      assertEquals(5, in.read(buf));
    }
    return new String(buf, StandardCharsets.UTF_8);
  }
}
