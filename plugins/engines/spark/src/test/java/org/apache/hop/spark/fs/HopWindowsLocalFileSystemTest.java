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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;
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

  private static String read(HopWindowsLocalFileSystem fs, Path file) throws Exception {
    byte[] buf = new byte[5];
    try (FSDataInputStream in = fs.open(file)) {
      assertEquals(5, in.read(buf));
    }
    return new String(buf, StandardCharsets.UTF_8);
  }
}
