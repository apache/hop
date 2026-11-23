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

package org.apache.hop.workflow.actions.checkfilelocked;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/** What the lock check answers for a file, and that the check leaves the file as it was. */
class LockFileTest {

  @TempDir Path folder;

  private final IVariables variables = new Variables();

  @Test
  void fileThatNobodyHoldsIsNotLocked() throws Exception {
    Path file = Files.writeString(folder.resolve("data.txt"), "data", StandardCharsets.UTF_8);

    LockFile lockFile = new LockFile(file.toString(), variables);

    assertFalse(lockFile.isLocked());
    assertEquals(file.toString(), lockFile.getFilename());
  }

  @Test
  void theCheckLeavesTheFileAsItWas() throws Exception {
    Path file = Files.writeString(folder.resolve("data.txt"), "data", StandardCharsets.UTF_8);
    BasicFileAttributes before = Files.readAttributes(file, BasicFileAttributes.class);

    // more than once: the first check must not leave anything behind that changes the second
    for (int i = 0; i < 3; i++) {
      assertFalse(new LockFile(file.toString(), variables).isLocked());
    }

    BasicFileAttributes after = Files.readAttributes(file, BasicFileAttributes.class);
    assertEquals("data", Files.readString(file, StandardCharsets.UTF_8));
    assertEquals(before.lastModifiedTime(), after.lastModifiedTime());
    assertEquals(before.fileKey(), after.fileKey());
    try (FileObject fileObject = HopVfs.getFileObject(file.toString(), variables)) {
      assertTrue(fileObject.exists(), "the file is still there for whoever resolves it next");
      assertTrue(fileObject.isFile());
    }
  }

  @Test
  void fileNameAsUrlIsChecked() throws Exception {
    Path file = Files.writeString(folder.resolve("data.txt"), "data", StandardCharsets.UTF_8);

    assertFalse(new LockFile(file.toUri().toString(), variables).isLocked());
    assertEquals("data", Files.readString(file, StandardCharsets.UTF_8));
  }

  @Test
  void missingFileIsNotLocked() throws Exception {
    Path missing = folder.resolve("missing.txt");

    assertFalse(new LockFile(missing.toString(), variables).isLocked());
    assertFalse(Files.exists(missing), "the check does not create the file");
  }

  @Test
  void folderIsNotLockedAndKeepsItsFiles() throws Exception {
    Path sub = Files.createDirectory(folder.resolve("sub"));
    Path child = Files.writeString(sub.resolve("child.txt"), "child", StandardCharsets.UTF_8);

    assertFalse(new LockFile(sub.toString(), variables).isLocked());
    assertEquals("child", Files.readString(child, StandardCharsets.UTF_8));
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void readOnlyFileInWritableFolderIsNotLocked() throws Exception {
    Path file = Files.writeString(folder.resolve("data.txt"), "data", StandardCharsets.UTF_8);
    Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("r--r--r--"));

    assertFalse(new LockFile(file.toString(), variables).isLocked());
    assertEquals("data", Files.readString(file, StandardCharsets.UTF_8));
  }

  /** A file that can not be renamed counts as locked: in a read-only folder that is every file. */
  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void fileInReadOnlyFolderIsLocked() throws Exception {
    Path readOnly = Files.createDirectory(folder.resolve("read-only"));
    Path file = Files.writeString(readOnly.resolve("data.txt"), "data", StandardCharsets.UTF_8);
    Files.setPosixFilePermissions(readOnly, PosixFilePermissions.fromString("r-xr-xr-x"));
    try {
      assumeFalse(
          Files.isWritable(readOnly), "folder is still writable, cannot run as this user (root?)");

      // more than once: a failed check must not change the answer of the next one
      for (int i = 0; i < 2; i++) {
        assertTrue(new LockFile(file.toString(), variables).isLocked());
      }
      assertEquals("data", Files.readString(file, StandardCharsets.UTF_8));
    } finally {
      Files.setPosixFilePermissions(readOnly, PosixFilePermissions.fromString("rwx------"));
    }
  }
}
