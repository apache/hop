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

package org.apache.hop.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TempFileUtilTest {

  private static boolean isPosix() {
    return FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
  }

  @Test
  void createsAnEmptyFileWithThePrefixAndSuffix() throws IOException {
    Path path = TempFileUtil.createTempFile("hop-unit-", ".tmp");
    try {
      assertTrue(Files.isRegularFile(path));
      assertEquals(0, Files.size(path));
      String name = path.getFileName().toString();
      assertTrue(name.startsWith("hop-unit-"), name);
      assertTrue(name.endsWith(".tmp"), name);
    } finally {
      Files.deleteIfExists(path);
    }
  }

  @Test
  void isReadableAndWritableByTheOwnerOnly() throws IOException {
    assumeTrue(isPosix(), "POSIX file permissions are not supported on this platform");
    Path path = TempFileUtil.createTempFile("hop-unit-", ".tmp");
    try {
      Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
      assertEquals(
          Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE), permissions);
    } finally {
      Files.deleteIfExists(path);
    }
  }

  @Test
  void generatesADistinctNameOnEveryCall() throws IOException {
    Path first = TempFileUtil.createTempFile("hop-unit-", ".tmp");
    Path second = TempFileUtil.createTempFile("hop-unit-", ".tmp");
    try {
      assertNotEquals(first, second);
    } finally {
      Files.deleteIfExists(first);
      Files.deleteIfExists(second);
    }
  }

  @Test
  void createTempFileObjectReturnsTheSameFile() throws IOException {
    File file = TempFileUtil.createTempFileObject("hop-unit-", ".tmp");
    try {
      assertTrue(file.isFile());
      if (isPosix()) {
        assertEquals(
            Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE),
            Files.getPosixFilePermissions(file.toPath()));
      }
    } finally {
      Files.deleteIfExists(file.toPath());
    }
  }
}
