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

package org.apache.hop.core.vfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Unit test for {@link HopVfs} */
class HopVfsTest {

  @AfterEach
  void tearDown() {
    // startsWithScheme(name, variables) bootstraps named VFS providers; clear so other core VFS
    // tests are not polluted by static HopVfs state.
    HopVfs.setBootstrapVariables(null);
    HopVfs.reset();
  }

  /**
   * Test to validate that startsWitScheme() returns true if the fileName starts with known protocol
   * like zip: jar: then it returns true else returns false
   */
  @Test
  void testStartsWithScheme() {
    String fileName =
        "zip:file:///SavedLinkedres.zip!Calculate median and percentiles using the group by transforms.hpl";
    assertTrue(HopVfs.startsWithScheme(fileName, new Variables()));

    fileName =
        "SavedLinkedres.zip!Calculate median and percentiles using the group by transforms.hpl";
    assertFalse(HopVfs.startsWithScheme(fileName, new Variables()));
  }

  @Test
  void testIsAbsolutePathRecognisesAbsoluteForms() {
    // POSIX
    assertTrue(HopVfs.isAbsolutePath("/home/me/project/test.hpl"));
    // Windows drive letter, both separators
    assertTrue(HopVfs.isAbsolutePath("C:/Users/me/test.hpl"));
    assertTrue(HopVfs.isAbsolutePath("C:\\Users\\me\\test.hpl"));
    // Windows UNC path
    assertTrue(HopVfs.isAbsolutePath("\\\\host\\share\\test.hpl"));
    // VFS URIs with a scheme
    assertTrue(HopVfs.isAbsolutePath("file:///home/me/test.hpl"));
    assertTrue(HopVfs.isAbsolutePath("s3://bucket/test.hpl"));
    // Tilde home directory paths (POSIX, Windows backslash, bare ~, file://~)
    assertTrue(HopVfs.isAbsolutePath("~"));
    assertTrue(HopVfs.isAbsolutePath("~/test.hpl"));
    assertTrue(HopVfs.isAbsolutePath("~\\test.hpl"));
    assertTrue(HopVfs.isAbsolutePath("file://~/test.hpl"));
    assertTrue(HopVfs.isAbsolutePath("file:~/test.hpl"));
  }

  @Test
  void testIsAbsolutePathRejectsRelativeForms() {
    assertFalse(HopVfs.isAbsolutePath(null));
    assertFalse(HopVfs.isAbsolutePath(""));
    assertFalse(HopVfs.isAbsolutePath("./test.hpl"));
    assertFalse(HopVfs.isAbsolutePath("test.hpl"));
    assertFalse(HopVfs.isAbsolutePath("sub/test.hpl"));
    // Windows drive-relative (no separator after the colon) is NOT an absolute path
    assertFalse(HopVfs.isAbsolutePath("C:test.hpl"));
    // Tilde followed by non-separator is not a home path
    assertFalse(HopVfs.isAbsolutePath("~test.hpl"));
  }

  @Test
  void testCheckForSchemeSuccess() {
    String[] schemes = {"hdfs"};
    String vfsFilename = "hdfs://company.com:8020/tmp/acltest/";

    boolean test = HopVfs.checkForScheme(schemes, true, vfsFilename);
    assertFalse(test);
  }

  @Test
  void testCheckForSchemeFail() {
    String[] schemes = {"file"};
    String vfsFilename = "hdfs://company.com:8020/tmp/acltest/";

    boolean test = HopVfs.checkForScheme(schemes, true, vfsFilename);
    assertTrue(test);
  }

  @Test
  void testRamFilesCache() throws Exception {
    String filename = "ram:///test-file.txt";
    FileObject fileObject = HopVfs.getFileObject(filename);

    assertNotNull(fileObject);
    try (OutputStream outputStream = fileObject.getContent().getOutputStream()) {
      outputStream.write("Test-content".getBytes());
    }
  }

  /**
   * Commons VFS caches directory children until {@link FileObject#refresh()}. Explorer hard-refresh
   * relies on an explicit refresh before re-listing so externally created files appear (issue
   * #7797).
   */
  @Test
  void testRefreshClearsChildrenCacheForExternalCreates() throws Exception {
    Path dir = Files.createTempDirectory("hop-vfs-children-");
    try {
      FileObject folder = HopVfs.getFileObject(dir.toAbsolutePath().toString());
      assertEquals(0, folder.getChildren().length);

      // Create a file outside this FileObject graph (same as a pipeline writing to disk)
      Files.writeString(dir.resolve("external.txt"), "created outside VFS");

      // Cached children may still look empty until refresh
      folder.refresh();
      FileObject[] children = folder.getChildren();
      assertEquals(1, children.length);
      assertEquals("external.txt", children[0].getName().getBaseName());
    } finally {
      Files.walk(dir)
          .sorted((a, b) -> b.compareTo(a))
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (Exception ignored) {
                  // best-effort cleanup
                }
              });
    }
  }

  @Test
  void testResolveHomeDirectory() {
    String userHome = System.getProperty("user.home");
    while (userHome.length() > 1 && (userHome.endsWith("/") || userHome.endsWith("\\"))) {
      userHome = userHome.substring(0, userHome.length() - 1);
    }

    // Bare ~
    assertEquals(userHome, HopVfs.resolveHomeDirectory("~"));

    // POSIX path
    assertEquals(userHome + "/project/file.txt", HopVfs.resolveHomeDirectory("~/project/file.txt"));

    // Windows backslash path
    assertEquals(
        userHome + "\\project\\file.txt", HopVfs.resolveHomeDirectory("~\\project\\file.txt"));

    // file:// and file: prefixes
    String filePrefixExpected =
        "file://" + (userHome.startsWith("/") ? "" : "/") + userHome + "/project/file.txt";
    assertEquals(filePrefixExpected, HopVfs.resolveHomeDirectory("file://~/project/file.txt"));
    assertEquals(
        "file:" + userHome + "/project/file.txt",
        HopVfs.resolveHomeDirectory("file:~/project/file.txt"));

    // Tilde not at the start should NOT be replaced
    assertEquals("/opt/hop/~", HopVfs.resolveHomeDirectory("/opt/hop/~"));
    assertEquals("/opt/hop/~/test", HopVfs.resolveHomeDirectory("/opt/hop/~/test"));
    assertEquals("foo~bar", HopVfs.resolveHomeDirectory("foo~bar"));
    assertEquals("s3://bucket/~/key", HopVfs.resolveHomeDirectory("s3://bucket/~/key"));

    // Tilde followed by non-separator characters should NOT be replaced
    assertEquals("~otheruser/dir", HopVfs.resolveHomeDirectory("~otheruser/dir"));
    assertEquals("~temp", HopVfs.resolveHomeDirectory("~temp"));

    // Null and empty
    assertEquals(null, HopVfs.resolveHomeDirectory(null));
    assertEquals("", HopVfs.resolveHomeDirectory(""));

    // Custom variable override
    Variables vars = new Variables();
    vars.setVariable("user.home", "/custom/home");
    assertEquals("/custom/home", HopVfs.resolveHomeDirectory("~", vars));
    assertEquals("/custom/home/sub/file.csv", HopVfs.resolveHomeDirectory("~/sub/file.csv", vars));
    assertEquals(
        "/custom/home\\sub\\file.csv", HopVfs.resolveHomeDirectory("~\\sub\\file.csv", vars));
  }

  @Test
  void testGetFileObjectWithTilde() throws Exception {
    String userHome = System.getProperty("user.home");
    FileObject homeObj = HopVfs.getFileObject("~");
    assertNotNull(homeObj);
    assertEquals(HopVfs.getFileObject(userHome).getName().getURI(), homeObj.getName().getURI());

    FileObject childObj = HopVfs.getFileObject("~/test-file-hop.txt");
    assertNotNull(childObj);
    assertEquals(
        HopVfs.getFileObject(userHome + "/test-file-hop.txt").getName().getURI(),
        childObj.getName().getURI());
  }
}
