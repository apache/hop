/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.vfs2;

import static org.apache.commons.vfs2.VfsTestUtils.assertSameMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** Test cases for reading file content. */
public class ContentTests extends AbstractProviderTestCase {

  /** Asserts every file in a folder exists and has the expected content. */
  private void assertSameContent(final FileInfo expected, final FileObject folder)
      throws Exception {
    for (final FileInfo fileInfo : expected.children.values()) {
      final FileObject child = folder.resolveFile(fileInfo.baseName, NameScope.CHILD);

      assertTrue(child.exists(), child.getName().toString());
      if (fileInfo.type == FileType.FILE) {
        assertSameContent(fileInfo.content, child);
      } else {
        assertSameContent(fileInfo, child);
      }
    }
  }

  /** Asserts that every expected file exists, and has the expected content. */
  @Test
  public void testAllContent() throws Exception {
    final FileInfo expectedFileInfo = buildExpectedStructure();
    final FileObject actualFolder = getReadFolder();

    assertSameContent(expectedFileInfo, actualFolder);
  }

  /** Tests attributes. */
  @Test
  public void testAttributes() throws FileSystemException {
    getReadFolder().getContent().getAttributes();
  }

  /** Tests that input streams are cleaned up on file close. */
  @Test
  public void testByteArrayReadAll() throws Exception {
    // Get the test file
    try (FileObject file = getReadFolder().resolveFile("file1.txt")) {
      assertEquals(FileType.FILE, file.getType());
      assertTrue(file.isFile());

      assertEquals(FILE1_CONTENT, new String(file.getContent().getByteArray()));
    }
  }

  /** Tests that children cannot be listed for non-folders. */
  @Test
  public void testChildren() throws FileSystemException {
    // Check for file
    FileObject file = getReadFolder().resolveFile("file1.txt");
    assertSame(FileType.FILE, file.getType());
    assertTrue(file.isFile());
    try {
      file.getChildren();
      fail();
    } catch (final FileSystemException e) {
      assertSameMessage("vfs.provider/list-children-not-folder.error", file, e);
    }

    // Should be able to get child by name
    file = file.resolveFile("some-child");
    assertNotNull(file);

    // Check for unknown file
    file = getReadFolder().resolveFile("unknown-file");
    assertFalse(file.exists());
    try {
      file.getChildren();
      fail();
    } catch (final FileSystemException e) {
      assertSameMessage("vfs.provider/list-children-not-folder.error", file, e);
    }

    // Should be able to get child by name
    final FileObject child = file.resolveFile("some-child");
    assertNotNull(child);
  }

  /** Tests content. */
  @Test
  public void testContent() throws Exception {
    // Test non-empty file
    FileObject file = getReadFolder().resolveFile("file1.txt");
    assertSameContent(FILE1_CONTENT, file);

    // Test empty file
    file = getReadFolder().resolveFile("empty.txt");
    assertSameContent("", file);
  }

  /** Tests existence determination. */
  @Test
  public void testExists() throws Exception {
    // Test a file
    FileObject file = getReadFolder().resolveFile("file1.txt");
    assertTrue(file.exists(), "file exists");
    assertNotSame(file.getType(), FileType.IMAGINARY, "file exists");

    // Test a folder
    file = getReadFolder().resolveFile("dir1");
    assertTrue(file.exists(), "folder exists");
    assertNotSame(file.getType(), FileType.IMAGINARY, "folder exists");

    // Test an unknown file
    file = getReadFolder().resolveFile("unknown-child");
    assertFalse(file.exists(), "unknown file does not exist");
    assertSame(file.getType(), FileType.IMAGINARY, "unknown file does not exist");

    // Test an unknown file in an unknown folder
    file = getReadFolder().resolveFile("unknown-folder/unknown-child");
    assertFalse(file.exists(), "unknown file does not exist");
    assertSame(file.getType(), FileType.IMAGINARY, "unknown file does not exist");
  }

  @Test
  public void testGetStringCharset() throws Exception {
    // Get the test file
    try (FileObject file = getReadFolder().resolveFile("file1.txt")) {
      assertEquals(FileType.FILE, file.getType());
      assertTrue(file.isFile());

      assertEquals(FILE1_CONTENT, file.getContent().getString(StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testGetStringString() throws Exception {
    // Get the test file
    try (FileObject file = getReadFolder().resolveFile("file1.txt")) {
      assertEquals(FileType.FILE, file.getType());
      assertTrue(file.isFile());

      assertEquals(FILE1_CONTENT, file.getContent().getString(StandardCharsets.UTF_8.name()));
    }
  }

  /** Tests that input streams are cleaned up on file close. */
  @Test
  public void testInputStreamMultipleCleanup() throws Exception {
    // Get the test file
    final FileObject file = getReadFolder().resolveFile("file1.txt");
    assertEquals(FileType.FILE, file.getType());
    assertTrue(file.isFile());

    // Open some input streams
    final InputStream instr1 = file.getContent().getInputStream();
    assertEquals(instr1.read(), FILE1_CONTENT.charAt(0));
    final InputStream instr2 = file.getContent().getInputStream();
    assertEquals(instr2.read(), FILE1_CONTENT.charAt(0));

    // Close the file
    file.close();

    // Check
    assertEquals(instr1.read(), -1);
    assertEquals(instr2.read(), -1);
  }

  /** Tests that input streams are cleaned up on file close. */
  @Test
  public void testInputStreamReadAll() throws Exception {
    // Get the test file
    try (FileObject file = getReadFolder().resolveFile("file1.txt")) {
      assertEquals(FileType.FILE, file.getType());
      assertTrue(file.isFile());

      final ByteArrayOutputStream output = new ByteArrayOutputStream();
      file.getContent().write(output);
      assertEquals(FILE1_CONTENT, new String(output.toByteArray()));
    }
  }

  /** Tests that input streams are cleaned up on file close. */
  @Test
  public void testInputStreamSingleCleanup() throws Exception {
    // Get the test file
    final FileObject file = getReadFolder().resolveFile("file1.txt");
    assertEquals(FileType.FILE, file.getType());
    assertTrue(file.isFile());

    // Open some input streams
    final InputStream instr1 = file.getContent().getInputStream();
    assertEquals(instr1.read(), FILE1_CONTENT.charAt(0));

    // Close the file
    file.close();

    // Check
    assertEquals(instr1.read(), -1);
  }

  /** Tests parent identity. */
  @Test
  public void testParent() throws FileSystemException {
    // Test when both exist
    FileObject folder = getReadFolder().resolveFile("dir1");
    FileObject child = folder.resolveFile("file3.txt");
    assertTrue(folder.exists(), "folder exists");
    assertTrue(child.exists(), "child exists");
    assertSame(folder, child.getParent());

    // Test when file does not exist
    child = folder.resolveFile("unknown-file");
    assertTrue(folder.exists(), "folder exists");
    assertFalse(child.exists(), "child does not exist");
    assertSame(folder, child.getParent());

    // Test when neither exists
    folder = getReadFolder().resolveFile("unknown-folder");
    child = folder.resolveFile("unknown-file");
    assertFalse(folder.exists(), "folder does not exist");
    assertFalse(child.exists(), "child does not exist");
    assertSame(folder, child.getParent());

    // Test the parent of the root of the file system
    // TODO - refactor out test cases for layered vs originating fs
    final FileSystem fileSystem = getFileSystem();
    final FileObject root = fileSystem.getRoot();
    if (fileSystem.getParentLayer() == null) {
      // No parent layer, so parent should be null
      assertNull(root.getParent(), "root has null parent");
    } else {
      // Parent should be parent of parent layer.
      assertSame(fileSystem.getParentLayer().getParent(), root.getParent());
    }
  }

  /** Tests concurrent reads on different files works. */
  @Test
  public void testReadMultipleConcurrent() throws Exception {
    final FileObject file = getReadFolder().resolveFile("file1.txt");
    assertTrue(file.exists());
    final FileObject emptyFile = getReadFolder().resolveFile("empty.txt");
    assertTrue(emptyFile.exists());

    // Start reading from the file
    try (InputStream instr = file.getContent().getInputStream()) {
      // Try to read from other file
      assertSameContent("", emptyFile);
    }
  }

  /** Tests concurrent reads on a file. */
  @Test
  public void testReadSingleConcurrent() throws Exception {
    final FileObject file = getReadFolder().resolveFile("file1.txt");
    assertTrue(file.exists());

    // Start reading from the file
    try (InputStream instr = file.getContent().getInputStream()) {
      // Start reading again
      file.getContent().getInputStream().close();
    }
  }

  /** Tests concurrent reads on a file. */
  @Test
  public void testReadSingleSequential() throws Exception {
    final FileObject file = getReadFolder().resolveFile("file1.txt");
    assertTrue(file.exists());

    file.getContent().getInputStream().close();
    file.getContent().getInputStream().close();
  }

  /** Tests that content and file objects are usable after being closed. */
  @Test
  public void testReuse() throws Exception {
    // Get the test file
    final FileObject file = getReadFolder().resolveFile("file1.txt");
    assertEquals(FileType.FILE, file.getType());
    assertTrue(file.isFile());

    // Get the file content
    assertSameContent(FILE1_CONTENT, file);

    // Read the content again
    assertSameContent(FILE1_CONTENT, file);

    // Close the content + file
    file.getContent().close();
    file.close();

    // Read the content again
    assertSameContent(FILE1_CONTENT, file);
  }

  private void testRoot(final FileObject root) throws FileSystemException {
    assertTrue(root.exists());
    assertNotSame(root.getType(), FileType.IMAGINARY);
  }

  /** Tests root of file system exists. */
  @Test
  public void testRootAPI() throws FileSystemException {
    if (!getProviderConfig().isFileSystemRootAccessible()) {
      return;
    }
    testRoot(getFileSystem().getRoot());
  }

  /** Tests root of file system exists. */
  @Test
  public void testRootURI() throws FileSystemException {
    if (!getProviderConfig().isFileSystemRootAccessible()) {
      return;
    }
    final FileSystem fileSystem = getFileSystem();
    final String uri = fileSystem.getRootURI();
    testRoot(getManager().resolveFile(uri, fileSystem.getFileSystemOptions()));
  }

  /** Tests that unknown files have no content. */
  @Test
  public void testUnknownContent() throws Exception {

    // Try getting the content of an unknown file
    final FileObject unknownFile = getReadFolder().resolveFile("unknown-file");
    final FileContent content = unknownFile.getContent();
    try {
      content.getInputStream();
      fail();
    } catch (final FileSystemException e) {
      assertSameMessage("vfs.provider/read-not-file.error", unknownFile, e);
    }
    try {
      content.getSize();
      fail();
    } catch (final FileSystemException e) {
      assertSameMessage("vfs.provider/get-size-not-file.error", unknownFile, e);
    }
  }
}
