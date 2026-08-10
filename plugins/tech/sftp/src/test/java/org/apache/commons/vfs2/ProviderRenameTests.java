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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** File system test that check that a file system can be renamed. */
public class ProviderRenameTests extends AbstractProviderTestCase {

  /** Sets up a scratch folder for the test to use. */
  protected FileObject createScratchFolder() throws Exception {
    final FileObject scratchFolder = getWriteFolder();

    // Make sure the test folder is empty
    scratchFolder.delete(Selectors.EXCLUDE_SELF);
    scratchFolder.createFolder();

    return scratchFolder;
  }

  private String createTestFile(final FileObject file)
      throws FileSystemException, IOException, UnsupportedEncodingException, Exception {
    // Create the source file
    final String content = "Here is some sample content for the file.  Blah Blah Blah.";

    try (OutputStream os = file.getContent().getOutputStream()) {
      os.write(content.getBytes(StandardCharsets.UTF_8));
    }
    assertSameContent(content, file);
    return content;
  }

  /** Returns the capabilities required by the tests of this test case. */
  @Override
  protected Capability[] getRequiredCapabilities() {
    return new Capability[] {
      Capability.CREATE,
      Capability.DELETE,
      Capability.GET_TYPE,
      Capability.LIST_CHILDREN,
      Capability.READ_CONTENT,
      Capability.WRITE_CONTENT,
      Capability.RENAME
    };
  }

  private void moveFile(final FileObject scratchFolder, final FileObject file, final String content)
      throws FileSystemException, Exception {
    final FileObject fileMove = scratchFolder.resolveFile("file1move.txt");
    assertFalse(fileMove.exists());

    file.moveTo(fileMove);

    assertFalse(file.exists());
    assertTrue(fileMove.exists());

    assertSameContent(content, fileMove);

    // Delete the file.
    assertTrue(fileMove.exists());
    assertTrue(fileMove.delete());
  }

  /** Tests create-delete-create-a-file sequence on the same file system. */
  @Test
  public void testRenameFile() throws Exception {
    final FileObject scratchFolder = createScratchFolder();

    // Create direct child of the test folder
    final FileObject file = scratchFolder.resolveFile("file1.txt");
    assertFalse(file.exists());

    final String content = createTestFile(file);

    // Make sure we can move the new file to another file on the same file system
    moveFile(scratchFolder, file, content);
  }

  /**
   * Moves a file from a child folder to a parent folder to test what happens when the original
   * folder is now empty.
   *
   * <p>See [VFS-298] FTP: Exception is thrown when renaming a file.
   */
  @Test
  public void testRenameFileAndLeaveFolderEmpty() throws Exception {
    final FileObject scratchFolder = createScratchFolder();
    final FileObject folder = scratchFolder.resolveFile("folder");
    folder.createFolder();
    assertTrue(folder.exists());
    final FileObject file = folder.resolveFile("file1.txt");
    assertFalse(file.exists());

    final String content = createTestFile(file);

    // Make sure we can move the new file to another file on the same file system
    moveFile(scratchFolder, file, content);
    assertEquals(0, folder.getChildren().length);
  }

  /**
   * Tests moving a file to empty folder.
   *
   * <p>This fails with VFS-558, but only with a CacheStrategy.ON_CALL.
   */
  @Test
  public void testRenameFileIntoEmptyFolder() throws Exception {
    try (FileObject scratchFolder = createScratchFolder();
        // Create direct child of the test folder
        final FileObject file = scratchFolder.resolveFile("file1.txt")) {
      assertFalse(file.exists());

      final String content = createTestFile(file);

      final FileObject destFolder = scratchFolder.resolveFile("empty-target-folder");
      destFolder.createFolder();
      assertTrue(destFolder.getType().hasChildren(), "new destination must be folder");
      assertEquals(0, destFolder.getChildren().length, "new destination must be empty");

      moveFile(destFolder, file, content);
    }
  }

  /** Tests create-delete-create-a-file sequence on the same file system. */
  @Test
  public void testRenameFileWithSpaces() throws Exception {
    final FileObject scratchFolder = createScratchFolder();

    // Create direct child of the test folder
    final FileObject file = scratchFolder.resolveFile("file space.txt");
    assertFalse(file.exists());

    final String content = createTestFile(file);

    // Make sure we can move the new file to another file on the same file system
    moveFile(scratchFolder, file, content);
  }
}
