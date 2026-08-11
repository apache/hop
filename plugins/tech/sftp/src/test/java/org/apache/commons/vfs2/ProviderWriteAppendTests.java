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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** File system test that check that a file system can be modified. */
public class ProviderWriteAppendTests extends AbstractProviderTestCase {

  /** Sets up a scratch folder for the test to use. */
  protected FileObject createScratchFolder() throws Exception {
    final FileObject scratchFolder = getWriteFolder();

    // Make sure the test folder is empty
    scratchFolder.delete(Selectors.EXCLUDE_SELF);
    scratchFolder.createFolder();

    return scratchFolder;
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
      Capability.APPEND_CONTENT
    };
  }

  /** Tests create-delete-create-a-file sequence on the same file system. */
  @Test
  public void testAppendContent() throws Exception {
    try (FileObject scratchFolder = createScratchFolder();

        // Create direct child of the test folder
        final FileObject file = scratchFolder.resolveFile("file1.txt")) {
      assertFalse(file.exists());

      // Create the source file
      final String content = "Here is some sample content for the file.  Blah Blah Blah.";
      final String contentAppend = content + content;

      try (FileContent fileContent = file.getContent();
          OutputStream os = fileContent.getOutputStream()) {
        os.write(content.getBytes(StandardCharsets.UTF_8));
      }
      assertSameContent(content, file);

      // Append to the new file
      try (FileContent fileContent = file.getContent();
          OutputStream os2 = fileContent.getOutputStream(true)) {
        os2.write(content.getBytes(StandardCharsets.UTF_8));
      }
      assertSameContent(contentAppend, file);

      // Make sure we can copy the new file to another file on the same filesystem
      try (FileObject fileCopy = scratchFolder.resolveFile("file1copy.txt")) {
        assertFalse(fileCopy.exists());
        fileCopy.copyFrom(file, Selectors.SELECT_SELF);

        assertSameContent(contentAppend, fileCopy);

        // Delete the file.
        assertTrue(fileCopy.exists());
        assertTrue(fileCopy.delete());
      }
    }
  }

  /**
   * Tests append-write into a non-existing file.
   *
   * <p>See [VFS-807].
   */
  @Test
  public void testAppendToNonExisting() throws Exception {
    try (FileObject scratchFolder = createScratchFolder();

        // Create direct child of the test folder
        final FileObject file = scratchFolder.resolveFile("file2.txt")) {
      assertFalse(file.exists());

      // Create the source file
      final String content1 = "Here is some sample content for the file. Blah Blah Blah.";

      try (FileContent fileContent = file.getContent();
          OutputStream os = fileContent.getOutputStream(true)) {
        os.write(content1.getBytes(StandardCharsets.UTF_8));
      }
      assertSameContent(content1, file);
    }
  }
}
