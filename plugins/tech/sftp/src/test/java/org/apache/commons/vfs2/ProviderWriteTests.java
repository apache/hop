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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** File system test that check that a file system can be modified. */
public class ProviderWriteTests extends AbstractProviderTestCase {

  /** A test listener. */
  private static class TestListener implements FileListener {
    private static final Object CREATE = "create";
    private static final Object DELETE = "delete";
    private static final Object CHANGED = "changed";
    private final FileObject file;
    private final ArrayList<Object> events = new ArrayList<>();

    public TestListener(final FileObject file) {
      this.file = file;
    }

    public void addCreateEvent() {
      events.add(CREATE);
    }

    public void addDeleteEvent() {
      events.add(DELETE);
    }

    public void assertFinished() {
      assertEquals(0, events.size(), "Missing event");
    }

    @Override
    public void fileChanged(final FileChangeEvent event) throws Exception {
      assertFalse(events.isEmpty(), "Unexpected changed event");
      assertSame(CHANGED, events.remove(0), "Expecting a changed event");
      assertEquals(file, event.getFileObject(), Objects.toString(file));
      try {
        assertFalse(file.exists(), Objects.toString(file));
      } catch (final FileSystemException e) {
        fail();
      }
    }

    /** Called when a file is created. */
    @Override
    public void fileCreated(final FileChangeEvent event) {
      assertFalse(events.isEmpty(), "Unexpected create event");
      assertSame(CREATE, events.remove(0), "Expecting a create event");
      assertEquals(file, event.getFileObject(), Objects.toString(file));
      try {
        assertTrue(file.exists(), Objects.toString(file));
      } catch (final FileSystemException e) {
        fail();
      }
    }

    /** Called when a file is deleted. */
    @Override
    public void fileDeleted(final FileChangeEvent event) {
      assertFalse(events.isEmpty(), "Unexpected delete event");
      assertSame(DELETE, events.remove(0), "Expecting a delete event");
      assertEquals(file, event.getFileObject(), Objects.toString(file));
      try {
        assertFalse(file.exists(), Objects.toString(file));
      } catch (final FileSystemException e) {
        fail();
      }
    }
  }

  /** Ensures the names of a set of files match an expected set. */
  private void assertSameFileSet(final Set<String> names, final FileObject[] files) {
    // Make sure the sets are the same length
    assertEquals(names.size(), files.length);

    // Check for unexpected names
    for (final FileObject file : files) {
      assertTrue(names.contains(file.getName().getBaseName()));
    }
  }

  /** Sets up a scratch folder for the test to use. */
  protected FileObject createScratchFolder() throws Exception {
    final FileObject scratchFolder = getWriteFolder();

    // Make sure the test folder is empty
    scratchFolder.delete(Selectors.EXCLUDE_SELF);
    scratchFolder.createFolder();

    return scratchFolder;
  }

  protected FileObject getReadFolderDir1() throws FileSystemException {
    return getReadFolder().resolveFile("dir1");
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
      Capability.WRITE_CONTENT
    };
  }

  /** Tests overwriting a file on the same file system. */
  @Test
  public void testCopyFromOverwriteSameFileSystem() throws Exception {
    final FileObject scratchFolder = createScratchFolder();

    // Create direct child of the test folder
    final FileObject file = scratchFolder.resolveFile("file1.txt");
    assertFalse(file.exists());

    // Create the source file
    final String content = "Here is some sample content for the file.  Blah Blah Blah.";
    try (OutputStream os = file.getContent().getOutputStream()) {
      os.write(content.getBytes(StandardCharsets.UTF_8));
    }

    assertSameContent(content, file);

    // Make sure we can copy the new file to another file on the same filesystem
    final FileObject fileCopy = scratchFolder.resolveFile("file1copy.txt");
    assertFalse(fileCopy.exists());
    fileCopy.copyFrom(file, Selectors.SELECT_SELF);

    assertSameContent(content, fileCopy);

    // Make sure we can copy the same new file to the same target file on the same filesystem
    assertTrue(fileCopy.exists());
    fileCopy.copyFrom(file, Selectors.SELECT_SELF);

    assertSameContent(content, fileCopy);
  }

  /** Tests file copy to and from the same file system type. This was a problem w/ FTP. */
  @Test
  public void testCopySameFileSystem() throws Exception {
    final FileObject scratchFolder = createScratchFolder();

    // Create direct child of the test folder
    final FileObject file = scratchFolder.resolveFile("file1.txt");
    assertFalse(file.exists());

    // Create the source file
    final String content = "Here is some sample content for the file.  Blah Blah Blah.";
    try (OutputStream os = file.getContent().getOutputStream()) {
      os.write(content.getBytes(StandardCharsets.UTF_8));
    }

    assertSameContent(content, file);

    // Make sure we can copy the new file to another file on the same filesystem
    final FileObject fileCopy = scratchFolder.resolveFile("file1copy.txt");
    assertFalse(fileCopy.exists());
    fileCopy.copyFrom(file, Selectors.SELECT_SELF);

    assertSameContent(content, fileCopy);
  }

  /** Tests create-delete-create-a-file sequence on the same file system. */
  @Test
  public void testCreateDeleteCreateSameFileSystem() throws Exception {
    final FileObject scratchFolder = createScratchFolder();

    // Create direct child of the test folder
    final FileObject file = scratchFolder.resolveFile("file1.txt");
    assertFalse(file.exists());

    // Create the source file
    final String content = "Here is some sample content for the file.  Blah Blah Blah.";
    try (OutputStream os = file.getContent().getOutputStream()) {
      os.write(content.getBytes(StandardCharsets.UTF_8));
    }

    assertSameContent(content, file);

    // Make sure we can copy the new file to another file on the same filesystem
    final FileObject fileCopy = scratchFolder.resolveFile("file1copy.txt");
    assertFalse(fileCopy.exists());
    fileCopy.copyFrom(file, Selectors.SELECT_SELF);

    assertSameContent(content, fileCopy);

    // Delete the file.
    assertTrue(fileCopy.exists());
    assertTrue(fileCopy.delete());

    // Make sure we can copy the same new file to the same target file on the same filesystem
    assertFalse(fileCopy.exists());
    fileCopy.copyFrom(file, Selectors.SELECT_SELF);

    assertSameContent(content, fileCopy);
  }

  /*
   Tests concurrent read and write on the same file fails.
  */
  /*
   * imario@apache.org leave this to some sort of LockManager public void testConcurrentReadWrite() throws Exception {
   * final FileObject scratchFolder = createScratchFolder();
   *
   * final FileObject file = scratchFolder.resolveFile("file1.txt"); file.createFile();
   *
   * // Start reading from the file final InputStream instr = file.getContent().getInputStream();
   *
   * try { // Try to write to the file file.getContent().getOutputStream(); fail(); } catch (final FileSystemException
   * e) { // Check error message assertSameMessage("vfs.provider/write-in-use.error", file, e); } finally {
   * instr.close(); } }
   */

  /*
   Tests concurrent writes on the same file fails.
  */
  /*
   * imario@apache.org leave this to some sort of LockManager public void testConcurrentWrite() throws Exception {
   * final FileObject scratchFolder = createScratchFolder();
   *
   * final FileObject file = scratchFolder.resolveFile("file1.txt"); file.createFile();
   *
   * // Start writing to the file final OutputStream outstr = file.getContent().getOutputStream(); final String
   * testContent = "some content"; try { // Write some content to the first stream
   * outstr.write(testContent.getBytes());
   *
   * // Try to open another output stream file.getContent().getOutputStream(); fail(); } catch (final
   * FileSystemException e) { // Check error message assertSameMessage("vfs.provider/write-in-use.error", file, e); }
   * finally { outstr.close(); }
   *
   * // Make sure that the content written to the first stream is actually applied assertSameContent(testContent,
   * file); }
   */

  /** Tests deletion. */
  @Test
  public void testDelete() throws Exception {
    // Set up the test structure
    final FileObject folder = createScratchFolder();
    folder.resolveFile("file1.txt").createFile();
    folder.resolveFile("file%25.txt").createFile();
    folder.resolveFile("emptydir").createFolder();
    folder.resolveFile("dir1/file1.txt").createFile();
    folder.resolveFile("dir1/dir2/file2.txt").createFile();

    // Delete a file
    FileObject file = folder.resolveFile("file1.txt");
    assertTrue(file.exists());
    file.deleteAll();
    assertFalse(file.exists());

    // Delete a special name file
    file = folder.resolveFile("file%25.txt");
    assertTrue(file.exists());
    file.deleteAll();
    assertFalse(file.exists());

    // Delete an empty folder
    file = folder.resolveFile("emptydir");
    assertTrue(file.exists());
    file.deleteAll();
    assertFalse(file.exists());

    // Recursive delete
    file = folder.resolveFile("dir1");
    final FileObject file2 = file.resolveFile("dir2/file2.txt");
    assertTrue(file.exists());
    assertTrue(file2.exists());
    file.deleteAll();
    assertFalse(file.exists());
    assertFalse(file2.exists());

    // Delete a file that does not exist
    file = folder.resolveFile("some-folder/some-file");
    assertFalse(file.exists());
    file.deleteAll();
    assertFalse(file.exists());
  }

  /** Tests deletion. */
  @Test
  public void testDeleteAllDescendants() throws Exception {
    // Set up the test structure
    final FileObject folder = createScratchFolder();
    folder.resolveFile("file1.txt").createFile();
    folder.resolveFile("file%25.txt").createFile();
    folder.resolveFile("emptydir").createFolder();
    folder.resolveFile("dir1/file1.txt").createFile();
    folder.resolveFile("dir1/dir2/file2.txt").createFile();

    // Delete a file
    FileObject file = folder.resolveFile("file1.txt");
    assertTrue(file.exists());
    file.deleteAll();
    assertFalse(file.exists());

    // Delete a special name file
    file = folder.resolveFile("file%25.txt");
    assertTrue(file.exists());
    file.deleteAll();
    assertFalse(file.exists());

    // Delete an empty folder
    file = folder.resolveFile("emptydir");
    assertTrue(file.exists());
    file.deleteAll();
    assertFalse(file.exists());

    // Recursive delete
    file = folder.resolveFile("dir1");
    final FileObject file2 = file.resolveFile("dir2/file2.txt");
    assertTrue(file.exists());
    assertTrue(file2.exists());
    file.deleteAll();
    assertFalse(file.exists());
    assertFalse(file2.exists());

    // Delete a file that does not exist
    file = folder.resolveFile("some-folder/some-file");
    assertFalse(file.exists());
    file.deleteAll();
    assertFalse(file.exists());
  }

  /** Tests file creation. */
  @Test
  public void testFileCreate() throws Exception {
    final FileObject scratchFolder = createScratchFolder();

    // Create direct child of the test folder
    FileObject file = scratchFolder.resolveFile("file1.txt");
    assertFalse(file.exists());
    file.createFile();
    assertTrue(file.exists());
    assertSame(FileType.FILE, file.getType());
    assertTrue(file.isFile());
    assertEquals(0, file.getContent().getSize());
    assertTrue(file.getContent().isEmpty());
    assertFalse(file.isHidden());
    assertFalse(file.isSymbolicLink());
    assertTrue(file.isReadable());
    assertTrue(file.isWriteable());

    // Create direct child of the test folder - special name
    file = scratchFolder.resolveFile("file1%25.txt");
    assertFalse(file.exists());
    file.createFile();
    assertTrue(file.exists());
    assertSame(FileType.FILE, file.getType());
    assertTrue(file.isFile());
    assertEquals(0, file.getContent().getSize());
    assertFalse(file.isHidden());
    assertTrue(file.isReadable());
    assertTrue(file.isWriteable());

    // Create a descendant, where the intermediate folders don't exist
    file = scratchFolder.resolveFile("dir1/dir1/file1.txt");
    assertFalse(file.exists());
    assertFalse(file.getParent().exists());
    assertFalse(file.getParent().getParent().exists());
    file.createFile();
    assertTrue(file.exists());
    assertSame(FileType.FILE, file.getType());
    assertTrue(file.isFile());
    assertEquals(0, file.getContent().getSize());
    assertTrue(file.getParent().exists());
    assertTrue(file.getParent().getParent().exists());
    assertFalse(file.getParent().isHidden());
    assertFalse(file.getParent().getParent().isHidden());

    // Test creating a file that already exists
    assertTrue(file.exists());
    file.createFile();
    assertTrue(file.exists());
    assertTrue(file.isReadable());
    assertTrue(file.isWriteable());
  }

  /** Tests file/folder creation with mismatched types. */
  @Test
  public void testFileCreateMismatched() throws Exception {
    final FileObject scratchFolder = createScratchFolder();

    // Create a test file and folder
    final FileObject file = scratchFolder.resolveFile("dir1/file1.txt");
    file.createFile();
    assertEquals(FileType.FILE, file.getType());
    assertTrue(file.isFile());

    final FileObject folder = scratchFolder.resolveFile("dir1/dir2");
    folder.createFolder();
    assertEquals(FileType.FOLDER, folder.getType());
    assertTrue(folder.isFolder());

    // Attempt to create a file that already exists as a folder
    assertThrows(FileSystemException.class, () -> folder.createFile());

    // Attempt to create a folder that already exists as a file
    assertThrows(FileSystemException.class, () -> file.createFolder());

    // Attempt to create a folder as a child of a file
    final FileObject folder2 = file.resolveFile("some-child");
    assertThrows(FileSystemException.class, () -> folder2.createFolder());
  }

  /** Tests folder creation. */
  @Test
  public void testFolderCreate() throws Exception {
    final FileObject scratchFolder = createScratchFolder();

    // Create direct child of the test folder
    FileObject folder = scratchFolder.resolveFile("dir1");
    assertFalse(folder.exists());
    folder.createFolder();
    assertTrue(folder.exists());
    assertSame(FileType.FOLDER, folder.getType());
    assertTrue(folder.isFolder());
    assertEquals(0, folder.getChildren().length);

    // Create a descendant, where the intermediate folders don't exist
    folder = scratchFolder.resolveFile("dir2/dir1/dir1");
    assertFalse(folder.exists());
    assertFalse(folder.getParent().exists());
    assertFalse(folder.getParent().getParent().exists());
    folder.createFolder();
    assertTrue(folder.exists());
    assertSame(FileType.FOLDER, folder.getType());
    assertTrue(folder.isFolder());
    assertEquals(0, folder.getChildren().length);
    assertTrue(folder.getParent().exists());
    assertTrue(folder.getParent().getParent().exists());

    // Test creating a folder that already exists
    assertTrue(folder.exists());
    folder.createFolder();
  }

  /** Tests that test read folder is not hidden. */
  @Test
  public void testFolderIsHidden() throws Exception {
    final FileObject folder = getReadFolderDir1();
    assertFalse(folder.isHidden());
  }

  /** Tests that test read folder is readable. */
  @Test
  public void testFolderIsReadable() throws Exception {
    final FileObject folder = getReadFolderDir1();
    assertTrue(folder.isReadable());
  }

  /** Tests that test folder is writable. */
  @Test
  public void testFolderIsWritable() throws Exception {
    final FileObject folder = getWriteFolder().resolveFile("dir1");
    assertTrue(folder.isWriteable());
  }

  /** Test that children are handled correctly by create and delete. */
  @Test
  public void testListChildren() throws Exception {
    final FileObject folder = createScratchFolder();
    final HashSet<String> names = new HashSet<>();

    // Make sure the folder is empty
    assertEquals(0, folder.getChildren().length);

    // Create a child folder
    folder.resolveFile("dir1").createFolder();
    names.add("dir1");
    assertSameFileSet(names, folder.getChildren());

    // Create a child file
    folder.resolveFile("file1.html").createFile();
    names.add("file1.html");
    assertSameFileSet(names, folder.getChildren());

    // Create a descendent
    folder.resolveFile("dir2/file1.txt").createFile();
    names.add("dir2");
    assertSameFileSet(names, folder.getChildren());

    // Create a child file via an output stream
    final OutputStream outstr = folder.resolveFile("file2.txt").getContent().getOutputStream();
    outstr.close();
    names.add("file2.txt");
    assertSameFileSet(names, folder.getChildren());

    // Delete a child folder
    folder.resolveFile("dir1").deleteAll();
    names.remove("dir1");
    assertSameFileSet(names, folder.getChildren());

    // Delete a child file
    folder.resolveFile("file1.html").deleteAll();
    names.remove("file1.html");
    assertSameFileSet(names, folder.getChildren());

    // Recreate the folder
    folder.deleteAll();
    folder.createFolder();
    assertEquals(0, folder.getChildren().length);
  }

  /** Check listeners are notified of changes. */
  @Test
  public void testListener() throws Exception {
    final FileObject baseFile = createScratchFolder();

    final FileObject child = baseFile.resolveFile("newfile.txt");
    assertFalse(child.exists());

    final FileSystem fs = baseFile.getFileSystem();
    final TestListener listener = new TestListener(child);
    fs.addListener(child, listener);
    try {
      // Create as a folder
      listener.addCreateEvent();
      child.createFolder();
      listener.assertFinished();

      // Create the folder again. Should not get an event.
      child.createFolder();

      // Delete
      listener.addDeleteEvent();
      child.delete();
      listener.assertFinished();

      // Delete again. Should not get an event
      child.delete();

      // Create as a file
      listener.addCreateEvent();
      child.createFile();
      listener.assertFinished();

      // Create the file again. Should not get an event
      child.createFile();

      listener.addDeleteEvent();
      child.delete();

      // Create as a file, by writing to it.
      listener.addCreateEvent();
      child.getContent().getOutputStream().close();
      listener.assertFinished();

      // Recreate the file by writing to it
      child.getContent().getOutputStream().close();

      // Copy another file over the top
      final FileObject otherChild = baseFile.resolveFile("folder1");
      otherChild.createFolder();
      listener.addDeleteEvent();
      listener.addCreateEvent();
      child.copyFrom(otherChild, Selectors.SELECT_SELF);
      listener.assertFinished();

    } finally {
      fs.removeListener(child, listener);
    }
  }

  /**
   * Tests overwriting the file.
   *
   * <p>See [VFS-807].
   */
  @Test
  public void testOverwriteContent() throws Exception {
    final FileObject scratchFolder = createScratchFolder();

    // Create direct child of the test folder
    final FileObject file = scratchFolder.resolveFile("file1.txt");
    assertFalse(file.exists());

    // Create the source file
    final String content1 = "Here is some sample content for the file. Blah Blah Blah.";

    try (OutputStream os = file.getContent().getOutputStream()) {
      os.write(content1.getBytes(StandardCharsets.UTF_8));
    }
    assertSameContent(content1, file);

    // VFS-807, part 1: verify that writing to the existing file overwrites its content!
    // content2 must be shorter than content1
    final String content2 = "0123456789 ABCD";

    try (OutputStream os = file.getContent().getOutputStream()) {
      os.write(content2.getBytes(StandardCharsets.UTF_8));
    }
    assertSameContent(content2, file);
  }

  /** Tests overwriting a file on the same file system. */
  @Test
  public void testOverwriteSameFileSystem() throws Exception {
    final FileObject scratchFolder = createScratchFolder();

    // Create direct child of the test folder
    final FileObject file = scratchFolder.resolveFile("file1.txt");
    assertFalse(file.exists());

    // Create the source file
    final String content = "Here is some sample content for the file.  Blah Blah Blah.";
    try (OutputStream os = file.getContent().getOutputStream()) {
      os.write(content.getBytes(StandardCharsets.UTF_8));
    }

    assertSameContent(content, file);

    // Make sure we can copy the new file to another file on the same file system
    final FileObject fileCopy = scratchFolder.resolveFile("file1copy.txt");
    assertFalse(fileCopy.exists());
    file.getContent().write(fileCopy);

    assertSameContent(content, fileCopy);

    // Make sure we can copy the same new file to the same target file on the same file system
    assertTrue(fileCopy.exists());
    file.getContent().write(fileCopy);

    assertSameContent(content, fileCopy);

    // Make sure we can copy the same new file to the same target file on the same file system
    assertTrue(fileCopy.exists());
    file.getContent().write(fileCopy.getContent());

    assertSameContent(content, fileCopy);

    // Make sure we can copy the same new file to the same target file on the same file system
    assertTrue(fileCopy.exists());
    OutputStream outputStream = fileCopy.getContent().getOutputStream();
    try {
      file.getContent().write(outputStream);
    } finally {
      outputStream.close();
    }
    assertSameContent(content, fileCopy);

    // Make sure we can copy the same new file to the same target file on the same file system
    assertTrue(fileCopy.exists());
    outputStream = fileCopy.getContent().getOutputStream();
    try {
      file.getContent().write(outputStream, 1234);
    } finally {
      outputStream.close();
    }
    assertSameContent(content, fileCopy);
  }

  /** Tests file write to and from the same file system type. */
  @Test
  public void testWriteSameFileSystem() throws Exception {
    final FileObject scratchFolder = createScratchFolder();

    // Create direct child of the test folder
    final FileObject fileSource = scratchFolder.resolveFile("file1.txt");
    assertFalse(fileSource.exists());

    // Create the source file
    final String expectedString = "Here is some sample content for the file.  Blah Blah Blah.";
    try (OutputStream expectedOutputStream = fileSource.getContent().getOutputStream()) {
      expectedOutputStream.write(expectedString.getBytes(StandardCharsets.UTF_8));
    }

    assertSameContent(expectedString, fileSource);

    // Make sure we can copy the new file to another file on the same filesystem
    final FileObject fileTarget = scratchFolder.resolveFile("file1copy.txt");
    assertFalse(fileTarget.exists());

    final FileContent contentSource = fileSource.getContent();
    //
    // Tests FileContent#write(FileContent)
    contentSource.write(fileTarget.getContent());
    assertSameContent(expectedString, fileTarget);
    //
    // Tests FileContent#write(OutputStream)
    OutputStream outputStream = fileTarget.getContent().getOutputStream();
    try {
      contentSource.write(outputStream);
    } finally {
      outputStream.close();
    }
    assertSameContent(expectedString, fileTarget);
    //
    // Tests FileContent#write(OutputStream, int)
    outputStream = fileTarget.getContent().getOutputStream();
    try {
      contentSource.write(outputStream, 1234);
    } finally {
      outputStream.close();
    }
    assertSameContent(expectedString, fileTarget);
  }
}
