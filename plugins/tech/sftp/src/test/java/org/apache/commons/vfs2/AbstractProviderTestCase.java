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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.UnsynchronizedByteArrayInputStream;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;

/**
 * File system test cases, which verifies the structure and naming functionality.
 *
 * <p>Works from a base folder, and assumes a particular structure under that base folder.
 *
 * <p>Not intended to be executed individually, but instead to be part of a test suite using {@link
 * AbstractProviderTestSuite}.
 *
 * <p><strong>Pure JUnit 5:</strong> This class uses JUnit 5 lifecycle methods ({@code @BeforeEach},
 * {@code @AfterEach}) for capability checking and cleanup. Tests are dynamically generated via
 * {@link AbstractProviderTestSuite}.
 */
public abstract class AbstractProviderTestCase {

  // Expected contents of "file1.txt"
  public static final String FILE1_CONTENT = "This is a test file.";
  // Expected contents of test files
  public static final String TEST_FILE_CONTENT = "A test file.";

  private boolean addEmptyDir;
  private FileObject baseFolder;
  private DefaultFileSystemManager manager;
  private Method method;
  private ProviderTestConfig providerConfig;
  private FileObject readFolder;
  private FileObject writeFolder;

  protected void addEmptyDir(final boolean addEmptyDir) {
    this.addEmptyDir = addEmptyDir;
  }

  /**
   * Asserts that the content of a file is the same as expected. Checks the length reported by
   * getSize() is correct, then reads the content as a byte stream and compares the result with the
   * expected content. Assumes files are encoded using UTF-8.
   */
  protected void assertSameContent(final String expected, final FileObject fileObject)
      throws Exception {
    // Check the file exists, and is a file
    assertTrue(fileObject.exists());
    assertSame(FileType.FILE, fileObject.getType());
    assertTrue(fileObject.isFile());
    // Get file content as a binary stream
    final byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);
    // Check lengths
    final FileContent content = fileObject.getContent();
    assertEquals(expectedBytes.length, content.getSize(), "same content length");
    // Compare input streams
    try (InputStream in = content.getInputStream()) {
      assertTrue(
          IOUtils.contentEquals(
              UnsynchronizedByteArrayInputStream.builder().setByteArray(expectedBytes).get(), in));
    }
  }

  /**
   * Asserts that the content of a file is the same as expected. Checks the length reported by
   * getContentLength() is correct, then reads the content as a byte stream and compares the result
   * with the expected content. Assumes files are encoded using UTF-8.
   */
  protected void assertSameURLContent(final String expected, final URLConnection urlConnection)
      throws Exception {
    // Get file content as a binary stream
    final byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);
    // Check lengths
    assertEquals(expectedBytes.length, urlConnection.getContentLength(), "same content length");
    // Compare input streams
    try (InputStream in = urlConnection.getInputStream()) {
      assertTrue(
          IOUtils.contentEquals(
              UnsynchronizedByteArrayInputStream.builder().setByteArray(expectedBytes).get(), in));
    }
  }

  /**
   * Builds the expected structure of the read tests folder.
   *
   * @throws FileSystemException (possibly)
   */
  protected FileInfo buildExpectedStructure() throws FileSystemException {
    // Build the expected structure
    final FileInfo base = new FileInfo(getReadFolder().getName().getBaseName(), FileType.FOLDER);
    base.addFile("file1.txt", FILE1_CONTENT);
    // file%.txt - test out encoding
    base.addFile("file%25.txt", FILE1_CONTENT);

    // file?test.txt - test out encoding (test.txt is not the queryString)
    // as we do not know if the current file provider we need to
    // ask it to normalize the name
    // todo: move this into the FileInfo class to do it generally?
    /*
     * webdav-bug?: didn't manage to get the "?" correctly through webdavlib FileSystemManager fsm =
     * getReadFolder().getFileSystem().getFileSystemManager(); FileName fn =
     * fsm.resolveName(getReadFolder().getName(), "file%3ftest.txt"); String baseName = fn.getBaseName();
     * base.addFile(baseName, FILE1_CONTENT);
     */
    base.addFile("file space.txt", FILE1_CONTENT);

    base.addFile("empty.txt", "");
    if (addEmptyDir) {
      base.addFolder("emptydir");
    }

    final FileInfo dir = base.addFolder("dir1");
    dir.addFile("file1.txt", TEST_FILE_CONTENT);
    dir.addFile("file2.txt", TEST_FILE_CONTENT);
    dir.addFile("file3.txt", TEST_FILE_CONTENT);

    final FileInfo subdir1 = dir.addFolder("subdir1");
    subdir1.addFile("file1.txt", TEST_FILE_CONTENT);
    subdir1.addFile("file2.txt", TEST_FILE_CONTENT);
    subdir1.addFile("file3.txt", TEST_FILE_CONTENT);

    final FileInfo subdir2 = dir.addFolder("subdir2");
    subdir2.addFile("file1.txt", TEST_FILE_CONTENT);
    subdir2.addFile("file2.txt", TEST_FILE_CONTENT);
    subdir2.addFile("file3.txt", TEST_FILE_CONTENT);

    final FileInfo subdir3 = dir.addFolder("subdir3");
    subdir3.addFile("file1.txt", TEST_FILE_CONTENT);
    subdir3.addFile("file2.txt", TEST_FILE_CONTENT);
    subdir3.addFile("file3.txt", TEST_FILE_CONTENT);

    final FileInfo subdir4 = dir.addFolder("subdir4.jar");
    subdir4.addFile("file1.txt", TEST_FILE_CONTENT);
    subdir4.addFile("file2.txt", TEST_FILE_CONTENT);
    subdir4.addFile("file3.txt", TEST_FILE_CONTENT);

    return base;
  }

  /**
   * JUnit 5 lifecycle method to check capabilities before each test. Uses Assumptions to skip tests
   * when capabilities are not met.
   */
  @BeforeEach
  public void checkCapabilitiesJunit5() throws FileSystemException {
    if (readFolder == null) {
      return;
    }

    final Capability[] caps = getRequiredCapabilities();
    if (caps != null) {
      for (final Capability cap : caps) {
        final FileSystem fs = getFileSystem();
        Assumptions.assumeTrue(
            fs.hasCapability(cap),
            () -> "Skipping test because file system does not have capability: " + cap);
      }
    }
  }

  /** JUnit 5 lifecycle method to verify file system is properly closed after each test. */
  @AfterEach
  public void checkFileSystemClosedJunit5() throws FileSystemException {
    if (readFolder != null && ((AbstractFileSystem) readFolder.getFileSystem()).isOpen()) {
      throw new IllegalStateException(
          getClass().getName() + ": filesystem has open streams after test");
    }
  }

  /**
   * creates a new uninitialized file system manager
   *
   * @throws Exception
   */
  protected DefaultFileSystemManager createManager() throws Exception {
    final DefaultFileSystemManager fs = getProviderConfig().getDefaultFileSystemManager();
    fs.setFilesCache(getProviderConfig().getFilesCache());
    getProviderConfig().prepare(fs);
    if (!fs.hasProvider("file")) {
      fs.addProvider("file", new DefaultLocalFileProvider());
    }
    return fs;
  }

  /**
   * Returns the base test folder. This is the parent of both the read test and write test folders.
   */
  public FileObject getBaseFolder() {
    return baseFolder;
  }

  /**
   * some provider config do some post-initialization in getBaseTestFolder. This is a hack to allow
   * access to this code for {@code createManager}
   */
  public FileObject getBaseTestFolder(final FileSystemManager fs) throws Exception {
    return providerConfig.getBaseTestFolder(fs);
  }

  protected FileSystem getFileSystem() {
    final FileObject readFolder = getReadFolder();
    assertNotNull(readFolder, "This test's read folder should not be null");
    return readFolder.getFileSystem();
  }

  /** Gets the file system manager used by this test. */
  protected DefaultFileSystemManager getManager() {
    return manager;
  }

  /** Gets the provider configuration. */
  public ProviderTestConfig getProviderConfig() {
    return providerConfig;
  }

  /** Gets the read test folder. */
  protected FileObject getReadFolder() {
    return readFolder;
  }

  /**
   * Gets the capabilities required by the tests of this test case. The tests are not run if the
   * provider being tested does not support all the required capabilities. Return null or an empty
   * array to always run the tests.
   *
   * <p>This implementation returns null.
   */
  protected Capability[] getRequiredCapabilities() {
    return null;
  }

  /** Gets the write test folder. */
  protected FileObject getWriteFolder() {
    return writeFolder;
  }

  /** Configures this test. */
  public void setConfig(
      final DefaultFileSystemManager manager,
      final ProviderTestConfig providerConfig,
      final FileObject baseFolder,
      final FileObject readFolder,
      final FileObject writeFolder) {
    this.manager = manager;
    this.providerConfig = providerConfig;
    this.baseFolder = baseFolder;
    this.readFolder = readFolder;
    this.writeFolder = writeFolder;
    assertNotNull(manager, "setConfig manager");
    assertNotNull(providerConfig, "setConfig providerConfig");
    assertNotNull(baseFolder, "setConfig baseFolder");
    assertNotNull(readFolder, "setConfig readFolder");
    assertNotNull(writeFolder, "setConfig writeFolder");
  }

  /** Sets the test method. */
  public void setMethod(final Method method) {
    this.method = method;
  }

  /**
   * Sets the write test folder.
   *
   * @param folder
   */
  protected void setWriteFolder(final FileObject folder) {
    writeFolder = folder;
  }

  @Override
  public String toString() {
    return "AbstractProviderTestCase [baseFolder="
        + baseFolder
        + ", readFolder="
        + readFolder
        + ", writeFolder="
        + writeFolder
        + ", manager="
        + manager
        + ", providerConfig="
        + providerConfig
        + ", method="
        + method
        + ", addEmptyDir="
        + addEmptyDir
        + "]";
  }
}
