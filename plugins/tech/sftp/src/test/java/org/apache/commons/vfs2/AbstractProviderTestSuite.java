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

import static org.apache.commons.vfs2.VfsTestUtils.getTestDirectory;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.vfs2.impl.DefaultFileReplicator;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.PrivilegedFileReplicator;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;

/**
 * JUnit 5 base class for provider test suites.
 *
 * <p>This class replaces the JUnit 3 {@link AbstractTestSuite} with a pure JUnit 5 implementation
 * using {@code @TestFactory} for dynamic test generation.
 *
 * <p>Subclasses should override {@link #addBaseTests()} to add test classes to the suite.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractProviderTestSuite {

  public static final String WRITE_TESTS_FOLDER = "write-tests";
  public static final String READ_TESTS_FOLDER = "read-tests";

  private final ProviderTestConfig providerConfig;
  private final String prefix;
  private final boolean addEmptyDir;
  private final List<Class<?>> testClasses = new ArrayList<>();

  protected FileObject baseFolder;
  protected FileObject readFolder;
  protected FileObject writeFolder;
  protected DefaultFileSystemManager manager;
  private File tempDir;
  private boolean isSetUp = false;

  protected AbstractProviderTestSuite(
      final ProviderTestConfig providerConfig, final String prefix) {
    this(providerConfig, prefix, false);
  }

  protected AbstractProviderTestSuite(
      final ProviderTestConfig providerConfig, final String prefix, final boolean addEmptyDir) {
    this.providerConfig = providerConfig;
    this.prefix = prefix;
    this.addEmptyDir = addEmptyDir;
  }

  /**
   * Adds base tests - excludes the nested test cases. Subclasses should override this method to add
   * test classes via {@link #addTests(Class)}.
   */
  protected abstract void addBaseTests() throws Exception;

  /** Adds the tests from a class to this suite. */
  protected void addTests(final Class<?> testClass) throws Exception {
    // Verify the class
    if (!AbstractProviderTestCase.class.isAssignableFrom(testClass)) {
      throw new Exception(
          "Test class "
              + testClass.getName()
              + " is not assignable to "
              + AbstractProviderTestCase.class.getName());
    }
    testClasses.add(testClass);
  }

  /** Asserts that the temp dir is empty or gone. */
  private void checkTempDir(final String assertMsg) {
    if (tempDir != null && tempDir.exists()) {
      assertTrue(
          tempDir.isDirectory() && ArrayUtils.isEmpty(tempDir.list()),
          assertMsg + " (" + tempDir.getAbsolutePath() + ")");
    }
  }

  /** Creates dynamic tests for a single test class. */
  private Stream<DynamicTest> createTestsForClass(final Class<?> testClass) throws Exception {
    final List<DynamicTest> tests = new ArrayList<>();

    // Locate the test methods
    final Method[] methods = testClass.getMethods();
    for (final Method method : methods) {
      if (!method.getName().startsWith("test")
          || Modifier.isStatic(method.getModifiers())
          || method.getReturnType() != Void.TYPE
          || method.getParameterTypes().length != 0) {
        continue;
      }

      // Create a dynamic test for this method
      final String testName = prefix + method.getName();
      tests.add(
          DynamicTest.dynamicTest(
              testName,
              () -> {
                // Create test instance
                final AbstractProviderTestCase testCase =
                    (AbstractProviderTestCase) testClass.getConstructor().newInstance();
                testCase.addEmptyDir(addEmptyDir);
                testCase.setConfig(manager, providerConfig, baseFolder, readFolder, writeFolder);

                // Check capabilities before running the test
                final Capability[] caps = testCase.getRequiredCapabilities();
                if (caps != null) {
                  final FileSystem fs = testCase.getFileSystem();
                  for (final Capability cap : caps) {
                    if (!fs.hasCapability(cap)) {
                      // Skip test if capability is not supported
                      assumeTrue(
                          false,
                          "Skipping test because file system does not have capability: " + cap);
                    }
                  }
                }

                // Run the test method
                try {
                  method.invoke(testCase);
                } catch (final java.lang.reflect.InvocationTargetException e) {
                  throw e.getTargetException();
                }

                // Check that file system is properly closed
                if (readFolder != null
                    && ((org.apache.commons.vfs2.provider.AbstractFileSystem)
                            readFolder.getFileSystem())
                        .isOpen()) {
                  throw new IllegalStateException(
                      testClass.getName()
                          + ": filesystem has open streams after: "
                          + method.getName());
                }
              }));
    }

    return tests.stream();
  }

  protected DefaultFileSystemManager getManager() {
    return manager;
  }

  /** Gets the read test folder. */
  protected FileObject getReadFolder() {
    return readFolder;
  }

  /** Gets the write test folder. */
  protected FileObject getWriteFolder() {
    return writeFolder;
  }

  /** Creates dynamic tests for all test classes added via {@link #addTests(Class)}. */
  @TestFactory
  Stream<DynamicTest> providerTests() throws Exception {
    // Ensure setUp() has been called
    // Note: @TestFactory is evaluated before @BeforeAll in some JUnit 5 versions
    setUp();

    // Add test classes if not already added
    if (testClasses.isEmpty()) {
      addBaseTests();
    }

    // If no test classes were added and baseFolder is null, return empty stream
    // This allows tests with only @Test methods (no base tests) to run
    if (testClasses.isEmpty() && baseFolder == null) {
      return Stream.empty();
    }

    if (testClasses.isEmpty()) {
      fail("No test classes added");
    }

    return testClasses.stream()
        .flatMap(
            testClass -> {
              try {
                return createTestsForClass(testClass);
              } catch (final Exception e) {
                throw new RuntimeException("Failed to create tests for " + testClass.getName(), e);
              }
            });
  }

  @BeforeAll
  protected void setUp() throws Exception {
    if (isSetUp) {
      return;
    }
    isSetUp = true;

    // Locate the temp directory, and clean it up
    tempDir = getTestDirectory("temp");
    FileUtils.cleanDirectory(tempDir);
    checkTempDir("Temp dir not empty before test");

    // Create the file system manager
    manager = providerConfig.getDefaultFileSystemManager();
    manager.setFilesCache(providerConfig.getFilesCache());

    final DefaultFileReplicator replicator = new DefaultFileReplicator(tempDir);
    manager.setReplicator(new PrivilegedFileReplicator(replicator));
    manager.setTemporaryFileStore(replicator);

    providerConfig.prepare(manager);

    if (!manager.hasProvider("file")) {
      manager.addProvider("file", new DefaultLocalFileProvider());
    }

    manager.init();

    // Locate the base folders
    baseFolder = providerConfig.getBaseTestFolder(manager);
    if (baseFolder != null) {
      readFolder = baseFolder.resolveFile(READ_TESTS_FOLDER);
      writeFolder = baseFolder.resolveFile(WRITE_TESTS_FOLDER);

      // Make some assumptions about the read folder
      assertTrue(readFolder.exists(), "Folder does not exist: " + readFolder);
      assertNotEquals(FileName.ROOT_PATH, readFolder.getName().getPath());
    }
  }

  @AfterAll
  protected void tearDown() throws Exception {
    if (readFolder != null) {
      readFolder.close();
      readFolder = null;
    }
    if (writeFolder != null) {
      writeFolder.close();
      writeFolder = null;
    }
    if (baseFolder != null) {
      baseFolder.close();
      baseFolder = null;
    }

    // Suggest to threads (SoftRefFilesCache) to free all files.
    System.gc();
    Thread.sleep(1000);
    System.gc();
    Thread.sleep(1000);
    System.gc();
    Thread.sleep(1000);
    System.gc();
    Thread.sleep(1000);

    if (manager != null) {
      manager.freeUnusedResources();
      manager.close();
      manager = null;
    }

    // Give a chance for any threads to end.
    Thread.sleep(20);

    // Make sure temp directory is empty or gone
    checkTempDir("Temp dir not empty after test");
    VFS.close();
  }
}
