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

package org.apache.hop.vfs.sftp.provider;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VfsTestUtils;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link SftpFileObject#refresh()} correctly clears cached {@code attrs} even when the
 * file was never attached.
 *
 * <p>Regression test for the bug where {@code attrs} can be populated without going through {@code
 * attach()} (via {@code setStat()} in {@code doListChildrenResolved()}). Without the fix, {@code
 * refresh()} skips clearing {@code attrs} because {@code attached == false}, causing {@code
 * exists()} to return stale results.
 */
public class SftpRefreshCachedStateTest {

  @BeforeEach
  public void setUp() throws Exception {
    SftpTestServerHelper.startServer();
  }

  @AfterEach
  public void tearDown() throws InterruptedException {
    SftpTestServerHelper.stopServer();
  }

  /**
   * Tests that {@code exists()} returns {@code false} for a file that was deleted, when the child's
   * {@code attrs} was populated via the parent's listing without {@code attach()}.
   *
   * <p>The key is that we do NOT call {@code exists()} or {@code getType()} on the child before
   * deleting — only the parent's {@code getChildren()} populates the child's {@code attrs} via
   * {@code setStat()} (without triggering {@code attach()}). Then after deleting the file, {@code
   * refresh()} must clear the stale {@code attrs}.
   */
  @Test
  public void testExistsReturnsFalseAfterFileDeletedWithStaleAttrs() throws Exception {
    final File testDir = VfsTestUtils.getTestDirectoryFile();
    final File tempFile = new File(testDir, "sftp-refresh-test-file.txt");
    tempFile.createNewFile();
    try (final DefaultFileSystemManager manager = new DefaultFileSystemManager()) {
      manager.addProvider("sftp", new SftpFileProvider());
      manager.init();
      final FileSystemOptions options = new FileSystemOptions();
      final SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
      builder.setStrictHostKeyChecking(options, "no");
      final String uri = SftpTestServerHelper.getConnectionUri();
      // List the parent's children. doListChildrenResolved() calls setStat()
      // on each child, populating attrs WITHOUT calling attach().
      final FileObject parent = manager.resolveFile(uri, options);
      final FileObject[] children = parent.getChildren();
      // Find our file among the children — its attrs is set but attached=false.
      FileObject file = null;
      for (final FileObject child : children) {
        if (child.getName().getBaseName().equals("sftp-refresh-test-file.txt")) {
          file = child;
          break;
        }
      }
      assertNotNull(file, "File should be found in parent listing");
      // Delete the file directly on the filesystem.
      assertTrue(tempFile.delete(), "Temp file should be deleted");
      // Refresh the child. Before the fix, refresh() skipped clearing attrs
      // because attached == false (attrs was set via setStat, not attach).
      file.refresh();
      // exists() must return false. Before the fix, doGetType() found the stale
      // attrs (non-null), skipped statSelf(), and returned FILE instead of IMAGINARY.
      assertFalse(file.exists(), "exists() must return false after file is deleted and refreshed");
    } finally {
      tempFile.delete();
    }
  }
}
