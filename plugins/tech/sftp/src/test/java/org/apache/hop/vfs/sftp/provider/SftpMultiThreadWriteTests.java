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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.vfs2.AbstractProviderTestCase;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.VFS;
import org.junit.jupiter.api.Test;

/** MultiThread tests for writing with SFTP provider. */
public class SftpMultiThreadWriteTests extends AbstractProviderTestCase {

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
      Capability.WRITE_CONTENT
    };
  }

  /**
   * Tests file copy from local file system in parallel mode. This was a problem with SFTP channels.
   */
  @Test
  public void testParallelCopyFromLocalFileSystem() throws Exception {
    final File localFile = new File("src/test/resources/test-data/test.zip");

    final FileObject localFileObject = VFS.getManager().toFileObject(localFile);

    final FileObject scratchFolder = createScratchFolder();

    final List<Callable<Boolean>> tasks = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      final String fileName = "file" + i + "copy.txt";
      tasks.add(
          () -> {
            try {
              final FileObject fileObjectCopy = scratchFolder.resolveFile(fileName);

              assertFalse(fileObjectCopy.exists());
              fileObjectCopy.copyFrom(localFileObject, Selectors.SELECT_SELF);
            } catch (final Throwable e) {
              return false;
            }
            return true;
          });
    }

    final ExecutorService service = Executors.newFixedThreadPool(10);
    try {
      final List<Future<Boolean>> futures = service.invokeAll(tasks);
      assertTrue(
          futures.stream()
              .allMatch(
                  future -> {
                    try {
                      return future.get(5, TimeUnit.SECONDS);
                    } catch (final Exception e) {
                      return false;
                    }
                  }));
    } finally {
      service.shutdown();
    }
  }
}
