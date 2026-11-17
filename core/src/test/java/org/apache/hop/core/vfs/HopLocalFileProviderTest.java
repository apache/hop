/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.vfs;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopLocalFileProviderTest {

  @TempDir Path tempDir;

  @Test
  void aFolderCreatedByAnotherWriterInTheMeantimeIsNotAnError() throws Exception {
    DefaultFileSystemManager manager = new DefaultFileSystemManager();
    manager.addProvider("file", new HopLocalFileProvider());
    manager.init();
    try {
      Path folder = tempDir.resolve("out").resolve("copy");
      FileObject target = manager.resolveFile(folder.toUri().toString());
      // Seen as missing ...
      assertTrue(!target.exists());
      // ... then another copy of the transform, or another process, creates it
      Files.createDirectories(folder);
      target.createFolder();
      assertTrue(Files.isDirectory(folder));
    } finally {
      manager.close();
    }
  }

  /** Hop VFS reads and writes local files through this provider. */
  @Test
  void hopVfsCreatesAFolderThatAppearedInTheMeantime() throws Exception {
    Path folder = tempDir.resolve("vfs").resolve("copy");
    FileObject target = HopVfs.getFileObject(folder.toString());
    assertTrue(!target.exists());
    Files.createDirectories(folder);

    target.createFolder();

    assertTrue(Files.isDirectory(folder));
  }
}
