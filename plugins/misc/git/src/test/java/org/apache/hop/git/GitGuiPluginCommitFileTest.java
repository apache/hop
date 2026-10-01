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

package org.apache.hop.git;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GitGuiPluginCommitFileTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testRepoRelativeFileResolution(@TempDir Path tempDir) throws Exception {
    FileObject gitDir =
        HopVfs.getFileObject(tempDir.resolve("my-repo").toAbsolutePath().toString());
    gitDir.createFolder();

    FileObject subfolder = gitDir.resolveFile("subfolder");
    subfolder.createFolder();

    FileObject pipelineFile = subfolder.resolveFile("test-pipeline.hpl");
    try (OutputStream out = HopVfs.getOutputStream(pipelineFile, false)) {
      out.write("<pipeline/>".getBytes(StandardCharsets.UTF_8));
    }

    // Relative path from git status (e.g. subfolder/test-pipeline.hpl)
    String relativePath = "subfolder/test-pipeline.hpl";
    FileObject resolved = gitDir.resolveFile(relativePath);

    assertTrue(resolved.exists());
    assertEquals(HopVfs.getFilename(pipelineFile), HopVfs.getFilename(resolved));

    // Non-existent relative file
    String nonExistent = "subfolder/does-not-exist.hpl";
    FileObject resolvedNonExistent = gitDir.resolveFile(nonExistent);
    assertFalse(resolvedNonExistent.exists());
  }
}
