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

package org.apache.hop.core.fatjar;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.impl.DecoratedFileObject;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class VfsStagingStoreTest {

  private static final String NAME = "hop-spark-package-abc.zip";
  private static final byte[] CONTENT = {1, 2, 3, 4};

  @TempDir Path tempDir;

  @BeforeAll
  static void init() throws Exception {
    HopLogStore.init();
    HopClientEnvironment.init();
  }

  /** An S3 rename whose copy the store made, but whose answer the client could not read. */
  @Test
  void aRenameTheStoreDidIsAcceptedThoughItsAnswerFailed() throws Exception {
    Path staging = tempDir.resolve("staging");
    upload(staging, true);

    assertEquals(List.of(NAME), names(staging), "staged file only, partial removed");
    assertArrayEquals(CONTENT, Files.readAllBytes(staging.resolve(NAME)));
  }

  @Test
  void aRenameThatDidNotHappenFailsTheUpload() throws Exception {
    Path staging = tempDir.resolve("staging");
    HopException e = assertThrows(HopException.class, () -> upload(staging, false));

    assertEquals(List.of(), names(staging), "nothing staged, partial removed");
    assertTrue(e.getMessage().contains("Unable to stage"), e.getMessage());
  }

  private void upload(Path staging, boolean storeCopies) throws Exception {
    Path local = tempDir.resolve(NAME);
    Files.write(local, CONTENT);
    VfsStagingStore store = new VfsStagingStore(new Variables(), staging.toString(), "", "x");

    try (MockedStatic<HopVfs> vfs = mockStatic(HopVfs.class, CALLS_REAL_METHODS)) {
      vfs.when(
              () ->
                  HopVfs.getFileObject(
                      contains(StagingCleanup.UPLOAD_INFIX), any(IVariables.class)))
          .thenAnswer(
              call -> new UnreadableRenameAnswer((FileObject) call.callRealMethod(), storeCopies));
      store.upload(local, NAME);
    }
  }

  private static List<String> names(Path folder) throws Exception {
    try (Stream<Path> files = Files.list(folder)) {
      return files.map(file -> file.getFileName().toString()).sorted().toList();
    }
  }

  /** A rename the way S3 does it: a copy that leaves the source, then an answer that fails. */
  private static final class UnreadableRenameAnswer extends DecoratedFileObject {
    private final boolean storeCopies;

    UnreadableRenameAnswer(FileObject file, boolean storeCopies) {
      super(file);
      this.storeCopies = storeCopies;
    }

    @Override
    public void moveTo(FileObject destFile) throws FileSystemException {
      if (storeCopies) {
        destFile.copyFrom(getDecoratedFileObject(), Selectors.SELECT_SELF);
      }
      throw new FileSystemException("Text '2026-11-27T17:44:37.45Z' could not be parsed");
    }
  }
}
