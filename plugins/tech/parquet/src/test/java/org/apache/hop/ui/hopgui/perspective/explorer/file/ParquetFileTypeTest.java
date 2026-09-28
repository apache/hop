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

package org.apache.hop.ui.hopgui.perspective.explorer.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ParquetFileTypeTest {

  private final ParquetFileType fileType = new ParquetFileType();

  @TempDir private Path tempDir;

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void opensParquetAndParqExtensions() throws Exception {
    assertEquals(".parquet", fileType.getDefaultFileExtension());
    assertTrue(fileType.supportsOpening());
    assertTrue(fileType.isHandledBy("/data/orders.PARQUET", false));
    assertTrue(fileType.isHandledBy("folder/part.parq", false));
    assertTrue(fileType.isHandledBy("folder/part.Parq", false));
    assertFalse(fileType.isHandledBy("folder/part.csv", false));
    assertFalse(fileType.isHandledBy("folder/part.parquet.bak", false));
  }

  @Test
  void isReadOnlyInTheExplorer() {
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_CLOSE));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_FILE_HISTORY));
    assertFalse(fileType.hasCapability(IHopFileType.CAPABILITY_SAVE));
    assertFalse(fileType.hasCapability(IHopFileType.CAPABILITY_SAVE_AS));
    assertFalse(fileType.hasCapability(IHopFileType.CAPABILITY_NEW));
  }

  @Test
  void keepsTheLargeFileConfirmationForRemoteFiles() throws Exception {
    Path local = tempDir.resolve("sample.parquet");
    try (FileObject file = HopVfs.getFileObject(local.toString())) {
      assertFalse(ParquetFileType.keepsLargeFileConfirmation(file));
    }
    try (FileObject remote = HopVfs.getFileObject("ram:///parquet-preview/sample.parquet")) {
      assertTrue(ParquetFileType.keepsLargeFileConfirmation(remote));
    }
  }

  @Test
  void createsTheExplorerHandler() {
    assertInstanceOf(
        ParquetExplorerFileTypeHandler.class, fileType.createFileTypeHandler(null, null, null));
  }
}
