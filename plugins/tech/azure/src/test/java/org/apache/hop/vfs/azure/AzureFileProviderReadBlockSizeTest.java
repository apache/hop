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

package org.apache.hop.vfs.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.Test;

/** The read block size setting of an Azure connection. */
class AzureFileProviderReadBlockSizeTest {

  @Test
  void blankIsTheDefault() throws FileSystemException {
    assertEquals(HopVfs.DEFAULT_READ_BLOCK_SIZE, AzureFileProvider.readBlockSize(null));
    assertEquals(HopVfs.DEFAULT_READ_BLOCK_SIZE, AzureFileProvider.readBlockSize(" "));
  }

  @Test
  void parsesSizesAndBytes() throws FileSystemException {
    assertEquals(16 * 1024 * 1024, AzureFileProvider.readBlockSize("16MB"));
    assertEquals(512 * 1024, AzureFileProvider.readBlockSize("512KB"));
    assertEquals(1048576, AzureFileProvider.readBlockSize("1048576"));
  }

  @Test
  void refusesSizesOutOfRange() {
    assertThrows(FileSystemException.class, () -> AzureFileProvider.readBlockSize("1KB"));
    assertThrows(FileSystemException.class, () -> AzureFileProvider.readBlockSize("1GB"));
    assertThrows(FileSystemException.class, () -> AzureFileProvider.readBlockSize("lots"));
  }
}
