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

package org.apache.hop.ui.hopgui.vfs.explorer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class VfsExplorerStatusTest {

  @Test
  void statusLineMatchesTheRequestedShape() {
    assertEquals("890ms", VfsExplorerOperationsPanel.formatStatusElapsed(890));
    assertEquals("1.5 s", VfsExplorerOperationsPanel.formatStatusElapsed(1500));

    long twoPointOneGb = 2254857830L;
    long oneHundredTwentyMb = 120L * 1024 * 1024;
    assertEquals("2.1GB", VfsListingCounts.formatSize(twoPointOneGb));
    assertEquals("120MB", VfsListingCounts.formatSize(oneHundredTwentyMb));
    assertEquals("0B", VfsListingCounts.formatSize(0));

    VfsExplorerOperation operation =
        new VfsExplorerOperation("Listing hdfs://some/folder", "hdfs://some/folder");
    operation.complete();
    VfsListingCounts counts = new VfsListingCounts(200, twoPointOneGb, 10, oneHundredTwentyMb);
    String line = VfsExplorerOperationsPanel.formatStatusLine(operation, counts);
    assertTrue(line.startsWith("Listing hdfs://some/folder - Done - "));
    assertTrue(line.contains("ms - 200 files (2.1GB) - 10 files selected (120MB)"));
  }

  @Test
  void oneRowUsesTheSingular() {
    VfsListingCounts counts = new VfsListingCounts(1, 5, 1, 5);
    assertEquals(" - 1 file (5B) - 1 file selected (5B)", counts.statusSuffix());
  }

  @Test
  void folderSizeIsNotAdded() {
    VfsFileRow folder = row("incoming", true, VfsFileRow.UNKNOWN);
    VfsFileRow file = row("notes.txt", false, 5);
    assertEquals(5L, VfsListingCounts.bytes(List.of(folder, file)));
    assertEquals(2, VfsListingCounts.of(List.of(folder, file), List.of()).getListed());
    assertEquals(5L, VfsListingCounts.of(List.of(folder, file), List.of(file)).getSelectedBytes());
  }

  private static VfsFileRow row(String name, boolean folder, long size) {
    return new VfsFileRow(
        name, "ram:///" + name, folder, "", size, "", VfsFileRow.UNKNOWN, "", "", "");
  }
}
