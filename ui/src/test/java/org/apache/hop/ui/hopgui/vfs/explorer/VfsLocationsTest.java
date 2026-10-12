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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

class VfsLocationsTest {

  @Test
  void folderOpensItselfAndAFileOpensItsParent() {
    assertEquals("/tmp", VfsLocations.folderToBrowse("/tmp", true));
    assertEquals("/tmp", VfsLocations.folderToBrowse("/tmp/notes.txt", false));
    assertEquals("ram://", VfsLocations.folderToBrowse("ram:///notes.txt", false));
    assertEquals("ram:///dir", VfsLocations.folderToBrowse("ram:///dir/notes.txt", false));
    assertEquals("/", VfsLocations.parentOf("/notes.txt"));
    assertNull(VfsLocations.folderToBrowse("notes.txt", false));
    assertNull(VfsLocations.folderToBrowse("  ", true));
  }

  @Test
  void explorerMenuCallbackHasNoArguments() throws Exception {
    // GuiMenuWidgets.executeMenuItem looks up the public no-argument method by name.
    Method method = VfsFileExplorerViews.class.getMethod("openSelectionInVfsExplorer");
    assertEquals(0, method.getParameterCount());
  }

  @Test
  void underRequiresASeparatorBoundary() {
    assertTrue(VfsLocations.isUnder("ram:///a", "ram:///a/b"));
    assertFalse(VfsLocations.isUnder("ram:///a", "ram:///ab"));
    assertFalse(VfsLocations.isUnder("ram:///a", "ram:///a"));
    assertFalse(VfsLocations.isUnder("ram:///a/", "ram:///a"));
  }

  @Test
  void aPlainPathAndItsFileUriAreTheSameLocation() {
    assertTrue(VfsLocations.sameLocation("/home/x", "file:///home/x/"));
    assertTrue(VfsLocations.sameLocation("C:\\data\\in", "file:///C:/data/in"));
    assertTrue(VfsLocations.sameLocation("C:\\", "file:///C:/"));
    assertFalse(VfsLocations.sameLocation("/home/x", "file:///home/xy"));
    assertFalse(VfsLocations.sameLocation("/home/x", "s3:///home/x"));
    assertTrue(VfsLocations.isUnder("/home/x", "file:///home/x/sub"));
    assertTrue(VfsLocations.isUnder("file:///home/x", "/home/x/sub/deeper"));
    assertFalse(VfsLocations.isUnder("/home/x", "file:///home/xy"));
  }

  @Test
  void aPathInsideTheProjectIsWrittenWithTheVariable() {
    assertEquals(
        "${PROJECT_HOME}/data/in.csv",
        VfsLocations.relativeTo("/home/p/data/in.csv", "PROJECT_HOME", "/home/p/"));
    assertEquals(
        "${PROJECT_HOME}/data",
        VfsLocations.relativeTo("file:///home/p/data", "PROJECT_HOME", "/home/p"));
    assertEquals("${PROJECT_HOME}", VfsLocations.relativeTo("/home/p", "PROJECT_HOME", "/home/p"));
    assertNull(VfsLocations.relativeTo("/home/pq/x", "PROJECT_HOME", "/home/p"));
    assertNull(VfsLocations.relativeTo("s3:///bucket/x", "PROJECT_HOME", "/home/p"));
  }
}
