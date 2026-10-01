/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.pipeline.transforms.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MarkDownPreviewExplorerFileTypeTest {

  private MarkDownPreviewExplorerFileType fileType;

  @BeforeEach
  void setUp() {
    fileType = new MarkDownPreviewExplorerFileType();
  }

  @Test
  void testGetName() {
    assertEquals("MarkDown Preview", fileType.getName());
  }

  @Test
  void testHasNoFileExtensions() {
    // The preview tab renders editor content, it is never a file on disk
    assertNotNull(fileType.getFilterExtensions());
    assertEquals(0, fileType.getFilterExtensions().length);
    assertNotNull(fileType.getFilterNames());
    assertEquals(0, fileType.getFilterNames().length);
  }

  @Test
  void testDoesNotHandleFilesOnDisk() throws Exception {
    assertFalse(fileType.isHandledBy("notes.md", false));
    assertFalse(fileType.isHandledBy("notes.txt", false));
  }

  @Test
  void testCanOnlyBeClosed() {
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_CLOSE));
    assertFalse(fileType.hasCapability(IHopFileType.CAPABILITY_NEW));
    assertFalse(fileType.hasCapability(IHopFileType.CAPABILITY_SAVE));
    assertFalse(fileType.hasCapability(IHopFileType.CAPABILITY_SAVE_AS));
  }

  @Test
  void testHasImageForTheTabIcon() {
    assertEquals("markdown.svg", fileType.getFileTypeImage());
  }
}
