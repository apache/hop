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

package org.apache.hop.pipeline.transforms.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TextExplorerFileTypeTest {

  private TextExplorerFileType fileType;

  @BeforeEach
  void setUp() {
    fileType = new TextExplorerFileType();
  }

  @Test
  void testConstructor() {
    assertNotNull(fileType);
  }

  @Test
  void testGetName() {
    assertEquals("TXT File", fileType.getName());
  }

  @Test
  void testGetDefaultFileExtension() {
    assertEquals(".txt", fileType.getDefaultFileExtension());
  }

  @Test
  void testGetFilterExtensions() {
    String[] extensions = fileType.getFilterExtensions();
    assertNotNull(extensions);
    assertEquals(1, extensions.length);
    assertEquals("*.txt", extensions[0]);
  }

  @Test
  void testGetFilterNames() {
    String[] names = fileType.getFilterNames();
    assertNotNull(names);
    assertEquals(1, names.length);
    assertEquals("TXT files", names[0]);
  }

  @Test
  void testHasCapabilitySave() {
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_SAVE));
  }

  @Test
  void testHasCapabilityClose() {
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_CLOSE));
  }

  @Test
  void testHasCapabilityFileHistory() {
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_FILE_HISTORY));
  }

  @Test
  void testHasCapabilityCopy() {
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_COPY));
  }

  @Test
  void testHasCapabilitySelect() {
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_SELECT));
  }

  @Test
  void testMultipleFilterExtensions() {
    String[] extensions = fileType.getFilterExtensions();
    String[] names = fileType.getFilterNames();

    // Ensure filter extensions and names have the same length
    assertEquals(extensions.length, names.length);
  }

  @Test
  void testFilterExtensionsMatchFilterNames() {
    String[] extensions = fileType.getFilterExtensions();
    String[] names = fileType.getFilterNames();

    // Verify the correspondence between extensions and names
    for (int i = 0; i < extensions.length; i++) {
      assertNotNull(extensions[i]);
      assertNotNull(names[i]);
      assertFalse(extensions[i].isEmpty());
      assertFalse(names[i].isEmpty());
    }
  }

  @Test
  void testGetCapabilities() {
    assertNotNull(fileType.getCapabilities());
    assertTrue(fileType.getCapabilities().size() > 0);
  }

  @Test
  void testFileTypeIsForTextFiles() {
    assertTrue(fileType.getDefaultFileExtension().endsWith(".txt"));
    assertTrue(fileType.getFilterExtensions()[0].contains("txt"));
    assertTrue(fileType.getFilterNames()[0].toUpperCase().contains("TXT"));
  }

  @Test
  void testTextFileHasSameCapabilitiesAsMarkdown() {
    // Text files should have the same capabilities as Markdown
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_SAVE));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_CLOSE));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_FILE_HISTORY));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_COPY));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_SELECT));
  }
}
