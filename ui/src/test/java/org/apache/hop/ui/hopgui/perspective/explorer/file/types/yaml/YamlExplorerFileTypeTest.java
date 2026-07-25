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

package org.apache.hop.ui.hopgui.perspective.explorer.file.types.yaml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit test for {@link YamlExplorerFileType} */
class YamlExplorerFileTypeTest {

  private YamlExplorerFileType fileType;

  @BeforeEach
  void setUp() {
    fileType = new YamlExplorerFileType();
  }

  @Test
  void testConstructor() {
    assertNotNull(fileType);
  }

  @Test
  void testGetName() {
    assertEquals("YAML File", fileType.getName());
  }

  @Test
  void testGetDefaultFileExtension() {
    assertEquals(".yaml", fileType.getDefaultFileExtension());
  }

  @Test
  void testGetFilterExtensions() {
    String[] extensions = fileType.getFilterExtensions();
    assertNotNull(extensions);
    assertEquals(2, extensions.length);
    assertEquals("*.yml", extensions[0]);
    assertEquals("*.yaml", extensions[1]);
  }

  @Test
  void testGetFilterNames() {
    String[] names = fileType.getFilterNames();
    assertNotNull(names);
    assertEquals(2, names.length);
    assertEquals("YAML files", names[0]);
    assertEquals("YAML files", names[1]);
  }

  @Test
  void testFilterExtensionsMatchFilterNames() {
    String[] extensions = fileType.getFilterExtensions();
    String[] names = fileType.getFilterNames();
    assertEquals(extensions.length, names.length);
    for (int i = 0; i < extensions.length; i++) {
      assertNotNull(extensions[i]);
      assertNotNull(names[i]);
      assertFalse(extensions[i].isEmpty());
      assertFalse(names[i].isEmpty());
    }
  }

  @Test
  void testHasExpectedCapabilities() {
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_SAVE));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_SAVE_AS));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_CLOSE));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_FILE_HISTORY));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_COPY));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_CUT));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_PASTE));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_SELECT));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_SEARCH));
    assertFalse(fileType.hasCapability(IHopFileType.CAPABILITY_NEW));
    assertFalse(fileType.hasCapability(IHopFileType.CAPABILITY_START));
  }

  @Test
  void testGetCapabilities() {
    assertNotNull(fileType.getCapabilities());
    assertFalse(fileType.getCapabilities().isEmpty());
  }

  @Test
  void testGetFileTypeImage() {
    assertEquals("ui/images/file.svg", fileType.getFileTypeImage());
  }

  @Test
  void testCreateFileTypeHandlerReturnsYamlHandler() {
    assertInstanceOf(
        YamlExplorerFileTypeHandler.class, fileType.createFileTypeHandler(null, null, null));
  }

  @Test
  void testNewFileReturnsEmptyHandler() throws Exception {
    assertInstanceOf(EmptyHopFileTypeHandler.class, fileType.newFile(null, null));
  }

  @Test
  void testIsHandledByYmlAndYamlExtensions() throws Exception {
    assertTrue(fileType.isHandledBy("config.yml", false));
    assertTrue(fileType.isHandledBy("config.yaml", false));
    assertFalse(fileType.isHandledBy("config.json", false));
    assertFalse(fileType.isHandledBy("config.txt", false));
  }

  @Test
  void testSupportsOpening() {
    assertTrue(fileType.supportsOpening());
  }
}
