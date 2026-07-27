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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.text.BaseTextExplorerFileTypeHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CmdExplorerFileTypeTest {

  private CmdExplorerFileType fileType;

  @BeforeEach
  void setUp() {
    fileType = new CmdExplorerFileType();
  }

  @Test
  void testConstructor() {
    assertNotNull(fileType);
  }

  @Test
  void testGetDefaultFileExtension() {
    assertEquals(".cmd", fileType.getDefaultFileExtension());
  }

  @Test
  void testGetFilterExtensions() {
    String[] extensions = fileType.getFilterExtensions();
    assertNotNull(extensions);
    assertEquals(2, extensions.length);
    assertEquals("*.cmd", extensions[0]);
    assertEquals("*.CMD", extensions[1]);
  }

  @Test
  void testGetFilterNames() {
    String[] names = fileType.getFilterNames();
    assertNotNull(names);
    assertEquals(1, names.length);
    assertEquals("Command files", names[0]);
  }

  @Test
  void testHasExpectedCapabilities() {
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_SAVE));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_SAVE_AS));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_CLOSE));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_FILE_HISTORY));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_COPY));
    assertTrue(fileType.hasCapability(IHopFileType.CAPABILITY_SELECT));
    assertFalse(fileType.hasCapability(IHopFileType.CAPABILITY_NEW));
  }

  @Test
  void testCreateFileTypeHandlerReturnsBatHandler() {
    assertInstanceOf(
        BatExplorerFileTypeHandler.class, fileType.createFileTypeHandler(null, null, null));
  }

  @Test
  void testHandlerReturnsBatLanguageId() throws Exception {
    BatExplorerFileTypeHandler handler = new BatExplorerFileTypeHandler(null, null, null);
    Method method = BaseTextExplorerFileTypeHandler.class.getDeclaredMethod("getLanguageId");
    method.setAccessible(true);
    assertEquals("bat", method.invoke(handler));
  }
}
