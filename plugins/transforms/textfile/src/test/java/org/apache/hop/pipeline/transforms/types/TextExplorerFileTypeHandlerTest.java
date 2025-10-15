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

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ui.hopgui.perspective.explorer.file.IExplorerFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.text.BaseTextExplorerFileTypeHandler;
import org.junit.jupiter.api.Test;

class TextExplorerFileTypeHandlerTest {

  @Test
  void testHandlerExtendsBaseTextExplorerFileTypeHandler() {
    // Verify that TextExplorerFileTypeHandler extends the correct base class
    assertTrue(
        BaseTextExplorerFileTypeHandler.class.isAssignableFrom(TextExplorerFileTypeHandler.class));
  }

  @Test
  void testHandlerImplementsIExplorerFileTypeHandler() {
    // Verify that TextExplorerFileTypeHandler implements the correct interface
    assertTrue(IExplorerFileTypeHandler.class.isAssignableFrom(TextExplorerFileTypeHandler.class));
  }

  @Test
  void testHandlerHasCorrectConstructor() {
    // Verify the constructor signature exists with correct parameters
    try {
      TextExplorerFileTypeHandler.class.getConstructor(
          org.apache.hop.ui.hopgui.HopGui.class,
          org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective.class,
          org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile.class);
      assertTrue(true);
    } catch (NoSuchMethodException e) {
      throw new AssertionError("Expected constructor not found", e);
    }
  }
}
