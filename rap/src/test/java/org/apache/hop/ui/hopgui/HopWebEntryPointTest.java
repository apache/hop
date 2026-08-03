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
 */

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.hop.core.gui.plugin.key.KeyboardShortcut;
import org.junit.jupiter.api.Test;

class HopWebEntryPointTest {

  @Test
  void preservesNativeTextEditingShortcutsAsActiveOnly() {
    String[] activeShortcuts = {"CTRL+C", "CTRL+V", "CTRL+X", "CTRL+S"};

    String[] cancelledShortcuts = HopWebEntryPoint.buildCancelledKeyboardShortcuts(activeShortcuts);

    assertArrayEquals(new String[] {"CTRL+C", "CTRL+V", "CTRL+X", "CTRL+S"}, activeShortcuts);
    assertArrayEquals(new String[] {"CTRL+S"}, cancelledShortcuts);
  }

  @Test
  void removesDuplicateCancelledShortcuts() {
    String[] cancelledShortcuts =
        HopWebEntryPoint.buildCancelledKeyboardShortcuts(
            new String[] {"CTRL+S", "CTRL+C", "CTRL+S"});

    assertEquals(cancelledShortcuts.length, Arrays.stream(cancelledShortcuts).distinct().count());
  }

  @Test
  void mapsMacCommandShortcutToRapControlShortcut() {
    KeyboardShortcut shortcut = mock(KeyboardShortcut.class);
    when(shortcut.getKeyCode()).thenReturn((int) 'c');
    when(shortcut.isCommand()).thenReturn(true);

    assertEquals("CTRL+C", new HopWebEntryPoint().convertToRapFormat(shortcut));
  }
}
