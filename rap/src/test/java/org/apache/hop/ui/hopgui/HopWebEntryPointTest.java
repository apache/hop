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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.hop.core.gui.plugin.key.KeyboardShortcut;
import org.eclipse.swt.SWT;
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
  void preservesBareNavigationKeysAsActiveOnly() {
    // Canvas pan registers bare arrows/HOME as ACTIVE_KEYS; CANCEL would block caret movement in
    // text fields (issue #7833). Modifier combos stay cancellable for app shortcuts.
    String[] activeShortcuts = {
      "ARROW_LEFT",
      "ARROW_RIGHT",
      "ARROW_UP",
      "ARROW_DOWN",
      "HOME",
      "END",
      "PAGE_UP",
      "PAGE_DOWN",
      "CTRL+ARROW_LEFT",
      "CTRL+S"
    };

    String[] cancelledShortcuts = HopWebEntryPoint.buildCancelledKeyboardShortcuts(activeShortcuts);

    assertArrayEquals(new String[] {"CTRL+ARROW_LEFT", "CTRL+S"}, cancelledShortcuts);
  }

  @Test
  void preservesShiftNavigationKeysAsActiveOnly() {
    // SHIFT+arrow moves the selection on the canvas but still extends the selection in text
    // fields, so the browser must keep handling it.
    String[] activeShortcuts = {
      "SHIFT+ARROW_LEFT",
      "SHIFT+ARROW_RIGHT",
      "SHIFT+ARROW_UP",
      "SHIFT+ARROW_DOWN",
      "SHIFT+HOME",
      "SHIFT+END",
      "CTRL+SHIFT+ARROW_UP",
      "CTRL+S"
    };

    String[] cancelledShortcuts = HopWebEntryPoint.buildCancelledKeyboardShortcuts(activeShortcuts);

    assertArrayEquals(new String[] {"CTRL+SHIFT+ARROW_UP", "CTRL+S"}, cancelledShortcuts);
  }

  @Test
  void mapsShiftArrowShortcutToRapFormat() {
    KeyboardShortcut shortcut = mock(KeyboardShortcut.class);
    when(shortcut.getKeyCode()).thenReturn(SWT.ARROW_LEFT);
    when(shortcut.isShift()).thenReturn(true);

    assertEquals("SHIFT+ARROW_LEFT", new HopWebEntryPoint().convertToRapFormat(shortcut));
  }

  @Test
  void removesDuplicateCancelledShortcuts() {
    String[] cancelledShortcuts =
        HopWebEntryPoint.buildCancelledKeyboardShortcuts(
            new String[] {"CTRL+S", "CTRL+C", "CTRL+S"});

    assertEquals(cancelledShortcuts.length, Arrays.stream(cancelledShortcuts).distinct().count());
  }

  @Test
  void refusesBareLetterShortcuts() {
    // RAP cancels the browser's handling of every key it is told about, so a bare "z" - the
    // pipeline canvas shortcut that opens a referenced object - took the letter z away from every
    // text field in Hop Web.
    KeyboardShortcut shortcut = mock(KeyboardShortcut.class);
    when(shortcut.getKeyCode()).thenReturn((int) 'z');

    assertNull(new HopWebEntryPoint().convertToRapFormat(shortcut));
  }

  @Test
  void stillMapsTheSameLetterWithAModifier() {
    KeyboardShortcut shortcut = mock(KeyboardShortcut.class);
    when(shortcut.getKeyCode()).thenReturn((int) 'z');
    when(shortcut.isControl()).thenReturn(true);

    assertEquals("CTRL+Z", new HopWebEntryPoint().convertToRapFormat(shortcut));
  }

  @Test
  void keepsUnmodifiedSpecialKeys() {
    // Special keys type nothing, so cancelling them costs the browser nothing.
    KeyboardShortcut shortcut = mock(KeyboardShortcut.class);
    when(shortcut.getKeyCode()).thenReturn(SWT.ARROW_LEFT);

    assertEquals("ARROW_LEFT", new HopWebEntryPoint().convertToRapFormat(shortcut));
  }

  @Test
  void mapsMacCommandShortcutToRapControlShortcut() {
    KeyboardShortcut shortcut = mock(KeyboardShortcut.class);
    when(shortcut.getKeyCode()).thenReturn((int) 'c');
    when(shortcut.isCommand()).thenReturn(true);

    assertEquals("CTRL+C", new HopWebEntryPoint().convertToRapFormat(shortcut));
  }
}
