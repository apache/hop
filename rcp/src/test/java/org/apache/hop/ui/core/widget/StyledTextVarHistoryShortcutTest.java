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

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Ctrl+Z / Ctrl+Y in the script and Java editors (StyledTextVar), issue #4940. */
@Tag("uitest")
class StyledTextVarHistoryShortcutTest extends SwtBotTestBase {

  @Test
  void ctrlZAndCtrlYWalkTheEditHistory() {
    Shell shell = new Shell(display);
    shell.setLayout(new FillLayout());
    try {
      StyledTextVar text =
          new StyledTextVar(
              new Variables(), shell, SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, false, false, false);
      shell.setSize(400, 300);
      shell.open();

      text.setText("base");
      assertFalse(text.canUndo(), "Loading the script must not be an undoable edit");

      type(text, "X");
      type(text, "Y");
      assertEquals("baseXY", text.getText());
      assertTrue(text.canUndo());
      assertFalse(text.canRedo());

      press(text.getTextWidget(), 'z', SWT.MOD1);
      assertEquals("baseX", text.getText());
      press(text.getTextWidget(), 'z', SWT.MOD1);
      assertEquals("base", text.getText());
      // Applying undo used to record itself, so the next Ctrl+Z put the edit back.
      press(text.getTextWidget(), 'z', SWT.MOD1);
      assertEquals("base", text.getText());
      assertFalse(text.canUndo());
      assertTrue(text.canRedo());

      press(text.getTextWidget(), 'y', SWT.MOD1);
      assertEquals("baseX", text.getText());
      press(text.getTextWidget(), 'Z', SWT.MOD1 | SWT.MOD2);
      assertEquals("baseXY", text.getText(), "Ctrl+Shift+Z still redoes");
      assertFalse(text.canRedo());

      press(text.getTextWidget(), 'z', SWT.MOD1);
      press(text.getTextWidget(), 'z', SWT.MOD1);
      assertEquals("base", text.getText());
      type(text, "Q");
      assertEquals("baseQ", text.getText());
      press(text.getTextWidget(), 'y', SWT.MOD1);
      assertEquals("baseQ", text.getText(), "A new edit clears redo");
      assertFalse(text.canRedo());
    } finally {
      shell.dispose();
    }
  }

  private static void type(StyledTextVar text, String value) {
    text.setCaretPosition(text.getCharCount());
    text.insert(value);
  }

  private static void press(StyledText widget, char key, int stateMask) {
    Event event = new Event();
    event.type = SWT.KeyDown;
    event.keyCode = key;
    event.stateMask = stateMask;
    event.doit = true;
    widget.notifyListeners(SWT.KeyDown, event);
    assertFalse(event.doit, "The editor shortcut must be consumed");
  }
}
