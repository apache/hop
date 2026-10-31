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
 *
 */

package org.apache.hop.git;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.finders.UIThreadRunnable;
import org.eclipse.swtbot.swt.finder.results.IntResult;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The mouse wheel scrolls the text diff, also when one side of it is empty. */
@Tag("uitest")
class TextDiffDialogUiTest extends SwtBotTestBase {
  private static final String TITLE = "Text diff test";
  private static final int WHEEL_STEPS = 10;

  @Test
  @DisplayName("the wheel scrolls a file the commit added")
  void wheelScrollsAddedFile() {
    assertWheelScrolls(null, lines(200), 1);
  }

  @Test
  @DisplayName("the wheel scrolls a file the commit deleted")
  void wheelScrollsDeletedFile() {
    assertWheelScrolls(lines(200), null, 0);
  }

  @Test
  @DisplayName("the wheel scrolls both sides of a modified file")
  void wheelScrollsModifiedFile() {
    assertWheelScrolls(lines(200), lines(200) + "added\n", 0);
    assertWheelScrolls(lines(200), lines(200) + "added\n", 1);
  }

  /**
   * Opens the dialog, turns the wheel over one of its editors and checks the editors with content
   * moved down one line per step.
   */
  private void assertWheelScrolls(String left, String right, int editorIndex) {
    withDialog(
        parent -> {
          TextDiffDialog dialog = new TextDiffDialog(parent, left, right);
          dialog.setTitle(TITLE);
          dialog.open();
        },
        swtBot -> {
          // Close the shell rather than pressing OK, which would save the dialog settings
          SWTBotShell shell = swtBot.shell(TITLE);
          SWTBot bot = shell.bot();
          StyledText leftEditor = bot.styledText(0).widget;
          StyledText rightEditor = bot.styledText(1).widget;
          StyledText wheelEditor = editorIndex == 0 ? leftEditor : rightEditor;

          int lineHeight = onUi(wheelEditor::getLineHeight);
          for (int i = 0; i < WHEEL_STEPS; i++) {
            UIThreadRunnable.syncExec(
                () -> {
                  Event event = new Event();
                  event.count = -1;
                  wheelEditor.notifyListeners(SWT.MouseVerticalWheel, event);
                });
          }

          int expected = WHEEL_STEPS * lineHeight;
          if (left != null) {
            assertEquals(expected, onUi(leftEditor::getTopPixel), "left top pixel");
          }
          if (right != null) {
            assertEquals(expected, onUi(rightEditor::getTopPixel), "right top pixel");
          }
          shell.close();
        });
  }

  private static int onUi(IntResult result) {
    return UIThreadRunnable.syncExec(result);
  }

  private static String lines(int count) {
    StringBuilder builder = new StringBuilder();
    for (int i = 1; i <= count; i++) {
      builder.append("line ").append(i).append('\n');
    }
    return builder.toString();
  }
}
