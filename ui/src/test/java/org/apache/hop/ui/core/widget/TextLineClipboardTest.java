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

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TextLineClipboardTest {

  @Test
  void emptyTextIsAnEmptyLine() {
    assertArrayEquals(new int[] {0, 0}, TextLineClipboard.lineRange(null, 0));
    assertArrayEquals(new int[] {0, 0}, TextLineClipboard.lineRange("", 0));
    assertArrayEquals(new int[] {0, 0}, TextLineClipboard.lineRange("", -3));
    assertArrayEquals(new int[] {0, 0}, TextLineClipboard.lineRange("", 4));
  }

  @Test
  void singleLineHasNoInventedBreak() {
    assertArrayEquals(new int[] {0, 5}, TextLineClipboard.lineRange("hello", 0));
    assertArrayEquals(new int[] {0, 5}, TextLineClipboard.lineRange("hello", 3));
    assertArrayEquals(new int[] {0, 5}, TextLineClipboard.lineRange("hello", 5));
    assertArrayEquals(new int[] {0, 5}, TextLineClipboard.lineRange("hello", 9));
  }

  @Test
  void lineIncludesItsBreakAndTheCaretAfterTheBreakStartsTheNextLine() {
    String text = "ab\ncd";
    assertArrayEquals(new int[] {0, 3}, TextLineClipboard.lineRange(text, 0));
    assertArrayEquals(new int[] {0, 3}, TextLineClipboard.lineRange(text, 2));
    assertArrayEquals(new int[] {3, 5}, TextLineClipboard.lineRange(text, 3));
    assertArrayEquals(new int[] {3, 5}, TextLineClipboard.lineRange(text, 5));
  }

  @Test
  void caretBetweenCarriageReturnAndLineFeedStaysOnThatLine() {
    String text = "ab\r\ncd";
    assertArrayEquals(new int[] {0, 4}, TextLineClipboard.lineRange(text, 2));
    assertArrayEquals(new int[] {0, 4}, TextLineClipboard.lineRange(text, 3));
    assertArrayEquals(new int[] {4, 6}, TextLineClipboard.lineRange(text, 4));
  }

  @Test
  void blankLinesAndATrailingEmptyLine() {
    assertArrayEquals(new int[] {0, 1}, TextLineClipboard.lineRange("\n\n", 0));
    assertArrayEquals(new int[] {1, 2}, TextLineClipboard.lineRange("\n\n", 1));
    assertArrayEquals(new int[] {2, 2}, TextLineClipboard.lineRange("\n\n", 2));
    assertArrayEquals(new int[] {3, 3}, TextLineClipboard.lineRange("ab\n", 3));
  }

  @Test
  void loneCarriageReturnIsALineBreak() {
    assertArrayEquals(new int[] {0, 3}, TextLineClipboard.lineRange("ab\rcd", 1));
    assertArrayEquals(new int[] {3, 5}, TextLineClipboard.lineRange("ab\rcd", 3));
  }

  @Test
  void copySelectsTheLineThenRestoresTheCaret() {
    FakeEditor editor = new FakeEditor("ab\ncd", 4, true, false, false);
    assertTrue(TextLineClipboard.copyOrCutCurrentLine(editor, false));
    assertEquals("cd", editor.copied);
    assertEquals(4, editor.caret);
    assertFalse(editor.cutCalled);
  }

  @Test
  void cutRemovesTheLineAndParksTheCaretAtItsStart() {
    FakeEditor editor = new FakeEditor("ab\ncd", 1, true, false, false);
    assertTrue(TextLineClipboard.copyOrCutCurrentLine(editor, true));
    assertEquals("ab\n", editor.copied);
    assertEquals("cd", editor.text);
    assertEquals(0, editor.caret);
    assertTrue(editor.cutCalled);
  }

  @Test
  void selectionPasswordAndReadOnlyCutAreLeftToTheWidget() {
    FakeEditor selected = new FakeEditor("ab\ncd", 1, true, false, true);
    assertFalse(TextLineClipboard.copyOrCutCurrentLine(selected, false));
    assertNull(selected.copied);

    FakeEditor password = new FakeEditor("secret", 2, true, true, false);
    assertFalse(TextLineClipboard.copyOrCutCurrentLine(password, false));
    assertNull(password.copied);

    FakeEditor readOnly = new FakeEditor("ab\ncd", 1, false, false, false);
    assertFalse(TextLineClipboard.copyOrCutCurrentLine(readOnly, true));
    assertEquals("ab\ncd", readOnly.text);
    assertTrue(TextLineClipboard.copyOrCutCurrentLine(readOnly, false));
    assertEquals("ab\n", readOnly.copied);
  }

  /** Records copy/cut without an SWT display. */
  private static final class FakeEditor implements TextLineClipboard.Editor {
    private String text;
    private int caret;
    private final boolean editable;
    private final boolean password;
    private final boolean selection;
    private String copied;
    private boolean cutCalled;

    private FakeEditor(
        String text, int caret, boolean editable, boolean password, boolean selection) {
      this.text = text;
      this.caret = caret;
      this.editable = editable;
      this.password = password;
      this.selection = selection;
    }

    @Override
    public boolean isPassword() {
      return password;
    }

    @Override
    public boolean isEditable() {
      return editable;
    }

    @Override
    public boolean hasSelection() {
      return selection;
    }

    @Override
    public String getText() {
      return text;
    }

    @Override
    public int getCaret() {
      return caret;
    }

    @Override
    public void select(int start, int end) {
      copied = text.substring(start, end);
    }

    @Override
    public void copy() {}

    @Override
    public void cut() {
      cutCalled = true;
      int[] range = TextLineClipboard.lineRange(text, caret);
      text = text.substring(0, range[0]) + text.substring(range[1]);
      caret = range[0];
    }

    @Override
    public void setCaret(int offset) {
      caret = offset;
    }
  }
}
