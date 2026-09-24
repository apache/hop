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

import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Widget;

/**
 * Copies or cuts the line under the caret when a text widget has no selection.
 *
 * <p>A selection is left to the widget. Password fields are left alone. The line includes its line
 * break ({@code \n}, {@code \r\n} or {@code \r}) when it has one; the last line does not gain a
 * break it did not have.
 */
public final class TextLineClipboard {

  private TextLineClipboard() {}

  /**
   * Line that contains {@code caret}, as a half-open range {@code [start, end)}.
   *
   * <p>A caret sitting between the CR and LF of a CRLF still belongs to the line that ends with
   * that break.
   */
  public static int[] lineRange(String text, int caret) {
    if (text == null) {
      text = "";
    }
    int length = text.length();
    if (caret < 0) {
      caret = 0;
    } else if (caret > length) {
      caret = length;
    }
    if (caret > 0
        && caret < length
        && text.charAt(caret - 1) == '\r'
        && text.charAt(caret) == '\n') {
      caret--;
    }

    int start = caret;
    while (start > 0) {
      char previous = text.charAt(start - 1);
      if (previous == '\n' || previous == '\r') {
        break;
      }
      start--;
    }

    int end = caret;
    while (end < length) {
      char current = text.charAt(end);
      if (current == '\r') {
        end++;
        if (end < length && text.charAt(end) == '\n') {
          end++;
        }
        break;
      }
      if (current == '\n') {
        end++;
        break;
      }
      end++;
    }
    return new int[] {start, end};
  }

  /**
   * Copies or cuts the current line when {@code widget} has no selection.
   *
   * @param cut {@code true} to cut, {@code false} to copy
   * @return {@code true} when the line was copied or cut. {@code false} when the widget should keep
   *     its own copy/cut (a selection, a password field, a read-only cut, or an unsupported
   *     widget).
   */
  public static boolean copyOrCutCurrentLine(Widget widget, boolean cut) {
    try {
      Editor editor = editorFor(widget);
      if (editor == null) {
        return false;
      }
      return copyOrCutCurrentLine(editor, cut);
    } catch (SWTException e) {
      return false;
    }
  }

  static boolean copyOrCutCurrentLine(Editor editor, boolean cut) {
    if (editor == null || editor.isPassword() || editor.hasSelection()) {
      return false;
    }
    if (cut && !editor.isEditable()) {
      return false;
    }
    String text = editor.getText();
    if (text == null) {
      text = "";
    }
    int caret = editor.getCaret();
    int[] range = lineRange(text, caret);
    editor.select(range[0], range[1]);
    if (cut) {
      editor.cut();
      editor.setCaret(range[0]);
    } else {
      try {
        editor.copy();
      } finally {
        editor.setCaret(caret);
      }
    }
    return true;
  }

  private static Editor editorFor(Widget widget) {
    if (widget instanceof Text text) {
      return new TextEditor(text);
    }
    if (widget instanceof Combo combo) {
      return new ComboEditor(combo);
    }
    if (widget instanceof CCombo combo) {
      return new CComboEditor(combo);
    }
    if (isStyledText(widget)) {
      return StyledTextLineClipboard.editorFor(widget);
    }
    return null;
  }

  private static boolean isStyledText(Widget widget) {
    try {
      Class<?> type = Class.forName("org.eclipse.swt.custom.StyledText");
      return type.isInstance(widget);
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /** A text caret that can copy or cut its own selection. Package-visible for tests. */
  interface Editor {
    boolean isPassword();

    boolean isEditable();

    boolean hasSelection();

    String getText();

    int getCaret();

    void select(int start, int end);

    void copy();

    void cut();

    void setCaret(int offset);
  }

  private static final class TextEditor implements Editor {
    private final Text text;

    private TextEditor(Text text) {
      this.text = text;
    }

    @Override
    public boolean isPassword() {
      return (text.getStyle() & SWT.PASSWORD) != 0;
    }

    @Override
    public boolean isEditable() {
      return text.getEditable();
    }

    @Override
    public boolean hasSelection() {
      return text.getSelectionCount() > 0;
    }

    @Override
    public String getText() {
      return text.getText();
    }

    @Override
    public int getCaret() {
      return text.getCaretPosition();
    }

    @Override
    public void select(int start, int end) {
      text.setSelection(start, end);
    }

    @Override
    public void copy() {
      text.copy();
    }

    @Override
    public void cut() {
      text.cut();
    }

    @Override
    public void setCaret(int offset) {
      text.setSelection(offset);
    }
  }

  private static final class ComboEditor implements Editor {
    private final Combo combo;

    private ComboEditor(Combo combo) {
      this.combo = combo;
    }

    @Override
    public boolean isPassword() {
      return (combo.getStyle() & SWT.PASSWORD) != 0;
    }

    @Override
    public boolean isEditable() {
      return (combo.getStyle() & SWT.READ_ONLY) == 0;
    }

    @Override
    public boolean hasSelection() {
      Point selection = combo.getSelection();
      return selection != null && selection.x != selection.y;
    }

    @Override
    public String getText() {
      return combo.getText();
    }

    @Override
    public int getCaret() {
      return combo.getCaretPosition();
    }

    @Override
    public void select(int start, int end) {
      combo.setSelection(new Point(start, end));
    }

    @Override
    public void copy() {
      combo.copy();
    }

    @Override
    public void cut() {
      combo.cut();
    }

    @Override
    public void setCaret(int offset) {
      combo.setSelection(new Point(offset, offset));
    }
  }

  private static final class CComboEditor implements Editor {
    private final CCombo combo;

    private CComboEditor(CCombo combo) {
      this.combo = combo;
    }

    @Override
    public boolean isPassword() {
      return false;
    }

    @Override
    public boolean isEditable() {
      return combo.getEditable();
    }

    @Override
    public boolean hasSelection() {
      Point selection = combo.getSelection();
      return selection != null && selection.x != selection.y;
    }

    @Override
    public String getText() {
      return combo.getText();
    }

    @Override
    public int getCaret() {
      Point selection = combo.getSelection();
      return selection == null ? 0 : selection.x;
    }

    @Override
    public void select(int start, int end) {
      combo.setSelection(new Point(start, end));
    }

    @Override
    public void copy() {
      combo.copy();
    }

    @Override
    public void cut() {
      combo.cut();
    }

    @Override
    public void setCaret(int offset) {
      combo.setSelection(new Point(offset, offset));
    }
  }
}
