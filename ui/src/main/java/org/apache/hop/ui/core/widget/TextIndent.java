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

import org.apache.hop.core.Const;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Widget;

/**
 * Indents or outdents the lines touched by a selection in a multi-line text field.
 *
 * <p>Tab adds {@link #tabSize()} spaces at the start of each of those lines. Shift+Tab removes one
 * indent: a leading tab, or up to that many leading spaces. The width comes from the environment
 * variable {@code HOP_TEXT_TAB_SIZE} and otherwise from the configuration variable of the same
 * name. The default is 2. Single-line fields, read-only fields, and Hop Web are left alone. Hop Web
 * indents in the browser.
 */
public final class TextIndent {

  static final String ATTACHED = "HOP_TEXT_INDENT";

  /** Widest indent accepted from {@code HOP_TEXT_TAB_SIZE}. Larger values fall back to 2. */
  static final int MAX_SIZE = 32;

  public static final int DEFAULT_SIZE = 2;

  private static final Class<?> STYLED_TEXT = loadStyledText();

  private static final int BLOCKED_MODIFIERS = SWT.CTRL | SWT.ALT | SWT.COMMAND | SWT.MOD1;

  private TextIndent() {}

  /**
   * Spaces per indent. The process environment wins over the configuration variable. Blank or
   * unusable values fall back to {@link #DEFAULT_SIZE}.
   */
  public static int tabSize() {
    return parse(
        System.getenv(Const.HOP_TEXT_TAB_SIZE), System.getProperty(Const.HOP_TEXT_TAB_SIZE));
  }

  static int parse(String environment, String property) {
    Integer fromEnvironment = parseOne(environment);
    if (fromEnvironment != null) {
      return fromEnvironment;
    }
    Integer fromProperty = parseOne(property);
    if (fromProperty != null) {
      return fromProperty;
    }
    return DEFAULT_SIZE;
  }

  private static Integer parseOne(String raw) {
    if (raw == null) {
      return null;
    }
    String trimmed = raw.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    try {
      int value = Integer.parseInt(trimmed);
      if (value < 1 || value > MAX_SIZE) {
        return null;
      }
      return value;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Indent or outdent the lines covered by {@code [anchor, caret]}.
   *
   * <p>A selection that ends at the first column of a line does not include that line. A caret
   * between the CR and LF of a CRLF stays on the line that break belongs to.
   */
  public static Edit edit(String text, int anchor, int caret, int size, boolean outdent) {
    if (text == null) {
      text = "";
    }
    if (size < 1) {
      size = DEFAULT_SIZE;
    }
    int length = text.length();
    int from = clamp(Math.min(anchor, caret), length);
    int to = clamp(Math.max(anchor, caret), length);

    int blockStart = lineStart(text, from);
    int blockEnd = includedEnd(text, from, to);

    StringBuilder replacement = new StringBuilder();
    int newFrom = -1;
    int newTo = -1;
    int cursor = blockStart;
    while (true) {
      int contentEnd = lineContentEnd(text, cursor);
      if (contentEnd > blockEnd) {
        contentEnd = blockEnd;
      }
      String line = text.substring(cursor, contentEnd);
      String changed = outdent ? outdentLine(line, size) : indentLine(line, size);
      int shift = Math.abs(changed.length() - line.length());
      int lineNew = blockStart + replacement.length();
      newFrom = mapPoint(newFrom, from, cursor, contentEnd, lineNew, shift, outdent);
      newTo = mapPoint(newTo, to, cursor, contentEnd, lineNew, shift, outdent);
      replacement.append(changed);
      cursor = contentEnd;
      if (cursor >= blockEnd) {
        break;
      }
      int breakEnd = breakEnd(text, cursor);
      if (breakEnd > blockEnd) {
        breakEnd = blockEnd;
      }
      int breakNew = blockStart + replacement.length();
      newFrom = mapBreak(newFrom, from, cursor, breakEnd, breakNew);
      newTo = mapBreak(newTo, to, cursor, breakEnd, breakNew);
      replacement.append(text, cursor, breakEnd);
      cursor = breakEnd;
      if (cursor >= blockEnd) {
        break;
      }
    }

    int delta = replacement.length() - (blockEnd - blockStart);
    if (newFrom < 0) {
      newFrom = from + delta;
    }
    if (newTo < 0) {
      newTo = to + delta;
    }
    return new Edit(blockStart, blockEnd - blockStart, replacement.toString(), newFrom, newTo);
  }

  /**
   * Apply Tab or Shift+Tab to {@code event}. Hop Web returns {@code false} so the browser script
   * can indent during the key gesture.
   *
   * @return {@code true} when the event was consumed
   */
  public static boolean handleKey(Event event) {
    if (event == null || event.widget == null || isWeb()) {
      return false;
    }
    if (!isPlainTab(event.keyCode, event.character, event.stateMask)) {
      return false;
    }
    if (!apply(event.widget, (event.stateMask & SWT.SHIFT) != 0)) {
      return false;
    }
    event.doit = false;
    return true;
  }

  /**
   * Keep Tab and Shift+Tab inside an editable multi-line field. Registered after the widget's own
   * traverse listener, which would otherwise move focus on Shift+Tab.
   */
  public static void handleTraverse(Event event) {
    if (event == null || event.widget == null) {
      return;
    }
    if (event.detail != SWT.TRAVERSE_TAB_NEXT && event.detail != SWT.TRAVERSE_TAB_PREVIOUS) {
      return;
    }
    if ((event.stateMask & BLOCKED_MODIFIERS) != 0) {
      return;
    }
    if (!canIndent(event.widget)) {
      return;
    }
    event.doit = false;
    event.detail = SWT.TRAVERSE_NONE;
  }

  /** Listen for Tab traversal on a multi-line field. A second call does nothing. */
  public static void attach(Widget widget) {
    if (!(widget instanceof Control control)) {
      return;
    }
    try {
      if (control.isDisposed() || control.getData(ATTACHED) != null || !isMultiLine(control)) {
        return;
      }
      control.setData(ATTACHED, Boolean.TRUE);
      control.addListener(SWT.Traverse, TextIndent::handleTraverse);
    } catch (SWTException e) {
      // The widget was disposed while focus moved.
    }
  }

  static boolean apply(Widget widget, boolean outdent) {
    try {
      if (widget instanceof Text text) {
        return applyText(text, outdent);
      }
      if (isStyledText(widget)) {
        return TextIndentStyled.apply(widget, outdent);
      }
      return false;
    } catch (SWTException e) {
      return false;
    }
  }

  private static boolean applyText(Text text, boolean outdent) {
    if (!canIndentText(text)) {
      return false;
    }
    Point selection = text.getSelection();
    int anchor = selection == null ? text.getCaretPosition() : selection.x;
    int caret = selection == null ? anchor : selection.y;
    String original = text.getText();
    if (original == null) {
      original = "";
    }
    Edit edit = edit(original, anchor, caret, tabSize(), outdent);
    int end = edit.replaceStart() + edit.replaceLength();
    String slice = original.substring(edit.replaceStart(), Math.min(end, original.length()));
    if (!slice.equals(edit.replacement())) {
      text.setSelection(edit.replaceStart(), end);
      text.insert(edit.replacement());
      text.setSelection(edit.selectionStart(), edit.selectionEnd());
    } else if (anchor != edit.selectionStart() || caret != edit.selectionEnd()) {
      text.setSelection(edit.selectionStart(), edit.selectionEnd());
    }
    return true;
  }

  private static boolean canIndent(Widget widget) {
    try {
      if (widget instanceof Text text) {
        return canIndentText(text);
      }
      if (isStyledText(widget)) {
        return TextIndentStyled.canIndent(widget);
      }
      return false;
    } catch (SWTException e) {
      return false;
    }
  }

  private static boolean canIndentText(Text text) {
    return !text.isDisposed()
        && text.isEnabled()
        && text.getEditable()
        && (text.getStyle() & SWT.MULTI) != 0
        && (text.getStyle() & SWT.READ_ONLY) == 0
        && (text.getStyle() & SWT.PASSWORD) == 0;
  }

  private static boolean isMultiLine(Widget widget) {
    if (widget instanceof Text text) {
      return (text.getStyle() & SWT.MULTI) != 0;
    }
    return isStyledText(widget) && TextIndentStyled.isMultiLine(widget);
  }

  private static boolean isPlainTab(int keyCode, char character, int stateMask) {
    if ((stateMask & BLOCKED_MODIFIERS) != 0) {
      return false;
    }
    return keyCode == SWT.TAB || character == '\t';
  }

  private static boolean isWeb() {
    return EnvironmentUtils.getInstance().isWeb();
  }

  private static boolean isStyledText(Widget widget) {
    return STYLED_TEXT != null && STYLED_TEXT.isInstance(widget);
  }

  private static Class<?> loadStyledText() {
    try {
      return Class.forName("org.eclipse.swt.custom.StyledText");
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  private static String indentLine(String line, int size) {
    return " ".repeat(size) + line;
  }

  private static String outdentLine(String line, int size) {
    if (!line.isEmpty() && line.charAt(0) == '\t') {
      return line.substring(1);
    }
    int spaces = 0;
    int limit = Math.min(size, line.length());
    while (spaces < limit && line.charAt(spaces) == ' ') {
      spaces++;
    }
    return line.substring(spaces);
  }

  private static int mapPoint(
      int already, int pos, int start, int end, int newStart, int shift, boolean outdent) {
    if (already >= 0 || pos < start || pos > end) {
      return already;
    }
    int relative = pos - start;
    int mapped = outdent ? Math.max(0, relative - shift) : relative + shift;
    return newStart + mapped;
  }

  private static int mapBreak(int already, int pos, int start, int end, int newStart) {
    if (already >= 0 || pos <= start || pos > end) {
      return already;
    }
    return newStart + (pos - start);
  }

  private static int includedEnd(String text, int from, int to) {
    if (to > from && isAtNextLine(text, to)) {
      return lineContentEnd(text, to - 1);
    }
    return lineContentEnd(text, to);
  }

  private static boolean isAtNextLine(String text, int offset) {
    if (offset <= 0 || offset > text.length()) {
      return false;
    }
    if (offset < text.length() && text.charAt(offset - 1) == '\r' && text.charAt(offset) == '\n') {
      return false;
    }
    return isBreak(text.charAt(offset - 1));
  }

  private static int lineStart(String text, int offset) {
    int index = normalize(text, clamp(offset, text.length()));
    while (index > 0 && !isBreak(text.charAt(index - 1))) {
      index--;
    }
    return index;
  }

  private static int lineContentEnd(String text, int offset) {
    int index = normalize(text, clamp(offset, text.length()));
    if (index < text.length() && isBreak(text.charAt(index))) {
      return index;
    }
    while (index < text.length() && !isBreak(text.charAt(index))) {
      index++;
    }
    return index;
  }

  private static int breakEnd(String text, int offset) {
    if (offset >= text.length()) {
      return offset;
    }
    char current = text.charAt(offset);
    if (current == '\r') {
      int next = offset + 1;
      if (next < text.length() && text.charAt(next) == '\n') {
        return next + 1;
      }
      return next;
    }
    if (current == '\n') {
      return offset + 1;
    }
    return offset;
  }

  /** A caret between CR and LF still belongs to the line that ends with that break. */
  private static int normalize(String text, int offset) {
    if (offset > 0
        && offset < text.length()
        && text.charAt(offset - 1) == '\r'
        && text.charAt(offset) == '\n') {
      return offset - 1;
    }
    return offset;
  }

  private static boolean isBreak(char character) {
    return character == '\n' || character == '\r';
  }

  private static int clamp(int offset, int length) {
    if (offset < 0) {
      return 0;
    }
    if (offset > length) {
      return length;
    }
    return offset;
  }

  /** Replacement of {@code [replaceStart, replaceStart + replaceLength)} and the new selection. */
  public record Edit(
      int replaceStart,
      int replaceLength,
      String replacement,
      int selectionStart,
      int selectionEnd) {}
}
