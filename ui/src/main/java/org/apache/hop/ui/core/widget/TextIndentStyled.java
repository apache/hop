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
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Widget;

/**
 * StyledText half of {@link TextIndent}. Kept in its own class so Hop Web, which has no StyledText,
 * does not load this class.
 */
final class TextIndentStyled {

  private TextIndentStyled() {}

  static boolean isMultiLine(Widget widget) {
    return widget instanceof StyledText styledText && (styledText.getStyle() & SWT.SINGLE) == 0;
  }

  static boolean canIndent(Widget widget) {
    if (!(widget instanceof StyledText styledText)) {
      return false;
    }
    return !styledText.isDisposed()
        && styledText.isEnabled()
        && styledText.getEditable()
        && (styledText.getStyle() & SWT.SINGLE) == 0;
  }

  static boolean apply(Widget widget, boolean outdent) {
    if (!(widget instanceof StyledText styledText) || !canIndent(styledText)) {
      return false;
    }
    Point selection = styledText.getSelection();
    int anchor = selection == null ? styledText.getCaretOffset() : selection.x;
    int caret = selection == null ? anchor : selection.y;
    String original = styledText.getText();
    if (original == null) {
      original = "";
    }
    TextIndent.Edit edit = TextIndent.edit(original, anchor, caret, TextIndent.tabSize(), outdent);
    int end = edit.replaceStart() + edit.replaceLength();
    String slice = original.substring(edit.replaceStart(), Math.min(end, original.length()));
    if (!slice.equals(edit.replacement())) {
      styledText.replaceTextRange(edit.replaceStart(), edit.replaceLength(), edit.replacement());
      styledText.setSelection(edit.selectionStart(), edit.selectionEnd());
    } else if (anchor != edit.selectionStart() || caret != edit.selectionEnd()) {
      styledText.setSelection(edit.selectionStart(), edit.selectionEnd());
    }
    return true;
  }
}
