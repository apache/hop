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

import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.widgets.Widget;

/**
 * StyledText half of {@link TextLineClipboard}. Kept in its own class so Hop Web, which has no
 * StyledText, does not load this class.
 */
final class StyledTextLineClipboard {

  private StyledTextLineClipboard() {}

  static TextLineClipboard.Editor editorFor(Widget widget) {
    if (!(widget instanceof StyledText styledText)) {
      return null;
    }
    return new Editor(styledText);
  }

  private static final class Editor implements TextLineClipboard.Editor {
    private final StyledText text;

    private Editor(StyledText text) {
      this.text = text;
    }

    @Override
    public boolean isPassword() {
      return false;
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
      return text.getCaretOffset();
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
}
