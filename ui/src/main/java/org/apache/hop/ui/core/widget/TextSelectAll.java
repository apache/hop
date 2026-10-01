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

import org.eclipse.swt.SWTException;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Widget;

/**
 * Selects all of the text in a text widget.
 *
 * <p>{@link org.eclipse.swt.custom.StyledText} does not bind Ctrl/Cmd+A, and on Windows the native
 * text control does not either. Hop Web cancels that chord so the graph can use it, which also
 * takes it away from the browser. Call this from the key handler instead.
 */
public final class TextSelectAll {

  private TextSelectAll() {}

  /**
   * Selects all text in {@code widget}.
   *
   * @return {@code true} when the widget supports it. {@code false} for anything else, including a
   *     widget that was disposed while the key was delivered.
   */
  public static boolean selectAll(Widget widget) {
    if (widget == null) {
      return false;
    }
    try {
      if (widget instanceof Text text) {
        text.selectAll();
        return true;
      }
      if (widget instanceof Combo combo) {
        combo.setSelection(new Point(0, length(combo.getText())));
        return true;
      }
      if (widget instanceof CCombo combo) {
        combo.setSelection(new Point(0, length(combo.getText())));
        return true;
      }
      if (isStyledText(widget)) {
        return StyledTextSelectAll.selectAll(widget);
      }
      return false;
    } catch (SWTException e) {
      return false;
    }
  }

  private static int length(String text) {
    return text == null ? 0 : text.length();
  }

  private static boolean isStyledText(Widget widget) {
    try {
      Class<?> type = Class.forName("org.eclipse.swt.custom.StyledText");
      return type.isInstance(widget);
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
