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

/**
 * The blue {@code <null>} marker a data grid draws for a NULL value.
 *
 * <p>Color alone is not the marker. Editing a null cell replaces the text and, until the commit
 * path runs, leaves the blue foreground behind. Readers that look only at the color then keep
 * treating the typed value as null.
 */
public final class NullPlaceholder {

  /** Text drawn for a NULL cell. A real value that happens to be this text is not null. */
  public static final String TEXT = "<null>";

  private NullPlaceholder() {}

  /**
   * True when the cell still shows the null marker: the placeholder text and the null color
   * together. Either one on its own is a real value (or a value that has already been edited).
   *
   * @param text the cell text
   * @param foregroundIsNullColor true when the cell foreground is the null color
   * @return true when the cell is still the null placeholder
   */
  public static boolean isPlaceholder(String text, boolean foregroundIsNullColor) {
    return foregroundIsNullColor && TEXT.equals(text);
  }

  /**
   * True when a commit should drop the null color. The color stays only while the committed text is
   * still the placeholder, so every reader of the grid stops treating the cell as null.
   *
   * <p>On Windows, clicking OK runs the editor's focus-lost handler synchronously before the
   * button's selection event. The color has to change in that same commit. A later pass is too
   * late: OK has already read the cell.
   *
   * @param committedText the text just written back to the cell
   * @return true when the null color should be cleared
   */
  public static boolean clearNullColor(String committedText) {
    return !TEXT.equals(committedText);
  }
}
