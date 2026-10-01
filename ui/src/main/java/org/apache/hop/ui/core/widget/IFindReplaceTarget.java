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

/**
 * Editor operations required by {@link org.apache.hop.ui.core.dialog.FindReplaceDialog}.
 *
 * <p>Implemented by multi-line {@link TextComposite} widgets and by {@code IContentEditorWidget}
 * (explorer text files). Keep this interface free of desktop-only SWT types so Hop Web can
 * implement it.
 */
public interface IFindReplaceTarget {

  /**
   * @return full editor text
   */
  String getText();

  /**
   * Replace the full editor text.
   *
   * @param text new content
   */
  void setText(String text);

  /**
   * @return selected text, or an empty string if there is no selection
   */
  String getSelectionText();

  /**
   * @return number of selected characters
   */
  int getSelectionCount();

  /**
   * Select the range {@code [start, end)}.
   *
   * @param start start offset (inclusive)
   * @param end end offset (exclusive)
   */
  void setSelection(int start, int end);

  /**
   * @return caret offset from the start of the text
   */
  int getCaretPosition();

  /**
   * Move the caret (and collapse the selection).
   *
   * @param position caret offset
   */
  void setCaretPosition(int position);

  /**
   * Replace the current selection with {@code text}, or insert at the caret when nothing is
   * selected.
   *
   * @param text replacement or insertion
   */
  void insert(String text);

  /**
   * @return {@code true} when the user can edit the text
   */
  boolean isEditable();

  /**
   * @return {@code true} when the underlying control has been disposed
   */
  boolean isDisposed();

  /**
   * Give focus to the editor.
   *
   * @return {@code true} if focus was assigned
   */
  boolean setFocus();

  /** Refresh toolbar enablement after an edit. Default is a no-op. */
  default void updateToolbar() {}
}
