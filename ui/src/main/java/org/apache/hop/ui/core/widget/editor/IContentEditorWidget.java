/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use it except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.widget.editor;

import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.widgets.Control;

/**
 * Common interface for a content/code editor widget used in both Hop GUI (desktop) and Hop Web.
 * Desktop implementation uses RSyntaxTextArea; web implementation uses Monaco or a fallback.
 *
 * <p>Allows setting/getting text, setting the language (for syntax highlighting), and listening for
 * modifications through a single API.
 */
public interface IContentEditorWidget {

  /**
   * The SWT control to attach to a layout (e.g. the editor composite or the AWT bridge canvas).
   *
   * @return the control that should be laid out
   */
  Control getControl();

  /**
   * Get the current text content.
   *
   * @return full text in the editor
   */
  String getText();

  /**
   * Set the full text content. May fire modify listeners.
   *
   * @param text the new content
   */
  void setText(String text);

  /**
   * Set the full text content without firing modify listeners. Use when loading or reloading
   * content so that the handler does not mark the file as changed.
   *
   * @param text the new content
   */
  void setTextSuppressModify(String text);

  /**
   * Set the language/mode used for syntax highlighting and validation. Interpretation is
   * implementation-specific; use lowercase identifiers such as "json", "xml", "javascript".
   *
   * @param languageId language identifier (e.g. "json", "xml", "javascript")
   */
  void setLanguage(String languageId);

  /**
   * Set whether the editor is read-only. When true, the user cannot edit the content; used e.g.
   * when viewing binary files as text.
   *
   * @param readOnly true to make the editor read-only, false to allow editing
   */
  void setReadOnly(boolean readOnly);

  /**
   * Add a listener that is notified when the content is modified by the user.
   *
   * @param listener the listener to add
   */
  void addModifyListener(ModifyListener listener);

  /**
   * Remove a previously added modify listener.
   *
   * @param listener the listener to remove
   */
  void removeModifyListener(ModifyListener listener);

  /** Select all text in the editor. */
  void selectAll();

  /** Clear selection. */
  void unselectAll();

  /** Copy selected text to clipboard. */
  void copy();
}
