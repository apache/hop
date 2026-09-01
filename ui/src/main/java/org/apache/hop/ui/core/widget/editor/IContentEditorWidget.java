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

import org.apache.hop.ui.core.widget.IFindReplaceTarget;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.widgets.Control;
import org.jspecify.annotations.Nullable;

/**
 * Common interface for a content/code editor widget used in both Hop GUI (desktop) and Hop Web.
 * Desktop implementation uses RSyntaxTextArea; web implementation uses Monaco or a fallback.
 *
 * <p>Allows setting/getting text, setting the language (for syntax highlighting), and listening for
 * modifications through a single API.
 */
public interface IContentEditorWidget extends IFindReplaceTarget {

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "ContentEditor-Toolbar";

  public static final String GUI_PLUGIN_CONTEXT_MENU_PARENT_ID = "ContentEditor-ContextMenu";

  /**
   * SWT widget data key for a {@link Runnable} invoked on Ctrl+Enter / Cmd+Enter (execute the
   * statement around the caret in a SQL editor).
   */
  String DATA_EXECUTE_ACTION = "hop.contentEditor.executeAction";

  /**
   * Set on the editor control for the duration of a Ctrl+Enter keystroke so StyledText cannot
   * insert the Return. GTK VerifyKey events often arrive with {@code stateMask == 0}.
   */
  String DATA_EAT_EXECUTE_NEWLINE = "hop.contentEditor.eatExecuteNewline";

  /**
   * Walk ancestors of {@code start} for {@link #DATA_EXECUTE_ACTION}.
   *
   * @return the action, or {@code null} when none is registered
   */
  static Runnable executeActionOf(Control start) {
    Control current = start;
    while (current != null && !current.isDisposed()) {
      Object data = current.getData(DATA_EXECUTE_ACTION);
      if (data instanceof Runnable runnable) {
        return runnable;
      }
      current = current.getParent();
    }
    return null;
  }

  static boolean eatExecuteNewlineArmed(Control start) {
    Control current = start;
    while (current != null && !current.isDisposed()) {
      if (Boolean.TRUE.equals(current.getData(DATA_EAT_EXECUTE_NEWLINE))) {
        return true;
      }
      current = current.getParent();
    }
    return false;
  }

  /** Ctrl or Cmd held, Shift not held. */
  static boolean isExecuteModifier(int stateMask) {
    if ((stateMask & SWT.SHIFT) != 0) {
      return false;
    }
    return (stateMask & (SWT.MOD1 | SWT.CONTROL | SWT.COMMAND)) != 0;
  }

  /** Return / Enter, including keypad. */
  static boolean isExecuteNewline(int keyCode, char character) {
    int code = keyCode & SWT.KEY_MASK;
    return keyCode == SWT.CR
        || keyCode == SWT.KEYPAD_CR
        || code == SWT.CR
        || character == SWT.CR
        || character == SWT.LF;
  }

  /**
   * Ctrl/Cmd+Enter without Shift (main or keypad). GTK often reports Return as {@link SWT#CR} with
   * {@code character == 0} when Control is down.
   */
  static boolean isExecuteKey(int stateMask, int keyCode, char character) {
    return isExecuteModifier(stateMask) && isExecuteNewline(keyCode, character);
  }

  /**
   * True when {@code text} is only a line delimiter (what StyledText inserts for Enter).
   *
   * @param text the replacement text from an {@link SWT#Verify} event
   * @return true if the insert is a newline and should be eaten for Ctrl+Enter
   */
  static boolean isLineDelimiterText(@Nullable String text) {
    return "\n".equals(text) || "\r".equals(text) || "\r\n".equals(text);
  }

  /**
   * {@link SWT#TRAVERSE_RETURN} with Ctrl/Cmd. StyledText sets {@code doit=true} for Return when
   * modifiers are held so the shell can activate its default button; that consumes the key before
   * KeyDown on GTK.
   */
  static boolean isExecuteTraverse(int detail, int stateMask) {
    return detail == SWT.TRAVERSE_RETURN && isExecuteModifier(stateMask);
  }

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
   * Get the language used for syntax highlighting and validation.
   *
   * @return language identifier (e.g. "json", "xml", "javascript")
   */
  @Nullable String getLanguage();

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

  /** Cut selected text to clipboard. No-op if not supported by the implementation. */
  void cut();

  /** Paste from clipboard at the caret. No-op if not supported by the implementation. */
  void paste();

  /** Undo the last edit. No-op if not supported by the implementation. */
  void undo();

  /** Redo the last undone edit. No-op if not supported by the implementation. */
  void redo();

  @Override
  default boolean isDisposed() {
    Control control = getControl();
    return control == null || control.isDisposed();
  }

  @Override
  default boolean setFocus() {
    Control control = getControl();
    return control != null && !control.isDisposed() && control.setFocus();
  }
}
