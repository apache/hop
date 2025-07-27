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

import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.dnd.Clipboard;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.events.MenuDetectListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;

public abstract class TextComposite extends Composite {
  private static final Class<?> PKG = StyledTextComp.class;

  /**
   * Constructs a new instance of this class given its parent and a style value describing its
   * behavior and appearance.
   *
   * <p>The style value is either one of the style constants defined in class <code>SWT</code> which
   * is applicable to instances of this class, or must be built by <em>bitwise OR</em>'ing together
   * (that is, using the <code>int</code> "|" operator) two or more of those <code>SWT</code> style
   * constants. The class description lists the style constants that are applicable to the class.
   * Style bits are also inherited from superclasses.
   *
   * @param parent a widget which will be the parent of the new instance (cannot be null)
   * @param style the style of widget to construct
   * @throws IllegalArgumentException
   */
  public TextComposite(Composite parent, int style) {
    super(parent, style);
  }

  public abstract void addModifyListener(ModifyListener lsMod);

  public abstract void addLineStyleListener();

  public abstract void addLineStyleListener(List<String> keywords);

  public void addLineStyleListener(String scriptEngine) {
    throw new UnsupportedOperationException("Cannot specify a script engine");
  }

  /**
   * Adds the listener to the collection of listeners who will be notified when the
   * platform-specific context menu trigger has occurred, by sending it one of the messages defined
   * in the <code>MenuDetectListener</code> interface.
   *
   * @param listener the listener which should be notified
   */
  public abstract void addMenuDetectListener(MenuDetectListener listener);

  /** Sets the receiver's pop up menu to the argument. */
  public abstract void setMenu(Menu menu);

  /** Cuts the selected text. */
  public abstract void cut();

  /** Copies the selected text. */
  public abstract void copy();

  /** Pastes text from clipboard. */
  public abstract void paste();

  /** Selects all the text in the receiver. */
  public abstract void selectAll();

  /**
   * Returns the caret position relative to the start of the text.
   *
   * <p>Indexing is zero based.
   *
   * @return the caret position relative to the start of the text.
   */
  public abstract int getCaretPosition();

  /**
   * Sets the caret position.
   *
   * @param position set caret offset, relative to the first character in the text.
   */
  public abstract void setCaretPosition(int position);

  /**
   * Gets the number of characters.
   *
   * @return number of characters in the widget
   */
  public abstract int getCharCount();

  /**
   * @return The caret line number, starting from 1.
   */
  public int getLineNumber() {
    String text = getText();
    if (StringUtils.isEmpty(text)) {
      return 1;
    }

    int rowNumber = 1;
    int textPosition = getCaretPosition();
    while (textPosition > 0) {
      if (text.charAt(textPosition - 1) == '\n') {
        rowNumber++;
      }
      textPosition--;
    }

    return rowNumber;
  }

  /**
   * @return The caret column number, starting from 1.
   */
  public int getColumnNumber() {
    String text = getText();
    if (StringUtils.isEmpty(text)) {
      return 1;
    }

    int columnNumber = 1;
    int textPosition = getCaretPosition();
    while (textPosition > 0
        && text.charAt(textPosition - 1) != '\n'
        && text.charAt(textPosition - 1) != '\r') {
      textPosition--;
      columnNumber++;
    }

    return columnNumber;
  }

  /**
   * Returns the widget text.
   *
   * <p>The text for a text widget is the characters in the widget, or an empty string if this has
   * never been set.
   *
   * @return the widget text
   */
  public abstract String getText();

  /** Sets the contents of the receiver to the given string. */
  public abstract void setText(String text);

  /**
   * Returns the number of selected characters.
   *
   * @return the number of selected characters.
   */
  public abstract int getSelectionCount();

  /** Gets the selected text, or an empty string if there is no current selection. */
  public abstract String getSelectionText();

  /**
   * Inserts a string.
   *
   * <p>The old selection is replaced with the new text.
   *
   * @param strInsert the string
   */
  public abstract void insert(String strInsert);

  /**
   * Sets the selection.
   *
   * <p>Indexing is zero based. The range of a selection is from 0..N where N is the number of
   * characters in the widget.
   *
   * @param start new caret position
   */
  public abstract void setSelection(int start);

  /**
   * Sets the selection to the range specified by the given start and end indices.
   *
   * <p>Indexing is zero based. The range of a selection is from 0..N where N is the number of
   * characters in the widget.
   *
   * <p>
   *
   * @param start the start of the range
   * @param end the end of the range
   */
  public abstract void setSelection(int start, int end);

  /**
   * Returns the editable state.
   *
   * @return whether or not the receiver is editable
   */
  public abstract boolean isEditable();

  /**
   * Sets the editable state.
   *
   * @param editable the new editable state
   */
  public abstract void setEditable(boolean editable);

  /**
   * Check if something is stored inside the Clipboard.
   *
   * @return false if no text is available inside the Clipboard
   */
  protected boolean checkPaste() {
    try {
      Clipboard clipboard = new Clipboard(getParent().getDisplay());
      String text = (String) clipboard.getContents(TextTransfer.getInstance());
      if (text != null && !text.isEmpty()) {
        return true;
      } else {
        return false;
      }
    } catch (Exception e) {
      return false;
    }
  }

  protected void undo() {}

  protected void redo() {}

  protected boolean isSupportUnoRedo() {
    return false;
  }

  protected void buildingStyledTextMenu(Menu popupMenu) {

    if (isSupportUnoRedo()) {
      final MenuItem undoItem = new MenuItem(popupMenu, SWT.PUSH);
      undoItem.setText(
          OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "WidgetDialog.Styled.Undo")));
      undoItem.setImage(
          GuiResource.getInstance()
              .getImage("ui/images/undo.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
      undoItem.addListener(SWT.Selection, event -> undo());

      final MenuItem redoItem = new MenuItem(popupMenu, SWT.PUSH);
      redoItem.setText(
          OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "WidgetDialog.Styled.Redo")));
      redoItem.setImage(
          GuiResource.getInstance()
              .getImage("ui/images/redo.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
      redoItem.addListener(SWT.Selection, event -> redo());

      new MenuItem(popupMenu, SWT.SEPARATOR);
    }

    final MenuItem cutItem = new MenuItem(popupMenu, SWT.PUSH);
    cutItem.setText(
        OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "WidgetDialog.Styled.Cut")));
    cutItem.setImage(
        GuiResource.getInstance()
            .getImage("ui/images/cut.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
    cutItem.addListener(SWT.Selection, event -> cut());

    final MenuItem copyItem = new MenuItem(popupMenu, SWT.PUSH);
    copyItem.setText(
        OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "WidgetDialog.Styled.Copy")));
    copyItem.setImage(
        GuiResource.getInstance()
            .getImage("ui/images/copy.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
    copyItem.addListener(SWT.Selection, event -> copy());

    final MenuItem pasteItem = new MenuItem(popupMenu, SWT.PUSH);
    pasteItem.setText(
        OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "WidgetDialog.Styled.Paste")));
    pasteItem.setImage(
        GuiResource.getInstance()
            .getImage("ui/images/paste.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
    pasteItem.addListener(SWT.Selection, event -> paste());

    new MenuItem(popupMenu, SWT.SEPARATOR);

    final MenuItem selectAllItem = new MenuItem(popupMenu, SWT.PUSH);
    selectAllItem.setText(
        OsHelper.customizeMenuitemText(
            BaseMessages.getString(PKG, "WidgetDialog.Styled.SelectAll")));
    selectAllItem.setImage(
        GuiResource.getInstance()
            .getImage(
                "ui/images/select-all.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
    selectAllItem.addListener(SWT.Selection, event -> selectAll());

    addListener(
        SWT.KeyDown,
        event -> {
          if (isSupportUnoRedo()
              && event.keyCode == 'z'
              && (event.stateMask & SWT.MOD1) != 0
              && (event.stateMask & SWT.MOD2) != 0) {
            redo();
          } else if (isSupportUnoRedo()
              && event.keyCode == 'z'
              && (event.stateMask & SWT.MOD1) != 0) {
            undo();
          } else if (event.keyCode == 'a' && (event.stateMask & SWT.MOD1) != 0) {
            selectAll();
          } else if (event.keyCode == 'f' && (event.stateMask & SWT.MOD1) != 0) {
            // TODO: implement FIND
            // find();
          } else if (event.keyCode == 'h' && (event.stateMask & SWT.MOD1) != 0) {
            // TODO: implement FIND AND REPLACE
            // findAndReplace();
          }
        });

    addMenuDetectListener(
        event -> {
          pasteItem.setEnabled(checkPaste());
          if (getSelectionCount() > 0) {
            cutItem.setEnabled(true);
            copyItem.setEnabled(true);
          } else {
            cutItem.setEnabled(false);
            copyItem.setEnabled(false);
          }
        });

    setMenu(popupMenu);
  }
}
