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

import java.util.LinkedList;
import java.util.List;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.highlight.JavaHighlight;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.LineStyleListener;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.MenuDetectListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Menu;

public class StyledTextVar extends TextComposite {
  private static final Class<?> PKG = StyledTextVar.class;

  // Modification for Undo/Redo on Styled Text
  private static final int MAX_STACK_SIZE = 25;

  private final StyledText wText;
  private final Menu wPopupMenu;
  private List<UndoRedoStack> undoStack;
  private List<UndoRedoStack> redoStack;

  private boolean fullSelection = false;

  public StyledTextVar(IVariables variables, Composite parent, int style) {
    this(variables, parent, style, true, false);
  }

  public StyledTextVar(IVariables variables, Composite parent, int style, boolean varsSensitive) {
    this(variables, parent, style, varsSensitive, false);
  }

  public StyledTextVar(
      IVariables variables,
      Composite parent,
      int style,
      boolean varsSensitive,
      boolean variableIconOnTop) {

    super(parent, SWT.NONE);

    undoStack = new LinkedList<>();
    redoStack = new LinkedList<>();

    wText = new StyledText(this, style);
    wPopupMenu = new Menu(parent.getShell(), SWT.POP_UP);
    this.setLayout(new FormLayout());

    buildingStyledTextMenu(wPopupMenu);

    addUndoRedoSupport();

    // Default layout without variables
    wText.setLayoutData(new FormDataBuilder().top().left().right(100, 0).bottom(100, 0).result());

    // Special layout for variables decorator
    if (varsSensitive) {
      wText.addKeyListener(new ControlSpaceKeyAdapter(variables, wText));
      if (variableIconOnTop) {
        final Label wIcon = new Label(this, SWT.RIGHT);
        PropsUi.setLook(wIcon);
        wIcon.setToolTipText(BaseMessages.getString(PKG, "StyledTextComp.tooltip.InsertVariable"));
        wIcon.setImage(GuiResource.getInstance().getImageVariableMini());
        wIcon.setLayoutData(new FormDataBuilder().top().right(100, 0).result());
        wText.setLayoutData(
            new FormDataBuilder()
                .top(new FormAttachment(wIcon, 0, 0))
                .left()
                .right(100, 0)
                .bottom(100, 0)
                .result());
      } else {
        Label controlDecoration = new Label(this, SWT.NONE);
        controlDecoration.setImage(GuiResource.getInstance().getImageVariableMini());
        controlDecoration.setToolTipText(
            BaseMessages.getString(PKG, "StyledTextComp.tooltip.InsertVariable"));
        PropsUi.setLook(controlDecoration);
        controlDecoration.setLayoutData(new FormDataBuilder().top().right(100, 0).result());
        wText.setLayoutData(
            new FormDataBuilder()
                .top()
                .left()
                .right(new FormAttachment(controlDecoration, 0, 0))
                .bottom(100, 0)
                .result());
      }
    }
  }

  @Override
  public String getSelectionText() {
    return wText.getSelectionText();
  }

  @Override
  public int getCaretPosition() {
    return wText.getCaretOffset();
  }

  @Override
  public void setCaretPosition(int offset) {
    wText.setCaretOffset(offset);
  }

  @Override
  public int getCharCount() {
    return wText.getCharCount();
  }

  @Override
  public String getText() {
    return wText.getText();
  }

  @Override
  public void setText(String text) {
    wText.setText(text);
  }

  @Override
  public void insert(String strInsert) {
    wText.insert(strInsert);
  }

  @Override
  public void addListener(int eventType, Listener listener) {
    wText.addListener(eventType, listener);
  }

  public void addModifyListener(ModifyListener lsMod) {
    wText.addModifyListener(lsMod);
  }

  @Override
  public void addLineStyleListener() {
    addLineStyleListener(new JavaHighlight());
  }

  @Override
  public void addLineStyleListener(List<String> keywords) {
    // No listener required
  }

  public void addLineStyleListener(LineStyleListener lineStyler) {
    wText.addLineStyleListener(lineStyler);
  }

  public void addKeyListener(KeyAdapter keyAdapter) {
    wText.addKeyListener(keyAdapter);
  }

  public void addFocusListener(FocusAdapter focusAdapter) {
    wText.addFocusListener(focusAdapter);
  }

  public void addMouseListener(MouseAdapter mouseAdapter) {
    wText.addMouseListener(mouseAdapter);
  }

  @Override
  public void addMenuDetectListener(MenuDetectListener listener) {
    wText.addMenuDetectListener(listener);
  }

  @Override
  public int getSelectionCount() {
    return wText.getSelectionCount();
  }

  @Override
  public void setSelection(int start) {
    wText.setSelection(start);
  }

  @Override
  public void setSelection(int start, int end) {
    wText.setSelection(start, end);
  }

  @Override
  public void setBackground(Color color) {
    super.setBackground(color);
    wText.setBackground(color);
  }

  @Override
  public void setForeground(Color color) {
    super.setForeground(color);
    wText.setForeground(color);
  }

  @Override
  public void setFont(Font fnt) {
    wText.setFont(fnt);
  }

  public StyledText getTextWidget() {
    return wText;
  }

  @Override
  public boolean isEditable() {
    return wText.getEditable();
  }

  @Override
  public void setEditable(boolean editable) {
    wText.setEditable(editable);
  }

  @Override
  public void setEnabled(boolean enabled) {
    wText.setEnabled(enabled);
    // StyledText component does not get the "disabled" look, so it needs to be applied explicitly
    if (Display.getDefault() != null) {
      wText.setBackground(
          enabled
              ? GuiResource.getInstance().getColorWhite()
              : GuiResource.getInstance().getColorBackground());
    }
  }

  @Override
  public void cut() {
    wText.cut();
  }

  @Override
  public void copy() {
    wText.copy();
  }

  @Override
  public void paste() {
    wText.paste();
  }

  @Override
  public void selectAll() {
    wText.selectAll();
  }

  @Override
  public void setMenu(Menu menu) {
    wText.setMenu(menu);
  }

  @Override
  protected boolean isSupportUnoRedo() {
    return true;
  }

  // Add support functions for Undo/Redo on text widget
  protected void addUndoRedoSupport() {

    wText.addListener(
        SWT.Selection,
        event -> {
          if (getSelectionCount() == wText.getCharCount()) {
            fullSelection = true;
          }
        });

    wText.addExtendedModifyListener(
        event -> {
          int eventLength = event.length;
          int eventStartPostition = event.start;

          String newText = getText();
          String repText = event.replacedText;
          String oldText = "";
          int eventType = -1;

          if ((event.length != newText.length()) || (fullSelection)) {
            if (repText != null && !repText.isEmpty()) {
              oldText =
                  newText.substring(0, event.start)
                      + repText
                      + newText.substring(event.start + event.length);
              eventType = UndoRedoStack.DELETE;
              eventLength = repText.length();
            } else {
              oldText =
                  newText.substring(0, event.start) + newText.substring(event.start + event.length);
              eventType = UndoRedoStack.INSERT;
            }

            if ((oldText != null && !oldText.isEmpty()) || (eventStartPostition == event.length)) {
              UndoRedoStack urs =
                  new UndoRedoStack(eventStartPostition, newText, oldText, eventLength, eventType);

              // Stack is full
              if (undoStack.size() == MAX_STACK_SIZE) {
                undoStack.remove(undoStack.size() - 1);
              }
              undoStack.add(0, urs);
            }
          }
          fullSelection = false;
        });
  }

  @Override
  protected void undo() {
    if (!undoStack.isEmpty()) {
      UndoRedoStack undo = undoStack.remove(0);
      if (redoStack.size() == MAX_STACK_SIZE) {
        redoStack.remove(redoStack.size() - 1);
      }
      UndoRedoStack redo =
          new UndoRedoStack(
              undo.getCursorPosition(),
              undo.getReplacedText(),
              getText(),
              undo.getEventLength(),
              undo.getType());
      fullSelection = false;
      setText(undo.getReplacedText());
      if (undo.getType() == UndoRedoStack.INSERT) {
        setCaretPosition(undo.getCursorPosition());
      } else if (undo.getType() == UndoRedoStack.DELETE) {
        setCaretPosition(undo.getCursorPosition() + undo.getEventLength());
        setSelection(undo.getCursorPosition(), undo.getCursorPosition() + undo.getEventLength());
        if (getSelectionCount() == getCharCount()) {
          fullSelection = true;
        }
      }
      redoStack.add(0, redo);
    }
  }

  @Override
  protected void redo() {
    if (!redoStack.isEmpty()) {
      UndoRedoStack redo = redoStack.remove(0);
      if (undoStack.size() == MAX_STACK_SIZE) {
        undoStack.remove(undoStack.size() - 1);
      }
      UndoRedoStack undo =
          new UndoRedoStack(
              redo.getCursorPosition(),
              redo.getReplacedText(),
              getText(),
              redo.getEventLength(),
              redo.getType());
      fullSelection = false;
      setText(redo.getReplacedText());
      if (redo.getType() == UndoRedoStack.INSERT) {
        setCaretPosition(redo.getCursorPosition());
      } else if (redo.getType() == UndoRedoStack.DELETE) {
        setCaretPosition(redo.getCursorPosition() + redo.getEventLength());
        setSelection(redo.getCursorPosition(), redo.getCursorPosition() + redo.getEventLength());
        if (getSelectionCount() == getCharCount()) {
          fullSelection = true;
        }
      }
      undoStack.add(0, undo);
    }
  }
}
