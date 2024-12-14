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
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Text;

public class StyledTextComp extends TextComposite {
  private static final Class<?> PKG = StyledTextComp.class;

  private final Text textWidget;
  private final Menu popupMenu;

  public StyledTextComp(IVariables variables, Composite parent, int style) {
    this(variables, parent, style, true, false);
  }

  public StyledTextComp(IVariables variables, Composite parent, int style, boolean varsSensitive) {
    this(variables, parent, style, varsSensitive, false);
  }

  public StyledTextComp(
      IVariables variables,
      Composite parent,
      int args,
      boolean varsSensitive,
      boolean variableIconOnTop) {

    super(parent, SWT.NONE);
    textWidget = new Text(this, args);
    popupMenu = new Menu(parent.getShell(), SWT.POP_UP);

    this.setLayout(new FormLayout());

    buildingStyledTextMenu(popupMenu);

    // Default layout without variables
    textWidget.setLayoutData(
        new FormDataBuilder().top().left().right(100, 0).bottom(100, 0).result());

    // Special layout for variables decorator
    if (varsSensitive) {
      textWidget.addKeyListener(new ControlSpaceKeyAdapter(variables, textWidget));

      if (variableIconOnTop) {
        final Label wIcon = new Label(this, SWT.RIGHT);
        PropsUi.setLook(wIcon);
        wIcon.setToolTipText(BaseMessages.getString(PKG, "StyledTextComp.tooltip.InsertVariable"));
        wIcon.setImage(GuiResource.getInstance().getImageVariableMini());
        wIcon.setLayoutData(new FormDataBuilder().top().right(100, 0).result());
        textWidget.setLayoutData(
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
        textWidget.setLayoutData(
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
    return textWidget.getSelectionText();
  }

  @Override
  public int getCaretPosition() {
    return textWidget.getCaretPosition();
  }

  @Override
  public void setCaretPosition(int offset) {
    // Does not exist in Text widget
  }

  @Override
  public int getCharCount() {
    return textWidget.getCharCount();
  }

  @Override
  public String getText() {
    return textWidget.getText();
  }

  @Override
  public void setText(String text) {
    textWidget.setText(text);
  }

  @Override
  public void insert(String strInsert) {
    textWidget.insert(strInsert);
  }

  @Override
  public void addListener(int eventType, Listener listener) {
    textWidget.addListener(eventType, listener);
  }

  @Override
  public void addModifyListener(ModifyListener lsMod) {
    textWidget.addModifyListener(lsMod);
  }

  @Override
  public void addLineStyleListener() {
    // No listener required
  }

  @Override
  public void addLineStyleListener(List<String> sqlKeywords) {
    // No listener required
  }

  public void addKeyListener(KeyAdapter keyAdapter) {
    textWidget.addKeyListener(keyAdapter);
  }

  public void addFocusListener(FocusAdapter focusAdapter) {
    textWidget.addFocusListener(focusAdapter);
  }

  public void addMouseListener(MouseAdapter mouseAdapter) {
    textWidget.addMouseListener(mouseAdapter);
  }

  @Override
  public void addMenuDetectListener(MenuDetectListener listener) {
    textWidget.addMenuDetectListener(listener);
  }

  @Override
  public int getSelectionCount() {
    return textWidget.getSelectionCount();
  }

  @Override
  public void setSelection(int start) {
    textWidget.setSelection(start);
  }

  @Override
  public void setSelection(int start, int end) {
    textWidget.setSelection(start, end);
  }

  @Override
  public boolean setFocus() {
    return textWidget.setFocus();
  }

  @Override
  public void setBackground(Color color) {
    super.setBackground(color);
    textWidget.setBackground(color);
  }

  @Override
  public void setForeground(Color color) {
    super.setForeground(color);
    textWidget.setForeground(color);
  }

  @Override
  public void setFont(Font fnt) {
    textWidget.setFont(fnt);
  }

  public Text getTextWidget() {
    return textWidget;
  }

  @Override
  public boolean isEditable() {
    return textWidget.getEditable();
  }

  @Override
  public void setEditable(boolean editable) {
    textWidget.setEditable(editable);
  }

  @Override
  public void setEnabled(boolean enabled) {
    textWidget.setEnabled(enabled);
  }

  @Override
  public void cut() {
    textWidget.cut();
  }

  @Override
  public void copy() {
    textWidget.copy();
  }

  @Override
  public void paste() {
    textWidget.paste();
  }

  @Override
  public void selectAll() {
    textWidget.selectAll();
  }

  @Override
  public void setMenu(Menu menu) {
    textWidget.setMenu(menu);
  }
}
