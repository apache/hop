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

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.TraverseListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;

/**
 * A Widget that combines a Text widget with a Variable button that will insert an Environment
 * variable. The tool tip of the text widget shows the content of the Text widget with expanded
 * variables.
 *
 * @author Matt
 * @since 17-may-2006
 */
public class TextVar extends Composite {
  protected static Class<?> PKG = TextVar.class; // For Translator

  protected String toolTipText;

  protected IGetCaretPosition getCaretPositionInterface;

  protected IInsertText insertTextInterface;

  protected ControlSpaceKeyAdapter controlSpaceKeyAdapter;

  protected IVariables variables;

  protected Text wText;

  protected ModifyListener modifyListenerTooltipText;

  public TextVar(IVariables variables, Composite composite, int flags) {
    this(variables, composite, flags, null, null, null);
  }

  public TextVar(IVariables variables, Composite composite, int flags, String toolTipText) {
    this(variables, composite, flags, toolTipText, null, null);
  }

  public TextVar(
      IVariables variables,
      Composite composite,
      int flags,
      IGetCaretPosition getCaretPositionInterface,
      IInsertText insertTextInterface) {
    this(variables, composite, flags, null, getCaretPositionInterface, insertTextInterface);
  }

  public TextVar(
      IVariables variables,
      Composite composite,
      int flags,
      String toolTipText,
      IGetCaretPosition getCaretPositionInterface,
      IInsertText insertTextInterface) {
    super(composite, SWT.NONE);
    initialize(
        variables,
        composite,
        flags,
        toolTipText,
        getCaretPositionInterface,
        insertTextInterface,
        null);
  }

  public TextVar(
      Composite composite,
      IVariables variables,
      int flags,
      IGetCaretPosition getCaretPositionInterface,
      IInsertText insertTextInterface,
      SelectionListener selectionListener) {
    this(
        variables,
        composite,
        flags,
        null,
        getCaretPositionInterface,
        insertTextInterface,
        selectionListener);
  }

  public TextVar(
      IVariables variables,
      Composite composite,
      int flags,
      String toolTipText,
      IGetCaretPosition getCaretPositionInterface,
      IInsertText insertTextInterface,
      SelectionListener selectionListener) {
    super(composite, SWT.NONE);
    initialize(
        variables,
        composite,
        flags,
        toolTipText,
        getCaretPositionInterface,
        insertTextInterface,
        selectionListener);
  }

  protected void initialize(
      IVariables variables,
      Composite composite,
      int flags,
      String toolTipText,
      IGetCaretPosition getCaretPositionInterface,
      IInsertText insertTextInterface,
      SelectionListener selectionListener) {

    this.toolTipText = toolTipText;
    this.getCaretPositionInterface = getCaretPositionInterface;
    this.insertTextInterface = insertTextInterface;
    this.variables = variables;

    PropsUi.getInstance().setLook(this);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 0;
    formLayout.marginHeight = 0;
    formLayout.marginTop = 0;
    formLayout.marginBottom = 0;

    this.setLayout(formLayout);

    // Add the variable $ image on the top right of the control
    //
    Label wlImage = new Label(this, SWT.NONE);
    wlImage.setImage(GuiResource.getInstance().getImageVariable());
    wlImage.setToolTipText(BaseMessages.getString(PKG, "TextVar.tooltip.InsertVariable"));
    FormData fdlImage = new FormData();
    fdlImage.top = new FormAttachment(0, 0);
    fdlImage.right = new FormAttachment(100, 0);
    wlImage.setLayoutData(fdlImage);

    // add a text field on it...
    wText = new Text(this, flags);
    FormData fdText = new FormData();
    fdText.top = new FormAttachment(0, 0);
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(wlImage, 0);
    fdText.bottom = new FormAttachment(100, 0);
    wText.setLayoutData(fdText);

    modifyListenerTooltipText = getModifyListenerTooltipText(wText);
    wText.addModifyListener(modifyListenerTooltipText);

    controlSpaceKeyAdapter =
        new ControlSpaceKeyAdapter(
            variables, wText, getCaretPositionInterface, insertTextInterface);
    wText.addKeyListener(controlSpaceKeyAdapter);
  }

  /** @return the getCaretPositionInterface */
  public IGetCaretPosition getGetCaretPositionInterface() {
    return getCaretPositionInterface;
  }

  /** @param getCaretPositionInterface the getCaretPositionInterface to set */
  public void setGetCaretPositionInterface(IGetCaretPosition getCaretPositionInterface) {
    this.getCaretPositionInterface = getCaretPositionInterface;
  }

  /** @return the insertTextInterface */
  public IInsertText getInsertTextInterface() {
    return insertTextInterface;
  }

  /** @param insertTextInterface the insertTextInterface to set */
  public void setInsertTextInterface(IInsertText insertTextInterface) {
    this.insertTextInterface = insertTextInterface;
  }

  protected ModifyListener getModifyListenerTooltipText(final Text textField) {
    return e -> {
      if (textField.getEchoChar() == '\0') { // Can't show passwords ;-)

        String tip = textField.getText();
        if (!Utils.isEmpty(tip) && !Utils.isEmpty(toolTipText)) {
          tip += Const.CR + Const.CR + toolTipText;
        }

        if (Utils.isEmpty(tip)) {
          tip = toolTipText;
        }
        textField.setToolTipText(variables.resolve(tip));
      }
    };
  }

  /** @return the text in the Text widget */
  public String getText() {
    return wText.getText();
  }

  /** @param text the text in the Text widget to set. */
  public void setText(String text) {
    wText.setText(text);
    modifyListenerTooltipText.modifyText(null);
  }

  public Text getTextWidget() {
    return wText;
  }

  @Override
  public void addListener(int eventType, Listener listener) {
    wText.addListener(eventType, listener);
  }

  /**
   * Add a modify listener to the text widget
   *
   * @param modifyListener
   */
  public void addModifyListener(ModifyListener modifyListener) {
    wText.addModifyListener(modifyListener);
  }

  public void addSelectionListener(SelectionAdapter lsDef) {
    wText.addSelectionListener(lsDef);
  }

  public void addKeyListener(KeyListener lsKey) {
    wText.addKeyListener(lsKey);
  }

  public void addFocusListener(FocusListener lsFocus) {
    wText.addFocusListener(lsFocus);
  }

  public void setEchoChar(char c) {
    wText.setEchoChar(c);
  }

  public void setEnabled(boolean flag) {
    wText.setEnabled(flag);
  }

  public boolean setFocus() {
    return wText.setFocus();
  }

  public void addTraverseListener(TraverseListener tl) {
    wText.addTraverseListener(tl);
  }

  public void setToolTipText(String toolTipText) {
    this.toolTipText = toolTipText;
    wText.setToolTipText(toolTipText);
    modifyListenerTooltipText.modifyText(null);
  }

  public void setEditable(boolean editable) {
    wText.setEditable(editable);
  }

  public void setSelection(int i) {
    wText.setSelection(i);
  }

  public void selectAll() {
    wText.selectAll();
  }

  public void showSelection() {
    wText.showSelection();
  }

  public void setVariables(IVariables vars) {
    variables = vars;
    controlSpaceKeyAdapter.setVariables(variables);
    modifyListenerTooltipText.modifyText(null);
  }
}
