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

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.TraverseListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;

/**
 * A Widget that combines a Check Box widget with a Variable button that will insert an Environment
 * variable.
 *
 * @author Matt
 * @since 9-august-2006
 */
public class CheckBoxVar extends Composite {
  private static final Class<?> PKG = CheckBoxVar.class; // For Translator

  private static final PropsUi props = PropsUi.getInstance();

  private Button wBox;

  private TextVar wText;

  public CheckBoxVar(IVariables variables, Composite composite, int flags) {
    this(variables, composite, flags, null);
  }

  public CheckBoxVar(
      final IVariables variables, final Composite composite, int flags, String variable) {
    super(composite, SWT.NONE);

    props.setLook(this);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 0;
    formLayout.marginHeight = 0;
    formLayout.marginTop = 0;
    formLayout.marginBottom = 0;

    this.setLayout(formLayout);

    // add a text field on it...
    wBox = new Button(this, flags);
    props.setLook(wBox);
    wText = new TextVar(variables, this, flags | SWT.NO_BACKGROUND);
    wText
        .getTextWidget()
        .setForeground(GuiResource.getInstance().getColorRed()); // Put it in a red color to make it
    // shine...
    wText
        .getTextWidget()
        .setBackground(composite.getBackground()); // make it blend in with the rest...
    wText.setText(Const.NVL(variable, ""));

    FormData fdBox = new FormData();
    fdBox.top = new FormAttachment(wText, 0, SWT.CENTER);
    fdBox.left = new FormAttachment(0, 0);
    wBox.setLayoutData(fdBox);

    FormData fdText = new FormData();
    fdText.top = new FormAttachment(0, 0);
    fdText.left = new FormAttachment(wBox, props.getMargin());
    fdText.right = new FormAttachment(100, 0);
    wText.setLayoutData(fdText);
  }

  /** @return the text in the Text widget */
  public String getText() {
    return wBox.getText();
  }

  /** @param text the text in the Text widget to set. */
  public void setText(String text) {
    wBox.setText(text);
  }

  public void addSelectionListener(SelectionAdapter lsDef) {
    wBox.addSelectionListener(lsDef);
  }

  public void addKeyListener(KeyListener lsKey) {
    wBox.addKeyListener(lsKey);
  }

  public void addFocusListener(FocusListener lsFocus) {
    wBox.addFocusListener(lsFocus);
  }

  public void setEnabled(boolean flag) {
    wBox.setEnabled(flag);
  }

  public void setSelection(boolean selection) {
    wBox.setSelection(selection);
  }

  public boolean getSelection() {
    return wBox.getSelection();
  }

  public boolean setFocus() {
    return wBox.setFocus();
  }

  public void addTraverseListener(TraverseListener tl) {
    wBox.addTraverseListener(tl);
  }

  public String getVariableName() {
    return wText.getText();
  }

  public void setVariableName(String variableName) {
    if (variableName != null) {
      wText.setText(variableName);
    } else {
      wText.setText("");
    }
  }

  public TextVar getTextVar() {
    return wText;
  }
}
