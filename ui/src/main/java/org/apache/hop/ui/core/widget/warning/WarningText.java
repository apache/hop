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

package org.apache.hop.ui.core.widget.warning;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.CheckBoxVar;
import org.eclipse.jface.fieldassist.ControlDecoration;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.TraverseListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * A Widget that combines a Text widget with a "Warning" image to the left. It's shown when there is
 * a warning condition in the text field.
 *
 * @author Matt
 * @since 25-FEB-2009
 */
public class WarningText extends Composite implements ISupportsWarning {
  private static final Class<?> PKG = CheckBoxVar.class; // For Translator

  private ControlDecoration warningControlDecoration;

  private Text wText;

  private List<IWarning> warningInterfaces;

  public WarningText(Composite composite, int flags) {
    super(composite, SWT.NONE);

    warningInterfaces = new ArrayList<>();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 0;
    formLayout.marginHeight = 0;
    formLayout.marginTop = 0;
    formLayout.marginBottom = 0;

    this.setLayout(formLayout);

    // add a text field on it...
    wText = new Text(this, flags);

    warningControlDecoration = new ControlDecoration(wText, SWT.CENTER | SWT.RIGHT);
    Image warningImage = GuiResource.getInstance().getImageWarning();
    warningControlDecoration.setImage(warningImage);
    warningControlDecoration.setDescriptionText(
        BaseMessages.getString(PKG, "TextVar.tooltip.FieldIsInUse"));
    warningControlDecoration.hide();

    // If something has changed, check the warning interfaces
    //
    wText.addModifyListener(
        new ModifyListener() {
          public void modifyText(ModifyEvent arg0) {

            // Verify all the warning interfaces.
            // Show the first that has a warning to show...
            //
            boolean foundOne = false;
            for (IWarning warningInterface : warningInterfaces) {
              IWarningMessage warningSituation =
                  warningInterface.getWarningSituation(wText.getText(), wText, this);
              if (warningSituation.isWarning()) {
                foundOne = true;
                warningControlDecoration.show();
                warningControlDecoration.setDescriptionText(warningSituation.getWarningMessage());
                break;
              }
            }
            if (!foundOne) {
              warningControlDecoration.hide();
            }
          }
        });

    FormData fdText = new FormData();
    fdText.top = new FormAttachment(0, 0);
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(100, -warningImage.getBounds().width);
    wText.setLayoutData(fdText);
  }

  /** @return the text in the Text widget */
  public String getText() {
    return wText.getText();
  }

  /** @param text the text in the Text widget to set. */
  public void setText(String text) {
    wText.setText(text);
  }

  public Text getTextWidget() {
    return wText;
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

  public void addWarning(IWarning warningInterface) {
    warningInterfaces.add(warningInterface);
  }

  public void removeWarningInterface(IWarning warningInterface) {
    warningInterfaces.remove(warningInterface);
  }

  public List<IWarning> getWarningInterfaces() {
    return warningInterfaces;
  }
}
