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
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.TraverseListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;

/**
 * A Widget that combines a Text widget with a Variable button that will insert an Environment
 * variable. The tool tip of the text widget shows the content of the Text widget with expanded
 * variables.
 */
public class ComboVar extends Composite {
  private static final Class<?> PKG = ComboVar.class;

  private String toolTipText;

  private IGetCaretPosition getCaretPositionInterface;

  private IInsertText insertTextInterface;

  private ControlSpaceKeyAdapter controlSpaceKeyAdapter;

  private IVariables variables;

  private CCombo wCombo;
  private Label wlImage;
  private Label wNamingImage;
  private FormData fdCombo;

  private ModifyListener modifyListenerTooltipText;

  private boolean variablesEnabled = true;
  private String namingSchemeType;
  private boolean namingShortcutAttached;

  public ComboVar(IVariables variables, Composite composite, int flags) {
    this(variables, composite, flags, null, null, null);
  }

  public ComboVar(IVariables variables, Composite composite, int flags, String toolTipText) {
    this(variables, composite, flags, toolTipText, null, null);
  }

  public ComboVar(
      IVariables variables,
      Composite composite,
      int flags,
      IGetCaretPosition getCaretPositionInterface,
      IInsertText insertTextInterface) {
    this(variables, composite, flags, null, getCaretPositionInterface, insertTextInterface);
  }

  public ComboVar(
      IVariables variables,
      Composite composite,
      int flags,
      String toolTipText,
      IGetCaretPosition getCaretPositionInterface,
      IInsertText insertTextInterface) {
    super(composite, SWT.NONE);
    this.toolTipText = toolTipText;
    this.getCaretPositionInterface = getCaretPositionInterface;
    this.insertTextInterface = insertTextInterface;
    this.variables = variables;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 0;
    formLayout.marginHeight = 0;
    formLayout.marginTop = 0;
    formLayout.marginBottom = 0;

    this.setLayout(formLayout);

    // Add the variable $ image on the top right of the control
    //
    wlImage = new Label(this, SWT.NONE);
    wlImage.setImage(GuiResource.getInstance().getImageVariableMini());
    wlImage.setToolTipText(BaseMessages.getString(PKG, "TextVar.tooltip.InsertVariable"));
    FormData fdlImage = new FormData();
    fdlImage.top = new FormAttachment(0, 0);
    fdlImage.right = new FormAttachment(100, 0);
    wlImage.setLayoutData(fdlImage);

    // add a text field on it...
    wCombo = new CCombo(this, flags);
    PropsUi.setLook(wCombo);
    modifyListenerTooltipText = getModifyListenerTooltipText(wCombo);
    wCombo.addModifyListener(modifyListenerTooltipText);
    fdCombo = new FormData();
    fdCombo.top = new FormAttachment(0, 0);
    fdCombo.left = new FormAttachment(0, 0);
    fdCombo.right = new FormAttachment(wlImage, 0);
    wCombo.setLayoutData(fdCombo);

    controlSpaceKeyAdapter =
        new ControlSpaceKeyAdapter(
            variables, wCombo, getCaretPositionInterface, insertTextInterface);
    wCombo.addKeyListener(controlSpaceKeyAdapter);
    attachNamingShortcut();
  }

  /**
   * Enable or disable variable support. When disabled the {@code $} indicator is hidden and
   * CTRL-SPACE no longer opens the variable popup.
   *
   * @param enabled true to keep the default variable behavior
   * @return this widget for chaining
   */
  public ComboVar setVariablesEnabled(boolean enabled) {
    this.variablesEnabled = enabled;
    if (wlImage != null && !wlImage.isDisposed()) {
      wlImage.setVisible(enabled);
    }
    if (wCombo != null && !wCombo.isDisposed() && controlSpaceKeyAdapter != null) {
      wCombo.removeKeyListener(controlSpaceKeyAdapter);
      if (enabled) {
        wCombo.addKeyListener(controlSpaceKeyAdapter);
      }
    }
    relayoutCombo();
    return this;
  }

  public boolean isVariablesEnabled() {
    return variablesEnabled;
  }

  /**
   * Opt this field into naming-scheme shortcuts. {@code type} is a {@link NamingSchemeTypes} code.
   *
   * @param type scheme type code
   * @return this widget for chaining
   */
  public ComboVar enableNamingSchemes(String type) {
    this.namingSchemeType = type;
    attachNamingShortcut();
    if (wNamingImage == null && wCombo != null && !wCombo.isDisposed()) {
      wNamingImage = new Label(this, SWT.NONE);
      PropsUi.setLook(wNamingImage);
      wNamingImage.setImage(GuiResource.getInstance().getImageNamingMini());
      wNamingImage.setToolTipText(BaseMessages.getString(PKG, "TextVar.tooltip.NamingScheme"));
      FormData fdlNaming = new FormData();
      fdlNaming.top = new FormAttachment(0, 0);
      wNamingImage.setLayoutData(fdlNaming);
      TextWidgetShortcutKeyAdapter.attachIndicatorClick(
          wNamingImage,
          () ->
              TextWidgetShortcutContext.builder()
                  .control(wCombo)
                  .variables(variables)
                  .getText(this::getText)
                  .setText(this::setText)
                  .namingSchemeType(namingSchemeType)
                  .variablesEnabled(variablesEnabled)
                  .build());
    }
    relayoutCombo();
    return this;
  }

  public String getNamingSchemeType() {
    return namingSchemeType;
  }

  private void attachNamingShortcut() {
    if (namingShortcutAttached || wCombo == null || wCombo.isDisposed()) {
      return;
    }
    wCombo.addKeyListener(
        new TextWidgetShortcutKeyAdapter(
            () ->
                TextWidgetShortcutContext.builder()
                    .control(wCombo)
                    .variables(variables)
                    .getText(this::getText)
                    .setText(this::setText)
                    .namingSchemeType(namingSchemeType)
                    .variablesEnabled(variablesEnabled)
                    .build()));
    namingShortcutAttached = true;
  }

  private void relayoutCombo() {
    if (fdCombo == null || wCombo == null || wCombo.isDisposed()) {
      return;
    }
    Control right = null;
    if (variablesEnabled && wlImage != null && !wlImage.isDisposed()) {
      wlImage.setVisible(true);
      FormData fdVar = (FormData) wlImage.getLayoutData();
      if (fdVar != null) {
        fdVar.right = new FormAttachment(100, 0);
        wlImage.setLayoutData(fdVar);
      }
      right = wlImage;
    } else if (wlImage != null && !wlImage.isDisposed()) {
      wlImage.setVisible(false);
    }
    if (wNamingImage != null && !wNamingImage.isDisposed()) {
      FormData fdNaming = (FormData) wNamingImage.getLayoutData();
      if (fdNaming == null) {
        fdNaming = new FormData();
        fdNaming.top = new FormAttachment(0, 0);
      }
      if (right != null) {
        fdNaming.right = new FormAttachment(right, 0);
      } else {
        fdNaming.right = new FormAttachment(100, 0);
      }
      wNamingImage.setLayoutData(fdNaming);
      right = wNamingImage;
    }
    if (right != null) {
      fdCombo.right = new FormAttachment(right, 0);
    } else {
      fdCombo.right = new FormAttachment(100, 0);
    }
    wCombo.setLayoutData(fdCombo);
    layout(true, true);
  }

  /**
   * @return the getCaretPositionInterface
   */
  public IGetCaretPosition getGetCaretPositionInterface() {
    return getCaretPositionInterface;
  }

  /**
   * @param getCaretPositionInterface the getCaretPositionInterface to set
   */
  public void setGetCaretPositionInterface(IGetCaretPosition getCaretPositionInterface) {
    this.getCaretPositionInterface = getCaretPositionInterface;
  }

  /**
   * @return the insertTextInterface
   */
  public IInsertText getInsertTextInterface() {
    return insertTextInterface;
  }

  /**
   * @param insertTextInterface the insertTextInterface to set
   */
  public void setInsertTextInterface(IInsertText insertTextInterface) {
    this.insertTextInterface = insertTextInterface;
  }

  private ModifyListener getModifyListenerTooltipText(final CCombo comboField) {
    return e -> {
      String tip = comboField.getText();
      if (!Utils.isEmpty(tip) && !Utils.isEmpty(toolTipText)) {
        tip += Const.CR + Const.CR + toolTipText;
      }

      if (Utils.isEmpty(tip)) {
        tip = toolTipText;
      }
      comboField.setToolTipText(variables.resolve(tip));
    };
  }

  /**
   * @return the text in the Text widget
   */
  public String getText() {
    return wCombo.getText();
  }

  /**
   * @param text the text in the Text widget to set.
   */
  public void setText(String text) {
    wCombo.setText(text);
    modifyListenerTooltipText.modifyText(null);
  }

  public CCombo getCComboWidget() {
    return wCombo;
  }

  @Override
  public void addListener(int eventType, Listener listener) {
    wCombo.addListener(eventType, listener);
  }

  /**
   * Add a modify listener to the text widget
   *
   * @param modifyListener
   */
  public void addModifyListener(ModifyListener modifyListener) {
    wCombo.addModifyListener(modifyListener);
  }

  public void addSelectionListener(SelectionListener lsDef) {
    wCombo.addSelectionListener(lsDef);
  }

  @Override
  public void addKeyListener(KeyListener lsKey) {
    wCombo.addKeyListener(lsKey);
  }

  @Override
  public void addFocusListener(FocusListener lsFocus) {
    wCombo.addFocusListener(lsFocus);
  }

  @Override
  public void setEnabled(boolean flag) {
    wCombo.setEnabled(flag);
  }

  @Override
  public synchronized boolean setFocus() {
    if (wCombo != null && !wCombo.isDisposed()) {
      synchronized (wCombo) {
        if (!wCombo.isEnabled()
            || !wCombo.getVisible()
            || !wCombo.isVisible()
            || !wCombo.isFocusControl()) {
          return false;
        }
        return wCombo.setFocus();
      }
    } else {
      return false;
    }
  }

  @Override
  public void dispose() {
    if (wCombo != null && !wCombo.isDisposed()) {
      wCombo.dispose();
    }
    if (wlImage != null && !wlImage.isDisposed()) {
      wlImage.dispose();
    }
    if (wNamingImage != null && !wNamingImage.isDisposed()) {
      wNamingImage.dispose();
    }
    super.dispose();
  }

  @Override
  public void addTraverseListener(TraverseListener tl) {
    wCombo.addTraverseListener(tl);
  }

  @Override
  public void setToolTipText(String toolTipText) {
    this.toolTipText = toolTipText;
    wCombo.setToolTipText(toolTipText);
    modifyListenerTooltipText.modifyText(null);
  }

  public void setEditable(boolean editable) {
    wCombo.setEditable(editable);
  }

  public void setVariables(IVariables vars) {
    variables = vars;
    controlSpaceKeyAdapter.setVariables(variables);
    modifyListenerTooltipText.modifyText(null);
  }

  public void setItems(String[] items) {
    wCombo.setItems(items);
  }

  public String[] getItems() {
    return wCombo.getItems();
  }

  public void add(String item) {
    wCombo.add(item);
  }

  public int getItemCount() {
    return wCombo.getItemCount();
  }

  public int getSelectionIndex() {
    return wCombo.getSelectionIndex();
  }

  public void removeAll() {
    wCombo.removeAll();
  }

  public void remove(int index) {
    wCombo.remove(index);
  }

  public void select(int index) {
    wCombo.select(index);
    modifyListenerTooltipText.modifyText(null);
  }
}
