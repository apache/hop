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
import org.apache.hop.ui.util.EnvironmentUtils;
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
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;

/**
 * A Widget that combines a Text widget with a Variable button that will insert an Environment
 * variable. The tool tip of the text widget shows the content of the Text widget with expanded
 * variables.
 */
public class TextVar extends Composite {
  protected static Class<?> PKG = TextVar.class;

  protected String toolTipText;

  protected IGetCaretPosition getCaretPositionInterface;

  protected IInsertText insertTextInterface;

  protected ControlSpaceKeyAdapter controlSpaceKeyAdapter;

  protected IVariables variables;

  protected Text wText;

  /** Variable ($) indicator; always present. */
  protected Label wVariableImage;

  /** Optional expanded-integer (#) indicator; created by {@link #enableExpandedInteger()}. */
  protected Label wHashImage;

  /** Optional naming-scheme (N) indicator; created by {@link #enableNamingSchemes(String)}. */
  protected Label wNamingImage;

  protected FormData fdText;

  protected ModifyListener modifyListenerTooltipText;

  /** When false, the $ indicator and CTRL-SPACE variable popup are hidden. */
  protected boolean variablesEnabled = true;

  /** Opt-in naming-scheme type code ({@link NamingSchemeTypes}); null means not a name field. */
  protected String namingSchemeType;

  private boolean namingShortcutAttached;

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

    PropsUi.setLook(this);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 0;
    formLayout.marginHeight = 0;
    formLayout.marginTop = 0;
    formLayout.marginBottom = 0;

    this.setLayout(formLayout);

    // Add the variable $ image on the top right of the control
    //
    wVariableImage = new Label(this, SWT.NONE);
    PropsUi.setLook(wVariableImage);
    wVariableImage.setImage(GuiResource.getInstance().getImageVariableMini());
    wVariableImage.setToolTipText(BaseMessages.getString(PKG, "TextVar.tooltip.InsertVariable"));
    FormData fdlImage = new FormData();
    fdlImage.top = new FormAttachment(0, 0);
    fdlImage.right = new FormAttachment(100, 0);
    wVariableImage.setLayoutData(fdlImage);

    // add a text field on it...
    wText = new Text(this, flags);
    PropsUi.setLook(wText);
    fdText = new FormData();
    fdText.top = new FormAttachment(0, 0);
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(wVariableImage, 0);
    fdText.bottom = new FormAttachment(100, 0);
    wText.setLayoutData(fdText);

    modifyListenerTooltipText = getModifyListenerTooltipText(wText);
    wText.addModifyListener(modifyListenerTooltipText);

    controlSpaceKeyAdapter =
        new ControlSpaceKeyAdapter(
            variables, wText, getCaretPositionInterface, insertTextInterface);
    wText.addKeyListener(controlSpaceKeyAdapter);
    attachNamingShortcut();
  }

  /**
   * Show a mini {@code #} indicator that this field accepts expanded integer notation (grouping
   * separators, k/m/g/b suffixes, scientific forms). Call after construction on fields that use
   * {@link Const#toIntExpanded(String, int)} / {@link Const#toLongExpanded(String, long)}.
   *
   * <p>Layout becomes: {@code [ text ........ ] [#] [$]}. Safe to call more than once.
   *
   * @return this widget for chaining
   */
  public TextVar enableExpandedInteger() {
    if (wHashImage != null || wVariableImage == null || wText == null) {
      return this;
    }

    wHashImage = new Label(this, SWT.NONE);
    PropsUi.setLook(wHashImage);
    wHashImage.setImage(GuiResource.getInstance().getImageHashMini());
    wHashImage.setToolTipText(BaseMessages.getString(PKG, "TextVar.tooltip.ExpandedInteger"));
    FormData fdlHash = new FormData();
    fdlHash.top = new FormAttachment(0, 0);
    wHashImage.setLayoutData(fdlHash);

    relayoutTextField();
    return this;
  }

  /**
   * Enable or disable variable support. When disabled the {@code $} indicator is hidden and
   * CTRL-SPACE no longer opens the variable popup. Naming-scheme shortcuts stay available when
   * {@link #enableNamingSchemes(String)} was called.
   *
   * @param enabled true to keep the default variable behavior
   * @return this widget for chaining
   */
  public TextVar setVariablesEnabled(boolean enabled) {
    this.variablesEnabled = enabled;
    if (wVariableImage != null && !wVariableImage.isDisposed()) {
      wVariableImage.setVisible(enabled);
    }
    if (wText != null && !wText.isDisposed() && controlSpaceKeyAdapter != null) {
      wText.removeKeyListener(controlSpaceKeyAdapter);
      if (enabled) {
        wText.addKeyListener(controlSpaceKeyAdapter);
      }
    }
    relayoutTextField();
    updateNamingTooltip();
    return this;
  }

  public boolean isVariablesEnabled() {
    return variablesEnabled;
  }

  /**
   * Opt this field into naming-scheme shortcuts (CTRL-SHIFT-N, and CTRL-SPACE when variables are
   * disabled). {@code type} is a {@link NamingSchemeTypes} code.
   *
   * @param type scheme type code
   * @return this widget for chaining
   */
  public TextVar enableNamingSchemes(String type) {
    this.namingSchemeType = type;
    attachNamingShortcut();
    if (wNamingImage == null && wText != null && !wText.isDisposed()) {
      wNamingImage = new Label(this, SWT.NONE);
      PropsUi.setLook(wNamingImage);
      wNamingImage.setImage(GuiResource.getInstance().getImageNamingMini());
      wNamingImage.setToolTipText(BaseMessages.getString(PKG, "TextVar.tooltip.NamingScheme"));
      FormData fdlNaming = new FormData();
      fdlNaming.top = new FormAttachment(0, 0);
      wNamingImage.setLayoutData(fdlNaming);
      TextWidgetShortcutKeyAdapter.attachIndicatorClick(wNamingImage, this::buildShortcutContext);
    } else if (wNamingImage != null && !wNamingImage.isDisposed()) {
      wNamingImage.setToolTipText(BaseMessages.getString(PKG, "TextVar.tooltip.NamingScheme"));
    }
    relayoutTextField();
    updateNamingTooltip();
    return this;
  }

  /**
   * Name-field helper: disable variables and enable naming schemes of the given type.
   *
   * @param type scheme type code
   * @return this widget for chaining
   */
  public TextVar asNameField(String type) {
    setVariablesEnabled(false);
    return enableNamingSchemes(type);
  }

  public String getNamingSchemeType() {
    return namingSchemeType;
  }

  protected void attachNamingShortcut() {
    if (namingShortcutAttached || wText == null || wText.isDisposed()) {
      return;
    }
    wText.addKeyListener(new TextWidgetShortcutKeyAdapter(this::buildShortcutContext));
    namingShortcutAttached = true;
  }

  protected TextWidgetShortcutContext buildShortcutContext() {
    return TextWidgetShortcutContext.builder()
        .control(wText)
        .variables(variables)
        .getText(this::getText)
        .setText(this::setText)
        .namingSchemeType(namingSchemeType)
        .variablesEnabled(variablesEnabled)
        .build();
  }

  protected void updateNamingTooltip() {
    if (wText == null || wText.isDisposed() || Utils.isEmpty(namingSchemeType)) {
      return;
    }
    String namingTip = BaseMessages.getString(PKG, "TextVar.tooltip.NamingScheme");
    if (Utils.isEmpty(toolTipText)) {
      setToolTipText(namingTip);
    }
  }

  /**
   * Control that sits to the right of the indicator cluster (browse button on {@link
   * TextVarButton}, otherwise the widget edge).
   */
  protected Control getRightmostFixedControl() {
    return null;
  }

  private void attachIndicator(Control indicator, Control rightNeighbor) {
    FormData fd = (FormData) indicator.getLayoutData();
    if (fd == null) {
      fd = new FormData();
      fd.top = new FormAttachment(0, 0);
    }
    if (rightNeighbor != null) {
      fd.right = new FormAttachment(rightNeighbor, 0);
    } else {
      fd.right = new FormAttachment(100, 0);
    }
    indicator.setLayoutData(fd);
  }

  /**
   * Rebind the text field's right edge to the visible indicator(s). Layout is {@code [ text ] [#]
   * [N] [$]} depending on which extras are enabled.
   */
  protected void relayoutTextField() {
    if (fdText == null || wText == null || wText.isDisposed()) {
      return;
    }
    Control right = getRightmostFixedControl();
    if (variablesEnabled && wVariableImage != null && !wVariableImage.isDisposed()) {
      wVariableImage.setVisible(true);
      attachIndicator(wVariableImage, right);
      right = wVariableImage;
    } else if (wVariableImage != null && !wVariableImage.isDisposed()) {
      wVariableImage.setVisible(false);
    }
    if (wNamingImage != null && !wNamingImage.isDisposed()) {
      attachIndicator(wNamingImage, right);
      right = wNamingImage;
    }
    if (wHashImage != null && !wHashImage.isDisposed()) {
      attachIndicator(wHashImage, right);
      right = wHashImage;
    }
    if (right != null) {
      fdText.right = new FormAttachment(right, 0);
    } else {
      fdText.right = new FormAttachment(100, 0);
    }
    wText.setLayoutData(fdText);
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

  protected ModifyListener getModifyListenerTooltipText(final Text textField) {
    return e -> {
      // Never put PASSWORD field contents in the tooltip: echo char is not always non-\\0 (e.g.
      // some SWT/Cocoa combinations), so rely on style as well as echo char.
      if (textField.getEchoChar() == '\0' && (textField.getStyle() & SWT.PASSWORD) == 0) {

        String tip = textField.getText();
        if (!Utils.isEmpty(tip) && !Utils.isEmpty(toolTipText)) {
          tip += Const.CR + Const.CR + toolTipText;
        }

        if (Utils.isEmpty(tip)) {
          tip = toolTipText;
        }
        if (PropsUi.getInstance().resolveVariablesInToolTips()) {
          textField.setToolTipText(variables.resolve(tip));
        } else {
          textField.setToolTipText(tip);
        }
      }
    };
  }

  /**
   * @return the text in the Text widget
   */
  public String getText() {
    return wText.getText();
  }

  /**
   * @param text the text in the Text widget to set.
   */
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

  @Override
  public void addKeyListener(KeyListener lsKey) {
    wText.addKeyListener(lsKey);
  }

  @Override
  public void addFocusListener(FocusListener lsFocus) {
    wText.addFocusListener(lsFocus);
  }

  public void setEchoChar(char c) {
    wText.setEchoChar(c);
  }

  @Override
  public void setEnabled(boolean flag) {
    wText.setEnabled(flag);
  }

  @Override
  public boolean setFocus() {
    return wText.setFocus();
  }

  public void setMessage(String message) {
    wText.setMessage(message);
  }

  @Override
  public void addTraverseListener(TraverseListener tl) {
    wText.addTraverseListener(tl);
  }

  @Override
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
    if (!EnvironmentUtils.getInstance().isWeb()) {
      wText.showSelection();
    }
  }

  public void setVariables(IVariables vars) {
    variables = vars;
    controlSpaceKeyAdapter.setVariables(variables);
    modifyListenerTooltipText.modifyText(null);
  }
}
