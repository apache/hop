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

package org.apache.hop.workflow.actions.simpleeval;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.FieldType;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.SuccessBooleanCondition;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.SuccessNumberCondition;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.SuccessStringCondition;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.ValueType;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** This dialog allows you to edit the Simple Eval action settings. */
public class ActionSimpleEvalDialog extends ActionDialog {
  private static final Class<?> PKG = ActionSimpleEval.class;

  private ActionSimpleEval action;

  private boolean changed;

  private Label wlSuccessWhenSet;
  private Button wSuccessWhenSet;

  private Label wlSuccessStringCondition;
  private Label wlFieldType;
  private Label wlMask;
  private Combo wSuccessStringCondition;
  private Combo wValueType;
  private Combo wFieldType;
  private ComboVar wMask;

  private Label wlSuccessNumberCondition;
  private Combo wSuccessNumberCondition;

  private Label wlSuccessBooleanCondition;
  private Combo wSuccessBooleanCondition;

  private Label wlCompareValue;
  private TextVar wCompareValue;

  private Label wlMinValue;
  private TextVar wMinValue;

  private Label wlMaxValue;
  private TextVar wMaxValue;

  private Label wlVariableName;
  private TextVar wVariableName;

  private Label wlFieldName;
  private TextVar wFieldName;

  public ActionSimpleEvalDialog(
      Shell parent, ActionSimpleEval action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionSimpleEval.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = action.hasChanged();

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "ActionSimpleEval.Tab.General.Label"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    // SuccessOngrouping?
    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    Group wSource = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSource);
    wSource.setText(BaseMessages.getString(PKG, "ActionSimpleEval.Source.Group.Label"));
    FormLayout sourcegroupLayout = new FormLayout();
    sourcegroupLayout.marginWidth = 10;
    sourcegroupLayout.marginHeight = 10;
    wSource.setLayout(sourcegroupLayout);

    // Evaluate value (variable ou field from previous result entry)?
    Label wlValueType = new Label(wSource, SWT.RIGHT);
    wlValueType.setText(BaseMessages.getString(PKG, "ActionSimpleEval.ValueType.Label"));
    PropsUi.setLook(wlValueType);
    FormData fdlValueType = new FormData();
    fdlValueType.left = new FormAttachment(0, -margin);
    fdlValueType.right = new FormAttachment(middle, -margin);
    fdlValueType.top = new FormAttachment(0, margin);
    wlValueType.setLayoutData(fdlValueType);
    wValueType = new Combo(wSource, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wValueType.setItems(ValueType.getDescriptions());

    PropsUi.setLook(wValueType);
    FormData fdValueType = new FormData();
    fdValueType.left = new FormAttachment(middle, 0);
    fdValueType.top = new FormAttachment(0, margin);
    fdValueType.right = new FormAttachment(100, 0);
    wValueType.setLayoutData(fdValueType);
    wValueType.addListener(SWT.Selection, e -> refresh());

    // Name of the field to evaluate
    wlFieldName = new Label(wSource, SWT.RIGHT);
    wlFieldName.setText(BaseMessages.getString(PKG, "ActionSimpleEval.FieldName.Label"));
    PropsUi.setLook(wlFieldName);
    FormData fdlFieldName = new FormData();
    fdlFieldName.left = new FormAttachment(0, 0);
    fdlFieldName.top = new FormAttachment(wValueType, margin);
    fdlFieldName.right = new FormAttachment(middle, -margin);
    wlFieldName.setLayoutData(fdlFieldName);

    wFieldName =
        new TextVar(
            variables,
            wSource,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionSimpleEval.FieldName.Tooltip"));
    PropsUi.setLook(wFieldName);
    FormData fdFieldName = new FormData();
    fdFieldName.left = new FormAttachment(middle, 0);
    fdFieldName.top = new FormAttachment(wValueType, margin);
    fdFieldName.right = new FormAttachment(100, -margin);
    wFieldName.setLayoutData(fdFieldName);
    wFieldName.addListener(SWT.Modify, e -> action.setChanged());

    // Name of the variable to evaluate
    wlVariableName = new Label(wSource, SWT.RIGHT);
    wlVariableName.setText(BaseMessages.getString(PKG, "ActionSimpleEval.Variable.Label"));
    PropsUi.setLook(wlVariableName);
    FormData fdlVariableName = new FormData();
    fdlVariableName.left = new FormAttachment(0, 0);
    fdlVariableName.top = new FormAttachment(wValueType, margin);
    fdlVariableName.right = new FormAttachment(middle, -margin);
    wlVariableName.setLayoutData(fdlVariableName);

    wVariableName =
        new TextVar(
            variables,
            wSource,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionSimpleEval.Variable.Tooltip"));
    PropsUi.setLook(wVariableName);
    FormData fdVariableName = new FormData();
    fdVariableName.left = new FormAttachment(middle, 0);
    fdVariableName.top = new FormAttachment(wValueType, margin);
    fdVariableName.right = new FormAttachment(100, -margin);
    wVariableName.setLayoutData(fdVariableName);
    wVariableName.addListener(SWT.Modify, e -> action.setChanged());

    // Field type
    wlFieldType = new Label(wSource, SWT.RIGHT);
    wlFieldType.setText(BaseMessages.getString(PKG, "ActionSimpleEval.FieldType.Label"));
    PropsUi.setLook(wlFieldType);
    FormData fdlFieldType = new FormData();
    fdlFieldType.left = new FormAttachment(0, 0);
    fdlFieldType.right = new FormAttachment(middle, -margin);
    fdlFieldType.top = new FormAttachment(wVariableName, margin);
    wlFieldType.setLayoutData(fdlFieldType);
    wFieldType = new Combo(wSource, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wFieldType.setItems(FieldType.getDescriptions());

    PropsUi.setLook(wFieldType);
    FormData fdFieldType = new FormData();
    fdFieldType.left = new FormAttachment(middle, 0);
    fdFieldType.top = new FormAttachment(wVariableName, margin);
    fdFieldType.right = new FormAttachment(100, 0);
    wFieldType.setLayoutData(fdFieldType);
    wFieldType.addListener(
        SWT.Selection,
        e -> {
          refresh();
          action.setChanged();
        });

    // Mask
    wlMask = new Label(wSource, SWT.RIGHT);
    wlMask.setText(BaseMessages.getString(PKG, "ActionSimpleEval.Mask.Label"));
    PropsUi.setLook(wlMask);
    FormData fdlMask = new FormData();
    fdlMask.left = new FormAttachment(0, 0);
    fdlMask.right = new FormAttachment(middle, -margin);
    fdlMask.top = new FormAttachment(wFieldType, margin);
    wlMask.setLayoutData(fdlMask);

    wMask = new ComboVar(variables, wSource, SWT.BORDER | SWT.READ_ONLY);
    wMask.setItems(Const.getDateFormats());
    wMask.setEditable(true);
    PropsUi.setLook(wMask);
    FormData fdMask = new FormData();
    fdMask.left = new FormAttachment(middle, 0);
    fdMask.top = new FormAttachment(wFieldType, margin);
    fdMask.right = new FormAttachment(100, 0);
    wMask.setLayoutData(fdMask);
    wMask.addListener(SWT.Selection, e -> action.setChanged());

    FormData fdSource = new FormData();
    fdSource.left = new FormAttachment(0, margin);
    fdSource.top = new FormAttachment(wName, margin);
    fdSource.right = new FormAttachment(100, -margin);
    wSource.setLayoutData(fdSource);
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    // SuccessOngrouping?
    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    Group wSuccessOn = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSuccessOn);
    wSuccessOn.setText(BaseMessages.getString(PKG, "ActionSimpleEval.SuccessOn.Group.Label"));

    FormLayout successongroupLayout = new FormLayout();
    successongroupLayout.marginWidth = 10;
    successongroupLayout.marginHeight = 10;

    wSuccessOn.setLayout(successongroupLayout);

    // Success when variable is not set?
    wlSuccessWhenSet = new Label(wSuccessOn, SWT.RIGHT);
    wlSuccessWhenSet.setText(BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenSet.Label"));
    PropsUi.setLook(wlSuccessWhenSet);
    FormData fdlSuccessWhenSet = new FormData();
    fdlSuccessWhenSet.left = new FormAttachment(0, 0);
    fdlSuccessWhenSet.top = new FormAttachment(wVariableName, margin);
    fdlSuccessWhenSet.right = new FormAttachment(middle, -margin);
    wlSuccessWhenSet.setLayoutData(fdlSuccessWhenSet);
    wSuccessWhenSet = new Button(wSuccessOn, SWT.CHECK);
    wSuccessWhenSet.setToolTipText(
        BaseMessages.getString(PKG, "ActionSimpleEval.SuccessWhenSet.Tooltip"));
    PropsUi.setLook(wSuccessWhenSet);
    FormData fdSuccessWhenSet = new FormData();
    fdSuccessWhenSet.left = new FormAttachment(middle, 0);
    fdSuccessWhenSet.top = new FormAttachment(wlSuccessWhenSet, 0, SWT.CENTER);
    fdSuccessWhenSet.right = new FormAttachment(100, 0);
    wSuccessWhenSet.setLayoutData(fdSuccessWhenSet);
    wSuccessWhenSet.addListener(
        SWT.Selection,
        e -> {
          refresh();
          action.setChanged();
        });

    // Success String Condition
    wlSuccessStringCondition = new Label(wSuccessOn, SWT.RIGHT);
    wlSuccessStringCondition.setText(
        BaseMessages.getString(PKG, "ActionSimpleEval.SuccessCondition.Label"));
    PropsUi.setLook(wlSuccessStringCondition);
    FormData fdlSuccessStringCondition = new FormData();
    fdlSuccessStringCondition.left = new FormAttachment(0, 0);
    fdlSuccessStringCondition.right = new FormAttachment(middle, -margin);
    fdlSuccessStringCondition.top = new FormAttachment(wSuccessWhenSet, margin);
    wlSuccessStringCondition.setLayoutData(fdlSuccessStringCondition);

    wSuccessStringCondition = new Combo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wSuccessStringCondition.setItems(SuccessStringCondition.getDescriptions());
    wSuccessStringCondition.select(0); // +1: starts at -1
    PropsUi.setLook(wSuccessStringCondition);
    FormData fdSuccessStringCondition = new FormData();
    fdSuccessStringCondition.left = new FormAttachment(middle, 0);
    fdSuccessStringCondition.top = new FormAttachment(wSuccessWhenSet, margin);
    fdSuccessStringCondition.right = new FormAttachment(100, 0);
    wSuccessStringCondition.setLayoutData(fdSuccessStringCondition);
    wSuccessStringCondition.addListener(
        SWT.Selection,
        e -> {
          refresh();
          action.setChanged();
        });

    // Success Number or Date Condition
    wlSuccessNumberCondition = new Label(wSuccessOn, SWT.RIGHT);
    wlSuccessNumberCondition.setText(
        BaseMessages.getString(PKG, "ActionSimpleEval.SuccessNumberCondition.Label"));
    PropsUi.setLook(wlSuccessNumberCondition);
    FormData fdlSuccessNumberCondition = new FormData();
    fdlSuccessNumberCondition.left = new FormAttachment(0, 0);
    fdlSuccessNumberCondition.right = new FormAttachment(middle, -margin);
    fdlSuccessNumberCondition.top = new FormAttachment(wSuccessWhenSet, margin);
    wlSuccessNumberCondition.setLayoutData(fdlSuccessNumberCondition);

    wSuccessNumberCondition = new Combo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wSuccessNumberCondition.setItems(SuccessNumberCondition.getDescriptions());
    wSuccessNumberCondition.select(0); // +1: starts at -1

    PropsUi.setLook(wSuccessNumberCondition);
    FormData fdSuccessNumberCondition = new FormData();
    fdSuccessNumberCondition.left = new FormAttachment(middle, 0);
    fdSuccessNumberCondition.top = new FormAttachment(wSuccessWhenSet, margin);
    fdSuccessNumberCondition.right = new FormAttachment(100, 0);
    wSuccessNumberCondition.setLayoutData(fdSuccessNumberCondition);
    wSuccessNumberCondition.addListener(
        SWT.Selection,
        e -> {
          refresh();
          action.setChanged();
        });

    // Success Boolean Condition
    wlSuccessBooleanCondition = new Label(wSuccessOn, SWT.RIGHT);
    wlSuccessBooleanCondition.setText(
        BaseMessages.getString(PKG, "ActionSimpleEval.SuccessBooleanCondition.Label"));
    PropsUi.setLook(wlSuccessBooleanCondition);
    FormData fdlSuccessBooleanCondition = new FormData();
    fdlSuccessBooleanCondition.left = new FormAttachment(0, 0);
    fdlSuccessBooleanCondition.right = new FormAttachment(middle, -margin);
    fdlSuccessBooleanCondition.top = new FormAttachment(wSuccessWhenSet, margin);
    wlSuccessBooleanCondition.setLayoutData(fdlSuccessBooleanCondition);

    wSuccessBooleanCondition = new Combo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wSuccessBooleanCondition.setItems(SuccessBooleanCondition.getDescriptions());
    wSuccessBooleanCondition.select(0); // +1: starts at -1

    PropsUi.setLook(wSuccessBooleanCondition);
    FormData fdSuccessBooleanCondition = new FormData();
    fdSuccessBooleanCondition.left = new FormAttachment(middle, 0);
    fdSuccessBooleanCondition.top = new FormAttachment(wSuccessWhenSet, margin);
    fdSuccessBooleanCondition.right = new FormAttachment(100, 0);
    wSuccessBooleanCondition.setLayoutData(fdSuccessBooleanCondition);
    wSuccessBooleanCondition.addListener(
        SWT.Selection,
        e -> {
          refresh();
          action.setChanged();
        });

    // Compare with value
    wlCompareValue = new Label(wSuccessOn, SWT.RIGHT);
    wlCompareValue.setText(BaseMessages.getString(PKG, "ActionSimpleEval.CompareValue.Label"));
    PropsUi.setLook(wlCompareValue);
    FormData fdlCompareValue = new FormData();
    fdlCompareValue.left = new FormAttachment(0, 0);
    fdlCompareValue.top = new FormAttachment(wSuccessNumberCondition, margin);
    fdlCompareValue.right = new FormAttachment(middle, -margin);
    wlCompareValue.setLayoutData(fdlCompareValue);

    wCompareValue =
        new TextVar(
            variables,
            wSuccessOn,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionSimpleEval.CompareValue.Tooltip"));
    PropsUi.setLook(wCompareValue);
    FormData fdCompareValue = new FormData();
    fdCompareValue.left = new FormAttachment(middle, 0);
    fdCompareValue.top = new FormAttachment(wSuccessNumberCondition, margin);
    fdCompareValue.right = new FormAttachment(100, -margin);
    wCompareValue.setLayoutData(fdCompareValue);
    wCompareValue.addListener(SWT.Modify, e -> action.setChanged());

    // Min value
    wlMinValue = new Label(wSuccessOn, SWT.RIGHT);
    wlMinValue.setText(BaseMessages.getString(PKG, "ActionSimpleEval.MinValue.Label"));
    PropsUi.setLook(wlMinValue);
    FormData fdlMinValue = new FormData();
    fdlMinValue.left = new FormAttachment(0, 0);
    fdlMinValue.top = new FormAttachment(wSuccessNumberCondition, margin);
    fdlMinValue.right = new FormAttachment(middle, -margin);
    wlMinValue.setLayoutData(fdlMinValue);

    wMinValue =
        new TextVar(
            variables,
            wSuccessOn,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionSimpleEval.MinValue.Tooltip"));
    PropsUi.setLook(wMinValue);
    FormData fdMinValue = new FormData();
    fdMinValue.left = new FormAttachment(middle, 0);
    fdMinValue.top = new FormAttachment(wSuccessNumberCondition, margin);
    fdMinValue.right = new FormAttachment(100, -margin);
    wMinValue.setLayoutData(fdMinValue);
    wMinValue.addListener(SWT.Modify, e -> action.setChanged());

    // Maximum value
    wlMaxValue = new Label(wSuccessOn, SWT.RIGHT);
    wlMaxValue.setText(BaseMessages.getString(PKG, "ActionSimpleEval.MaxValue.Label"));
    PropsUi.setLook(wlMaxValue);
    FormData fdlMaxValue = new FormData();
    fdlMaxValue.left = new FormAttachment(0, 0);
    fdlMaxValue.top = new FormAttachment(wMinValue, margin);
    fdlMaxValue.right = new FormAttachment(middle, -margin);
    wlMaxValue.setLayoutData(fdlMaxValue);

    wMaxValue =
        new TextVar(
            variables,
            wSuccessOn,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionSimpleEval.MaxValue.Tooltip"));
    PropsUi.setLook(wMaxValue);
    FormData fdMaxValue = new FormData();
    fdMaxValue.left = new FormAttachment(middle, 0);
    fdMaxValue.top = new FormAttachment(wMinValue, margin);
    fdMaxValue.right = new FormAttachment(100, -margin);
    wMaxValue.setLayoutData(fdMaxValue);
    wMaxValue.addListener(SWT.Modify, e -> action.setChanged());

    FormData fdSuccessOn = new FormData();
    fdSuccessOn.left = new FormAttachment(0, margin);
    fdSuccessOn.top = new FormAttachment(wSource, margin);
    fdSuccessOn.right = new FormAttachment(100, -margin);
    wSuccessOn.setLayoutData(fdSuccessOn);
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    PropsUi.setLook(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wCancel, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    getData();
    refresh();

    wTabFolder.setSelection(0);
    focusActionName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }
    wValueType.setText(action.getValueType().getDescription());
    wFieldName.setText(Const.nullToEmpty(action.getFieldName()));
    wVariableName.setText(Const.nullToEmpty(action.getVariableName()));
    wSuccessWhenSet.setSelection(action.isSuccessWhenVarSet());
    wFieldType.setText(action.getFieldType().getDescription());
    wMask.setText(Const.nullToEmpty(action.getMask()));
    wCompareValue.setText(Const.nullToEmpty(action.getCompareValue()));
    wMinValue.setText(Const.nullToEmpty(action.getMinValue()));
    wMaxValue.setText(Const.nullToEmpty(action.getMaxValue()));
    wSuccessStringCondition.setText(action.getSuccessStringCondition().getDescription());
    wSuccessNumberCondition.setText(action.getSuccessNumberCondition().getDescription());
    wSuccessBooleanCondition.setText(action.getSuccessBooleanCondition().getDescription());
  }

  @Override
  protected void onActionNameModified() {
    action.setChanged();
  }

  private void refresh() {
    boolean evaluatepreviousRowField =
        ValueType.lookupDescription(wValueType.getText()) == ValueType.FIELD;
    boolean evaluateVariable =
        ValueType.lookupDescription(wValueType.getText()) == ValueType.VARIABLE;
    wlVariableName.setVisible(evaluateVariable);
    wVariableName.setVisible(evaluateVariable);
    wlFieldName.setVisible(evaluatepreviousRowField);
    wFieldName.setVisible(evaluatepreviousRowField);
    wlSuccessWhenSet.setVisible(evaluateVariable);
    wSuccessWhenSet.setVisible(evaluateVariable);

    boolean successWhenSet = wSuccessWhenSet.getSelection() && evaluateVariable;

    wlFieldType.setVisible(!successWhenSet);
    wFieldType.setVisible(!successWhenSet);

    boolean valueTypeDate =
        FieldType.lookupDescription(wFieldType.getText()) == FieldType.DATE_TIME;
    wlMask.setVisible(!successWhenSet && valueTypeDate);
    wMask.setVisible(!successWhenSet && valueTypeDate);

    boolean valueTypeString = FieldType.lookupDescription(wFieldType.getText()) == FieldType.STRING;
    wlSuccessStringCondition.setVisible(!successWhenSet && valueTypeString);
    wSuccessStringCondition.setVisible(!successWhenSet && valueTypeString);

    boolean valueTypeNumber =
        FieldType.lookupDescription(wFieldType.getText()) == FieldType.NUMBER
            || FieldType.lookupDescription(wFieldType.getText()) == FieldType.DATE_TIME;
    wlSuccessNumberCondition.setVisible(!successWhenSet && valueTypeNumber);
    wSuccessNumberCondition.setVisible(!successWhenSet && valueTypeNumber);

    boolean valueTypeBoolean =
        FieldType.lookupDescription(wFieldType.getText()) == FieldType.BOOLEAN;
    wlSuccessBooleanCondition.setVisible(!successWhenSet && valueTypeBoolean);
    wSuccessBooleanCondition.setVisible(!successWhenSet && valueTypeBoolean);

    boolean compareValue =
        valueTypeString
            || (!valueTypeString
                && SuccessNumberCondition.lookupDescription(wSuccessNumberCondition.getText())
                    != SuccessNumberCondition.BETWEEN);
    wlCompareValue.setVisible(!successWhenSet && compareValue && !valueTypeBoolean);
    wCompareValue.setVisible(!successWhenSet && compareValue && !valueTypeBoolean);
    wlMinValue.setVisible(!successWhenSet && !compareValue && !valueTypeBoolean);
    wMinValue.setVisible(!successWhenSet && !compareValue && !valueTypeBoolean);
    wlMaxValue.setVisible(!successWhenSet && !compareValue && !valueTypeBoolean);
    wMaxValue.setVisible(!successWhenSet && !compareValue && !valueTypeBoolean);
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  private void ok() {

    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setText(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }

    action.setName(wName.getText());
    action.setValueType(ValueType.lookupDescription(wValueType.getText()));
    action.setFieldName(wFieldName.getText());
    action.setVariableName(wVariableName.getText());
    action.setFieldType(FieldType.lookupDescription(wFieldType.getText()));
    action.setMask(wMask.getText());
    action.setCompareValue(wCompareValue.getText());
    action.setMinValue(wMinValue.getText());
    action.setMaxValue(wMaxValue.getText());
    action.setSuccessStringCondition(
        SuccessStringCondition.lookupDescription(wSuccessStringCondition.getText()));
    action.setSuccessNumberCondition(
        SuccessNumberCondition.lookupDescription(wSuccessNumberCondition.getText()));
    action.setSuccessBooleanCondition(
        SuccessBooleanCondition.lookupDescription(wSuccessBooleanCondition.getText()));
    action.setSuccessWhenVarSet(wSuccessWhenSet.getSelection());
    dispose();
  }
}
