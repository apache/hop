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

package org.apache.hop.workflow.actions.simpleeval;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

/**
 * This dialog allows you to edit the XML valid action settings.
 *
 * @author Samatar Hassan
 * @since 01-01-2000
 */

public class ActionSimpleEvalDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionSimpleEval.class; // For Translator

  private Text wName;

  private ActionSimpleEval action;
  private Shell shell;

  private boolean changed;

  private Label wlsuccessWhenSet;
  private Button wSuccessWhenSet;

  private Label wlSuccessCondition;
  private Label wlFieldType;
  private Label wlMask;
  private CCombo wSuccessCondition, wValueType, wFieldType;
  private ComboVar wMask;

  private Label wlSuccessNumberCondition;
  private CCombo wSuccessNumberCondition;

  private Label wlSuccessBooleanCondition;
  private CCombo wSuccessBooleanCondition;

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

  public ActionSimpleEvalDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionSimpleEval) action;
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobSimpleEval.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobSimpleEval.Name.Label" ) );
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData(fdlName);
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData(fdName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobSimpleEval.Tab.General.Label" ) );

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // SuccessOngrouping?
    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    Group wSource = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wSource);
    wSource.setText( BaseMessages.getString( PKG, "JobSimpleEval.Source.Group.Label" ) );
    FormLayout sourcegroupLayout = new FormLayout();
    sourcegroupLayout.marginWidth = 10;
    sourcegroupLayout.marginHeight = 10;
    wSource.setLayout( sourcegroupLayout );

    // Evaluate value (variable ou field from previous result entry)?
    Label wlValueType = new Label(wSource, SWT.RIGHT);
    wlValueType.setText( BaseMessages.getString( PKG, "JobSimpleEval.ValueType.Label" ) );
    props.setLook(wlValueType);
    FormData fdlValueType = new FormData();
    fdlValueType.left = new FormAttachment( 0, -margin );
    fdlValueType.right = new FormAttachment( middle, -margin );
    fdlValueType.top = new FormAttachment( 0, margin );
    wlValueType.setLayoutData( fdlValueType );
    wValueType = new CCombo(wSource, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wValueType.setItems( ActionSimpleEval.valueTypeDesc );

    props.setLook( wValueType );
    FormData fdValueType = new FormData();
    fdValueType.left = new FormAttachment( middle, 0 );
    fdValueType.top = new FormAttachment( 0, margin );
    fdValueType.right = new FormAttachment( 100, 0 );
    wValueType.setLayoutData( fdValueType );
    wValueType.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

        refresh();

      }
    } );

    // Name of the field to evaluate
    wlFieldName = new Label(wSource, SWT.RIGHT );
    wlFieldName.setText( BaseMessages.getString( PKG, "JobSimpleEval.FieldName.Label" ) );
    props.setLook( wlFieldName );
    FormData fdlFieldName = new FormData();
    fdlFieldName.left = new FormAttachment( 0, 0 );
    fdlFieldName.top = new FormAttachment( wValueType, margin );
    fdlFieldName.right = new FormAttachment( middle, -margin );
    wlFieldName.setLayoutData(fdlFieldName);

    wFieldName =
      new TextVar( variables, wSource, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobSimpleEval.FieldName.Tooltip" ) );
    props.setLook( wFieldName );
    wFieldName.addModifyListener( lsMod );
    FormData fdFieldName = new FormData();
    fdFieldName.left = new FormAttachment( middle, 0 );
    fdFieldName.top = new FormAttachment( wValueType, margin );
    fdFieldName.right = new FormAttachment( 100, -margin );
    wFieldName.setLayoutData(fdFieldName);

    // Name of the variable to evaluate
    wlVariableName = new Label(wSource, SWT.RIGHT );
    wlVariableName.setText( BaseMessages.getString( PKG, "JobSimpleEval.Variable.Label" ) );
    props.setLook( wlVariableName );
    FormData fdlVariableName = new FormData();
    fdlVariableName.left = new FormAttachment( 0, 0 );
    fdlVariableName.top = new FormAttachment( wValueType, margin );
    fdlVariableName.right = new FormAttachment( middle, -margin );
    wlVariableName.setLayoutData(fdlVariableName);

    wVariableName =
      new TextVar( variables, wSource, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobSimpleEval.Variable.Tooltip" ) );
    props.setLook( wVariableName );
    wVariableName.addModifyListener( lsMod );
    FormData fdVariableName = new FormData();
    fdVariableName.left = new FormAttachment( middle, 0 );
    fdVariableName.top = new FormAttachment( wValueType, margin );
    fdVariableName.right = new FormAttachment( 100, -margin );
    wVariableName.setLayoutData(fdVariableName);

    // Field type
    wlFieldType = new Label(wSource, SWT.RIGHT );
    wlFieldType.setText( BaseMessages.getString( PKG, "JobSimpleEval.FieldType.Label" ) );
    props.setLook( wlFieldType );
    FormData fdlFieldType = new FormData();
    fdlFieldType.left = new FormAttachment( 0, 0 );
    fdlFieldType.right = new FormAttachment( middle, -margin );
    fdlFieldType.top = new FormAttachment( wVariableName, margin );
    wlFieldType.setLayoutData( fdlFieldType );
    wFieldType = new CCombo(wSource, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wFieldType.setItems( ActionSimpleEval.fieldTypeDesc );

    props.setLook( wFieldType );
    FormData fdFieldType = new FormData();
    fdFieldType.left = new FormAttachment( middle, 0 );
    fdFieldType.top = new FormAttachment( wVariableName, margin );
    fdFieldType.right = new FormAttachment( 100, 0 );
    wFieldType.setLayoutData( fdFieldType );
    wFieldType.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

        refresh();

      }
    } );

    // Mask
    wlMask = new Label(wSource, SWT.RIGHT );
    wlMask.setText( BaseMessages.getString( PKG, "JobSimpleEval.Mask.Label" ) );
    props.setLook( wlMask );
    FormData fdlMask = new FormData();
    fdlMask.left = new FormAttachment( 0, 0 );
    fdlMask.right = new FormAttachment( middle, -margin );
    fdlMask.top = new FormAttachment( wFieldType, margin );
    wlMask.setLayoutData( fdlMask );

    wMask = new ComboVar( variables, wSource, SWT.BORDER | SWT.READ_ONLY );
    wMask.setItems( Const.getDateFormats() );
    wMask.setEditable( true );
    props.setLook( wMask );
    FormData fdMask = new FormData();
    fdMask.left = new FormAttachment( middle, 0 );
    fdMask.top = new FormAttachment( wFieldType, margin );
    fdMask.right = new FormAttachment( 100, 0 );
    wMask.setLayoutData( fdMask );
    wMask.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

      }
    } );

    FormData fdSource = new FormData();
    fdSource.left = new FormAttachment( 0, margin );
    fdSource.top = new FormAttachment( wName, margin );
    fdSource.right = new FormAttachment( 100, -margin );
    wSource.setLayoutData(fdSource);
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    // SuccessOngrouping?
    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    Group wSuccessOn = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wSuccessOn);
    wSuccessOn.setText( BaseMessages.getString( PKG, "JobSimpleEval.SuccessOn.Group.Label" ) );

    FormLayout successongroupLayout = new FormLayout();
    successongroupLayout.marginWidth = 10;
    successongroupLayout.marginHeight = 10;

    wSuccessOn.setLayout( successongroupLayout );

    // Success when variable is not set?
    wlsuccessWhenSet = new Label(wSuccessOn, SWT.RIGHT );
    wlsuccessWhenSet.setText( BaseMessages.getString( PKG, "JobSimpleEval.SuccessWhenSet.Label" ) );
    props.setLook( wlsuccessWhenSet );
    FormData fdlsuccessWhenSet = new FormData();
    fdlsuccessWhenSet.left = new FormAttachment( 0, 0 );
    fdlsuccessWhenSet.top = new FormAttachment( wVariableName, margin );
    fdlsuccessWhenSet.right = new FormAttachment( middle, -margin );
    wlsuccessWhenSet.setLayoutData(fdlsuccessWhenSet);
    wSuccessWhenSet = new Button(wSuccessOn, SWT.CHECK );
    wSuccessWhenSet.setToolTipText( BaseMessages.getString( PKG, "JobSimpleEval.SuccessWhenSet.Tooltip" ) );
    props.setLook( wSuccessWhenSet );
    FormData fdSuccessWhenSet = new FormData();
    fdSuccessWhenSet.left = new FormAttachment( middle, 0 );
    fdSuccessWhenSet.top = new FormAttachment( wVariableName, margin );
    fdSuccessWhenSet.right = new FormAttachment( 100, 0 );
    wSuccessWhenSet.setLayoutData(fdSuccessWhenSet);
    wSuccessWhenSet.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        refresh();
        action.setChanged();
      }
    } );

    // Success Condition
    wlSuccessCondition = new Label(wSuccessOn, SWT.RIGHT );
    wlSuccessCondition.setText( BaseMessages.getString( PKG, "JobSimpleEval.SuccessCondition.Label" ) );
    props.setLook( wlSuccessCondition );
    FormData fdlSuccessCondition = new FormData();
    fdlSuccessCondition.left = new FormAttachment( 0, 0 );
    fdlSuccessCondition.right = new FormAttachment( middle, 0 );
    fdlSuccessCondition.top = new FormAttachment( wSuccessWhenSet, margin );
    wlSuccessCondition.setLayoutData( fdlSuccessCondition );

    wSuccessCondition = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wSuccessCondition.setItems( ActionSimpleEval.successConditionDesc );
    wSuccessCondition.select( 0 ); // +1: starts at -1

    props.setLook( wSuccessCondition );
    FormData fdSuccessCondition = new FormData();
    fdSuccessCondition.left = new FormAttachment( middle, 0 );
    fdSuccessCondition.top = new FormAttachment( wSuccessWhenSet, margin );
    fdSuccessCondition.right = new FormAttachment( 100, 0 );
    wSuccessCondition.setLayoutData( fdSuccessCondition );
    wSuccessCondition.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

      }
    } );

    // Success number(date) Condition
    wlSuccessNumberCondition = new Label(wSuccessOn, SWT.RIGHT );
    wlSuccessNumberCondition.setText( BaseMessages.getString( PKG, "JobSimpleEval.SuccessNumberCondition.Label" ) );
    props.setLook( wlSuccessNumberCondition );
    FormData fdlSuccessNumberCondition = new FormData();
    fdlSuccessNumberCondition.left = new FormAttachment( 0, 0 );
    fdlSuccessNumberCondition.right = new FormAttachment( middle, -margin );
    fdlSuccessNumberCondition.top = new FormAttachment( wSuccessWhenSet, margin );
    wlSuccessNumberCondition.setLayoutData(fdlSuccessNumberCondition);

    wSuccessNumberCondition = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wSuccessNumberCondition.setItems( ActionSimpleEval.successNumberConditionDesc );
    wSuccessNumberCondition.select( 0 ); // +1: starts at -1

    props.setLook( wSuccessNumberCondition );
    FormData fdSuccessNumberCondition = new FormData();
    fdSuccessNumberCondition.left = new FormAttachment( middle, 0 );
    fdSuccessNumberCondition.top = new FormAttachment( wSuccessWhenSet, margin );
    fdSuccessNumberCondition.right = new FormAttachment( 100, 0 );
    wSuccessNumberCondition.setLayoutData(fdSuccessNumberCondition);
    wSuccessNumberCondition.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        refresh();

      }
    } );

    // Success Boolean Condition
    wlSuccessBooleanCondition = new Label(wSuccessOn, SWT.RIGHT );
    wlSuccessBooleanCondition
      .setText( BaseMessages.getString( PKG, "JobSimpleEval.SuccessBooleanCondition.Label" ) );
    props.setLook( wlSuccessBooleanCondition );
    FormData fdlSuccessBooleanCondition = new FormData();
    fdlSuccessBooleanCondition.left = new FormAttachment( 0, 0 );
    fdlSuccessBooleanCondition.right = new FormAttachment( middle, -margin );
    fdlSuccessBooleanCondition.top = new FormAttachment( wSuccessWhenSet, margin );
    wlSuccessBooleanCondition.setLayoutData(fdlSuccessBooleanCondition);

    wSuccessBooleanCondition = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wSuccessBooleanCondition.setItems( ActionSimpleEval.successBooleanConditionDesc );
    wSuccessBooleanCondition.select( 0 ); // +1: starts at -1

    props.setLook( wSuccessBooleanCondition );
    FormData fdSuccessBooleanCondition = new FormData();
    fdSuccessBooleanCondition.left = new FormAttachment( middle, 0 );
    fdSuccessBooleanCondition.top = new FormAttachment( wSuccessWhenSet, margin );
    fdSuccessBooleanCondition.right = new FormAttachment( 100, 0 );
    wSuccessBooleanCondition.setLayoutData(fdSuccessBooleanCondition);
    wSuccessBooleanCondition.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        refresh();

      }
    } );

    // Compare with value
    wlCompareValue = new Label(wSuccessOn, SWT.RIGHT );
    wlCompareValue.setText( BaseMessages.getString( PKG, "JobSimpleEval.CompareValue.Label" ) );
    props.setLook( wlCompareValue );
    FormData fdlCompareValue = new FormData();
    fdlCompareValue.left = new FormAttachment( 0, 0 );
    fdlCompareValue.top = new FormAttachment( wSuccessNumberCondition, margin );
    fdlCompareValue.right = new FormAttachment( middle, -margin );
    wlCompareValue.setLayoutData(fdlCompareValue);

    wCompareValue =
      new TextVar( variables, wSuccessOn, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobSimpleEval.CompareValue.Tooltip" ) );
    props.setLook( wCompareValue );
    wCompareValue.addModifyListener( lsMod );
    FormData fdCompareValue = new FormData();
    fdCompareValue.left = new FormAttachment( middle, 0 );
    fdCompareValue.top = new FormAttachment( wSuccessNumberCondition, margin );
    fdCompareValue.right = new FormAttachment( 100, -margin );
    wCompareValue.setLayoutData(fdCompareValue);

    // Min value
    wlMinValue = new Label(wSuccessOn, SWT.RIGHT );
    wlMinValue.setText( BaseMessages.getString( PKG, "JobSimpleEval.MinValue.Label" ) );
    props.setLook( wlMinValue );
    FormData fdlMinValue = new FormData();
    fdlMinValue.left = new FormAttachment( 0, 0 );
    fdlMinValue.top = new FormAttachment( wSuccessNumberCondition, margin );
    fdlMinValue.right = new FormAttachment( middle, -margin );
    wlMinValue.setLayoutData(fdlMinValue);

    wMinValue =
      new TextVar( variables, wSuccessOn, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobSimpleEval.MinValue.Tooltip" ) );
    props.setLook( wMinValue );
    wMinValue.addModifyListener( lsMod );
    FormData fdMinValue = new FormData();
    fdMinValue.left = new FormAttachment( middle, 0 );
    fdMinValue.top = new FormAttachment( wSuccessNumberCondition, margin );
    fdMinValue.right = new FormAttachment( 100, -margin );
    wMinValue.setLayoutData(fdMinValue);

    // Maximum value
    wlMaxValue = new Label(wSuccessOn, SWT.RIGHT );
    wlMaxValue.setText( BaseMessages.getString( PKG, "JobSimpleEval.MaxValue.Label" ) );
    props.setLook( wlMaxValue );
    FormData fdlMaxValue = new FormData();
    fdlMaxValue.left = new FormAttachment( 0, 0 );
    fdlMaxValue.top = new FormAttachment( wMinValue, margin );
    fdlMaxValue.right = new FormAttachment( middle, -margin );
    wlMaxValue.setLayoutData(fdlMaxValue);

    wMaxValue =
      new TextVar( variables, wSuccessOn, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobSimpleEval.MaxValue.Tooltip" ) );
    props.setLook( wMaxValue );
    wMaxValue.addModifyListener( lsMod );
    FormData fdMaxValue = new FormData();
    fdMaxValue.left = new FormAttachment( middle, 0 );
    fdMaxValue.top = new FormAttachment( wMinValue, margin );
    fdMaxValue.right = new FormAttachment( 100, -margin );
    wMaxValue.setLayoutData(fdMaxValue);

    FormData fdSuccessOn = new FormData();
    fdSuccessOn.left = new FormAttachment( 0, margin );
    fdSuccessOn.top = new FormAttachment(wSource, margin );
    fdSuccessOn.right = new FormAttachment( 100, -margin );
    wSuccessOn.setLayoutData(fdSuccessOn);
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    props.setLook(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData(fdTabFolder);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, wTabFolder);

    // Add listeners
    Listener lsCancel = e -> cancel();
    Listener lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel);
    wOk.addListener( SWT.Selection, lsOk);

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    wName.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    refresh();

    wTabFolder.setSelection( 0 );
    BaseTransformDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.nullToEmpty( action.getName() ) );
    wValueType.setText( ActionSimpleEval.getValueTypeDesc( action.valuetype ) );
    if ( action.getFieldName() != null ) {
      wFieldName.setText( action.getFieldName() );
    }
    if ( action.getVariableName() != null ) {
      wVariableName.setText( action.getVariableName() );
    }
    wSuccessWhenSet.setSelection( action.isSuccessWhenVarSet() );
    wFieldType.setText( ActionSimpleEval.getFieldTypeDesc( action.fieldtype ) );
    if ( action.getMask() != null ) {
      wMask.setText( action.getMask() );
    }
    if ( action.getCompareValue() != null ) {
      wCompareValue.setText( action.getCompareValue() );
    }
    if ( action.getMinValue() != null ) {
      wMinValue.setText( action.getMinValue() );
    }
    if ( action.getMaxValue() != null ) {
      wMaxValue.setText( action.getMaxValue() );
    }
    wSuccessCondition.setText( ActionSimpleEval.getSuccessConditionDesc( action.successcondition ) );
    wSuccessNumberCondition.setText( ActionSimpleEval
      .getSuccessNumberConditionDesc( action.successnumbercondition ) );
    wSuccessBooleanCondition.setText( ActionSimpleEval
      .getSuccessBooleanConditionDesc( action.successbooleancondition ) );

    wName.selectAll();
    wName.setFocus();
  }

  private void refresh() {
    boolean evaluatepreviousRowField =
      ActionSimpleEval.getValueTypeByDesc( wValueType.getText() ) == ActionSimpleEval.VALUE_TYPE_FIELD;
    boolean evaluateVariable =
      ActionSimpleEval.getValueTypeByDesc( wValueType.getText() ) == ActionSimpleEval.VALUE_TYPE_VARIABLE;
    wlVariableName.setVisible( evaluateVariable );
    wVariableName.setVisible( evaluateVariable );
    wlFieldName.setVisible( evaluatepreviousRowField );
    wFieldName.setVisible( evaluatepreviousRowField );
    wlsuccessWhenSet.setVisible( evaluateVariable );
    wSuccessWhenSet.setVisible( evaluateVariable );

    boolean successWhenSet = wSuccessWhenSet.getSelection() && evaluateVariable;

    wlFieldType.setVisible( !successWhenSet );
    wFieldType.setVisible( !successWhenSet );

    boolean valueTypeDate =
      ActionSimpleEval.getFieldTypeByDesc( wFieldType.getText() ) == ActionSimpleEval.FIELD_TYPE_DATE_TIME;
    wlMask.setVisible( !successWhenSet && valueTypeDate );
    wMask.setVisible( !successWhenSet && valueTypeDate );

    boolean valueTypeString =
      ActionSimpleEval.getFieldTypeByDesc( wFieldType.getText() ) == ActionSimpleEval.FIELD_TYPE_STRING;
    wlSuccessCondition.setVisible( !successWhenSet && valueTypeString );
    wSuccessCondition.setVisible( !successWhenSet && valueTypeString );

    boolean valueTypeNumber =
      ActionSimpleEval.getFieldTypeByDesc( wFieldType.getText() ) == ActionSimpleEval.FIELD_TYPE_NUMBER
        || ActionSimpleEval.getFieldTypeByDesc( wFieldType.getText() ) == ActionSimpleEval.FIELD_TYPE_DATE_TIME;
    wlSuccessNumberCondition.setVisible( !successWhenSet && valueTypeNumber );
    wSuccessNumberCondition.setVisible( !successWhenSet && valueTypeNumber );

    boolean valueTypeBoolean =
      ActionSimpleEval.getFieldTypeByDesc( wFieldType.getText() ) == ActionSimpleEval.FIELD_TYPE_BOOLEAN;
    wlSuccessBooleanCondition.setVisible( !successWhenSet && valueTypeBoolean );
    wSuccessBooleanCondition.setVisible( !successWhenSet && valueTypeBoolean );

    boolean compareValue =
      valueTypeString
        || ( !valueTypeString && ActionSimpleEval.getSuccessNumberConditionByDesc( wSuccessNumberCondition
        .getText() ) != ActionSimpleEval.SUCCESS_NUMBER_CONDITION_BETWEEN );
    wlCompareValue.setVisible( !successWhenSet && compareValue && !valueTypeBoolean );
    wCompareValue.setVisible( !successWhenSet && compareValue && !valueTypeBoolean );
    wlMinValue.setVisible( !successWhenSet && !compareValue && !valueTypeBoolean );
    wMinValue.setVisible( !successWhenSet && !compareValue && !valueTypeBoolean );
    wlMaxValue.setVisible( !successWhenSet && !compareValue && !valueTypeBoolean );
    wMaxValue.setVisible( !successWhenSet && !compareValue && !valueTypeBoolean );
  }

  private void cancel() {
    action.setChanged( changed );
    action = null;
    dispose();
  }

  private void ok() {

    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setText( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    action.setName( wName.getText() );

    action.valuetype = ActionSimpleEval.getValueTypeByDesc( wValueType.getText() );
    action.setFieldName( wFieldName.getText() );
    action.setVariableName( wVariableName.getText() );

    action.fieldtype = ActionSimpleEval.getFieldTypeByDesc( wFieldType.getText() );
    action.setMask( wMask.getText() );
    action.setCompareValue( wCompareValue.getText() );
    action.setMinValue( wMinValue.getText() );
    action.setMaxValue( wMaxValue.getText() );
    action.successcondition = ActionSimpleEval.getSuccessConditionByDesc( wSuccessCondition.getText() );
    action.successnumbercondition =
      ActionSimpleEval.getSuccessNumberConditionByDesc( wSuccessNumberCondition.getText() );
    action.successbooleancondition =
      ActionSimpleEval.getSuccessBooleanConditionByDesc( wSuccessBooleanCondition.getText() );
    action.setSuccessWhenVarSet( wSuccessWhenSet.getSelection() );
    dispose();
  }
}
