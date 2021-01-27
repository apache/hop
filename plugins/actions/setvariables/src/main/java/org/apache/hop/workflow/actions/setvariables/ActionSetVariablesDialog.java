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

package org.apache.hop.workflow.actions.setvariables;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

/**
 * This dialog allows you to edit the Set variables action settings.
 *
 * @author Samatar Hassan
 * @since 06-05-2007
 */
public class ActionSetVariablesDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionSetVariables.class; // For Translator

  private Text wName;

  private Button wVarSubs;

  private ActionSetVariables action;
  private Shell shell;

  private TableView wFields;

  private TextVar wFilename;

  private CCombo wFileVariableType;

  private boolean changed;

  public ActionSetVariablesDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionSetVariables) action;

    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "ActionSetVariables.Name.Default" ) );
    }
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
    shell.setText( BaseMessages.getString( PKG, "ActionSetVariables.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Name line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "ActionSetVariables.Name.Label" ) );
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

    Group gFilename = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(gFilename);
    gFilename.setText( BaseMessages.getString( PKG, "ActionSetVariables.FilenameGroup.Label" ) );

    FormLayout groupFilenameLayout = new FormLayout();
    groupFilenameLayout.marginWidth = 10;
    groupFilenameLayout.marginHeight = 10;
    gFilename.setLayout( groupFilenameLayout );

    // Name line
    Label wlFilename = new Label(gFilename, SWT.RIGHT);
    wlFilename.setText( BaseMessages.getString( PKG, "ActionSetVariables.Filename.Label" ) );
    props.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.right = new FormAttachment( middle, -margin );
    fdlFilename.top = new FormAttachment( 0, margin );
    wlFilename.setLayoutData(fdlFilename);
    wFilename = new TextVar( variables, gFilename, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( 0, margin );
    fdFilename.right = new FormAttachment( 100, 0 );
    wFilename.setLayoutData(fdFilename);

    // file variable type line
    Label wlFileVariableType = new Label(gFilename, SWT.RIGHT);
    wlFileVariableType.setText( BaseMessages.getString( PKG, "ActionSetVariables.FileVariableType.Label" ) );
    props.setLook(wlFileVariableType);
    FormData fdlFileVariableType = new FormData();
    fdlFileVariableType.left = new FormAttachment( 0, 0 );
    fdlFileVariableType.right = new FormAttachment( middle, -margin );
    fdlFileVariableType.top = new FormAttachment( wFilename, margin );
    wlFileVariableType.setLayoutData(fdlFileVariableType);
    wFileVariableType = new CCombo(gFilename, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wFileVariableType );
    wFileVariableType.addModifyListener( lsMod );
    FormData fdFileVariableType = new FormData();
    fdFileVariableType.left = new FormAttachment( middle, 0 );
    fdFileVariableType.top = new FormAttachment( wFilename, margin );
    fdFileVariableType.right = new FormAttachment( 100, 0 );
    wFileVariableType.setLayoutData(fdFileVariableType);
    wFileVariableType.setItems( ActionSetVariables.getVariableTypeDescriptions() );

    FormData fdgFilename = new FormData();
    fdgFilename.left = new FormAttachment( 0, margin );
    fdgFilename.top = new FormAttachment( wName, margin );
    fdgFilename.right = new FormAttachment( 100, -margin );
    gFilename.setLayoutData(fdgFilename);

    //
    // START OF SETTINGS GROUP
    //
    Group gSettings = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(gSettings);
    gSettings.setText( BaseMessages.getString( PKG, "ActionSetVariables.Settings.Label" ) );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    gSettings.setLayout( groupLayout );

    Label wlVarSubs = new Label(gSettings, SWT.RIGHT);
    wlVarSubs.setText( BaseMessages.getString( PKG, "ActionSetVariables.VarsReplace.Label" ) );
    props.setLook(wlVarSubs);
    FormData fdlVarSubs = new FormData();
    fdlVarSubs.left = new FormAttachment( 0, 0 );
    fdlVarSubs.top = new FormAttachment( wName, margin );
    fdlVarSubs.right = new FormAttachment( middle, -margin );
    wlVarSubs.setLayoutData(fdlVarSubs);
    wVarSubs = new Button(gSettings, SWT.CHECK );
    props.setLook( wVarSubs );
    wVarSubs.setToolTipText( BaseMessages.getString( PKG, "ActionSetVariables.VarsReplace.Tooltip" ) );
    FormData fdVarSubs = new FormData();
    fdVarSubs.left = new FormAttachment( middle, 0 );
    fdVarSubs.top = new FormAttachment( wName, margin );
    fdVarSubs.right = new FormAttachment( 100, 0 );
    wVarSubs.setLayoutData(fdVarSubs);
    wVarSubs.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    FormData fdgSettings = new FormData();
    fdgSettings.left = new FormAttachment( 0, margin );
    fdgSettings.top = new FormAttachment(gFilename, margin );
    fdgSettings.right = new FormAttachment( 100, -margin );
    gSettings.setLayoutData(fdgSettings);

    // ///////////////////////////////////////////////////////////
    // / END OF SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "SetVariableDialog.Variables.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment(gSettings, margin );
    wlFields.setLayoutData(fdlFields);

    int rows =
      action.variableName == null
        ? 1 : ( action.variableName.length == 0 ? 0 : action.variableName.length );
    final int FieldsRows = rows;

    ColumnInfo[] colinf =
      {
        new ColumnInfo(
          BaseMessages.getString( PKG, "SetVariableDialog.Fields.Column.VariableName" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SetVariableDialog.Fields.Column.Value" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SetVariableDialog.Fields.Column.VariableType" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ActionSetVariables.getVariableTypeDescriptions(), false ), };
    colinf[ 0 ].setUsingVariables( true );
    colinf[ 1 ].setUsingVariables( true );

    wFields =
      new TableView(
    		  variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, -50 );
    wFields.setLayoutData(fdFields);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, wFields );

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

    wFilename.setText( Const.NVL( action.getFilename(), "" ) );
    wFileVariableType.setText( ActionSetVariables.getVariableTypeDescription( action.getFileVariableType() ) );

    wVarSubs.setSelection( action.isReplaceVars() );

    if ( action.variableName != null ) {
      for ( int i = 0; i < action.variableName.length; i++ ) {
        TableItem ti = wFields.table.getItem( i );
        if ( action.variableName[ i ] != null ) {
          ti.setText( 1, action.variableName[ i ] );
        }
        if ( action.getVariableValue()[ i ] != null ) {
          ti.setText( 2, action.getVariableValue()[ i ] );
        }

        ti.setText( 3, ActionSetVariables.getVariableTypeDescription( action.getVariableType()[ i ] ) );

      }
      wFields.setRowNums();
      wFields.optWidth( true );
    }

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged( changed );
    action = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    action.setName( wName.getText() );

    action.setFilename( wFilename.getText() );
    action.setFileVariableType( ActionSetVariables.getVariableType( wFileVariableType.getText() ) );
    action.setReplaceVars( wVarSubs.getSelection() );

    int nrItems = wFields.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        nr++;
      }
    }
    action.variableName = new String[ nr ];
    action.variableValue = new String[ nr ];
    action.variableType = new int[ nr ];

    nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String varname = wFields.getNonEmpty( i ).getText( 1 );
      String varvalue = wFields.getNonEmpty( i ).getText( 2 );
      String vartype = wFields.getNonEmpty( i ).getText( 3 );

      if ( varname != null && varname.length() != 0 ) {
        action.variableName[ nr ] = varname;
        action.variableValue[ nr ] = varvalue;
        action.variableType[ nr ] = ActionSetVariables.getVariableType( vartype );
        nr++;
      }
    }

    dispose();
  }
}
