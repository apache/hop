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

package org.apache.hop.pipeline.transforms.execsqlrow;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class ExecSqlRowDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ExecSqlRowMeta.class; // For Translator

  private boolean gotPreviousFields = false;
  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Text wInsertField;

  private Text wUpdateField;

  private Text wDeleteField;

  private Text wReadField;

  private CCombo wSqlFieldName;

  private Text wCommit;

  private final ExecSqlRowMeta input;

  private Button wSqlFromFile;

  private Button wSendOneStatement;

  public ExecSqlRowDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (ExecSqlRowMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ExecSqlRowDialog.Shell.Label" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "ExecSqlRowDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );

    // Connection line
    wConnection = addConnectionLine( shell, wTransformName, input.getDatabaseMeta(), lsMod );
    if ( input.getDatabaseMeta() == null && pipelineMeta.nrDatabases() == 1 ) {
      wConnection.select( 0 );
    }
    wConnection.addModifyListener( lsMod );

    // Commit line
    Label wlCommit = new Label(shell, SWT.RIGHT);
    wlCommit.setText( BaseMessages.getString( PKG, "ExecSqlRowDialog.Commit.Label" ) );
    props.setLook(wlCommit);
    FormData fdlCommit = new FormData();
    fdlCommit.left = new FormAttachment( 0, 0 );
    fdlCommit.top = new FormAttachment( wConnection, margin );
    fdlCommit.right = new FormAttachment( middle, -margin );
    wlCommit.setLayoutData(fdlCommit);
    wCommit = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCommit );
    wCommit.addModifyListener( lsMod );
    FormData fdCommit = new FormData();
    fdCommit.left = new FormAttachment( middle, 0 );
    fdCommit.top = new FormAttachment( wConnection, margin );
    fdCommit.right = new FormAttachment( 100, 0 );
    wCommit.setLayoutData(fdCommit);

    Label wlSendOneStatement = new Label(shell, SWT.RIGHT);
    wlSendOneStatement.setText( BaseMessages.getString( PKG, "ExecSqlRowDialog.SendOneStatement.Label" ) );
    props.setLook(wlSendOneStatement);
    FormData fdlSendOneStatement = new FormData();
    fdlSendOneStatement.left = new FormAttachment( 0, 0 );
    fdlSendOneStatement.top = new FormAttachment( wCommit, margin );
    fdlSendOneStatement.right = new FormAttachment( middle, -margin );
    wlSendOneStatement.setLayoutData(fdlSendOneStatement);
    wSendOneStatement = new Button( shell, SWT.CHECK );
    wSendOneStatement.setToolTipText( BaseMessages.getString( PKG, "ExecSqlRowDialog.SendOneStatement.Tooltip" ) );
    props.setLook( wSendOneStatement );
    FormData fdSendOneStatement = new FormData();
    fdSendOneStatement.left = new FormAttachment( middle, 0 );
    fdSendOneStatement.top = new FormAttachment( wlSendOneStatement, 0, SWT.CENTER );
    fdSendOneStatement.right = new FormAttachment( 100, 0 );
    wSendOneStatement.setLayoutData(fdSendOneStatement);
    wSendOneStatement.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // SQLFieldName field
    Label wlSqlFieldName = new Label(shell, SWT.RIGHT);
    wlSqlFieldName.setText( BaseMessages.getString( PKG, "ExecSqlRowDialog.SQLFieldName.Label" ) );
    props.setLook(wlSqlFieldName);
    FormData fdlSqlFieldName = new FormData();
    fdlSqlFieldName.left = new FormAttachment( 0, 0 );
    fdlSqlFieldName.right = new FormAttachment( middle, -margin );
    fdlSqlFieldName.top = new FormAttachment( wSendOneStatement, 2 * margin );
    wlSqlFieldName.setLayoutData(fdlSqlFieldName);
    wSqlFieldName = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    wSqlFieldName.setEditable( true );
    props.setLook( wSqlFieldName );
    wSqlFieldName.addModifyListener( lsMod );
    FormData fdSqlFieldName = new FormData();
    fdSqlFieldName.left = new FormAttachment( middle, 0 );
    fdSqlFieldName.top = new FormAttachment( wSendOneStatement, 2 * margin );
    fdSqlFieldName.right = new FormAttachment( 100, -margin );
    wSqlFieldName.setLayoutData(fdSqlFieldName);
    wSqlFieldName.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        get();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    Label wlSqlFromFile = new Label(shell, SWT.RIGHT);
    wlSqlFromFile.setText( BaseMessages.getString( PKG, "ExecSqlRowDialog.SQLFromFile.Label" ) );
    props.setLook(wlSqlFromFile);
    FormData fdlSqlFromFile = new FormData();
    fdlSqlFromFile.left = new FormAttachment( 0, 0 );
    fdlSqlFromFile.top = new FormAttachment( wSqlFieldName, margin );
    fdlSqlFromFile.right = new FormAttachment( middle, -margin );
    wlSqlFromFile.setLayoutData(fdlSqlFromFile);
    wSqlFromFile = new Button( shell, SWT.CHECK );
    wSqlFromFile.setToolTipText( BaseMessages.getString( PKG, "ExecSqlRowDialog.SQLFromFile.Tooltip" ) );
    props.setLook( wSqlFromFile );
    FormData fdSqlFromFile = new FormData();
    fdSqlFromFile.left = new FormAttachment( middle, 0 );
    fdSqlFromFile.top = new FormAttachment( wlSqlFromFile, 0, SWT.CENTER );
    fdSqlFromFile.right = new FormAttachment( 100, 0 );
    wSqlFromFile.setLayoutData(fdSqlFromFile);
    wSqlFromFile.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // ///////////////////////////////
    // START OF Additional Fields GROUP //
    // ///////////////////////////////

    Group wAdditionalFields = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wAdditionalFields);
    wAdditionalFields.setText( BaseMessages.getString( PKG, "ExecSqlRowDialog.wAdditionalFields.Label" ) );

    FormLayout AdditionalFieldsgroupLayout = new FormLayout();
    AdditionalFieldsgroupLayout.marginWidth = 10;
    AdditionalFieldsgroupLayout.marginHeight = 10;
    wAdditionalFields.setLayout( AdditionalFieldsgroupLayout );

    // insert field
    Label wlInsertField = new Label(wAdditionalFields, SWT.RIGHT);
    wlInsertField.setText( BaseMessages.getString( PKG, "ExecSqlRowDialog.InsertField.Label" ) );
    props.setLook(wlInsertField);
    FormData fdlInsertField = new FormData();
    fdlInsertField.left = new FormAttachment( 0, margin );
    fdlInsertField.right = new FormAttachment( middle, -margin );
    fdlInsertField.top = new FormAttachment( wSqlFromFile, margin );
    wlInsertField.setLayoutData(fdlInsertField);
    wInsertField = new Text(wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInsertField );
    wInsertField.addModifyListener( lsMod );
    FormData fdInsertField = new FormData();
    fdInsertField.left = new FormAttachment( middle, 0 );
    fdInsertField.top = new FormAttachment( wSqlFromFile, margin );
    fdInsertField.right = new FormAttachment( 100, 0 );
    wInsertField.setLayoutData(fdInsertField);

    // Update field
    Label wlUpdateField = new Label(wAdditionalFields, SWT.RIGHT);
    wlUpdateField.setText( BaseMessages.getString( PKG, "ExecSqlRowDialog.UpdateField.Label" ) );
    props.setLook(wlUpdateField);
    FormData fdlUpdateField = new FormData();
    fdlUpdateField.left = new FormAttachment( 0, margin );
    fdlUpdateField.right = new FormAttachment( middle, -margin );
    fdlUpdateField.top = new FormAttachment( wInsertField, margin );
    wlUpdateField.setLayoutData(fdlUpdateField);
    wUpdateField = new Text(wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUpdateField );
    wUpdateField.addModifyListener( lsMod );
    FormData fdUpdateField = new FormData();
    fdUpdateField.left = new FormAttachment( middle, 0 );
    fdUpdateField.top = new FormAttachment( wInsertField, margin );
    fdUpdateField.right = new FormAttachment( 100, 0 );
    wUpdateField.setLayoutData(fdUpdateField);

    // Delete field
    Label wlDeleteField = new Label(wAdditionalFields, SWT.RIGHT);
    wlDeleteField.setText( BaseMessages.getString( PKG, "ExecSqlRowDialog.DeleteField.Label" ) );
    props.setLook(wlDeleteField);
    FormData fdlDeleteField = new FormData();
    fdlDeleteField.left = new FormAttachment( 0, margin );
    fdlDeleteField.right = new FormAttachment( middle, -margin );
    fdlDeleteField.top = new FormAttachment( wUpdateField, margin );
    wlDeleteField.setLayoutData(fdlDeleteField);
    wDeleteField = new Text(wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDeleteField );
    wDeleteField.addModifyListener( lsMod );
    FormData fdDeleteField = new FormData();
    fdDeleteField.left = new FormAttachment( middle, 0 );
    fdDeleteField.top = new FormAttachment( wUpdateField, margin );
    fdDeleteField.right = new FormAttachment( 100, 0 );
    wDeleteField.setLayoutData(fdDeleteField);

    // Read field
    Label wlReadField = new Label(wAdditionalFields, SWT.RIGHT);
    wlReadField.setText( BaseMessages.getString( PKG, "ExecSqlRowDialog.ReadField.Label" ) );
    props.setLook(wlReadField);
    FormData fdlReadField = new FormData();
    fdlReadField.left = new FormAttachment( 0, 0 );
    fdlReadField.right = new FormAttachment( middle, -margin );
    fdlReadField.top = new FormAttachment( wDeleteField, margin );
    wlReadField.setLayoutData(fdlReadField);
    wReadField = new Text(wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wReadField );
    wReadField.addModifyListener( lsMod );
    FormData fdReadField = new FormData();
    fdReadField.left = new FormAttachment( middle, 0 );
    fdReadField.top = new FormAttachment( wDeleteField, margin );
    fdReadField.right = new FormAttachment( 100, 0 );
    wReadField.setLayoutData(fdReadField);

    FormData fdAdditionalFields = new FormData();
    fdAdditionalFields.left = new FormAttachment( 0, margin );
    fdAdditionalFields.top = new FormAttachment( wSqlFromFile, 2 * margin );
    fdAdditionalFields.right = new FormAttachment( 100, -margin );
    wAdditionalFields.setLayoutData(fdAdditionalFields);

    // ///////////////////////////////
    // END OF Additional Fields GROUP //
    // ///////////////////////////////

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, wAdditionalFields);

    // Add listeners
    lsCancel = e -> cancel();
    lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    input.setChanged( changed );

    // Set the shell size, based upon previous time...
    setSize();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wCommit.setText( "" + input.getCommitSize() );
    if ( input.getSqlFieldName() != null ) {
      wSqlFieldName.setText( input.getSqlFieldName() );
    }
    if ( input.getDatabaseMeta() != null ) {
      wConnection.setText( input.getDatabaseMeta().getName() );
    }

    if ( input.getUpdateField() != null ) {
      wUpdateField.setText( input.getUpdateField() );
    }
    if ( input.getInsertField() != null ) {
      wInsertField.setText( input.getInsertField() );
    }
    if ( input.getDeleteField() != null ) {
      wDeleteField.setText( input.getDeleteField() );
    }
    if ( input.getReadField() != null ) {
      wReadField.setText( input.getReadField() );
    }
    wSqlFromFile.setSelection( input.isSqlFromfile() );
    wSendOneStatement.setSelection( input.IsSendOneStatement() );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }
    input.setCommitSize( Const.toInt( wCommit.getText(), 0 ) );
    transformName = wTransformName.getText(); // return value
    input.setSqlFieldName( wSqlFieldName.getText() );
    // copy info to TextFileInputMeta class (input)
    input.setDatabaseMeta( pipelineMeta.findDatabase( wConnection.getText() ) );

    input.setInsertField( wInsertField.getText() );
    input.setUpdateField( wUpdateField.getText() );
    input.setDeleteField( wDeleteField.getText() );
    input.setReadField( wReadField.getText() );
    input.setSqlFromfile( wSqlFromFile.getSelection() );
    input.SetSendOneStatement( wSendOneStatement.getSelection() );
    if ( input.getDatabaseMeta() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "ExecSqlRowDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "ExecSqlRowDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
      return;
    }

    dispose();
  }

  private void get() {
    if ( !gotPreviousFields ) {
      gotPreviousFields = true;
      try {
        String sqlfield = wSqlFieldName.getText();
        wSqlFieldName.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wSqlFieldName.removeAll();
          wSqlFieldName.setItems( r.getFieldNames() );
        }
        if ( sqlfield != null ) {
          wSqlFieldName.setText( sqlfield );
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "ExecSqlRowDialog.FailedToGetFields.DialogTitle" ), BaseMessages
          .getString( PKG, "ExecSqlRowDialog.FailedToGetFields.DialogMessage" ), ke );
      }
    }
  }
}
