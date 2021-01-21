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

package org.apache.hop.pipeline.transforms.columnexists;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class ColumnExistsDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ColumnExistsDialog.class; // For Translator

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Label wlTableName;
  private CCombo wTableName;

  private Text wResult;

  private Label wlTablenameText;
  private TextVar wTablenameText;

  private CCombo wColumnName;

  private Button wTablenameInField;

  private TextVar wSchemaname;

  private final ColumnExistsMeta input;

  public ColumnExistsDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (ColumnExistsMeta) in;
  }

  @Override
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
    shell.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.TransformName.Label" ) );
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
    wConnection = addConnectionLine( shell, wTransformName, input.getDatabase(), lsMod );

    // Schema name line
    // Schema name
    Label wlSchemaname = new Label(shell, SWT.RIGHT);
    wlSchemaname.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.Schemaname.Label" ) );
    props.setLook(wlSchemaname);
    FormData fdlSchemaname = new FormData();
    fdlSchemaname.left = new FormAttachment( 0, 0 );
    fdlSchemaname.right = new FormAttachment( middle, -margin );
    fdlSchemaname.top = new FormAttachment( wConnection, margin * 2 );
    wlSchemaname.setLayoutData(fdlSchemaname);

    Button wbSchema = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbSchema);
    wbSchema.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment( wConnection, 2 * margin );
    fdbSchema.right = new FormAttachment( 100, 0 );
    wbSchema.setLayoutData(fdbSchema);
    wbSchema.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getSchemaNames();
      }
    } );

    wSchemaname = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchemaname );
    wSchemaname.setToolTipText( BaseMessages.getString( PKG, "ColumnExistsDialog.Schemaname.Tooltip" ) );
    wSchemaname.addModifyListener( lsMod );
    FormData fdSchemaname = new FormData();
    fdSchemaname.left = new FormAttachment( middle, 0 );
    fdSchemaname.top = new FormAttachment( wConnection, margin * 2 );
    fdSchemaname.right = new FormAttachment(wbSchema, -margin );
    wSchemaname.setLayoutData(fdSchemaname);

    // TablenameText fieldname ...
    wlTablenameText = new Label( shell, SWT.RIGHT );
    wlTablenameText.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.TablenameTextField.Label" ) );
    props.setLook( wlTablenameText );
    FormData fdlTablenameText = new FormData();
    fdlTablenameText.left = new FormAttachment( 0, 0 );
    fdlTablenameText.right = new FormAttachment( middle, -margin );
    fdlTablenameText.top = new FormAttachment(wbSchema, margin );
    wlTablenameText.setLayoutData(fdlTablenameText);

    Button wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTable);
    wbTable.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment(wbSchema, margin );
    wbTable.setLayoutData( fdbTable );
    wbTable.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getTableName();
      }
    } );

    wTablenameText = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTablenameText.setToolTipText( BaseMessages.getString( PKG, "ColumnExistsDialog.TablenameTextField.Tooltip" ) );
    props.setLook( wTablenameText );
    wTablenameText.addModifyListener( lsMod );
    FormData fdTablenameText = new FormData();
    fdTablenameText.left = new FormAttachment( middle, 0 );
    fdTablenameText.top = new FormAttachment(wbSchema, margin );
    fdTablenameText.right = new FormAttachment(wbTable, -margin );
    wTablenameText.setLayoutData(fdTablenameText);

    // Is tablename is field?
    Label wlTablenameInField = new Label(shell, SWT.RIGHT);
    wlTablenameInField.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.TablenameInfield.Label" ) );
    props.setLook(wlTablenameInField);
    FormData fdlTablenameInField = new FormData();
    fdlTablenameInField.left = new FormAttachment( 0, 0 );
    fdlTablenameInField.top = new FormAttachment( wTablenameText, margin );
    fdlTablenameInField.right = new FormAttachment( middle, -margin );
    wlTablenameInField.setLayoutData(fdlTablenameInField);
    wTablenameInField = new Button( shell, SWT.CHECK );
    wTablenameInField.setToolTipText( BaseMessages.getString( PKG, "ColumnExistsDialog.TablenameInfield.Tooltip" ) );
    props.setLook( wTablenameInField );
    FormData fdTablenameInField = new FormData();
    fdTablenameInField.left = new FormAttachment( middle, 0 );
    fdTablenameInField.top = new FormAttachment( wlTablenameInField, 0, SWT.CENTER );
    fdTablenameInField.right = new FormAttachment( 100, 0 );
    wTablenameInField.setLayoutData(fdTablenameInField);
    SelectionAdapter lsSelR = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
        activeTablenameInField();
      }
    };
    wTablenameInField.addSelectionListener( lsSelR );

    // Dynamic tablename
    wlTableName = new Label( shell, SWT.RIGHT );
    wlTableName.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.TableName.Label" ) );
    props.setLook( wlTableName );
    FormData fdlTableName = new FormData();
    fdlTableName.left = new FormAttachment( 0, 0 );
    fdlTableName.right = new FormAttachment( middle, -margin );
    fdlTableName.top = new FormAttachment( wTablenameInField, margin * 2 );
    wlTableName.setLayoutData(fdlTableName);

    wTableName = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wTableName );
    wTableName.addModifyListener( lsMod );
    FormData fdTableName = new FormData();
    fdTableName.left = new FormAttachment( middle, 0 );
    fdTableName.top = new FormAttachment( wTablenameInField, margin * 2 );
    fdTableName.right = new FormAttachment( 100, -margin );
    wTableName.setLayoutData(fdTableName);
    wTableName.addFocusListener( new FocusListener() {
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

    // Dynamic column name field
    Label wlColumnName = new Label(shell, SWT.RIGHT);
    wlColumnName.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.ColumnName.Label" ) );
    props.setLook(wlColumnName);
    FormData fdlColumnName = new FormData();
    fdlColumnName.left = new FormAttachment( 0, 0 );
    fdlColumnName.right = new FormAttachment( middle, -margin );
    fdlColumnName.top = new FormAttachment( wTableName, margin );
    wlColumnName.setLayoutData(fdlColumnName);

    wColumnName = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wColumnName );
    wColumnName.addModifyListener( lsMod );
    FormData fdColumnName = new FormData();
    fdColumnName.left = new FormAttachment( middle, 0 );
    fdColumnName.top = new FormAttachment( wTableName, margin );
    fdColumnName.right = new FormAttachment( 100, -margin );
    wColumnName.setLayoutData(fdColumnName);
    wColumnName.addFocusListener( new FocusListener() {
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

    // Result fieldname ...
    Label wlResult = new Label(shell, SWT.RIGHT);
    wlResult.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.ResultField.Label" ) );
    props.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment( 0, 0 );
    fdlResult.right = new FormAttachment( middle, -margin );
    fdlResult.top = new FormAttachment( wColumnName, margin * 2 );
    wlResult.setLayoutData(fdlResult);
    wResult = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResult.setToolTipText( BaseMessages.getString( PKG, "ColumnExistsDialog.ResultField.Tooltip" ) );
    props.setLook( wResult );
    wResult.addModifyListener( lsMod );
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, 0 );
    fdResult.top = new FormAttachment( wColumnName, margin * 2 );
    fdResult.right = new FormAttachment( 100, 0 );
    wResult.setLayoutData(fdResult);

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] {
      wOk, wCancel }, margin, wResult );

    // Add listeners
    lsOk = e -> ok();

    lsCancel = e -> cancel();

    wOk.addListener( SWT.Selection, lsOk );
    wCancel.addListener( SWT.Selection, lsCancel );

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

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    activeTablenameInField();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void activeTablenameInField() {
    wlTableName.setEnabled( wTablenameInField.getSelection() );
    wTableName.setEnabled( wTablenameInField.getSelection() );
    wTablenameText.setEnabled( !wTablenameInField.getSelection() );
    wlTablenameText.setEnabled( !wTablenameInField.getSelection() );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "ColumnExistsDialog.Log.GettingKeyInfo" ) );
    }

    if ( input.getDatabase() != null ) {
      wConnection.setText( input.getDatabase().getName() );
    }
    if ( input.getSchemaname() != null ) {
      wSchemaname.setText( input.getSchemaname() );
    }
    if ( input.getTablename() != null ) {
      wTablenameText.setText( input.getTablename() );
    }
    wTablenameInField.setSelection( input.isTablenameInField() );
    if ( input.getDynamicTablenameField() != null ) {
      wTableName.setText( input.getDynamicTablenameField() );
    }
    if ( input.getDynamicColumnnameField() != null ) {
      wColumnName.setText( input.getDynamicColumnnameField() );
    }
    if ( input.getResultFieldName() != null ) {
      wResult.setText( input.getResultFieldName() );
    }

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

    input.setDatabase( pipelineMeta.findDatabase( wConnection.getText() ) );
    input.setSchemaname( wSchemaname.getText() );
    input.setTablename( wTablenameText.getText() );
    input.setTablenameInField( wTablenameInField.getSelection() );
    input.setDynamicTablenameField( wTableName.getText() );
    input.setDynamicColumnnameField( wColumnName.getText() );
    input.setResultFieldName( wResult.getText() );

    transformName = wTransformName.getText(); // return value

    if ( input.getDatabase() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "ColumnExistsDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
    }

    dispose();
  }

  private void get() {
    try {
      String columnName = wColumnName.getText();
      String tableName = wTableName.getText();

      wColumnName.removeAll();
      wTableName.removeAll();
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        r.getFieldNames();

        for ( int i = 0; i < r.getFieldNames().length; i++ ) {
          wTableName.add( r.getFieldNames()[ i ] );
          wColumnName.add( r.getFieldNames()[ i ] );
        }
      }
      wColumnName.setText( columnName );
      wTableName.setText( tableName );
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "ColumnExistsDialog.FailedToGetFields.DialogTitle" ),
        BaseMessages.getString( PKG, "ColumnExistsDialog.FailedToGetFields.DialogMessage" ), ke );
    }

  }

  private void getTableName() {
    String connectionName = wConnection.getText();
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase( connectionName );
    if ( databaseMeta != null ) {
      DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases() );
      std.setSelectedSchemaAndTable( wSchemaname.getText(), wTablenameText.getText() );
      if ( std.open() ) {
        wSchemaname.setText( Const.NVL( std.getSchemaName(), "" ) );
        wTablenameText.setText( Const.NVL( std.getTableName(), "" ) );
      }
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "ColumnExistsDialog.ConnectionError2.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
      mb.open();
    }

  }

  private void getSchemaNames() {
    if ( wSchemaname.isDisposed() ) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase( wConnection.getText() );
    if ( databaseMeta != null ) {
      Database database = new Database( loggingObject, variables, databaseMeta );
      try {
        database.connect();
        String[] schemas = database.getSchemas();

        if ( null != schemas && schemas.length > 0 ) {
          schemas = Const.sortStrings( schemas );
          EnterSelectionDialog dialog =
            new EnterSelectionDialog( shell, schemas,
              BaseMessages.getString( PKG, "System.Dialog.AvailableSchemas.Title", wConnection.getText() ),
              BaseMessages.getString( PKG, "System.Dialog.AvailableSchemas.Message" ) );
          String d = dialog.open();
          if ( d != null ) {
            wSchemaname.setText( Const.NVL( d.toString(), "" ) );
          }

        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "System.Dialog.AvailableSchemas.Empty.Message" ) );
          mb.setText( BaseMessages.getString( PKG, "System.Dialog.AvailableSchemas.Empty.Title" ) );
          mb.open();
        }
      } catch ( Exception e ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ),
          BaseMessages.getString( PKG, "System.Dialog.AvailableSchemas.ConnectionError" ), e );
      } finally {
        if ( database != null ) {
          database.disconnect();
          database = null;
        }
      }
    }
  }

}
