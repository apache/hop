/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.trans.steps.columnexists;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.steps.columnexists.ColumnExistsMeta;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

@PluginDialog( id = "ColumnExists", image = "CEX.svg", pluginType = PluginDialog.PluginType.STEP,
  documentationUrl = "https://wiki.pentaho.com/display/EAI/Check+if+a+column+exists" )
public class ColumnExistsDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = ColumnExistsDialog.class; // for i18n purposes, needed by Translator2!!

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Label wlTableName;
  private CCombo wTableName;
  private FormData fdlTableName, fdTableName;

  private Label wlResult;
  private Text wResult;
  private FormData fdlResult, fdResult;

  private Label wlTablenameText;
  private TextVar wTablenameText;
  private FormData fdlTablenameText, fdTablenameText;

  private Label wlColumnName;
  private CCombo wColumnName;
  private FormData fdlColumnName, fdColumnName;

  private Label wlTablenameInField;
  private Button wTablenameInField;
  private FormData fdlTablenameInField, fdTablenameInField;

  // Schema name
  private Label wlSchemaname;
  private TextVar wSchemaname;
  private FormData fdlSchemaname, fdSchemaname;

  private FormData fdbSchema;
  private Button wbSchema;

  private Button wbTable;

  private ColumnExistsMeta input;

  public ColumnExistsDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (ColumnExistsMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    // Connection line
    wConnection = addConnectionLine( shell, wStepname, input.getDatabase(), lsMod );

    // Schema name line
    wlSchemaname = new Label( shell, SWT.RIGHT );
    wlSchemaname.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.Schemaname.Label" ) );
    props.setLook( wlSchemaname );
    fdlSchemaname = new FormData();
    fdlSchemaname.left = new FormAttachment( 0, 0 );
    fdlSchemaname.right = new FormAttachment( middle, -margin );
    fdlSchemaname.top = new FormAttachment( wConnection, margin * 2 );
    wlSchemaname.setLayoutData( fdlSchemaname );

    wbSchema = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSchema );
    wbSchema.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment( wConnection, 2 * margin );
    fdbSchema.right = new FormAttachment( 100, 0 );
    wbSchema.setLayoutData( fdbSchema );
    wbSchema.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getSchemaNames();
      }
    } );

    wSchemaname = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchemaname );
    wSchemaname.setToolTipText( BaseMessages.getString( PKG, "ColumnExistsDialog.Schemaname.Tooltip" ) );
    wSchemaname.addModifyListener( lsMod );
    fdSchemaname = new FormData();
    fdSchemaname.left = new FormAttachment( middle, 0 );
    fdSchemaname.top = new FormAttachment( wConnection, margin * 2 );
    fdSchemaname.right = new FormAttachment( wbSchema, -margin );
    wSchemaname.setLayoutData( fdSchemaname );

    // TablenameText fieldname ...
    wlTablenameText = new Label( shell, SWT.RIGHT );
    wlTablenameText.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.TablenameTextField.Label" ) );
    props.setLook( wlTablenameText );
    fdlTablenameText = new FormData();
    fdlTablenameText.left = new FormAttachment( 0, 0 );
    fdlTablenameText.right = new FormAttachment( middle, -margin );
    fdlTablenameText.top = new FormAttachment( wbSchema, margin );
    wlTablenameText.setLayoutData( fdlTablenameText );

    wbTable = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTable );
    wbTable.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment( wbSchema, margin );
    wbTable.setLayoutData( fdbTable );
    wbTable.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getTableName();
      }
    } );

    wTablenameText = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTablenameText.setToolTipText( BaseMessages.getString( PKG, "ColumnExistsDialog.TablenameTextField.Tooltip" ) );
    props.setLook( wTablenameText );
    wTablenameText.addModifyListener( lsMod );
    fdTablenameText = new FormData();
    fdTablenameText.left = new FormAttachment( middle, 0 );
    fdTablenameText.top = new FormAttachment( wbSchema, margin );
    fdTablenameText.right = new FormAttachment( wbTable, -margin );
    wTablenameText.setLayoutData( fdTablenameText );

    // Is tablename is field?
    wlTablenameInField = new Label( shell, SWT.RIGHT );
    wlTablenameInField.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.TablenameInfield.Label" ) );
    props.setLook( wlTablenameInField );
    fdlTablenameInField = new FormData();
    fdlTablenameInField.left = new FormAttachment( 0, 0 );
    fdlTablenameInField.top = new FormAttachment( wTablenameText, margin );
    fdlTablenameInField.right = new FormAttachment( middle, -margin );
    wlTablenameInField.setLayoutData( fdlTablenameInField );
    wTablenameInField = new Button( shell, SWT.CHECK );
    wTablenameInField.setToolTipText( BaseMessages.getString( PKG, "ColumnExistsDialog.TablenameInfield.Tooltip" ) );
    props.setLook( wTablenameInField );
    fdTablenameInField = new FormData();
    fdTablenameInField.left = new FormAttachment( middle, 0 );
    fdTablenameInField.top = new FormAttachment( wTablenameText, margin );
    fdTablenameInField.right = new FormAttachment( 100, 0 );
    wTablenameInField.setLayoutData( fdTablenameInField );
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
    fdlTableName = new FormData();
    fdlTableName.left = new FormAttachment( 0, 0 );
    fdlTableName.right = new FormAttachment( middle, -margin );
    fdlTableName.top = new FormAttachment( wTablenameInField, margin * 2 );
    wlTableName.setLayoutData( fdlTableName );

    wTableName = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wTableName );
    wTableName.addModifyListener( lsMod );
    fdTableName = new FormData();
    fdTableName.left = new FormAttachment( middle, 0 );
    fdTableName.top = new FormAttachment( wTablenameInField, margin * 2 );
    fdTableName.right = new FormAttachment( 100, -margin );
    wTableName.setLayoutData( fdTableName );
    wTableName.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        get();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // Dynamic column name field
    wlColumnName = new Label( shell, SWT.RIGHT );
    wlColumnName.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.ColumnName.Label" ) );
    props.setLook( wlColumnName );
    fdlColumnName = new FormData();
    fdlColumnName.left = new FormAttachment( 0, 0 );
    fdlColumnName.right = new FormAttachment( middle, -margin );
    fdlColumnName.top = new FormAttachment( wTableName, margin );
    wlColumnName.setLayoutData( fdlColumnName );

    wColumnName = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wColumnName );
    wColumnName.addModifyListener( lsMod );
    fdColumnName = new FormData();
    fdColumnName.left = new FormAttachment( middle, 0 );
    fdColumnName.top = new FormAttachment( wTableName, margin );
    fdColumnName.right = new FormAttachment( 100, -margin );
    wColumnName.setLayoutData( fdColumnName );
    wColumnName.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        get();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // Result fieldname ...
    wlResult = new Label( shell, SWT.RIGHT );
    wlResult.setText( BaseMessages.getString( PKG, "ColumnExistsDialog.ResultField.Label" ) );
    props.setLook( wlResult );
    fdlResult = new FormData();
    fdlResult.left = new FormAttachment( 0, 0 );
    fdlResult.right = new FormAttachment( middle, -margin );
    fdlResult.top = new FormAttachment( wColumnName, margin * 2 );
    wlResult.setLayoutData( fdlResult );
    wResult = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResult.setToolTipText( BaseMessages.getString( PKG, "ColumnExistsDialog.ResultField.Tooltip" ) );
    props.setLook( wResult );
    wResult.addModifyListener( lsMod );
    fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, 0 );
    fdResult.top = new FormAttachment( wColumnName, margin * 2 );
    fdResult.right = new FormAttachment( 100, 0 );
    wResult.setLayoutData( fdResult );

    // THE BUTTONS
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] {
      wOK, wCancel }, margin, wResult );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

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
    return stepname;
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

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    input.setDatabase( transMeta.findDatabase( wConnection.getText() ) );
    input.setSchemaname( wSchemaname.getText() );
    input.setTablename( wTablenameText.getText() );
    input.setTablenameInField( wTablenameInField.getSelection() );
    input.setDynamicTablenameField( wTableName.getText() );
    input.setDynamicColumnnameField( wColumnName.getText() );
    input.setResultFieldName( wResult.getText() );

    stepname = wStepname.getText(); // return value

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
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
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
    DatabaseMeta databaseMeta = transMeta.findDatabase( connectionName );
    if ( databaseMeta != null ) {
      DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, databaseMeta, transMeta.getDatabases() );
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
    DatabaseMeta databaseMeta = transMeta.findDatabase( wConnection.getText() );
    if ( databaseMeta != null ) {
      Database database = new Database( loggingObject, databaseMeta );
      database.shareVariablesWith( transMeta );
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
