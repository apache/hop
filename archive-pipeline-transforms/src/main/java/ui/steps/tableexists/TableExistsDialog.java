/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.pipeline.transforms.tableexists;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transforms.tableexists.TableExistsMeta;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
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

public class TableExistsDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = TableExistsMeta.class; // for i18n purposes, needed by Translator!!

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Label wlTableName;
  private CCombo wTableName;
  private FormData fdlTableName, fdTableName;

  private Label wlResult;
  private Text wResult;
  private FormData fdlResult, fdResult;

  // Schema name
  private Label wlSchemaname;
  private TextVar wSchemaname;
  private FormData fdlSchemaname, fdSchemaname;

  private TableExistsMeta input;

  private FormData fdbSchema;
  private Button wbSchema;

  public TableExistsDialog( Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (TableExistsMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "TableExistsDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "TableExistsDialog.TransformName.Label" ) );
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
    if ( input.getDatabase() == null && pipelineMeta.nrDatabases() == 1 ) {
      wConnection.select( 0 );
    }
    wConnection.addModifyListener( lsMod );

    // Schema name line
    wlSchemaname = new Label( shell, SWT.RIGHT );
    wlSchemaname.setText( BaseMessages.getString( PKG, "TableExistsDialog.Schemaname.Label" ) );
    props.setLook( wlSchemaname );
    fdlSchemaname = new FormData();
    fdlSchemaname.left = new FormAttachment( 0, 0 );
    fdlSchemaname.right = new FormAttachment( middle, -margin );
    fdlSchemaname.top = new FormAttachment( wConnection, 2 * margin );
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

    wSchemaname = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchemaname );
    wSchemaname.setToolTipText( BaseMessages.getString( PKG, "TableExistsDialog.Schemaname.Tooltip" ) );
    wSchemaname.addModifyListener( lsMod );
    fdSchemaname = new FormData();
    fdSchemaname.left = new FormAttachment( middle, 0 );
    fdSchemaname.top = new FormAttachment( wConnection, 2 * margin );
    fdSchemaname.right = new FormAttachment( wbSchema, -margin );
    wSchemaname.setLayoutData( fdSchemaname );

    wlTableName = new Label( shell, SWT.RIGHT );
    wlTableName.setText( BaseMessages.getString( PKG, "TableExistsDialog.TableName.Label" ) );
    props.setLook( wlTableName );
    fdlTableName = new FormData();
    fdlTableName.left = new FormAttachment( 0, 0 );
    fdlTableName.right = new FormAttachment( middle, -margin );
    fdlTableName.top = new FormAttachment( wbSchema, margin );
    wlTableName.setLayoutData( fdlTableName );

    wTableName = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wTableName );
    wTableName.addModifyListener( lsMod );
    fdTableName = new FormData();
    fdTableName.left = new FormAttachment( middle, 0 );
    fdTableName.top = new FormAttachment( wbSchema, margin );
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

    // Result fieldname ...
    wlResult = new Label( shell, SWT.RIGHT );
    wlResult.setText( BaseMessages.getString( PKG, "TableExistsDialog.ResultField.Label" ) );
    props.setLook( wlResult );
    fdlResult = new FormData();
    fdlResult.left = new FormAttachment( 0, 0 );
    fdlResult.right = new FormAttachment( middle, -margin );
    fdlResult.top = new FormAttachment( wTableName, margin * 2 );
    wlResult.setLayoutData( fdlResult );
    wResult = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResult.setToolTipText( BaseMessages.getString( PKG, "TableExistsDialog.ResultField.Tooltip" ) );
    props.setLook( wResult );
    wResult.addModifyListener( lsMod );
    fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, 0 );
    fdResult.top = new FormAttachment( wTableName, margin * 2 );
    fdResult.right = new FormAttachment( 100, 0 );
    wResult.setLayoutData( fdResult );

    // THE BUTTONS
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, wResult );

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
    input.setChanged( changed );

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
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "TableExistsDialog.Log.GettingKeyInfo" ) );
    }

    if ( input.getDatabase() != null ) {
      wConnection.setText( input.getDatabase().getName() );
    }
    if ( input.getDynamicTablenameField() != null ) {
      wTableName.setText( input.getDynamicTablenameField() );
    }
    if ( input.getSchemaname() != null ) {
      wSchemaname.setText( input.getSchemaname() );
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
    input.setDynamicTablenameField( wTableName.getText() );
    input.setResultFieldName( wResult.getText() );

    transformName = wTransformName.getText(); // return value

    if ( input.getDatabase() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "TableExistsDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "TableExistsDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
    }

    dispose();
  }

  private void get() {
    try {

      wTableName.removeAll();
      IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
      if ( r != null ) {
        r.getFieldNames();

        for ( int i = 0; i < r.getFieldNames().length; i++ ) {
          wTableName.add( r.getFieldNames()[ i ] );
        }
      }

    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "TableExistsDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "TableExistsDialog.FailedToGetFields.DialogMessage" ), ke );
    }

  }

  private void getSchemaNames() {
    if ( wSchemaname.isDisposed() ) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase( wConnection.getText() );
    if ( databaseMeta != null ) {
      Database database = new Database( loggingObject, databaseMeta );
      database.shareVariablesWith( pipelineMeta );
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
        new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
          .getString( PKG, "System.Dialog.AvailableSchemas.ConnectionError" ), e );
      } finally {
        if ( database != null ) {
          database.disconnect();
          database = null;
        }
      }
    }
  }
}
