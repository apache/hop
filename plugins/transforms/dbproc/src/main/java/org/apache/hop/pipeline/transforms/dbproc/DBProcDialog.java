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

package org.apache.hop.pipeline.transforms.dbproc;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DBProcDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = DBProcMeta.class; // For Translator

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wProcName;

  private Button wAutoCommit;

  private Text wResult;

  private CCombo wResultType;

  private TableView wFields;

  private final DBProcMeta input;

  private ColumnInfo[] fieldColumns;

  private final Map<String, Integer> inputFields;

  public DBProcDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (DBProcMeta) in;
    inputFields = new HashMap<>();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();

    SelectionAdapter lsSelMod = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "DBProcDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();


    // The buttons go at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    Button wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "DBProcDialog.GetFields.Button" ) );
    wGet.addListener( SWT.Selection, e -> get() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wGet, wCancel, }, margin, null );


    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "DBProcDialog.TransformName.Label" ) );
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

    // ProcName line...
    // add button to get list of procedures on selected connection...
    Button wbProcName = new Button( shell, SWT.PUSH );
    wbProcName.setText( BaseMessages.getString( PKG, "DBProcDialog.Finding.Button" ) );
    FormData fdbProcName = new FormData();
    fdbProcName.right = new FormAttachment( 100, 0 );
    fdbProcName.top = new FormAttachment( wConnection, margin * 2 );
    wbProcName.setLayoutData( fdbProcName );
    wbProcName.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        DatabaseMeta dbInfo = pipelineMeta.findDatabase( wConnection.getText() );
        if ( dbInfo != null ) {
          Database db = new Database( loggingObject, variables, dbInfo );
          try {
            db.connect();
            String[] procs = db.getProcedures();
            if ( procs != null && procs.length > 0 ) {
              EnterSelectionDialog esd =
                new EnterSelectionDialog( shell, procs,
                  BaseMessages.getString( PKG, "DBProcDialog.EnterSelection.DialogTitle" ),
                  BaseMessages.getString( PKG, "DBProcDialog.EnterSelection.DialogMessage" ) );
              String proc = esd.open();
              if ( proc != null ) {
                wProcName.setText( proc );
              }
            } else {
              MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
              mb.setMessage( BaseMessages.getString( PKG, "DBProcDialog.NoProceduresFound.DialogMessage" ) );
              mb.setText( BaseMessages.getString( PKG, "DBProcDialog.NoProceduresFound.DialogTitle" ) );
              mb.open();
            }
          } catch ( HopDatabaseException dbe ) {
            new ErrorDialog( shell,
              BaseMessages.getString( PKG, "DBProcDialog.ErrorGettingProceduresList.DialogTitle" ),
              BaseMessages.getString( PKG, "DBProcDialog.ErrorGettingProceduresList.DialogMessage" ), dbe );
          } finally {
            db.disconnect();
          }
        }
      }
    } );

    Label wlProcName = new Label( shell, SWT.RIGHT );
    wlProcName.setText( BaseMessages.getString( PKG, "DBProcDialog.ProcedureName.Label" ) );
    props.setLook( wlProcName );
    FormData fdlProcName = new FormData();
    fdlProcName.left = new FormAttachment( 0, 0 );
    fdlProcName.right = new FormAttachment( middle, -margin );
    fdlProcName.top = new FormAttachment( wConnection, margin * 2 );
    wlProcName.setLayoutData( fdlProcName );

    wProcName = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wProcName );
    wProcName.addModifyListener( lsMod );
    FormData fdProcName = new FormData();
    fdProcName.left = new FormAttachment( middle, 0 );
    fdProcName.top = new FormAttachment( wConnection, margin * 2 );
    fdProcName.right = new FormAttachment( wbProcName, -margin );
    wProcName.setLayoutData( fdProcName );

    // AutoCommit line
    Label wlAutoCommit = new Label( shell, SWT.RIGHT );
    wlAutoCommit.setText( BaseMessages.getString( PKG, "DBProcDialog.AutoCommit.Label" ) );
    wlAutoCommit.setToolTipText( BaseMessages.getString( PKG, "DBProcDialog.AutoCommit.Tooltip" ) );
    props.setLook( wlAutoCommit );
    FormData fdlAutoCommit = new FormData();
    fdlAutoCommit.left = new FormAttachment( 0, 0 );
    fdlAutoCommit.top = new FormAttachment( wProcName, margin );
    fdlAutoCommit.right = new FormAttachment( middle, -margin );
    wlAutoCommit.setLayoutData( fdlAutoCommit );
    wAutoCommit = new Button( shell, SWT.CHECK );
    wAutoCommit.setToolTipText( BaseMessages.getString( PKG, "DBProcDialog.AutoCommit.Tooltip" ) );
    props.setLook( wAutoCommit );
    FormData fdAutoCommit = new FormData();
    fdAutoCommit.left = new FormAttachment( middle, 0 );
    fdAutoCommit.top = new FormAttachment( wlAutoCommit, 0, SWT.CENTER );
    fdAutoCommit.right = new FormAttachment( 100, 0 );
    wAutoCommit.setLayoutData( fdAutoCommit );
    wAutoCommit.addSelectionListener( lsSelMod );

    // Result line...
    Label wlResult = new Label( shell, SWT.RIGHT );
    wlResult.setText( BaseMessages.getString( PKG, "DBProcDialog.Result.Label" ) );
    props.setLook( wlResult );
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment( 0, 0 );
    fdlResult.right = new FormAttachment( middle, -margin );
    fdlResult.top = new FormAttachment( wAutoCommit, margin * 2 );
    wlResult.setLayoutData( fdlResult );
    wResult = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResult );
    wResult.addModifyListener( lsMod );
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, 0 );
    fdResult.top = new FormAttachment( wAutoCommit, margin * 2 );
    fdResult.right = new FormAttachment( 100, 0 );
    wResult.setLayoutData( fdResult );

    // ResultType line
    Label wlResultType = new Label( shell, SWT.RIGHT );
    wlResultType.setText( BaseMessages.getString( PKG, "DBProcDialog.ResultType.Label" ) );
    props.setLook( wlResultType );
    FormData fdlResultType = new FormData();
    fdlResultType.left = new FormAttachment( 0, 0 );
    fdlResultType.right = new FormAttachment( middle, -margin );
    fdlResultType.top = new FormAttachment( wResult, margin );
    wlResultType.setLayoutData( fdlResultType );
    wResultType = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wResultType );
    String[] types = ValueMetaFactory.getValueMetaNames();
    for ( String type : types ) {
      wResultType.add( type );
    }
    wResultType.select( 0 );
    wResultType.addModifyListener( lsMod );
    FormData fdResultType = new FormData();
    fdResultType.left = new FormAttachment( middle, 0 );
    fdResultType.top = new FormAttachment( wResult, margin );
    fdResultType.right = new FormAttachment( 100, 0 );
    wResultType.setLayoutData( fdResultType );

    Label wlFields = new Label( shell, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "DBProcDialog.Parameters.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wResultType, margin );
    wlFields.setLayoutData( fdlFields );

    final int nrRows = input.getArgument().length;

    fieldColumns = new ColumnInfo[] {
      new ColumnInfo(
        BaseMessages.getString( PKG, "DBProcDialog.ColumnInfo.Name" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "" }, false ),
      new ColumnInfo(
        BaseMessages.getString( PKG, "DBProcDialog.ColumnInfo.Direction" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "IN", "OUT", "INOUT" } ),
      new ColumnInfo(
        BaseMessages.getString( PKG, "DBProcDialog.ColumnInfo.Type" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        ValueMetaFactory.getValueMetaNames() ),
    };
    wFields = new TableView( variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, fieldColumns, nrRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wOk, -2 * margin );
    wFields.setLayoutData( fdFields );

    //
    // Search the fields in the background

    final Runnable runnable = () -> {
      TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
      if ( transformMeta != null ) {
        try {
          IRowMeta row = pipelineMeta.getPrevTransformFields( variables, transformMeta );

          // Remember these fields...
          for ( int i = 0; i < row.size(); i++ ) {
            inputFields.put( row.getValueMeta( i ).getName(), i );
          }
          setComboBoxes();
        } catch ( HopException e ) {
          logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
        }
      }
    };
    new Thread( runnable ).start();


    // Add listeners
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

    lsResize = event -> {
      Point size = shell.getSize();
      wFields.setSize( size.x - 10, size.y - 50 );
      wFields.table.setSize( size.x - 10, size.y - 50 );
      wFields.redraw();
    };
    shell.addListener( SWT.Resize, lsResize );

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

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[ entries.size() ] );

    Const.sortStrings( fieldNames );
    fieldColumns[ 0 ].setComboValues( fieldNames );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    int i;
    logDebug( BaseMessages.getString( PKG, "DBProcDialog.Log.GettingKeyInfo" ) );

    if ( input.getArgument() != null ) {
      for ( i = 0; i < input.getArgument().length; i++ ) {
        TableItem item = wFields.table.getItem( i );
        if ( input.getArgument()[ i ] != null ) {
          item.setText( 1, input.getArgument()[ i ] );
        }
        if ( input.getArgumentDirection()[ i ] != null ) {
          item.setText( 2, input.getArgumentDirection()[ i ] );
        }
        item.setText( 3, ValueMetaFactory.getValueMetaName( input.getArgumentType()[ i ] ) );
      }
    }

    if ( input.getDatabase() != null ) {
      wConnection.setText( input.getDatabase().getName() );
    }
    if ( input.getProcedure() != null ) {
      wProcName.setText( input.getProcedure() );
    }
    if ( input.getResultName() != null ) {
      wResult.setText( input.getResultName() );
    }
    wResultType.setText( ValueMetaFactory.getValueMetaName( input.getResultType() ) );

    wAutoCommit.setSelection( input.isAutoCommit() );

    wFields.setRowNums();
    wFields.optWidth( true );

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

    int i;

    int nrargs = wFields.nrNonEmpty();

    input.allocate( nrargs );

    logDebug( BaseMessages.getString( PKG, "DBProcDialog.Log.FoundArguments", String.valueOf( nrargs ) ) );
    //CHECKSTYLE:Indentation:OFF
    for ( i = 0; i < nrargs; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      input.getArgument()[ i ] = item.getText( 1 );
      input.getArgumentDirection()[ i ] = item.getText( 2 );
      input.getArgumentType()[ i ] = ValueMetaFactory.getIdForValueMeta( item.getText( 3 ) );
    }

    input.setDatabase( pipelineMeta.findDatabase( wConnection.getText() ) );
    input.setProcedure( wProcName.getText() );
    input.setResultName( wResult.getText() );
    input.setResultType( ValueMetaFactory.getIdForValueMeta( wResultType.getText() ) );
    input.setAutoCommit( wAutoCommit.getSelection() );

    transformName = wTransformName.getText(); // return value

    if ( input.getDatabase() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "DBProcDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "DBProcDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
    }

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null && !r.isEmpty() ) {
        ITableItemInsertListener listener = ( tableItem, v ) -> {
          tableItem.setText( 2, "IN" );
          return true;
        };
        BaseTransformDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] { 3 }, -1, -1, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "DBProcDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "DBProcDialog.FailedToGetFields.DialogMessage" ), ke );
    }

  }
}
