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

package org.apache.hop.pipeline.transforms.update;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.SqlStatement;
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
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SqlEditor;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

public class UpdateDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = UpdateMeta.class; // For Translator

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TableView wKey;

  private TextVar wSchema;

  private TextVar wTable;

  private TableView wReturn;

  private TextVar wCommit;

  private Button wBatch;

  private Label wlErrorIgnored;
  private Button wErrorIgnored;

  private Label wlIgnoreFlagField;
  private Text wIgnoreFlagField;

  private final UpdateMeta input;

  private ColumnInfo[] ciKey;

  private ColumnInfo[] ciReturn;

  private final Map<String, Integer> inputFields;

  private Button wSkipLookup;

  /**
   * List of ColumnInfo that should have the field names of the selected database table
   */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  public UpdateDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (UpdateMeta) in;
    inputFields = new HashMap<>();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    ModifyListener lsTableMod = arg0 -> {
      input.setChanged();
      setTableFieldCombo();
    };
    SelectionListener lsSelection = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setTableFieldCombo();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "UpdateDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "UpdateDialog.TransformName.Label" ) );
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
    wConnection.addSelectionListener( lsSelection );

    // Schema line...
    Label wlSchema = new Label(shell, SWT.RIGHT);
    wlSchema.setText( BaseMessages.getString( PKG, "UpdateDialog.TargetSchema.Label" ) );
    props.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment( 0, 0 );
    fdlSchema.right = new FormAttachment( middle, -margin );
    fdlSchema.top = new FormAttachment( wConnection, margin * 2 );
    wlSchema.setLayoutData(fdlSchema);

    Button wbSchema = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbSchema);
    wbSchema.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment( wConnection, 2 * margin );
    fdbSchema.right = new FormAttachment( 100, 0 );
    wbSchema.setLayoutData(fdbSchema);

    wSchema = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchema );
    wSchema.addModifyListener( lsTableMod );
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment( middle, 0 );
    fdSchema.top = new FormAttachment( wConnection, margin * 2 );
    fdSchema.right = new FormAttachment(wbSchema, -margin );
    wSchema.setLayoutData(fdSchema);

    // Table line...
    Label wlTable = new Label(shell, SWT.RIGHT);
    wlTable.setText( BaseMessages.getString( PKG, "UpdateDialog.TargetTable.Label" ) );
    props.setLook(wlTable);
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment( 0, 0 );
    fdlTable.right = new FormAttachment( middle, -margin );
    fdlTable.top = new FormAttachment(wbSchema, margin );
    wlTable.setLayoutData(fdlTable);

    Button wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTable);
    wbTable.setText( BaseMessages.getString( PKG, "UpdateDialog.Browse.Button" ) );
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment(wbSchema, margin );
    wbTable.setLayoutData(fdbTable);

    wTable = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTable );
    wTable.addModifyListener( lsTableMod );
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment( middle, 0 );
    fdTable.top = new FormAttachment(wbSchema, margin );
    fdTable.right = new FormAttachment(wbTable, -margin );
    wTable.setLayoutData(fdTable);

    // Commit line
    Label wlCommit = new Label(shell, SWT.RIGHT);
    wlCommit.setText( BaseMessages.getString( PKG, "UpdateDialog..Commit.Label" ) );
    props.setLook(wlCommit);
    FormData fdlCommit = new FormData();
    fdlCommit.left = new FormAttachment( 0, 0 );
    fdlCommit.top = new FormAttachment( wTable, margin );
    fdlCommit.right = new FormAttachment( middle, -margin );
    wlCommit.setLayoutData(fdlCommit);
    wCommit = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCommit );
    wCommit.addModifyListener( lsMod );
    FormData fdCommit = new FormData();
    fdCommit.left = new FormAttachment( middle, 0 );
    fdCommit.top = new FormAttachment( wTable, margin );
    fdCommit.right = new FormAttachment( 100, 0 );
    wCommit.setLayoutData(fdCommit);

    // Batch update
    Label wlBatch = new Label(shell, SWT.RIGHT);
    wlBatch.setText( BaseMessages.getString( PKG, "UpdateDialog.Batch.Label" ) );
    props.setLook(wlBatch);
    FormData fdlBatch = new FormData();
    fdlBatch.left = new FormAttachment( 0, 0 );
    fdlBatch.top = new FormAttachment( wCommit, margin );
    fdlBatch.right = new FormAttachment( middle, -margin );
    wlBatch.setLayoutData(fdlBatch);
    wBatch = new Button( shell, SWT.CHECK );
    props.setLook( wBatch );
    FormData fdBatch = new FormData();
    fdBatch.left = new FormAttachment( middle, 0 );
    fdBatch.top = new FormAttachment( wlBatch, 0, SWT.CENTER );
    fdBatch.right = new FormAttachment( 100, 0 );
    wBatch.setLayoutData(fdBatch);
    wBatch.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        setFlags();
        input.setChanged();
      }
    } );

    // UsePart update
    Label wlSkipLookup = new Label(shell, SWT.RIGHT);
    wlSkipLookup.setText( BaseMessages.getString( PKG, "UpdateDialog.SkipLookup.Label" ) );
    props.setLook(wlSkipLookup);
    FormData fdlSkipLookup = new FormData();
    fdlSkipLookup.left = new FormAttachment( 0, 0 );
    fdlSkipLookup.top = new FormAttachment( wBatch, margin );
    fdlSkipLookup.right = new FormAttachment( middle, -margin );
    wlSkipLookup.setLayoutData(fdlSkipLookup);
    wSkipLookup = new Button( shell, SWT.CHECK );
    wSkipLookup.setToolTipText( BaseMessages.getString( PKG, "UpdateDialog.SkipLookup.Tooltip" ) );
    props.setLook( wSkipLookup );
    FormData fdSkipLookup = new FormData();
    fdSkipLookup.left = new FormAttachment( middle, 0 );
    fdSkipLookup.top = new FormAttachment( wlSkipLookup, 0, SWT.CENTER );
    fdSkipLookup.right = new FormAttachment( 100, 0 );
    wSkipLookup.setLayoutData(fdSkipLookup);
    wSkipLookup.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setActiveIgnoreLookup();
      }
    } );

    wlErrorIgnored = new Label( shell, SWT.RIGHT );
    wlErrorIgnored.setText( BaseMessages.getString( PKG, "UpdateDialog.ErrorIgnored.Label" ) );
    props.setLook( wlErrorIgnored );
    FormData fdlErrorIgnored = new FormData();
    fdlErrorIgnored.left = new FormAttachment( 0, 0 );
    fdlErrorIgnored.top = new FormAttachment( wSkipLookup, margin );
    fdlErrorIgnored.right = new FormAttachment( middle, -margin );
    wlErrorIgnored.setLayoutData(fdlErrorIgnored);
    wErrorIgnored = new Button( shell, SWT.CHECK );
    props.setLook( wErrorIgnored );
    wErrorIgnored.setToolTipText( BaseMessages.getString( PKG, "UpdateDialog.ErrorIgnored.ToolTip" ) );
    FormData fdErrorIgnored = new FormData();
    fdErrorIgnored.left = new FormAttachment( middle, 0 );
    fdErrorIgnored.top = new FormAttachment( wlErrorIgnored, 0, SWT.CENTER );
    wErrorIgnored.setLayoutData(fdErrorIgnored);
    wErrorIgnored.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setFlags();
      }
    } );

    wlIgnoreFlagField = new Label( shell, SWT.LEFT );
    wlIgnoreFlagField.setText( BaseMessages.getString( PKG, "UpdateDialog.FlagField.Label" ) );
    props.setLook( wlIgnoreFlagField );
    FormData fdlIgnoreFlagField = new FormData();
    fdlIgnoreFlagField.left = new FormAttachment( wErrorIgnored, margin );
    fdlIgnoreFlagField.top = new FormAttachment( wSkipLookup, margin );
    wlIgnoreFlagField.setLayoutData(fdlIgnoreFlagField);
    wIgnoreFlagField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wIgnoreFlagField );
    wIgnoreFlagField.addModifyListener( lsMod );
    FormData fdIgnoreFlagField = new FormData();
    fdIgnoreFlagField.left = new FormAttachment( wlIgnoreFlagField, margin );
    fdIgnoreFlagField.top = new FormAttachment( wSkipLookup, margin );
    fdIgnoreFlagField.right = new FormAttachment( 100, 0 );
    wIgnoreFlagField.setLayoutData(fdIgnoreFlagField);

    Label wlKey = new Label(shell, SWT.NONE);
    wlKey.setText( BaseMessages.getString( PKG, "UpdateDialog.Key.Label" ) );
    props.setLook(wlKey);
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment( 0, 0 );
    fdlKey.top = new FormAttachment( wIgnoreFlagField, margin );
    wlKey.setLayoutData(fdlKey);

    int nrKeyCols = 4;
    int nrKeyRows = ( input.getKeyStream() != null ? input.getKeyStream().length : 1 );

    ciKey = new ColumnInfo[ nrKeyCols ];
    ciKey[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "UpdateDialog.ColumnInfo.TableField" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "" }, false );
    ciKey[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "UpdateDialog.ColumnInfo.Comparator" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "=", "= ~NULL", "<>", "<", "<=", ">", ">=", "LIKE", "BETWEEN", "IS NULL", "IS NOT NULL" } );
    ciKey[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "UpdateDialog.ColumnInfo.StreamField1" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "" }, false );
    ciKey[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "UpdateDialog.ColumnInfo.StreamField2" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "" }, false );
    tableFieldColumns.add( ciKey[ 0 ] );
    wKey =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKey,
        nrKeyRows, lsMod, props );

    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "UpdateDialog.GetFields.Button" ) );
    fdGet = new FormData();
    fdGet.right = new FormAttachment( 100, 0 );
    fdGet.top = new FormAttachment(wlKey, margin );
    wGet.setLayoutData( fdGet );

    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment( 0, 0 );
    fdKey.top = new FormAttachment(wlKey, margin );
    fdKey.right = new FormAttachment( wGet, -margin );
    fdKey.bottom = new FormAttachment(wlKey, 190 );
    wKey.setLayoutData(fdKey);

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wSql = new Button( shell, SWT.PUSH );
    wSql.setText( BaseMessages.getString( PKG, "UpdateDialog.SQL.Button" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wSql, wCancel }, margin, null );

    // THE UPDATE/INSERT TABLE
    Label wlReturn = new Label(shell, SWT.NONE);
    wlReturn.setText( BaseMessages.getString( PKG, "UpdateDialog.Return.Label" ) );
    props.setLook(wlReturn);
    FormData fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment( 0, 0 );
    fdlReturn.top = new FormAttachment( wKey, margin );
    wlReturn.setLayoutData(fdlReturn);

    int UpInsCols = 2;
    int UpInsRows = ( input.getUpdateLookup() != null ? input.getUpdateLookup().length : 1 );

    ciReturn = new ColumnInfo[ UpInsCols ];
    ciReturn[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "UpdateDialog.ColumnInfo.TableField" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "" }, false );
    ciReturn[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "UpdateDialog.ColumnInfo.StreamField" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "" }, false );
    tableFieldColumns.add( ciReturn[ 0 ] );
    wReturn =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciReturn,
        UpInsRows, lsMod, props );

    Button wGetLU = new Button(shell, SWT.PUSH);
    wGetLU.setText( BaseMessages.getString( PKG, "UpdateDialog.GetAndUpdateFields" ) );
    FormData fdGetLU = new FormData();
    fdGetLU.top = new FormAttachment(wlReturn, margin );
    fdGetLU.right = new FormAttachment( 100, 0 );
    wGetLU.setLayoutData(fdGetLU);

    FormData fdReturn = new FormData();
    fdReturn.left = new FormAttachment( 0, 0 );
    fdReturn.top = new FormAttachment(wlReturn, margin );
    fdReturn.right = new FormAttachment(wGetLU, -margin );
    fdReturn.bottom = new FormAttachment( wOk, -2 * margin );
    wReturn.setLayoutData(fdReturn);

    //
    // Search the fields in the background
    //

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
    lsOk = e -> ok();
    lsGet = e -> get();
    Listener lsGetLU = e -> getUpdate();
    lsSql = e -> create();
    lsCancel = e -> cancel();

    wOk.addListener( SWT.Selection, lsOk );
    wGet.addListener( SWT.Selection, lsGet );
    wGetLU.addListener( SWT.Selection, lsGetLU);
    wSql.addListener( SWT.Selection, lsSql );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wSchema.addSelectionListener( lsDef );
    wTable.addSelectionListener( lsDef );
    wCommit.addSelectionListener( lsDef );
    wIgnoreFlagField.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );
    wbSchema.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getSchemaNames();
      }
    } );
    wbTable.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getTableName();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();
    getData();
    setActiveIgnoreLookup();
    setTableFieldCombo();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  public void setActiveIgnoreLookup() {
    if ( wSkipLookup.getSelection() ) {
      wErrorIgnored.setSelection( false );
      wIgnoreFlagField.setText( "" );
    }
    wErrorIgnored.setEnabled( !wSkipLookup.getSelection() );
    wlErrorIgnored.setEnabled( !wSkipLookup.getSelection() );
    wlIgnoreFlagField.setEnabled( !wSkipLookup.getSelection() && wErrorIgnored.getSelection() );
    wIgnoreFlagField.setEnabled( !wSkipLookup.getSelection() && wErrorIgnored.getSelection() );

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
    // Key fields
    ciKey[ 2 ].setComboValues( fieldNames );
    ciKey[ 3 ].setComboValues( fieldNames );
    // return fields
    ciReturn[ 1 ].setComboValues( fieldNames );
  }

  public void setFlags() {
    wlIgnoreFlagField.setEnabled( wErrorIgnored.getSelection() );
    wIgnoreFlagField.setEnabled( wErrorIgnored.getSelection() );

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase( wConnection.getText() );
    boolean hasErrorHandling = pipelineMeta.findTransform( transformName ).isDoingErrorHandling();

    // Can't use batch yet when grabbing auto-generated keys...
    // Only enable batch option when not returning keys.
    // If we are on PostgreSQL (and look-a-likes), error handling is not supported. (PDI-366)
    //
    boolean enableBatch = wBatch.getSelection();
    enableBatch =
      enableBatch
        && !( databaseMeta != null && databaseMeta.supportsErrorHandlingOnBatchUpdates() && hasErrorHandling );
  }

  private void setTableFieldCombo() {
    Runnable fieldLoader = () -> {
      if ( !wTable.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed() ) {
        final String tableName = wTable.getText(), connectionName = wConnection.getText(), schemaName =
          wSchema.getText();

        // clear
        for ( ColumnInfo colInfo : tableFieldColumns ) {
          colInfo.setComboValues( new String[] {} );
        }
        if ( !Utils.isEmpty( tableName ) ) {
          DatabaseMeta databaseMeta = pipelineMeta.findDatabase( connectionName );
          if ( databaseMeta != null ) {
            Database db = new Database( loggingObject, variables, databaseMeta );
            try {
              db.connect();

              IRowMeta r =
                db.getTableFieldsMeta(
                  variables.resolve( schemaName ),
                  variables.resolve( tableName ) );
              if ( null != r ) {
                String[] fieldNames = r.getFieldNames();
                if ( null != fieldNames ) {
                  for ( ColumnInfo colInfo : tableFieldColumns ) {
                    colInfo.setComboValues( fieldNames );
                  }
                }
              }
            } catch ( Exception e ) {
              for ( ColumnInfo colInfo : tableFieldColumns ) {
                colInfo.setComboValues( new String[] {} );
              }
              // ignore any errors here. drop downs will not be
              // filled, but no problem for the user
            } finally {
              try {
                if ( db != null ) {
                  db.disconnect();
                }
              } catch ( Exception ignored ) {
                // ignore any errors here.
                db = null;
              }
            }
          }
        }
      }
    };
    shell.getDisplay().asyncExec( fieldLoader );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "UpdateDialog.Log.GettingKeyInfo" ) );
    }

    wCommit.setText( input.getCommitSizeVar() );
    wBatch.setSelection( input.useBatchUpdate() );
    wSkipLookup.setSelection( input.isSkipLookup() );
    wErrorIgnored.setSelection( input.isErrorIgnored() );
    if ( input.getIgnoreFlagField() != null ) {
      wIgnoreFlagField.setText( input.getIgnoreFlagField() );
    }

    if ( input.getKeyStream() != null ) {
      for ( int i = 0; i < input.getKeyStream().length; i++ ) {
        TableItem item = wKey.table.getItem( i );
        if ( input.getKeyLookup()[ i ] != null ) {
          item.setText( 1, input.getKeyLookup()[ i ] );
        }
        if ( input.getKeyCondition()[ i ] != null ) {
          item.setText( 2, input.getKeyCondition()[ i ] );
        }
        if ( input.getKeyStream()[ i ] != null ) {
          item.setText( 3, input.getKeyStream()[ i ] );
        }
        if ( input.getKeyStream2()[ i ] != null ) {
          item.setText( 4, input.getKeyStream2()[ i ] );
        }
      }
    }

    if ( input.getUpdateLookup() != null ) {
      for ( int i = 0; i < input.getUpdateLookup().length; i++ ) {
        TableItem item = wReturn.table.getItem( i );
        if ( input.getUpdateLookup()[ i ] != null ) {
          item.setText( 1, input.getUpdateLookup()[ i ] );
        }
        if ( input.getUpdateStream()[ i ] != null ) {
          item.setText( 2, input.getUpdateStream()[ i ] );
        }
      }
    }

    if ( input.getSchemaName() != null ) {
      wSchema.setText( input.getSchemaName() );
    }
    if ( input.getTableName() != null ) {
      wTable.setText( input.getTableName() );
    }
    if ( input.getDatabaseMeta() != null ) {
      wConnection.setText( input.getDatabaseMeta().getName() );
    }

    wKey.setRowNums();
    wKey.optWidth( true );
    wReturn.setRowNums();
    wReturn.optWidth( true );

    setFlags();

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void getInfo( UpdateMeta inf ) {
    // Table ktable = wKey.table;
    int nrkeys = wKey.nrNonEmpty();
    int nrFields = wReturn.nrNonEmpty();

    inf.allocate( nrkeys, nrFields );

    inf.setCommitSize( wCommit.getText() );
    inf.setUseBatchUpdate( wBatch.getSelection() );
    inf.setSkipLookup( wSkipLookup.getSelection() );

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "UpdateDialog.Log.FoundKeys", nrkeys + "" ) );
    }
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrkeys; i++ ) {
      TableItem item = wKey.getNonEmpty( i );
      inf.getKeyLookup()[ i ] = item.getText( 1 );
      inf.getKeyCondition()[ i ] = item.getText( 2 );
      inf.getKeyStream()[ i ] = item.getText( 3 );
      inf.getKeyStream2()[ i ] = item.getText( 4 );
    }

    // Table ftable = wReturn.table;

    logDebug( BaseMessages.getString( PKG, "UpdateDialog.Log.FoundFields", nrFields + "" ) );
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wReturn.getNonEmpty( i );
      inf.getUpdateLookup()[ i ] = item.getText( 1 );
      inf.getUpdateStream()[ i ] = item.getText( 2 );
    }

    inf.setSchemaName( wSchema.getText() );
    inf.setTableName( wTable.getText() );
    inf.setDatabaseMeta( pipelineMeta.findDatabase( wConnection.getText() ) );

    inf.setErrorIgnored( wErrorIgnored.getSelection() );
    inf.setIgnoreFlagField( wIgnoreFlagField.getText() );

    transformName = wTransformName.getText(); // return value
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    // Get the information for the dialog into the input structure.
    getInfo( input );

    if ( input.getDatabaseMeta() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "UpdateDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "UpdateDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
      return;
    }

    dispose();
  }

  private void getTableName() {
    String connectionName = wConnection.getText();
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase( connectionName );
    if ( databaseMeta != null ) {
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "UpdateDialog.Log.LookingAtConnection" ) + databaseMeta.toString() );
      }

      DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases() );
      std.setSelectedSchemaAndTable( wSchema.getText(), wTable.getText() );
      if ( std.open() ) {
        wSchema.setText( Const.NVL( std.getSchemaName(), "" ) );
        wTable.setText( Const.NVL( std.getTableName(), "" ) );
        setTableFieldCombo();
      }
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "UpdateDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "UpdateDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
    }
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null && !r.isEmpty() ) {
        ITableItemInsertListener listener = ( tableItem, v ) -> {
          tableItem.setText( 2, "=" );
          return true;
        };
        BaseTransformDialog.getFieldsFromPrevious( r, wKey, 1, new int[] { 1, 3 }, new int[] {}, -1, -1, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "UpdateDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "UpdateDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  private void getUpdate() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null && !r.isEmpty() ) {
        BaseTransformDialog.getFieldsFromPrevious( r, wReturn, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, null );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "UpdateDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "UpdateDialog.FailedToGetFields.DialogMessage" ), ke );
    }

  }

  // Generate code for create table...
  // Conversions done by Database
  private void create() {
    try {
      UpdateMeta info = new UpdateMeta();
      getInfo( info );

      String name = transformName; // new name might not yet be linked to other transforms!
      TransformMeta transforminfo = new TransformMeta( BaseMessages.getString( PKG, "UpdateDialog.TransformMeta.Title" ), name, info );
      IRowMeta prev = pipelineMeta.getPrevTransformFields( variables, transformName );

      SqlStatement sql = info.getSqlStatements( variables, pipelineMeta, transforminfo, prev, metadataProvider );
      if ( !sql.hasError() ) {
        if ( sql.hasSql() ) {
          SqlEditor sqledit =
            new SqlEditor( shell, SWT.NONE, variables,  info.getDatabaseMeta(), DbCache.getInstance(), sql
              .getSql() );
          sqledit.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "UpdateDialog.NoSQLNeeds.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "UpdateDialog.NoSQLNeeds.DialogTitle" ) );
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( sql.getError() );
        mb.setText( BaseMessages.getString( PKG, "UpdateDialog.SQLError.DialogTitle" ) );
        mb.open();
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "UpdateDialog.CouldNotBuildSQL.DialogTitle" ), BaseMessages
        .getString( PKG, "UpdateDialog.CouldNotBuildSQL.DialogMessage" ), ke );
    }

  }

  private void getSchemaNames() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase( wConnection.getText() );
    if ( databaseMeta != null ) {
      Database database = new Database( loggingObject, variables, databaseMeta );
      try {
        database.connect();
        String[] schemas = database.getSchemas();

        if ( null != schemas && schemas.length > 0 ) {
          schemas = Const.sortStrings( schemas );
          EnterSelectionDialog dialog = new EnterSelectionDialog( shell, schemas,
            BaseMessages.getString( PKG, "UpdateDialog.AvailableSchemas.Title", wConnection.getText() ),
            BaseMessages.getString( PKG, "UpdateDialog.AvailableSchemas.Message", wConnection.getText() ) );
          String d = dialog.open();
          if ( d != null ) {
            wSchema.setText( Const.NVL( d, "" ) );
            setTableFieldCombo();
          }

        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "UpdateDialog.NoSchema.Error" ) );
          mb.setText( BaseMessages.getString( PKG, "UpdateDialog.GetSchemas.Error" ) );
          mb.open();
        }
      } catch ( Exception e ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "System.Dialog.Error.Title" ),
          BaseMessages.getString( PKG, "UpdateDialog.ErrorGettingSchemas" ), e );
      } finally {
        database.disconnect();
      }
    }
  }
}
