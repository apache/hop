/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.pgbulkloader;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SqlEditor;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

public class PGBulkLoaderDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = PGBulkLoaderMeta.class; // for i18n purposes, needed by Translator!!

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wSchema;

  private TextVar wTable;

  private CCombo wLoadAction;

  private TableView wReturn;

  private TextVar wEnclosure;

  private TextVar wDelimiter;

  private TextVar wDbNameOverride;

  private Button wStopOnError;

  private final PGBulkLoaderMeta input;

  private static final String[] ALL_FILETYPES = new String[] { BaseMessages.getString(
    PKG, "PGBulkLoaderDialog.Filetype.All" ) };

  private ColumnInfo[] ciReturn;

  private final Map<String, Integer> inputFields;

  /**
   * List of ColumnInfo that should have the field names of the selected database table
   */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  public PGBulkLoaderDialog( Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (PGBulkLoaderMeta) in;
    inputFields = new HashMap<>();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    FocusListener lsFocusLost = new FocusAdapter() {
      public void focusLost( FocusEvent arg0 ) {
        setTableFieldCombo();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.TransformName.Label" ) );
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

    // Schema line...
    Label wlSchema = new Label(shell, SWT.RIGHT);
    wlSchema.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.TargetSchema.Label" ) );
    props.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment( 0, 0 );
    fdlSchema.right = new FormAttachment( middle, -margin );
    fdlSchema.top = new FormAttachment( wConnection, margin * 2 );
    wlSchema.setLayoutData(fdlSchema);

    wSchema = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchema );
    wSchema.addModifyListener( lsMod );
    wSchema.addFocusListener( lsFocusLost );
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment( middle, 0 );
    fdSchema.top = new FormAttachment( wConnection, margin * 2 );
    fdSchema.right = new FormAttachment( 100, 0 );
    wSchema.setLayoutData(fdSchema);

    // Table line...
    Label wlTable = new Label(shell, SWT.RIGHT);
    wlTable.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.TargetTable.Label" ) );
    props.setLook(wlTable);
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment( 0, 0 );
    fdlTable.right = new FormAttachment( middle, -margin );
    fdlTable.top = new FormAttachment( wSchema, margin );
    wlTable.setLayoutData(fdlTable);

    Button wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTable);
    wbTable.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.Browse.Button" ) );
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment( wSchema, margin );
    wbTable.setLayoutData(fdbTable);
    wTable = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTable );
    wTable.addModifyListener( lsMod );
    wTable.addFocusListener( lsFocusLost );
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment( middle, 0 );
    fdTable.top = new FormAttachment( wSchema, margin );
    fdTable.right = new FormAttachment(wbTable, -margin );
    wTable.setLayoutData(fdTable);

    // Load Action line
    Label wlLoadAction = new Label(shell, SWT.RIGHT);
    wlLoadAction.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.LoadAction.Label" ) );
    props.setLook(wlLoadAction);
    FormData fdlLoadAction = new FormData();
    fdlLoadAction.left = new FormAttachment( 0, 0 );
    fdlLoadAction.right = new FormAttachment( middle, -margin );
    fdlLoadAction.top = new FormAttachment( wTable, margin );
    wlLoadAction.setLayoutData(fdlLoadAction);
    wLoadAction = new CCombo( shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wLoadAction.add( BaseMessages.getString( PKG, "PGBulkLoaderDialog.InsertLoadAction.Label" ) );
    wLoadAction.add( BaseMessages.getString( PKG, "PGBulkLoaderDialog.TruncateLoadAction.Label" ) );

    wLoadAction.select( 0 ); // +1: starts at -1
    wLoadAction.addModifyListener( lsMod );

    props.setLook( wLoadAction );
    FormData fdLoadAction = new FormData();
    fdLoadAction.left = new FormAttachment( middle, 0 );
    fdLoadAction.top = new FormAttachment( wTable, margin );
    fdLoadAction.right = new FormAttachment( 100, 0 );
    wLoadAction.setLayoutData(fdLoadAction);

    fdLoadAction = new FormData();
    fdLoadAction.left = new FormAttachment( middle, 0 );
    fdLoadAction.top = new FormAttachment( wTable, margin );
    fdLoadAction.right = new FormAttachment( 100, 0 );
    wLoadAction.setLayoutData(fdLoadAction);

    // Db Name Override line
    Label wlDbNameOverride = new Label(shell, SWT.RIGHT);
    wlDbNameOverride.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.DbNameOverride.Label" ) );
    props.setLook(wlDbNameOverride);
    FormData fdlDbNameOverride = new FormData();
    fdlDbNameOverride.left = new FormAttachment( 0, 0 );
    fdlDbNameOverride.top = new FormAttachment( wLoadAction, margin );
    fdlDbNameOverride.right = new FormAttachment( middle, -margin );
    wlDbNameOverride.setLayoutData(fdlDbNameOverride);
    wDbNameOverride = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDbNameOverride );
    wDbNameOverride.addModifyListener( lsMod );
    FormData fdDbNameOverride = new FormData();
    fdDbNameOverride.left = new FormAttachment( middle, 0 );
    fdDbNameOverride.top = new FormAttachment( wLoadAction, margin );
    fdDbNameOverride.right = new FormAttachment( 100, 0 );
    wDbNameOverride.setLayoutData(fdDbNameOverride);

    // Enclosure line
    Label wlEnclosure = new Label(shell, SWT.RIGHT);
    wlEnclosure.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.Enclosure.Label" ) );
    props.setLook(wlEnclosure);
    FormData fdlEnclosure = new FormData();
    fdlEnclosure.left = new FormAttachment( 0, 0 );
    fdlEnclosure.top = new FormAttachment( wDbNameOverride, margin );
    fdlEnclosure.right = new FormAttachment( middle, -margin );
    wlEnclosure.setLayoutData(fdlEnclosure);
    wEnclosure = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEnclosure );
    wEnclosure.addModifyListener( lsMod );
    FormData fdEnclosure = new FormData();
    fdEnclosure.left = new FormAttachment( middle, 0 );
    fdEnclosure.top = new FormAttachment( wDbNameOverride, margin );
    fdEnclosure.right = new FormAttachment( 100, 0 );
    wEnclosure.setLayoutData(fdEnclosure);

    // Delimiter
    Label wlDelimiter = new Label(shell, SWT.RIGHT);
    wlDelimiter.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.Delimiter.Label" ) );
    props.setLook(wlDelimiter);
    FormData fdlDelimiter = new FormData();
    fdlDelimiter.left = new FormAttachment( 0, 0 );
    fdlDelimiter.top = new FormAttachment( wEnclosure, margin );
    fdlDelimiter.right = new FormAttachment( middle, -margin );
    wlDelimiter.setLayoutData(fdlDelimiter);
    wDelimiter = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDelimiter );
    wDelimiter.addModifyListener( lsMod );
    FormData fdDelimiter = new FormData();
    fdDelimiter.left = new FormAttachment( middle, 0 );
    fdDelimiter.top = new FormAttachment( wEnclosure, margin );
    fdDelimiter.right = new FormAttachment( 100, 0 );
    wDelimiter.setLayoutData(fdDelimiter);

    // Stop on Error line
    Label wlStopOnError = new Label(shell, SWT.RIGHT);
    wlStopOnError.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.StopOnError.Label" ) );
    props.setLook(wlStopOnError);
    FormData fdlStopOnError = new FormData();
    fdlStopOnError.left = new FormAttachment( 0, 0 );
    fdlStopOnError.top = new FormAttachment( wDelimiter, margin );
    fdlStopOnError.right = new FormAttachment( middle, -margin );
    wlStopOnError.setLayoutData(fdlStopOnError);
    wStopOnError = new Button( shell, SWT.CHECK );
    props.setLook( wStopOnError );
    FormData fdStopOnError = new FormData();
    fdStopOnError.left = new FormAttachment( middle, 0 );
    fdStopOnError.top = new FormAttachment( wlStopOnError, 0, SWT.CENTER );
    fdStopOnError.right = new FormAttachment( 100, 0 );
    wStopOnError.setLayoutData(fdStopOnError);

    wStopOnError.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wSql = new Button( shell, SWT.PUSH );
    wSql.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.SQL.Button" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    setButtonPositions( new Button[] { wOk, wSql, wCancel }, margin, null );

    // The field Table
    Label wlReturn = new Label(shell, SWT.NONE);
    wlReturn.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.Fields.Label" ) );
    props.setLook(wlReturn);
    FormData fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment( 0, 0 );
    fdlReturn.top = new FormAttachment( wStopOnError, margin );
    wlReturn.setLayoutData(fdlReturn);

    int UpInsCols = 3;
    int UpInsRows = ( input.getFieldTable() != null ? input.getFieldTable().length : 1 );

    ciReturn = new ColumnInfo[ UpInsCols ];
    ciReturn[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PGBulkLoaderDialog.ColumnInfo.TableField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciReturn[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PGBulkLoaderDialog.ColumnInfo.StreamField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciReturn[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PGBulkLoaderDialog.ColumnInfo.DateMask" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {
        "", BaseMessages.getString( PKG, "PGBulkLoaderDialog.PassThrough.Label" ),
        BaseMessages.getString( PKG, "PGBulkLoaderDialog.DateMask.Label" ),
        BaseMessages.getString( PKG, "PGBulkLoaderDialog.DateTimeMask.Label" ) }, true );
    tableFieldColumns.add( ciReturn[ 0 ] );
    wReturn =
      new TableView(
        pipelineMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciReturn,
        UpInsRows, lsMod, props );

    Button wGetLU = new Button(shell, SWT.PUSH);
    wGetLU.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.GetFields.Label" ) );
    FormData fdGetLU = new FormData();
    fdGetLU.top = new FormAttachment(wlReturn, margin );
    fdGetLU.right = new FormAttachment( 100, 0 );
    wGetLU.setLayoutData(fdGetLU);

    Button wDoMapping = new Button(shell, SWT.PUSH);
    wDoMapping.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.EditMapping.Label" ) );
    FormData fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment(wGetLU, margin );
    fdDoMapping.right = new FormAttachment( 100, 0 );
    wDoMapping.setLayoutData(fdDoMapping);

    wDoMapping.addListener( SWT.Selection, arg0 -> generateMappings() );

    FormData fdReturn = new FormData();
    fdReturn.left = new FormAttachment( 0, 0 );
    fdReturn.top = new FormAttachment(wlReturn, margin );
    fdReturn.right = new FormAttachment(wDoMapping, -margin ); // fix margin error
    fdReturn.bottom = new FormAttachment( wOk, -2 * margin );
    wReturn.setLayoutData(fdReturn);

    //
    // Search the fields in the background
    //

    final Runnable runnable = () -> {
      TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
      if ( transformMeta != null ) {
        try {
          IRowMeta row = pipelineMeta.getPrevTransformFields( transformMeta );

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
    Listener lsGetLU = e -> getUpdate();
    lsSql = e -> create();
    lsCancel = e -> cancel();

    wOk.addListener( SWT.Selection, lsOk );
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
    wDbNameOverride.addSelectionListener( lsDef );
    wEnclosure.addSelectionListener( lsDef );
    wDelimiter.addSelectionListener( lsDef );
    wStopOnError.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
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

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    logDebug( BaseMessages.getString( PKG, "PGBulkLoaderDialog.Log.GettingKeyInfo" ) );

    if ( input.getFieldTable() != null ) {
      for ( int i = 0; i < input.getFieldTable().length; i++ ) {
        TableItem item = wReturn.table.getItem( i );
        if ( input.getFieldTable()[ i ] != null ) {
          item.setText( 1, input.getFieldTable()[ i ] );
        }
        if ( input.getFieldStream()[ i ] != null ) {
          item.setText( 2, input.getFieldStream()[ i ] );
        }
        String dateMask = input.getDateMask()[ i ];
        if ( dateMask != null ) {
          if ( PGBulkLoaderMeta.DATE_MASK_PASS_THROUGH.equals( dateMask ) ) {
            item.setText( 3, BaseMessages.getString( PKG, "PGBulkLoaderDialog.PassThrough.Label" ) );
          } else if ( PGBulkLoaderMeta.DATE_MASK_DATE.equals( dateMask ) ) {
            item.setText( 3, BaseMessages.getString( PKG, "PGBulkLoaderDialog.DateMask.Label" ) );
          } else if ( PGBulkLoaderMeta.DATE_MASK_DATETIME.equals( dateMask ) ) {
            item.setText( 3, BaseMessages.getString( PKG, "PGBulkLoaderDialog.DateTimeMask.Label" ) );
          } else {
            item.setText( 3, "" );
          }
        } else {
          item.setText( 3, "" );
        }
      }
    }

    if ( input.getDatabaseMeta() != null ) {
      wConnection.setText( input.getDatabaseMeta().getName() );
    }
    if ( input.getSchemaName() != null ) {
      wSchema.setText( input.getSchemaName() );
    }
    if ( input.getTableName() != null ) {
      wTable.setText( input.getTableName() );
    }
    if ( input.getDelimiter() != null ) {
      wDelimiter.setText( input.getDelimiter() );
    }
    if ( input.getEnclosure() != null ) {
      wEnclosure.setText( input.getEnclosure() );
    }
    wStopOnError.setSelection( input.isStopOnError() );
    if ( input.getDbNameOverride() != null ) {
      wDbNameOverride.setText( input.getDbNameOverride() );
    }

    String action = input.getLoadAction();
    if ( PGBulkLoaderMeta.ACTION_INSERT.equals( action ) ) {
      wLoadAction.select( 0 );
    } else if ( PGBulkLoaderMeta.ACTION_TRUNCATE.equals( action ) ) {
      wLoadAction.select( 1 );
    } else {
      logDebug( "Internal error: load_action set to default 'insert'" );
      wLoadAction.select( 0 );
    }

    wReturn.setRowNums();
    wReturn.optWidth( true );

    wTransformName.selectAll();
    wTransformName.setFocus();
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
    // return fields
    ciReturn[ 1 ].setComboValues( fieldNames );

  }

  /**
   * Reads in the fields from the previous transforms and from the ONE next transform and opens an EnterMappingDialog with this
   * information. After the user did the mapping, those information is put into the Select/Rename table.
   */
  private void generateMappings() {

    // Determine the source and target fields...
    //
    IRowMeta sourceFields;
    IRowMeta targetFields;

    try {
      sourceFields = pipelineMeta.getPrevTransformFields( transformMeta );
    } catch ( HopException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "PGBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Title" ),
        BaseMessages.getString( PKG, "PGBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Message" ), e );
      return;
    }
    // refresh data
    input.setDatabaseMeta( pipelineMeta.findDatabase( wConnection.getText() ) );
    input.setTableName( pipelineMeta.environmentSubstitute( wTable.getText() ) );
    ITransformMeta transformMetaInterface = transformMeta.getTransform();
    try {
      targetFields = transformMetaInterface.getRequiredFields( pipelineMeta );
    } catch ( HopException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "PGBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Title" ),
        BaseMessages.getString( PKG, "PGBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Message" ), e );
      return;
    }

    String[] inputNames = new String[ sourceFields.size() ];
    for ( int i = 0; i < sourceFields.size(); i++ ) {
      IValueMeta value = sourceFields.getValueMeta( i );
      inputNames[ i ] = value.getName() + EnterMappingDialog.STRING_ORIGIN_SEPARATOR + value.getOrigin() + ")";
    }

    // Create the existing mapping list...
    //
    List<SourceToTargetMapping> mappings = new ArrayList<>();
    StringBuilder missingSourceFields = new StringBuilder();
    StringBuilder missingTargetFields = new StringBuilder();

    int nrFields = wReturn.nrNonEmpty();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wReturn.getNonEmpty( i );
      String source = item.getText( 2 );
      String target = item.getText( 1 );

      int sourceIndex = sourceFields.indexOfValue( source );
      if ( sourceIndex < 0 ) {
        missingSourceFields.append( Const.CR ).append( "   " ).append( source ).append( " --> " ).append( target );
      }
      int targetIndex = targetFields.indexOfValue( target );
      if ( targetIndex < 0 ) {
        missingTargetFields.append( Const.CR ).append( "   " ).append( source ).append( " --> " ).append( target );
      }
      if ( sourceIndex < 0 || targetIndex < 0 ) {
        continue;
      }

      SourceToTargetMapping mapping = new SourceToTargetMapping( sourceIndex, targetIndex );
      mappings.add( mapping );
    }

    // show a confirm dialog if some missing field was found
    //
    if ( missingSourceFields.length() > 0 || missingTargetFields.length() > 0 ) {

      String message = "";
      if ( missingSourceFields.length() > 0 ) {
        message +=
          BaseMessages.getString(
            PKG, "PGBulkLoaderDialog.DoMapping.SomeSourceFieldsNotFound", missingSourceFields.toString() )
            + Const.CR;
      }
      if ( missingTargetFields.length() > 0 ) {
        message +=
          BaseMessages.getString(
            PKG, "PGBulkLoaderDialog.DoMapping.SomeTargetFieldsNotFound", missingSourceFields.toString() )
            + Const.CR;
      }
      message += Const.CR;
      message +=
        BaseMessages.getString( PKG, "PGBulkLoaderDialog.DoMapping.SomeFieldsNotFoundContinue" ) + Const.CR;
      MessageDialog.setDefaultImage( GuiResource.getInstance().getImageHopUi() );
      boolean goOn =
        MessageDialog.openConfirm( shell, BaseMessages.getString(
          PKG, "PGBulkLoaderDialog.DoMapping.SomeFieldsNotFoundTitle" ), message );
      if ( !goOn ) {
        return;
      }
    }
    EnterMappingDialog d =
      new EnterMappingDialog( PGBulkLoaderDialog.this.shell, sourceFields.getFieldNames(), targetFields
        .getFieldNames(), mappings );
    mappings = d.open();

    // mappings == null if the user pressed cancel
    //
    if ( mappings != null ) {
      // Clear and re-populate!
      //
      wReturn.table.removeAll();
      wReturn.table.setItemCount( mappings.size() );
      for ( int i = 0; i < mappings.size(); i++ ) {
        SourceToTargetMapping mapping = mappings.get( i );
        TableItem item = wReturn.table.getItem( i );
        item.setText( 2, sourceFields.getValueMeta( mapping.getSourcePosition() ).getName() );
        item.setText( 1, targetFields.getValueMeta( mapping.getTargetPosition() ).getName() );
      }
      wReturn.setRowNums();
      wReturn.optWidth( true );
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void getInfo( PGBulkLoaderMeta inf ) {
    int nrFields = wReturn.nrNonEmpty();

    inf.allocate( nrFields );

    inf.setDbNameOverride( wDbNameOverride.getText() );

    logDebug( BaseMessages.getString( PKG, "PGBulkLoaderDialog.Log.FoundFields", "" + nrFields ) );
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wReturn.getNonEmpty( i );
      inf.getFieldTable()[ i ] = item.getText( 1 );
      inf.getFieldStream()[ i ] = item.getText( 2 );
      if ( BaseMessages.getString( PKG, "PGBulkLoaderDialog.PassThrough.Label" ).equals( item.getText( 3 ) ) ) {
        inf.getDateMask()[ i ] = PGBulkLoaderMeta.DATE_MASK_PASS_THROUGH;
      } else if ( BaseMessages.getString( PKG, "PGBulkLoaderDialog.DateMask.Label" ).equals( item.getText( 3 ) ) ) {
        inf.getDateMask()[ i ] = PGBulkLoaderMeta.DATE_MASK_DATE;
      } else if ( BaseMessages
        .getString( PKG, "PGBulkLoaderDialog.DateTimeMask.Label" ).equals( item.getText( 3 ) ) ) {
        inf.getDateMask()[ i ] = PGBulkLoaderMeta.DATE_MASK_DATETIME;
      } else {
        inf.getDateMask()[ i ] = "";
      }
    }

    inf.setSchemaName( wSchema.getText() );
    inf.setTableName( wTable.getText() );
    inf.setDatabaseMeta( pipelineMeta.findDatabase( wConnection.getText() ) );
    inf.setDelimiter( wDelimiter.getText() );
    inf.setEnclosure( wEnclosure.getText() );
    inf.setStopOnError( wStopOnError.getSelection() );

    /*
     * /* Set the loadaction
     */
    String action = wLoadAction.getText();
    if ( BaseMessages.getString( PKG, "PGBulkLoaderDialog.InsertLoadAction.Label" ).equals( action ) ) {
      inf.setLoadAction( PGBulkLoaderMeta.ACTION_INSERT );
    } else if ( BaseMessages.getString( PKG, "PGBulkLoaderDialog.TruncateLoadAction.Label" ).equals( action ) ) {
      inf.setLoadAction( PGBulkLoaderMeta.ACTION_TRUNCATE );
    } else {
      logDebug( "Internal error: load_action set to default 'insert', value found '" + action + "'." );
      inf.setLoadAction( PGBulkLoaderMeta.ACTION_INSERT );
    }

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
      mb.setMessage( BaseMessages.getString( PKG, "PGBulkLoaderDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
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
      logDebug( BaseMessages.getString( PKG, "PGBulkLoaderDialog.Log.LookingAtConnection" ) + databaseMeta.toString() );

      DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, databaseMeta, pipelineMeta.getDatabases() );
      std.setSelectedSchemaAndTable( wSchema.getText(), wTable.getText() );
      if ( std.open() ) {
        wSchema.setText( Const.NVL( std.getSchemaName(), "" ) );
        wTable.setText( Const.NVL( std.getTableName(), "" ) );
      }
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "PGBulkLoaderDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
    }
  }

  private void getUpdate() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
      if ( r != null ) {
        ITableItemInsertListener listener = ( tableItem, v ) -> {
          if ( v.getType() == IValueMeta.TYPE_DATE ) {
            // The default is date mask.
            tableItem.setText( 3, BaseMessages.getString( PKG, "PGBulkLoaderDialog.DateMask.Label" ) );
          } else {
            tableItem.setText( 3, "" );
          }
          return true;
        };
        BaseTransformDialog.getFieldsFromPrevious( r, wReturn, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "PGBulkLoaderDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "PGBulkLoaderDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  // Generate code for create table...
  // Conversions done by Database
  private void create() {
    try {
      PGBulkLoaderMeta info = new PGBulkLoaderMeta();
      getInfo( info );

      String name = transformName; // new name might not yet be linked to other transforms!
      TransformMeta transformMeta =
        new TransformMeta( BaseMessages.getString( PKG, "PGBulkLoaderDialog.TransformMeta.Title" ), name, info );
      IRowMeta prev = pipelineMeta.getPrevTransformFields( transformName );

      SqlStatement sql = info.getSqlStatements( pipelineMeta, transformMeta, prev, metadataProvider );
      if ( !sql.hasError() ) {
        if ( sql.hasSql() ) {
          SqlEditor sqledit =
            new SqlEditor( pipelineMeta, shell, SWT.NONE, info.getDatabaseMeta(), DbCache.getInstance(), sql
              .getSql() );
          sqledit.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "PGBulkLoaderDialog.NoSQLNeeds.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.NoSQLNeeds.DialogTitle" ) );
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( sql.getError() );
        mb.setText( BaseMessages.getString( PKG, "PGBulkLoaderDialog.SQLError.DialogTitle" ) );
        mb.open();
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "PGBulkLoaderDialog.CouldNotBuildSQL.DialogTitle" ), BaseMessages
        .getString( PKG, "PGBulkLoaderDialog.CouldNotBuildSQL.DialogMessage" ), ke );
    }

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
          DatabaseMeta ci = pipelineMeta.findDatabase( connectionName );
          if ( ci != null ) {
            Database db = new Database( loggingObject, ci );
            try {
              db.connect();

              String schemaTable =
                ci.getQuotedSchemaTableCombination( pipelineMeta.environmentSubstitute( schemaName ), pipelineMeta
                  .environmentSubstitute( tableName ) );
              IRowMeta r = db.getTableFields( schemaTable );
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
}
