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

package org.apache.hop.ui.pipeline.transforms.mysqlbulkloader;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.SourceToTargetMapping;
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
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.mysqlbulkloader.MySQLBulkLoaderMeta;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SQLEditor;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.TableItemInsertListener;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
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
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Dialog class for the MySQL bulk loader transform.
 */
public class MySQLBulkLoaderDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = MySQLBulkLoaderMeta.class; // for i18n purposes, needed by Translator!!

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Label wlSchema;
  private TextVar wSchema;
  private FormData fdlSchema, fdSchema;

  private Label wlTable;
  private Button wbTable;
  private TextVar wTable;
  private FormData fdlTable, fdbTable, fdTable;

  private Label wlFifoFile;
  private TextVar wFifoFile;
  private FormData fdlFifoFile, fdFifoFile;

  private Label wlReplace;
  private Button wReplace;
  private FormData fdlReplace, fdReplace;

  private Label wlIgnore;
  private Button wIgnore;
  private FormData fdlIgnore, fdIgnore;

  private Label wlLocal;
  private Button wLocal;
  private FormData fdlLocal, fdLocal;

  private Label wlDelimiter;
  private Button wbDelimiter;
  private TextVar wDelimiter;
  private FormData fdlDelimiter, fdDelimiter;

  private Label wlEnclosure;
  private TextVar wEnclosure;
  private FormData fdlEnclosure, fdEnclosure;

  private Label wlEscapeChar;
  private TextVar wEscapeChar;
  private FormData fdlEscapeChar, fdEscapeChar;

  private Label wlCharSet;
  private TextVar wCharSet;
  private FormData fdlCharSet, fdCharSet;

  private Label wlBulkSize;
  private TextVar wBulkSize;
  private FormData fdlBulkSize, fdBulkSize;

  private Label wlReturn;
  private TableView wReturn;
  private FormData fdlReturn, fdReturn;

  private Button wGetLU;
  private FormData fdGetLU;
  private Listener lsGetLU;

  private Button wDoMapping;
  private FormData fdDoMapping;

  private MySQLBulkLoaderMeta input;

  private ColumnInfo[] ciReturn;

  private Map<String, Integer> inputFields;

  /**
   * List of ColumnInfo that should have the field names of the selected database table
   */
  private List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();

  public MySQLBulkLoaderDialog( Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (MySQLBulkLoaderMeta) in;
    inputFields = new HashMap<String, Integer>();
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
    shell.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.TransformName.Label" ) );
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
    wlSchema = new Label( shell, SWT.RIGHT );
    wlSchema.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.TargetSchema.Label" ) );
    props.setLook( wlSchema );
    fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment( 0, 0 );
    fdlSchema.right = new FormAttachment( middle, -margin );
    fdlSchema.top = new FormAttachment( wConnection, margin * 2 );
    wlSchema.setLayoutData( fdlSchema );

    wSchema = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchema );
    wSchema.addModifyListener( lsMod );
    wSchema.addFocusListener( lsFocusLost );
    fdSchema = new FormData();
    fdSchema.left = new FormAttachment( middle, 0 );
    fdSchema.top = new FormAttachment( wConnection, margin * 2 );
    fdSchema.right = new FormAttachment( 100, 0 );
    wSchema.setLayoutData( fdSchema );

    // Table line...
    wlTable = new Label( shell, SWT.RIGHT );
    wlTable.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.TargetTable.Label" ) );
    props.setLook( wlTable );
    fdlTable = new FormData();
    fdlTable.left = new FormAttachment( 0, 0 );
    fdlTable.right = new FormAttachment( middle, -margin );
    fdlTable.top = new FormAttachment( wSchema, margin );
    wlTable.setLayoutData( fdlTable );

    wbTable = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTable );
    wbTable.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.Browse.Button" ) );
    fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment( wSchema, margin );
    wbTable.setLayoutData( fdbTable );
    wTable = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTable );
    wTable.addModifyListener( lsMod );
    wTable.addFocusListener( lsFocusLost );
    fdTable = new FormData();
    fdTable.left = new FormAttachment( middle, 0 );
    fdTable.top = new FormAttachment( wSchema, margin );
    fdTable.right = new FormAttachment( wbTable, -margin );
    wTable.setLayoutData( fdTable );

    // FifoFile line...
    wlFifoFile = new Label( shell, SWT.RIGHT );
    wlFifoFile.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.FifoFile.Label" ) );
    props.setLook( wlFifoFile );
    fdlFifoFile = new FormData();
    fdlFifoFile.left = new FormAttachment( 0, 0 );
    fdlFifoFile.right = new FormAttachment( middle, -margin );
    fdlFifoFile.top = new FormAttachment( wTable, margin );
    wlFifoFile.setLayoutData( fdlFifoFile );
    wFifoFile = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFifoFile );
    wFifoFile.addModifyListener( lsMod );
    fdFifoFile = new FormData();
    fdFifoFile.left = new FormAttachment( middle, 0 );
    fdFifoFile.top = new FormAttachment( wTable, margin );
    fdFifoFile.right = new FormAttachment( 100, 0 );
    wFifoFile.setLayoutData( fdFifoFile );

    // Delimiter line...
    wlDelimiter = new Label( shell, SWT.RIGHT );
    wlDelimiter.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.Delimiter.Label" ) );
    props.setLook( wlDelimiter );
    fdlDelimiter = new FormData();
    fdlDelimiter.left = new FormAttachment( 0, 0 );
    fdlDelimiter.right = new FormAttachment( middle, -margin );
    fdlDelimiter.top = new FormAttachment( wFifoFile, margin );
    wlDelimiter.setLayoutData( fdlDelimiter );
    wbDelimiter = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbDelimiter );
    wbDelimiter.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.Delimiter.Button" ) );
    FormData fdbDelimiter = new FormData();
    fdbDelimiter.top = new FormAttachment( wFifoFile, margin );
    fdbDelimiter.right = new FormAttachment( 100, 0 );
    wbDelimiter.setLayoutData( fdbDelimiter );
    wDelimiter = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDelimiter );
    wDelimiter.addModifyListener( lsMod );
    fdDelimiter = new FormData();
    fdDelimiter.left = new FormAttachment( middle, 0 );
    fdDelimiter.top = new FormAttachment( wFifoFile, margin );
    fdDelimiter.right = new FormAttachment( wbDelimiter, -margin );
    wDelimiter.setLayoutData( fdDelimiter );
    // Allow the insertion of tabs as separator...
    wbDelimiter.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent se ) {
        Text t = wDelimiter.getTextWidget();
        if ( t != null ) {
          t.insert( "\t" );
        }
      }
    } );

    // Enclosure line...
    wlEnclosure = new Label( shell, SWT.RIGHT );
    wlEnclosure.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.Enclosure.Label" ) );
    props.setLook( wlEnclosure );
    fdlEnclosure = new FormData();
    fdlEnclosure.left = new FormAttachment( 0, 0 );
    fdlEnclosure.right = new FormAttachment( middle, -margin );
    fdlEnclosure.top = new FormAttachment( wDelimiter, margin );
    wlEnclosure.setLayoutData( fdlEnclosure );
    wEnclosure = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEnclosure );
    wEnclosure.addModifyListener( lsMod );
    fdEnclosure = new FormData();
    fdEnclosure.left = new FormAttachment( middle, 0 );
    fdEnclosure.top = new FormAttachment( wDelimiter, margin );
    fdEnclosure.right = new FormAttachment( 100, 0 );
    wEnclosure.setLayoutData( fdEnclosure );

    // EscapeChar line...
    wlEscapeChar = new Label( shell, SWT.RIGHT );
    wlEscapeChar.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.EscapeChar.Label" ) );
    props.setLook( wlEscapeChar );
    fdlEscapeChar = new FormData();
    fdlEscapeChar.left = new FormAttachment( 0, 0 );
    fdlEscapeChar.right = new FormAttachment( middle, -margin );
    fdlEscapeChar.top = new FormAttachment( wEnclosure, margin );
    wlEscapeChar.setLayoutData( fdlEscapeChar );
    wEscapeChar = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEscapeChar );
    wEscapeChar.addModifyListener( lsMod );
    fdEscapeChar = new FormData();
    fdEscapeChar.left = new FormAttachment( middle, 0 );
    fdEscapeChar.top = new FormAttachment( wEnclosure, margin );
    fdEscapeChar.right = new FormAttachment( 100, 0 );
    wEscapeChar.setLayoutData( fdEscapeChar );

    // CharSet line...
    wlCharSet = new Label( shell, SWT.RIGHT );
    wlCharSet.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.CharSet.Label" ) );
    props.setLook( wlCharSet );
    fdlCharSet = new FormData();
    fdlCharSet.left = new FormAttachment( 0, 0 );
    fdlCharSet.right = new FormAttachment( middle, -margin );
    fdlCharSet.top = new FormAttachment( wEscapeChar, margin );
    wlCharSet.setLayoutData( fdlCharSet );
    wCharSet = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCharSet );
    wCharSet.addModifyListener( lsMod );
    fdCharSet = new FormData();
    fdCharSet.left = new FormAttachment( middle, 0 );
    fdCharSet.top = new FormAttachment( wEscapeChar, margin );
    fdCharSet.right = new FormAttachment( 100, 0 );
    wCharSet.setLayoutData( fdCharSet );

    // BulkSize line...
    wlBulkSize = new Label( shell, SWT.RIGHT );
    wlBulkSize.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.BulkSize.Label" ) );
    props.setLook( wlBulkSize );
    fdlBulkSize = new FormData();
    fdlBulkSize.left = new FormAttachment( 0, 0 );
    fdlBulkSize.right = new FormAttachment( middle, -margin );
    fdlBulkSize.top = new FormAttachment( wCharSet, margin );
    wlBulkSize.setLayoutData( fdlBulkSize );
    wBulkSize = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBulkSize );
    wBulkSize.addModifyListener( lsMod );
    fdBulkSize = new FormData();
    fdBulkSize.left = new FormAttachment( middle, 0 );
    fdBulkSize.top = new FormAttachment( wCharSet, margin );
    fdBulkSize.right = new FormAttachment( 100, 0 );
    wBulkSize.setLayoutData( fdBulkSize );

    // Replace line...
    wlReplace = new Label( shell, SWT.RIGHT );
    wlReplace.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.Replace.Label" ) );
    props.setLook( wlReplace );
    fdlReplace = new FormData();
    fdlReplace.left = new FormAttachment( 0, 0 );
    fdlReplace.right = new FormAttachment( middle, -margin );
    fdlReplace.top = new FormAttachment( wBulkSize, margin * 2 );
    wlReplace.setLayoutData( fdlReplace );

    wReplace = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wReplace );
    fdReplace = new FormData();
    fdReplace.left = new FormAttachment( middle, 0 );
    fdReplace.top = new FormAttachment( wBulkSize, margin * 2 );
    fdReplace.right = new FormAttachment( 100, 0 );
    wReplace.setLayoutData( fdReplace );
    wReplace.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        if ( wReplace.getSelection() ) {
          wIgnore.setSelection( false );
        }
        input.setChanged();
      }
    } );

    // Ignore line...
    wlIgnore = new Label( shell, SWT.RIGHT );
    wlIgnore.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.Ignore.Label" ) );
    props.setLook( wlIgnore );
    fdlIgnore = new FormData();
    fdlIgnore.left = new FormAttachment( 0, 0 );
    fdlIgnore.right = new FormAttachment( middle, -margin );
    fdlIgnore.top = new FormAttachment( wReplace, margin * 2 );
    wlIgnore.setLayoutData( fdlIgnore );

    wIgnore = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wIgnore );
    fdIgnore = new FormData();
    fdIgnore.left = new FormAttachment( middle, 0 );
    fdIgnore.top = new FormAttachment( wReplace, margin * 2 );
    fdIgnore.right = new FormAttachment( 100, 0 );
    wIgnore.setLayoutData( fdIgnore );
    wIgnore.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        if ( wIgnore.getSelection() ) {
          wReplace.setSelection( false );
        }
        input.setChanged();
      }
    } );

    // Local line...
    wlLocal = new Label( shell, SWT.RIGHT );
    wlLocal.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.Local.Label" ) );
    props.setLook( wlLocal );
    fdlLocal = new FormData();
    fdlLocal.left = new FormAttachment( 0, 0 );
    fdlLocal.right = new FormAttachment( middle, -margin );
    fdlLocal.top = new FormAttachment( wIgnore, margin * 2 );
    wlLocal.setLayoutData( fdlLocal );

    wLocal = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wLocal );
    fdLocal = new FormData();
    fdLocal.left = new FormAttachment( middle, 0 );
    fdLocal.top = new FormAttachment( wIgnore, margin * 2 );
    fdLocal.right = new FormAttachment( 100, 0 );
    wLocal.setLayoutData( fdLocal );
    wLocal.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    } );

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wSql = new Button( shell, SWT.PUSH );
    wSql.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.SQL.Button" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel, wSql }, margin, null );

    // The field Table
    wlReturn = new Label( shell, SWT.NONE );
    wlReturn.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.Fields.Label" ) );
    props.setLook( wlReturn );
    fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment( 0, 0 );
    fdlReturn.top = new FormAttachment( wLocal, margin );
    wlReturn.setLayoutData( fdlReturn );

    int UpInsCols = 3;
    int UpInsRows = ( input.getFieldTable() != null ? input.getFieldTable().length : 1 );

    ciReturn = new ColumnInfo[ UpInsCols ];
    ciReturn[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.ColumnInfo.TableField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciReturn[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.ColumnInfo.StreamField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciReturn[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.ColumnInfo.FormatOK" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, MySQLBulkLoaderMeta.getFieldFormatTypeDescriptions(), true );

    tableFieldColumns.add( ciReturn[ 0 ] );
    wReturn =
      new TableView(
        pipelineMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciReturn,
        UpInsRows, lsMod, props );

    wGetLU = new Button( shell, SWT.PUSH );
    wGetLU.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.GetFields.Label" ) );
    fdGetLU = new FormData();
    fdGetLU.top = new FormAttachment( wlReturn, margin );
    fdGetLU.right = new FormAttachment( 100, 0 );
    wGetLU.setLayoutData( fdGetLU );

    wDoMapping = new Button( shell, SWT.PUSH );
    wDoMapping.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.EditMapping.Label" ) );
    fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment( wGetLU, margin );
    fdDoMapping.right = new FormAttachment( 100, 0 );
    wDoMapping.setLayoutData( fdDoMapping );

    wDoMapping.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event arg0 ) {
        generateMappings();
      }
    } );

    fdReturn = new FormData();
    fdReturn.left = new FormAttachment( 0, 0 );
    fdReturn.top = new FormAttachment( wlReturn, margin );
    fdReturn.right = new FormAttachment( wDoMapping, -margin );
    fdReturn.bottom = new FormAttachment( wOk, -2 * margin );
    wReturn.setLayoutData( fdReturn );

    //
    // Search the fields in the background
    //

    final Runnable runnable = new Runnable() {
      public void run() {
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
      }
    };
    new Thread( runnable ).start();

    // Add listeners
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsGetLU = new Listener() {
      public void handleEvent( Event e ) {
        getUpdate();
      }
    };
    lsSql = new Listener() {
      public void handleEvent( Event e ) {
        create();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOk.addListener( SWT.Selection, lsOk );
    wGetLU.addListener( SWT.Selection, lsGetLU );
    wSql.addListener( SWT.Selection, lsSql );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wSchema.addSelectionListener( lsDef );
    wFifoFile.addSelectionListener( lsDef );
    wTable.addSelectionListener( lsDef );
    wDelimiter.addSelectionListener( lsDef );
    wEnclosure.addSelectionListener( lsDef );
    wCharSet.addSelectionListener( lsDef );
    wBulkSize.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wbTable.addSelectionListener( new SelectionAdapter() {
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
        BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Title" ),
        BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Message" ), e );
      return;
    }
    // refresh data
    input.setDatabaseMeta( pipelineMeta.findDatabase( wConnection.getText() ) );
    input.setTableName( pipelineMeta.environmentSubstitute( wTable.getText() ) );
    ITransform transformMetaInterface = transformMeta.getITransform();
    try {
      targetFields = transformMetaInterface.getRequiredFields( pipelineMeta );
    } catch ( HopException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Title" ),
        BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Message" ), e );
      return;
    }

    String[] inputNames = new String[ sourceFields.size() ];
    for ( int i = 0; i < sourceFields.size(); i++ ) {
      IValueMeta value = sourceFields.getValueMeta( i );
      inputNames[ i ] = value.getName() + EnterMappingDialog.STRING_ORIGIN_SEPARATOR + value.getOrigin() + ")";
    }

    // Create the existing mapping list...
    //
    List<SourceToTargetMapping> mappings = new ArrayList<SourceToTargetMapping>();
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
            PKG, "MySQLBulkLoaderDialog.DoMapping.SomeSourceFieldsNotFound", missingSourceFields.toString() )
            + Const.CR;
      }
      if ( missingTargetFields.length() > 0 ) {
        message +=
          BaseMessages.getString(
            PKG, "MySQLBulkLoaderDialog.DoMapping.SomeTargetFieldsNotFound", missingSourceFields.toString() )
            + Const.CR;
      }
      message += Const.CR;
      message +=
        BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.DoMapping.SomeFieldsNotFoundContinue" ) + Const.CR;
      MessageDialog.setDefaultImage( GUIResource.getInstance().getImageHopUi() );
      boolean goOn =
        MessageDialog.openConfirm( shell, BaseMessages.getString(
          PKG, "MySQLBulkLoaderDialog.DoMapping.SomeFieldsNotFoundTitle" ), message );
      if ( !goOn ) {
        return;
      }
    }
    EnterMappingDialog d =
      new EnterMappingDialog( MySQLBulkLoaderDialog.this.shell, sourceFields.getFieldNames(), targetFields
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
        item.setText( 3, MySQLBulkLoaderMeta.getFieldFormatTypeDescription( input.getFieldFormatType()[ i ] ) );
      }
      wReturn.setRowNums();
      wReturn.optWidth( true );
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.Log.GettingKeyInfo" ) );
    }

    wEnclosure.setText( Const.NVL( input.getEnclosure(), "" ) );
    wDelimiter.setText( Const.NVL( input.getDelimiter(), "" ) );
    wEscapeChar.setText( Const.NVL( input.getEscapeChar(), "" ) );
    wCharSet.setText( Const.NVL( input.getEncoding(), "" ) );
    wReplace.setSelection( input.isReplacingData() );
    wIgnore.setSelection( input.isIgnoringErrors() );
    wLocal.setSelection( input.isLocalFile() );
    wBulkSize.setText( Const.NVL( input.getBulkSize(), "" ) );

    if ( input.getFieldTable() != null ) {
      for ( int i = 0; i < input.getFieldTable().length; i++ ) {
        TableItem item = wReturn.table.getItem( i );
        if ( input.getFieldTable()[ i ] != null ) {
          item.setText( 1, input.getFieldTable()[ i ] );
        }
        if ( input.getFieldStream()[ i ] != null ) {
          item.setText( 2, input.getFieldStream()[ i ] );
        }
        item.setText( 3, MySQLBulkLoaderMeta.getFieldFormatTypeDescription( input.getFieldFormatType()[ i ] ) );
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
    if ( input.getFifoFileName() != null ) {
      wFifoFile.setText( input.getFifoFileName() );
    }

    wReturn.setRowNums();
    wReturn.optWidth( true );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<String, Integer>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[ entries.size() ] );
    Const.sortStrings( fieldNames );
    // return fields
    ciReturn[ 1 ].setComboValues( fieldNames );
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void getInfo( MySQLBulkLoaderMeta inf ) {
    int nrFields = wReturn.nrNonEmpty();

    inf.allocate( nrFields );

    inf.setEnclosure( wEnclosure.getText() );
    inf.setDelimiter( wDelimiter.getText() );
    inf.setEscapeChar( wEscapeChar.getText() );
    inf.setEncoding( wCharSet.getText() );
    inf.setReplacingData( wReplace.getSelection() );
    inf.setIgnoringErrors( wIgnore.getSelection() );
    inf.setLocalFile( wLocal.getSelection() );
    inf.setBulkSize( wBulkSize.getText() );

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.Log.FoundFields", "" + nrFields ) );
    }
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wReturn.getNonEmpty( i );
      inf.getFieldTable()[ i ] = item.getText( 1 );
      inf.getFieldStream()[ i ] = item.getText( 2 );
      inf.getFieldFormatType()[ i ] = MySQLBulkLoaderMeta.getFieldFormatType( item.getText( 3 ) );
    }

    inf.setSchemaName( wSchema.getText() );
    inf.setTableName( wTable.getText() );
    inf.setDatabaseMeta( pipelineMeta.findDatabase( wConnection.getText() ) );
    inf.setFifoFileName( wFifoFile.getText() );

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
      mb.setMessage( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.InvalidConnection.DialogTitle" ) );
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
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.Log.LookingAtConnection" ) + databaseMeta.toString() );
      }

      DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, databaseMeta, pipelineMeta.getDatabases() );
      std.setSelectedSchemaAndTable( wSchema.getText(), wTable.getText() );
      if ( std.open() ) {
        wSchema.setText( Const.NVL( std.getSchemaName(), "" ) );
        wTable.setText( Const.NVL( std.getTableName(), "" ) );
      }
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
    }
  }

  private void getUpdate() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
      if ( r != null ) {
        TableItemInsertListener listener = new TableItemInsertListener() {
          public boolean tableItemInserted( TableItem tableItem, IValueMeta v ) {
            if ( v.getType() == IValueMeta.TYPE_DATE ) {
              // The default is : format is OK for dates, see if this sticks later on...
              //
              tableItem.setText( 3, "Y" );
            } else {
              tableItem.setText( 3, "Y" ); // default is OK too...
            }
            return true;
          }
        };
        BaseTransformDialog.getFieldsFromPrevious( r, wReturn, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.FailedToGetFields.DialogTitle" ),
        BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  // Generate code for create table...
  // Conversions done by Database
  private void create() {
    try {
      MySQLBulkLoaderMeta info = new MySQLBulkLoaderMeta();
      getInfo( info );

      String name = transformName; // new name might not yet be linked to other transforms!
      TransformMeta transformMeta =
        new TransformMeta( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.TransformMeta.Title" ), name, info );
      IRowMeta prev = pipelineMeta.getPrevTransformFields( transformName );

      SQLStatement sql = info.getSqlStatements( pipelineMeta, transformMeta, prev, metadataProvider );
      if ( !sql.hasError() ) {
        if ( sql.hasSQL() ) {
          SQLEditor sqledit =
            new SQLEditor( pipelineMeta, shell, SWT.NONE, info.getDatabaseMeta(), DbCache.getInstance(), sql
              .getSql() );
          sqledit.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.NoSQLNeeds.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.NoSQLNeeds.DialogTitle" ) );
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( sql.getError() );
        mb.setText( BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.SQLError.DialogTitle" ) );
        mb.open();
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "MySQLBulkLoaderDialog.CouldNotBuildSQL.DialogTitle" ), BaseMessages
        .getString( PKG, "MySQLBulkLoaderDialog.CouldNotBuildSQL.DialogMessage" ), ke );
    }

  }

  private void setTableFieldCombo() {
    Runnable fieldLoader = new Runnable() {
      public void run() {
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
      }
    };
    shell.getDisplay().asyncExec( fieldLoader );
  }
}
