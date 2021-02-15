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

package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.*;
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
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
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
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;
import org.apache.hop.core.variables.IVariables;

public class SynchronizeAfterMergeDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SynchronizeAfterMergeMeta.class; // For Translator

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TableView wKey;

  private TextVar wSchema;

  private Label wlTable;
  private Button wbTable;
  private TextVar wTable;

  private TableView wReturn;

  private TextVar wCommit;

  private Label wlTableField;
  private CCombo wTableField;

  private Button wTablenameInField;

  private Button wBatch;

  private Button wPerformLookup;

  private CCombo wOperationField;

  private TextVar wOrderInsert;

  private TextVar wOrderDelete;

  private TextVar wOrderUpdate;

  private final SynchronizeAfterMergeMeta input;

  private final Map<String, Integer> inputFields;

  private ColumnInfo[] ciKey;

  private ColumnInfo[] ciReturn;

  private boolean gotPreviousFields = false;

  /**
   * List of ColumnInfo that should have the field names of the selected database table
   */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  public SynchronizeAfterMergeDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (SynchronizeAfterMergeMeta) in;
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
    SelectionListener lsSimpleSelection = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // THE BUTTONS go at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wSql = new Button( shell, SWT.PUSH );
    wSql.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.SQL.Button" ) );
    wSql.addListener( SWT.Selection, e -> create() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wSql, wCancel }, margin, null );

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.TransformName.Label" ) );
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

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.GeneralTab.TabTitle" ) );

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // Connection line
    wConnection = addConnectionLine(wGeneralComp, wTransformName, input.getDatabaseMeta(), lsMod );
    wConnection.addSelectionListener( lsSelection );

    // Schema line...
    Label wlSchema = new Label(wGeneralComp, SWT.RIGHT);
    wlSchema.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.TargetSchema.Label" ) );
    props.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment( 0, 0 );
    fdlSchema.right = new FormAttachment( middle, -margin );
    fdlSchema.top = new FormAttachment( wConnection, margin * 2 );
    wlSchema.setLayoutData(fdlSchema);

    Button wbSchema = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbSchema);
    wbSchema.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment( wConnection, 2 * margin );
    fdbSchema.right = new FormAttachment( 100, 0 );
    wbSchema.setLayoutData(fdbSchema);

    wSchema = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchema );
    wSchema.addModifyListener( lsTableMod );
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment( middle, 0 );
    fdSchema.top = new FormAttachment( wConnection, margin * 2 );
    fdSchema.right = new FormAttachment(wbSchema, -margin );
    wSchema.setLayoutData(fdSchema);

    // Table line...
    wlTable = new Label(wGeneralComp, SWT.RIGHT );
    wlTable.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.TargetTable.Label" ) );
    props.setLook( wlTable );
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment( 0, 0 );
    fdlTable.right = new FormAttachment( middle, -margin );
    fdlTable.top = new FormAttachment(wbSchema, margin );
    wlTable.setLayoutData(fdlTable);

    wbTable = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTable );
    wbTable.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.Browse.Button" ) );
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment(wbSchema, margin );
    wbTable.setLayoutData(fdbTable);

    wTable = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTable );
    wTable.addModifyListener( lsTableMod );
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment( middle, 0 );
    fdTable.top = new FormAttachment(wbSchema, margin );
    fdTable.right = new FormAttachment( wbTable, -margin );
    wTable.setLayoutData(fdTable);

    // Commit line
    Label wlCommit = new Label(wGeneralComp, SWT.RIGHT);
    wlCommit.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.CommitSize.Label" ) );
    props.setLook(wlCommit);
    FormData fdlCommit = new FormData();
    fdlCommit.left = new FormAttachment( 0, 0 );
    fdlCommit.top = new FormAttachment( wTable, margin );
    fdlCommit.right = new FormAttachment( middle, -margin );
    wlCommit.setLayoutData(fdlCommit);

    wCommit = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCommit );
    wCommit.addModifyListener( lsMod );
    FormData fdCommit = new FormData();
    fdCommit.left = new FormAttachment( middle, 0 );
    fdCommit.top = new FormAttachment( wTable, margin );
    fdCommit.right = new FormAttachment( 100, 0 );
    wCommit.setLayoutData(fdCommit);

    // UsePart update
    Label wlBatch = new Label(wGeneralComp, SWT.RIGHT);
    wlBatch.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.Batch.Label" ) );
    props.setLook(wlBatch);
    FormData fdlBatch = new FormData();
    fdlBatch.left = new FormAttachment( 0, 0 );
    fdlBatch.top = new FormAttachment( wCommit, margin );
    fdlBatch.right = new FormAttachment( middle, -margin );
    wlBatch.setLayoutData(fdlBatch);
    wBatch = new Button(wGeneralComp, SWT.CHECK );
    wBatch.setToolTipText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.Batch.Tooltip" ) );
    wBatch.addSelectionListener( lsSimpleSelection );
    props.setLook( wBatch );
    FormData fdBatch = new FormData();
    fdBatch.left = new FormAttachment( middle, 0 );
    fdBatch.top = new FormAttachment( wlBatch, 0, SWT.CENTER );
    fdBatch.right = new FormAttachment( 100, 0 );
    wBatch.setLayoutData(fdBatch);

    // TablenameInField line
    Label wlTablenameInField = new Label(wGeneralComp, SWT.RIGHT);
    wlTablenameInField.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.TablenameInField.Label" ) );
    props.setLook(wlTablenameInField);
    FormData fdlTablenameInField = new FormData();
    fdlTablenameInField.left = new FormAttachment( 0, 0 );
    fdlTablenameInField.top = new FormAttachment( wBatch, margin );
    fdlTablenameInField.right = new FormAttachment( middle, -margin );
    wlTablenameInField.setLayoutData(fdlTablenameInField);
    wTablenameInField = new Button(wGeneralComp, SWT.CHECK );
    wTablenameInField.setToolTipText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.TablenameInField.Tooltip" ) );
    props.setLook( wTablenameInField );
    FormData fdTablenameInField = new FormData();
    fdTablenameInField.left = new FormAttachment( middle, 0 );
    fdTablenameInField.top = new FormAttachment( wlTablenameInField, 0, SWT.CENTER );
    fdTablenameInField.right = new FormAttachment( 100, 0 );
    wTablenameInField.setLayoutData(fdTablenameInField);
    wTablenameInField.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeTablenameField();
        input.setChanged();
      }
    } );

    wlTableField = new Label(wGeneralComp, SWT.RIGHT );
    wlTableField.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.TableField.Label" ) );
    props.setLook( wlTableField );
    FormData fdlTableField = new FormData();
    fdlTableField.left = new FormAttachment( 0, 0 );
    fdlTableField.top = new FormAttachment( wTablenameInField, margin );
    fdlTableField.right = new FormAttachment( middle, -margin );
    wlTableField.setLayoutData(fdlTableField);
    wTableField = new CCombo(wGeneralComp, SWT.BORDER | SWT.READ_ONLY );
    wTableField.setEditable( true );
    props.setLook( wTableField );
    wTableField.addModifyListener( lsMod );
    FormData fdTableField = new FormData();
    fdTableField.left = new FormAttachment( middle, 0 );
    fdTableField.top = new FormAttachment( wTablenameInField, margin );
    fdTableField.right = new FormAttachment( 100, 0 );
    wTableField.setLayoutData(fdTableField);
    wTableField.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        getFields();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    Label wlKey = new Label(wGeneralComp, SWT.NONE);
    wlKey.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.Keys.Label" ) );
    props.setLook(wlKey);
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment( 0, 0 );
    fdlKey.top = new FormAttachment( wTableField, margin );
    wlKey.setLayoutData(fdlKey);

    int nrKeyCols = 4;
    int nrKeyRows = ( input.getKeyStream() != null ? input.getKeyStream().length : 1 );

    ciKey = new ColumnInfo[ nrKeyCols ];
    ciKey[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.ColumnInfo.TableField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciKey[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.ColumnInfo.Comparator" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "=", "<>", "<", "<=",
        ">", ">=", "LIKE", "BETWEEN", "IS NULL", "IS NOT NULL" } );
    ciKey[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.ColumnInfo.StreamField1" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciKey[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.ColumnInfo.StreamField2" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    tableFieldColumns.add( ciKey[ 0 ] );
    wKey =
      new TableView( variables, wGeneralComp, SWT.BORDER
        | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKey, nrKeyRows, lsMod, props );

    wGet = new Button(wGeneralComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.GetFields.Button" ) );
    fdGet = new FormData();
    fdGet.right = new FormAttachment( 100, 0 );
    fdGet.top = new FormAttachment(wlKey, margin );
    wGet.setLayoutData( fdGet );

    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment( 0, 0 );
    fdKey.top = new FormAttachment(wlKey, margin );
    fdKey.right = new FormAttachment( wGet, -margin );
    fdKey.bottom = new FormAttachment(wlKey, 160 );
    wKey.setLayoutData(fdKey);


    // THE UPDATE/INSERT TABLE
    Label wlReturn = new Label(wGeneralComp, SWT.NONE);
    wlReturn.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.UpdateFields.Label" ) );
    props.setLook(wlReturn);
    FormData fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment( 0, 0 );
    fdlReturn.top = new FormAttachment( wKey, margin );
    wlReturn.setLayoutData(fdlReturn);

    int UpInsCols = 3;
    int UpInsRows = ( input.getUpdateLookup() != null ? input.getUpdateLookup().length : 1 );

    ciReturn = new ColumnInfo[ UpInsCols ];
    ciReturn[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.ColumnInfo.TableField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciReturn[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.ColumnInfo.StreamField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciReturn[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.ColumnInfo.Update" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "Y", "N" } );
    tableFieldColumns.add( ciReturn[ 0 ] );
    wReturn =
      new TableView( variables, wGeneralComp, SWT.BORDER
        | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciReturn, UpInsRows, lsMod, props );

    Button wGetLU = new Button(wGeneralComp, SWT.PUSH);
    wGetLU.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.GetAndUpdateFields.Label" ) );
    FormData fdGetLU = new FormData();
    fdGetLU.top = new FormAttachment(wlReturn, margin );
    fdGetLU.right = new FormAttachment( 100, 0 );
    wGetLU.setLayoutData(fdGetLU);

    FormData fdReturn = new FormData();
    fdReturn.left = new FormAttachment( 0, 0 );
    fdReturn.top = new FormAttachment(wlReturn, margin );
    fdReturn.right = new FormAttachment(wGetLU, -margin );
    fdReturn.bottom = new FormAttachment( 100, -2 * margin );
    wReturn.setLayoutData(fdReturn);

    Button wDoMapping = new Button(wGeneralComp, SWT.PUSH);
    wDoMapping.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.EditMapping.Label" ) );
    FormData fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment(wGetLU, margin );
    fdDoMapping.right = new FormAttachment( 100, 0 );
    wDoMapping.setLayoutData(fdDoMapping);

    wDoMapping.addListener( SWT.Selection, arg0 -> generateMappings());

    //
    // Search the fields in the background
    //

    final Runnable runnable = () -> {
      // This is running in a new process: copy some HopVariables info
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
    lsGet = e -> get();
    Listener lsGetLU = e -> getUpdate();

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

    // ////////////////////////
    // START OF ADVANCED TAB ///
    // ////////////////////////

    CTabItem wAdvancedTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdvancedTab.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.AdvancedTab.TabTitle" ) );

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wAdvancedComp);

    FormLayout advancedLayout = new FormLayout();
    advancedLayout.marginWidth = 3;
    advancedLayout.marginHeight = 3;
    wAdvancedComp.setLayout( advancedLayout );

    // ///////////////////////////////
    // START OF OPERATION ORDER GROUP //
    // ///////////////////////////////

    Group wOperationOrder = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    props.setLook(wOperationOrder);
    wOperationOrder.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.OperationOrder.Label" ) );

    FormLayout OriginFilesgroupLayout = new FormLayout();
    OriginFilesgroupLayout.marginWidth = 10;
    OriginFilesgroupLayout.marginHeight = 10;
    wOperationOrder.setLayout( OriginFilesgroupLayout );

    Label wlOperationField = new Label(wOperationOrder, SWT.RIGHT);
    wlOperationField.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.OperationField.Label" ) );
    props.setLook(wlOperationField);
    FormData fdlOperationField = new FormData();
    fdlOperationField.left = new FormAttachment( 0, 0 );
    fdlOperationField.top = new FormAttachment( wTableField, margin );
    fdlOperationField.right = new FormAttachment( middle, -margin );
    wlOperationField.setLayoutData(fdlOperationField);
    wOperationField = new CCombo(wOperationOrder, SWT.BORDER | SWT.READ_ONLY );
    wOperationField.setEditable( true );
    props.setLook( wOperationField );
    wOperationField.addModifyListener( lsMod );
    FormData fdOperationField = new FormData();
    fdOperationField.left = new FormAttachment( middle, 0 );
    fdOperationField.top = new FormAttachment( wTableField, margin );
    fdOperationField.right = new FormAttachment( 100, 0 );
    wOperationField.setLayoutData(fdOperationField);
    wOperationField.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        getFields();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // OrderInsert line...
    Label wlOrderInsert = new Label(wOperationOrder, SWT.RIGHT);
    wlOrderInsert.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.OrderInsert.Label" ) );
    props.setLook(wlOrderInsert);
    FormData fdlOrderInsert = new FormData();
    fdlOrderInsert.left = new FormAttachment( 0, 0 );
    fdlOrderInsert.right = new FormAttachment( middle, -margin );
    fdlOrderInsert.top = new FormAttachment( wOperationField, margin );
    wlOrderInsert.setLayoutData(fdlOrderInsert);

    wOrderInsert = new TextVar( variables, wOperationOrder, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wOrderInsert.setToolTipText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.OrderInsert.ToolTip" ) );
    props.setLook( wOrderInsert );
    wOrderInsert.addModifyListener( lsMod );
    FormData fdOrderInsert = new FormData();
    fdOrderInsert.left = new FormAttachment( middle, 0 );
    fdOrderInsert.top = new FormAttachment( wOperationField, margin );
    fdOrderInsert.right = new FormAttachment( 100, 0 );
    wOrderInsert.setLayoutData(fdOrderInsert);

    // OrderUpdate line...
    Label wlOrderUpdate = new Label(wOperationOrder, SWT.RIGHT);
    wlOrderUpdate.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.OrderUpdate.Label" ) );
    props.setLook(wlOrderUpdate);
    FormData fdlOrderUpdate = new FormData();
    fdlOrderUpdate.left = new FormAttachment( 0, 0 );
    fdlOrderUpdate.right = new FormAttachment( middle, -margin );
    fdlOrderUpdate.top = new FormAttachment( wOrderInsert, margin );
    wlOrderUpdate.setLayoutData(fdlOrderUpdate);

    wOrderUpdate = new TextVar( variables, wOperationOrder, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wOrderUpdate.setToolTipText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.OrderUpdate.ToolTip" ) );
    props.setLook( wOrderUpdate );
    wOrderUpdate.addModifyListener( lsMod );
    FormData fdOrderUpdate = new FormData();
    fdOrderUpdate.left = new FormAttachment( middle, 0 );
    fdOrderUpdate.top = new FormAttachment( wOrderInsert, margin );
    fdOrderUpdate.right = new FormAttachment( 100, 0 );
    wOrderUpdate.setLayoutData(fdOrderUpdate);

    // OrderDelete line...
    Label wlOrderDelete = new Label(wOperationOrder, SWT.RIGHT);
    wlOrderDelete.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.OrderDelete.Label" ) );
    props.setLook(wlOrderDelete);
    FormData fdlOrderDelete = new FormData();
    fdlOrderDelete.left = new FormAttachment( 0, 0 );
    fdlOrderDelete.right = new FormAttachment( middle, -margin );
    fdlOrderDelete.top = new FormAttachment( wOrderUpdate, margin );
    wlOrderDelete.setLayoutData(fdlOrderDelete);

    wOrderDelete = new TextVar( variables, wOperationOrder, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wOrderDelete.setToolTipText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.OrderDelete.ToolTip" ) );
    props.setLook( wOrderDelete );
    wOrderDelete.addModifyListener( lsMod );
    FormData fdOrderDelete = new FormData();
    fdOrderDelete.left = new FormAttachment( middle, 0 );
    fdOrderDelete.top = new FormAttachment( wOrderUpdate, margin );
    fdOrderDelete.right = new FormAttachment( 100, 0 );
    wOrderDelete.setLayoutData(fdOrderDelete);

    // Perform a lookup?
    Label wlPerformLookup = new Label(wOperationOrder, SWT.RIGHT);
    wlPerformLookup.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.PerformLookup.Label" ) );
    props.setLook(wlPerformLookup);
    FormData fdlPerformLookup = new FormData();
    fdlPerformLookup.left = new FormAttachment( 0, 0 );
    fdlPerformLookup.top = new FormAttachment( wOrderDelete, margin );
    fdlPerformLookup.right = new FormAttachment( middle, -margin );
    wlPerformLookup.setLayoutData(fdlPerformLookup);
    wPerformLookup = new Button(wOperationOrder, SWT.CHECK );
    wPerformLookup.setToolTipText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.PerformLookup.Tooltip" ) );
    wPerformLookup.addSelectionListener( lsSimpleSelection );
    props.setLook( wPerformLookup );
    FormData fdPerformLookup = new FormData();
    fdPerformLookup.left = new FormAttachment( middle, 0 );
    fdPerformLookup.top = new FormAttachment( wlPerformLookup, 0, SWT.CENTER );
    fdPerformLookup.right = new FormAttachment( 100, 0 );
    wPerformLookup.setLayoutData(fdPerformLookup);

    FormData fdOperationOrder = new FormData();
    fdOperationOrder.left = new FormAttachment( 0, margin );
    fdOperationOrder.top = new FormAttachment( wTransformName, margin );
    fdOperationOrder.right = new FormAttachment( 100, -margin );
    wOperationOrder.setLayoutData(fdOperationOrder);

    // ///////////////////////////////////////////////////////////
    // / END OF Operation order GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdAdvancedComp = new FormData();
    fdAdvancedComp.left = new FormAttachment( 0, 0 );
    fdAdvancedComp.top = new FormAttachment( 0, 0 );
    fdAdvancedComp.right = new FormAttachment( 100, 0 );
    fdAdvancedComp.bottom = new FormAttachment( 100, 0 );
    wAdvancedComp.setLayoutData(fdAdvancedComp);

    wAdvancedComp.layout();
    wAdvancedTab.setControl(wAdvancedComp);
    props.setLook(wAdvancedComp);

    // ///////////////////////////////////////////////////////////
    // / END OF ADVANCED TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData(fdTabFolder);

    wGet.addListener( SWT.Selection, lsGet );
    wGetLU.addListener( SWT.Selection, lsGetLU);

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wSchema.addSelectionListener( lsDef );
    wTable.addSelectionListener( lsDef );
    wCommit.addSelectionListener( lsDef );

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

    wbTable.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getTableName();
      }
    } );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();
    getData();
    setTableFieldCombo();
    activeTablenameField();
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
      sourceFields = pipelineMeta.getPrevTransformFields( variables, transformMeta );
    } catch ( HopException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.DoMapping.UnableToFindSourceFields.Title" ),
        BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.DoMapping.UnableToFindSourceFields.Message" ), e );
      return;
    }

    // refresh data
    input.setDatabaseMeta( pipelineMeta.findDatabase( wConnection.getText() ) );
    input.setTableName( variables.resolve( wTable.getText() ) );
    ITransformMeta transformMetaInterface = transformMeta.getTransform();
    try {
      targetFields = transformMetaInterface.getRequiredFields( variables );
    } catch ( HopException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.DoMapping.UnableToFindTargetFields.Title" ),
        BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.DoMapping.UnableToFindTargetFields.Message" ), e );
      return;
    }

    String[] inputNames = new String[ sourceFields.size() ];
    for ( int i = 0; i < sourceFields.size(); i++ ) {
      IValueMeta value = sourceFields.getValueMeta( i );
      inputNames[ i ] = value.getName();
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
            PKG, "SynchronizeAfterMergeDialog.DoMapping.SomeSourceFieldsNotFound", missingSourceFields
              .toString() )
            + Const.CR;
      }
      if ( missingTargetFields.length() > 0 ) {
        message +=
          BaseMessages.getString(
            PKG, "SynchronizeAfterMergeDialog.DoMapping.SomeTargetFieldsNotFound", missingSourceFields
              .toString() )
            + Const.CR;
      }
      message += Const.CR;
      message +=
        BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.DoMapping.SomeFieldsNotFoundContinue" )
          + Const.CR;
      MessageDialog.setDefaultImage( GuiResource.getInstance().getImageHopUi() );
      boolean goOn =
        MessageDialog.openConfirm( shell, BaseMessages.getString(
          PKG, "SynchronizeAfterMergeDialog.DoMapping.SomeFieldsNotFoundTitle" ), message );
      if ( !goOn ) {
        return;
      }
    }
    EnterMappingDialog d =
      new EnterMappingDialog( SynchronizeAfterMergeDialog.this.shell, sourceFields.getFieldNames(), targetFields
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

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray(new String[entries.size()]);

    Const.sortStrings(fieldNames);
    ciKey[ 2 ].setComboValues(fieldNames);
    ciKey[ 3 ].setComboValues(fieldNames);
    ciReturn[ 1 ].setComboValues(fieldNames);
  }

  private void activeTablenameField() {
    wlTableField.setEnabled( wTablenameInField.getSelection() );
    wTableField.setEnabled( wTablenameInField.getSelection() );
    wlTable.setEnabled( !wTablenameInField.getSelection() );
    wTable.setEnabled( !wTablenameInField.getSelection() );
    wbTable.setEnabled( !wTablenameInField.getSelection() );
    wSql.setEnabled( !wTablenameInField.getSelection() );

  }

  private void getFields() {
    if ( !gotPreviousFields ) {
      try {
        String field = wTableField.getText();
        String fieldoperation = wOperationField.getText();
        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wTableField.setItems( r.getFieldNames() );
          wOperationField.setItems( r.getFieldNames() );
        }
        if ( field != null ) {
          wTableField.setText( field );
        }
        if ( fieldoperation != null ) {
          wOperationField.setText( fieldoperation );
        }
      } catch ( HopException ke ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.Log.GettingKeyInfo" ) );
    }

    wCommit.setText( input.getCommitSize() );
    wTablenameInField.setSelection( input.istablenameInField() );
    if ( input.gettablenameField() != null ) {
      wTableField.setText( input.gettablenameField() );
    }
    wBatch.setSelection( input.useBatchUpdate() );
    if ( input.getOperationOrderField() != null ) {
      wOperationField.setText( input.getOperationOrderField() );
    }
    if ( input.getOrderInsert() != null ) {
      wOrderInsert.setText( input.getOrderInsert() );
    }
    if ( input.getOrderUpdate() != null ) {
      wOrderUpdate.setText( input.getOrderUpdate() );
    }
    if ( input.getOrderDelete() != null ) {
      wOrderDelete.setText( input.getOrderDelete() );
    }
    wPerformLookup.setSelection( input.isPerformLookup() );

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
        if ( input.getUpdate()[ i ] == null || input.getUpdate()[ i ] ) {
          item.setText( 3, "Y" );
        } else {
          item.setText( 3, "N" );
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

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void getInfo( SynchronizeAfterMergeMeta inf ) {
    // Table ktable = wKey.table;
    int nrkeys = wKey.nrNonEmpty();
    int nrFields = wReturn.nrNonEmpty();

    inf.allocate( nrkeys, nrFields );

    inf.setCommitSize( wCommit.getText() );
    inf.settablenameInField( wTablenameInField.getSelection() );
    inf.settablenameField( wTableField.getText() );
    inf.setUseBatchUpdate( wBatch.getSelection() );
    inf.setPerformLookup( wPerformLookup.getSelection() );

    inf.setOperationOrderField( wOperationField.getText() );
    inf.setOrderInsert( wOrderInsert.getText() );
    inf.setOrderUpdate( wOrderUpdate.getText() );
    inf.setOrderDelete( wOrderDelete.getText() );

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.Log.FoundKeys", nrkeys + "" ) );
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

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.Log.FoundFields", nrFields + "" ) );
    }
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wReturn.getNonEmpty( i );
      inf.getUpdateLookup()[ i ] = item.getText( 1 );
      inf.getUpdateStream()[ i ] = item.getText( 2 );
      inf.getUpdate()[ i ] = "Y".equals( item.getText( 3 ) );
    }

    inf.setSchemaName( wSchema.getText() );
    inf.setTableName( wTable.getText() );
    inf.setDatabaseMeta( pipelineMeta.findDatabase( wConnection.getText() ) );

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
      mb.setMessage( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.InvalidConnection.DialogTitle" ) );
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
        logDebug( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.Log.LookingAtConnection" )
          + databaseMeta.toString() );
      }

      DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases() );
      std.setSelectedSchemaAndTable( wSchema.getText(), wTable.getText() );
      if ( std.open() ) {
        wSchema.setText( Const.NVL( std.getSchemaName(), "" ) );
        wTable.setText( Const.NVL( std.getTableName(), "" ) );
        wTable.setFocus();
        setTableFieldCombo();
      }
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.InvalidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.InvalidConnection.DialogTitle" ) );
      mb.open();
    }
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        ITableItemInsertListener listener = (tableItem, v) -> {
          tableItem.setText( 2, "=" );
          return true;
        };
        BaseTransformDialog.getFieldsFromPrevious( r, wKey, 1, new int[] { 1, 3 }, new int[] {}, -1, -1, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString(
        PKG, "SynchronizeAfterMergeDialog.FailedToGetFields.DialogTitle" ), BaseMessages.getString(
        PKG, "SynchronizeAfterMergeDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  private void getUpdate() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        ITableItemInsertListener listener = (tableItem, v) -> {
          tableItem.setText( 3, "Y" );
          return true;
        };
        BaseTransformDialog.getFieldsFromPrevious( r, wReturn, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString(
        PKG, "SynchronizeAfterMergeDialog.FailedToGetFields.DialogTitle" ), BaseMessages.getString(
        PKG, "SynchronizeAfterMergeDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  // Generate code for create table...
  // Conversions done by Database
  private void create() {
    try {
      SynchronizeAfterMergeMeta info = new SynchronizeAfterMergeMeta();
      getInfo( info );

      String name = transformName; // new name might not yet be linked to other transforms!
      TransformMeta transformMeta =
        new TransformMeta( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.TransformMeta.Title" ), name, info );
      IRowMeta prev = pipelineMeta.getPrevTransformFields( variables, transformName );

      SqlStatement sql = info.getSqlStatements( variables, pipelineMeta, transformMeta, prev, metadataProvider );
      if ( !sql.hasError() ) {
        if ( sql.hasSql() ) {
          SqlEditor sqledit =
            new SqlEditor( shell, SWT.NONE, variables,  info.getDatabaseMeta(), DbCache.getInstance(), sql
              .getSql() );
          sqledit.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.NoSQLNeeds.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.NoSQLNeeds.DialogTitle" ) );
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( sql.getError() );
        mb.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.SQLError.DialogTitle" ) );
        mb.open();
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString(
        PKG, "SynchronizeAfterMergeDialog.CouldNotBuildSQL.DialogTitle" ), BaseMessages.getString(
        PKG, "SynchronizeAfterMergeDialog.CouldNotBuildSQL.DialogMessage" ), ke );
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
          EnterSelectionDialog dialog =
            new EnterSelectionDialog( shell, schemas, BaseMessages.getString(
              PKG, "SynchronizeAfterMergeDialog.AvailableSchemas.Title", wConnection.getText() ), BaseMessages
              .getString( PKG, "SynchronizeAfterMergeDialog.AvailableSchemas.Message", wConnection.getText() ) );
          String d = dialog.open();
          if ( d != null ) {
            wSchema.setText( Const.NVL( d, "" ) );
            setTableFieldCombo();
          }

        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.NoSchema.Error" ) );
          mb.setText( BaseMessages.getString( PKG, "SynchronizeAfterMergeDialog.GetSchemas.Error" ) );
          mb.open();
        }
      } catch ( Exception e ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
          .getString( PKG, "SynchronizeAfterMergeDialog.ErrorGettingSchemas" ), e );
      } finally {
        database.disconnect();
      }
    }
  }
}
