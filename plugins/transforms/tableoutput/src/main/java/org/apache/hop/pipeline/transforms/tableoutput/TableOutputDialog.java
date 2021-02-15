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

package org.apache.hop.pipeline.transforms.tableoutput;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.*;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
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
import org.apache.hop.ui.core.widget.*;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
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

public class TableOutputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = TableOutputMeta.class; // For Translator

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wSchema;

  private Label wlTable;
  private TextVar wTable;

  private TextVar wCommit;

  private Label wlTruncate;
  private Button wTruncate;

  private Label wlIgnore;
  private Button wIgnore;

  private Button wSpecifyFields;

  private Label wlBatch;
  private Button wBatch;

  private Button wUsePart;

  private Label wlPartField;
  private ComboVar wPartField;

  private Label wlPartMonthly;
  private Button wPartMonthly;

  private Label wlPartDaily;
  private Button wPartDaily;

  private Button wNameInField;

  private Label wlNameField;
  private ComboVar wNameField;

  private Label wlNameInTable;
  private Button wNameInTable;

  private Button wReturnKeys;

  private Label wlReturnField;
  private TextVar wReturnField;

  private TableView wFields;

  private Button wGetFields;

  private Button wDoMapping;

  private final TableOutputMeta input;

  private final Map<String, Integer> inputFields;

  private ColumnInfo[] ciFields;

  private boolean gotPreviousFields = false;

  /**
   * List of ColumnInfo that should have the field names of the selected database table
   */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  /**
   * Constructor.
   */
  public TableOutputDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (TableOutputMeta) in;
    inputFields = new HashMap<>();
  }

  /**
   * Open the dialog.
   */
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsTableMod = arg0 -> {
      input.setChanged();
      setTableFieldCombo();
    };
    SelectionListener lsSelection = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setTableFieldCombo();
        validateSelection();
      }
    };
    backupChanged = input.hasChanged();

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "TableOutputDialog.DialogTitle" ) );

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );

    // Connection line
    wConnection = addConnectionLine( shell, wTransformName, input.getDatabaseMeta(), null );
    wConnection.addModifyListener( e -> setFlags() );
    wConnection.addSelectionListener( lsSelection );

    // Schema line...
    Label wlSchema = new Label( shell, SWT.RIGHT );
    wlSchema.setText( BaseMessages.getString( PKG, "TableOutputDialog.TargetSchema.Label" ) );
    props.setLook( wlSchema );
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment( 0, 0 );
    fdlSchema.right = new FormAttachment( middle, -margin );
    fdlSchema.top = new FormAttachment( wConnection, margin * 2 );
    wlSchema.setLayoutData( fdlSchema );

    Button wbSchema = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSchema );
    wbSchema.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment( wConnection, 2 * margin );
    fdbSchema.right = new FormAttachment( 100, 0 );
    wbSchema.setLayoutData( fdbSchema );

    wSchema = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchema );
    wSchema.addModifyListener( lsTableMod );
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment( middle, 0 );
    fdSchema.top = new FormAttachment( wConnection, margin * 2 );
    fdSchema.right = new FormAttachment( wbSchema, -margin );
    wSchema.setLayoutData( fdSchema );

    // Table line...
    wlTable = new Label( shell, SWT.RIGHT );
    wlTable.setText( BaseMessages.getString( PKG, "TableOutputDialog.TargetTable.Label" ) );
    props.setLook( wlTable );
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment( 0, 0 );
    fdlTable.right = new FormAttachment( middle, -margin );
    fdlTable.top = new FormAttachment( wbSchema, margin );
    wlTable.setLayoutData( fdlTable );

    Button wbTable = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTable );
    wbTable.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment( wbSchema, margin );
    wbTable.setLayoutData( fdbTable );

    wTable = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTable );
    wTable.addModifyListener( lsTableMod );
    FormData fdTable = new FormData();
    fdTable.top = new FormAttachment( wbSchema, margin );
    fdTable.left = new FormAttachment( middle, 0 );
    fdTable.right = new FormAttachment( wbTable, -margin );
    wTable.setLayoutData( fdTable );

    // Commit size ...
    Label wlCommit = new Label( shell, SWT.RIGHT );
    wlCommit.setText( BaseMessages.getString( PKG, "TableOutputDialog.CommitSize.Label" ) );
    props.setLook( wlCommit );
    FormData fdlCommit = new FormData();
    fdlCommit.left = new FormAttachment( 0, 0 );
    fdlCommit.right = new FormAttachment( middle, -margin );
    fdlCommit.top = new FormAttachment( wbTable, margin );
    wlCommit.setLayoutData( fdlCommit );
    wCommit = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCommit );
    FormData fdCommit = new FormData();
    fdCommit.left = new FormAttachment( middle, 0 );
    fdCommit.top = new FormAttachment( wlCommit, 0, SWT.CENTER );
    fdCommit.right = new FormAttachment( 100, 0 );
    wCommit.setLayoutData( fdCommit );

    // Truncate table
    wlTruncate = new Label( shell, SWT.RIGHT );
    wlTruncate.setText( BaseMessages.getString( PKG, "TableOutputDialog.TruncateTable.Label" ) );
    props.setLook( wlTruncate );
    FormData fdlTruncate = new FormData();
    fdlTruncate.left = new FormAttachment( 0, 0 );
    fdlTruncate.top = new FormAttachment( wCommit, margin );
    fdlTruncate.right = new FormAttachment( middle, -margin );
    wlTruncate.setLayoutData( fdlTruncate );
    wTruncate = new Button( shell, SWT.CHECK );
    props.setLook( wTruncate );
    FormData fdTruncate = new FormData();
    fdTruncate.left = new FormAttachment( middle, 0 );
    fdTruncate.top = new FormAttachment( wlTruncate, 0, SWT.CENTER );
    fdTruncate.right = new FormAttachment( 100, 0 );
    wTruncate.setLayoutData( fdTruncate );
    SelectionAdapter lsSelMod = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wTruncate.addSelectionListener( lsSelMod );

    // Ignore errors
    wlIgnore = new Label( shell, SWT.RIGHT );
    wlIgnore.setText( BaseMessages.getString( PKG, "TableOutputDialog.IgnoreInsertErrors.Label" ) );
    props.setLook( wlIgnore );
    FormData fdlIgnore = new FormData();
    fdlIgnore.left = new FormAttachment( 0, 0 );
    fdlIgnore.top = new FormAttachment( wTruncate, margin );
    fdlIgnore.right = new FormAttachment( middle, -margin );
    wlIgnore.setLayoutData( fdlIgnore );
    wIgnore = new Button( shell, SWT.CHECK );
    props.setLook( wIgnore );
    FormData fdIgnore = new FormData();
    fdIgnore.left = new FormAttachment( middle, 0 );
    fdIgnore.top = new FormAttachment( wlIgnore, 0, SWT.CENTER );
    fdIgnore.right = new FormAttachment( 100, 0 );
    wIgnore.setLayoutData( fdIgnore );
    wIgnore.addSelectionListener( lsSelMod );

    // Specify fields
    Label wlSpecifyFields = new Label( shell, SWT.RIGHT );
    wlSpecifyFields.setText( BaseMessages.getString( PKG, "TableOutputDialog.SpecifyFields.Label" ) );
    props.setLook( wlSpecifyFields );
    FormData fdlSpecifyFields = new FormData();
    fdlSpecifyFields.left = new FormAttachment( 0, 0 );
    fdlSpecifyFields.top = new FormAttachment( wIgnore, margin );
    fdlSpecifyFields.right = new FormAttachment( middle, -margin );
    wlSpecifyFields.setLayoutData( fdlSpecifyFields );
    wSpecifyFields = new Button( shell, SWT.CHECK );
    props.setLook( wSpecifyFields );
    FormData fdSpecifyFields = new FormData();
    fdSpecifyFields.left = new FormAttachment( middle, 0 );
    fdSpecifyFields.top = new FormAttachment( wlSpecifyFields, 0, SWT.CENTER );
    fdSpecifyFields.right = new FormAttachment( 100, 0 );
    wSpecifyFields.setLayoutData( fdSpecifyFields );
    wSpecifyFields.addSelectionListener( lsSelMod );

    // If the flag is off, gray out the fields tab e.g.
    wSpecifyFields.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        setFlags();
      }
    } );

    CTabFolder wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF KEY TAB ///
    // /
    CTabItem wMainTab = new CTabItem( wTabFolder, SWT.NONE );
    wMainTab.setText( BaseMessages.getString( PKG, "TableOutputDialog.MainTab.CTabItem" ) );

    FormLayout mainLayout = new FormLayout();
    mainLayout.marginWidth = 3;
    mainLayout.marginHeight = 3;

    Composite wMainComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wMainComp );
    wMainComp.setLayout( mainLayout );

    // Partitioning support

    // Use partitioning?
    Label wlUsePart = new Label( wMainComp, SWT.RIGHT );
    wlUsePart.setText( BaseMessages.getString( PKG, "TableOutputDialog.UsePart.Label" ) );
    wlUsePart.setToolTipText( BaseMessages.getString( PKG, "TableOutputDialog.UsePart.Tooltip" ) );
    props.setLook( wlUsePart );
    FormData fdlUsePart = new FormData();
    fdlUsePart.left = new FormAttachment( 0, 0 );
    fdlUsePart.top = new FormAttachment( 0, margin );
    fdlUsePart.right = new FormAttachment( middle, -margin );
    wlUsePart.setLayoutData( fdlUsePart );
    wUsePart = new Button( wMainComp, SWT.CHECK );
    props.setLook( wUsePart );
    FormData fdUsePart = new FormData();
    fdUsePart.left = new FormAttachment( middle, 0 );
    fdUsePart.top = new FormAttachment( wlUsePart, 0, SWT.CENTER );
    fdUsePart.right = new FormAttachment( 100, 0 );
    wUsePart.setLayoutData( fdUsePart );
    wUsePart.addSelectionListener( lsSelMod );

    wUsePart.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        if ( wUsePart.getSelection() ) {
          wNameInField.setSelection( false );
        }
        setFlags();
      }
    } );

    // Partitioning field
    wlPartField = new Label( wMainComp, SWT.RIGHT );
    wlPartField.setText( BaseMessages.getString( PKG, "TableOutputDialog.PartField.Label" ) );
    props.setLook( wlPartField );
    FormData fdlPartField = new FormData();
    fdlPartField.top = new FormAttachment( wUsePart, margin );
    fdlPartField.left = new FormAttachment( 0, 0 );
    fdlPartField.right = new FormAttachment( middle, -margin );
    wlPartField.setLayoutData( fdlPartField );
    wPartField = new ComboVar( variables, wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPartField );
    FormData fdPartField = new FormData();
    fdPartField.top = new FormAttachment( wlPartField,  0, SWT.CENTER );
    fdPartField.left = new FormAttachment( middle, 0 );
    fdPartField.right = new FormAttachment( 100, 0 );
    wPartField.setLayoutData( fdPartField );
    wPartField.addFocusListener( new FocusListener() {
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

    // Partition per month
    wlPartMonthly = new Label( wMainComp, SWT.RIGHT );
    wlPartMonthly.setText( BaseMessages.getString( PKG, "TableOutputDialog.PartMonthly.Label" ) );
    wlPartMonthly.setToolTipText( BaseMessages.getString( PKG, "TableOutputDialog.PartMonthly.Tooltip" ) );
    props.setLook( wlPartMonthly );
    FormData fdlPartMonthly = new FormData();
    fdlPartMonthly.left = new FormAttachment( 0, 0 );
    fdlPartMonthly.top = new FormAttachment( wPartField, margin );
    fdlPartMonthly.right = new FormAttachment( middle, -margin );
    wlPartMonthly.setLayoutData( fdlPartMonthly );
    wPartMonthly = new Button( wMainComp, SWT.RADIO );
    props.setLook( wPartMonthly );
    FormData fdPartMonthly = new FormData();
    fdPartMonthly.left = new FormAttachment( middle, 0 );
    fdPartMonthly.top = new FormAttachment( wlPartMonthly, 0, SWT.CENTER );
    fdPartMonthly.right = new FormAttachment( 100, 0 );
    wPartMonthly.setLayoutData( fdPartMonthly );
    wPartMonthly.addSelectionListener( lsSelMod );

    wPartMonthly.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        wPartMonthly.setSelection( true );
        wPartDaily.setSelection( false );
      }
    } );

    // Partition per month
    wlPartDaily = new Label( wMainComp, SWT.RIGHT );
    wlPartDaily.setText( BaseMessages.getString( PKG, "TableOutputDialog.PartDaily.Label" ) );
    wlPartDaily.setToolTipText( BaseMessages.getString( PKG, "TableOutputDialog.PartDaily.Tooltip" ) );
    props.setLook( wlPartDaily );
    FormData fdlPartDaily = new FormData();
    fdlPartDaily.left = new FormAttachment( 0, 0 );
    fdlPartDaily.top = new FormAttachment( wPartMonthly, margin );
    fdlPartDaily.right = new FormAttachment( middle, -margin );
    wlPartDaily.setLayoutData( fdlPartDaily );
    wPartDaily = new Button( wMainComp, SWT.RADIO );
    props.setLook( wPartDaily );
    FormData fdPartDaily = new FormData();
    fdPartDaily.left = new FormAttachment( middle, 0 );
    fdPartDaily.top = new FormAttachment( wlPartDaily, 0, SWT.CENTER );
    fdPartDaily.right = new FormAttachment( 100, 0 );
    wPartDaily.setLayoutData( fdPartDaily );
    wPartDaily.addSelectionListener( lsSelMod );

    wPartDaily.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        wPartDaily.setSelection( true );
        wPartMonthly.setSelection( false );
      }
    } );

    // Batch update
    wlBatch = new Label( wMainComp, SWT.RIGHT );
    wlBatch.setText( BaseMessages.getString( PKG, "TableOutputDialog.Batch.Label" ) );
    props.setLook( wlBatch );
    FormData fdlBatch = new FormData();
    fdlBatch.left = new FormAttachment( 0, 0 );
    fdlBatch.top = new FormAttachment( wPartDaily, 5 * margin );
    fdlBatch.right = new FormAttachment( middle, -margin );
    wlBatch.setLayoutData( fdlBatch );
    wBatch = new Button( wMainComp, SWT.CHECK );
    props.setLook( wBatch );
    FormData fdBatch = new FormData();
    fdBatch.left = new FormAttachment( middle, 0 );
    fdBatch.top = new FormAttachment( wlBatch, 0, SWT.CENTER );
    fdBatch.right = new FormAttachment( 100, 0 );
    wBatch.setLayoutData( fdBatch );
    wBatch.addSelectionListener( lsSelMod );
    wBatch.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        setFlags();
      }
    } );

    // NameInField
    Label wlNameInField = new Label( wMainComp, SWT.RIGHT );
    wlNameInField.setText( BaseMessages.getString( PKG, "TableOutputDialog.NameInField.Label" ) );
    props.setLook( wlNameInField );
    FormData fdlNameInField = new FormData();
    fdlNameInField.left = new FormAttachment( 0, 0 );
    fdlNameInField.top = new FormAttachment( wBatch, margin * 5 );
    fdlNameInField.right = new FormAttachment( middle, -margin );
    wlNameInField.setLayoutData( fdlNameInField );
    wNameInField = new Button( wMainComp, SWT.CHECK );
    props.setLook( wNameInField );
    FormData fdNameInField = new FormData();
    fdNameInField.left = new FormAttachment( middle, 0 );
    fdNameInField.top = new FormAttachment( wlNameInField, 0, SWT.CENTER );
    fdNameInField.right = new FormAttachment( 100, 0 );
    wNameInField.setLayoutData( fdNameInField );
    wNameInField.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent se ) {
        if ( wNameInField.getSelection() ) {
          wUsePart.setSelection( false );
        }
        setFlags();
      }
    } );
    wNameInField.addSelectionListener( lsSelMod );

    // NameField size ...
    wlNameField = new Label( wMainComp, SWT.RIGHT );
    wlNameField.setText( BaseMessages.getString( PKG, "TableOutputDialog.NameField.Label" ) );
    props.setLook( wlNameField );
    FormData fdlNameField = new FormData();
    fdlNameField.left = new FormAttachment( 0, 0 );
    fdlNameField.top = new FormAttachment( wNameInField, margin );
    fdlNameField.right = new FormAttachment( middle, -margin );
    wlNameField.setLayoutData( fdlNameField );
    wNameField = new ComboVar( variables, wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wNameField );
    FormData fdNameField = new FormData();
    fdNameField.left = new FormAttachment( middle, 0 );
    fdNameField.top = new FormAttachment( wlNameField,  0, SWT.CENTER );
    fdNameField.right = new FormAttachment( 100, 0 );
    wNameField.setLayoutData( fdNameField );
    wNameField.addFocusListener( new FocusListener() {
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

    // NameInTable
    wlNameInTable = new Label( wMainComp, SWT.RIGHT );
    wlNameInTable.setText( BaseMessages.getString( PKG, "TableOutputDialog.NameInTable.Label" ) );
    props.setLook( wlNameInTable );
    FormData fdlNameInTable = new FormData();
    fdlNameInTable.left = new FormAttachment( 0, 0 );
    fdlNameInTable.top = new FormAttachment( wNameField, margin );
    fdlNameInTable.right = new FormAttachment( middle, -margin );
    wlNameInTable.setLayoutData( fdlNameInTable );
    wNameInTable = new Button( wMainComp, SWT.CHECK );
    props.setLook( wNameInTable );
    FormData fdNameInTable = new FormData();
    fdNameInTable.left = new FormAttachment( middle, 0 );
    fdNameInTable.top = new FormAttachment( wlNameInTable, 0, SWT.CENTER );
    fdNameInTable.right = new FormAttachment( 100, 0 );
    wNameInTable.setLayoutData( fdNameInTable );
    wNameInTable.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        setFlags();
      }
    } );
    wNameInTable.addSelectionListener( lsSelMod );

    // Return generated keys?
    Label wlReturnKeys = new Label( wMainComp, SWT.RIGHT );
    wlReturnKeys.setText( BaseMessages.getString( PKG, "TableOutputDialog.ReturnKeys.Label" ) );
    wlReturnKeys.setToolTipText( BaseMessages.getString( PKG, "TableOutputDialog.ReturnKeys.Tooltip" ) );
    props.setLook( wlReturnKeys );
    FormData fdlReturnKeys = new FormData();
    fdlReturnKeys.left = new FormAttachment( 0, 0 );
    fdlReturnKeys.top = new FormAttachment( wNameInTable, margin * 5 );
    fdlReturnKeys.right = new FormAttachment( middle, -margin );
    wlReturnKeys.setLayoutData( fdlReturnKeys );
    wReturnKeys = new Button( wMainComp, SWT.CHECK );
    props.setLook( wReturnKeys );
    FormData fdReturnKeys = new FormData();
    fdReturnKeys.left = new FormAttachment( middle, 0 );
    fdReturnKeys.top = new FormAttachment( wlReturnKeys,  0, SWT.CENTER );
    fdReturnKeys.right = new FormAttachment( 100, 0 );
    wReturnKeys.setLayoutData( fdReturnKeys );
    wReturnKeys.addSelectionListener( lsSelMod );
    wReturnKeys.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        setFlags();
      }
    } );

    // ReturnField size ...
    wlReturnField = new Label( wMainComp, SWT.RIGHT );
    wlReturnField.setText( BaseMessages.getString( PKG, "TableOutputDialog.ReturnField.Label" ) );
    props.setLook( wlReturnField );
    FormData fdlReturnField = new FormData();
    fdlReturnField.left = new FormAttachment( 0, 0 );
    fdlReturnField.right = new FormAttachment( middle, -margin );
    fdlReturnField.top = new FormAttachment( wReturnKeys, margin );
    wlReturnField.setLayoutData( fdlReturnField );
    wReturnField = new TextVar( variables, wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wReturnField );
    FormData fdReturnField = new FormData();
    fdReturnField.left = new FormAttachment( middle, 0 );
    fdReturnField.top = new FormAttachment( wlReturnField, 0, SWT.CENTER );
    fdReturnField.right = new FormAttachment( 100, 0 );
    wReturnField.setLayoutData(fdReturnField);

    FormData fdMainComp = new FormData();
    fdMainComp.left = new FormAttachment( 0, 0 );
    fdMainComp.top = new FormAttachment( 0, 0 );
    fdMainComp.right = new FormAttachment( 100, 0 );
    fdMainComp.bottom = new FormAttachment( 100, 0 );
    wMainComp.setLayoutData( fdMainComp );

    wMainComp.layout();
    wMainTab.setControl( wMainComp );

    //
    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "TableOutputDialog.FieldsTab.CTabItem.Title" ) );

    Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFieldsComp );

    FormLayout fieldsCompLayout = new FormLayout();
    fieldsCompLayout.marginWidth = Const.FORM_MARGIN;
    fieldsCompLayout.marginHeight = Const.FORM_MARGIN;
    wFieldsComp.setLayout( fieldsCompLayout );

    // The fields table
    Label wlFields = new Label(wFieldsComp, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "TableOutputDialog.InsertFields.Label" ) );
    props.setLook(wlFields);
    FormData fdlUpIns = new FormData();
    fdlUpIns.left = new FormAttachment( 0, 0 );
    fdlUpIns.top = new FormAttachment( 0, margin );
    wlFields.setLayoutData( fdlUpIns );

    int tableCols = 2;
    int UpInsRows = ( input.getFieldStream() != null ? input.getFieldStream().length : 1 );

    ciFields = new ColumnInfo[ tableCols ];
    ciFields[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "TableOutputDialog.ColumnInfo.TableField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciFields[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "TableOutputDialog.ColumnInfo.StreamField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    tableFieldColumns.add( ciFields[ 0 ] );
    wFields =
      new TableView( variables, wFieldsComp, SWT.BORDER
        | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciFields, UpInsRows, null, props );

    wGetFields = new Button( wFieldsComp, SWT.PUSH );
    wGetFields.setText( BaseMessages.getString( PKG, "TableOutputDialog.GetFields.Button" ) );
    FormData fdGetFields = new FormData();
    fdGetFields.top = new FormAttachment(wlFields, margin );
    fdGetFields.right = new FormAttachment( 100, 0 );
    wGetFields.setLayoutData(fdGetFields);

    wDoMapping = new Button( wFieldsComp, SWT.PUSH );
    wDoMapping.setText( BaseMessages.getString( PKG, "TableOutputDialog.DoMapping.Button" ) );
    FormData fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment( wGetFields, margin );
    fdDoMapping.right = new FormAttachment( 100, 0 );
    wDoMapping.setLayoutData(fdDoMapping);

    wDoMapping.addListener( SWT.Selection, arg0 -> generateMappings() );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment( wDoMapping, -margin );
    fdFields.bottom = new FormAttachment( 100, -2 * margin );
    wFields.setLayoutData( fdFields );

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );

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

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCreate = new Button( shell, SWT.PUSH );
    wCreate.setText( BaseMessages.getString( PKG, "System.Button.SQL" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel, wCreate }, margin, null );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wSpecifyFields, 3*margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -margin );
    wTabFolder.setLayoutData( fdTabFolder );

    // Add listeners
    lsOk = e -> ok();
    lsCreate = e -> sql();
    lsCancel = e -> cancel();
    lsGet = e -> get();

    wOk.addListener( SWT.Selection, lsOk );
    wCreate.addListener( SWT.Selection, lsCreate );
    wCancel.addListener( SWT.Selection, lsCancel );
    wGetFields.addListener( SWT.Selection, lsGet );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wCommit.addSelectionListener( lsDef );
    wSchema.addSelectionListener( lsDef );
    wTable.addSelectionListener( lsDef );
    wPartField.addSelectionListener( lsDef );
    wNameField.addSelectionListener( lsDef );
    wReturnField.addSelectionListener( lsDef );

    wbTable.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getTableName();
      }
    } );
    wbSchema.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getSchemaNames();
      }
    } );
    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    setTableFieldCombo();
    input.setChanged( backupChanged );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void getFields() {
    if ( !gotPreviousFields ) {
      try {
        String field = wNameField.getText();
        String partfield = wPartField.getText();
        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wNameField.setItems( r.getFieldNames() );
          wPartField.setItems( r.getFieldNames() );
        }
        if ( field != null ) {
          wNameField.setText( field );
        }
        if ( partfield != null ) {
          wPartField.setText( partfield );
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "TableOutputDialog.FailedToGetFields.DialogTitle" ), BaseMessages
          .getString( PKG, "TableOutputDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
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
        BaseMessages.getString( PKG, "TableOutputDialog.DoMapping.UnableToFindSourceFields.Title" ),
        BaseMessages.getString( PKG, "TableOutputDialog.DoMapping.UnableToFindSourceFields.Message" ), e );
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
        BaseMessages.getString( PKG, "TableOutputDialog.DoMapping.UnableToFindTargetFields.Title" ),
        BaseMessages.getString( PKG, "TableOutputDialog.DoMapping.UnableToFindTargetFields.Message" ), e );
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

    int nrFields = wFields.nrNonEmpty();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
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
        message += BaseMessages.getString( PKG, "TableOutputDialog.DoMapping.SomeSourceFieldsNotFound",
          missingSourceFields.toString() ) + Const.CR;
      }
      if ( missingTargetFields.length() > 0 ) {
        message += BaseMessages.getString( PKG, "TableOutputDialog.DoMapping.SomeTargetFieldsNotFound",
          missingSourceFields.toString() ) + Const.CR;
      }
      message += Const.CR;
      message +=
        BaseMessages.getString( PKG, "TableOutputDialog.DoMapping.SomeFieldsNotFoundContinue" ) + Const.CR;
      MessageDialog.setDefaultImage( GuiResource.getInstance().getImageHopUi() );
      boolean goOn =
        MessageDialog.openConfirm( shell, BaseMessages.getString(
          PKG, "TableOutputDialog.DoMapping.SomeFieldsNotFoundTitle" ), message );
      if ( !goOn ) {
        return;
      }
    }
    EnterMappingDialog d =
      new EnterMappingDialog( TableOutputDialog.this.shell, sourceFields.getFieldNames(), targetFields
        .getFieldNames(), mappings );
    mappings = d.open();

    // mappings == null if the user pressed cancel
    //
    if ( mappings != null ) {
      // Clear and re-populate!
      //
      wFields.table.removeAll();
      wFields.table.setItemCount( mappings.size() );
      for ( int i = 0; i < mappings.size(); i++ ) {
        SourceToTargetMapping mapping = mappings.get( i );
        TableItem item = wFields.table.getItem( i );
        item.setText( 2, sourceFields.getValueMeta( mapping.getSourcePosition() ).getName() );
        item.setText( 1, targetFields.getValueMeta( mapping.getTargetPosition() ).getName() );
      }
      wFields.setRowNums();
      wFields.optWidth( true );
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
              PKG, "TableOutputDialog.AvailableSchemas.Title", wConnection.getText() ), BaseMessages
              .getString( PKG, "TableOutputDialog.AvailableSchemas.Message", wConnection.getText() ) );
          String d = dialog.open();
          if ( d != null ) {
            wSchema.setText( Const.NVL( d, "" ) );
            setTableFieldCombo();
          }

        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "TableOutputDialog.NoSchema.Error" ) );
          mb.setText( BaseMessages.getString( PKG, "TableOutputDialog.GetSchemas.Error" ) );
          mb.open();
        }
      } catch ( Exception e ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
          .getString( PKG, "TableOutputDialog.ErrorGettingSchemas" ), e );
      } finally {
        database.disconnect();
      }
    }
  }

  private void validateSelection() {
    Runnable fieldLoader = () -> {
      isConnectionSupported();
    };
    shell.getDisplay().asyncExec( fieldLoader );
  }

  protected void isConnectionSupported() {
    final String tableName = wTable.getText(), connectionName = wConnection.getText();

    if ( !Utils.isEmpty( tableName ) ) {
      DatabaseMeta dbmeta = pipelineMeta.findDatabase( connectionName );
      if ( dbmeta != null && !dbmeta.getIDatabase().supportsStandardTableOutput() ) {
        showUnsupportedConnectionMessageBox( dbmeta.getIDatabase() );
      }
    }
  }

  protected void showUnsupportedConnectionMessageBox( IDatabase dbi ) {
    String title = BaseMessages.getString( PKG, "TableOutput.UnsupportedConnection.DialogTitle" );
    String message = dbi.getUnsupportedTableOutputMessage();
    String close = BaseMessages.getString( PKG, "System.Button.Close" );

    MessageDialog dialog =
      new MessageDialog( shell, title, GuiResource.getInstance().getImageHopUi(), message, SWT.ICON_WARNING,
        new String[] { close }, 0 );
    dialog.open();
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
                // ignore any errors here. Nothing we can do if
                // connection fails to close properly
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

    String[] fieldNames = entries.toArray( new String[ entries.size() ] );

    Const.sortStrings( fieldNames );
    ciFields[ 1 ].setComboValues( fieldNames );
  }

  public void setFlags() {
    // Do we want to return keys?
    boolean returnKeys = wReturnKeys.getSelection();

    // Can't use batch yet when grabbing auto-generated keys or sometimes when we use error handling
    boolean useBatch = wBatch.getSelection() && !returnKeys;

    // Only enable batch option when not returning keys.
    boolean enableBatch = !returnKeys;

    // Can't ignore errors when using batch inserts.
    boolean useIgnore = !useBatch;

    // Do we use partitioning?
    boolean usePartitioning = wUsePart.getSelection();

    // Do we get the tablename from a field?
    boolean isTableNameInField = wNameInField.getSelection();

    // Can't truncate when doing partitioning or with table name in field
    boolean enableTruncate = !( usePartitioning || isTableNameInField );

    // Do we need to turn partitioning off?
    boolean useTruncate = wTruncate.getSelection() && enableTruncate;

    // Use the table-name specified or get it from a field?
    boolean useTablename = !( isTableNameInField );

    wUsePart.setSelection( usePartitioning );
    wNameInField.setSelection( isTableNameInField );
    wBatch.setSelection( useBatch );
    wReturnKeys.setSelection( returnKeys );
    wTruncate.setSelection( useTruncate );

    wIgnore.setEnabled( useIgnore );
    wlIgnore.setEnabled( useIgnore );

    wlPartMonthly.setEnabled( usePartitioning );
    wPartMonthly.setEnabled( usePartitioning );
    wlPartDaily.setEnabled( usePartitioning );
    wPartDaily.setEnabled( usePartitioning );
    wlPartField.setEnabled( usePartitioning );
    wPartField.setEnabled( usePartitioning );

    wlNameField.setEnabled( isTableNameInField );
    wNameField.setEnabled( isTableNameInField );
    wlNameInTable.setEnabled( isTableNameInField );
    wNameInTable.setEnabled( isTableNameInField );

    wlTable.setEnabled( useTablename );
    wTable.setEnabled( useTablename );

    wlTruncate.setEnabled( enableTruncate );
    wTruncate.setEnabled( enableTruncate );

    wlReturnField.setEnabled( returnKeys );
    wReturnField.setEnabled( returnKeys );

    wlBatch.setEnabled( enableBatch );
    wBatch.setEnabled( enableBatch );

    boolean specifyFields = wSpecifyFields.getSelection();
    wFields.setEnabled( specifyFields );
    wGetFields.setEnabled( specifyFields );
    wDoMapping.setEnabled( specifyFields );

    // If people specify the fields we don't want them to use the "store name
    // in table" button.
    wlNameInTable.setEnabled( isTableNameInField && !specifyFields );
    wNameInTable.setEnabled( isTableNameInField && !specifyFields );

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase( wConnection.getText() );
    if ( databaseMeta != null ) {
      if ( !databaseMeta.supportsAutoGeneratedKeys() ) {
        wReturnKeys.setEnabled( false );
        wReturnKeys.setSelection( false );

        wReturnField.setEnabled( false );
        wReturnField.setText( "" );
      } else {
        wReturnKeys.setEnabled( true );
        wReturnField.setEnabled( true );
      }
    } else {
      wReturnKeys.setEnabled( true );
      wReturnField.setEnabled( true );
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getSchemaName() != null ) {
      wSchema.setText( input.getSchemaName() );
    }
    if ( input.getTableName() != null ) {
      wTable.setText( input.getTableName() );
    }
    if ( input.getDatabaseMeta() != null ) {
      wConnection.setText( input.getDatabaseMeta().getName() );
    }

    wTruncate.setSelection( input.truncateTable() );
    wIgnore.setSelection( input.ignoreErrors() );
    wBatch.setSelection( input.useBatchUpdate() );

    wCommit.setText( input.getCommitSize() );

    wUsePart.setSelection( input.isPartitioningEnabled() );
    wPartDaily.setSelection( input.isPartitioningDaily() );
    wPartMonthly.setSelection( input.isPartitioningMonthly() );
    if ( input.getPartitioningField() != null ) {
      wPartField.setText( input.getPartitioningField() );
    }

    wNameInField.setSelection( input.isTableNameInField() );
    if ( input.getTableNameField() != null ) {
      wNameField.setText( input.getTableNameField() );
    }
    wNameInTable.setSelection( input.isTableNameInTable() );

    wReturnKeys.setSelection( input.isReturningGeneratedKeys() );
    if ( input.getGeneratedKeyField() != null ) {
      wReturnField.setText( input.getGeneratedKeyField() );
    }

    wSpecifyFields.setSelection( input.specifyFields() );

    for ( int i = 0; i < input.getFieldDatabase().length; i++ ) {
      TableItem item = wFields.table.getItem( i );
      if ( input.getFieldDatabase()[ i ] != null ) {
        item.setText( 1, input.getFieldDatabase()[ i ] );
      }
      if ( input.getFieldStream()[ i ] != null ) {
        item.setText( 2, input.getFieldStream()[ i ] );
      }
    }

    setFlags();

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void getInfo( TableOutputMeta info ) {
    info.setSchemaName( wSchema.getText() );
    info.setTableName( wTable.getText() );
    info.setDatabaseMeta( pipelineMeta.findDatabase( wConnection.getText() ) );
    info.setCommitSize( wCommit.getText() );
    info.setTruncateTable( wTruncate.getSelection() );
    info.setIgnoreErrors( wIgnore.getSelection() );
    info.setUseBatchUpdate( wBatch.getSelection() );
    info.setPartitioningEnabled( wUsePart.getSelection() );
    info.setPartitioningField( wPartField.getText() );
    info.setPartitioningDaily( wPartDaily.getSelection() );
    info.setPartitioningMonthly( wPartMonthly.getSelection() );
    info.setTableNameInField( wNameInField.getSelection() );
    info.setTableNameField( wNameField.getText() );
    info.setTableNameInTable( wNameInTable.getSelection() );
    info.setReturningGeneratedKeys( wReturnKeys.getSelection() );
    info.setGeneratedKeyField( wReturnField.getText() );
    info.setSpecifyFields( wSpecifyFields.getSelection() );

    int nrRows = wFields.nrNonEmpty();
    info.allocate( nrRows );
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrRows; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      info.getFieldDatabase()[ i ] = Const.NVL( item.getText( 1 ), "" );
      info.getFieldStream()[ i ] = Const.NVL( item.getText( 2 ), "" );
    }
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo( input );

    if ( input.getDatabaseMeta() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "TableOutputDialog.ConnectionError.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
      mb.open();
      return;
    }

    // PDI-6211 : Show a warning in case batch processing is not truly supported in combination
    // with error handling...
    // Show it *every* time the user OK's the dialog.
    //
    DatabaseMeta databaseMeta = input.getDatabaseMeta();
    boolean supportsBatchErrorHandling =
      databaseMeta != null && databaseMeta.supportsErrorHandlingOnBatchUpdates();
    boolean hasErrorHandling = transformMeta.isDoingErrorHandling();
    if ( !supportsBatchErrorHandling && hasErrorHandling ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_WARNING );
      mb.setMessage( BaseMessages.getString(
        PKG, "TableOutput.Warning.ErrorHandlingIsNotFullySupportedWithBatchProcessing" ) );
      mb.setText( BaseMessages.getString( PKG, "TableOutput.Warning" ) );
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
        logDebug( BaseMessages.getString( PKG, "TableOutputDialog.Log.LookingAtConnection", databaseMeta.toString() ) );
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
      mb.setMessage( BaseMessages.getString( PKG, "TableOutputDialog.ConnectionError2.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
      mb.open();
    }

  }

  /**
   * Fill up the fields table with the incoming fields.
   */
  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null && !r.isEmpty() ) {
        BaseTransformDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, null );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "TableOutputDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "TableOutputDialog.FailedToGetFields.DialogMessage" ), ke );
    }

  }

  // Generate code for create table...
  // Conversions done by Database
  //
  private void sql() {
    try {
      TableOutputMeta info = new TableOutputMeta();
      getInfo( info );
      IRowMeta prev = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( info.isTableNameInField() && !info.isTableNameInTable() && info.getTableNameField().length() > 0 ) {
        int idx = prev.indexOfValue( info.getTableNameField() );
        if ( idx >= 0 ) {
          prev.removeValueMeta( idx );
        }
      }
      TransformMeta transformMeta = pipelineMeta.findTransform( transformName );

      if ( info.specifyFields() ) {
        // Only use the fields that were specified.
        IRowMeta prevNew = new RowMeta();

        for ( int i = 0; i < info.getFieldDatabase().length; i++ ) {
          IValueMeta insValue = prev.searchValueMeta( info.getFieldStream()[ i ] );
          if ( insValue != null ) {
            IValueMeta insertValue = insValue.clone();
            insertValue.setName( info.getFieldDatabase()[ i ] );
            prevNew.addValueMeta( insertValue );
          } else {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "TableOutputDialog.FailedToFindField.Message", info.getFieldStream()[ i ] ) );
          }
        }
        prev = prevNew;
      }

      boolean autoInc = false;
      String pk = null;

      // Add the auto-increment field too if any is present.
      //
      if ( info.isReturningGeneratedKeys() && !Utils.isEmpty( info.getGeneratedKeyField() ) ) {
        IValueMeta valueMeta = new ValueMetaInteger( info.getGeneratedKeyField() );
        valueMeta.setLength( 15 );
        prev.addValueMeta( 0, valueMeta );
        autoInc = true;
        pk = info.getGeneratedKeyField();
      }

      if ( isValidRowMeta( prev ) ) {
        SqlStatement sql = info.getSqlStatements( variables, pipelineMeta, transformMeta, prev, pk, autoInc, pk );
        if ( !sql.hasError() ) {
          if ( sql.hasSql() ) {
            SqlEditor sqledit = new SqlEditor( shell, SWT.NONE, variables,  info.getDatabaseMeta(), DbCache.getInstance(), sql.getSql() );
            sqledit.open();
          } else {
            String message = BaseMessages.getString( PKG, "TableOutputDialog.NoSQL.DialogMessage" );
            String text = BaseMessages.getString( PKG, "TableOutputDialog.NoSQL.DialogTitle" );
            showMessage( shell, SWT.OK | SWT.ICON_INFORMATION, message, text );
          }
        } else {
          String text = BaseMessages.getString( PKG, "System.Dialog.Error.Title" );
          showMessage( shell, SWT.OK | SWT.ICON_ERROR, sql.getError(), text );
        }
      } else {
        String message = BaseMessages.getString( PKG, "TableOutputDialog.NoSQL.EmptyCSVFields" );
        String text = BaseMessages.getString( PKG, "TableOutputDialog.NoSQL.DialogTitle" );
        showMessage( shell, SWT.OK | SWT.ICON_ERROR, message, text );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "TableOutputDialog.BuildSQLError.DialogTitle" ), BaseMessages
        .getString( PKG, "TableOutputDialog.BuildSQLError.DialogMessage" ), ke );
    }
  }

  private void showMessage( Shell shell, int style, String message, String text ) {
    MessageBox mb = new MessageBox( shell, style );
    mb.setMessage( message );
    mb.setText( text );
    mb.open();
  }

  private static boolean isValidRowMeta( IRowMeta rowMeta ) {
    for ( IValueMeta value : rowMeta.getValueMetaList() ) {
      String name = value.getName();
      if ( name == null || name.isEmpty() ) {
        return false;
      }
    }
    return true;
  }
}
