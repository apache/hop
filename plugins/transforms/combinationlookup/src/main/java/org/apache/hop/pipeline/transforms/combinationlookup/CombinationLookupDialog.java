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

package org.apache.hop.pipeline.transforms.combinationlookup;

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
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
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

public class CombinationLookupDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = CombinationLookupDialog.class; // For Translator

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wSchema;

  private TextVar wTable;

  private Text wCommit;

  private Text wCachesize;

  private Button wPreloadCache;

  private Text wTk;

  private Label wlAutoinc;
  private Button wAutoinc;

  private Label wlTableMax;
  private Button wTableMax;

  private Label wlSeqButton;
  private Button wSeqButton;
  private Text wSeq;

  private Button wReplace;

  private Button wHashcode;

  private TableView wKey;

  private Label wlHashfield;
  private Text wHashfield;

  private Text wLastUpdateField;

  private ColumnInfo[] ciKey;

  private final CombinationLookupMeta input;

  private DatabaseMeta ci;

  private final Map<String, Integer> inputFields;

  /**
   * List of ColumnInfo that should have the field names of the selected database table
   */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  public CombinationLookupDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (CombinationLookupMeta) in;
    inputFields = new HashMap<>();
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

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
    backupChanged = input.hasChanged();
    ci = input.getDatabaseMeta();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.TransformName.Label" ) );
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
    wConnection.addModifyListener( e -> {
      // We have new content: change ci connection:
      ci = pipelineMeta.findDatabase( wConnection.getText() );
      setAutoincUse();
      setSequence();
      input.setChanged();
    } );

    // Schema line...
    Label wlSchema = new Label( shell, SWT.RIGHT );
    wlSchema.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.TargetSchema.Label" ) );
    props.setLook( wlSchema );
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment( 0, 0 );
    fdlSchema.right = new FormAttachment( middle, -margin );
    fdlSchema.top = new FormAttachment( wConnection, margin );
    wlSchema.setLayoutData( fdlSchema );

    Button wbSchema = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSchema );
    wbSchema.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment( wConnection, margin );
    fdbSchema.right = new FormAttachment( 100, 0 );
    wbSchema.setLayoutData( fdbSchema );

    wSchema = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchema );
    wSchema.addModifyListener( lsTableMod );
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment( middle, 0 );
    fdSchema.top = new FormAttachment( wConnection, margin );
    fdSchema.right = new FormAttachment( wbSchema, -margin );
    wSchema.setLayoutData( fdSchema );

    // Table line...
    Label wlTable = new Label( shell, SWT.RIGHT );
    wlTable.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.Target.Label" ) );
    props.setLook( wlTable );
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment( 0, 0 );
    fdlTable.right = new FormAttachment( middle, -margin );
    fdlTable.top = new FormAttachment( wbSchema, margin );
    wlTable.setLayoutData( fdlTable );

    Button wbTable = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTable );
    wbTable.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.BrowseTable.Button" ) );
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment( wbSchema, margin );
    wbTable.setLayoutData( fdbTable );

    wTable = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTable );
    wTable.addModifyListener( lsTableMod );
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment( middle, 0 );
    fdTable.top = new FormAttachment( wbSchema, margin );
    fdTable.right = new FormAttachment( wbTable, -margin );
    wTable.setLayoutData( fdTable );

    // Commit size ...
    Label wlCommit = new Label( shell, SWT.RIGHT );
    wlCommit.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.Commitsize.Label" ) );
    props.setLook( wlCommit );
    FormData fdlCommit = new FormData();
    fdlCommit.left = new FormAttachment( 0, 0 );
    fdlCommit.right = new FormAttachment( middle, -margin );
    fdlCommit.top = new FormAttachment( wTable, margin );
    wlCommit.setLayoutData( fdlCommit );
    wCommit = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCommit );
    wCommit.addModifyListener( lsMod );
    FormData fdCommit = new FormData();
    fdCommit.top = new FormAttachment( wTable, margin );
    fdCommit.left = new FormAttachment( middle, 0 );
    fdCommit.right = new FormAttachment( middle + ( 100 - middle ) / 3, -margin );
    wCommit.setLayoutData( fdCommit );

    // Cache size
    Label wlCachesize = new Label( shell, SWT.RIGHT );
    wlCachesize.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.Cachesize.Label" ) );
    props.setLook( wlCachesize );
    FormData fdlCachesize = new FormData();
    fdlCachesize.top = new FormAttachment( wTable, margin );
    fdlCachesize.left = new FormAttachment( wCommit, margin );
    fdlCachesize.right = new FormAttachment( middle + 2 * ( 100 - middle ) / 3, -margin );
    wlCachesize.setLayoutData( fdlCachesize );
    wCachesize = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCachesize );
    wCachesize.addModifyListener( lsMod );
    FormData fdCachesize = new FormData();
    fdCachesize.top = new FormAttachment( wTable, margin );
    fdCachesize.left = new FormAttachment( wlCachesize, margin );
    fdCachesize.right = new FormAttachment( 100, 0 );
    wCachesize.setLayoutData( fdCachesize );
    wCachesize.setToolTipText( BaseMessages.getString( PKG, "CombinationLookupDialog.Cachesize.ToolTip" ) );

    // Preload Cache
    Label wlPreloadCache = new Label( shell, SWT.RIGHT );
    wlPreloadCache.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.PreloadCache.Label" ) );
    props.setLook( wlPreloadCache );
    FormData fdlPreloadCache = new FormData();
    fdlPreloadCache.top = new FormAttachment( wCachesize, margin );
    fdlPreloadCache.left = new FormAttachment( wCommit, margin );
    fdlPreloadCache.right = new FormAttachment( middle + 2 * ( 100 - middle ) / 3, -margin );
    wlPreloadCache.setLayoutData( fdlPreloadCache );
    wPreloadCache = new Button( shell, SWT.CHECK );
    props.setLook( wPreloadCache );
    FormData fdPreloadCache = new FormData();
    fdPreloadCache.top = new FormAttachment( wlPreloadCache, 0, SWT.CENTER );
    fdPreloadCache.left = new FormAttachment( wlPreloadCache, margin );
    fdPreloadCache.right = new FormAttachment( 100, 0 );
    wPreloadCache.setLayoutData( fdPreloadCache );

    //
    // The Lookup fields: usually the (business) key
    //
    Label wlKey = new Label( shell, SWT.NONE );
    wlKey.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.Keyfields.Label" ) );
    props.setLook( wlKey );
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment( 0, 0 );
    fdlKey.top = new FormAttachment( wlPreloadCache, margin );
    fdlKey.right = new FormAttachment( 100, 0 );
    wlKey.setLayoutData( fdlKey );

    int nrKeyCols = 2;
    int nrKeyRows = ( input.getKeyField() != null ? input.getKeyField().length : 1 );

    ciKey = new ColumnInfo[ nrKeyCols ];
    ciKey[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "CombinationLookupDialog.ColumnInfo.DimensionField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciKey[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "CombinationLookupDialog.ColumnInfo.FieldInStream" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    tableFieldColumns.add( ciKey[ 0 ] );
    wKey =
      new TableView(
        variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKey,
        nrKeyRows, lsMod, props );

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.GetFields.Button" ) );
    Button wCreate = new Button( shell, SWT.PUSH );
    wCreate.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.SQL.Button" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    setButtonPositions( new Button[] { wOk, wGet, wCreate, wCancel }, margin, null );

    // Last update field:
    Label wlLastUpdateField = new Label( shell, SWT.RIGHT );
    wlLastUpdateField.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.LastUpdateField.Label" ) );
    props.setLook( wlLastUpdateField );
    FormData fdlLastUpdateField = new FormData();
    fdlLastUpdateField.left = new FormAttachment( 0, 0 );
    fdlLastUpdateField.right = new FormAttachment( middle, -margin );
    fdlLastUpdateField.bottom = new FormAttachment( wOk, -2 * margin );
    wlLastUpdateField.setLayoutData( fdlLastUpdateField );
    wLastUpdateField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLastUpdateField );
    wLastUpdateField.addModifyListener( lsMod );
    FormData fdLastUpdateField = new FormData();
    fdLastUpdateField.left = new FormAttachment( middle, 0 );
    fdLastUpdateField.right = new FormAttachment( 100, 0 );
    fdLastUpdateField.bottom = new FormAttachment( wOk, -2 * margin );
    wLastUpdateField.setLayoutData( fdLastUpdateField );

    // Hash field:
    wlHashfield = new Label( shell, SWT.RIGHT );
    wlHashfield.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.Hashfield.Label" ) );
    props.setLook( wlHashfield );
    FormData fdlHashfield = new FormData();
    fdlHashfield.left = new FormAttachment( 0, 0 );
    fdlHashfield.right = new FormAttachment( middle, -margin );
    fdlHashfield.bottom = new FormAttachment( wLastUpdateField, -margin );
    wlHashfield.setLayoutData( fdlHashfield );
    wHashfield = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHashfield );
    wHashfield.addModifyListener( lsMod );
    FormData fdHashfield = new FormData();
    fdHashfield.left = new FormAttachment( middle, 0 );
    fdHashfield.right = new FormAttachment( 100, 0 );
    fdHashfield.bottom = new FormAttachment( wLastUpdateField, -margin );
    wHashfield.setLayoutData( fdHashfield );

    // Output the input rows or one (1) log-record?
    Label wlHashcode = new Label( shell, SWT.RIGHT );
    wlHashcode.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.Hashcode.Label" ) );
    props.setLook( wlHashcode );
    FormData fdlHashcode = new FormData();
    fdlHashcode.left = new FormAttachment( 0, 0 );
    fdlHashcode.right = new FormAttachment( middle, -margin );
    fdlHashcode.bottom = new FormAttachment( wHashfield, -margin );
    wlHashcode.setLayoutData( fdlHashcode );
    wHashcode = new Button( shell, SWT.CHECK );
    props.setLook( wHashcode );
    FormData fdHashcode = new FormData();
    fdHashcode.left = new FormAttachment( middle, 0 );
    fdHashcode.right = new FormAttachment( 100, 0 );
    fdHashcode.bottom = new FormAttachment( wlHashcode, 0, SWT.CENTER );
    wHashcode.setLayoutData( fdHashcode );
    wHashcode.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        enableFields();
      }
    } );

    // Replace lookup fields in the output stream?
    Label wlReplace = new Label( shell, SWT.RIGHT );
    wlReplace.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.Replace.Label" ) );
    props.setLook( wlReplace );
    FormData fdlReplace = new FormData();
    fdlReplace.left = new FormAttachment( 0, 0 );
    fdlReplace.right = new FormAttachment( middle, -margin );
    fdlReplace.bottom = new FormAttachment( wHashcode, -margin );
    wlReplace.setLayoutData( fdlReplace );
    wReplace = new Button( shell, SWT.CHECK );
    props.setLook( wReplace );
    FormData fdReplace = new FormData();
    fdReplace.left = new FormAttachment( middle, 0 );
    fdReplace.bottom = new FormAttachment( wlReplace, 0, SWT.CENTER );
    fdReplace.right = new FormAttachment( 100, 0 );
    wReplace.setLayoutData( fdReplace );
    wReplace.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        enableFields();
      }
    } );

    Group gTechGroup = new Group( shell, SWT.SHADOW_ETCHED_IN );
    gTechGroup.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.TechGroup.Label" ) );
    GridLayout gridLayout = new GridLayout( 3, false );
    gTechGroup.setLayout( gridLayout );
    FormData fdTechGroup = new FormData();
    fdTechGroup.left = new FormAttachment( middle, 0 );
    fdTechGroup.bottom = new FormAttachment( wReplace, -margin );
    fdTechGroup.right = new FormAttachment( 100, 0 );
    gTechGroup.setBackground( shell.getBackground() ); // the default looks ugly
    gTechGroup.setLayoutData( fdTechGroup );

    // Use maximum of table + 1
    wTableMax = new Button( gTechGroup, SWT.RADIO );
    props.setLook( wTableMax );
    wTableMax.setSelection( false );
    GridData gdTableMax = new GridData();
    wTableMax.setLayoutData( gdTableMax );
    wTableMax.setToolTipText( BaseMessages.getString(
      PKG, "CombinationLookupDialog.TableMaximum.Tooltip", Const.CR ) );
    wlTableMax = new Label( gTechGroup, SWT.LEFT );
    wlTableMax.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.TableMaximum.Label" ) );
    props.setLook( wlTableMax );
    GridData gdlTableMax = new GridData( GridData.FILL_BOTH );
    gdlTableMax.horizontalSpan = 2;
    gdlTableMax.verticalSpan = 1;
    wlTableMax.setLayoutData( gdlTableMax );

    // Sequence Check Button
    wSeqButton = new Button( gTechGroup, SWT.RADIO );
    props.setLook( wSeqButton );
    wSeqButton.setSelection( false );
    GridData gdSeqButton = new GridData();
    wSeqButton.setLayoutData( gdSeqButton );
    wSeqButton
      .setToolTipText( BaseMessages.getString( PKG, "CombinationLookupDialog.Sequence.Tooltip", Const.CR ) );
    wlSeqButton = new Label( gTechGroup, SWT.LEFT );
    wlSeqButton.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.Sequence.Label" ) );
    props.setLook( wlSeqButton );
    GridData gdlSeqButton = new GridData();
    wlSeqButton.setLayoutData( gdlSeqButton );

    wSeq = new Text( gTechGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSeq );
    wSeq.addModifyListener( lsMod );
    GridData gdSeq = new GridData( GridData.FILL_HORIZONTAL );
    wSeq.setLayoutData( gdSeq );
    wSeq.addFocusListener( new FocusListener() {
      public void focusGained( FocusEvent arg0 ) {
        input.setTechKeyCreation( CombinationLookupMeta.CREATION_METHOD_SEQUENCE );
        wSeqButton.setSelection( true );
        wAutoinc.setSelection( false );
        wTableMax.setSelection( false );
      }

      public void focusLost( FocusEvent arg0 ) {
      }
    } );

    // Use an autoincrement field?
    wAutoinc = new Button( gTechGroup, SWT.RADIO );
    props.setLook( wAutoinc );
    wAutoinc.setSelection( false );
    GridData gdAutoinc = new GridData();
    wAutoinc.setLayoutData( gdAutoinc );
    wAutoinc.setToolTipText( BaseMessages.getString(
      PKG, "CombinationLookupDialog.AutoincButton.Tooltip", Const.CR ) );
    wlAutoinc = new Label( gTechGroup, SWT.LEFT );
    wlAutoinc.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.Autoincrement.Label" ) );
    props.setLook( wlAutoinc );
    GridData gdlAutoinc = new GridData();
    wlAutoinc.setLayoutData( gdlAutoinc );

    setTableMax();
    setSequence();
    setAutoincUse();

    // Technical key field:
    Label wlTk = new Label( shell, SWT.RIGHT );
    wlTk.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.TechnicalKey.Label" ) );
    props.setLook( wlTk );
    FormData fdlTk = new FormData();
    fdlTk.left = new FormAttachment( 0, 0 );
    fdlTk.right = new FormAttachment( middle, -margin );
    fdlTk.bottom = new FormAttachment( gTechGroup, -margin );
    wlTk.setLayoutData( fdlTk );
    wTk = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTk );
    FormData fdTk = new FormData();
    fdTk.left = new FormAttachment( middle, 0 );
    fdTk.bottom = new FormAttachment( gTechGroup, -margin );
    fdTk.right = new FormAttachment( 100, 0 );
    wTk.setLayoutData( fdTk );

    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment( 0, 0 );
    fdKey.top = new FormAttachment( wlKey, margin );
    fdKey.right = new FormAttachment( 100, 0 );
    fdKey.bottom = new FormAttachment( wTk, -margin );
    wKey.setLayoutData( fdKey );

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
    Listener lsGet = e -> get();
    Listener lsCreate = e -> create();
    lsCancel = e -> cancel();

    wOk.addListener( SWT.Selection, lsOk );
    wGet.addListener( SWT.Selection, lsGet );
    wCreate.addListener( SWT.Selection, lsCreate );
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
    wSeq.addSelectionListener( lsDef );
    wTk.addSelectionListener( lsDef );
    wCachesize.addSelectionListener( lsDef );
    wHashfield.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );
    wbSchema.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getSchemaNames();
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
    input.setChanged( backupChanged );

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
    // Key fields
    ciKey[ 1 ].setComboValues( fieldNames );
  }

  public void enableFields() {
    wHashfield.setEnabled( wHashcode.getSelection() );
    wHashfield.setVisible( wHashcode.getSelection() );
    wlHashfield.setEnabled( wHashcode.getSelection() );
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
            Database db = new Database( loggingObject, variables, ci );
            try {
              db.connect();

              String schemaTable =
                ci.getQuotedSchemaTableCombination( variables, schemaName, tableName);
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

  public void setAutoincUse() {
    boolean enable = ( ci == null ) || ( ci.supportsAutoinc() && ci.supportsAutoGeneratedKeys() );

    wlAutoinc.setEnabled( enable );
    wAutoinc.setEnabled( enable );
    if ( !enable && wAutoinc.getSelection() ) {
      wAutoinc.setSelection( false );
      wSeqButton.setSelection( false );
      wTableMax.setSelection( true );
    }
  }

  public void setTableMax() {
    wlTableMax.setEnabled( true );
    wTableMax.setEnabled( true );
  }

  public void setSequence() {
    boolean seq = ( ci == null ) || ci.supportsSequences();
    wSeq.setEnabled( seq );
    wlSeqButton.setEnabled( seq );
    wSeqButton.setEnabled( seq );
    if ( !seq && wSeqButton.getSelection() ) {
      wAutoinc.setSelection( false );
      wSeqButton.setSelection( false );
      wTableMax.setSelection( true );
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    logDebug( BaseMessages.getString( PKG, "CombinationLookupDialog.Log.GettingKeyInfo" ) );

    if ( input.getKeyField() != null ) {
      for ( int i = 0; i < input.getKeyField().length; i++ ) {
        TableItem item = wKey.table.getItem( i );
        if ( input.getKeyLookup()[ i ] != null ) {
          item.setText( 1, input.getKeyLookup()[ i ] );
        }
        if ( input.getKeyField()[ i ] != null ) {
          item.setText( 2, input.getKeyField()[ i ] );
        }
      }
    }

    wPreloadCache.setSelection( input.getPreloadCache() );
    wReplace.setSelection( input.replaceFields() );
    wHashcode.setSelection( input.useHash() );
    wHashfield.setEnabled( input.useHash() );
    wHashfield.setVisible( input.useHash() );
    wlHashfield.setEnabled( input.useHash() );

    String techKeyCreation = input.getTechKeyCreation();
    if ( techKeyCreation == null ) {
      // Determine the creation of the technical key for
      // backwards compatibility. Can probably be removed at
      // version 3.x or so (Sven Boden).
      DatabaseMeta database = input.getDatabaseMeta();
      if ( database == null || !database.supportsAutoinc() ) {
        input.setUseAutoinc( false );
      }
      wAutoinc.setSelection( input.isUseAutoinc() );

      wSeqButton.setSelection( input.getSequenceFrom() != null && input.getSequenceFrom().length() > 0 );
      if ( !input.isUseAutoinc() && ( input.getSequenceFrom() == null || input.getSequenceFrom().length() <= 0 ) ) {
        wTableMax.setSelection( true );
      }

      if ( database != null && database.supportsSequences() && input.getSequenceFrom() != null ) {
        wSeq.setText( input.getSequenceFrom() );
        input.setUseAutoinc( false );
        wTableMax.setSelection( false );
      }
    } else {
      // KETTLE post 2.2 version:
      // The "creation" field now determines the behaviour of the
      // key creation.
      if ( CombinationLookupMeta.CREATION_METHOD_AUTOINC.equals( techKeyCreation ) ) {
        wAutoinc.setSelection( true );
      } else if ( ( CombinationLookupMeta.CREATION_METHOD_SEQUENCE.equals( techKeyCreation ) ) ) {
        wSeqButton.setSelection( true );
      } else { // the rest
        wTableMax.setSelection( true );
        input.setTechKeyCreation( CombinationLookupMeta.CREATION_METHOD_TABLEMAX );
      }
      if ( input.getSequenceFrom() != null ) {
        wSeq.setText( input.getSequenceFrom() );
      }
    }
    setAutoincUse();
    setSequence();
    setTableMax();
    if ( input.getSchemaName() != null ) {
      wSchema.setText( input.getSchemaName() );
    }
    if ( input.getTableName() != null ) {
      wTable.setText( input.getTableName() );
    }
    if ( input.getTechnicalKeyField() != null ) {
      wTk.setText( input.getTechnicalKeyField() );
    }

    if ( input.getDatabaseMeta() != null ) {
      wConnection.setText( input.getDatabaseMeta().getName() );
    }
    if ( input.getHashField() != null ) {
      wHashfield.setText( input.getHashField() );
    }

    wCommit.setText( "" + input.getCommitSize() );
    wCachesize.setText( "" + input.getCacheSize() );

    wLastUpdateField.setText( Const.NVL( input.getLastUpdateField(), "" ) );

    wKey.setRowNums();
    wKey.optWidth( true );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    CombinationLookupMeta oldMetaState = (CombinationLookupMeta) input.clone();

    getInfo( input );
    transformName = wTransformName.getText(); // return value

    if ( pipelineMeta.findDatabase( wConnection.getText() ) == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "CombinationLookupDialog.NoValidConnection.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.NoValidConnection.DialogTitle" ) );
      mb.open();
    }
    if ( !input.equals( oldMetaState ) ) {
      input.setChanged();
    }
    dispose();
  }

  private void getInfo( CombinationLookupMeta in ) {
    int nrkeys = wKey.nrNonEmpty();

    in.allocate( nrkeys );

    logDebug( BaseMessages.getString( PKG, "CombinationLookupDialog.Log.SomeKeysFound", String.valueOf( nrkeys ) ) );
    for ( int i = 0; i < nrkeys; i++ ) {
      TableItem item = wKey.getNonEmpty( i );
      //CHECKSTYLE:Indentation:OFF
      in.getKeyLookup()[ i ] = item.getText( 1 );
      in.getKeyField()[ i ] = item.getText( 2 );
    }

    in.setPreloadCache( wPreloadCache.getSelection() );
    in.setUseAutoinc( wAutoinc.getSelection() && wAutoinc.isEnabled() );
    in.setReplaceFields( wReplace.getSelection() );
    in.setUseHash( wHashcode.getSelection() );
    in.setHashField( wHashfield.getText() );
    in.setSchemaName( wSchema.getText() );
    in.setTablename( wTable.getText() );
    in.setTechnicalKeyField( wTk.getText() );
    if ( wAutoinc.getSelection() ) {
      in.setTechKeyCreation( CombinationLookupMeta.CREATION_METHOD_AUTOINC );
      in.setUseAutoinc( true ); // for downwards compatibility
      in.setSequenceFrom( null );
    } else if ( wSeqButton.getSelection() ) {
      in.setTechKeyCreation( CombinationLookupMeta.CREATION_METHOD_SEQUENCE );
      in.setUseAutoinc( false );
      in.setSequenceFrom( wSeq.getText() );
    } else { // all the rest
      in.setTechKeyCreation( CombinationLookupMeta.CREATION_METHOD_TABLEMAX );
      in.setUseAutoinc( false );
      in.setSequenceFrom( null );
    }

    in.setDatabaseMeta( pipelineMeta.findDatabase( wConnection.getText() ) );

    in.setCommitSize( Const.toInt( wCommit.getText(), 0 ) );
    in.setCacheSize( Const.toInt( wCachesize.getText(), 0 ) );

    in.setLastUpdateField( wLastUpdateField.getText() );
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
              PKG, "CombinationLookupDialog.AvailableSchemas.Title", wConnection.getText() ), BaseMessages
              .getString( PKG, "CombinationLookupDialog.AvailableSchemas.Message", wConnection.getText() ) );
          String d = dialog.open();
          if ( d != null ) {
            wSchema.setText( Const.NVL( d, "" ) );
            setTableFieldCombo();
          }

        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "CombinationLookupDialog.NoSchema.Error" ) );
          mb.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.GetSchemas.Error" ) );
          mb.open();
        }
      } catch ( Exception e ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
          .getString( PKG, "CombinationLookupDialog.ErrorGettingSchemas" ), e );
      } finally {
        database.disconnect();
      }
    }
  }

  private void getTableName() {
    String connectionName = wConnection.getText();
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase( connectionName );
    if ( databaseMeta != null ) {
      logDebug( BaseMessages.getString( PKG, "CombinationLookupDialog.Log.LookingAtConnection", databaseMeta.toString() ) );

      DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases() );
      std.setSelectedSchemaAndTable( wSchema.getText(), wTable.getText() );
      if ( std.open() ) {
        wSchema.setText( Const.NVL( std.getSchemaName(), "" ) );
        wTable.setText( Const.NVL( std.getTableName(), "" ) );
        setTableFieldCombo();
      }
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "CombinationLookupDialog.ConnectionError2.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
      mb.open();
    }
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null && !r.isEmpty() ) {
        BaseTransformDialog.getFieldsFromPrevious(
          r, wKey, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, ( tableItem, v ) -> {
            tableItem.setText( 3, "N" );
            return true;
          } );
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString(
        PKG, "CombinationLookupDialog.UnableToGetFieldsError.DialogTitle" ), BaseMessages.getString(
        PKG, "CombinationLookupDialog.UnableToGetFieldsError.DialogMessage" ), ke );
    }
  }

  /**
   * Generate code for create table. Conversions done by database.
   */
  private void create() {
    try {
      // Gather info...
      CombinationLookupMeta info = new CombinationLookupMeta();
      getInfo( info );
      String name = transformName; // new name might not yet be linked to other transforms!
      TransformMeta transformMeta =
        new TransformMeta( BaseMessages.getString( PKG, "CombinationLookupDialog.TransformMeta.Title" ), name, info );
      IRowMeta prev = pipelineMeta.getPrevTransformFields( variables, transformName );

      SqlStatement sql = info.getSqlStatements( variables, pipelineMeta, transformMeta, prev, metadataProvider );
      if ( !sql.hasError() ) {
        if ( sql.hasSql() ) {
          SqlEditor sqledit =
            new SqlEditor( shell, SWT.NONE, variables, info.getDatabaseMeta(), DbCache.getInstance(), sql
              .getSql() );
          sqledit.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "CombinationLookupDialog.NoSQLNeeds.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.NoSQLNeeds.DialogTitle" ) );
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( sql.getError() );
        mb.setText( BaseMessages.getString( PKG, "CombinationLookupDialog.SQLError.DialogTitle" ) );
        mb.open();
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages
        .getString( PKG, "CombinationLookupDialog.UnableToCreateSQL.DialogTitle" ), BaseMessages.getString(
        PKG, "CombinationLookupDialog.UnableToCreateSQL.DialogMessage" ), ke );
    }
  }
}
