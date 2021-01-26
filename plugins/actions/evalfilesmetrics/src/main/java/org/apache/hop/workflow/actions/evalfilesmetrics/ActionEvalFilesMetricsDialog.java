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

package org.apache.hop.workflow.actions.evalfilesmetrics;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

/**
 * This dialog allows you to edit the eval files metrics action settings.
 *
 * @author Samatar Hassan
 * @since 26-02-2010
 */
public class ActionEvalFilesMetricsDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionEvalFilesMetrics.class; // For Translator

  private static final String[] FILETYPES = new String[] { BaseMessages.getString(
    PKG, "JobEvalFilesMetrics.Filetype.All" ) };

  private Text wName;

  private Label wlSourceFileFolder;
  private Button wbSourceFileFolder, wbSourceDirectory;

  private TextVar wSourceFileFolder;

  private ActionEvalFilesMetrics action;
  private Shell shell;

  private boolean changed;

  private Label wlFields;

  private TableView wFields;

  private Label wlWildcard;
  private TextVar wWildcard;

  private Label wlResultFilenamesWildcard;
  private TextVar wResultFilenamesWildcard;

  private Label wlResultFieldFile;
  private TextVar wResultFieldFile;

  private Label wlResultFieldWildcard;
  private TextVar wResultFieldWildcard;

  private Label wlResultFieldIncludeSubFolders;
  private TextVar wResultFieldIncludeSubFolders;

  private Button wbdSourceFileFolder; // Delete
  private Button wbeSourceFileFolder; // Edit
  private Button wbaSourceFileFolder; // Add or change

  private CCombo wSuccessNumberCondition;

  private Label wlScale;
  private CCombo wScale;

  private CCombo wSourceFiles;

  private CCombo wEvaluationType;

  private Label wlCompareValue;
  private TextVar wCompareValue;

  private Label wlMinValue;
  private TextVar wMinValue;

  private Label wlMaxValue;
  private TextVar wMaxValue;

  public ActionEvalFilesMetricsDialog( Shell parent, IAction action,
                                       WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionEvalFilesMetrics) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Name.Label" ) );
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData(fdlName);
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData(fdName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Tab.General.Label" ) );

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // SETTINGS grouping?
    // ////////////////////////
    // START OF SETTINGS GROUP
    //

    Group wSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wSettings);
    wSettings.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Settings.Label" ) );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wSettings.setLayout( groupLayout );

    // SourceFiles
    Label wlSourceFiles = new Label(wSettings, SWT.RIGHT);
    wlSourceFiles.setText( BaseMessages.getString( PKG, "JobEvalFilesMetricsDialog.SourceFiles.Label" ) );
    props.setLook(wlSourceFiles);
    FormData fdlSourceFiles = new FormData();
    fdlSourceFiles.left = new FormAttachment( 0, 0 );
    fdlSourceFiles.right = new FormAttachment( middle, -margin );
    fdlSourceFiles.top = new FormAttachment( wName, margin );
    wlSourceFiles.setLayoutData(fdlSourceFiles);

    wSourceFiles = new CCombo(wSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wSourceFiles.setItems( ActionEvalFilesMetrics.SourceFilesDesc );
    wSourceFiles.select( 0 ); // +1: starts at -1

    props.setLook( wSourceFiles );
    FormData fdSourceFiles = new FormData();
    fdSourceFiles.left = new FormAttachment( middle, 0 );
    fdSourceFiles.top = new FormAttachment( wName, margin );
    fdSourceFiles.right = new FormAttachment( 100, 0 );
    wSourceFiles.setLayoutData(fdSourceFiles);
    wSourceFiles.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        RefreshSourceFiles();
      }
    } );

    // ResultFilenamesWildcard
    wlResultFilenamesWildcard = new Label(wSettings, SWT.RIGHT );
    wlResultFilenamesWildcard.setText( BaseMessages.getString(
      PKG, "JobEvalFilesMetrics.ResultFilenamesWildcard.Label" ) );
    props.setLook( wlResultFilenamesWildcard );
    FormData fdlResultFilenamesWildcard = new FormData();
    fdlResultFilenamesWildcard.left = new FormAttachment( 0, 0 );
    fdlResultFilenamesWildcard.top = new FormAttachment( wSourceFiles, margin );
    fdlResultFilenamesWildcard.right = new FormAttachment( middle, -margin );
    wlResultFilenamesWildcard.setLayoutData(fdlResultFilenamesWildcard);

    wResultFilenamesWildcard = new TextVar( variables, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResultFilenamesWildcard.setToolTipText( BaseMessages.getString(
      PKG, "JobEvalFilesMetrics.ResultFilenamesWildcard.Tooltip" ) );
    props.setLook( wResultFilenamesWildcard );
    wResultFilenamesWildcard.addModifyListener( lsMod );
    FormData fdResultFilenamesWildcard = new FormData();
    fdResultFilenamesWildcard.left = new FormAttachment( middle, 0 );
    fdResultFilenamesWildcard.top = new FormAttachment( wSourceFiles, margin );
    fdResultFilenamesWildcard.right = new FormAttachment( 100, -margin );
    wResultFilenamesWildcard.setLayoutData(fdResultFilenamesWildcard);

    // ResultFieldFile
    wlResultFieldFile = new Label(wSettings, SWT.RIGHT );
    wlResultFieldFile.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.ResultFieldFile.Label" ) );
    props.setLook( wlResultFieldFile );
    FormData fdlResultFieldFile = new FormData();
    fdlResultFieldFile.left = new FormAttachment( 0, 0 );
    fdlResultFieldFile.top = new FormAttachment( wResultFilenamesWildcard, margin );
    fdlResultFieldFile.right = new FormAttachment( middle, -margin );
    wlResultFieldFile.setLayoutData(fdlResultFieldFile);

    wResultFieldFile = new TextVar( variables, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResultFieldFile.setToolTipText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.ResultFieldFile.Tooltip" ) );
    props.setLook( wResultFieldFile );
    wResultFieldFile.addModifyListener( lsMod );
    FormData fdResultFieldFile = new FormData();
    fdResultFieldFile.left = new FormAttachment( middle, 0 );
    fdResultFieldFile.top = new FormAttachment( wResultFilenamesWildcard, margin );
    fdResultFieldFile.right = new FormAttachment( 100, -margin );
    wResultFieldFile.setLayoutData(fdResultFieldFile);

    // ResultFieldWildcard
    wlResultFieldWildcard = new Label(wSettings, SWT.RIGHT );
    wlResultFieldWildcard.setText( BaseMessages.getString(
      PKG, "JobEvalWildcardsMetrics.ResultFieldWildcard.Label" ) );
    props.setLook( wlResultFieldWildcard );
    FormData fdlResultFieldWildcard = new FormData();
    fdlResultFieldWildcard.left = new FormAttachment( 0, 0 );
    fdlResultFieldWildcard.top = new FormAttachment( wResultFieldFile, margin );
    fdlResultFieldWildcard.right = new FormAttachment( middle, -margin );
    wlResultFieldWildcard.setLayoutData(fdlResultFieldWildcard);

    wResultFieldWildcard = new TextVar( variables, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResultFieldWildcard.setToolTipText( BaseMessages.getString(
      PKG, "JobEvalWildcardsMetrics.ResultFieldWildcard.Tooltip" ) );
    props.setLook( wResultFieldWildcard );
    wResultFieldWildcard.addModifyListener( lsMod );
    FormData fdResultFieldWildcard = new FormData();
    fdResultFieldWildcard.left = new FormAttachment( middle, 0 );
    fdResultFieldWildcard.top = new FormAttachment( wResultFieldFile, margin );
    fdResultFieldWildcard.right = new FormAttachment( 100, -margin );
    wResultFieldWildcard.setLayoutData(fdResultFieldWildcard);

    // ResultFieldIncludeSubFolders
    wlResultFieldIncludeSubFolders = new Label(wSettings, SWT.RIGHT );
    wlResultFieldIncludeSubFolders.setText( BaseMessages.getString(
      PKG, "JobEvalIncludeSubFolderssMetrics.ResultFieldIncludeSubFolders.Label" ) );
    props.setLook( wlResultFieldIncludeSubFolders );
    FormData fdlResultFieldIncludeSubFolders = new FormData();
    fdlResultFieldIncludeSubFolders.left = new FormAttachment( 0, 0 );
    fdlResultFieldIncludeSubFolders.top = new FormAttachment( wResultFieldWildcard, margin );
    fdlResultFieldIncludeSubFolders.right = new FormAttachment( middle, -margin );
    wlResultFieldIncludeSubFolders.setLayoutData(fdlResultFieldIncludeSubFolders);

    wResultFieldIncludeSubFolders = new TextVar( variables, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResultFieldIncludeSubFolders.setToolTipText( BaseMessages.getString(
      PKG, "JobEvalIncludeSubFolderssMetrics.ResultFieldIncludeSubFolders.Tooltip" ) );
    props.setLook( wResultFieldIncludeSubFolders );
    wResultFieldIncludeSubFolders.addModifyListener( lsMod );
    FormData fdResultFieldIncludeSubFolders = new FormData();
    fdResultFieldIncludeSubFolders.left = new FormAttachment( middle, 0 );
    fdResultFieldIncludeSubFolders.top = new FormAttachment( wResultFieldWildcard, margin );
    fdResultFieldIncludeSubFolders.right = new FormAttachment( 100, -margin );
    wResultFieldIncludeSubFolders.setLayoutData(fdResultFieldIncludeSubFolders);

    // EvaluationType
    Label wlEvaluationType = new Label(wSettings, SWT.RIGHT);
    wlEvaluationType.setText( BaseMessages.getString( PKG, "JobEvalFilesMetricsDialog.EvaluationType.Label" ) );
    props.setLook(wlEvaluationType);
    FormData fdlEvaluationType = new FormData();
    fdlEvaluationType.left = new FormAttachment( 0, 0 );
    fdlEvaluationType.right = new FormAttachment( middle, -margin );
    fdlEvaluationType.top = new FormAttachment( wResultFieldIncludeSubFolders, margin );
    wlEvaluationType.setLayoutData(fdlEvaluationType);

    wEvaluationType = new CCombo(wSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wEvaluationType.setItems( ActionEvalFilesMetrics.EvaluationTypeDesc );
    wEvaluationType.select( 0 ); // +1: starts at -1

    props.setLook( wEvaluationType );
    FormData fdEvaluationType = new FormData();
    fdEvaluationType.left = new FormAttachment( middle, 0 );
    fdEvaluationType.top = new FormAttachment( wResultFieldIncludeSubFolders, margin );
    fdEvaluationType.right = new FormAttachment( 100, 0 );
    wEvaluationType.setLayoutData(fdEvaluationType);
    wEvaluationType.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        RefreshSize();
        action.setChanged();
      }
    } );

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment( 0, margin );
    fdSettings.top = new FormAttachment( wName, margin );
    fdSettings.right = new FormAttachment( 100, -margin );
    wSettings.setLayoutData(fdSettings);

    // ///////////////////////////////////////////////////////////
    // / END OF SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // SourceFileFolder line
    wlSourceFileFolder = new Label(wGeneralComp, SWT.RIGHT );
    wlSourceFileFolder.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.SourceFileFolder.Label" ) );
    props.setLook( wlSourceFileFolder );
    FormData fdlSourceFileFolder = new FormData();
    fdlSourceFileFolder.left = new FormAttachment( 0, 0 );
    fdlSourceFileFolder.top = new FormAttachment(wSettings, 2 * margin );
    fdlSourceFileFolder.right = new FormAttachment( middle, -margin );
    wlSourceFileFolder.setLayoutData(fdlSourceFileFolder);

    // Browse Source folders button ...
    wbSourceDirectory = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSourceDirectory );
    wbSourceDirectory.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.BrowseFolders.Label" ) );
    FormData fdbSourceDirectory = new FormData();
    fdbSourceDirectory.right = new FormAttachment( 100, 0 );
    fdbSourceDirectory.top = new FormAttachment(wSettings, margin );
    wbSourceDirectory.setLayoutData(fdbSourceDirectory);

    wbSourceDirectory.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wSourceFileFolder, variables ));

    // Browse Source files button ...
    wbSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSourceFileFolder );
    wbSourceFileFolder.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.BrowseFiles.Label" ) );
    FormData fdbSourceFileFolder = new FormData();
    fdbSourceFileFolder.right = new FormAttachment( wbSourceDirectory, -margin );
    fdbSourceFileFolder.top = new FormAttachment(wSettings, margin );
    wbSourceFileFolder.setLayoutData(fdbSourceFileFolder);

    // Browse Destination file add button ...
    wbaSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbaSourceFileFolder );
    wbaSourceFileFolder.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.FilenameAdd.Button" ) );
    FormData fdbaSourceFileFolder = new FormData();
    fdbaSourceFileFolder.right = new FormAttachment( wbSourceFileFolder, -margin );
    fdbaSourceFileFolder.top = new FormAttachment(wSettings, margin );
    wbaSourceFileFolder.setLayoutData(fdbaSourceFileFolder);

    wSourceFileFolder = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSourceFileFolder
      .setToolTipText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.SourceFileFolder.Tooltip" ) );

    props.setLook( wSourceFileFolder );
    wSourceFileFolder.addModifyListener( lsMod );
    FormData fdSourceFileFolder = new FormData();
    fdSourceFileFolder.left = new FormAttachment( middle, 0 );
    fdSourceFileFolder.top = new FormAttachment(wSettings, 2 * margin );
    fdSourceFileFolder.right = new FormAttachment( wbSourceFileFolder, -55 );
    wSourceFileFolder.setLayoutData(fdSourceFileFolder);

    // Whenever something changes, set the tooltip to the expanded version:
    wSourceFileFolder.addModifyListener( e -> wSourceFileFolder.setToolTipText( variables.resolve( wSourceFileFolder.getText() ) ) );

    wbSourceFileFolder.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wSourceFileFolder, variables,
      new String[] { "*" }, FILETYPES, true )
    );


    // Buttons to the right of the screen...
    wbdSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdSourceFileFolder );
    wbdSourceFileFolder.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.FilenameDelete.Button" ) );
    wbdSourceFileFolder
      .setToolTipText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.FilenameDelete.Tooltip" ) );
    FormData fdbdSourceFileFolder = new FormData();
    fdbdSourceFileFolder.right = new FormAttachment( 100, 0 );
    fdbdSourceFileFolder.top = new FormAttachment( wSourceFileFolder, 40 );
    wbdSourceFileFolder.setLayoutData(fdbdSourceFileFolder);

    wbeSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbeSourceFileFolder );
    wbeSourceFileFolder.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.FilenameEdit.Button" ) );
    FormData fdbeSourceFileFolder = new FormData();
    fdbeSourceFileFolder.right = new FormAttachment( 100, 0 );
    fdbeSourceFileFolder.left = new FormAttachment( wbdSourceFileFolder, 0, SWT.LEFT );
    fdbeSourceFileFolder.top = new FormAttachment( wbdSourceFileFolder, margin );
    wbeSourceFileFolder.setLayoutData(fdbeSourceFileFolder);

    // Wildcard
    wlWildcard = new Label(wGeneralComp, SWT.RIGHT );
    wlWildcard.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Wildcard.Label" ) );
    props.setLook( wlWildcard );
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment( 0, 0 );
    fdlWildcard.top = new FormAttachment( wSourceFileFolder, margin );
    fdlWildcard.right = new FormAttachment( middle, -margin );
    wlWildcard.setLayoutData(fdlWildcard);

    wWildcard = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wWildcard.setToolTipText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Wildcard.Tooltip" ) );
    props.setLook( wWildcard );
    wWildcard.addModifyListener( lsMod );
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment( middle, 0 );
    fdWildcard.top = new FormAttachment( wSourceFileFolder, margin );
    fdWildcard.right = new FormAttachment( wbSourceFileFolder, -55 );
    wWildcard.setLayoutData(fdWildcard);

    wlFields = new Label(wGeneralComp, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.right = new FormAttachment( middle, -margin );
    fdlFields.top = new FormAttachment( wWildcard, margin );
    wlFields.setLayoutData(fdlFields);

    int rows = action.getSourceFileFolder() == null ? 1
      : ( action.getSourceFileFolder().length == 0 ? 0 : action.getSourceFileFolder().length );
    final int FieldsRows = rows;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobEvalFilesMetrics.Fields.SourceFileFolder.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobEvalFilesMetrics.Fields.Wildcard.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobEvalFilesMetrics.Fields.IncludeSubDirs.Label" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ActionEvalFilesMetrics.IncludeSubFoldersDesc ) };

    colinf[ 0 ].setUsingVariables( true );
    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Fields.SourceFileFolder.Tooltip" ) );
    colinf[ 1 ].setUsingVariables( true );
    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Fields.Wildcard.Tooltip" ) );

    wFields =
      new TableView(
    		  variables, wGeneralComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( wbeSourceFileFolder, -margin );
    fdFields.bottom = new FormAttachment( 100, -margin );
    wFields.setLayoutData(fdFields);

    // RefreshArgFromPrevious();

    // Add the file to the list of files...
    SelectionAdapter selA = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        wFields.add( new String[] { wSourceFileFolder.getText(), wWildcard.getText() } );
        wSourceFileFolder.setText( "" );

        wWildcard.setText( "" );
        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth( true );
      }
    };
    wbaSourceFileFolder.addSelectionListener( selA );
    wSourceFileFolder.addSelectionListener( selA );

    // Delete files from the list of files...
    wbdSourceFileFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        int[] idx = wFields.getSelectionIndices();
        wFields.remove( idx );
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

    // Edit the selected file & remove from the list...
    wbeSourceFileFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        int idx = wFields.getSelectionIndex();
        if ( idx >= 0 ) {
          String[] string = wFields.getItem( idx );
          wSourceFileFolder.setText( string[ 0 ] );
          wWildcard.setText( string[ 1 ] );
          wFields.remove( idx );
        }
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

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

    // ////////////////////////////////////
    // START OF ADVANCED TAB ///
    // ///////////////////////////////////

    CTabItem wAdvancedTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdvancedTab.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.Tab.Advanced.Label" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wAdvancedComp);
    wAdvancedComp.setLayout( contentLayout );

    // SuccessOngrouping?
    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    Group wSuccessOn = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    props.setLook(wSuccessOn);
    wSuccessOn.setText( BaseMessages.getString( PKG, "JobEvalFilesMetrics.SuccessOn.Group.Label" ) );

    FormLayout successongroupLayout = new FormLayout();
    successongroupLayout.marginWidth = 10;
    successongroupLayout.marginHeight = 10;

    wSuccessOn.setLayout( successongroupLayout );

    // Scale
    wlScale = new Label(wSuccessOn, SWT.RIGHT );
    wlScale.setText( BaseMessages.getString( PKG, "JobEvalFilesMetricsDialog.Scale.Label" ) );
    props.setLook( wlScale );
    FormData fdlScale = new FormData();
    fdlScale.left = new FormAttachment( 0, 0 );
    fdlScale.right = new FormAttachment( middle, -margin );
    fdlScale.top = new FormAttachment( 0, margin );
    wlScale.setLayoutData(fdlScale);

    wScale = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wScale.setItems( ActionEvalFilesMetrics.scaleDesc );
    wScale.select( 0 ); // +1: starts at -1

    props.setLook( wScale );
    FormData fdScale = new FormData();
    fdScale.left = new FormAttachment( middle, 0 );
    fdScale.top = new FormAttachment( 0, margin );
    fdScale.right = new FormAttachment( 100, 0 );
    wScale.setLayoutData(fdScale);
    wScale.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Success number Condition
    Label wlSuccessNumberCondition = new Label(wSuccessOn, SWT.RIGHT);
    wlSuccessNumberCondition.setText( BaseMessages.getString(
      PKG, "JobEvalFilesMetricsDialog.SuccessCondition.Label" ) );
    props.setLook(wlSuccessNumberCondition);
    FormData fdlSuccessNumberCondition = new FormData();
    fdlSuccessNumberCondition.left = new FormAttachment( 0, 0 );
    fdlSuccessNumberCondition.right = new FormAttachment( middle, -margin );
    fdlSuccessNumberCondition.top = new FormAttachment( wScale, margin );
    wlSuccessNumberCondition.setLayoutData(fdlSuccessNumberCondition);

    wSuccessNumberCondition = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wSuccessNumberCondition.setItems( ActionEvalFilesMetrics.successNumberConditionDesc );
    wSuccessNumberCondition.select( 0 ); // +1: starts at -1

    props.setLook( wSuccessNumberCondition );
    FormData fdSuccessNumberCondition = new FormData();
    fdSuccessNumberCondition.left = new FormAttachment( middle, 0 );
    fdSuccessNumberCondition.top = new FormAttachment( wScale, margin );
    fdSuccessNumberCondition.right = new FormAttachment( 100, 0 );
    wSuccessNumberCondition.setLayoutData(fdSuccessNumberCondition);
    wSuccessNumberCondition.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        refresh();
        action.setChanged();
      }
    } );

    // Compare with value
    wlCompareValue = new Label(wSuccessOn, SWT.RIGHT );
    wlCompareValue.setText( BaseMessages.getString( PKG, "JobEvalFilesMetricsDialog.CompareValue.Label" ) );
    props.setLook( wlCompareValue );
    FormData fdlCompareValue = new FormData();
    fdlCompareValue.left = new FormAttachment( 0, 0 );
    fdlCompareValue.top = new FormAttachment( wSuccessNumberCondition, margin );
    fdlCompareValue.right = new FormAttachment( middle, -margin );
    wlCompareValue.setLayoutData(fdlCompareValue);

    wCompareValue =
      new TextVar( variables, wSuccessOn, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobEvalFilesMetricsDialog.CompareValue.Tooltip" ) );
    props.setLook( wCompareValue );
    wCompareValue.addModifyListener( lsMod );
    FormData fdCompareValue = new FormData();
    fdCompareValue.left = new FormAttachment( middle, 0 );
    fdCompareValue.top = new FormAttachment( wSuccessNumberCondition, margin );
    fdCompareValue.right = new FormAttachment( 100, -margin );
    wCompareValue.setLayoutData(fdCompareValue);

    // Min value
    wlMinValue = new Label(wSuccessOn, SWT.RIGHT );
    wlMinValue.setText( BaseMessages.getString( PKG, "JobEvalFilesMetricsDialog.MinValue.Label" ) );
    props.setLook( wlMinValue );
    FormData fdlMinValue = new FormData();
    fdlMinValue.left = new FormAttachment( 0, 0 );
    fdlMinValue.top = new FormAttachment( wSuccessNumberCondition, margin );
    fdlMinValue.right = new FormAttachment( middle, -margin );
    wlMinValue.setLayoutData(fdlMinValue);

    wMinValue =
      new TextVar( variables, wSuccessOn, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobEvalFilesMetricsDialog.MinValue.Tooltip" ) );
    props.setLook( wMinValue );
    wMinValue.addModifyListener( lsMod );
    FormData fdMinValue = new FormData();
    fdMinValue.left = new FormAttachment( middle, 0 );
    fdMinValue.top = new FormAttachment( wSuccessNumberCondition, margin );
    fdMinValue.right = new FormAttachment( 100, -margin );
    wMinValue.setLayoutData(fdMinValue);

    // Maximum value
    wlMaxValue = new Label(wSuccessOn, SWT.RIGHT );
    wlMaxValue.setText( BaseMessages.getString( PKG, "JobEvalFilesMetricsDialog.MaxValue.Label" ) );
    props.setLook( wlMaxValue );
    FormData fdlMaxValue = new FormData();
    fdlMaxValue.left = new FormAttachment( 0, 0 );
    fdlMaxValue.top = new FormAttachment( wMinValue, margin );
    fdlMaxValue.right = new FormAttachment( middle, -margin );
    wlMaxValue.setLayoutData(fdlMaxValue);

    wMaxValue =
      new TextVar( variables, wSuccessOn, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobEvalFilesMetricsDialog.MaxValue.Tooltip" ) );
    props.setLook( wMaxValue );
    wMaxValue.addModifyListener( lsMod );
    FormData fdMaxValue = new FormData();
    fdMaxValue.left = new FormAttachment( middle, 0 );
    fdMaxValue.top = new FormAttachment( wMinValue, margin );
    fdMaxValue.right = new FormAttachment( 100, -margin );
    wMaxValue.setLayoutData(fdMaxValue);

    FormData fdSuccessOn = new FormData();
    fdSuccessOn.left = new FormAttachment( 0, margin );
    fdSuccessOn.top = new FormAttachment( 0, margin );
    fdSuccessOn.right = new FormAttachment( 100, -margin );
    wSuccessOn.setLayoutData(fdSuccessOn);
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdAdvancedComp = new FormData();
    fdAdvancedComp.left = new FormAttachment( 0, 0 );
    fdAdvancedComp.top = new FormAttachment( 0, 0 );
    fdAdvancedComp.right = new FormAttachment( 100, 0 );
    fdAdvancedComp.bottom = new FormAttachment( 100, 0 );
    wAdvancedComp.setLayoutData(wAdvancedComp);

    wAdvancedComp.layout();
    wAdvancedTab.setControl(wAdvancedComp);

    // ///////////////////////////////////////////////////////////
    // / END OF ADVANCED TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData(fdTabFolder);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, wTabFolder);

    // Add listeners
    Listener lsCancel = e -> cancel();
    Listener lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel);
    wOk.addListener( SWT.Selection, lsOk);

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    wName.addSelectionListener(lsDef);
    wSourceFileFolder.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    refresh();
    RefreshSize();
    RefreshSourceFiles();
    wTabFolder.setSelection( 0 );
    BaseTransformDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  private void RefreshSourceFiles() {
    boolean useStaticFiles =
      ( ActionEvalFilesMetrics.getSourceFilesByDesc( wSourceFiles.getText() )
        == ActionEvalFilesMetrics.SOURCE_FILES_FILES );
    wlFields.setEnabled( useStaticFiles );
    wFields.setEnabled( useStaticFiles );
    wbdSourceFileFolder.setEnabled( useStaticFiles );
    wbeSourceFileFolder.setEnabled( useStaticFiles );
    wbSourceFileFolder.setEnabled( useStaticFiles );
    wbaSourceFileFolder.setEnabled( useStaticFiles );
    wlSourceFileFolder.setEnabled( useStaticFiles );
    wSourceFileFolder.setEnabled( useStaticFiles );

    wlWildcard.setEnabled( useStaticFiles );
    wWildcard.setEnabled( useStaticFiles );
    wbSourceDirectory.setEnabled( useStaticFiles );

    boolean setResultWildcard =
      ( ActionEvalFilesMetrics.getSourceFilesByDesc( wSourceFiles.getText() )
        == ActionEvalFilesMetrics.SOURCE_FILES_FILENAMES_RESULT );
    wlResultFilenamesWildcard.setEnabled( setResultWildcard );
    wResultFilenamesWildcard.setEnabled( setResultWildcard );

    boolean setResultFields =
      ( ActionEvalFilesMetrics.getSourceFilesByDesc( wSourceFiles.getText() )
        == ActionEvalFilesMetrics.SOURCE_FILES_PREVIOUS_RESULT );
    wlResultFieldIncludeSubFolders.setEnabled( setResultFields );
    wResultFieldIncludeSubFolders.setEnabled( setResultFields );
    wlResultFieldFile.setEnabled( setResultFields );
    wResultFieldFile.setEnabled( setResultFields );
    wlResultFieldWildcard.setEnabled( setResultFields );
    wResultFieldWildcard.setEnabled( setResultFields );
  }

  private void RefreshSize() {
    boolean useSize =
      ( ActionEvalFilesMetrics.getEvaluationTypeByDesc( wEvaluationType.getText() )
        == ActionEvalFilesMetrics.EVALUATE_TYPE_SIZE );
    wlScale.setVisible( useSize );
    wScale.setVisible( useSize );
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( action.getName() != null ) {
      wName.setText( action.getName() );
    }

    if ( action.getSourceFileFolder() != null ) {
      for ( int i = 0; i < action.getSourceFileFolder().length; i++ ) {
        TableItem ti = wFields.table.getItem( i );
        if ( action.getSourceFileFolder()[ i ] != null ) {
          ti.setText( 1, action.getSourceFileFolder()[ i ] );
        }

        if ( action.getSourceWildcard()[ i ] != null ) {
          ti.setText( 2, action.getSourceWildcard()[ i ] );
        }

        if ( action.getSourceIncludeSubfolders()[ i ] != null ) {
          ti.setText( 3,
            ActionEvalFilesMetrics.getIncludeSubFoldersDesc( action.getSourceIncludeSubfolders()[ i ] ) );
        }
      }
      wFields.setRowNums();
      wFields.optWidth( true );
    }
    if ( action.getResultFilenamesWildcard() != null ) {
      wResultFilenamesWildcard.setText( action.getResultFilenamesWildcard() );
    }
    if ( action.getResultFieldFile() != null ) {
      wResultFieldFile.setText( action.getResultFieldFile() );
    }
    if ( action.getResultFieldWildcard() != null ) {
      wResultFieldWildcard.setText( action.getResultFieldWildcard() );
    }
    if ( action.getResultFieldIncludeSubfolders() != null ) {
      wResultFieldIncludeSubFolders.setText( action.getResultFieldIncludeSubfolders() );
    }
    wSourceFiles.setText( ActionEvalFilesMetrics.getSourceFilesDesc( action.sourceFiles ) );
    wEvaluationType.setText( ActionEvalFilesMetrics.getEvaluationTypeDesc( action.evaluationType ) );
    wScale.setText( ActionEvalFilesMetrics.getScaleDesc( action.scale ) );
    wSuccessNumberCondition.setText( ActionEvalFilesMetrics
      .getSuccessNumberConditionDesc( action.getSuccessConditionType() ) );
    if ( action.getCompareValue() != null ) {
      wCompareValue.setText( action.getCompareValue() );
    }
    if ( action.getMinValue() != null ) {
      wMinValue.setText( action.getMinValue() );
    }
    if ( action.getMaxValue() != null ) {
      wMaxValue.setText( action.getMaxValue() );
    }

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged( changed );
    action = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    action.setName( wName.getText() );
    action.setResultFilenamesWildcard( wResultFilenamesWildcard.getText() );
    action.setResultFieldFile( wResultFieldFile.getText() );
    action.setResultFieldWildcard( wResultFieldWildcard.getText() );
    action.setResultFieldIncludeSubfolders( wResultFieldIncludeSubFolders.getText() );
    action.sourceFiles = ActionEvalFilesMetrics.getSourceFilesByDesc( wSourceFiles.getText() );
    action.evaluationType = ActionEvalFilesMetrics.getEvaluationTypeByDesc( wEvaluationType.getText() );
    action.scale = ActionEvalFilesMetrics.getScaleByDesc( wScale.getText() );
    action.setSuccessConditionType(
    		ActionEvalFilesMetrics.getSuccessNumberConditionByDesc( wSuccessNumberCondition.getText() ) );
    action.setCompareValue( wCompareValue.getText() );
    action.setMinValue( wMinValue.getText() );
    action.setMaxValue( wMaxValue.getText() );
    int nrItems = wFields.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        nr++;
      }
    }
    String[] sourceFileFolder = new String[ nr ];
    String[] sourceWildcard = new String[ nr ];
    String[] sourceIncludeSubfolders = new String[ nr ];
    nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String source = wFields.getNonEmpty( i ).getText( 1 );
      String wild = wFields.getNonEmpty( i ).getText( 2 );
      String includeSubFolders = wFields.getNonEmpty( i ).getText( 3 );
      if ( source != null && source.length() != 0 ) {
        sourceFileFolder[ nr ] = source;
        sourceWildcard[ nr ] = wild;
        sourceIncludeSubfolders[ nr ] = ActionEvalFilesMetrics.getIncludeSubFolders( includeSubFolders );
        nr++;
      }
    }
    action.setSourceFileFolder( sourceFileFolder );
    action.setSourceWildcard( sourceWildcard );
    action.setSourceIncludeSubfolders( sourceIncludeSubfolders );
    dispose();
  }

  private void refresh() {
    boolean compareValue =
      ( ActionEvalFilesMetrics.getSuccessNumberConditionByDesc( wSuccessNumberCondition.getText() )
        != ActionEvalFilesMetrics.SUCCESS_NUMBER_CONDITION_BETWEEN );
    wlCompareValue.setVisible( compareValue );
    wCompareValue.setVisible( compareValue );
    wlMinValue.setVisible( !compareValue );
    wMinValue.setVisible( !compareValue );
    wlMaxValue.setVisible( !compareValue );
    wMaxValue.setVisible( !compareValue );
  }
}
