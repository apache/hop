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

package org.apache.hop.pipeline.transforms.exceloutput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.nio.charset.Charset;
import java.util.List;
import java.util.*;

public class ExcelOutputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ExcelOutputMeta.class; // For Translator

  private Label wlTempDirectory;
  private TextVar wTempDirectory;

  private Button wbTempDir;

  private CCombo wHeaderFontName;

  private TextVar wHeaderFontSize;

  private TextVar wRowFontSize;

  private CCombo wRowFontColor;

  private TextVar wImage;

  private TextVar wHeaderRowHeight;

  private CCombo wRowFontName;

  private CCombo wHeaderFontUnderline;

  private CCombo wHeaderFontOrientation;

  private CCombo wHeaderAlignment;

  private TextVar wFilename;

  private TextVar wExtension;

  private Button wAddTransformNr;

  private Label wlAddDate;
  private Button wAddDate;

  private Button wHeaderFontBold;

  private Button wHeaderFontItalic;

  private CCombo wHeaderFontColor;

  private CCombo wHeaderBackGroundColor;

  private CCombo wRowBackGroundColor;

  private Label wlAddTime;
  private Button wAddTime;

  private Button wProtectSheet;

  private Button wHeader;

  private Button wFooter;

  private CCombo wEncoding;

  private Text wSplitEvery;

  private Button wTemplate;

  private Label wlTemplateAppend;
  private Button wTemplateAppend;

  private Label wlTemplateFilename;
  private Button wbTemplateFilename;
  private TextVar wTemplateFilename;

  private Label wlPassword;
  private TextVar wPassword;

  private TextVar wSheetname;

  private TableView wFields;

  private final ExcelOutputMeta input;

  private Button wMinWidth;
  private boolean gotEncodings = false;

  private Button wAddToResult;

  private Button wAppend;

  private Button wDoNotOpenNewFileInit;

  private Button wSpecifyFormat;

  private Label wlDateTimeFormat;
  private CCombo wDateTimeFormat;

  private Button wAutoSize;

  private Button wNullIsBlank;

  private Button wUseTempFiles;

  private Button wCreateParentFolder;

  private ColumnInfo[] colinf;

  private final Map<String, Integer> inputFields;

  public ExcelOutputDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (ExcelOutputMeta) in;
    inputFields = new HashMap<>();
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Buttons at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null);


    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.top = new FormAttachment( 0, margin );
    fdlTransformName.right = new FormAttachment( middle, -margin );
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
    // START OF FILE TAB///
    // /
    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.FileTab.TabTitle" ) );

    Composite wFileComp = new Composite(wTabFolder, SWT.NONE );
    props.setLook( wFileComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout( fileLayout );

    // Filename line
    Label wlFilename = new Label(wFileComp, SWT.RIGHT);
    wlFilename.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.Filename.Label" ) );
    props.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( 0, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData(fdlFilename);

    Button wbFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbFilename);
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( 0, 0 );
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( 0, margin );
    fdFilename.right = new FormAttachment(wbFilename, -margin );
    wFilename.setLayoutData(fdFilename);

    // Create Parent Folder
    Label wlCreateParentFolder = new Label(wFileComp, SWT.RIGHT);
    wlCreateParentFolder.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.CreateParentFolder.Label" ) );
    props.setLook(wlCreateParentFolder);
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment( 0, 0 );
    fdlCreateParentFolder.top = new FormAttachment( wFilename, margin );
    fdlCreateParentFolder.right = new FormAttachment( middle, -margin );
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
    wCreateParentFolder = new Button( wFileComp, SWT.CHECK );
    wCreateParentFolder.setToolTipText( BaseMessages.getString(
      PKG, "ExcelOutputDialog.CreateParentFolder.Tooltip" ) );
    props.setLook( wCreateParentFolder );
    FormData fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment( middle, 0 );
    fdCreateParentFolder.top = new FormAttachment( wlCreateParentFolder, 0, SWT.CENTER );
    fdCreateParentFolder.right = new FormAttachment( 100, 0 );
    wCreateParentFolder.setLayoutData(fdCreateParentFolder);
    wCreateParentFolder.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Open new File at Init
    Label wlDoNotOpenNewFileInit = new Label(wFileComp, SWT.RIGHT);
    wlDoNotOpenNewFileInit.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.DoNotOpenNewFileInit.Label" ) );
    props.setLook(wlDoNotOpenNewFileInit);
    FormData fdlDoNotOpenNewFileInit = new FormData();
    fdlDoNotOpenNewFileInit.left = new FormAttachment( 0, 0 );
    fdlDoNotOpenNewFileInit.top = new FormAttachment( wCreateParentFolder, margin );
    fdlDoNotOpenNewFileInit.right = new FormAttachment( middle, -margin );
    wlDoNotOpenNewFileInit.setLayoutData(fdlDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit = new Button( wFileComp, SWT.CHECK );
    wDoNotOpenNewFileInit.setToolTipText( BaseMessages.getString(
      PKG, "ExcelOutputDialog.DoNotOpenNewFileInit.Tooltip" ) );
    props.setLook( wDoNotOpenNewFileInit );
    FormData fdDoNotOpenNewFileInit = new FormData();
    fdDoNotOpenNewFileInit.left = new FormAttachment( middle, 0 );
    fdDoNotOpenNewFileInit.top = new FormAttachment( wlDoNotOpenNewFileInit, 0, SWT.CENTER );
    fdDoNotOpenNewFileInit.right = new FormAttachment( 100, 0 );
    wDoNotOpenNewFileInit.setLayoutData(fdDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Extension line
    Label wlExtension = new Label(wFileComp, SWT.RIGHT);
    wlExtension.setText( BaseMessages.getString( PKG, "System.Label.Extension" ) );
    props.setLook(wlExtension);
    FormData fdlExtension = new FormData();
    fdlExtension.left = new FormAttachment( 0, 0 );
    fdlExtension.top = new FormAttachment( wDoNotOpenNewFileInit, margin );
    fdlExtension.right = new FormAttachment( middle, -margin );
    wlExtension.setLayoutData(fdlExtension);
    wExtension = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wExtension.setText( "" );
    props.setLook( wExtension );
    wExtension.addModifyListener( lsMod );
    FormData fdExtension = new FormData();
    fdExtension.left = new FormAttachment( middle, 0 );
    fdExtension.top = new FormAttachment( wDoNotOpenNewFileInit, margin );
    fdExtension.right = new FormAttachment(wbFilename, -margin );
    wExtension.setLayoutData(fdExtension);

    // Create multi-part file?
    Label wlAddTransformNr = new Label(wFileComp, SWT.RIGHT);
    wlAddTransformNr.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.AddTransformnr.Label" ) );
    props.setLook(wlAddTransformNr);
    FormData fdlAddTransformNr = new FormData();
    fdlAddTransformNr.left = new FormAttachment( 0, 0 );
    fdlAddTransformNr.top = new FormAttachment( wExtension, margin );
    fdlAddTransformNr.right = new FormAttachment( middle, -margin );
    wlAddTransformNr.setLayoutData(fdlAddTransformNr);
    wAddTransformNr = new Button( wFileComp, SWT.CHECK );
    props.setLook( wAddTransformNr );
    FormData fdAddTransformNr = new FormData();
    fdAddTransformNr.left = new FormAttachment( middle, 0 );
    fdAddTransformNr.top = new FormAttachment( wlAddTransformNr, 0, SWT.CENTER );
    fdAddTransformNr.right = new FormAttachment( 100, 0 );
    wAddTransformNr.setLayoutData(fdAddTransformNr);
    wAddTransformNr.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddDate = new Label( wFileComp, SWT.RIGHT );
    wlAddDate.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.AddDate.Label" ) );
    props.setLook( wlAddDate );
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment( 0, 0 );
    fdlAddDate.top = new FormAttachment( wAddTransformNr, margin );
    fdlAddDate.right = new FormAttachment( middle, -margin );
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button( wFileComp, SWT.CHECK );
    props.setLook( wAddDate );
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment( middle, 0 );
    fdAddDate.top = new FormAttachment( wlAddDate, 0, SWT.CENTER );
    fdAddDate.right = new FormAttachment( 100, 0 );
    wAddDate.setLayoutData(fdAddDate);
    wAddDate.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );
    // Create multi-part file?
    wlAddTime = new Label( wFileComp, SWT.RIGHT );
    wlAddTime.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.AddTime.Label" ) );
    props.setLook( wlAddTime );
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment( 0, 0 );
    fdlAddTime.top = new FormAttachment( wAddDate, margin );
    fdlAddTime.right = new FormAttachment( middle, -margin );
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button( wFileComp, SWT.CHECK );
    props.setLook( wAddTime );
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment( middle, 0 );
    fdAddTime.top = new FormAttachment( wlAddTime, 0, SWT.CENTER );
    fdAddTime.right = new FormAttachment( 100, 0 );
    wAddTime.setLayoutData(fdAddTime);
    wAddTime.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Specify date time format?
    Label wlSpecifyFormat = new Label(wFileComp, SWT.RIGHT);
    wlSpecifyFormat.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.SpecifyFormat.Label" ) );
    props.setLook(wlSpecifyFormat);
    FormData fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment( 0, 0 );
    fdlSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdlSpecifyFormat.right = new FormAttachment( middle, -margin );
    wlSpecifyFormat.setLayoutData(fdlSpecifyFormat);
    wSpecifyFormat = new Button( wFileComp, SWT.CHECK );
    props.setLook( wSpecifyFormat );
    wSpecifyFormat.setToolTipText( BaseMessages.getString( PKG, "ExcelOutputDialog.SpecifyFormat.Tooltip" ) );
    FormData fdSpecifyFormat = new FormData();
    fdSpecifyFormat.left = new FormAttachment( middle, 0 );
    fdSpecifyFormat.top = new FormAttachment( wlSpecifyFormat, 0, SWT.CENTER );
    fdSpecifyFormat.right = new FormAttachment( 100, 0 );
    wSpecifyFormat.setLayoutData(fdSpecifyFormat);
    wSpecifyFormat.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setDateTimeFormat();
      }
    } );

    // Prepare a list of possible DateTimeFormats...
    String[] dats = Const.getDateFormats();

    // DateTimeFormat
    wlDateTimeFormat = new Label( wFileComp, SWT.RIGHT );
    wlDateTimeFormat.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.DateTimeFormat.Label" ) );
    props.setLook( wlDateTimeFormat );
    FormData fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment( 0, 0 );
    fdlDateTimeFormat.top = new FormAttachment( wSpecifyFormat, 2*margin );
    fdlDateTimeFormat.right = new FormAttachment( middle, -margin );
    wlDateTimeFormat.setLayoutData(fdlDateTimeFormat);
    wDateTimeFormat = new CCombo( wFileComp, SWT.BORDER | SWT.READ_ONLY );
    wDateTimeFormat.setEditable( true );
    props.setLook( wDateTimeFormat );
    wDateTimeFormat.addModifyListener( lsMod );
    FormData fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment( middle, 0 );
    fdDateTimeFormat.top = new FormAttachment( wSpecifyFormat, 2*margin );
    fdDateTimeFormat.right = new FormAttachment( 100, 0 );
    wDateTimeFormat.setLayoutData(fdDateTimeFormat);
    for (String dat : dats) {
      wDateTimeFormat.add(dat);
    }

    Button wbShowFiles = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbShowFiles);
    wbShowFiles.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.ShowFiles.Button" ) );
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.top = new FormAttachment( wDateTimeFormat, margin * 3 );
    wbShowFiles.setLayoutData(fdbShowFiles);
    wbShowFiles.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        ExcelOutputMeta tfoi = new ExcelOutputMeta();
        getInfo( tfoi );
        String[] files = tfoi.getFiles( variables );
        if ( files != null && files.length > 0 ) {
          EnterSelectionDialog esd =
            new EnterSelectionDialog( shell, files,
              BaseMessages.getString( PKG, "ExcelOutputDialog.SelectOutputFiles.DialogTitle" ),
              BaseMessages.getString( PKG, "ExcelOutputDialog.SelectOutputFiles.DialogMessage" ) );
          esd.setViewOnly();
          esd.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "ExcelOutputDialog.NoFilesFound.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
          mb.open();
        }
      }
    } );

    // Add File to the result files name
    Label wlAddToResult = new Label(wFileComp, SWT.RIGHT);
    wlAddToResult.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.AddFileToResult.Label" ) );
    props.setLook(wlAddToResult);
    FormData fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment( 0, 0 );
    fdlAddToResult.top = new FormAttachment(wbShowFiles, 2 * margin );
    fdlAddToResult.right = new FormAttachment( middle, -margin );
    wlAddToResult.setLayoutData(fdlAddToResult);
    wAddToResult = new Button( wFileComp, SWT.CHECK );
    wAddToResult.setToolTipText( BaseMessages.getString( PKG, "ExcelOutputDialog.AddFileToResult.Tooltip" ) );
    props.setLook( wAddToResult );
    FormData fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment( middle, 0 );
    fdAddToResult.top = new FormAttachment( wlAddToResult, 0, SWT.CENTER );
    fdAddToResult.right = new FormAttachment( 100, 0 );
    wAddToResult.setLayoutData(fdAddToResult);
    SelectionAdapter lsSelR = new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wAddToResult.addSelectionListener( lsSelR );

    FormData fdFileComp = new FormData();
    fdFileComp.left = new FormAttachment( 0, 0 );
    fdFileComp.top = new FormAttachment( 0, 0 );
    fdFileComp.right = new FormAttachment( 100, 0 );
    fdFileComp.bottom = new FormAttachment( 100, 0 );
    wFileComp.setLayoutData(fdFileComp);

    wFileComp.layout();
    wFileTab.setControl( wFileComp );

    // ///////////////////////////////////////////////////////////
    // / END OF FILE TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.ContentTab.TabTitle" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE );
    props.setLook( wContentComp );
    wContentComp.setLayout( contentLayout );

    // Append checkbox
    Label wlAppend = new Label(wContentComp, SWT.RIGHT);
    wlAppend.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.Append.Label" ) );
    props.setLook(wlAppend);
    FormData fdlAppend = new FormData();
    fdlAppend.left = new FormAttachment( 0, 0 );
    fdlAppend.top = new FormAttachment( 0, 0 );
    fdlAppend.right = new FormAttachment( middle, -margin );
    wlAppend.setLayoutData(fdlAppend);
    wAppend = new Button( wContentComp, SWT.CHECK );
    props.setLook( wAppend );
    wAppend.setToolTipText( BaseMessages.getString( PKG, "ExcelOutputDialog.Append.Tooltip" ) );
    FormData fdAppend = new FormData();
    fdAppend.left = new FormAttachment( middle, 0 );
    fdAppend.top = new FormAttachment( wlAppend, 0, SWT.CENTER );
    fdAppend.right = new FormAttachment( 100, 0 );
    wAppend.setLayoutData(fdAppend);
    wAppend.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    } );

    Label wlHeader = new Label(wContentComp, SWT.RIGHT);
    wlHeader.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.Header.Label" ) );
    props.setLook(wlHeader);
    FormData fdlHeader = new FormData();
    fdlHeader.left = new FormAttachment( 0, 0 );
    fdlHeader.top = new FormAttachment( wAppend, margin );
    fdlHeader.right = new FormAttachment( middle, -margin );
    wlHeader.setLayoutData(fdlHeader);
    wHeader = new Button( wContentComp, SWT.CHECK );
    props.setLook( wHeader );
    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment( middle, 0 );
    fdHeader.top = new FormAttachment( wlHeader, 0, SWT.CENTER );
    fdHeader.right = new FormAttachment( 100, 0 );
    wHeader.setLayoutData(fdHeader);
    wHeader.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    Label wlFooter = new Label(wContentComp, SWT.RIGHT);
    wlFooter.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.Footer.Label" ) );
    props.setLook(wlFooter);
    FormData fdlFooter = new FormData();
    fdlFooter.left = new FormAttachment( 0, 0 );
    fdlFooter.top = new FormAttachment( wHeader, margin );
    fdlFooter.right = new FormAttachment( middle, -margin );
    wlFooter.setLayoutData(fdlFooter);
    wFooter = new Button( wContentComp, SWT.CHECK );
    props.setLook( wFooter );
    FormData fdFooter = new FormData();
    fdFooter.left = new FormAttachment( middle, 0 );
    fdFooter.top = new FormAttachment( wlFooter, 0, SWT.CENTER );
    fdFooter.right = new FormAttachment( 100, 0 );
    wFooter.setLayoutData(fdFooter);
    wFooter.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    Label wlEncoding = new Label(wContentComp, SWT.RIGHT);
    wlEncoding.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.Encoding.Label" ) );
    props.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment( 0, 0 );
    fdlEncoding.top = new FormAttachment( wFooter, 2*margin );
    fdlEncoding.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new CCombo( wContentComp, SWT.BORDER | SWT.READ_ONLY );
    wEncoding.setEditable( true );
    props.setLook( wEncoding );
    wEncoding.addModifyListener( lsMod );
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment( middle, 0 );
    fdEncoding.top = new FormAttachment( wFooter, 2*margin );
    fdEncoding.right = new FormAttachment( 100, 0 );
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addFocusListener( new FocusListener() {
      @Override
      public void focusLost( FocusEvent e ) {
      }

      @Override
      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setEncodings();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    Label wlSplitEvery = new Label(wContentComp, SWT.RIGHT);
    wlSplitEvery.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.SplitEvery.Label" ) );
    props.setLook(wlSplitEvery);
    FormData fdlSplitEvery = new FormData();
    fdlSplitEvery.left = new FormAttachment( 0, 0 );
    fdlSplitEvery.top = new FormAttachment( wEncoding, margin );
    fdlSplitEvery.right = new FormAttachment( middle, -margin );
    wlSplitEvery.setLayoutData(fdlSplitEvery);
    wSplitEvery = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSplitEvery );
    wSplitEvery.addModifyListener( lsMod );
    FormData fdSplitEvery = new FormData();
    fdSplitEvery.left = new FormAttachment( middle, 0 );
    fdSplitEvery.top = new FormAttachment( wEncoding, margin );
    fdSplitEvery.right = new FormAttachment( 100, 0 );
    wSplitEvery.setLayoutData(fdSplitEvery);

    // Sheet name line
    Label wlSheetname = new Label(wContentComp, SWT.RIGHT);
    wlSheetname.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.Sheetname.Label" ) );
    props.setLook(wlSheetname);
    FormData fdlSheetname = new FormData();
    fdlSheetname.left = new FormAttachment( 0, 0 );
    fdlSheetname.top = new FormAttachment( wSplitEvery, margin );
    fdlSheetname.right = new FormAttachment( middle, -margin );
    wlSheetname.setLayoutData(fdlSheetname);
    wSheetname = new TextVar( variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSheetname.setToolTipText( BaseMessages.getString( PKG, "ExcelOutputDialog.Sheetname.Tooltip" ) );
    props.setLook( wSheetname );
    wSheetname.addModifyListener( lsMod );
    FormData fdSheetname = new FormData();
    fdSheetname.left = new FormAttachment( middle, 0 );
    fdSheetname.top = new FormAttachment( wSplitEvery, margin );
    fdSheetname.right = new FormAttachment( 100, 0 );
    wSheetname.setLayoutData(fdSheetname);

    // Protect Sheet?
    Label wlProtectSheet = new Label(wContentComp, SWT.RIGHT);
    wlProtectSheet.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.ProtectSheet.Label" ) );
    props.setLook(wlProtectSheet);
    FormData fdlProtectSheet = new FormData();
    fdlProtectSheet.left = new FormAttachment( 0, 0 );
    fdlProtectSheet.top = new FormAttachment( wSheetname, margin );
    fdlProtectSheet.right = new FormAttachment( middle, -margin );
    wlProtectSheet.setLayoutData(fdlProtectSheet);
    wProtectSheet = new Button( wContentComp, SWT.CHECK );
    props.setLook( wProtectSheet );
    FormData fdProtectSheet = new FormData();
    fdProtectSheet.left = new FormAttachment( middle, 0 );
    fdProtectSheet.top = new FormAttachment( wlProtectSheet, 0, SWT.CENTER );
    fdProtectSheet.right = new FormAttachment( 100, 0 );
    wProtectSheet.setLayoutData(fdProtectSheet);
    wProtectSheet.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {

        EnablePassword();
      }
    } );

    // Password line
    wlPassword = new Label( wContentComp, SWT.RIGHT );
    wlPassword.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.Password.Label" ) );
    props.setLook( wlPassword );
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment( 0, 0 );
    fdlPassword.top = new FormAttachment( wProtectSheet, margin );
    fdlPassword.right = new FormAttachment( middle, -margin );
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new PasswordTextVar( variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wPassword.setToolTipText( BaseMessages.getString( PKG, "ExcelOutputDialog.Password.Tooltip" ) );
    props.setLook( wPassword );
    wPassword.addModifyListener( lsMod );
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment( middle, 0 );
    fdPassword.top = new FormAttachment( wProtectSheet, margin );
    fdPassword.right = new FormAttachment( 100, 0 );
    wPassword.setLayoutData(fdPassword);

    // auto size columns?
    Label wlAutoSize = new Label(wContentComp, SWT.RIGHT);
    wlAutoSize.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.AutoSize.Label" ) );
    props.setLook(wlAutoSize);
    FormData fdlAutoSize = new FormData();
    fdlAutoSize.left = new FormAttachment( 0, 0 );
    fdlAutoSize.top = new FormAttachment( wPassword, margin );
    fdlAutoSize.right = new FormAttachment( middle, -margin );
    wlAutoSize.setLayoutData(fdlAutoSize);
    wAutoSize = new Button( wContentComp, SWT.CHECK );
    props.setLook( wAutoSize );
    wAutoSize.setToolTipText( BaseMessages.getString( PKG, "ExcelOutputDialog.AutoSize.Tooltip" ) );
    FormData fdAutoSize = new FormData();
    fdAutoSize.left = new FormAttachment( middle, 0 );
    fdAutoSize.top = new FormAttachment( wlAutoSize, 0, SWT.CENTER );
    fdAutoSize.right = new FormAttachment( 100, 0 );
    wAutoSize.setLayoutData(fdAutoSize);
    wAutoSize.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        EnableAutoSize();
      }
    } );

    // write null values as blank cells ?
    Label wlNullIsBlank = new Label(wContentComp, SWT.RIGHT);
    wlNullIsBlank.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.NullIsBlank.Label" ) );
    props.setLook(wlNullIsBlank);
    FormData fdlNullIsBlank = new FormData();
    fdlNullIsBlank.left = new FormAttachment( 0, 0 );
    fdlNullIsBlank.top = new FormAttachment( wAutoSize, margin );
    fdlNullIsBlank.right = new FormAttachment( middle, -margin );
    wlNullIsBlank.setLayoutData(fdlNullIsBlank);
    wNullIsBlank = new Button( wContentComp, SWT.CHECK );
    props.setLook( wNullIsBlank );
    wNullIsBlank.setToolTipText( BaseMessages.getString( PKG, "ExcelOutputDialog.NullIsBlank.Tooltip" ) );
    FormData fdNullIsBlank = new FormData();
    fdNullIsBlank.left = new FormAttachment( middle, 0 );
    fdNullIsBlank.top = new FormAttachment( wlNullIsBlank, 0, SWT.CENTER );
    fdNullIsBlank.right = new FormAttachment( 100, 0 );
    wNullIsBlank.setLayoutData(fdNullIsBlank);

    // use temporary files?
    Label wlUseTempFiles = new Label(wContentComp, SWT.RIGHT);
    wlUseTempFiles.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.useTempFile.Label" ) );
    props.setLook(wlUseTempFiles);
    FormData fdlUseTempFiles = new FormData();
    fdlUseTempFiles.left = new FormAttachment( 0, 0 );
    fdlUseTempFiles.top = new FormAttachment( wNullIsBlank, margin );
    fdlUseTempFiles.right = new FormAttachment( middle, -margin );
    wlUseTempFiles.setLayoutData(fdlUseTempFiles);
    wUseTempFiles = new Button( wContentComp, SWT.CHECK );
    props.setLook( wUseTempFiles );
    wUseTempFiles.setToolTipText( BaseMessages.getString( PKG, "ExcelOutputDialog.useTempFile.Tooltip" ) );
    FormData fdUseTempFiles = new FormData();
    fdUseTempFiles.left = new FormAttachment( middle, 0 );
    fdUseTempFiles.top = new FormAttachment( wlUseTempFiles, 0, SWT.CENTER );
    fdUseTempFiles.right = new FormAttachment( 100, 0 );
    wUseTempFiles.setLayoutData(fdUseTempFiles);
    wUseTempFiles.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        useTempFile();
      }
    } );

    // TempDirectory line
    wlTempDirectory = new Label( wContentComp, SWT.RIGHT );
    wlTempDirectory.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.TempDirectory.Label" ) );
    props.setLook( wlTempDirectory );
    FormData fdlTempDirectory = new FormData();
    fdlTempDirectory.left = new FormAttachment( 0, 0 );
    fdlTempDirectory.top = new FormAttachment( wUseTempFiles, margin );
    fdlTempDirectory.right = new FormAttachment( middle, -margin );
    wlTempDirectory.setLayoutData(fdlTempDirectory);

    // Select TempDir
    wbTempDir = new Button( wContentComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTempDir );
    wbTempDir.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbTempDir = new FormData();
    fdbTempDir.right = new FormAttachment( 100, -margin );
    fdbTempDir.top = new FormAttachment( wUseTempFiles, margin );
    wbTempDir.setLayoutData(fdbTempDir);

    wTempDirectory = new TextVar( variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTempDirectory.setToolTipText( BaseMessages.getString( PKG, "ExcelOutputDialog.TempDirectory.Tooltip" ) );
    props.setLook( wTempDirectory );
    wTempDirectory.addModifyListener( lsMod );
    FormData fdTempDirectory = new FormData();
    fdTempDirectory.left = new FormAttachment( middle, 0 );
    fdTempDirectory.top = new FormAttachment( wUseTempFiles, margin );
    fdTempDirectory.right = new FormAttachment( wbTempDir, -margin );
    wTempDirectory.setLayoutData(fdTempDirectory);
    wTempDirectory.addModifyListener( e -> input.setChanged() );

    // ///////////////////////////////
    // START OF Template Group GROUP //
    // ///////////////////////////////

    Group wTemplateGroup = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wTemplateGroup);
    wTemplateGroup.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.TemplateGroup.Label" ) );

    FormLayout TemplateGroupgroupLayout = new FormLayout();
    TemplateGroupgroupLayout.marginWidth = 10;
    TemplateGroupgroupLayout.marginHeight = 10;
    wTemplateGroup.setLayout( TemplateGroupgroupLayout );

    // Use template
    Label wlTemplate = new Label(wTemplateGroup, SWT.RIGHT);
    wlTemplate.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.Template.Label" ) );
    props.setLook(wlTemplate);
    FormData fdlTemplate = new FormData();
    fdlTemplate.left = new FormAttachment( 0, 0 );
    fdlTemplate.top = new FormAttachment( wTempDirectory, margin );
    fdlTemplate.right = new FormAttachment( middle, -margin );
    wlTemplate.setLayoutData(fdlTemplate);
    wTemplate = new Button(wTemplateGroup, SWT.CHECK );
    props.setLook( wTemplate );
    FormData fdTemplate = new FormData();
    fdTemplate.left = new FormAttachment( middle, 0 );
    fdTemplate.top = new FormAttachment( wlTemplate, 0, SWT.CENTER );
    fdTemplate.right = new FormAttachment( 100, 0 );
    wTemplate.setLayoutData(fdTemplate);
    wTemplate.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        EnableTemplate();
      }
    } );

    // TemplateFilename line
    wlTemplateFilename = new Label(wTemplateGroup, SWT.RIGHT );
    wlTemplateFilename.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.TemplateFilename.Label" ) );
    props.setLook( wlTemplateFilename );
    FormData fdlTemplateFilename = new FormData();
    fdlTemplateFilename.left = new FormAttachment( 0, 0 );
    fdlTemplateFilename.top = new FormAttachment( wTemplate, margin );
    fdlTemplateFilename.right = new FormAttachment( middle, -margin );
    wlTemplateFilename.setLayoutData(fdlTemplateFilename);

    wbTemplateFilename = new Button(wTemplateGroup, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTemplateFilename );
    wbTemplateFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbTemplateFilename = new FormData();
    fdbTemplateFilename.right = new FormAttachment( 100, 0 );
    fdbTemplateFilename.top = new FormAttachment( wTemplate, 0 );
    wbTemplateFilename.setLayoutData(fdbTemplateFilename);

    wTemplateFilename = new TextVar( variables, wTemplateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTemplateFilename );
    wTemplateFilename.addModifyListener( lsMod );
    FormData fdTemplateFilename = new FormData();
    fdTemplateFilename.left = new FormAttachment( middle, 0 );
    fdTemplateFilename.top = new FormAttachment( wTemplate, margin );
    fdTemplateFilename.right = new FormAttachment( wbTemplateFilename, -margin );
    wTemplateFilename.setLayoutData(fdTemplateFilename);

    // Template Append
    wlTemplateAppend = new Label(wTemplateGroup, SWT.RIGHT );
    wlTemplateAppend.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.TemplateAppend.Label" ) );
    props.setLook( wlTemplateAppend );
    FormData fdlTemplateAppend = new FormData();
    fdlTemplateAppend.left = new FormAttachment( 0, 0 );
    fdlTemplateAppend.top = new FormAttachment( wTemplateFilename, margin );
    fdlTemplateAppend.right = new FormAttachment( middle, -margin );
    wlTemplateAppend.setLayoutData(fdlTemplateAppend);
    wTemplateAppend = new Button(wTemplateGroup, SWT.CHECK );
    props.setLook( wTemplateAppend );
    FormData fdTemplateAppend = new FormData();
    fdTemplateAppend.left = new FormAttachment( middle, 0 );
    fdTemplateAppend.top = new FormAttachment( wlTemplateAppend, 0, SWT.CENTER );
    fdTemplateAppend.right = new FormAttachment( 100, 0 );
    wTemplateAppend.setLayoutData(fdTemplateAppend);
    wTemplateAppend.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    FormData fdTemplateGroup = new FormData();
    fdTemplateGroup.left = new FormAttachment( 0, margin );
    fdTemplateGroup.top = new FormAttachment( wTempDirectory, margin );
    fdTemplateGroup.right = new FormAttachment( 100, -margin );
    wTemplateGroup.setLayoutData(fdTemplateGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Template Group GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment( 0, 0 );
    fdContentComp.top = new FormAttachment( 0, 0 );
    fdContentComp.right = new FormAttachment( 100, 0 );
    fdContentComp.bottom = new FormAttachment( 100, 0 );
    wContentComp.setLayoutData(fdContentComp);

    wContentComp.layout();
    wContentTab.setControl( wContentComp );

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    // Custom tab...
    //
    CTabItem wCustomTab = new CTabItem(wTabFolder, SWT.NONE);
    wCustomTab.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.CustomTab.TabTitle" ) );

    FormLayout CustomLayout = new FormLayout();
    CustomLayout.marginWidth = Const.FORM_MARGIN;
    CustomLayout.marginHeight = Const.FORM_MARGIN;

    Composite wCustomComp = new Composite(wTabFolder, SWT.NONE );
    wCustomComp.setLayout( CustomLayout );
    props.setLook( wCustomComp );

    // ///////////////////////////////
    // START OF Header Font GROUP //
    // ///////////////////////////////

    Group wFontHeaderGroup = new Group(wCustomComp, SWT.SHADOW_NONE);
    props.setLook(wFontHeaderGroup);
    wFontHeaderGroup.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.FontHeaderGroup.Label" ) );

    FormLayout FontHeadergroupLayout = new FormLayout();
    FontHeadergroupLayout.marginWidth = 10;
    FontHeadergroupLayout.marginHeight = 10;
    wFontHeaderGroup.setLayout( FontHeadergroupLayout );

    // Header font name
    Label wlHeaderFontName = new Label(wFontHeaderGroup, SWT.RIGHT);
    wlHeaderFontName.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.HeaderFontName.Label" ) );
    props.setLook(wlHeaderFontName);
    FormData fdlHeaderFontName = new FormData();
    fdlHeaderFontName.left = new FormAttachment( 0, 0 );
    fdlHeaderFontName.top = new FormAttachment( 0, margin );
    fdlHeaderFontName.right = new FormAttachment( middle, -margin );
    wlHeaderFontName.setLayoutData(fdlHeaderFontName);
    wHeaderFontName = new CCombo(wFontHeaderGroup, SWT.BORDER | SWT.READ_ONLY );
    wHeaderFontName.setItems( ExcelOutputMeta.fontNameDesc );
    props.setLook( wHeaderFontName );
    wHeaderFontName.addModifyListener( lsMod );
    FormData fdHeaderFontName = new FormData();
    fdHeaderFontName.left = new FormAttachment( middle, 0 );
    fdHeaderFontName.top = new FormAttachment( 0, margin );
    fdHeaderFontName.right = new FormAttachment( 100, 0 );
    wHeaderFontName.setLayoutData(fdHeaderFontName);

    // Header font size
    Label wlHeaderFontSize = new Label(wFontHeaderGroup, SWT.RIGHT);
    wlHeaderFontSize.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.HeaderFontSize.Label" ) );
    props.setLook(wlHeaderFontSize);
    FormData fdlHeaderFontSize = new FormData();
    fdlHeaderFontSize.left = new FormAttachment( 0, 0 );
    fdlHeaderFontSize.top = new FormAttachment( wHeaderFontName, margin );
    fdlHeaderFontSize.right = new FormAttachment( middle, -margin );
    wlHeaderFontSize.setLayoutData(fdlHeaderFontSize);
    wHeaderFontSize = new TextVar( variables, wFontHeaderGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wHeaderFontSize.setToolTipText( BaseMessages.getString( PKG, "ExcelOutputDialog.HeaderFontSize.Tooltip" ) );
    props.setLook( wHeaderFontSize );
    wHeaderFontSize.addModifyListener( lsMod );
    FormData fdHeaderFontSize = new FormData();
    fdHeaderFontSize.left = new FormAttachment( middle, 0 );
    fdHeaderFontSize.top = new FormAttachment( wHeaderFontName, margin );
    fdHeaderFontSize.right = new FormAttachment( 100, 0 );
    wHeaderFontSize.setLayoutData(fdHeaderFontSize);

    // Header font bold?
    Label wlHeaderFontBold = new Label(wFontHeaderGroup, SWT.RIGHT);
    wlHeaderFontBold.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.HeaderFontBold.Label" ) );
    props.setLook(wlHeaderFontBold);
    FormData fdlHeaderFontBold = new FormData();
    fdlHeaderFontBold.left = new FormAttachment( 0, 0 );
    fdlHeaderFontBold.top = new FormAttachment( wHeaderFontSize, margin );
    fdlHeaderFontBold.right = new FormAttachment( middle, -margin );
    wlHeaderFontBold.setLayoutData(fdlHeaderFontBold);
    wHeaderFontBold = new Button(wFontHeaderGroup, SWT.CHECK );
    props.setLook( wHeaderFontBold );
    FormData fdHeaderFontBold = new FormData();
    fdHeaderFontBold.left = new FormAttachment( middle, 0 );
    fdHeaderFontBold.top = new FormAttachment( wlHeaderFontBold, 0, SWT.CENTER );
    fdHeaderFontBold.right = new FormAttachment( 100, 0 );
    wHeaderFontBold.setLayoutData(fdHeaderFontBold);
    wHeaderFontBold.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Header font bold?
    Label wlHeaderFontItalic = new Label(wFontHeaderGroup, SWT.RIGHT);
    wlHeaderFontItalic.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.HeaderFontItalic.Label" ) );
    props.setLook(wlHeaderFontItalic);
    FormData fdlHeaderFontItalic = new FormData();
    fdlHeaderFontItalic.left = new FormAttachment( 0, 0 );
    fdlHeaderFontItalic.top = new FormAttachment( wHeaderFontBold, margin );
    fdlHeaderFontItalic.right = new FormAttachment( middle, -margin );
    wlHeaderFontItalic.setLayoutData(fdlHeaderFontItalic);
    wHeaderFontItalic = new Button(wFontHeaderGroup, SWT.CHECK );
    props.setLook( wHeaderFontItalic );
    FormData fdHeaderFontItalic = new FormData();
    fdHeaderFontItalic.left = new FormAttachment( middle, 0 );
    fdHeaderFontItalic.top = new FormAttachment( wlHeaderFontItalic, 0, SWT.CENTER );
    fdHeaderFontItalic.right = new FormAttachment( 100, 0 );
    wHeaderFontItalic.setLayoutData(fdHeaderFontItalic);
    wHeaderFontItalic.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Font header uderline?
    Label wlHeaderFontUnderline = new Label(wFontHeaderGroup, SWT.RIGHT);
    wlHeaderFontUnderline.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.HeaderFontUnderline.Label" ) );
    props.setLook(wlHeaderFontUnderline);
    FormData fdlHeaderFontUnderline = new FormData();
    fdlHeaderFontUnderline.left = new FormAttachment( 0, 0 );
    fdlHeaderFontUnderline.top = new FormAttachment( wHeaderFontItalic, margin );
    fdlHeaderFontUnderline.right = new FormAttachment( middle, -margin );
    wlHeaderFontUnderline.setLayoutData(fdlHeaderFontUnderline);
    wHeaderFontUnderline = new CCombo(wFontHeaderGroup, SWT.BORDER | SWT.READ_ONLY );
    wHeaderFontUnderline.setItems( ExcelOutputMeta.font_underlineDesc );
    props.setLook( wHeaderFontUnderline );
    wHeaderFontUnderline.addModifyListener( lsMod );
    FormData fdHeaderFontUnderline = new FormData();
    fdHeaderFontUnderline.left = new FormAttachment( middle, 0 );
    fdHeaderFontUnderline.top = new FormAttachment( wHeaderFontItalic, margin );
    fdHeaderFontUnderline.right = new FormAttachment( 100, 0 );
    wHeaderFontUnderline.setLayoutData(fdHeaderFontUnderline);

    // Font header orientation
    Label wlHeaderFontOrientation = new Label(wFontHeaderGroup, SWT.RIGHT);
    wlHeaderFontOrientation
      .setText( BaseMessages.getString( PKG, "ExcelOutputDialog.HeaderFontOrientation.Label" ) );
    props.setLook(wlHeaderFontOrientation);
    FormData fdlHeaderFontOrientation = new FormData();
    fdlHeaderFontOrientation.left = new FormAttachment( 0, 0 );
    fdlHeaderFontOrientation.top = new FormAttachment( wHeaderFontUnderline, margin );
    fdlHeaderFontOrientation.right = new FormAttachment( middle, -margin );
    wlHeaderFontOrientation.setLayoutData(fdlHeaderFontOrientation);
    wHeaderFontOrientation = new CCombo(wFontHeaderGroup, SWT.BORDER | SWT.READ_ONLY );
    wHeaderFontOrientation.setItems( ExcelOutputMeta.font_orientationDesc );
    props.setLook( wHeaderFontOrientation );
    wHeaderFontOrientation.addModifyListener( lsMod );
    FormData fdHeaderFontOrientation = new FormData();
    fdHeaderFontOrientation.left = new FormAttachment( middle, 0 );
    fdHeaderFontOrientation.top = new FormAttachment( wHeaderFontUnderline, margin );
    fdHeaderFontOrientation.right = new FormAttachment( 100, 0 );
    wHeaderFontOrientation.setLayoutData(fdHeaderFontOrientation);

    // Font header color
    Label wlHeaderFontColor = new Label(wFontHeaderGroup, SWT.RIGHT);
    wlHeaderFontColor.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.HeaderFontColor.Label" ) );
    props.setLook(wlHeaderFontColor);
    FormData fdlHeaderFontColor = new FormData();
    fdlHeaderFontColor.left = new FormAttachment( 0, 0 );
    fdlHeaderFontColor.top = new FormAttachment( wHeaderFontOrientation, margin );
    fdlHeaderFontColor.right = new FormAttachment( middle, -margin );
    wlHeaderFontColor.setLayoutData(fdlHeaderFontColor);

    wHeaderFontColor = new CCombo(wFontHeaderGroup, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wHeaderFontColor );
    FormData fdHeaderFontColor = new FormData();
    fdHeaderFontColor.left = new FormAttachment( middle, 0 );
    fdHeaderFontColor.top = new FormAttachment( wHeaderFontOrientation, margin );
    fdHeaderFontColor.right = new FormAttachment( 100, 0 );
    wHeaderFontColor.setLayoutData(fdHeaderFontColor);
    wHeaderFontColor.setItems( ExcelOutputMeta.fontColorDesc );

    // Font header background color
    Label wlHeaderBackGroundColor = new Label(wFontHeaderGroup, SWT.RIGHT);
    wlHeaderBackGroundColor
      .setText( BaseMessages.getString( PKG, "ExcelOutputDialog.HeaderBackGroundColor.Label" ) );
    props.setLook(wlHeaderBackGroundColor);
    FormData fdlHeaderBackGroundColor = new FormData();
    fdlHeaderBackGroundColor.left = new FormAttachment( 0, 0 );
    fdlHeaderBackGroundColor.top = new FormAttachment( wHeaderFontColor, margin );
    fdlHeaderBackGroundColor.right = new FormAttachment( middle, -margin );
    wlHeaderBackGroundColor.setLayoutData(fdlHeaderBackGroundColor);

    wHeaderBackGroundColor = new CCombo(wFontHeaderGroup, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wHeaderBackGroundColor );
    FormData fdHeaderBackGroundColor = new FormData();
    fdHeaderBackGroundColor.left = new FormAttachment( middle, 0 );
    fdHeaderBackGroundColor.top = new FormAttachment( wHeaderFontColor, margin );
    fdHeaderBackGroundColor.right = new FormAttachment( 100, 0 );
    wHeaderBackGroundColor.setLayoutData(fdHeaderBackGroundColor);
    wHeaderBackGroundColor.setItems( ExcelOutputMeta.fontColorDesc );

    // Header font size
    Label wlHeaderRowHeight = new Label(wFontHeaderGroup, SWT.RIGHT);
    wlHeaderRowHeight.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.HeaderRowHeight.Label" ) );
    props.setLook(wlHeaderRowHeight);
    FormData fdlHeaderRowHeight = new FormData();
    fdlHeaderRowHeight.left = new FormAttachment( 0, 0 );
    fdlHeaderRowHeight.top = new FormAttachment( wHeaderBackGroundColor, margin );
    fdlHeaderRowHeight.right = new FormAttachment( middle, -margin );
    wlHeaderRowHeight.setLayoutData(fdlHeaderRowHeight);
    wHeaderRowHeight = new TextVar( variables, wFontHeaderGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wHeaderRowHeight.setToolTipText( BaseMessages.getString( PKG, "ExcelOutputDialog.HeaderRowHeight.Tooltip" ) );
    props.setLook( wHeaderRowHeight );
    wHeaderRowHeight.addModifyListener( lsMod );
    FormData fdHeaderRowHeight = new FormData();
    fdHeaderRowHeight.left = new FormAttachment( middle, 0 );
    fdHeaderRowHeight.top = new FormAttachment( wHeaderBackGroundColor, margin );
    fdHeaderRowHeight.right = new FormAttachment( 100, 0 );
    wHeaderRowHeight.setLayoutData(fdHeaderRowHeight);

    // Header Alignment
    Label wlHeaderAlignment = new Label(wFontHeaderGroup, SWT.RIGHT);
    wlHeaderAlignment.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.HeaderAlignment.Label" ) );
    props.setLook(wlHeaderAlignment);
    FormData fdlHeaderAlignment = new FormData();
    fdlHeaderAlignment.left = new FormAttachment( 0, 0 );
    fdlHeaderAlignment.top = new FormAttachment( wHeaderRowHeight, margin );
    fdlHeaderAlignment.right = new FormAttachment( middle, -margin );
    wlHeaderAlignment.setLayoutData(fdlHeaderAlignment);
    wHeaderAlignment = new CCombo(wFontHeaderGroup, SWT.BORDER | SWT.READ_ONLY );
    wHeaderAlignment.setItems( ExcelOutputMeta.font_alignmentDesc );
    props.setLook( wHeaderAlignment );
    wHeaderAlignment.addModifyListener( lsMod );
    FormData fdHeaderAlignment = new FormData();
    fdHeaderAlignment.left = new FormAttachment( middle, 0 );
    fdHeaderAlignment.top = new FormAttachment( wHeaderRowHeight, margin );
    fdHeaderAlignment.right = new FormAttachment( 100, 0 );
    wHeaderAlignment.setLayoutData(fdHeaderAlignment);

    // Select Image
    Button wbImage = new Button(wFontHeaderGroup, SWT.PUSH | SWT.CENTER);
    props.setLook(wbImage);
    wbImage.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.AddImage" ) );
    FormData fdbImage = new FormData();
    fdbImage.right = new FormAttachment( 100, 0 );
    fdbImage.top = new FormAttachment( wHeaderAlignment, margin );
    wbImage.setLayoutData(fdbImage);
    wbImage.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wImage, variables,
      new String[] { "*.png", "*.*" },
      new String[] {
        BaseMessages.getString( PKG, "ExcelOutputDialog.FileType.PNGFiles" ),
        BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
      true )
    );


    // Image line
    Label wlImage = new Label(wFontHeaderGroup, SWT.RIGHT);
    wlImage.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.Image.Label" ) );
    props.setLook(wlImage);
    FormData fdlImage = new FormData();
    fdlImage.left = new FormAttachment( 0, 0 );
    fdlImage.top = new FormAttachment( wHeaderAlignment, margin );
    fdlImage.right = new FormAttachment( middle, -margin );
    wlImage.setLayoutData(fdlImage);

    wImage = new TextVar( variables, wFontHeaderGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wImage );
    wImage.addModifyListener( lsMod );
    FormData fdImage = new FormData();
    fdImage.left = new FormAttachment( middle, 0 );
    fdImage.top = new FormAttachment( wHeaderAlignment, margin );
    fdImage.right = new FormAttachment(wbImage, -margin );
    wImage.setLayoutData(fdImage);

    FormData fdFontHeaderGroup = new FormData();
    fdFontHeaderGroup.left = new FormAttachment( 0, margin );
    fdFontHeaderGroup.top = new FormAttachment( 0, margin );
    fdFontHeaderGroup.right = new FormAttachment( 100, -margin );
    wFontHeaderGroup.setLayoutData(fdFontHeaderGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Font Group GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF Row Font GROUP //
    // ///////////////////////////////

    Group wFontRowGroup = new Group(wCustomComp, SWT.SHADOW_NONE);
    props.setLook(wFontRowGroup);
    wFontRowGroup.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.FontRowGroup.Label" ) );
    FormLayout FontRowGroupLayout = new FormLayout();
    FontRowGroupLayout.marginWidth = 10;
    FontRowGroupLayout.marginHeight = 10;
    wFontRowGroup.setLayout( FontRowGroupLayout );

    // Font Row name
    Label wlRowFontName = new Label(wFontRowGroup, SWT.RIGHT);
    wlRowFontName.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.RowFontName.Label" ) );
    props.setLook(wlRowFontName);
    FormData fdlRowFontName = new FormData();
    fdlRowFontName.left = new FormAttachment( 0, 0 );
    fdlRowFontName.top = new FormAttachment( 0, margin );
    fdlRowFontName.right = new FormAttachment( middle, -margin );
    wlRowFontName.setLayoutData(fdlRowFontName);
    wRowFontName = new CCombo(wFontRowGroup, SWT.BORDER | SWT.READ_ONLY );
    wRowFontName.setItems( ExcelOutputMeta.fontNameDesc );
    props.setLook( wRowFontName );
    wRowFontName.addModifyListener( lsMod );
    FormData fdRowFontName = new FormData();
    fdRowFontName.left = new FormAttachment( middle, 0 );
    fdRowFontName.top = new FormAttachment( 0, margin );
    fdRowFontName.right = new FormAttachment( 100, 0 );
    wRowFontName.setLayoutData(fdRowFontName);

    // Row font size
    Label wlRowFontSize = new Label(wFontRowGroup, SWT.RIGHT);
    wlRowFontSize.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.RowFontSize.Label" ) );
    props.setLook(wlRowFontSize);
    FormData fdlRowFontSize = new FormData();
    fdlRowFontSize.left = new FormAttachment( 0, 0 );
    fdlRowFontSize.top = new FormAttachment( wRowFontName, margin );
    fdlRowFontSize.right = new FormAttachment( middle, -margin );
    wlRowFontSize.setLayoutData(fdlRowFontSize);
    wRowFontSize = new TextVar( variables, wFontRowGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wRowFontSize.setToolTipText( BaseMessages.getString( PKG, "ExcelOutputDialog.RowFontSize.Tooltip" ) );
    props.setLook( wRowFontSize );
    wRowFontSize.addModifyListener( lsMod );
    FormData fdRowFontSize = new FormData();
    fdRowFontSize.left = new FormAttachment( middle, 0 );
    fdRowFontSize.top = new FormAttachment( wRowFontName, margin );
    fdRowFontSize.right = new FormAttachment( 100, 0 );
    wRowFontSize.setLayoutData(fdRowFontSize);

    // Font Row color
    Label wlRowFontColor = new Label(wFontRowGroup, SWT.RIGHT);
    wlRowFontColor.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.RowFontColor.Label" ) );
    props.setLook(wlRowFontColor);
    FormData fdlRowFontColor = new FormData();
    fdlRowFontColor.left = new FormAttachment( 0, 0 );
    fdlRowFontColor.top = new FormAttachment( wRowFontSize, margin );
    fdlRowFontColor.right = new FormAttachment( middle, -margin );
    wlRowFontColor.setLayoutData(fdlRowFontColor);
    wRowFontColor = new CCombo(wFontRowGroup, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wRowFontColor );
    FormData fdRowFontColor = new FormData();
    fdRowFontColor.left = new FormAttachment( middle, 0 );
    fdRowFontColor.top = new FormAttachment( wRowFontSize, margin );
    fdRowFontColor.right = new FormAttachment( 100, 0 );
    wRowFontColor.setLayoutData(fdRowFontColor);
    wRowFontColor.setItems( ExcelOutputMeta.fontColorDesc );

    // Font Row background color
    Label wlRowBackGroundColor = new Label(wFontRowGroup, SWT.RIGHT);
    wlRowBackGroundColor.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.RowBackGroundColor.Label" ) );
    props.setLook(wlRowBackGroundColor);
    FormData fdlRowBackGroundColor = new FormData();
    fdlRowBackGroundColor.left = new FormAttachment( 0, 0 );
    fdlRowBackGroundColor.top = new FormAttachment( wRowFontColor, margin );
    fdlRowBackGroundColor.right = new FormAttachment( middle, -margin );
    wlRowBackGroundColor.setLayoutData(fdlRowBackGroundColor);

    wRowBackGroundColor = new CCombo(wFontRowGroup, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wRowBackGroundColor );
    FormData fdRowBackGroundColor = new FormData();
    fdRowBackGroundColor.left = new FormAttachment( middle, 0 );
    fdRowBackGroundColor.top = new FormAttachment( wRowFontColor, margin );
    fdRowBackGroundColor.right = new FormAttachment( 100, 0 );
    wRowBackGroundColor.setLayoutData(fdRowBackGroundColor);
    wRowBackGroundColor.setItems( ExcelOutputMeta.fontColorDesc );

    FormData fdFontRowGroup = new FormData();
    fdFontRowGroup.left = new FormAttachment( 0, margin );
    fdFontRowGroup.top = new FormAttachment(wFontHeaderGroup, margin );
    fdFontRowGroup.right = new FormAttachment( 100, -margin );
    wFontRowGroup.setLayoutData(fdFontRowGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Row Font Group
    // ///////////////////////////////////////////////////////////

    FormData fdCustomComp = new FormData();
    fdCustomComp.left = new FormAttachment( 0, 0 );
    fdCustomComp.top = new FormAttachment( 0, 0 );
    fdCustomComp.right = new FormAttachment( 100, 0 );
    fdCustomComp.bottom = new FormAttachment( 100, 0 );
    wCustomComp.setLayoutData(fdCustomComp);

    wCustomComp.layout();
    wCustomTab.setControl( wCustomComp );
    // ///////////////////////////////////////////////////////////
    // / END OF customer TAB
    // ///////////////////////////////////////////////////////////

    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.FieldsTab.TabTitle" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE );
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook( wFieldsComp );

    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    wGet.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.GetFields" ) );

    wMinWidth = new Button( wFieldsComp, SWT.PUSH );
    wMinWidth.setText( BaseMessages.getString( PKG, "ExcelOutputDialog.MinWidth.Button" ) );
    wMinWidth.setToolTipText( BaseMessages.getString( PKG, "ExcelOutputDialog.MinWidth.Tooltip" ) );

    setButtonPositions( new Button[] { wGet, wMinWidth }, margin, null );

    final int FieldsRows = input.getOutputFields().length;

    // Prepare a list of possible formats...
    String[] formats =
      new String[] {
        // Numbers
        "#", "0", "0.00", "#,##0", "#,##0.00", "$#,##0;($#,##0)", "$#,##0;($#,##0)", "$#,##0;($#,##0)",
        "$#,##0;($#,##0)", "0%", "0.00%", "0.00E00", "#,##0;(#,##0)", "#,##0;(#,##0)", "#,##0.00;(#,##0.00)",
        "#,##0.00;(#,##0.00)", "#,##0;(#,##0)", "#,##0;(#,##0)", "#,##0.00;(#,##0.00)", "#,##0.00;(#,##0.00)",
        "#,##0.00;(#,##0.00)", "##0.0E0",

        // Forces text
        "@",

        // Dates
        "M/d/yy", "d-MMM-yy", "d-MMM", "MMM-yy", "h:mm a", "h:mm:ss a", "H:mm", "H:mm:ss", "M/d/yy H:mm",
        "mm:ss", "H:mm:ss", "H:mm:ss", };

    colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "ExcelOutputDialog.NameColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ExcelOutputDialog.TypeColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames() ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ExcelOutputDialog.FormatColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, formats ), };

    wFields =
      new TableView(
        variables, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -margin );
    wFields.setLayoutData(fdFields);

    //
    // Search the fields in the background

    final Runnable runnable = () -> {
      TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
      if ( transformMeta != null ) {
        try {
          IRowMeta row = pipelineMeta.getPrevTransformFields( variables, transformMeta );

          // Remember these fields...
          for ( int i = 0; i < row.size(); i++ ) {
            inputFields.put( row.getValueMeta( i ).getName(), i);
          }
          setComboBoxes();
        } catch ( HopException e ) {
          logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
        }
      }
    };
    new Thread( runnable ).start();

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners
    wGet.addListener( SWT.Selection, e -> get() );
    wMinWidth.addListener( SWT.Selection, e -> setMinimalWidth());

    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wFilename.addSelectionListener( lsDef );
    wTemplateFilename.addSelectionListener( lsDef );

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener( e -> wFilename.setToolTipText( variables.resolve( wFilename.getText() ) ) );
    wTemplateFilename.addModifyListener( e -> wTemplateFilename.setToolTipText( variables.resolve( wTemplateFilename.getText() ) ) );

    wbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFilename, variables,
      new String[] { "*.xls", "*.*" },
      new String[] {
        BaseMessages.getString( PKG, "System.FileType.ExcelFiles" ),
        BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
      true )
    );
    wbTemplateFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wTemplateFilename, variables,
      new String[] { "*.xls", "*.*" },
      new String[] {
        BaseMessages.getString( PKG, "System.FileType.ExcelFiles" ),
        BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
      true )
    );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
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

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    setDateTimeFormat();
    EnableAutoSize();
    useTempFile();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void EnableAutoSize() {
    wMinWidth.setEnabled( !wAutoSize.getSelection() );
  }

  private void setDateTimeFormat() {
    if ( wSpecifyFormat.getSelection() ) {
      wAddDate.setSelection( false );
      wAddTime.setSelection( false );
    }

    wDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wlDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wAddDate.setEnabled( !wSpecifyFormat.getSelection() );
    wlAddDate.setEnabled( !wSpecifyFormat.getSelection() );
    wAddTime.setEnabled( !wSpecifyFormat.getSelection() );
    wlAddTime.setEnabled( !wSpecifyFormat.getSelection() );

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
    colinf[ 0 ].setComboValues( fieldNames );
  }

  private void setEncodings() {
    // Encoding of the text file:
    if ( !gotEncodings ) {
      gotEncodings = true;

      wEncoding.removeAll();
      List<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
      for (Charset charSet : values) {
        wEncoding.add(charSet.displayName());
      }

      // Now select the default!
      String defEncoding = Const.getEnvironmentVariable( "file.encoding", "UTF-8" );
      int idx = Const.indexOfString( defEncoding, wEncoding.getItems() );
      if ( idx >= 0 ) {
        wEncoding.select( idx );
      }
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getFileName() != null ) {
      wFilename.setText( input.getFileName() );
    }
    wDoNotOpenNewFileInit.setSelection( input.isDoNotOpenNewFileInit() );
    if ( input.getExtension() != null ) {
      wExtension.setText( input.getExtension() );
    }
    if ( input.getEncoding() != null ) {
      wEncoding.setText( input.getEncoding() );
    }
    if ( input.getTemplateFileName() != null ) {
      wTemplateFilename.setText( input.getTemplateFileName() );
    }
    wUseTempFiles.setSelection( input.isUseTempFiles() );
    if ( input.getTempDirectory() != null ) {
      wTempDirectory.setText( input.getTempDirectory() );
    }
    wSplitEvery.setText( "" + input.getSplitEvery() );
    wAppend.setSelection( input.isAppend() );
    wHeader.setSelection( input.isHeaderEnabled() );
    wFooter.setSelection( input.isFooterEnabled() );
    wAddDate.setSelection( input.isDateInFilename() );
    wAddTime.setSelection( input.isTimeInFilename() );

    if ( input.getDateTimeFormat() != null ) {
      wDateTimeFormat.setText( input.getDateTimeFormat() );
    }
    wSpecifyFormat.setSelection( input.isSpecifyFormat() );

    wCreateParentFolder.setSelection( input.isCreateParentFolder() );
    wAddToResult.setSelection( input.isAddToResultFiles() );
    wAutoSize.setSelection( input.isAutoSizeColums() );
    wNullIsBlank.setSelection( input.isNullBlank() );

    wAddTransformNr.setSelection( input.isTransformNrInFilename() );
    wTemplate.setSelection( input.isTemplateEnabled() );
    wTemplateAppend.setSelection( input.isTemplateAppend() );
    if ( input.getSheetname() != null ) {
      wSheetname.setText( input.getSheetname() );
    } else {
      wSheetname.setText( "Sheet1" );
    }
    wProtectSheet.setSelection( input.isSheetProtected() );

    EnablePassword();
    EnableTemplate();

    if ( input.getPassword() != null ) {
      wPassword.setText( input.getPassword() );
    }
    if ( isDebug() ) {
      logDebug( "getting fields info..." );
    }

    for ( int i = 0; i < input.getOutputFields().length; i++ ) {
      ExcelField field = input.getOutputFields()[ i ];

      TableItem item = wFields.table.getItem( i );
      if ( field.getName() != null ) {
        item.setText( 1, field.getName() );
      }
      item.setText( 2, field.getTypeDesc() );
      if ( field.getFormat() != null ) {
        item.setText( 3, field.getFormat() );
      }
    }

    wFields.optWidth( true );

    // Header Font settings
    wHeaderFontName.setText( ExcelOutputMeta.getFontNameDesc( input.getHeaderFontName() ) );
    wHeaderFontSize.setText( input.getHeaderFontSize() );
    wHeaderFontBold.setSelection( input.isHeaderFontBold() );
    wHeaderFontItalic.setSelection( input.isHeaderFontItalic() );
    wHeaderFontUnderline.setText( ExcelOutputMeta.getFontUnderlineDesc( input.getHeaderFontUnderline() ) );
    wHeaderFontOrientation.setText( ExcelOutputMeta.getFontOrientationDesc( input.getHeaderFontOrientation() ) );
    wHeaderFontColor.setText( ExcelOutputMeta.getFontColorDesc( input.getHeaderFontColor() ) );
    wHeaderBackGroundColor.setText( ExcelOutputMeta.getFontColorDesc( input.getHeaderBackGroundColor() ) );
    wHeaderRowHeight.setText( Const.NVL( input.getHeaderRowHeight(), "" + ExcelOutputMeta.DEFAULT_ROW_HEIGHT ) );
    wHeaderAlignment.setText( ExcelOutputMeta.getFontAlignmentDesc( input.getHeaderAlignment() ) );
    if ( input.getHeaderImage() != null ) {
      wImage.setText( input.getHeaderImage() );
    }

    // Row font settings
    wRowFontName.setText( ExcelOutputMeta.getFontNameDesc( input.getRowFontName() ) );
    wRowFontSize.setText( input.getRowFontSize() );
    wRowFontColor.setText( ExcelOutputMeta.getFontColorDesc( input.getRowFontColor() ) );
    wRowBackGroundColor.setText( ExcelOutputMeta.getFontColorDesc( input.getRowBackGroundColor() ) );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;

    input.setChanged( backupChanged );

    dispose();
  }

  private void getInfo( ExcelOutputMeta tfoi ) {
    tfoi.setFileName( wFilename.getText() );
    tfoi.setEncoding( wEncoding.getText() );
    tfoi.setDoNotOpenNewFileInit( wDoNotOpenNewFileInit.getSelection() );
    tfoi.setExtension( wExtension.getText() );
    tfoi.setTemplateFileName( wTemplateFilename.getText() );
    tfoi.setSplitEvery( Const.toInt( wSplitEvery.getText(), 0 ) );
    tfoi.setAppend( wAppend.getSelection() );
    tfoi.setHeaderEnabled( wHeader.getSelection() );
    tfoi.setFooterEnabled( wFooter.getSelection() );
    tfoi.setTransformNrInFilename( wAddTransformNr.getSelection() );
    tfoi.setDateInFilename( wAddDate.getSelection() );
    tfoi.setTimeInFilename( wAddTime.getSelection() );
    tfoi.setUseTempFiles( wUseTempFiles.getSelection() );
    tfoi.setTempDirectory( wTempDirectory.getText() );
    tfoi.setDateTimeFormat( wDateTimeFormat.getText() );
    tfoi.setSpecifyFormat( wSpecifyFormat.getSelection() );
    tfoi.setAutoSizeColums( wAutoSize.getSelection() );
    tfoi.setNullIsBlank( wNullIsBlank.getSelection() );

    tfoi.setAddToResultFiles( wAddToResult.getSelection() );
    tfoi.setCreateParentFolder( wCreateParentFolder.getSelection() );
    tfoi.setProtectSheet( wProtectSheet.getSelection() );
    tfoi.setPassword( wPassword.getText() );
    tfoi.setTemplateEnabled( wTemplate.getSelection() );
    tfoi.setTemplateAppend( wTemplateAppend.getSelection() );
    if ( wSheetname.getText() != null ) {
      tfoi.setSheetname( wSheetname.getText() );
    } else {
      tfoi.setSheetname( "Sheet 1" );
    }

    int i;
    // Table table = wFields.table;

    int nrFields = wFields.nrNonEmpty();

    tfoi.allocate( nrFields );

    for ( i = 0; i < nrFields; i++ ) {
      ExcelField field = new ExcelField();

      TableItem item = wFields.getNonEmpty( i );
      field.setName( item.getText( 1 ) );
      field.setType( item.getText( 2 ) );
      field.setFormat( item.getText( 3 ) );

      //CHECKSTYLE:Indentation:OFF
      tfoi.getOutputFields()[ i ] = field;
    }
    // Header font
    tfoi.setHeaderFontName( ExcelOutputMeta.getFontNameByDesc( wHeaderFontName.getText() ) );
    tfoi.setHeaderFontSize( wHeaderFontSize.getText() );
    tfoi.setHeaderFontBold( wHeaderFontBold.getSelection() );
    tfoi.setHeaderFontItalic( wHeaderFontItalic.getSelection() );
    tfoi.setHeaderFontUnderline( ExcelOutputMeta.getFontUnderlineByDesc( wHeaderFontUnderline.getText() ) );
    tfoi.setHeaderFontOrientation( ExcelOutputMeta.getFontOrientationByDesc( wHeaderFontOrientation.getText() ) );
    tfoi.setHeaderFontColor( ExcelOutputMeta.getFontColorByDesc( wHeaderFontColor.getText() ) );
    tfoi.setHeaderBackGroundColor( ExcelOutputMeta.getFontColorByDesc( wHeaderBackGroundColor.getText() ) );
    tfoi.setHeaderRowHeight( wHeaderRowHeight.getText() );
    tfoi.setHeaderAlignment( ExcelOutputMeta.getFontAlignmentByDesc( wHeaderAlignment.getText() ) );
    tfoi.setHeaderImage( wImage.getText() );

    // Row font
    tfoi.setRowFontName( ExcelOutputMeta.getFontNameByDesc( wRowFontName.getText() ) );
    tfoi.setRowFontSize( wRowFontSize.getText() );
    tfoi.setRowFontColor( ExcelOutputMeta.getFontColorByDesc( wRowFontColor.getText() ) );
    tfoi.setRowBackGroundColor( ExcelOutputMeta.getFontColorByDesc( wRowBackGroundColor.getText() ) );
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo( input );

    dispose();
  }

  private void EnablePassword() {
    input.setChanged();

    wPassword.setEnabled( wProtectSheet.getSelection() );
    wlPassword.setEnabled( wProtectSheet.getSelection() );

  }

  private void EnableTemplate() {
    input.setChanged();

    wlTemplateFilename.setEnabled( wTemplate.getSelection() );
    wTemplateFilename.setEnabled( wTemplate.getSelection() );
    wbTemplateFilename.setEnabled( wTemplate.getSelection() );
    wlTemplateAppend.setEnabled( wTemplate.getSelection() );
    wTemplateAppend.setEnabled( wTemplate.getSelection() );

  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        ITableItemInsertListener listener = ( tableItem, v ) -> {
          if ( v.isNumber() ) {
            if ( v.getLength() > 0 ) {
              int le = v.getLength();
              int pr = v.getPrecision();

              if ( v.getPrecision() <= 0 ) {
                pr = 0;
              }

              String mask = "";
              for ( int m = 0; m < le - pr; m++ ) {
                mask += "0";
              }
              if ( pr > 0 ) {
                mask += ".";
              }
              for ( int m = 0; m < pr; m++ ) {
                mask += "0";
              }
              tableItem.setText( 3, mask );
            }
          }
          return true;
        };
        BaseTransformDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] { 2 }, 4, 5, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
        .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
    }

  }

  /**
   * Sets the output width to minimal width...
   */
  public void setMinimalWidth() {
    int nrNonEmptyFields = wFields.nrNonEmpty();
    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      item.setText( 4, "" );
      item.setText( 5, "" );

      int type = ValueMetaFactory.getIdForValueMeta( item.getText( 2 ) );
      switch ( type ) {
        case IValueMeta.TYPE_STRING:
          item.setText( 3, "" );
          break;
        case IValueMeta.TYPE_INTEGER:
          item.setText( 3, "0" );
          break;
        case IValueMeta.TYPE_NUMBER:
          item.setText( 3, "0.#####" );
          break;
        case IValueMeta.TYPE_DATE:
          break;
        default:
          break;
      }
    }
    wFields.optWidth( true );
  }

  private void useTempFile() {
    wTempDirectory.setEnabled( wUseTempFiles.getSelection() );
    wlTempDirectory.setEnabled( wUseTempFiles.getSelection() );
    wbTempDir.setEnabled( wUseTempFiles.getSelection() );
  }
}
