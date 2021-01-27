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

package org.apache.hop.pipeline.transforms.xml.xmloutput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
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

public class XmlOutputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = XmlOutputMeta.class; // For Translator

  private Label wlFilename;
  private TextVar wFilename;

  private Label wlExtension;
  private TextVar wExtension;

  private Button wServletOutput;

  private Label wlAddTransformnr;
  private Button wAddTransformnr;

  private Label wlAddDate;
  private Button wAddDate;

  private Label wlAddTime;
  private Button wAddTime;

  private Button wbShowFiles;

  private Button wZipped;

  private Button wOmitNullValues;

  private CCombo wEncoding;

  private Text wNameSpace;

  private CCombo wMainElement;

  private CCombo wRepeatElement;

  private Label wlSplitEvery;
  private Text wSplitEvery;

  private TableView wFields;

  private final XmlOutputMeta input;

  private boolean gotEncodings = false;

  private Label wlAddToResult;
  private Button wAddToResult;

  private Label wlDoNotOpenNewFileInit;
  private Button wDoNotOpenNewFileInit;

  private Label wlSpecifyFormat;
  private Button wSpecifyFormat;

  private Label wlDateTimeFormat;
  private CCombo wDateTimeFormat;

  private ColumnInfo[] colinf;

  private final Map<String, Integer> inputFields;

  public XmlOutputDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (XmlOutputMeta) in;
    inputFields = new HashMap<>();
  }

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
    shell.setText( BaseMessages.getString( PKG, "XMLOutputDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

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
    wFileTab.setText( BaseMessages.getString( PKG, "XMLOutputDialog.FileTab.Tab" ) );

    Composite wFileComp = new Composite(wTabFolder, SWT.NONE );
    props.setLook( wFileComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout( fileLayout );

    // Filename line
    wlFilename = new Label( wFileComp, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "XMLOutputDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( 0, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData(fdlFilename);

    Button wbFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbFilename);
    wbFilename.setText( BaseMessages.getString( PKG, "XMLOutputDialog.Browse.Button" ) );
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

    // Open new File at Init
    wlDoNotOpenNewFileInit = new Label( wFileComp, SWT.RIGHT );
    wlDoNotOpenNewFileInit.setText( BaseMessages.getString( PKG, "XMLOutputDialog.DoNotOpenNewFileInit.Label" ) );
    props.setLook( wlDoNotOpenNewFileInit );
    FormData fdlDoNotOpenNewFileInit = new FormData();
    fdlDoNotOpenNewFileInit.left = new FormAttachment( 0, 0 );
    fdlDoNotOpenNewFileInit.top = new FormAttachment( wFilename, margin );
    fdlDoNotOpenNewFileInit.right = new FormAttachment( middle, -margin );
    wlDoNotOpenNewFileInit.setLayoutData(fdlDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit = new Button( wFileComp, SWT.CHECK );
    wDoNotOpenNewFileInit.setToolTipText( BaseMessages.getString( PKG, "XMLOutputDialog.DoNotOpenNewFileInit.Tooltip" ) );
    props.setLook( wDoNotOpenNewFileInit );
    FormData fdDoNotOpenNewFileInit = new FormData();
    fdDoNotOpenNewFileInit.left = new FormAttachment( middle, 0 );
    fdDoNotOpenNewFileInit.top = new FormAttachment( wlDoNotOpenNewFileInit, 0, SWT.CENTER );
    fdDoNotOpenNewFileInit.right = new FormAttachment( 100, 0 );
    wDoNotOpenNewFileInit.setLayoutData(fdDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Run this as a command instead?
    Label wlServletOutput = new Label(wFileComp, SWT.RIGHT);
    wlServletOutput.setText( BaseMessages.getString( PKG, "XMLOutputDialog.ServletOutput.Label" ) );
    props.setLook(wlServletOutput);
    FormData fdlServletOutput = new FormData();
    fdlServletOutput.left = new FormAttachment( 0, 0 );
    fdlServletOutput.top = new FormAttachment( wDoNotOpenNewFileInit, margin );
    fdlServletOutput.right = new FormAttachment( middle, -margin );
    wlServletOutput.setLayoutData(fdlServletOutput);
    wServletOutput = new Button( wFileComp, SWT.CHECK );
    wServletOutput.setToolTipText( BaseMessages.getString( PKG, "XMLOutputDialog.ServletOutput.Tooltip" ) );
    props.setLook( wServletOutput );
    FormData fdServletOutput = new FormData();
    fdServletOutput.left = new FormAttachment( middle, 0 );
    fdServletOutput.top = new FormAttachment( wlServletOutput, 0, SWT.CENTER );
    fdServletOutput.right = new FormAttachment( 100, 0 );
    wServletOutput.setLayoutData(fdServletOutput);
    wServletOutput.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setFlagsServletOption();
      }
    } );

    // Extension line
    wlExtension = new Label( wFileComp, SWT.RIGHT );
    wlExtension.setText( BaseMessages.getString( PKG, "XMLOutputDialog.Extension.Label" ) );
    props.setLook( wlExtension );
    FormData fdlExtension = new FormData();
    fdlExtension.left = new FormAttachment( 0, 0 );
    fdlExtension.top = new FormAttachment( wServletOutput, margin );
    fdlExtension.right = new FormAttachment( middle, -margin );
    wlExtension.setLayoutData(fdlExtension);
    wExtension = new TextVar( variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wExtension.setText( "" );
    props.setLook( wExtension );
    wExtension.addModifyListener( lsMod );
    FormData fdExtension = new FormData();
    fdExtension.left = new FormAttachment( middle, 0 );
    fdExtension.top = new FormAttachment( wServletOutput, margin );
    fdExtension.right = new FormAttachment( 100, 0 );
    wExtension.setLayoutData(fdExtension);

    // Create multi-part file?
    wlAddTransformnr = new Label( wFileComp, SWT.RIGHT );
    wlAddTransformnr.setText( BaseMessages.getString( PKG, "XMLOutputDialog.AddTransformNr.Label" ) );
    props.setLook( wlAddTransformnr );
    FormData fdlAddTransformnr = new FormData();
    fdlAddTransformnr.left = new FormAttachment( 0, 0 );
    fdlAddTransformnr.top = new FormAttachment( wExtension, margin );
    fdlAddTransformnr.right = new FormAttachment( middle, -margin );
    wlAddTransformnr.setLayoutData(fdlAddTransformnr);
    wAddTransformnr = new Button( wFileComp, SWT.CHECK );
    props.setLook( wAddTransformnr );
    FormData fdAddTransformnr = new FormData();
    fdAddTransformnr.left = new FormAttachment( middle, 0 );
    fdAddTransformnr.top = new FormAttachment( wlAddTransformnr, 0, SWT.CENTER);
    fdAddTransformnr.right = new FormAttachment( 100, 0 );
    wAddTransformnr.setLayoutData(fdAddTransformnr);
    wAddTransformnr.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddDate = new Label( wFileComp, SWT.RIGHT );
    wlAddDate.setText( BaseMessages.getString( PKG, "XMLOutputDialog.AddDate.Label" ) );
    props.setLook( wlAddDate );
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment( 0, 0 );
    fdlAddDate.top = new FormAttachment( wAddTransformnr, margin );
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
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );
    // Create multi-part file?
    wlAddTime = new Label( wFileComp, SWT.RIGHT );
    wlAddTime.setText( BaseMessages.getString( PKG, "XMLOutputDialog.AddTime.Label" ) );
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
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Specify date time format?
    wlSpecifyFormat = new Label( wFileComp, SWT.RIGHT );
    wlSpecifyFormat.setText( BaseMessages.getString( PKG, "XMLOutputDialog.SpecifyFormat.Label" ) );
    props.setLook( wlSpecifyFormat );
    FormData fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment( 0, 0 );
    fdlSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdlSpecifyFormat.right = new FormAttachment( middle, -margin );
    wlSpecifyFormat.setLayoutData(fdlSpecifyFormat);
    wSpecifyFormat = new Button( wFileComp, SWT.CHECK );
    props.setLook( wSpecifyFormat );
    wSpecifyFormat.setToolTipText( BaseMessages.getString( PKG, "XMLOutputDialog.SpecifyFormat.Tooltip" ) );
    FormData fdSpecifyFormat = new FormData();
    fdSpecifyFormat.left = new FormAttachment( middle, 0 );
    fdSpecifyFormat.top = new FormAttachment( wlSpecifyFormat, 0, SWT.CENTER );
    fdSpecifyFormat.right = new FormAttachment( 100, 0 );
    wSpecifyFormat.setLayoutData(fdSpecifyFormat);
    wSpecifyFormat.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setDateTimeFormat();
      }
    } );

    // Prepare a list of possible DateTimeFormats...
    String[] dats = Const.getDateFormats();

    // DateTimeFormat
    wlDateTimeFormat = new Label( wFileComp, SWT.RIGHT );
    wlDateTimeFormat.setText( BaseMessages.getString( PKG, "XMLOutputDialog.DateTimeFormat.Label" ) );
    props.setLook( wlDateTimeFormat );
    FormData fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment( 0, 0 );
    fdlDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdlDateTimeFormat.right = new FormAttachment( middle, -margin );
    wlDateTimeFormat.setLayoutData(fdlDateTimeFormat);
    wDateTimeFormat = new CCombo( wFileComp, SWT.BORDER | SWT.READ_ONLY );
    wDateTimeFormat.setEditable( true );
    props.setLook( wDateTimeFormat );
    wDateTimeFormat.addModifyListener( lsMod );
    FormData fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment( middle, 0 );
    fdDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdDateTimeFormat.right = new FormAttachment( 100, 0 );
    wDateTimeFormat.setLayoutData(fdDateTimeFormat);
    for (String dat : dats) {
      wDateTimeFormat.add(dat);
    }

    wbShowFiles = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowFiles );
    wbShowFiles.setText( BaseMessages.getString( PKG, "XMLOutputDialog.ShowFiles.Button" ) );
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.top = new FormAttachment( wDateTimeFormat, margin * 2 );
    wbShowFiles.setLayoutData(fdbShowFiles);
    wbShowFiles.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        XmlOutputMeta tfoi = new XmlOutputMeta();
        getInfo( tfoi );
        String[] files = tfoi.getFiles( variables );
        if ( files != null && files.length > 0 ) {
          EnterSelectionDialog esd =
              new EnterSelectionDialog( shell, files, BaseMessages.getString( PKG,
                  "XMLOutputDialog.OutputFiles.DialogTitle" ), BaseMessages.getString( PKG,
                  "XMLOutputDialog.OutputFiles.DialogMessage" ) );
          esd.setViewOnly();
          esd.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "XMLOutputDialog.NoFilesFound.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
          mb.open();
        }
      }
    } );

    // Add File to the result files name
    wlAddToResult = new Label( wFileComp, SWT.RIGHT );
    wlAddToResult.setText( BaseMessages.getString( PKG, "XMLOutputDialog.AddFileToResult.Label" ) );
    props.setLook( wlAddToResult );
    FormData fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment( 0, 0 );
    fdlAddToResult.top = new FormAttachment( wbShowFiles, 2 * margin );
    fdlAddToResult.right = new FormAttachment( middle, -margin );
    wlAddToResult.setLayoutData(fdlAddToResult);
    wAddToResult = new Button( wFileComp, SWT.CHECK );
    wAddToResult.setToolTipText( BaseMessages.getString( PKG, "XMLOutputDialog.AddFileToResult.Tooltip" ) );
    props.setLook( wAddToResult );
    FormData fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment( middle, 0 );
    fdAddToResult.top = new FormAttachment( wlAddToResult, 0, SWT.CENTER );
    fdAddToResult.right = new FormAttachment( 100, 0 );
    wAddToResult.setLayoutData(fdAddToResult);
    SelectionAdapter lsSelR = new SelectionAdapter() {
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
    wContentTab.setText( BaseMessages.getString( PKG, "XMLOutputDialog.ContentTab.TabTitle" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE );
    props.setLook( wContentComp );
    wContentComp.setLayout( contentLayout );

    Label wlZipped = new Label(wContentComp, SWT.RIGHT);
    wlZipped.setText( BaseMessages.getString( PKG, "XMLOutputDialog.Zipped.Label" ) );
    props.setLook(wlZipped);
    FormData fdlZipped = new FormData();
    fdlZipped.left = new FormAttachment( 0, 0 );
    fdlZipped.top = new FormAttachment( 0, 0 );
    fdlZipped.right = new FormAttachment( middle, -margin );
    wlZipped.setLayoutData(fdlZipped);
    wZipped = new Button( wContentComp, SWT.CHECK );
    props.setLook( wZipped );
    FormData fdZipped = new FormData();
    fdZipped.left = new FormAttachment( middle, 0 );
    fdZipped.top = new FormAttachment( wlZipped, 0, SWT.CENTER );
    fdZipped.right = new FormAttachment( 100, 0 );
    wZipped.setLayoutData(fdZipped);
    wZipped.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    Label wlEncoding = new Label(wContentComp, SWT.RIGHT);
    wlEncoding.setText( BaseMessages.getString( PKG, "XMLOutputDialog.Encoding.Label" ) );
    props.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment( 0, 0 );
    fdlEncoding.top = new FormAttachment( wZipped, margin );
    fdlEncoding.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new CCombo( wContentComp, SWT.BORDER | SWT.READ_ONLY );
    wEncoding.setEditable( true );
    props.setLook( wEncoding );
    wEncoding.addModifyListener( lsMod );
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment( middle, 0 );
    fdEncoding.top = new FormAttachment( wZipped, margin );
    fdEncoding.right = new FormAttachment( 100, 0 );
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setEncodings();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    Label wlNameSpace = new Label(wContentComp, SWT.RIGHT);
    wlNameSpace.setText( BaseMessages.getString( PKG, "XMLOutputDialog.NameSpace.Label" ) );
    props.setLook(wlNameSpace);
    FormData fdlNameSpace = new FormData();
    fdlNameSpace.left = new FormAttachment( 0, 0 );
    fdlNameSpace.top = new FormAttachment( wEncoding, margin );
    fdlNameSpace.right = new FormAttachment( middle, -margin );
    wlNameSpace.setLayoutData(fdlNameSpace);
    wNameSpace = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wNameSpace );
    wNameSpace.addModifyListener( lsMod );
    FormData fdNameSpace = new FormData();
    fdNameSpace.left = new FormAttachment( middle, 0 );
    fdNameSpace.top = new FormAttachment( wEncoding, margin );
    fdNameSpace.right = new FormAttachment( 100, 0 );
    wNameSpace.setLayoutData(fdNameSpace);

    Label wlMainElement = new Label(wContentComp, SWT.RIGHT);
    wlMainElement.setText( BaseMessages.getString( PKG, "XMLOutputDialog.MainElement.Label" ) );
    props.setLook(wlMainElement);
    FormData fdlMainElement = new FormData();
    fdlMainElement.left = new FormAttachment( 0, 0 );
    fdlMainElement.top = new FormAttachment( wNameSpace, margin );
    fdlMainElement.right = new FormAttachment( middle, -margin );
    wlMainElement.setLayoutData(fdlMainElement);
    wMainElement = new CCombo( wContentComp, SWT.BORDER | SWT.READ_ONLY );
    wMainElement.setEditable( true );
    props.setLook( wMainElement );
    wMainElement.addModifyListener( lsMod );
    FormData fdMainElement = new FormData();
    fdMainElement.left = new FormAttachment( middle, 0 );
    fdMainElement.top = new FormAttachment( wNameSpace, margin );
    fdMainElement.right = new FormAttachment( 100, 0 );
    wMainElement.setLayoutData(fdMainElement);

    Label wlRepeatElement = new Label(wContentComp, SWT.RIGHT);
    wlRepeatElement.setText( BaseMessages.getString( PKG, "XMLOutputDialog.RepeatElement.Label" ) );
    props.setLook(wlRepeatElement);
    FormData fdlRepeatElement = new FormData();
    fdlRepeatElement.left = new FormAttachment( 0, 0 );
    fdlRepeatElement.top = new FormAttachment( wMainElement, margin );
    fdlRepeatElement.right = new FormAttachment( middle, -margin );
    wlRepeatElement.setLayoutData(fdlRepeatElement);
    wRepeatElement = new CCombo( wContentComp, SWT.BORDER | SWT.READ_ONLY );
    wRepeatElement.setEditable( true );
    props.setLook( wRepeatElement );
    wRepeatElement.addModifyListener( lsMod );
    FormData fdRepeatElement = new FormData();
    fdRepeatElement.left = new FormAttachment( middle, 0 );
    fdRepeatElement.top = new FormAttachment( wMainElement, margin );
    fdRepeatElement.right = new FormAttachment( 100, 0 );
    wRepeatElement.setLayoutData(fdRepeatElement);

    wlSplitEvery = new Label( wContentComp, SWT.RIGHT );
    wlSplitEvery.setText( BaseMessages.getString( PKG, "XMLOutputDialog.SplitEvery.Label" ) );
    props.setLook( wlSplitEvery );
    FormData fdlSplitEvery = new FormData();
    fdlSplitEvery.left = new FormAttachment( 0, 0 );
    fdlSplitEvery.top = new FormAttachment( wRepeatElement, margin );
    fdlSplitEvery.right = new FormAttachment( middle, -margin );
    wlSplitEvery.setLayoutData(fdlSplitEvery);
    wSplitEvery = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSplitEvery );
    wSplitEvery.addModifyListener( lsMod );
    FormData fdSplitEvery = new FormData();
    fdSplitEvery.left = new FormAttachment( middle, 0 );
    fdSplitEvery.top = new FormAttachment( wRepeatElement, margin );
    fdSplitEvery.right = new FormAttachment( 100, 0 );
    wSplitEvery.setLayoutData(fdSplitEvery);

    Label wlOmitNullValues = new Label(wContentComp, SWT.RIGHT);
    wlOmitNullValues.setText( BaseMessages.getString( PKG, "XMLOutputDialog.OmitNullValues.Label" ) );
    props.setLook(wlOmitNullValues);
    FormData fdlOmitNullValues = new FormData();
    fdlOmitNullValues.left = new FormAttachment( 0, 0 );
    fdlOmitNullValues.top = new FormAttachment( wSplitEvery, margin );
    fdlOmitNullValues.right = new FormAttachment( middle, -margin );
    wlOmitNullValues.setLayoutData(fdlOmitNullValues);
    wOmitNullValues = new Button( wContentComp, SWT.CHECK );
    props.setLook( wOmitNullValues );
    FormData fdOmitNullValues = new FormData();
    fdOmitNullValues.left = new FormAttachment( middle, 0 );
    fdOmitNullValues.top = new FormAttachment( wlOmitNullValues, 0, SWT.CENTER );
    fdOmitNullValues.right = new FormAttachment( 100, 0 );
    wOmitNullValues.setLayoutData(fdOmitNullValues);
    wOmitNullValues.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

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

    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText( BaseMessages.getString( PKG, "XMLOutputDialog.FieldsTab.TabTitle" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE );
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook( wFieldsComp );

    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "XMLOutputDialog.Get.Button" ) );
    wGet.setToolTipText( BaseMessages.getString( PKG, "XMLOutputDialog.Get.Tooltip" ) );

    Button wMinWidth = new Button(wFieldsComp, SWT.PUSH);
    wMinWidth.setText( BaseMessages.getString( PKG, "XMLOutputDialog.MinWidth.Label" ) );
    wMinWidth.setToolTipText( BaseMessages.getString( PKG, "XMLOutputDialog.MinWidth.Tooltip" ) );

    setButtonPositions( new Button[] { wGet, wMinWidth}, margin, null );

    final int FieldsRows = input.getOutputFields().length;

    // Prepare a list of possible formats...
    String[] nums = Const.getNumberFormats();
    int totsize = dats.length + nums.length;
    String[] formats = new String[totsize];
    for ( int x = 0; x < dats.length; x++ ) {
      formats[x] = dats[x];
    }
    for ( int x = 0; x < nums.length; x++ ) {
      formats[dats.length + x] = nums[x];
    }

    colinf =
        new ColumnInfo[] {
          new ColumnInfo( BaseMessages.getString( PKG, "XMLOutputDialog.Fieldname.Column" ),
              ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "XMLOutputDialog.ElementName.Column" ),
              ColumnInfo.COLUMN_TYPE_TEXT, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "XMLOutputDialog.ContentType.Column" ),
              ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "Element", "Attribute", }, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "XMLOutputDialog.Type.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaBase.getTypes() ),
          new ColumnInfo( BaseMessages.getString( PKG, "XMLOutputDialog.Format.Column" ),
              ColumnInfo.COLUMN_TYPE_CCOMBO, formats ),
          new ColumnInfo( BaseMessages.getString( PKG, "XMLOutputDialog.Length.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
              false ),
          new ColumnInfo( BaseMessages.getString( PKG, "XMLOutputDialog.Precision.Column" ),
              ColumnInfo.COLUMN_TYPE_TEXT, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "XMLOutputDialog.Currency.Column" ),
              ColumnInfo.COLUMN_TYPE_TEXT, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "XMLOutputDialog.Decimal.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
              false ),
          new ColumnInfo( BaseMessages.getString( PKG, "XMLOutputDialog.Group.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
              false ),
          new ColumnInfo( BaseMessages.getString( PKG, "XMLOutputDialog.Null.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
              false ) };

    wFields =
        new TableView( variables, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod,
            props );

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
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wFilename.addSelectionListener( lsDef );

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener( e -> wFilename.setToolTipText( variables.resolve( wFilename.getText() ) ) );


    wbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( true, shell, wFilename, variables,
            new String[] {"*.xml", "*"},
            new String[] {
                    BaseMessages.getString( PKG, "System.FileType.XMLFiles" ),
                    BaseMessages.getString( PKG, "System.FileType.AllFiles" ) },
            true )
    );

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

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    setDateTimeFormat();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  protected void setFlagsServletOption() {
    boolean enableFilename = !wServletOutput.getSelection();
    wlFilename.setEnabled( enableFilename );
    wFilename.setEnabled( enableFilename );
    wlDoNotOpenNewFileInit.setEnabled( enableFilename );
    wDoNotOpenNewFileInit.setEnabled( enableFilename );

    wlExtension.setEnabled( enableFilename );
    wExtension.setEnabled( enableFilename );
    wlSplitEvery.setEnabled( enableFilename );
    wSplitEvery.setEnabled( enableFilename );
    wlAddDate.setEnabled( enableFilename );
    wAddDate.setEnabled( enableFilename );
    wlAddTime.setEnabled( enableFilename );
    wAddTime.setEnabled( enableFilename );
    wlDateTimeFormat.setEnabled( enableFilename );
    wDateTimeFormat.setEnabled( enableFilename );
    wlSpecifyFormat.setEnabled( enableFilename );
    wSpecifyFormat.setEnabled( enableFilename );
    wlAddTransformnr.setEnabled( enableFilename );
    wAddTransformnr.setEnabled( enableFilename );
    wbShowFiles.setEnabled( enableFilename );
    wlAddToResult.setEnabled( enableFilename );
    wAddToResult.setEnabled( enableFilename );
  }

  private void setDateTimeFormat() {
    if ( wSpecifyFormat.getSelection() ) {
      wAddDate.setSelection( false );
      wAddTime.setSelection( false );
    }

    wDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wlDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wAddDate.setEnabled( !wSpecifyFormat.getSelection() && !wServletOutput.getSelection() );
    wlAddDate.setEnabled( !wSpecifyFormat.getSelection() && !wServletOutput.getSelection() );
    wAddTime.setEnabled( !wSpecifyFormat.getSelection() && !wServletOutput.getSelection() );
    wlAddTime.setEnabled( !wSpecifyFormat.getSelection() && !wServletOutput.getSelection() );

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
    if ( input.getExtension() != null ) {
      wExtension.setText( input.getExtension() );
    }
    wDoNotOpenNewFileInit.setSelection( input.isDoNotOpenNewFileInit() );
    wServletOutput.setSelection( input.isServletOutput() );
    setFlagsServletOption();

    if ( input.getEncoding() != null ) {
      wEncoding.setText( input.getEncoding() );
    }
    if ( input.getNameSpace() != null ) {
      wNameSpace.setText( input.getNameSpace() );
    }
    if ( input.getMainElement() != null ) {
      wMainElement.setText( input.getMainElement() );
    }
    if ( input.getRepeatElement() != null ) {
      wRepeatElement.setText( input.getRepeatElement() );
    }

    wSplitEvery.setText( "" + input.getSplitEvery() );

    wZipped.setSelection( input.isZipped() );
    wOmitNullValues.setSelection( input.isOmitNullValues() );
    wAddDate.setSelection( input.isDateInFilename() );
    wAddTime.setSelection( input.isTimeInFilename() );
    wAddTransformnr.setSelection( input.isTransformNrInFilename() );

    wAddToResult.setSelection( input.isAddToResultFiles() );

    if ( input.getDateTimeFormat() != null ) {
      wDateTimeFormat.setText( input.getDateTimeFormat() );
    }
    wSpecifyFormat.setSelection( input.isSpecifyFormat() );

    if ( isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "XMLOutputDialog.Log.GettingFieldsInfo" ) );
    }

    for ( int i = 0; i < input.getOutputFields().length; i++ ) {
      XmlField field = input.getOutputFields()[i];

      TableItem item = wFields.table.getItem( i );
      int index = 1;

      if ( field.getFieldName() != null ) {
        item.setText( index++, field.getFieldName() );
      }
      if ( field.getElementName() != null ) {
        item.setText( index++, field.getElementName() );
      } else {
        // Fixup for defect JIRA PDI-607. Make it the same functionality
        // as the loading of the original XML file.
        if ( field.getFieldName() != null ) {
          item.setText( index++, field.getFieldName() );
        } else {
          index++;
        }
      }
      item.setText( index++, field.getContentType().name() );
      item.setText( index++, field.getTypeDesc() );
      if ( field.getFormat() != null ) {
        item.setText( index++, field.getFormat() );
      } else {
        index++;
      }
      if ( field.getLength() >= 0 ) {
        item.setText( index++, "" + field.getLength() );
      } else {
        index++;
      }
      if ( field.getPrecision() >= 0 ) {
        item.setText( index++, "" + field.getPrecision() );
      } else {
        index++;
      }
      if ( field.getCurrencySymbol() != null ) {
        item.setText( index++, field.getCurrencySymbol() );
      } else {
        index++;
      }
      if ( field.getDecimalSymbol() != null ) {
        item.setText( index++, field.getDecimalSymbol() );
      } else {
        index++;
      }
      if ( field.getGroupingSymbol() != null ) {
        item.setText( index++, field.getGroupingSymbol() );
      } else {
        index++;
      }
      if ( field.getNullString() != null ) {
        item.setText( index++, field.getNullString() );
      } else {
        index++;
      }
    }

    wFields.optWidth( true );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;

    input.setChanged( backupChanged );

    dispose();
  }

  private void getInfo( XmlOutputMeta xmlOutputMeta ) {
    xmlOutputMeta.setFileName( wFilename.getText() );
    xmlOutputMeta.setEncoding( wEncoding.getText() );
    xmlOutputMeta.setNameSpace( wNameSpace.getText() );
    xmlOutputMeta.setMainElement( wMainElement.getText() );
    xmlOutputMeta.setRepeatElement( wRepeatElement.getText() );
    xmlOutputMeta.setExtension( wExtension.getText() );
    xmlOutputMeta.setDoNotOpenNewFileInit( wDoNotOpenNewFileInit.getSelection() );
    xmlOutputMeta.setServletOutput( wServletOutput.getSelection() );
    xmlOutputMeta.setSplitEvery( Const.toInt( wSplitEvery.getText(), 0 ) );

    xmlOutputMeta.setDateTimeFormat( wDateTimeFormat.getText() );
    xmlOutputMeta.setSpecifyFormat( wSpecifyFormat.getSelection() );

    xmlOutputMeta.setTransformNrInFilename( wAddTransformnr.getSelection() );
    xmlOutputMeta.setDateInFilename( wAddDate.getSelection() );
    xmlOutputMeta.setTimeInFilename( wAddTime.getSelection() );
    xmlOutputMeta.setAddToResultFiles( wAddToResult.getSelection() );
    xmlOutputMeta.setZipped( wZipped.getSelection() );
    xmlOutputMeta.setOmitNullValues( wOmitNullValues.getSelection() );

    // Table table = wFields.table;

    int nrFields = wFields.nrNonEmpty();

    xmlOutputMeta.allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      XmlField field = new XmlField();

      TableItem item = wFields.getNonEmpty( i );
      int index = 1;
      field.setFieldName( item.getText( index++ ) );
      field.setElementName( item.getText( index++ ) );
      if ( field.getFieldName().equals( field.getElementName() ) ) {
        field.setElementName( "" );
      }
      field.setContentType( XmlField.ContentType.getIfPresent( item.getText( index++ ) ) );
      field.setType( item.getText( index++ ) );
      field.setFormat( item.getText( index++ ) );
      field.setLength( Const.toInt( item.getText( index++ ), -1 ) );
      field.setPrecision( Const.toInt( item.getText( index++ ), -1 ) );
      field.setCurrencySymbol( item.getText( index++ ) );
      field.setDecimalSymbol( item.getText( index++ ) );
      field.setGroupingSymbol( item.getText( index++ ) );
      field.setNullString( item.getText( index++ ) );

      // CHECKSTYLE:Indentation:OFF
      xmlOutputMeta.getOutputFields()[i] = field;
    }
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo( input );

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null && !r.isEmpty() ) {
        ITableItemInsertListener listener = ( tableItem, v ) -> {
          tableItem.setText( 3, XmlField.ContentType.Element.name() );
          if ( v.isNumber() ) {
            if ( v.getLength() > 0 ) {
              int le = v.getLength();
              int pr = v.getPrecision();

              if ( v.getPrecision() <= 0 ) {
                pr = 0;
              }

              String mask = " ";
              for ( int m = 0; m < le - pr; m++ ) {
                mask += "0";
              }
              if ( pr > 0 ) {
                mask += ".";
              }
              for ( int m = 0; m < pr; m++ ) {
                mask += "0";
              }
              tableItem.setText( 4, mask );
            }
          }
          return true;
        };
        BaseTransformDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] { 4 }, 6, 7, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
          .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
    }

  }

  /**
   * Sets the output width to minimal width...
   * 
   */
  public void setMinimalWidth() {
    int nrNonEmptyFields = wFields.nrNonEmpty();
    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      item.setText( 5, "" );
      item.setText( 6, "" );

      int type = ValueMetaBase.getType( item.getText( 2 ) );
      switch ( type ) {
        case IValueMeta.TYPE_STRING:
          item.setText( 4, "" );
          break;
        case IValueMeta.TYPE_INTEGER:
          item.setText( 4, "0" );
          break;
        case IValueMeta.TYPE_NUMBER:
          item.setText( 4, "0.#####" );
          break;
        case IValueMeta.TYPE_DATE:
          break;
        default:
          break;
      }
    }
    wFields.optWidth( true );
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>(keySet);

    String[] fieldNames = entries.toArray( new String[entries.size()] );

    Const.sortStrings( fieldNames );
    colinf[0].setComboValues( fieldNames );
  }
}
