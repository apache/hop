/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.sqlfileoutput;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.Props;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SqlEditor;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class SQLFileOutputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SQLFileOutputMeta.class;
  public static final String CONST_SYSTEM_DIALOG_ERROR_TITLE = "System.Dialog.Error.Title";

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wSchema;

  private TextVar wTable;

  private Label wlTruncate;
  private Button wTruncate;

  private Button wStartNewLine;

  private Button wAddToResult;

  private Button wAddCreate;

  private TextVar wFilename;

  private TextVar wExtension;

  private Button wAddTransformNr;

  private Button wAddDate;

  private Button wAddTime;

  private Button wAppend;

  private Text wSplitEvery;

  private CCombo wEncoding;

  private CCombo wFormat;

  private boolean gotEncodings = false;

  private Button wCreateParentFolder;

  private Button wDoNotOpenNewFileInit;

  private final SQLFileOutputMeta input;

  public SQLFileOutputDialog(
      Shell parent,
      IVariables variables,
      SQLFileOutputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "SQLFileOutputDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).sql(e -> sql()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.GeneralTab.TabTitle"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    // Connection grouping?
    // ////////////////////////
    // START OF Connection GROUP
    //

    Group wGConnection = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wGConnection);
    wGConnection.setText(
        BaseMessages.getString(PKG, "SQLFileOutputDialog.Group.ConnectionInfos.Label"));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wGConnection.setLayout(groupLayout);

    // Connection line
    wConnection = addConnectionLine(wGConnection, null, input.getConnection(), lsMod);

    // Schema line...
    Label wlSchema = new Label(wGConnection, SWT.RIGHT);
    wlSchema.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.TargetSchema.Label"));
    PropsUi.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment(0, 0);
    fdlSchema.right = new FormAttachment(middle, -margin);
    fdlSchema.top = new FormAttachment(wConnection, margin);
    wlSchema.setLayoutData(fdlSchema);

    wSchema = new TextVar(variables, wGConnection, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchema);
    wSchema.addModifyListener(lsMod);
    wSchema.setToolTipText(BaseMessages.getString(PKG, "SQLFileOutputDialog.TargetSchema.Tooltip"));
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.top = new FormAttachment(wConnection, margin);
    fdSchema.right = new FormAttachment(100, 0);
    wSchema.setLayoutData(fdSchema);

    // Table line...
    Label wlTable = new Label(wGConnection, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.TargetTable.Label"));
    PropsUi.setLook(wlTable);
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.right = new FormAttachment(middle, -margin);
    fdlTable.top = new FormAttachment(wSchema, margin);
    wlTable.setLayoutData(fdlTable);

    Button wbTable = new Button(wGConnection, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wSchema, margin);
    wbTable.setLayoutData(fdbTable);

    wTable = new TextVar(variables, wGConnection, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTable);
    wTable.setToolTipText(BaseMessages.getString(PKG, "SQLFileOutputDialog.TargetTable.Tooltip"));
    wTable.addModifyListener(lsMod);
    FormData fdTable = new FormData();
    fdTable.top = new FormAttachment(wSchema, margin);
    fdTable.left = new FormAttachment(middle, 0);
    fdTable.right = new FormAttachment(wbTable, -margin);
    wTable.setLayoutData(fdTable);

    FormData fdGConnection = new FormData();
    fdGConnection.left = new FormAttachment(0, margin);
    fdGConnection.top = new FormAttachment(wSpacer, margin);
    fdGConnection.right = new FormAttachment(100, -margin);
    wGConnection.setLayoutData(fdGConnection);

    // ///////////////////////////////////////////////////////////
    // / END OF Connection GROUP
    // ///////////////////////////////////////////////////////////

    // Connection grouping?
    // ////////////////////////
    // START OF FileName GROUP
    //

    Group wFileName = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wFileName);
    wFileName.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.Group.File.Label"));

    FormLayout groupFileLayout = new FormLayout();
    groupFileLayout.marginWidth = 10;
    groupFileLayout.marginHeight = 10;
    wFileName.setLayout(groupFileLayout);

    // Add Create table
    Label wlAddCreate = new Label(wFileName, SWT.RIGHT);
    wlAddCreate.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.CreateTable.Label"));
    PropsUi.setLook(wlAddCreate);
    FormData fdlAddCreate = new FormData();
    fdlAddCreate.left = new FormAttachment(0, 0);
    fdlAddCreate.top = new FormAttachment(wGConnection, margin);
    fdlAddCreate.right = new FormAttachment(middle, -margin);
    wlAddCreate.setLayoutData(fdlAddCreate);
    wAddCreate = new Button(wFileName, SWT.CHECK);
    wAddCreate.setToolTipText(
        BaseMessages.getString(PKG, "SQLFileOutputDialog.CreateTable.Tooltip"));
    PropsUi.setLook(wAddCreate);
    FormData fdAddCreate = new FormData();
    fdAddCreate.left = new FormAttachment(middle, 0);
    fdAddCreate.top = new FormAttachment(wlAddCreate, 0, SWT.CENTER);
    fdAddCreate.right = new FormAttachment(100, 0);
    wAddCreate.setLayoutData(fdAddCreate);
    SelectionAdapter lsSelMod =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            activateTruncate();
            input.setChanged();
          }
        };
    wAddCreate.addSelectionListener(lsSelMod);

    // Truncate table
    wlTruncate = new Label(wFileName, SWT.RIGHT);
    wlTruncate.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.TruncateTable.Label"));
    PropsUi.setLook(wlTruncate);
    FormData fdlTruncate = new FormData();
    fdlTruncate.left = new FormAttachment(0, 0);
    fdlTruncate.top = new FormAttachment(wAddCreate, margin);
    fdlTruncate.right = new FormAttachment(middle, -margin);
    wlTruncate.setLayoutData(fdlTruncate);
    wTruncate = new Button(wFileName, SWT.CHECK);
    wTruncate.setToolTipText(
        BaseMessages.getString(PKG, "SQLFileOutputDialog.TruncateTable.Tooltip"));
    PropsUi.setLook(wTruncate);
    FormData fdTruncate = new FormData();
    fdTruncate.left = new FormAttachment(middle, 0);
    fdTruncate.top = new FormAttachment(wlTruncate, 0, SWT.CENTER);
    fdTruncate.right = new FormAttachment(100, 0);
    wTruncate.setLayoutData(fdTruncate);
    SelectionAdapter lsSelTMod =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wTruncate.addSelectionListener(lsSelTMod);

    // Start New Line For each statement
    Label wlStartNewLine = new Label(wFileName, SWT.RIGHT);
    wlStartNewLine.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.StartNewLine.Label"));
    PropsUi.setLook(wlStartNewLine);
    FormData fdlStartNewLine = new FormData();
    fdlStartNewLine.left = new FormAttachment(0, 0);
    fdlStartNewLine.top = new FormAttachment(wTruncate, margin);
    fdlStartNewLine.right = new FormAttachment(middle, -margin);
    wlStartNewLine.setLayoutData(fdlStartNewLine);
    wStartNewLine = new Button(wFileName, SWT.CHECK);
    wStartNewLine.setToolTipText(
        BaseMessages.getString(PKG, "SQLFileOutputDialog.StartNewLine.Label"));
    PropsUi.setLook(wStartNewLine);
    FormData fdStartNewLine = new FormData();
    fdStartNewLine.left = new FormAttachment(middle, 0);
    fdStartNewLine.top = new FormAttachment(wlStartNewLine, 0, SWT.CENTER);
    fdStartNewLine.right = new FormAttachment(100, 0);
    wStartNewLine.setLayoutData(fdStartNewLine);
    SelectionAdapter lsSelSMod =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wStartNewLine.addSelectionListener(lsSelSMod);

    // Filename line
    Label wlFilename = new Label(wFileName, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wStartNewLine, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    Button wbFilename = new Button(wFileName, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wStartNewLine, 0);
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar(variables, wFileName, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(wStartNewLine, margin);
    fdFilename.right = new FormAttachment(wbFilename, -margin);
    wFilename.setLayoutData(fdFilename);

    // Create Parent Folder
    Label wlCreateParentFolder = new Label(wFileName, SWT.RIGHT);
    wlCreateParentFolder.setText(
        BaseMessages.getString(PKG, "SQLFileOutputDialog.CreateParentFolder.Label"));
    PropsUi.setLook(wlCreateParentFolder);
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment(0, 0);
    fdlCreateParentFolder.top = new FormAttachment(wFilename, margin);
    fdlCreateParentFolder.right = new FormAttachment(middle, -margin);
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
    wCreateParentFolder = new Button(wFileName, SWT.CHECK);
    wCreateParentFolder.setToolTipText(
        BaseMessages.getString(PKG, "SQLFileOutputDialog.CreateParentFolder.Tooltip"));
    PropsUi.setLook(wCreateParentFolder);
    FormData fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment(middle, 0);
    fdCreateParentFolder.top = new FormAttachment(wlCreateParentFolder, 0, SWT.CENTER);
    fdCreateParentFolder.right = new FormAttachment(100, 0);
    wCreateParentFolder.setLayoutData(fdCreateParentFolder);
    wCreateParentFolder.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Open new File at Init
    Label wlDoNotOpenNewFileInit = new Label(wFileName, SWT.RIGHT);
    wlDoNotOpenNewFileInit.setText(
        BaseMessages.getString(PKG, "SQLFileOutputDialog.DoNotOpenNewFileInit.Label"));
    PropsUi.setLook(wlDoNotOpenNewFileInit);
    FormData fdlDoNotOpenNewFileInit = new FormData();
    fdlDoNotOpenNewFileInit.left = new FormAttachment(0, 0);
    fdlDoNotOpenNewFileInit.top = new FormAttachment(wCreateParentFolder, margin);
    fdlDoNotOpenNewFileInit.right = new FormAttachment(middle, -margin);
    wlDoNotOpenNewFileInit.setLayoutData(fdlDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit = new Button(wFileName, SWT.CHECK);
    wDoNotOpenNewFileInit.setToolTipText(
        BaseMessages.getString(PKG, "SQLFileOutputDialog.DoNotOpenNewFileInit.Tooltip"));
    PropsUi.setLook(wDoNotOpenNewFileInit);
    FormData fdDoNotOpenNewFileInit = new FormData();
    fdDoNotOpenNewFileInit.left = new FormAttachment(middle, 0);
    fdDoNotOpenNewFileInit.top = new FormAttachment(wlDoNotOpenNewFileInit, 0, SWT.CENTER);
    fdDoNotOpenNewFileInit.right = new FormAttachment(100, 0);
    wDoNotOpenNewFileInit.setLayoutData(fdDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Extension line
    Label wlExtension = new Label(wFileName, SWT.RIGHT);
    wlExtension.setText(BaseMessages.getString(PKG, "System.Label.Extension"));
    PropsUi.setLook(wlExtension);
    FormData fdlExtension = new FormData();
    fdlExtension.left = new FormAttachment(0, 0);
    fdlExtension.top = new FormAttachment(wDoNotOpenNewFileInit, margin);
    fdlExtension.right = new FormAttachment(middle, -margin);
    wlExtension.setLayoutData(fdlExtension);

    wExtension = new TextVar(variables, wFileName, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wExtension);
    wExtension.addModifyListener(lsMod);
    FormData fdExtension = new FormData();
    fdExtension.left = new FormAttachment(middle, 0);
    fdExtension.top = new FormAttachment(wDoNotOpenNewFileInit, margin);
    fdExtension.right = new FormAttachment(100, -margin);
    wExtension.setLayoutData(fdExtension);

    // Create multi-part file?
    Label wlAddTransformNr = new Label(wFileName, SWT.RIGHT);
    wlAddTransformNr.setText(
        BaseMessages.getString(PKG, "SQLFileOutputDialog.AddTransformnr.Label"));
    PropsUi.setLook(wlAddTransformNr);
    FormData fdlAddTransformNr = new FormData();
    fdlAddTransformNr.left = new FormAttachment(0, 0);
    fdlAddTransformNr.top = new FormAttachment(wExtension, margin);
    fdlAddTransformNr.right = new FormAttachment(middle, -margin);
    wlAddTransformNr.setLayoutData(fdlAddTransformNr);
    wAddTransformNr = new Button(wFileName, SWT.CHECK);
    PropsUi.setLook(wAddTransformNr);
    FormData fdAddTransformNr = new FormData();
    fdAddTransformNr.left = new FormAttachment(middle, 0);
    fdAddTransformNr.top = new FormAttachment(wlAddTransformNr, 0, SWT.CENTER);
    fdAddTransformNr.right = new FormAttachment(100, 0);
    wAddTransformNr.setLayoutData(fdAddTransformNr);
    wAddTransformNr.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Create multi-part file?
    Label wlAddDate = new Label(wFileName, SWT.RIGHT);
    wlAddDate.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.AddDate.Label"));
    PropsUi.setLook(wlAddDate);
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment(0, 0);
    fdlAddDate.top = new FormAttachment(wAddTransformNr, margin);
    fdlAddDate.right = new FormAttachment(middle, -margin);
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button(wFileName, SWT.CHECK);
    PropsUi.setLook(wAddDate);
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment(middle, 0);
    fdAddDate.top = new FormAttachment(wlAddDate, 0, SWT.CENTER);
    fdAddDate.right = new FormAttachment(100, 0);
    wAddDate.setLayoutData(fdAddDate);
    wAddDate.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });
    // Create multi-part file?
    Label wlAddTime = new Label(wFileName, SWT.RIGHT);
    wlAddTime.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.AddTime.Label"));
    PropsUi.setLook(wlAddTime);
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment(0, 0);
    fdlAddTime.top = new FormAttachment(wAddDate, margin);
    fdlAddTime.right = new FormAttachment(middle, -margin);
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button(wFileName, SWT.CHECK);
    PropsUi.setLook(wAddTime);
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment(middle, 0);
    fdAddTime.top = new FormAttachment(wlAddTime, 0, SWT.CENTER);
    fdAddTime.right = new FormAttachment(100, 0);
    wAddTime.setLayoutData(fdAddTime);
    wAddTime.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Append to end of file?
    Label wlAppend = new Label(wFileName, SWT.RIGHT);
    wlAppend.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.Append.Label"));
    PropsUi.setLook(wlAppend);
    FormData fdlAppend = new FormData();
    fdlAppend.left = new FormAttachment(0, 0);
    fdlAppend.top = new FormAttachment(wAddTime, margin);
    fdlAppend.right = new FormAttachment(middle, -margin);
    wlAppend.setLayoutData(fdlAppend);
    wAppend = new Button(wFileName, SWT.CHECK);
    wAppend.setToolTipText(BaseMessages.getString(PKG, "SQLFileOutputDialog.Append.Tooltip"));
    PropsUi.setLook(wAppend);
    FormData fdAppend = new FormData();
    fdAppend.left = new FormAttachment(middle, 0);
    fdAppend.top = new FormAttachment(wlAppend, 0, SWT.CENTER);
    fdAppend.right = new FormAttachment(100, 0);
    wAppend.setLayoutData(fdAppend);
    wAppend.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    Label wlSplitEvery = new Label(wFileName, SWT.RIGHT);
    wlSplitEvery.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.SplitEvery.Label"));
    PropsUi.setLook(wlSplitEvery);
    FormData fdlSplitEvery = new FormData();
    fdlSplitEvery.left = new FormAttachment(0, 0);
    fdlSplitEvery.top = new FormAttachment(wAppend, margin);
    fdlSplitEvery.right = new FormAttachment(middle, -margin);
    wlSplitEvery.setLayoutData(fdlSplitEvery);

    wSplitEvery = new Text(wFileName, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSplitEvery);
    wSplitEvery.addModifyListener(lsMod);
    FormData fdSplitEvery = new FormData();
    fdSplitEvery.left = new FormAttachment(middle, 0);
    fdSplitEvery.top = new FormAttachment(wAppend, margin);
    fdSplitEvery.right = new FormAttachment(100, 0);
    wSplitEvery.setLayoutData(fdSplitEvery);

    Button wbShowFiles = new Button(wFileName, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.ShowFiles.Button"));
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment(middle, 0);
    fdbShowFiles.top = new FormAttachment(wSplitEvery, margin);
    wbShowFiles.setLayoutData(fdbShowFiles);
    wbShowFiles.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            SQLFileOutputMeta tfoi = new SQLFileOutputMeta();
            getInfo(tfoi);
            String[] files = tfoi.getFiles(variables, variables.resolve(wFilename.getText()));
            if (files != null && files.length > 0) {
              EnterSelectionDialog esd =
                  new EnterSelectionDialog(
                      shell,
                      files,
                      BaseMessages.getString(
                          PKG, "SQLFileOutputDialog.SelectOutputFiles.DialogTitle"),
                      BaseMessages.getString(
                          PKG, "SQLFileOutputDialog.SelectOutputFiles.DialogMessage"));
              esd.setViewOnly();
              esd.open();
            } else {
              MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
              mb.setMessage(
                  BaseMessages.getString(PKG, "SQLFileOutputDialog.NoFilesFound.DialogMessage"));
              mb.setText(BaseMessages.getString(PKG, "System.DialogTitle.Error"));
              mb.open();
            }
          }
        });

    // Add File to the result files name
    Label wlAddToResult = new Label(wFileName, SWT.RIGHT);
    wlAddToResult.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.AddFileToResult.Label"));
    PropsUi.setLook(wlAddToResult);
    FormData fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment(0, 0);
    fdlAddToResult.top = new FormAttachment(wbShowFiles, margin);
    fdlAddToResult.right = new FormAttachment(middle, -margin);
    wlAddToResult.setLayoutData(fdlAddToResult);
    wAddToResult = new Button(wFileName, SWT.CHECK);
    wAddToResult.setToolTipText(
        BaseMessages.getString(PKG, "SQLFileOutputDialog.AddFileToResult.Tooltip"));
    PropsUi.setLook(wAddToResult);
    FormData fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment(middle, 0);
    fdAddToResult.top = new FormAttachment(wlAddToResult, 0, SWT.CENTER);
    fdAddToResult.right = new FormAttachment(100, 0);
    wAddToResult.setLayoutData(fdAddToResult);
    SelectionAdapter lsSelR =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wAddToResult.addSelectionListener(lsSelR);

    FormData fdFileName = new FormData();
    fdFileName.left = new FormAttachment(0, margin);
    fdFileName.top = new FormAttachment(wGConnection, margin);
    fdFileName.right = new FormAttachment(100, -margin);
    wFileName.setLayoutData(fdFileName);

    // ///////////////////////////////////////////////////////////
    // / END OF FileName GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    PropsUi.setLook(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setFont(GuiResource.getInstance().getFontDefault());
    wContentTab.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.ContentTab.TabTitle"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wContentComp);
    wContentComp.setLayout(contentLayout);

    // Prepare a list of possible formats...
    String[] dats = Const.getDateFormats();

    // format
    Label wlFormat = new Label(wContentComp, SWT.RIGHT);
    wlFormat.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.DateFormat.Label"));
    PropsUi.setLook(wlFormat);
    FormData fdlFormat = new FormData();
    fdlFormat.left = new FormAttachment(0, 0);
    fdlFormat.top = new FormAttachment(0, margin);
    fdlFormat.right = new FormAttachment(middle, -margin);
    wlFormat.setLayoutData(fdlFormat);
    wFormat = new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY);
    wFormat.setEditable(true);
    PropsUi.setLook(wFormat);
    wFormat.addModifyListener(lsMod);
    FormData fdFormat = new FormData();
    fdFormat.left = new FormAttachment(middle, 0);
    fdFormat.top = new FormAttachment(0, margin);
    fdFormat.right = new FormAttachment(100, 0);
    wFormat.setLayoutData(fdFormat);

    for (String dat : dats) {
      wFormat.add(dat);
    }

    // Encoding
    Label wlEncoding = new Label(wContentComp, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.Encoding.Label"));
    PropsUi.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment(0, 0);
    fdlEncoding.top = new FormAttachment(wFormat, margin);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY);
    wEncoding.setEditable(true);
    PropsUi.setLook(wEncoding);
    wEncoding.addModifyListener(lsMod);
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment(middle, 0);
    fdEncoding.top = new FormAttachment(wFormat, margin);
    fdEncoding.right = new FormAttachment(100, 0);
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(org.eclipse.swt.events.FocusEvent e) {
            // Do Nothing
          }

          @Override
          public void focusGained(org.eclipse.swt.events.FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            setEncodings();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment(0, 0);
    fdContentComp.top = new FormAttachment(0, 0);
    fdContentComp.right = new FormAttachment(100, 0);
    fdContentComp.bottom = new FormAttachment(100, 0);
    wContentComp.setLayoutData(wContentComp);

    wContentComp.layout();
    wContentTab.setControl(wContentComp);

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners
    wbTable.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            getTableName();
          }
        });
    wbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                true,
                shell,
                wFilename,
                variables,
                new String[] {"*.sql", "*"},
                new String[] {"SQL File", BaseMessages.getString(PKG, "System.FileType.AllFiles")},
                true));

    wTabFolder.setSelection(0);

    getData();
    activateTruncate();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void activateTruncate() {

    wlTruncate.setEnabled(!wAddCreate.getSelection());
    wTruncate.setEnabled(!wAddCreate.getSelection());
    if (wAddCreate.getSelection()) {
      wTruncate.setSelection(false);
    }
  }

  private void setEncodings() {
    // Encoding of the text file:
    if (!gotEncodings) {
      gotEncodings = true;

      wEncoding.removeAll();
      List<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
      for (Charset charSet : values) {
        wEncoding.add(charSet.displayName());
      }

      // Now select the default!
      String defEncoding = Const.getEnvironmentVariable("file.encoding", "UTF-8");
      int idx = Const.indexOfString(defEncoding, wEncoding.getItems());
      if (idx >= 0) {
        wEncoding.select(idx);
      }
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getSchemaName() != null) {
      wSchema.setText(input.getSchemaName());
    }
    if (input.getTableName() != null) {
      wTable.setText(input.getTableName());
    }
    if (input.getConnection() != null) {
      wConnection.setText(input.getConnection());
    }

    if (input.getFile().getFileName() != null) {
      wFilename.setText(input.getFile().getFileName());
    }
    wCreateParentFolder.setSelection(input.getFile().isCreateParentFolder());
    if (input.getFile().getExtension() != null) {
      wExtension.setText(input.getFile().getExtension());
    }

    if (input.getDateFormat() != null) {
      wFormat.setText(input.getDateFormat());
    }

    wSplitEvery.setText("" + input.getFile().getSplitEvery());
    wAddDate.setSelection(input.getFile().isDateInFilename());
    wAddTime.setSelection(input.getFile().isTimeInFilename());
    wAppend.setSelection(input.getFile().isFileAppended());
    wAddTransformNr.setSelection(input.getFile().isTransformNrInFilename());

    wTruncate.setSelection(input.isTruncateTable());
    wAddCreate.setSelection(input.isCreateTable());

    if (input.getEncoding() != null) {
      wEncoding.setText(input.getEncoding());
    }
    wAddToResult.setSelection(input.isAddToResult());
    wStartNewLine.setSelection(input.isStartNewLine());
    wDoNotOpenNewFileInit.setSelection(input.getFile().isDoNotOpenNewFileInit());
  }

  private void cancel() {
    transformName = null;
    input.setChanged(backupChanged);
    dispose();
  }

  private void getInfo(SQLFileOutputMeta info) {
    info.setSchemaName(wSchema.getText());
    info.setTableName(wTable.getText());
    info.setConnection(wConnection.getText());
    info.setTruncateTable(wTruncate.getSelection());
    info.getFile().setCreateParentFolder(wCreateParentFolder.getSelection());

    info.setCreateTable(wAddCreate.getSelection());

    info.getFile().setFileName(wFilename.getText());
    info.getFile().setExtension(wExtension.getText());
    info.setDateFormat(wFormat.getText());
    info.getFile().setSplitEvery(Const.toInt(wSplitEvery.getText(), 0));
    info.getFile().setFileAppended(wAppend.getSelection());
    info.getFile().setTransformNrInFilename(wAddTransformNr.getSelection());
    info.getFile().setDateInFilename(wAddDate.getSelection());
    info.getFile().setTimeInFilename(wAddTime.getSelection());

    info.setEncoding(wEncoding.getText());
    info.setAddToResult(wAddToResult.getSelection());
    info.setStartNewLine(wStartNewLine.getSelection());
    info.getFile().setDoNotOpenNewFileInit(wDoNotOpenNewFileInit.getSelection());
  }

  private void ok() {
    transformName = wTransformName.getText(); // return value

    getInfo(input);

    if (input.getConnection() == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "SQLFileOutputDialog.ConnectionError.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, CONST_SYSTEM_DIALOG_ERROR_TITLE));
      mb.open();
      return;
    }

    dispose();
  }

  private void getTableName() {
    String connectionName = wConnection.getText();
    if (StringUtils.isEmpty(connectionName)) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
    if (databaseMeta != null) {

      logDebug(
          BaseMessages.getString(
              PKG, "SQLFileOutputDialog.Log.LookingAtConnection", databaseMeta.toString()));

      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases());
      std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
      if (std.open()) {
        wSchema.setText(Const.NVL(std.getSchemaName(), ""));
        wTable.setText(Const.NVL(std.getTableName(), ""));
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "SQLFileOutputDialog.ConnectionError2.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, CONST_SYSTEM_DIALOG_ERROR_TITLE));
      mb.open();
    }
  }

  // Generate code for create table...
  // Conversions done by Database
  //
  private void sql() {
    try {
      SQLFileOutputMeta info = new SQLFileOutputMeta();
      getInfo(info);
      IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);

      TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
      DatabaseMeta databaseMeta = pipelineMeta.findDatabase(transformMeta.getName(), variables);

      SqlStatement sql =
          info.getSqlStatements(variables, pipelineMeta, transformMeta, prev, metadataProvider);
      if (!sql.hasError()) {
        if (sql.hasSql()) {
          SqlEditor sqledit =
              new SqlEditor(
                  shell, SWT.NONE, variables, databaseMeta, DbCache.getInstance(), sql.getSql());
          sqledit.open();
        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
          mb.setMessage(BaseMessages.getString(PKG, "SQLFileOutputDialog.NoSQL.DialogMessage"));
          mb.setText(BaseMessages.getString(PKG, "SQLFileOutputDialog.NoSQL.DialogTitle"));
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(sql.getError());
        mb.setText(BaseMessages.getString(PKG, CONST_SYSTEM_DIALOG_ERROR_TITLE));
        mb.open();
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SQLFileOutputDialog.BuildSQLError.DialogTitle"),
          BaseMessages.getString(PKG, "SQLFileOutputDialog.BuildSQLError.DialogMessage"),
          ke);
    }
  }
}
