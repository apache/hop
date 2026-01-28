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

package org.apache.hop.pipeline.transforms.getfilesrowcount;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transforms.getfilesrowcount.GetFilesRowsCountMeta.GCFile;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
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
import org.eclipse.swt.widgets.TableItem;

public class GetFilesRowsCountDialog extends BaseTransformDialog {
  private static final Class<?> PKG = GetFilesRowsCountMeta.class;

  private Label wlExcludeFileMask;
  private TextVar wExcludeFileMask;

  private Label wlFilename;
  private Button wbbFilename; // Browse: add file or directory

  private Button wbdFilename; // Delete
  private Button wbeFilename; // Edit
  private Button wbaFilename; // Add or change
  private TextVar wFilename;

  private Label wlFilenameList;
  private TableView wFilenameList;

  private Label wlFileMask;
  private TextVar wFileMask;

  private Button wbShowFiles;

  private Button wInclFilesCount;

  private Label wlInclFilesCountField;
  private TextVar wInclFilesCountField;

  private TextVar wRowsCountField;

  private Label wlRowSeparator;
  private TextVar wRowSeparator;

  private CCombo wRowSeparatorFormat;

  private final GetFilesRowsCountMeta input;

  private Button wFileField;
  private Button wAddResult;
  private Button wSmartCount;

  private Label wlFilenameField;
  private CCombo wFilenameField;

  public GetFilesRowsCountDialog(
      Shell parent,
      IVariables variables,
      GetFilesRowsCountMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "GetFilesRowsCountDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).preview(e -> preview()).cancel(e -> cancel()).build();

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF FILE TAB ///
    // ////////////////////////
    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setFont(GuiResource.getInstance().getFontDefault());
    wFileTab.setText(BaseMessages.getString(PKG, "GetFilesRowsCountDialog.File.Tab"));

    Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFileComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout(fileLayout);

    // ///////////////////////////////
    // START OF Origin files GROUP //
    // ///////////////////////////////

    Group wOriginFiles = new Group(wFileComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wOriginFiles);
    wOriginFiles.setText(BaseMessages.getString(PKG, "GetFilesRowsCountDialog.wOriginFiles.Label"));

    FormLayout originFilesgroupLayout = new FormLayout();
    originFilesgroupLayout.marginWidth = 10;
    originFilesgroupLayout.marginHeight = 10;
    wOriginFiles.setLayout(originFilesgroupLayout);

    // Is Filename defined in a Field
    Label wlFileField = new Label(wOriginFiles, SWT.RIGHT);
    wlFileField.setText(BaseMessages.getString(PKG, "GetFilesRowsCountDialog.FileField.Label"));
    PropsUi.setLook(wlFileField);
    FormData fdlFileField = new FormData();
    fdlFileField.left = new FormAttachment(0, -margin);
    fdlFileField.top = new FormAttachment(0, margin);
    fdlFileField.right = new FormAttachment(middle, -margin);
    wlFileField.setLayoutData(fdlFileField);

    wFileField = new Button(wOriginFiles, SWT.CHECK);
    PropsUi.setLook(wFileField);
    wFileField.setToolTipText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.FileField.Tooltip"));
    FormData fdFileField = new FormData();
    fdFileField.left = new FormAttachment(middle, -margin);
    fdFileField.top = new FormAttachment(wlFileField, 0, SWT.CENTER);
    wFileField.setLayoutData(fdFileField);
    wFileField.addListener(SWT.Selection, e -> activateFileField());

    // Filename field
    wlFilenameField = new Label(wOriginFiles, SWT.RIGHT);
    wlFilenameField.setText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.FilenameField.Label"));
    PropsUi.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, -margin);
    fdlFilenameField.top = new FormAttachment(wFileField, margin);
    fdlFilenameField.right = new FormAttachment(middle, -margin);
    wlFilenameField.setLayoutData(fdlFilenameField);

    wFilenameField = new CCombo(wOriginFiles, SWT.BORDER | SWT.READ_ONLY);
    wFilenameField.setEditable(true);
    PropsUi.setLook(wFilenameField);
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment(middle, -margin);
    fdFilenameField.top = new FormAttachment(wFileField, margin);
    fdFilenameField.right = new FormAttachment(100, -margin);
    wFilenameField.setLayoutData(fdFilenameField);
    wFilenameField.addListener(
        SWT.FocusIn,
        e -> {
          Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
          shell.setCursor(busy);
          setFileField();
          shell.setCursor(null);
          busy.dispose();
        });

    FormData fdOriginFiles = new FormData();
    fdOriginFiles.left = new FormAttachment(0, margin);
    fdOriginFiles.top = new FormAttachment(wFilenameList, margin);
    fdOriginFiles.right = new FormAttachment(100, -margin);
    wOriginFiles.setLayoutData(fdOriginFiles);

    // ///////////////////////////////////////////////////////////
    // / END OF Origin files GROUP
    // ///////////////////////////////////////////////////////////

    // Filename line
    wlFilename = new Label(wFileComp, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "GetFilesRowsCountDialog.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wOriginFiles, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    wbbFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbFilename);
    wbbFilename.setText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.FilenameBrowse.Button"));
    wbbFilename.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wOriginFiles, margin);
    wbbFilename.setLayoutData(fdbFilename);

    wbaFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbaFilename);
    wbaFilename.setText(BaseMessages.getString(PKG, "GetFilesRowsCountDialog.FilenameAdd.Button"));
    wbaFilename.setToolTipText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.FilenameAdd.Tooltip"));
    FormData fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment(wbbFilename, -margin);
    fdbaFilename.top = new FormAttachment(wOriginFiles, margin);
    wbaFilename.setLayoutData(fdbaFilename);

    wFilename = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(wbaFilename, -margin);
    fdFilename.top = new FormAttachment(wOriginFiles, margin);
    wFilename.setLayoutData(fdFilename);

    wlFileMask = new Label(wFileComp, SWT.RIGHT);
    wlFileMask.setText(BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RegExp.Label"));
    PropsUi.setLook(wlFileMask);
    FormData fdlFileMask = new FormData();
    fdlFileMask.left = new FormAttachment(0, 0);
    fdlFileMask.top = new FormAttachment(wFilename, margin);
    fdlFileMask.right = new FormAttachment(middle, -margin);
    wlFileMask.setLayoutData(fdlFileMask);
    wFileMask = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFileMask);
    FormData fdFileMask = new FormData();
    fdFileMask.left = new FormAttachment(middle, 0);
    fdFileMask.top = new FormAttachment(wFilename, margin);
    fdFileMask.right = new FormAttachment(wFilename, 0, SWT.RIGHT);
    wFileMask.setLayoutData(fdFileMask);

    wlExcludeFileMask = new Label(wFileComp, SWT.RIGHT);
    wlExcludeFileMask.setText(
        BaseMessages.getString(PKG, "GetFilesRowsDialog.ExcludeFileMask.Label"));
    PropsUi.setLook(wlExcludeFileMask);
    FormData fdlExcludeFileMask = new FormData();
    fdlExcludeFileMask.left = new FormAttachment(0, 0);
    fdlExcludeFileMask.top = new FormAttachment(wFileMask, margin);
    fdlExcludeFileMask.right = new FormAttachment(middle, -margin);
    wlExcludeFileMask.setLayoutData(fdlExcludeFileMask);
    wExcludeFileMask = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wExcludeFileMask);
    FormData fdExcludeFileMask = new FormData();
    fdExcludeFileMask.left = new FormAttachment(middle, 0);
    fdExcludeFileMask.top = new FormAttachment(wFileMask, margin);
    fdExcludeFileMask.right = new FormAttachment(wFilename, 0, SWT.RIGHT);
    wExcludeFileMask.setLayoutData(fdExcludeFileMask);

    // Filename list line
    wlFilenameList = new Label(wFileComp, SWT.RIGHT);
    wlFilenameList.setText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.FilenameList.Label"));
    PropsUi.setLook(wlFilenameList);
    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment(0, 0);
    fdlFilenameList.top = new FormAttachment(wExcludeFileMask, margin);
    fdlFilenameList.right = new FormAttachment(middle, -margin);
    wlFilenameList.setLayoutData(fdlFilenameList);

    // Buttons to the right of the screen...
    wbdFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbdFilename);
    wbdFilename.setText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.FilenameRemove.Button"));
    wbdFilename.setToolTipText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.FilenameRemove.Tooltip"));
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment(100, 0);
    fdbdFilename.top = new FormAttachment(wlFilenameList, 0, SWT.TOP);
    wbdFilename.setLayoutData(fdbdFilename);

    wbeFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbeFilename);
    wbeFilename.setText(BaseMessages.getString(PKG, "GetFilesRowsCountDialog.FilenameEdit.Button"));
    wbeFilename.setToolTipText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.FilenameEdit.Tooltip"));
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment(100, 0);
    fdbeFilename.left = new FormAttachment(wbdFilename, 0, SWT.LEFT);
    fdbeFilename.top = new FormAttachment(wbdFilename, margin);
    wbeFilename.setLayoutData(fdbeFilename);

    wbShowFiles = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "GetFilesRowsCountDialog.ShowFiles.Button"));
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment(middle, 0);
    fdbShowFiles.bottom = new FormAttachment(100, 0);
    wbShowFiles.setLayoutData(fdbShowFiles);

    ColumnInfo[] colinfo = new ColumnInfo[5];
    colinfo[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GetFilesRowsCountDialog.Files.Filename.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinfo[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GetFilesRowsCountDialog.Files.Wildcard.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinfo[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GetFilesRowsDialog.Files.ExcludeWildcard.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    colinfo[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GetFilesRowsCountDialog.Required.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            GetFilesRowsCountMeta.RequiredFilesDesc);
    colinfo[4] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GetFilesRowsCountDialog.IncludeSubDirs.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            GetFilesRowsCountMeta.RequiredFilesDesc);

    colinfo[0].setUsingVariables(true);
    colinfo[1].setUsingVariables(true);
    colinfo[1].setToolTip(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.Files.Wildcard.Tooltip"));
    colinfo[2].setUsingVariables(true);
    colinfo[2].setToolTip(
        BaseMessages.getString(PKG, "GetFilesRowsDialog.Files.ExcludeWildcard.Tooltip"));

    wFilenameList =
        new TableView(
            variables,
            wFileComp,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
            colinfo,
            2,
            null,
            props);
    PropsUi.setLook(wFilenameList);

    FormData fdFilenameList = new FormData();
    fdFilenameList.left = new FormAttachment(middle, 0);
    fdFilenameList.right = new FormAttachment(wbdFilename, -margin);
    fdFilenameList.top = new FormAttachment(wExcludeFileMask, margin);
    fdFilenameList.bottom = new FormAttachment(wbShowFiles, -margin);
    wFilenameList.setLayoutData(fdFilenameList);

    FormData fdFileComp = new FormData();
    fdFileComp.left = new FormAttachment(0, 0);
    fdFileComp.top = new FormAttachment(0, 0);
    fdFileComp.right = new FormAttachment(100, 0);
    fdFileComp.bottom = new FormAttachment(100, 0);
    wFileComp.setLayoutData(fdFileComp);

    wFileComp.layout();
    wFileTab.setControl(wFileComp);

    // ///////////////////////////////////////////////////////////
    // / END OF FILE TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setFont(GuiResource.getInstance().getFontDefault());
    wContentTab.setText(BaseMessages.getString(PKG, "GetFilesRowsCountDialog.Content.Tab"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wContentComp);
    wContentComp.setLayout(contentLayout);

    // /////////////////////////////////
    // START OF Files Count Field GROUP
    // /////////////////////////////////

    Group wFilesCountFieldGroup = new Group(wContentComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wFilesCountFieldGroup);
    wFilesCountFieldGroup.setText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.Group.CountFilesFieldGroup.Label"));

    FormLayout countfilesfieldgroupLayout = new FormLayout();
    countfilesfieldgroupLayout.marginWidth = 10;
    countfilesfieldgroupLayout.marginHeight = 10;
    wFilesCountFieldGroup.setLayout(countfilesfieldgroupLayout);

    Label wlRowsCountField = new Label(wFilesCountFieldGroup, SWT.RIGHT);
    wlRowsCountField.setText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowsCountField.Label"));
    PropsUi.setLook(wlRowsCountField);
    FormData fdlRowsCountField = new FormData();
    fdlRowsCountField.left = new FormAttachment(wInclFilesCount, margin);
    fdlRowsCountField.top = new FormAttachment(0, margin);
    fdlRowsCountField.right = new FormAttachment(middle, -margin);
    wlRowsCountField.setLayoutData(fdlRowsCountField);
    wRowsCountField =
        new TextVar(variables, wFilesCountFieldGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRowsCountField);
    wRowsCountField.setToolTipText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowsCountField.Tooltip"));
    FormData fdRowsCountField = new FormData();
    fdRowsCountField.left = new FormAttachment(wlRowsCountField, margin);
    fdRowsCountField.top = new FormAttachment(0, margin);
    fdRowsCountField.right = new FormAttachment(100, 0);
    wRowsCountField.setLayoutData(fdRowsCountField);

    FormData fdFilesCountFieldGroup = new FormData();
    fdFilesCountFieldGroup.left = new FormAttachment(0, margin);
    fdFilesCountFieldGroup.top = new FormAttachment(0, margin);
    fdFilesCountFieldGroup.right = new FormAttachment(100, -margin);
    wFilesCountFieldGroup.setLayoutData(fdFilesCountFieldGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF ADDITIONNAL FIELDS GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Row separator GROUP
    // /////////////////////////////////

    Group wRowSeparatorGroup = new Group(wContentComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wRowSeparatorGroup);
    wRowSeparatorGroup.setText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.Group.RowSeparator.Label"));

    FormLayout rowseparatorgroupLayout = new FormLayout();
    rowseparatorgroupLayout.marginWidth = 10;
    rowseparatorgroupLayout.marginHeight = 10;
    wRowSeparatorGroup.setLayout(rowseparatorgroupLayout);

    Label wlRowSeparatorFormat = new Label(wRowSeparatorGroup, SWT.RIGHT);
    wlRowSeparatorFormat.setText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.Label"));
    PropsUi.setLook(wlRowSeparatorFormat);
    FormData fdlRowSeparatorFormat = new FormData();
    fdlRowSeparatorFormat.left = new FormAttachment(0, 0);
    fdlRowSeparatorFormat.top = new FormAttachment(wFilesCountFieldGroup, margin);
    fdlRowSeparatorFormat.right = new FormAttachment(middle, -margin);
    wlRowSeparatorFormat.setLayoutData(fdlRowSeparatorFormat);
    wRowSeparatorFormat = new CCombo(wRowSeparatorGroup, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wRowSeparatorFormat);
    wRowSeparatorFormat.add(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.CR.Label"));
    wRowSeparatorFormat.add(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.LF.Label"));
    wRowSeparatorFormat.add(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.CRLF.Label"));
    wRowSeparatorFormat.add(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.TAB.Label"));
    wRowSeparatorFormat.add(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.CUSTOM.Label"));
    wRowSeparatorFormat.select(0);
    FormData fdRowSeparatorFormat = new FormData();
    fdRowSeparatorFormat.left = new FormAttachment(middle, 0);
    fdRowSeparatorFormat.top = new FormAttachment(wFilesCountFieldGroup, margin);
    fdRowSeparatorFormat.right = new FormAttachment(100, 0);
    wRowSeparatorFormat.setLayoutData(fdRowSeparatorFormat);

    wRowSeparatorFormat.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            activeRowSeparator();
          }
        });

    wlRowSeparator = new Label(wRowSeparatorGroup, SWT.RIGHT);
    wlRowSeparator.setText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowSeparator.Label"));
    PropsUi.setLook(wlRowSeparator);
    FormData fdlRowSeparator = new FormData();
    fdlRowSeparator.left = new FormAttachment(wInclFilesCount, margin);
    fdlRowSeparator.top = new FormAttachment(wRowSeparatorFormat, margin);
    fdlRowSeparator.right = new FormAttachment(middle, -margin);
    wlRowSeparator.setLayoutData(fdlRowSeparator);
    wRowSeparator = new TextVar(variables, wRowSeparatorGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRowSeparator);
    wRowSeparator.setToolTipText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowSeparator.Tooltip"));
    FormData fdRowSeparator = new FormData();
    fdRowSeparator.left = new FormAttachment(wlRowSeparator, margin);
    fdRowSeparator.top = new FormAttachment(wRowSeparatorFormat, margin);
    fdRowSeparator.right = new FormAttachment(100, 0);
    wRowSeparator.setLayoutData(fdRowSeparator);

    Label wlSmartCount = new Label(wRowSeparatorGroup, SWT.RIGHT);
    wlSmartCount.setText(BaseMessages.getString(PKG, "GetFilesRowsCountDialog.SmartCount.Label"));
    PropsUi.setLook(wlSmartCount);
    FormData fdlSmartCount = new FormData();
    fdlSmartCount.left = new FormAttachment(0, 0);
    fdlSmartCount.top = new FormAttachment(wRowSeparator, margin);
    fdlSmartCount.right = new FormAttachment(middle, -margin);
    wlSmartCount.setLayoutData(fdlSmartCount);
    wSmartCount = new Button(wRowSeparatorGroup, SWT.CHECK);
    PropsUi.setLook(wSmartCount);
    wSmartCount.setToolTipText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.SmartCount.Tooltip"));
    FormData fdSmartCount = new FormData();
    fdSmartCount.left = new FormAttachment(middle, 0);
    fdSmartCount.top = new FormAttachment(wlSmartCount, 0, SWT.CENTER);
    wSmartCount.setLayoutData(fdSmartCount);

    FormData fdRowSeparatorGroup = new FormData();
    fdRowSeparatorGroup.left = new FormAttachment(0, margin);
    fdRowSeparatorGroup.top = new FormAttachment(wFilesCountFieldGroup, margin);
    fdRowSeparatorGroup.right = new FormAttachment(100, -margin);
    wRowSeparatorGroup.setLayoutData(fdRowSeparatorGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF ROW SEPARATOR GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Additional Fields GROUP
    // /////////////////////////////////

    Group wAdditionalGroup = new Group(wContentComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wAdditionalGroup);
    wAdditionalGroup.setText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.Group.AdditionalGroup.Label"));

    FormLayout additionalgroupLayout = new FormLayout();
    additionalgroupLayout.marginWidth = 10;
    additionalgroupLayout.marginHeight = 10;
    wAdditionalGroup.setLayout(additionalgroupLayout);

    Label wlInclFilesCount = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclFilesCount.setText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.InclCountFiles.Label"));
    PropsUi.setLook(wlInclFilesCount);
    FormData fdlInclFilesCount = new FormData();
    fdlInclFilesCount.left = new FormAttachment(0, 0);
    fdlInclFilesCount.top = new FormAttachment(wRowSeparatorGroup, margin);
    fdlInclFilesCount.right = new FormAttachment(middle, -margin);
    wlInclFilesCount.setLayoutData(fdlInclFilesCount);
    wInclFilesCount = new Button(wAdditionalGroup, SWT.CHECK);
    PropsUi.setLook(wInclFilesCount);
    wInclFilesCount.setToolTipText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.InclCountFiles.Tooltip"));
    FormData fdFilesCount = new FormData();
    fdFilesCount.left = new FormAttachment(middle, 0);
    fdFilesCount.top = new FormAttachment(wRowSeparatorGroup, margin);
    wInclFilesCount.setLayoutData(fdFilesCount);

    wlInclFilesCountField = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclFilesCountField.setText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.InclCountFilesField.Label"));
    PropsUi.setLook(wlInclFilesCountField);
    FormData fdlInclFilesCountField = new FormData();
    fdlInclFilesCountField.left = new FormAttachment(wInclFilesCount, margin);
    fdlInclFilesCountField.top = new FormAttachment(wRowSeparatorGroup, margin);
    wlInclFilesCountField.setLayoutData(fdlInclFilesCountField);
    wInclFilesCountField =
        new TextVar(variables, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInclFilesCountField);
    FormData fdInclFilesCountField = new FormData();
    fdInclFilesCountField.left = new FormAttachment(wlInclFilesCountField, margin);
    fdInclFilesCountField.top = new FormAttachment(wlInclFilesCountField, 0, SWT.CENTER);
    fdInclFilesCountField.right = new FormAttachment(100, 0);
    wInclFilesCountField.setLayoutData(fdInclFilesCountField);

    FormData fdAdditionalGroup = new FormData();
    fdAdditionalGroup.left = new FormAttachment(0, margin);
    fdAdditionalGroup.top = new FormAttachment(wRowSeparatorGroup, margin);
    fdAdditionalGroup.right = new FormAttachment(100, -margin);
    wAdditionalGroup.setLayoutData(fdAdditionalGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF ADDITIONNAL FIELDS GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF AddFileResult GROUP //
    // ///////////////////////////////

    Group wAddFileResult = new Group(wContentComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wAddFileResult);
    wAddFileResult.setText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.wAddFileResult.Label"));

    FormLayout addFileResultgroupLayout = new FormLayout();
    addFileResultgroupLayout.marginWidth = 10;
    addFileResultgroupLayout.marginHeight = 10;
    wAddFileResult.setLayout(addFileResultgroupLayout);

    Label wlAddResult = new Label(wAddFileResult, SWT.RIGHT);
    wlAddResult.setText(BaseMessages.getString(PKG, "GetFilesRowsCountDialog.AddResult.Label"));
    PropsUi.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment(0, 0);
    fdlAddResult.top = new FormAttachment(wAdditionalGroup, margin);
    fdlAddResult.right = new FormAttachment(middle, -margin);
    wlAddResult.setLayoutData(fdlAddResult);
    wAddResult = new Button(wAddFileResult, SWT.CHECK);
    PropsUi.setLook(wAddResult);
    wAddResult.setToolTipText(
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.AddResult.Tooltip"));
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment(middle, 0);
    fdAddResult.top = new FormAttachment(wlAddResult, 0, SWT.CENTER);
    wAddResult.setLayoutData(fdAddResult);

    FormData fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment(0, margin);
    fdAddFileResult.top = new FormAttachment(wAdditionalGroup, margin);
    fdAddFileResult.right = new FormAttachment(100, -margin);
    wAddFileResult.setLayoutData(fdAddFileResult);

    // ///////////////////////////////////////////////////////////
    // / END OF AddFileResult GROUP
    // ////////////////////////////////////

    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment(0, 0);
    fdContentComp.top = new FormAttachment(0, 0);
    fdContentComp.right = new FormAttachment(100, 0);
    fdContentComp.bottom = new FormAttachment(100, 0);
    wContentComp.setLayoutData(fdContentComp);

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

    // Add the file to the list of files...
    SelectionAdapter selA =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            wFilenameList.add(
                wFilename.getText(),
                wFileMask.getText(),
                wExcludeFileMask.getText(),
                GetFilesRowsCountMeta.RequiredFilesCode[0],
                GetFilesRowsCountMeta.RequiredFilesCode[0]);
            wFilename.setText("");
            wFileMask.setText("");
            wExcludeFileMask.setText("");
            wFilenameList.removeEmptyRows();
            wFilenameList.setRowNums();
            wFilenameList.optWidth(true);
          }
        };
    wbaFilename.addSelectionListener(selA);
    wFilename.addSelectionListener(selA);

    // Delete files from the list of files...
    wbdFilename.addListener(
        SWT.Selection,
        e -> {
          int[] idx = wFilenameList.getSelectionIndices();
          wFilenameList.remove(idx);
          wFilenameList.removeEmptyRows();
          wFilenameList.setRowNums();
        });

    // Edit the selected file & remove from the list...
    wbeFilename.addListener(
        SWT.Selection,
        e -> {
          int idx = wFilenameList.getSelectionIndex();
          if (idx >= 0) {
            String[] string = wFilenameList.getItem(idx);
            wFilename.setText(string[0]);
            wFileMask.setText(string[1]);
            wExcludeFileMask.setText(string[2]);
            wFilenameList.remove(idx);
          }
          wFilenameList.removeEmptyRows();
          wFilenameList.setRowNums();
        });

    // Show the files that are selected at this time...
    wbShowFiles.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            try {
              GetFilesRowsCountMeta tfii = new GetFilesRowsCountMeta();
              getInfo(tfii);
              FileInputList fileInputList = tfii.getFiles(variables);
              String[] files = fileInputList.getFileStrings();

              if (files.length > 0) {
                EnterSelectionDialog esd =
                    new EnterSelectionDialog(
                        shell,
                        files,
                        BaseMessages.getString(
                            PKG, "GetFilesRowsCountDialog.FilesReadSelection.DialogTitle"),
                        BaseMessages.getString(
                            PKG, "GetFilesRowsCountDialog.FilesReadSelection.DialogMessage"));
                esd.setViewOnly();
                esd.open();
              } else {
                MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
                mb.setMessage(
                    BaseMessages.getString(
                        PKG, "GetFilesRowsCountDialog.NoFileFound.DialogMessage"));
                mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
                mb.open();
              }
            } catch (HopException ex) {
              new ErrorDialog(
                  shell,
                  BaseMessages.getString(
                      PKG, "GetFilesRowsCountDialog.ErrorParsingData.DialogTitle"),
                  BaseMessages.getString(
                      PKG, "GetFilesRowsCountDialog.ErrorParsingData.DialogMessage"),
                  ex);
            }
          }
        });

    // Enable/disable the right fields to allow a row number to be added to each row...
    wInclFilesCount.addListener(SWT.Selection, e -> setIncludeRownum());

    // Whenever something changes, set the tooltip to the expanded version of the filename:
    wFilename.addModifyListener(e -> wFilename.setToolTipText(""));

    // Listen to the Browse... button
    wbbFilename.addListener(
        SWT.Selection,
        e -> {
          if (!Utils.isEmpty(wFileMask.getText())
              || !Utils.isEmpty(wExcludeFileMask.getText())) { // A mask: a directory!
            BaseDialog.presentDirectoryDialog(shell, wFileMask, variables);
          } else {
            BaseDialog.presentFileDialog(
                shell,
                wFilename,
                variables,
                new String[] {"*"},
                new String[] {BaseMessages.getString(PKG, "System.FileType.AllFiles")},
                true);
          }
        });

    wTabFolder.setSelection(0);

    getData(input);
    activateFileField();
    activeRowSeparator();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void activeRowSeparator() {
    wRowSeparator.setEnabled(wRowSeparatorFormat.getSelectionIndex() == 3);
    wlRowSeparator.setEnabled(wRowSeparatorFormat.getSelectionIndex() == 3);
  }

  public void setIncludeRownum() {
    wlInclFilesCountField.setEnabled(wInclFilesCount.getSelection());
    wInclFilesCountField.setEnabled(wInclFilesCount.getSelection());
  }

  private void activateFileField() {
    wlFilenameField.setEnabled(wFileField.getSelection());
    wFilenameField.setEnabled(wFileField.getSelection());

    wlFilename.setEnabled(!wFileField.getSelection());
    wbbFilename.setEnabled(!wFileField.getSelection());
    wbaFilename.setEnabled(!wFileField.getSelection());
    wFilename.setEnabled(!wFileField.getSelection());
    wlFileMask.setEnabled(!wFileField.getSelection());
    wFileMask.setEnabled(!wFileField.getSelection());
    wlExcludeFileMask.setEnabled(!wFileField.getSelection());
    wExcludeFileMask.setEnabled(!wFileField.getSelection());
    wlFilenameList.setEnabled(!wFileField.getSelection());
    wbdFilename.setEnabled(!wFileField.getSelection());
    wbeFilename.setEnabled(!wFileField.getSelection());
    wbShowFiles.setEnabled(!wFileField.getSelection());
    wlFilenameList.setEnabled(!wFileField.getSelection());
    wFilenameList.setEnabled(!wFileField.getSelection());
    wPreview.setEnabled(!wFileField.getSelection());
  }

  private void setFileField() {
    try {

      wFilenameField.removeAll();

      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        r.getFieldNames();

        for (int i = 0; i < r.getFieldNames().length; i++) {
          wFilenameField.add(r.getFieldNames()[i]);
        }
      }

    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "GetFilesRowsCountDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "GetFilesRowsCountDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  /**
   * Read the data from the GetFilesRowsCountMeta object and show it in this dialog.
   *
   * @param in The GetFilesRowsCountMeta object to obtain the data from.
   */
  public void getData(GetFilesRowsCountMeta in) {
    for (int i = 0; i < in.getFiles().size(); i++) {
      GCFile file = in.getFiles().get(i);
      wFilenameList.add(
          file.getName(),
          file.getMask(),
          file.getExcludeMask(),
          file.isRequired() ? "Y" : "N",
          file.isIncludeSubFolder() ? "Y" : "N");
    }
    wFilenameList.removeEmptyRows();
    wFilenameList.setRowNums();
    wFilenameList.optWidth(true);

    wInclFilesCount.setSelection(in.isIncludeFilesCount());
    wSmartCount.setSelection(in.isSmartCount());

    wInclFilesCountField.setText(Const.NVL(in.getFilesCountFieldName(), ""));
    wRowsCountField.setText(Const.NVL(in.getRowsCountFieldName(), ""));

    wRowSeparatorFormat.select(in.getRowSeparatorFormat().getIndex());

    wRowSeparator.setText(Const.NVL(in.getRowSeparator(), ""));

    wAddResult.setSelection(in.isAddResultFilename());
    wFileField.setSelection(in.isFileFromField());
    wFilenameField.setText(Const.NVL(in.getOutputFilenameField(), ""));

    setIncludeRownum();
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    try {
      getInfo(input);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "GetFilesRowsCountDialog.ErrorParsingData.DialogTitle"),
          BaseMessages.getString(PKG, "GetFilesRowsCountDialog.ErrorParsingData.DialogMessage"),
          e);
    }
    input.setChanged();
    dispose();
  }

  private void getInfo(GetFilesRowsCountMeta in) throws HopException {
    transformName = wTransformName.getText(); // return value

    in.setIncludeFilesCount(wInclFilesCount.getSelection());
    in.setFilesCountFieldName(wInclFilesCountField.getText());
    in.setRowsCountFieldName(wRowsCountField.getText());

    in.setRowSeparatorFormat(
        GetFilesRowsCountMeta.SeparatorFormat.lookupDescription(wRowSeparatorFormat.getText()));

    in.getFiles().clear();
    for (TableItem item : wFilenameList.getNonEmptyItems()) {
      GCFile file = new GCFile();
      file.setName(item.getText(1));
      file.setMask(item.getText(2));
      file.setExcludeMask(item.getText(3));
      file.setRequired(Const.toBoolean(item.getText(4)));
      file.setIncludeSubFolder(Const.toBoolean(item.getText(5)));
      in.getFiles().add(file);
    }

    if (wRowSeparator.getText().length() > 1) {
      if (wRowSeparator.getText().charAt(0) == '\\') {
        // Take the 2 first
        wRowSeparator.setText(wRowSeparator.getText().substring(0, 2));
      } else {
        wRowSeparator.setText(wRowSeparator.getText().substring(0, 1));
      }
    }
    in.setRowSeparator(wRowSeparator.getText());
    in.setSmartCount(wSmartCount.getSelection());
    in.setAddResultFilename(wAddResult.getSelection());
    in.setFileFromField(wFileField.getSelection());
    in.setOutputFilenameField(wFilenameField.getText());
  }

  // Preview the data
  private void preview() {
    try {

      GetFilesRowsCountMeta oneMeta = new GetFilesRowsCountMeta();
      getInfo(oneMeta);

      PipelineMeta previewMeta =
          PipelinePreviewFactory.generatePreviewPipeline(
              pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

      EnterNumberDialog numberDialog =
          new EnterNumberDialog(
              shell,
              props.getDefaultPreviewSize(),
              BaseMessages.getString(PKG, "GetFilesRowsCountDialog.NumberRows.DialogTitle"),
              BaseMessages.getString(PKG, "GetFilesRowsCountDialog.NumberRows.DialogMessage"));
      int previewSize = numberDialog.open();
      if (previewSize > 0) {
        PipelinePreviewProgressDialog progressDialog =
            new PipelinePreviewProgressDialog(
                shell,
                variables,
                previewMeta,
                new String[] {wTransformName.getText()},
                new int[] {previewSize});
        progressDialog.open();

        if (!progressDialog.isCancelled()) {
          Pipeline pipeline = progressDialog.getPipeline();
          String loggingText = progressDialog.getLoggingText();

          if (pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0) {
            EnterTextDialog etd =
                new EnterTextDialog(
                    shell,
                    BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
                    BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"),
                    loggingText,
                    true);
            etd.setReadOnly();
            etd.open();
          }

          PreviewRowsDialog prd =
              new PreviewRowsDialog(
                  shell,
                  variables,
                  SWT.NONE,
                  wTransformName.getText(),
                  progressDialog.getPreviewRowsMeta(wTransformName.getText()),
                  progressDialog.getPreviewRows(wTransformName.getText()),
                  loggingText);
          prd.open();
        }
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "GetFilesRowsCountDialog.ErrorPreviewingData.DialogTitle"),
          BaseMessages.getString(PKG, "GetFilesRowsCountDialog.ErrorPreviewingData.DialogMessage"),
          e);
    }
  }
}
