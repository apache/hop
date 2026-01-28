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

package org.apache.hop.pipeline.transforms.getsubfolders;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
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
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class GetSubFoldersDialog extends BaseTransformDialog {
  private static final Class<?> PKG = GetSubFoldersMeta.class;

  public static final String YES = BaseMessages.getString(PKG, "System.Combo.Yes");
  public static final String NO = BaseMessages.getString(PKG, "System.Combo.No");

  private Label wlFolderName;

  private Button wbbFolderName; // Browse: add directory

  private Button wbdFolderName; // Delete

  private Button wbeFolderName; // Edit

  private Button wbaFolderName; // Add or change

  private TextVar wFolderName;

  private Label wlFolderNameList;

  private TableView wFolderNameList;

  private final GetSubFoldersMeta input;

  private Button wFolderField;

  private Label wlFilenameField;
  private ComboVar wFolderNameField;

  private Label wlLimit;
  private Text wLimit;

  private Button wInclRowNumber;

  private Label wlInclRowNumberField;
  private TextVar wInclRowNumberField;

  public GetSubFoldersDialog(
      Shell parent,
      IVariables variables,
      GetSubFoldersMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "GetSubFoldersDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).preview(e -> preview()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF FILE TAB ///
    // ////////////////////////
    CTabItem wFolderTab = new CTabItem(wTabFolder, SWT.NONE);
    wFolderTab.setFont(GuiResource.getInstance().getFontDefault());
    wFolderTab.setText(BaseMessages.getString(PKG, "GetSubFoldersDialog.FolderTab.TabTitle"));

    Composite wFolderComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFolderComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFolderComp.setLayout(fileLayout);

    // ///////////////////////////////
    // START OF Origin files GROUP //
    // ///////////////////////////////

    Group wOriginFolders = new Group(wFolderComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wOriginFolders);
    wOriginFolders.setText(BaseMessages.getString(PKG, "GetSubFoldersDialog.wOriginFiles.Label"));

    FormLayout originFilesgroupLayout = new FormLayout();
    originFilesgroupLayout.marginWidth = 10;
    originFilesgroupLayout.marginHeight = 10;
    wOriginFolders.setLayout(originFilesgroupLayout);

    // Is Filename defined in a Field
    Label wlFileField = new Label(wOriginFolders, SWT.RIGHT);
    wlFileField.setText(BaseMessages.getString(PKG, "GetSubFoldersDialog.FolderField.Label"));
    PropsUi.setLook(wlFileField);
    FormData fdlFileField = new FormData();
    fdlFileField.left = new FormAttachment(0, -margin);
    fdlFileField.top = new FormAttachment(0, margin);
    fdlFileField.right = new FormAttachment(middle, -margin);
    wlFileField.setLayoutData(fdlFileField);

    wFolderField = new Button(wOriginFolders, SWT.CHECK);
    PropsUi.setLook(wFolderField);
    wFolderField.setToolTipText(
        BaseMessages.getString(PKG, "GetSubFoldersDialog.FileField.Tooltip"));
    FormData fdFileField = new FormData();
    fdFileField.left = new FormAttachment(middle, -margin);
    fdFileField.top = new FormAttachment(wlFileField, 0, SWT.CENTER);
    wFolderField.setLayoutData(fdFileField);
    SelectionAdapter lsFileField =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            activateFileField();
            input.setChanged();
          }
        };
    wFolderField.addSelectionListener(lsFileField);

    // Filename field
    wlFilenameField = new Label(wOriginFolders, SWT.RIGHT);
    wlFilenameField.setText(
        BaseMessages.getString(PKG, "GetSubFoldersDialog.wlFilenameField.Label"));
    PropsUi.setLook(wlFilenameField);
    FormData fdlFolderNameField = new FormData();
    fdlFolderNameField.left = new FormAttachment(0, -margin);
    fdlFolderNameField.top = new FormAttachment(wFolderField, margin);
    fdlFolderNameField.right = new FormAttachment(middle, -margin);
    wlFilenameField.setLayoutData(fdlFolderNameField);

    wFolderNameField = new ComboVar(variables, wOriginFolders, SWT.BORDER | SWT.READ_ONLY);
    wFolderNameField.setEditable(true);
    PropsUi.setLook(wFolderNameField);
    wFolderNameField.addModifyListener(lsMod);
    FormData fdFolderNameField = new FormData();
    fdFolderNameField.left = new FormAttachment(middle, -margin);
    fdFolderNameField.top = new FormAttachment(wFolderField, margin);
    fdFolderNameField.right = new FormAttachment(100, -margin);
    wFolderNameField.setLayoutData(fdFolderNameField);
    wFolderNameField.setEnabled(false);
    wFolderNameField.addListener(
        SWT.FocusIn,
        e -> {
          Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
          shell.setCursor(busy);
          BaseTransformDialog.getFieldsFromPrevious(
              variables, wFolderNameField, pipelineMeta, transformMeta);
          shell.setCursor(null);
          busy.dispose();
        });

    FormData fdOriginFolders = new FormData();
    fdOriginFolders.left = new FormAttachment(0, margin);
    fdOriginFolders.top = new FormAttachment(wFolderNameList, margin);
    fdOriginFolders.right = new FormAttachment(100, -margin);
    wOriginFolders.setLayoutData(fdOriginFolders);

    // ///////////////////////////////////////////////////////////
    // / END OF Origin files GROUP
    // ///////////////////////////////////////////////////////////

    // FolderName line
    wlFolderName = new Label(wFolderComp, SWT.RIGHT);
    wlFolderName.setText(BaseMessages.getString(PKG, "GetSubFoldersDialog.Filename.Label"));
    PropsUi.setLook(wlFolderName);
    FormData fdlFolderName = new FormData();
    fdlFolderName.left = new FormAttachment(0, 0);
    fdlFolderName.top = new FormAttachment(wOriginFolders, margin);
    fdlFolderName.right = new FormAttachment(middle, -margin);
    wlFolderName.setLayoutData(fdlFolderName);

    wbbFolderName = new Button(wFolderComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbFolderName);
    wbbFolderName.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbbFolderName.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbFolderName = new FormData();
    fdbFolderName.right = new FormAttachment(100, 0);
    fdbFolderName.top = new FormAttachment(wOriginFolders, margin);
    wbbFolderName.setLayoutData(fdbFolderName);

    wbaFolderName = new Button(wFolderComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbaFolderName);
    wbaFolderName.setText(BaseMessages.getString(PKG, "GetSubFoldersDialog.FolderNameAdd.Button"));
    wbaFolderName.setToolTipText(
        BaseMessages.getString(PKG, "GetSubFoldersDialog.FolderNameAdd.Tooltip"));
    FormData fdbaFolderName = new FormData();
    fdbaFolderName.right = new FormAttachment(wbbFolderName, -margin);
    fdbaFolderName.top = new FormAttachment(wOriginFolders, margin);
    wbaFolderName.setLayoutData(fdbaFolderName);

    wFolderName = new TextVar(variables, wFolderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFolderName);
    wFolderName.addModifyListener(lsMod);
    FormData fdFolderName = new FormData();
    fdFolderName.left = new FormAttachment(middle, 0);
    fdFolderName.right = new FormAttachment(wbaFolderName, -margin);
    fdFolderName.top = new FormAttachment(wOriginFolders, margin);
    wFolderName.setLayoutData(fdFolderName);

    // Filename list line
    wlFolderNameList = new Label(wFolderComp, SWT.RIGHT);
    wlFolderNameList.setText(
        BaseMessages.getString(PKG, "GetSubFoldersDialog.FolderNameList.Label"));
    PropsUi.setLook(wlFolderNameList);
    FormData fdlFolderNameList = new FormData();
    fdlFolderNameList.left = new FormAttachment(0, 0);
    fdlFolderNameList.top = new FormAttachment(wFolderName, margin);
    fdlFolderNameList.right = new FormAttachment(middle, -margin);
    wlFolderNameList.setLayoutData(fdlFolderNameList);

    // Buttons to the right of the screen...
    wbdFolderName = new Button(wFolderComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbdFolderName);
    wbdFolderName.setText(
        BaseMessages.getString(PKG, "GetSubFoldersDialog.FolderNameDelete.Button"));
    wbdFolderName.setToolTipText(
        BaseMessages.getString(PKG, "GetSubFoldersDialog.FolderNameDelete.Tooltip"));
    FormData fdbdFolderName = new FormData();
    fdbdFolderName.right = new FormAttachment(100, 0);
    fdbdFolderName.top = new FormAttachment(wlFolderNameList, 0, SWT.TOP);
    wbdFolderName.setLayoutData(fdbdFolderName);

    wbeFolderName = new Button(wFolderComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbeFolderName);
    wbeFolderName.setText(BaseMessages.getString(PKG, "GetSubFoldersDialog.FilenameEdit.Button"));
    wbeFolderName.setToolTipText(
        BaseMessages.getString(PKG, "GetSubFoldersDialog.FilenameEdit.Tooltip"));
    FormData fdbeFolderName = new FormData();
    fdbeFolderName.right = new FormAttachment(100, 0);
    fdbeFolderName.left = new FormAttachment(wbdFolderName, 0, SWT.LEFT);
    fdbeFolderName.top = new FormAttachment(wbdFolderName, margin);
    wbeFolderName.setLayoutData(fdbeFolderName);

    ColumnInfo[] columns = new ColumnInfo[2];
    columns[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GetSubFoldersDialog.FileDirColumn.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    columns[0].setUsingVariables(true);
    columns[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GetSubFoldersDialog.Required.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            YES,
            NO);
    columns[1].setToolTip(BaseMessages.getString(PKG, "GetSubFoldersDialog.Required.Tooltip"));

    wFolderNameList =
        new TableView(
            variables,
            wFolderComp,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
            columns,
            input.getFiles().size(),
            lsMod,
            props);
    PropsUi.setLook(wFolderNameList);
    FormData fdFolderNameList = new FormData();
    fdFolderNameList.left = new FormAttachment(middle, 0);
    fdFolderNameList.right = new FormAttachment(wbdFolderName, -margin);
    fdFolderNameList.top = new FormAttachment(wlFolderNameList, 0, SWT.TOP);
    fdFolderNameList.bottom = new FormAttachment(100, -margin);
    wFolderNameList.setLayoutData(fdFolderNameList);

    FormData fdFolderComp = new FormData();
    fdFolderComp.left = new FormAttachment(0, 0);
    fdFolderComp.top = new FormAttachment(0, 0);
    fdFolderComp.right = new FormAttachment(100, 0);
    fdFolderComp.bottom = new FormAttachment(100, 0);
    wFolderComp.setLayoutData(fdFolderComp);

    wFolderComp.layout();
    wFolderTab.setControl(wFolderComp);

    // ///////////////////////////////////////////////////////////
    // / END OF FILE TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // ////////////////////////
    // START OF Filter TAB ///
    // ////////////////////////
    CTabItem wSettingsTab = new CTabItem(wTabFolder, SWT.NONE);
    wSettingsTab.setFont(GuiResource.getInstance().getFontDefault());
    wSettingsTab.setText(BaseMessages.getString(PKG, "GetSubFoldersDialog.SettingsTab.TabTitle"));

    Composite wSettingsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wSettingsComp);

    FormLayout fileSettingLayout = new FormLayout();
    fileSettingLayout.marginWidth = 3;
    fileSettingLayout.marginHeight = 3;
    wSettingsComp.setLayout(fileSettingLayout);

    // /////////////////////////////////
    // START OF Additional Fields GROUP
    // /////////////////////////////////

    Group wAdditionalGroup = new Group(wSettingsComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wAdditionalGroup);
    wAdditionalGroup.setText(
        BaseMessages.getString(PKG, "GetSubFoldersDialog.Group.AdditionalGroup.Label"));

    FormLayout additionalgroupLayout = new FormLayout();
    additionalgroupLayout.marginWidth = 10;
    additionalgroupLayout.marginHeight = 10;
    wAdditionalGroup.setLayout(additionalgroupLayout);

    Label wlInclRowNumber = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclRowNumber.setText(BaseMessages.getString(PKG, "GetSubFoldersDialog.InclRowNumber.Label"));
    PropsUi.setLook(wlInclRowNumber);
    FormData fdlInclRowNumber = new FormData();
    fdlInclRowNumber.left = new FormAttachment(0, 0);
    fdlInclRowNumber.top = new FormAttachment(0, margin);
    fdlInclRowNumber.right = new FormAttachment(middle, -margin);
    wlInclRowNumber.setLayoutData(fdlInclRowNumber);
    wInclRowNumber = new Button(wAdditionalGroup, SWT.CHECK);
    PropsUi.setLook(wInclRowNumber);
    wInclRowNumber.setToolTipText(
        BaseMessages.getString(PKG, "GetSubFoldersDialog.InclRowNumber.Tooltip"));
    FormData fdRowNumber = new FormData();
    fdRowNumber.left = new FormAttachment(middle, 0);
    fdRowNumber.top = new FormAttachment(wlInclRowNumber, 0, SWT.CENTER);
    wInclRowNumber.setLayoutData(fdRowNumber);
    SelectionAdapter linclRowNumber =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            activateIncludeRowNum();
            input.setChanged();
          }
        };
    wInclRowNumber.addSelectionListener(linclRowNumber);

    wlInclRowNumberField = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclRowNumberField.setText(
        BaseMessages.getString(PKG, "GetSubFoldersDialog.InclRowNumberField.Label"));
    PropsUi.setLook(wlInclRowNumberField);
    FormData fdlInclRowNumberField = new FormData();
    fdlInclRowNumberField.left = new FormAttachment(wInclRowNumber, margin);
    fdlInclRowNumberField.top = new FormAttachment(0, margin);
    wlInclRowNumberField.setLayoutData(fdlInclRowNumberField);
    wInclRowNumberField =
        new TextVar(variables, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInclRowNumberField);
    wInclRowNumberField.addModifyListener(lsMod);
    FormData fdInclRowNumberField = new FormData();
    fdInclRowNumberField.left = new FormAttachment(wlInclRowNumberField, margin);
    fdInclRowNumberField.top = new FormAttachment(0, margin);
    fdInclRowNumberField.right = new FormAttachment(100, 0);
    wInclRowNumberField.setLayoutData(fdInclRowNumberField);

    FormData fdAdditionalGroup = new FormData();
    fdAdditionalGroup.left = new FormAttachment(0, margin);
    fdAdditionalGroup.top = new FormAttachment(0, margin);
    fdAdditionalGroup.right = new FormAttachment(100, -margin);
    wAdditionalGroup.setLayoutData(fdAdditionalGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF DESTINATION ADDRESS GROUP
    // ///////////////////////////////////////////////////////////

    wlLimit = new Label(wSettingsComp, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "GetSubFoldersDialog.Limit.Label"));
    PropsUi.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.top = new FormAttachment(wAdditionalGroup, margin);
    fdlLimit.right = new FormAttachment(middle, -margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(wSettingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLimit);
    wLimit.addModifyListener(lsMod);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.top = new FormAttachment(wAdditionalGroup, margin);
    fdLimit.right = new FormAttachment(100, 0);
    wLimit.setLayoutData(fdLimit);

    FormData fdSettingsComp = new FormData();
    fdSettingsComp.left = new FormAttachment(0, 0);
    fdSettingsComp.top = new FormAttachment(0, 0);
    fdSettingsComp.right = new FormAttachment(100, 0);
    fdSettingsComp.bottom = new FormAttachment(100, 0);
    wSettingsComp.setLayoutData(fdSettingsComp);

    wSettingsComp.layout();
    wSettingsTab.setControl(wSettingsComp);

    // ///////////////////////////////////////////////////////////
    // / END OF FILE Filter TAB
    // ///////////////////////////////////////////////////////////

    // Add the file to the list of files...
    Listener selA =
        e -> {
          wFolderNameList.add(wFolderName.getText());
          wFolderName.setText("");
          wFolderNameList.removeEmptyRows();
          wFolderNameList.setRowNums();
          wFolderNameList.optWidth(true);
        };
    wbaFolderName.addListener(SWT.Selection, selA);
    wFolderName.addListener(SWT.Selection, selA);

    // Delete files from the list of files...
    wbdFolderName.addListener(
        SWT.Selection,
        e -> {
          int[] idx = wFolderNameList.getSelectionIndices();
          wFolderNameList.remove(idx);
          wFolderNameList.removeEmptyRows();
          wFolderNameList.setRowNums();
        });

    // Edit the selected file & remove from the list...
    wbeFolderName.addListener(
        SWT.Selection,
        e -> {
          int idx = wFolderNameList.getSelectionIndex();
          if (idx >= 0) {
            String[] string = wFolderNameList.getItem(idx);
            wFolderName.setText(string[0]);
            wFolderNameList.remove(idx);
          }
          wFolderNameList.removeEmptyRows();
          wFolderNameList.setRowNums();
        });

    wbbFolderName.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wFolderName, variables));

    wTabFolder.setSelection(0);

    // Set the shell size, based upon previous time...
    getData(input);
    activateFileField();
    activateIncludeRowNum();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void activateIncludeRowNum() {
    wlInclRowNumberField.setEnabled(wInclRowNumber.getSelection());
    wInclRowNumberField.setEnabled(wInclRowNumber.getSelection());
  }

  private void activateFileField() {
    wlFilenameField.setEnabled(wFolderField.getSelection());
    wFolderNameField.setEnabled(wFolderField.getSelection());

    wlFolderName.setEnabled(!wFolderField.getSelection());
    wbbFolderName.setEnabled(!wFolderField.getSelection());
    wbaFolderName.setEnabled(!wFolderField.getSelection());
    wFolderName.setEnabled(!wFolderField.getSelection());
    wlFolderNameList.setEnabled(!wFolderField.getSelection());
    wbdFolderName.setEnabled(!wFolderField.getSelection());
    wbeFolderName.setEnabled(!wFolderField.getSelection());
    wlFolderNameList.setEnabled(!wFolderField.getSelection());
    wFolderNameList.setEnabled(!wFolderField.getSelection());
    wPreview.setEnabled(!wFolderField.getSelection());
    wlLimit.setEnabled(!wFolderField.getSelection());
    wLimit.setEnabled(!wFolderField.getSelection());
  }

  /**
   * Read the data from the TextFileInputMeta object and show it in this dialog.
   *
   * @param meta The TextFileInputMeta object to obtain the data from.
   */
  public void getData(GetSubFoldersMeta meta) {
    for (int i = 0; i < meta.getFiles().size(); i++) {
      GetSubFoldersMeta.GSFile file = meta.getFiles().get(i);
      TableItem item = wFolderNameList.table.getItem(i);
      item.setText(1, Const.NVL(file.getName(), ""));
      item.setText(
          2,
          file.isRequired()
              ? BaseMessages.getString(PKG, "System.Combo.Yes")
              : BaseMessages.getString(PKG, "System.Combo.No"));
    }
    wFolderNameList.optimizeTableView();

    wInclRowNumber.setSelection(meta.isIncludeRowNumber());
    wFolderField.setSelection(meta.isFolderNameDynamic());
    if (meta.getRowNumberField() != null) {
      wInclRowNumberField.setText(meta.getRowNumberField());
    }
    if (meta.getDynamicFolderNameField() != null) {
      wFolderNameField.setText(meta.getDynamicFolderNameField());
    }
    wLimit.setText("" + meta.getRowLimit());
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    getInfo(input);
    dispose();
  }

  private void getInfo(GetSubFoldersMeta in) {
    transformName = wTransformName.getText(); // return value

    in.getFiles().clear();
    for (TableItem item : wFolderNameList.getNonEmptyItems()) {
      GetSubFoldersMeta.GSFile file = new GetSubFoldersMeta.GSFile();
      file.setName(item.getText(1));
      file.setRequired(YES.equalsIgnoreCase(item.getText(2)));
      in.getFiles().add(file);
    }

    in.setIncludeRowNumber(wInclRowNumber.getSelection());
    in.setDynamicFolderNameField(wFolderNameField.getText());
    in.setFolderNameDynamic(wFolderField.getSelection());
    in.setRowNumberField(wInclRowNumberField.getText());
    in.setRowLimit(Const.toLong(wLimit.getText(), 0L));
  }

  // Preview the data
  private void preview() {
    // Create the XML input transform
    GetSubFoldersMeta oneMeta = new GetSubFoldersMeta();
    getInfo(oneMeta);

    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "GetSubFoldersDialog.PreviewSize.DialogTitle"),
            BaseMessages.getString(PKG, "GetSubFoldersDialog.PreviewSize.DialogMessage"));
    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      oneMeta.setRowLimit(previewSize);
      PipelineMeta previewMeta =
          PipelinePreviewFactory.generatePreviewPipeline(
              pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

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
                  BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
                  BaseMessages.getString(PKG, "GetSubFoldersDialog.ErrorInPreview.DialogMessage"),
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
  }
}
