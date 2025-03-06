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
package org.apache.hop.pipeline.transforms.accessoutput;

import io.github.spannm.jackcess.Database;
import io.github.spannm.jackcess.DatabaseBuilder;
import java.io.File;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class AccessOutputDialog extends BaseTransformDialog {
  private static final Class<?> PKG =
      AccessOutputDialog.class; // for i18n purposes, needed by Translator2!!

  private final AccessOutputMeta input;

  private TextVar wFileName;
  private Combo wFileFormat;
  private Button wCreateFile;

  private TextVar wTableName;
  private Button wCreateTable;
  private Button wTruncateTable;
  private Text wCommitSize;
  private Button wAddToResultFile;
  private Button wWaitFirstRowToCreateFile;

  public AccessOutputDialog(
      Shell parent,
      IVariables variables,
      AccessOutputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    shell.setMinimumSize(340, 340);
    setShellImage(shell, input);

    backupChanged = input.hasChanged();

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "AccessOutputDialog.DialogTitle"));

    // Transform name line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
    PropsUi.setLook(wlTransformName);
    FormData fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, 0);
    wlTransformName.setLayoutData(fdlTransformName);

    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addListener(SWT.Modify, e -> input.setChanged());
    wTransformName.addListener(SWT.DefaultSelection, e -> ok());
    FormData fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, 0);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Filename line
    Label wlFileName = new Label(shell, SWT.RIGHT);
    wlFileName.setText(BaseMessages.getString(PKG, "AccessOutputDialog.Filename.Label"));
    wlFileName.setToolTipText(BaseMessages.getString(PKG, "AccessOutputDialog.Filename.Tooltip"));
    PropsUi.setLook(wlFileName);
    FormData fdlFileName = new FormData();
    fdlFileName.left = new FormAttachment(0, 0);
    fdlFileName.top = new FormAttachment(wTransformName, margin);
    fdlFileName.right = new FormAttachment(middle, -margin);
    wlFileName.setLayoutData(fdlFileName);

    Button wbbFileName = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbFileName);
    wbbFileName.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbbFileName.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    wbbFileName.addListener(SWT.Selection, e -> onSelectFileName());
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wTransformName, margin);
    wbbFileName.setLayoutData(fdbFilename);

    wFileName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wFileName.setToolTipText(BaseMessages.getString(PKG, "AccessOutputDialog.Filename.Tooltip"));
    wFileName.addListener(SWT.Modify, e -> input.setChanged());
    PropsUi.setLook(wFileName);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(wbbFileName, -margin);
    fdFilename.top = new FormAttachment(wTransformName, margin);
    wFileName.setLayoutData(fdFilename);

    // File format
    Label wlFileFormat = new Label(shell, SWT.RIGHT);
    wlFileFormat.setText(BaseMessages.getString(PKG, "AccessOutputDialog.FileFormat.Label"));
    wlFileFormat.setToolTipText(
        BaseMessages.getString(PKG, "AccessOutputDialog.FileFormat.Tooltip"));
    PropsUi.setLook(wlFileFormat);
    FormData fdlFileFormat = new FormData();
    fdlFileFormat.left = new FormAttachment(0, 0);
    fdlFileFormat.right = new FormAttachment(middle, -margin);
    fdlFileFormat.top = new FormAttachment(wFileName, margin);
    wlFileFormat.setLayoutData(fdlFileFormat);
    wFileFormat = new Combo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wFileFormat.setItems(AccessFileFormat.getDescriptions());
    PropsUi.setLook(wFileFormat);
    FormData fdFileFormat = new FormData();
    fdFileFormat.left = new FormAttachment(middle, 0);
    fdFileFormat.top = new FormAttachment(wFileName, margin);
    fdFileFormat.right = new FormAttachment(100, 0);
    wFileFormat.setLayoutData(fdFileFormat);

    // Create file?
    Label wlCreateFile = new Label(shell, SWT.RIGHT);
    wlCreateFile.setText(BaseMessages.getString(PKG, "AccessOutputDialog.CreateFile.Label"));
    wlCreateFile.setToolTipText(
        BaseMessages.getString(PKG, "AccessOutputDialog.CreateFile.Tooltip"));
    PropsUi.setLook(wlCreateFile);
    FormData fdlCreateFile = new FormData();
    fdlCreateFile.left = new FormAttachment(0, 0);
    fdlCreateFile.top = new FormAttachment(wFileFormat, margin);
    fdlCreateFile.right = new FormAttachment(middle, -margin);
    wlCreateFile.setLayoutData(fdlCreateFile);
    wCreateFile = new Button(shell, SWT.CHECK);
    wCreateFile.setToolTipText(
        BaseMessages.getString(PKG, "AccessOutputDialog.CreateFile.Tooltip"));
    PropsUi.setLook(wCreateFile);
    FormData fdCreateFile = new FormData();
    fdCreateFile.left = new FormAttachment(middle, 0);
    fdCreateFile.top = new FormAttachment(wFileFormat, margin);
    fdCreateFile.right = new FormAttachment(100, 0);
    wCreateFile.setLayoutData(fdCreateFile);
    wCreateFile.addListener(SWT.Selection, e -> input.setChanged());

    // Wait first row to create file
    Label wlWaitFirstRowToCreateFile = new Label(shell, SWT.RIGHT);
    wlWaitFirstRowToCreateFile.setText(
        BaseMessages.getString(PKG, "AccessOutputDialog.WaitFirstRowToCreateFile.Label"));
    PropsUi.setLook(wlWaitFirstRowToCreateFile);
    FormData fdlWaitFirstRowToCreateFile = new FormData();
    fdlWaitFirstRowToCreateFile.left = new FormAttachment(0, 0);
    fdlWaitFirstRowToCreateFile.top = new FormAttachment(wCreateFile, margin);
    fdlWaitFirstRowToCreateFile.right = new FormAttachment(middle, -margin);
    wlWaitFirstRowToCreateFile.setLayoutData(fdlWaitFirstRowToCreateFile);
    wWaitFirstRowToCreateFile = new Button(shell, SWT.CHECK);
    wWaitFirstRowToCreateFile.setToolTipText(
        BaseMessages.getString(PKG, "AccessOutputDialog.WaitFirstRowToCreateFile.Tooltip"));
    PropsUi.setLook(wWaitFirstRowToCreateFile);
    FormData fdWaitFirstRowToCreateFile = new FormData();
    fdWaitFirstRowToCreateFile.left = new FormAttachment(middle, 0);
    fdWaitFirstRowToCreateFile.top = new FormAttachment(wCreateFile, margin);
    fdWaitFirstRowToCreateFile.right = new FormAttachment(100, 0);
    wWaitFirstRowToCreateFile.setLayoutData(fdWaitFirstRowToCreateFile);
    wWaitFirstRowToCreateFile.addListener(SWT.Selection, e -> input.setChanged());

    // Table line...
    Label wlTablename = new Label(shell, SWT.RIGHT);
    wlTablename.setText(BaseMessages.getString(PKG, "AccessOutputDialog.TargetTable.Label"));
    PropsUi.setLook(wlTablename);
    FormData fdlTablename = new FormData();
    fdlTablename.left = new FormAttachment(0, 0);
    fdlTablename.top = new FormAttachment(wWaitFirstRowToCreateFile, margin);
    fdlTablename.right = new FormAttachment(middle, -margin);
    wlTablename.setLayoutData(fdlTablename);

    Button wbbTableName = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbTableName);
    wbbTableName.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbbTableName.addListener(SWT.Selection, e -> onSelectTableName());
    FormData fdbTablename = new FormData();
    fdbTablename.right = new FormAttachment(100, 0);
    fdbTablename.top = new FormAttachment(wWaitFirstRowToCreateFile, margin);
    wbbTableName.setLayoutData(fdbTablename);

    wTableName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTableName.setToolTipText(
        BaseMessages.getString(PKG, "AccessOutputDialog.TargetTable.Tooltip"));
    PropsUi.setLook(wTableName);
    FormData fdTablename = new FormData();
    fdTablename.left = new FormAttachment(middle, 0);
    fdTablename.right = new FormAttachment(wbbTableName, -margin);
    fdTablename.top = new FormAttachment(wWaitFirstRowToCreateFile, margin);
    wTableName.setLayoutData(fdTablename);

    // Create table?
    Label wlCreateTable = new Label(shell, SWT.RIGHT);
    wlCreateTable.setText(BaseMessages.getString(PKG, "AccessOutputDialog.CreateTable.Label"));
    wlCreateTable.setToolTipText(
        BaseMessages.getString(PKG, "AccessOutputDialog.CreateTable.Tooltip"));
    PropsUi.setLook(wlCreateTable);
    FormData fdlCreateTable = new FormData();
    fdlCreateTable.left = new FormAttachment(0, 0);
    fdlCreateTable.top = new FormAttachment(wTableName, margin);
    fdlCreateTable.right = new FormAttachment(middle, -margin);
    wlCreateTable.setLayoutData(fdlCreateTable);
    wCreateTable = new Button(shell, SWT.CHECK);
    wCreateTable.setToolTipText(
        BaseMessages.getString(PKG, "AccessOutputDialog.CreateTable.Tooltip"));
    PropsUi.setLook(wCreateTable);
    FormData fdCreateTable = new FormData();
    fdCreateTable.left = new FormAttachment(middle, 0);
    fdCreateTable.top = new FormAttachment(wTableName, margin);
    fdCreateTable.right = new FormAttachment(100, 0);
    wCreateTable.setLayoutData(fdCreateTable);
    wCreateTable.addListener(SWT.Selection, e -> input.setChanged());

    // Truncate table?
    Label wlTruncateTable = new Label(shell, SWT.RIGHT);
    wlTruncateTable.setText(BaseMessages.getString(PKG, "AccessOutputDialog.TruncateTable.Label"));
    wlTruncateTable.setToolTipText(
        BaseMessages.getString(PKG, "AccessOutputDialog.TruncateTable.Tooltip"));
    PropsUi.setLook(wlTruncateTable);
    FormData fdlTruncateTable = new FormData();
    fdlTruncateTable.left = new FormAttachment(0, 0);
    fdlTruncateTable.top = new FormAttachment(wCreateTable, margin);
    fdlTruncateTable.right = new FormAttachment(middle, -margin);
    wlTruncateTable.setLayoutData(fdlTruncateTable);
    wTruncateTable = new Button(shell, SWT.CHECK);
    wTruncateTable.setToolTipText(
        BaseMessages.getString(PKG, "AccessOutputDialog.TruncateTable.Tooltip"));
    PropsUi.setLook(wTruncateTable);
    FormData fdTruncateTable = new FormData();
    fdTruncateTable.left = new FormAttachment(middle, 0);
    fdTruncateTable.top = new FormAttachment(wCreateTable, margin);
    fdTruncateTable.right = new FormAttachment(100, 0);
    wTruncateTable.setLayoutData(fdTruncateTable);
    wTruncateTable.addListener(SWT.Selection, e -> input.setChanged());

    // The commit size...
    Label wlCommitSize = new Label(shell, SWT.RIGHT);
    wlCommitSize.setText(BaseMessages.getString(PKG, "AccessOutputDialog.CommitSize.Label"));
    PropsUi.setLook(wlCommitSize);
    FormData fdlCommitSize = new FormData();
    fdlCommitSize.left = new FormAttachment(0, 0);
    fdlCommitSize.top = new FormAttachment(wTruncateTable, margin);
    fdlCommitSize.right = new FormAttachment(middle, -margin);
    wlCommitSize.setLayoutData(fdlCommitSize);

    wCommitSize = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wCommitSize.setToolTipText(
        BaseMessages.getString(PKG, "AccessOutputDialog.CommitSize.Tooltip"));
    PropsUi.setLook(wCommitSize);
    FormData fdCommitSize = new FormData();
    fdCommitSize.left = new FormAttachment(middle, 0);
    fdCommitSize.right = new FormAttachment(100, 0);
    fdCommitSize.top = new FormAttachment(wTruncateTable, margin);
    wCommitSize.setLayoutData(fdCommitSize);
    wCommitSize.addListener(SWT.Modify, e -> input.setChanged());

    // Add File to the result files name
    Label wlAddToResultFile = new Label(shell, SWT.RIGHT);
    wlAddToResultFile.setText(
        BaseMessages.getString(PKG, "AccessOutputMeta.AddToResultFile.Label"));
    PropsUi.setLook(wlAddToResultFile);
    FormData fdlAddToResultFile = new FormData();
    fdlAddToResultFile.left = new FormAttachment(0, 0);
    fdlAddToResultFile.top = new FormAttachment(wCommitSize, 2 * margin);
    fdlAddToResultFile.right = new FormAttachment(middle, -margin);
    wlAddToResultFile.setLayoutData(fdlAddToResultFile);
    wAddToResultFile = new Button(shell, SWT.CHECK);
    wAddToResultFile.setToolTipText(
        BaseMessages.getString(PKG, "AccessOutputMeta.AddToResultFile.Tooltip"));
    PropsUi.setLook(wAddToResultFile);
    FormData fdAddToResultFile = new FormData();
    fdAddToResultFile.left = new FormAttachment(middle, 0);
    fdAddToResultFile.top = new FormAttachment(wCommitSize, 2 * margin);
    fdAddToResultFile.right = new FormAttachment(100, 0);
    wAddToResultFile.setLayoutData(fdAddToResultFile);
    wAddToResultFile.addListener(SWT.Selection, e -> input.setChanged());

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getFileName() != null) {
      wFileName.setText(input.getFileName());
    }
    if (input.getFileFormat() != null) {
      wFileFormat.select(input.getFileFormat().getIndex());
    }

    if (input.getTableName() != null) {
      wTableName.setText(input.getTableName());
    }

    wCreateFile.setSelection(input.isCreateFile());
    wCreateTable.setSelection(input.isCreateFile());
    wTruncateTable.setSelection(input.isTruncateTable());
    if (input.getCommitSize() > 0) {
      wCommitSize.setText(Integer.toString(input.getCommitSize()));
    }
    wAddToResultFile.setSelection(input.isAddToResultFile());
    wWaitFirstRowToCreateFile.setSelection(input.isWaitFirstRowToCreateFile());

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(backupChanged);
    dispose();
  }

  private void getInfo(AccessOutputMeta info) {
    info.setFileName(wFileName.getText());
    if (wFileFormat.getSelectionIndex() != -1) {
      input.setFileFormat(AccessFileFormat.lookupDescription(wFileFormat.getText()));
    }
    info.setTableName(wTableName.getText());
    info.setCreateFile(wCreateFile.getSelection());
    info.setCreateTable(wCreateTable.getSelection());
    info.setTruncateTable(wTruncateTable.getSelection());
    info.setCommitSize(Const.toInt(wCommitSize.getText(), -1));
    info.setAddToResultFile(wAddToResultFile.getSelection());
    input.setWaitFirstRowToCreateFile(wWaitFirstRowToCreateFile.getSelection());
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo(input);

    dispose();
  }

  private void onSelectFileName() {
    BaseDialog.presentFileDialog(
        true,
        shell,
        wFileName,
        variables,
        new String[] {"*.mdb;*.MDB;*.accdb;*.ACCDB", "*"},
        new String[] {
          BaseMessages.getString(PKG, "AccessOutputDialog.FileType.AccessFiles"),
          BaseMessages.getString(PKG, "System.FileType.AllFiles")
        },
        true);
  }

  private void onSelectTableName() {
    AccessOutputMeta meta = new AccessOutputMeta();
    getInfo(meta);

    Database database = null;
    // New class: SelectTableDialog
    try {
      String fileName = variables.resolve(meta.getFileName());
      FileObject fileObject = HopVfs.getFileObject(fileName);
      File file = FileUtils.toFile(fileObject.getURL());

      if (!file.exists() || !file.isFile()) {
        throw new HopException(
            BaseMessages.getString(PKG, "AccessOutputMeta.Exception.FileDoesNotExist", fileName));
      }

      database = DatabaseBuilder.open(file);
      Set<String> set = database.getTableNames();
      String[] tablenames = set.toArray(new String[set.size()]);
      EnterSelectionDialog dialog =
          new EnterSelectionDialog(
              shell,
              tablenames,
              BaseMessages.getString(PKG, "AccessOutputDialog.Dialog.SelectATable.Title"),
              BaseMessages.getString(PKG, "AccessOutputDialog.Dialog.SelectATable.Message"));
      String tablename = dialog.open();
      if (tablename != null) {
        wTableName.setText(tablename);
      }
    } catch (Throwable e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "AccessOutputDialog.UnableToGetListOfTables.Title"),
          BaseMessages.getString(PKG, "AccessOutputDialog.UnableToGetListOfTables.Message"),
          e);
    } finally {
      // Don't forget to close the bugger.
      try {
        if (database != null) {
          database.close();
        }
      } catch (Exception e) {
        // Ignore close errors
      }
    }
  }
}
