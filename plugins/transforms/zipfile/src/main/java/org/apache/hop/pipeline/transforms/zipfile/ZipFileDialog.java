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

package org.apache.hop.pipeline.transforms.zipfile;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class ZipFileDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ZipFileMeta.class;

  private CCombo wSourceFileNameField;

  private CCombo wTargetFileNameField;

  private Button wAddResult;

  private Button wOverwriteZipEntry;

  private Button wCreateParentFolder;

  private Button wKeepFolders;

  private final ZipFileMeta input;

  private Label wlBaseFolderField;
  private CCombo wBaseFolderField;

  private CCombo wOperation;

  private Label wlMoveToFolderField;
  private CCombo wMoveToFolderField;

  private boolean gotPreviousFields = false;

  public ZipFileDialog(
      Shell parent, IVariables variables, ZipFileMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "ZipFileDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    SelectionAdapter lsSel =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    changed = input.hasChanged();

    ScrolledComposite sc = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(sc);
    FormData fdSc = new FormData();
    fdSc.left = new FormAttachment(0, 0);
    fdSc.top = new FormAttachment(wSpacer, 0);
    fdSc.right = new FormAttachment(100, 0);
    fdSc.bottom = new FormAttachment(wOk, -margin);
    sc.setLayoutData(fdSc);
    sc.setLayout(new FillLayout());

    Composite wContent = new Composite(sc, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    // ///////////////////////////////
    // START OF Settings GROUP //
    // ///////////////////////////////

    Group wSettingsGroup = new Group(wContent, SWT.SHADOW_NONE);
    PropsUi.setLook(wSettingsGroup);
    wSettingsGroup.setText(BaseMessages.getString(PKG, "ZipFileDialog.wSettingsGroup.Label"));

    FormLayout settingGroupLayout = new FormLayout();
    settingGroupLayout.marginWidth = 10;
    settingGroupLayout.marginHeight = 10;
    wSettingsGroup.setLayout(settingGroupLayout);

    // Create target parent folder?
    Label wlCreateParentFolder = new Label(wSettingsGroup, SWT.RIGHT);
    wlCreateParentFolder.setText(
        BaseMessages.getString(PKG, "ZipFileDialog.CreateParentFolder.Label"));
    PropsUi.setLook(wlCreateParentFolder);
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment(0, 0);
    fdlCreateParentFolder.top = new FormAttachment(0, margin);
    fdlCreateParentFolder.right = new FormAttachment(middle, -margin);
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
    wCreateParentFolder = new Button(wSettingsGroup, SWT.CHECK);
    PropsUi.setLook(wCreateParentFolder);
    wCreateParentFolder.setToolTipText(
        BaseMessages.getString(PKG, "ZipFileDialog.CreateParentFolder.Tooltip"));
    FormData fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment(middle, 0);
    fdCreateParentFolder.top = new FormAttachment(wlCreateParentFolder, 0, SWT.CENTER);
    wCreateParentFolder.setLayoutData(fdCreateParentFolder);
    wCreateParentFolder.addSelectionListener(lsSel);

    // Overwrite target file?
    Label wlOverwriteTarget = new Label(wSettingsGroup, SWT.RIGHT);
    wlOverwriteTarget.setText(BaseMessages.getString(PKG, "ZipFileDialog.OverwriteTarget.Label"));
    PropsUi.setLook(wlOverwriteTarget);
    FormData fdlOverwriteTarget = new FormData();
    fdlOverwriteTarget.left = new FormAttachment(0, 0);
    fdlOverwriteTarget.top = new FormAttachment(wCreateParentFolder, margin);
    fdlOverwriteTarget.right = new FormAttachment(middle, -margin);
    wlOverwriteTarget.setLayoutData(fdlOverwriteTarget);
    wOverwriteZipEntry = new Button(wSettingsGroup, SWT.CHECK);
    PropsUi.setLook(wOverwriteZipEntry);
    wOverwriteZipEntry.setToolTipText(
        BaseMessages.getString(PKG, "ZipFileDialog.OverwriteTarget.Tooltip"));
    FormData fdOverwriteTarget = new FormData();
    fdOverwriteTarget.left = new FormAttachment(middle, 0);
    fdOverwriteTarget.top = new FormAttachment(wlOverwriteTarget, 0, SWT.CENTER);
    wOverwriteZipEntry.setLayoutData(fdOverwriteTarget);
    wOverwriteZipEntry.addSelectionListener(lsSel);

    // Add Target filename to result filenames?
    Label wlAddResult = new Label(wSettingsGroup, SWT.RIGHT);
    wlAddResult.setText(BaseMessages.getString(PKG, "ZipFileDialog.AddResult.Label"));
    PropsUi.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment(0, 0);
    fdlAddResult.top = new FormAttachment(wOverwriteZipEntry, margin);
    fdlAddResult.right = new FormAttachment(middle, -margin);
    wlAddResult.setLayoutData(fdlAddResult);
    wAddResult = new Button(wSettingsGroup, SWT.CHECK);
    PropsUi.setLook(wAddResult);
    wAddResult.setToolTipText(BaseMessages.getString(PKG, "ZipFileDialog.AddResult.Tooltip"));
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment(middle, 0);
    fdAddResult.top = new FormAttachment(wlAddResult, 0, SWT.CENTER);
    wAddResult.setLayoutData(fdAddResult);
    wAddResult.addSelectionListener(lsSel);

    FormData fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment(0, margin);
    fdSettingsGroup.top = new FormAttachment(0, margin);
    fdSettingsGroup.right = new FormAttachment(100, -margin);
    wSettingsGroup.setLayoutData(fdSettingsGroup);

    // ///////////////////////////////
    // END OF Settings Fields GROUP //
    // ///////////////////////////////

    // SourceFileNameField field
    Label wlSourceFileNameField = new Label(wContent, SWT.RIGHT);
    wlSourceFileNameField.setText(
        BaseMessages.getString(PKG, "ZipFileDialog.SourceFileNameField.Label"));
    PropsUi.setLook(wlSourceFileNameField);
    FormData fdlSourceFileNameField = new FormData();
    fdlSourceFileNameField.left = new FormAttachment(0, 0);
    fdlSourceFileNameField.right = new FormAttachment(middle, -margin);
    fdlSourceFileNameField.top = new FormAttachment(wSettingsGroup, margin);
    wlSourceFileNameField.setLayoutData(fdlSourceFileNameField);

    wSourceFileNameField = new CCombo(wContent, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wSourceFileNameField);
    wSourceFileNameField.setEditable(true);
    wSourceFileNameField.addModifyListener(lsMod);
    FormData fdSourceFileNameField = new FormData();
    fdSourceFileNameField.left = new FormAttachment(middle, 0);
    fdSourceFileNameField.top = new FormAttachment(wSettingsGroup, margin);
    fdSourceFileNameField.right = new FormAttachment(100, -margin);
    wSourceFileNameField.setLayoutData(fdSourceFileNameField);
    wSourceFileNameField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable focuslost event
          }

          @Override
          public void focusGained(FocusEvent e) {
            get();
          }
        });
    // TargetFileNameField field
    Label wlTargetFileNameField = new Label(wContent, SWT.RIGHT);
    wlTargetFileNameField.setText(
        BaseMessages.getString(PKG, "ZipFileDialog.TargetFileNameField.Label"));
    PropsUi.setLook(wlTargetFileNameField);
    FormData fdlTargetFileNameField = new FormData();
    fdlTargetFileNameField.left = new FormAttachment(0, 0);
    fdlTargetFileNameField.right = new FormAttachment(middle, -margin);
    fdlTargetFileNameField.top = new FormAttachment(wSourceFileNameField, margin);
    wlTargetFileNameField.setLayoutData(fdlTargetFileNameField);

    wTargetFileNameField = new CCombo(wContent, SWT.BORDER | SWT.READ_ONLY);
    wTargetFileNameField.setEditable(true);
    PropsUi.setLook(wTargetFileNameField);
    wTargetFileNameField.addModifyListener(lsMod);
    FormData fdTargetFileNameField = new FormData();
    fdTargetFileNameField.left = new FormAttachment(middle, 0);
    fdTargetFileNameField.top = new FormAttachment(wSourceFileNameField, margin);
    fdTargetFileNameField.right = new FormAttachment(100, -margin);
    wTargetFileNameField.setLayoutData(fdTargetFileNameField);
    wTargetFileNameField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable focuslost event
          }

          @Override
          public void focusGained(FocusEvent e) {
            get();
          }
        });

    Label wlKeepFolders = new Label(wContent, SWT.RIGHT);
    wlKeepFolders.setText(BaseMessages.getString(PKG, "ZipFileDialog.KeepFolders.Label"));
    PropsUi.setLook(wlKeepFolders);
    FormData fdlKeepFolders = new FormData();
    fdlKeepFolders.left = new FormAttachment(0, 0);
    fdlKeepFolders.top = new FormAttachment(wTargetFileNameField, margin);
    fdlKeepFolders.right = new FormAttachment(middle, -margin);
    wlKeepFolders.setLayoutData(fdlKeepFolders);
    wKeepFolders = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wKeepFolders);
    wKeepFolders.setToolTipText(BaseMessages.getString(PKG, "ZipFileDialog.KeepFolders.Tooltip"));
    FormData fdKeepFolders = new FormData();
    fdKeepFolders.left = new FormAttachment(middle, 0);
    fdKeepFolders.top = new FormAttachment(wTargetFileNameField, margin);
    wKeepFolders.setLayoutData(fdKeepFolders);
    wKeepFolders.addSelectionListener(lsSel);
    wKeepFolders.addSelectionListener(
        new SelectionAdapter() {

          @Override
          public void widgetSelected(SelectionEvent arg0) {
            keepFolder();
          }
        });

    // BaseFolderField field
    wlBaseFolderField = new Label(wContent, SWT.RIGHT);
    wlBaseFolderField.setText(BaseMessages.getString(PKG, "ZipFileDialog.BaseFolderField.Label"));
    PropsUi.setLook(wlBaseFolderField);
    FormData fdlBaseFolderField = new FormData();
    fdlBaseFolderField.left = new FormAttachment(0, 0);
    fdlBaseFolderField.right = new FormAttachment(middle, -margin);
    fdlBaseFolderField.top = new FormAttachment(wKeepFolders, margin);
    wlBaseFolderField.setLayoutData(fdlBaseFolderField);

    wBaseFolderField = new CCombo(wContent, SWT.BORDER | SWT.READ_ONLY);
    wBaseFolderField.setEditable(true);
    PropsUi.setLook(wBaseFolderField);
    wBaseFolderField.addModifyListener(lsMod);
    FormData fdBaseFolderField = new FormData();
    fdBaseFolderField.left = new FormAttachment(middle, 0);
    fdBaseFolderField.top = new FormAttachment(wKeepFolders, margin);
    fdBaseFolderField.right = new FormAttachment(100, -margin);
    wBaseFolderField.setLayoutData(fdBaseFolderField);
    wBaseFolderField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable focuslost event
          }

          @Override
          public void focusGained(FocusEvent e) {
            get();
          }
        });

    // Operation
    Label wlOperation = new Label(wContent, SWT.RIGHT);
    wlOperation.setText(BaseMessages.getString(PKG, "ZipFileDialog.Operation.Label"));
    PropsUi.setLook(wlOperation);
    FormData fdlOperation = new FormData();
    fdlOperation.left = new FormAttachment(0, 0);
    fdlOperation.right = new FormAttachment(middle, -margin);
    fdlOperation.top = new FormAttachment(wBaseFolderField, margin);
    wlOperation.setLayoutData(fdlOperation);

    wOperation = new CCombo(wContent, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wOperation);
    wOperation.addModifyListener(lsMod);
    FormData fdOperation = new FormData();
    fdOperation.left = new FormAttachment(middle, 0);
    fdOperation.top = new FormAttachment(wBaseFolderField, margin);
    fdOperation.right = new FormAttachment(100, -margin);
    wOperation.setLayoutData(fdOperation);
    wOperation.setItems(ZipFileMeta.operationTypeDesc);
    wOperation.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            updateOperation();
          }
        });

    // MoveToFolderField field
    wlMoveToFolderField = new Label(wContent, SWT.RIGHT);
    wlMoveToFolderField.setText(
        BaseMessages.getString(PKG, "ZipFileDialog.MoveToFolderField.Label"));
    PropsUi.setLook(wlMoveToFolderField);
    FormData fdlMoveToFolderField = new FormData();
    fdlMoveToFolderField.left = new FormAttachment(0, 0);
    fdlMoveToFolderField.right = new FormAttachment(middle, -margin);
    fdlMoveToFolderField.top = new FormAttachment(wOperation, margin);
    wlMoveToFolderField.setLayoutData(fdlMoveToFolderField);

    wMoveToFolderField = new CCombo(wContent, SWT.BORDER | SWT.READ_ONLY);
    wMoveToFolderField.setEditable(true);
    PropsUi.setLook(wMoveToFolderField);
    wMoveToFolderField.addModifyListener(lsMod);
    FormData fdMoveToFolderField = new FormData();
    fdMoveToFolderField.left = new FormAttachment(middle, 0);
    fdMoveToFolderField.top = new FormAttachment(wOperation, margin);
    fdMoveToFolderField.right = new FormAttachment(100, -margin);
    wMoveToFolderField.setLayoutData(fdMoveToFolderField);
    wMoveToFolderField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable focuslost event
          }

          @Override
          public void focusGained(FocusEvent e) {
            get();
          }
        });

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    sc.setContent(wContent);
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);
    sc.setMinWidth(bounds.width);
    sc.setMinHeight(bounds.height);

    getData();
    keepFolder();
    updateOperation();
    input.setChanged(changed);

    focusTransformName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (log.isDebug()) {
      log.logDebug(toString(), BaseMessages.getString(PKG, "ZipFileDialog.Log.GettingKeyInfo"));
    }
    if (input.getBaseFolderField() != null) {
      wBaseFolderField.setText(input.getBaseFolderField());
    }
    if (input.getSourceFilenameField() != null) {
      wSourceFileNameField.setText(input.getSourceFilenameField());
    }
    if (input.getTargetFilenameField() != null) {
      wTargetFileNameField.setText(input.getTargetFilenameField());
    }
    wOperation.setText(ZipFileMeta.getOperationTypeDesc(input.getOperationType()));
    if (input.getMoveToFolderField() != null) {
      wMoveToFolderField.setText(input.getMoveToFolderField());
    }

    wAddResult.setSelection(input.isAddResultFilenames());
    wOverwriteZipEntry.setSelection(input.isOverwriteZipEntry());
    wCreateParentFolder.setSelection(input.isCreateParentFolder());
    wKeepFolders.setSelection(input.isKeepSourceFolder());
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "System.Error.TransformNameMissing.Message"));
      mb.setText(BaseMessages.getString(PKG, "System.Error.TransformNameMissing.Title"));
      mb.open();
      return;
    }
    input.setBaseFolderField(wBaseFolderField.getText());
    input.setSourceFilenameField(wSourceFileNameField.getText());
    input.setTargetFilenameField(wTargetFileNameField.getText());
    input.setAddResultFilenames(wAddResult.getSelection());
    input.setOverwriteZipEntry(wOverwriteZipEntry.getSelection());
    input.setCreateParentFolder(wCreateParentFolder.getSelection());
    input.setKeepSourceFolder(wKeepFolders.getSelection());
    input.setOperationType(ZipFileMeta.getOperationTypeByDesc(wOperation.getText()));
    input.setMoveToFolderField(wMoveToFolderField.getText());
    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void keepFolder() {
    wlBaseFolderField.setEnabled(wKeepFolders.getSelection());
    wBaseFolderField.setEnabled(wKeepFolders.getSelection());
  }

  private void get() {
    if (!gotPreviousFields) {
      gotPreviousFields = true;
      String source = wSourceFileNameField.getText();
      String target = wTargetFileNameField.getText();
      String base = wBaseFolderField.getText();

      try {

        wSourceFileNameField.removeAll();
        wTargetFileNameField.removeAll();
        wBaseFolderField.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          String[] fields = r.getFieldNames();
          wSourceFileNameField.setItems(fields);
          wTargetFileNameField.setItems(fields);
          wBaseFolderField.setItems(fields);
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "ZipFileDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "ZipFileDialog.FailedToGetFields.DialogMessage"),
            ke);
      } finally {
        if (source != null) {
          wSourceFileNameField.setText(source);
        }
        if (target != null) {
          wTargetFileNameField.setText(target);
        }
        if (base != null) {
          wBaseFolderField.setText(base);
        }
      }
    }
  }

  private void updateOperation() {
    wlMoveToFolderField.setEnabled(
        ZipFileMeta.getOperationTypeByDesc(wOperation.getText())
            == ZipFileMeta.OPERATION_TYPE_MOVE);
    wMoveToFolderField.setEnabled(
        ZipFileMeta.getOperationTypeByDesc(wOperation.getText())
            == ZipFileMeta.OPERATION_TYPE_MOVE);
  }
}
