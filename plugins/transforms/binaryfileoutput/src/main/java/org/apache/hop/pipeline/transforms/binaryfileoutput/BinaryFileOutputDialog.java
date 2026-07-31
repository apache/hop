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

package org.apache.hop.pipeline.transforms.binaryfileoutput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class BinaryFileOutputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BinaryFileOutputMeta.class;

  private CCombo wBinaryField;
  private CCombo wFilenameField;
  private Button wCreateParentFolder;
  private Button wOverwriteFile;
  private Button wAddResult;

  private final BinaryFileOutputMeta input;
  private boolean gotPreviousFields;

  public BinaryFileOutputDialog(
      Shell parent,
      IVariables variables,
      BinaryFileOutputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BinaryFileOutputDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    SelectionAdapter lsButtonChanged =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        };
    changed = input.hasChanged();

    Control lastControl = wSpacer;

    // Binary field
    Label wlBinaryField = new Label(shell, SWT.RIGHT);
    wlBinaryField.setText(BaseMessages.getString(PKG, "BinaryFileOutputDialog.BinaryField.Label"));
    PropsUi.setLook(wlBinaryField);
    FormData fdlBinaryField = new FormData();
    fdlBinaryField.left = new FormAttachment(0, 0);
    fdlBinaryField.right = new FormAttachment(middle, -margin);
    fdlBinaryField.top = new FormAttachment(lastControl, margin);
    wlBinaryField.setLayoutData(fdlBinaryField);

    wBinaryField = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wBinaryField);
    wBinaryField.setToolTipText(
        BaseMessages.getString(PKG, "BinaryFileOutputDialog.BinaryField.Tooltip"));
    wBinaryField.addModifyListener(lsMod);
    FormData fdBinaryField = new FormData();
    fdBinaryField.left = new FormAttachment(middle, 0);
    fdBinaryField.top = new FormAttachment(lastControl, margin);
    fdBinaryField.right = new FormAttachment(100, 0);
    wBinaryField.setLayoutData(fdBinaryField);
    wBinaryField.addFocusListener(fieldFocusListener());
    lastControl = wBinaryField;

    // Filename field
    Label wlFilenameField = new Label(shell, SWT.RIGHT);
    wlFilenameField.setText(
        BaseMessages.getString(PKG, "BinaryFileOutputDialog.FilenameField.Label"));
    PropsUi.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, 0);
    fdlFilenameField.right = new FormAttachment(middle, -margin);
    fdlFilenameField.top = new FormAttachment(lastControl, margin);
    wlFilenameField.setLayoutData(fdlFilenameField);

    wFilenameField = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wFilenameField);
    wFilenameField.setToolTipText(
        BaseMessages.getString(PKG, "BinaryFileOutputDialog.FilenameField.Tooltip"));
    wFilenameField.addModifyListener(lsMod);
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment(middle, 0);
    fdFilenameField.top = new FormAttachment(lastControl, margin);
    fdFilenameField.right = new FormAttachment(100, 0);
    wFilenameField.setLayoutData(fdFilenameField);
    wFilenameField.addFocusListener(fieldFocusListener());
    lastControl = wFilenameField;

    // Create parent folder
    Label wlCreateParentFolder = new Label(shell, SWT.RIGHT);
    wlCreateParentFolder.setText(
        BaseMessages.getString(PKG, "BinaryFileOutputDialog.CreateParentFolder.Label"));
    PropsUi.setLook(wlCreateParentFolder);
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment(0, 0);
    fdlCreateParentFolder.right = new FormAttachment(middle, -margin);
    fdlCreateParentFolder.top = new FormAttachment(lastControl, margin);
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);

    wCreateParentFolder = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wCreateParentFolder);
    wCreateParentFolder.setToolTipText(
        BaseMessages.getString(PKG, "BinaryFileOutputDialog.CreateParentFolder.Tooltip"));
    wCreateParentFolder.addSelectionListener(lsButtonChanged);
    FormData fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment(middle, 0);
    fdCreateParentFolder.top = new FormAttachment(wlCreateParentFolder, 0, SWT.CENTER);
    wCreateParentFolder.setLayoutData(fdCreateParentFolder);
    lastControl = wlCreateParentFolder;

    // Overwrite existing file
    Label wlOverwriteFile = new Label(shell, SWT.RIGHT);
    wlOverwriteFile.setText(BaseMessages.getString(PKG, "BinaryFileOutputDialog.Overwrite.Label"));
    PropsUi.setLook(wlOverwriteFile);
    FormData fdlOverwriteFile = new FormData();
    fdlOverwriteFile.left = new FormAttachment(0, 0);
    fdlOverwriteFile.right = new FormAttachment(middle, -margin);
    fdlOverwriteFile.top = new FormAttachment(lastControl, margin);
    wlOverwriteFile.setLayoutData(fdlOverwriteFile);

    wOverwriteFile = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wOverwriteFile);
    wOverwriteFile.setToolTipText(
        BaseMessages.getString(PKG, "BinaryFileOutputDialog.Overwrite.Tooltip"));
    wOverwriteFile.addSelectionListener(lsButtonChanged);
    FormData fdOverwriteFile = new FormData();
    fdOverwriteFile.left = new FormAttachment(middle, 0);
    fdOverwriteFile.top = new FormAttachment(wlOverwriteFile, 0, SWT.CENTER);
    wOverwriteFile.setLayoutData(fdOverwriteFile);
    lastControl = wlOverwriteFile;

    // Add filename to result
    Label wlAddResult = new Label(shell, SWT.RIGHT);
    wlAddResult.setText(BaseMessages.getString(PKG, "BinaryFileOutputDialog.AddResult.Label"));
    PropsUi.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment(0, 0);
    fdlAddResult.right = new FormAttachment(middle, -margin);
    fdlAddResult.top = new FormAttachment(lastControl, margin);
    wlAddResult.setLayoutData(fdlAddResult);

    wAddResult = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wAddResult);
    wAddResult.setToolTipText(
        BaseMessages.getString(PKG, "BinaryFileOutputDialog.AddResult.Tooltip"));
    wAddResult.addSelectionListener(lsButtonChanged);
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment(middle, 0);
    fdAddResult.top = new FormAttachment(wlAddResult, 0, SWT.CENTER);
    wAddResult.setLayoutData(fdAddResult);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private FocusListener fieldFocusListener() {
    return new FocusListener() {
      @Override
      public void focusLost(FocusEvent e) {
        // Do nothing
      }

      @Override
      public void focusGained(FocusEvent e) {
        Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
        shell.setCursor(busy);
        getFields();
        shell.setCursor(null);
        busy.dispose();
      }
    };
  }

  public void getData() {
    if (input.getBinaryField() != null) {
      wBinaryField.setText(input.getBinaryField());
    }
    if (input.getFilenameField() != null) {
      wFilenameField.setText(input.getFilenameField());
    }
    wCreateParentFolder.setSelection(input.isCreateParentFolder());
    wOverwriteFile.setSelection(input.isOverwriteFile());
    wAddResult.setSelection(input.isAddResultFilenames());
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
    input.setBinaryField(wBinaryField.getText());
    input.setFilenameField(wFilenameField.getText());
    input.setCreateParentFolder(wCreateParentFolder.getSelection());
    input.setOverwriteFile(wOverwriteFile.getSelection());
    input.setAddResultFilenames(wAddResult.getSelection());
    transformName = wTransformName.getText();
    dispose();
  }

  private void getFields() {
    if (gotPreviousFields) {
      return;
    }
    try {
      String binaryValue = wBinaryField.getText();
      String filenameValue = wFilenameField.getText();
      wBinaryField.removeAll();
      wFilenameField.removeAll();
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        String[] fields = r.getFieldNames();
        wBinaryField.setItems(fields);
        wFilenameField.setItems(fields);
      }
      if (binaryValue != null) {
        wBinaryField.setText(binaryValue);
      }
      if (filenameValue != null) {
        wFilenameField.setText(filenameValue);
      }
      gotPreviousFields = true;
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "BinaryFileOutputDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "BinaryFileOutputDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }
}
