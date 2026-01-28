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

package org.apache.hop.pipeline.transforms.filelocked;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class FileLockedDialog extends BaseTransformDialog {
  private static final Class<?> PKG = FileLockedMeta.class;

  private CCombo wFileName;

  private TextVar wResult;

  private Button wAddResult;

  private final FileLockedMeta input;

  private boolean gotPreviousFields = false;

  public FileLockedDialog(
      Shell parent, IVariables variables, FileLockedMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "FileLockedDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    Control lastControl = wSpacer;

    // filename field
    Label wlFileName = new Label(shell, SWT.RIGHT);
    wlFileName.setText(BaseMessages.getString(PKG, "FileLockedDialog.FileName.Label"));
    PropsUi.setLook(wlFileName);
    FormData fdlFileName = new FormData();
    fdlFileName.left = new FormAttachment(0, 0);
    fdlFileName.right = new FormAttachment(middle, -margin);
    fdlFileName.top = new FormAttachment(lastControl, margin);
    wlFileName.setLayoutData(fdlFileName);

    wFileName = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    wFileName.setEditable(true);
    PropsUi.setLook(wFileName);
    wFileName.addModifyListener(lsMod);
    FormData fdfileName = new FormData();
    fdfileName.left = new FormAttachment(middle, 0);
    fdfileName.top = new FormAttachment(lastControl, margin);
    fdfileName.right = new FormAttachment(100, -margin);
    wFileName.setLayoutData(fdfileName);
    wFileName.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable focusLost event
          }

          @Override
          public void focusGained(FocusEvent e) {
            get();
          }
        });

    // Result fieldname ...
    Label wlResult = new Label(shell, SWT.RIGHT);
    wlResult.setText(BaseMessages.getString(PKG, "FileLockedDialog.ResultField.Label"));
    PropsUi.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment(0, 0);
    fdlResult.right = new FormAttachment(middle, -margin);
    fdlResult.top = new FormAttachment(wFileName, margin);
    wlResult.setLayoutData(fdlResult);

    wResult = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wResult.setToolTipText(BaseMessages.getString(PKG, "FileLockedDialog.ResultField.Tooltip"));
    PropsUi.setLook(wResult);
    wResult.addModifyListener(lsMod);
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment(middle, 0);
    fdResult.top = new FormAttachment(wFileName, margin);
    fdResult.right = new FormAttachment(100, 0);
    wResult.setLayoutData(fdResult);

    // Add filename to result filenames?
    Label wlAddResult = new Label(shell, SWT.RIGHT);
    wlAddResult.setText(BaseMessages.getString(PKG, "FileLockedDialog.AddResult.Label"));
    PropsUi.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment(0, 0);
    fdlAddResult.top = new FormAttachment(wResult, margin);
    fdlAddResult.right = new FormAttachment(middle, -margin);
    wlAddResult.setLayoutData(fdlAddResult);
    wAddResult = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wAddResult);
    wAddResult.setToolTipText(BaseMessages.getString(PKG, "FileLockedDialog.AddResult.Tooltip"));
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment(middle, 0);
    fdAddResult.top = new FormAttachment(wlAddResult, 0, SWT.CENTER);
    wAddResult.setLayoutData(fdAddResult);

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "FileLockedDialog.Log.GettingKeyInfo"));
    }

    if (input.getFilenamefield() != null) {
      wFileName.setText(input.getFilenamefield());
    }
    if (input.getResultfieldname() != null) {
      wResult.setText(input.getResultfieldname());
    }
    wAddResult.setSelection(input.isAddresultfilenames());
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
    input.setFilenamefield(wFileName.getText());
    input.setResultfieldname(wResult.getText());
    input.setAddresultfilenames(wAddResult.getSelection());
    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void get() {
    if (!gotPreviousFields) {
      try {
        String filefield = wFileName.getText();
        wFileName.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wFileName.setItems(r.getFieldNames());
        }
        if (filefield != null) {
          wFileName.setText(filefield);
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "FileLockedDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "FileLockedDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
      gotPreviousFields = true;
    }
  }
}
