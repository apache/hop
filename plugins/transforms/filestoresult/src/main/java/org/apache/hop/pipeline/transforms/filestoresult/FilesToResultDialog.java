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

package org.apache.hop.pipeline.transforms.filestoresult;

import org.apache.hop.core.ResultFile;
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
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Shell;

public class FilesToResultDialog extends BaseTransformDialog {
  private static final Class<?> PKG = FilesToResultMeta.class;

  private CCombo wFilenameField;

  private List wTypes;

  private final FilesToResultMeta input;

  public FilesToResultDialog(
      Shell parent,
      IVariables variables,
      FilesToResultMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "FilesToResultDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    Shell parent = getParent();
    Display display = parent.getDisplay();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // The rest...

    // FilenameField line
    Label wlFilenameField = new Label(shell, SWT.RIGHT);
    wlFilenameField.setText(BaseMessages.getString(PKG, "FilesToResultDialog.FilenameField.Label"));
    PropsUi.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, 0);
    fdlFilenameField.top = new FormAttachment(wSpacer, margin);
    fdlFilenameField.right = new FormAttachment(middle, -margin);
    wlFilenameField.setLayoutData(fdlFilenameField);

    wFilenameField = new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wFilenameField.setToolTipText(
        BaseMessages.getString(PKG, "FilesToResultDialog.FilenameField.Tooltip"));
    PropsUi.setLook(wFilenameField);
    wFilenameField.addModifyListener(lsMod);
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment(middle, 0);
    fdFilenameField.top = new FormAttachment(wSpacer, margin);
    fdFilenameField.right = new FormAttachment(100, 0);
    wFilenameField.setLayoutData(fdFilenameField);

    /*
     * Get the field names from the previous transforms, in the background though
     */
    Runnable runnable =
        () -> {
          try {
            IRowMeta inputfields = pipelineMeta.getPrevTransformFields(variables, transformName);
            if (inputfields != null) {
              for (int i = 0; i < inputfields.size(); i++) {
                wFilenameField.add(inputfields.getValueMeta(i).getName());
              }
            }
          } catch (Exception ke) {
            new ErrorDialog(
                shell,
                BaseMessages.getString(PKG, "FilesToResultDialog.FailedToGetFields.DialogTitle"),
                BaseMessages.getString(PKG, "FilesToResultDialog.FailedToGetFields.DialogMessage"),
                ke);
          }
        };
    display.asyncExec(runnable);

    // Include Files?
    Label wlTypes = new Label(shell, SWT.RIGHT);
    wlTypes.setText(BaseMessages.getString(PKG, "FilesToResultDialog.TypeOfFile.Label"));
    PropsUi.setLook(wlTypes);
    FormData fdlTypes = new FormData();
    fdlTypes.left = new FormAttachment(0, 0);
    fdlTypes.top = new FormAttachment(wFilenameField, margin);
    fdlTypes.right = new FormAttachment(middle, -margin);
    wlTypes.setLayoutData(fdlTypes);
    wTypes = new List(shell, SWT.SINGLE | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    wTypes.setToolTipText(BaseMessages.getString(PKG, "FilesToResultDialog.TypeOfFile.Tooltip"));
    PropsUi.setLook(wTypes);
    FormData fdTypes = new FormData();
    fdTypes.left = new FormAttachment(middle, 0);
    fdTypes.top = new FormAttachment(wFilenameField, margin);
    fdTypes.bottom = new FormAttachment(wOk, -margin * 3);
    fdTypes.right = new FormAttachment(100, 0);
    wTypes.setLayoutData(fdTypes);
    for (int i = 0; i < ResultFile.getAllTypeDesc().length; i++) {
      wTypes.add(ResultFile.getAllTypeDesc()[i]);
    }

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    ResultFile.FileType fileType = input.getFileType();

    wTypes.select(fileType == null ? ResultFile.FileType.GENERAL.getType() : fileType.getType());
    if (input.getFilenameField() != null) {
      wFilenameField.setText(input.getFilenameField());
    }
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

    transformName = wTransformName.getText(); // return value

    input.setFilenameField(wFilenameField.getText());
    if (wTypes.getSelectionIndex() >= 0) {
      input.setFileType(ResultFile.FileType.lookupDescription(wTypes.getSelection()[0]));
    } else {
      input.setFileType(ResultFile.FileType.GENERAL);
    }

    dispose();
  }
}
