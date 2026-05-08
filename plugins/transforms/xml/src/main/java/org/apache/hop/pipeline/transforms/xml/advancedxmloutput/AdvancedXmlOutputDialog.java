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

package org.apache.hop.pipeline.transforms.xml.advancedxmloutput;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/**
 * Phase-1 stub dialog for the Advanced XML Output transform. Exposes only the filename and encoding
 * so the transform is editable in HopGui while the full tree designer (Phase 2) is implemented. The
 * XML tree itself is configured programmatically or by editing the metadata directly until the
 * designer ships.
 */
public class AdvancedXmlOutputDialog extends BaseTransformDialog {

  private static final Class<?> PKG = AdvancedXmlOutputMeta.class;

  private final AdvancedXmlOutputMeta input;

  private TextVar wFilename;
  private TextVar wExtension;
  private TextVar wEncoding;
  private Button wAddToResult;

  public AdvancedXmlOutputDialog(
      Shell parent,
      IVariables variables,
      AdvancedXmlOutputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    Composite wMain = new Composite(shell, SWT.NONE);
    PropsUi.setLook(wMain);
    FormLayout layout = new FormLayout();
    layout.marginWidth = 3;
    layout.marginHeight = 3;
    wMain.setLayout(layout);

    // Filename
    Label wlFilename = new Label(wMain, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(0, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);
    wFilename = new TextVar(variables, wMain, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(0, margin);
    fdFilename.right = new FormAttachment(100, 0);
    wFilename.setLayoutData(fdFilename);

    // Extension
    Label wlExt = new Label(wMain, SWT.RIGHT);
    wlExt.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Extension.Label"));
    PropsUi.setLook(wlExt);
    FormData fdlExt = new FormData();
    fdlExt.left = new FormAttachment(0, 0);
    fdlExt.top = new FormAttachment(wFilename, margin);
    fdlExt.right = new FormAttachment(middle, -margin);
    wlExt.setLayoutData(fdlExt);
    wExtension = new TextVar(variables, wMain, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wExtension);
    wExtension.addModifyListener(lsMod);
    FormData fdExt = new FormData();
    fdExt.left = new FormAttachment(middle, 0);
    fdExt.top = new FormAttachment(wFilename, margin);
    fdExt.right = new FormAttachment(100, 0);
    wExtension.setLayoutData(fdExt);

    // Encoding
    Label wlEnc = new Label(wMain, SWT.RIGHT);
    wlEnc.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Encoding.Label"));
    PropsUi.setLook(wlEnc);
    FormData fdlEnc = new FormData();
    fdlEnc.left = new FormAttachment(0, 0);
    fdlEnc.top = new FormAttachment(wExtension, margin);
    fdlEnc.right = new FormAttachment(middle, -margin);
    wlEnc.setLayoutData(fdlEnc);
    wEncoding = new TextVar(variables, wMain, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEncoding);
    wEncoding.addModifyListener(lsMod);
    FormData fdEnc = new FormData();
    fdEnc.left = new FormAttachment(middle, 0);
    fdEnc.top = new FormAttachment(wExtension, margin);
    fdEnc.right = new FormAttachment(100, 0);
    wEncoding.setLayoutData(fdEnc);

    // Add to result
    Label wlAddToResult = new Label(wMain, SWT.RIGHT);
    wlAddToResult.setText(
        BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.AddFileToResult.Label"));
    PropsUi.setLook(wlAddToResult);
    FormData fdlA = new FormData();
    fdlA.left = new FormAttachment(0, 0);
    fdlA.top = new FormAttachment(wEncoding, margin);
    fdlA.right = new FormAttachment(middle, -margin);
    wlAddToResult.setLayoutData(fdlA);
    wAddToResult = new Button(wMain, SWT.CHECK);
    PropsUi.setLook(wAddToResult);
    FormData fdA = new FormData();
    fdA.left = new FormAttachment(middle, 0);
    fdA.top = new FormAttachment(wlAddToResult, 0, SWT.CENTER);
    fdA.right = new FormAttachment(100, 0);
    wAddToResult.setLayoutData(fdA);

    // Notice
    Label wlNotice = new Label(wMain, SWT.WRAP);
    wlNotice.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Notice.Label"));
    PropsUi.setLook(wlNotice);
    FormData fdN = new FormData();
    fdN.left = new FormAttachment(0, 0);
    fdN.right = new FormAttachment(100, 0);
    fdN.top = new FormAttachment(wAddToResult, margin * 3);
    wlNotice.setLayoutData(fdN);

    FormData fdMain = new FormData();
    fdMain.left = new FormAttachment(0, 0);
    fdMain.top = new FormAttachment(wSpacer, margin);
    fdMain.right = new FormAttachment(100, 0);
    fdMain.bottom = new FormAttachment(100, -50);
    wMain.setLayoutData(fdMain);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void getData() {
    wFilename.setText(Const.NVL(input.getFileSupport().getFileName(), ""));
    wExtension.setText(Const.NVL(input.getFileSupport().getExtension(), ""));
    wEncoding.setText(Const.NVL(input.getEncoding(), Const.UTF_8));
    wAddToResult.setSelection(input.getFileSupport().isAddToResultFilenames());
  }

  private void cancel() {
    transformName = null;
    input.setChanged(backupChanged);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText();
    input.getFileSupport().setFileName(wFilename.getText());
    input.getFileSupport().setExtension(wExtension.getText());
    input.setEncoding(wEncoding.getText());
    input.getFileSupport().setAddToResultFilenames(wAddToResult.getSelection());
    dispose();
  }
}
