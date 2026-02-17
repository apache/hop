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

package org.apache.hop.pipeline.transforms.append;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** Dialog for the append transform. */
public class AppendDialog extends BaseTransformDialog {
  private static final Class<?> PKG = AppendDialog.class;

  private CCombo wHeadHop;

  private CCombo wTailHop;

  private final AppendMeta input;

  public AppendDialog(
      Shell parent, IVariables variables, AppendMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "AppendDialog.Shell.Label"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "AppendDialog.TransformName.Label"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Get the previous transforms...
    String[] previousTransforms = pipelineMeta.getPrevTransformNames(transformName);

    Label wlHeadHop = new Label(shell, SWT.RIGHT);
    wlHeadHop.setText(BaseMessages.getString(PKG, "AppendDialog.HeadHop.Label"));
    PropsUi.setLook(wlHeadHop);
    FormData fdlHeadHop = new FormData();
    fdlHeadHop.left = new FormAttachment(0, 0);
    fdlHeadHop.right = new FormAttachment(middle, -margin);
    fdlHeadHop.top = new FormAttachment(wTransformName, margin);
    wlHeadHop.setLayoutData(fdlHeadHop);
    wHeadHop = new CCombo(shell, SWT.BORDER);
    PropsUi.setLook(wHeadHop);

    if (previousTransforms != null) {
      wHeadHop.setItems(previousTransforms);
    }

    wHeadHop.addModifyListener(lsMod);
    FormData fdHeadHop = new FormData();
    fdHeadHop.left = new FormAttachment(middle, 0);
    fdHeadHop.top = new FormAttachment(wTransformName, margin);
    fdHeadHop.right = new FormAttachment(100, 0);
    wHeadHop.setLayoutData(fdHeadHop);

    Label wlTailHop = new Label(shell, SWT.RIGHT);
    wlTailHop.setText(BaseMessages.getString(PKG, "AppendDialog.TailHop.Label"));
    PropsUi.setLook(wlTailHop);
    FormData fdlTailHop = new FormData();
    fdlTailHop.left = new FormAttachment(0, 0);
    fdlTailHop.right = new FormAttachment(middle, -margin);
    fdlTailHop.top = new FormAttachment(wHeadHop, margin);
    wlTailHop.setLayoutData(fdlTailHop);
    wTailHop = new CCombo(shell, SWT.BORDER);
    PropsUi.setLook(wTailHop);

    if (previousTransforms != null) {
      wTailHop.setItems(previousTransforms);
    }

    wTailHop.addModifyListener(lsMod);
    FormData fdTailHop = new FormData();
    fdTailHop.top = new FormAttachment(wHeadHop, margin);
    fdTailHop.left = new FormAttachment(middle, 0);
    fdTailHop.right = new FormAttachment(100, 0);
    wTailHop.setLayoutData(fdTailHop);

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wCancel}, margin, wTailHop);

    // Add listeners
    wCancel.addListener(SWT.Selection, e -> cancel());
    wOk.addListener(SWT.Selection, e -> ok());

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    // If both fields are empty and exactly 2 transforms are attached, auto-fill head and tail
    if (Utils.isEmpty(input.getHeadTransformName())
        && Utils.isEmpty(input.getTailTransformName())) {
      String[] prev = pipelineMeta.getPrevTransformNames(transformName);
      if (prev != null && prev.length == 2) {
        input.setHeadTransformName(prev[0]);
        input.setTailTransformName(prev[1]);
      }
    }
    // Sync from hops (rename, insert-in-the-middle) and resolve streams
    input.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());

    List<IStream> infoStreams = input.getTransformIOMeta().getInfoStreams();
    wHeadHop.setText(Const.NVL(infoStreams.get(0).getTransformName(), ""));
    wTailHop.setText(Const.NVL(infoStreams.get(1).getTransformName(), ""));

    wTransformName.selectAll();
    wTransformName.setFocus();
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

    input.setHeadTransformName(wHeadHop.getText());
    input.setTailTransformName(wTailHop.getText());

    transformName = wTransformName.getText(); // return value

    dispose();
  }
}
