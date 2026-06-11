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

package org.apache.hop.pipeline.transforms.addsnowflakeid;

import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Text;

/** add snowflakeId dialog */
public class AddSnowflakeIdDialog extends BaseTransformDialog {
  private static final Class<?> PKG = AddSnowflakeIdDialog.class;

  private Text wValueName;
  private Spinner wDataCenterId;
  private Spinner wMachineId;

  private final AddSnowflakeIdMeta input;

  public AddSnowflakeIdDialog(
      Shell parent,
      IVariables variables,
      AddSnowflakeIdMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "AddSnowflakeIdDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // Value name line
    Label wlValueName = new Label(shell, SWT.RIGHT);
    wlValueName.setText(BaseMessages.getString(PKG, "AddSnowflakeIdDialog.ValueName.Label"));
    PropsUi.setLook(wlValueName);
    wlValueName.setLayoutData(
        new FormDataBuilder().left(0, 0).right(middle, -margin).top(wSpacer, margin).result());
    wValueName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wValueName.setText("");
    PropsUi.setLook(wValueName);
    wValueName.addModifyListener(lsMod);
    FormData fdValueName = new FormData();
    fdValueName.left = new FormAttachment(middle, 5);
    fdValueName.top = new FormAttachment(wSpacer, margin);
    fdValueName.right = new FormAttachment(100, 0);
    wValueName.setLayoutData(fdValueName);

    // data center no
    Label lDataCenterId = new Label(shell, SWT.RIGHT);
    lDataCenterId.setText(BaseMessages.getString(PKG, "AddSnowflakeIdDialog.DataCenterNo.Label"));
    lDataCenterId.setLayoutData(
        new FormDataBuilder().left(0, 0).right(middle, -margin).top(wlValueName, margin).result());
    PropsUi.setLook(lDataCenterId);
    wDataCenterId = createSpinner();
    PropsUi.setLook(wDataCenterId);
    wDataCenterId.addModifyListener(lsMod);
    wDataCenterId.setLayoutData(
        new FormDataBuilder().left(middle, 5).right(100, 0).top(wValueName, margin).result());

    // machine no
    Label lMachine = new Label(shell, SWT.RIGHT);
    lMachine.setText(BaseMessages.getString(PKG, "AddSnowflakeIdDialog.MachineNo.Label"));
    PropsUi.setLook(lMachine);
    lMachine.setLayoutData(
        new FormDataBuilder()
            .left(0, 0)
            .right(middle, -margin)
            .top(wDataCenterId, margin)
            .result());

    wMachineId = createSpinner();
    PropsUi.setLook(wMachineId);
    wMachineId.addModifyListener(lsMod);
    wMachineId.setLayoutData(
        new FormDataBuilder().left(middle, 5).right(100, 0).top(wDataCenterId, margin).result());

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    logDebug(BaseMessages.getString(PKG, "AddSnowflakeIdDialog.Log.GettingKeyInfo"));

    if (input.getValueName() != null) {
      wValueName.setText(input.getValueName());
    }

    wDataCenterId.setSelection(ensureRange0To32(input.getDataCenterId()));
    wMachineId.setSelection(ensureRange0To32(input.getMachineId()));
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

    transformName = wTransformName.getText();
    input.setDataCenterId(wDataCenterId.getSelection());
    input.setMachineId(wMachineId.getSelection());
    dispose();
  }

  /**
   * handler value [0,32)
   *
   * @param value init value
   * @return real value
   */
  private Integer ensureRange0To32(Integer value) {
    if (value == null || value < 0) {
      return 0;
    }

    if (value > 31) {
      return 31;
    }

    return value;
  }

  /**
   * create spinner control
   *
   * @return Spinner
   */
  private Spinner createSpinner() {
    Spinner spinner = new Spinner(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    spinner.setMinimum(0);
    spinner.setMaximum(31);
    spinner.setIncrement(1);
    return spinner;
  }
}
