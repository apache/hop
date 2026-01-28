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

package org.apache.hop.pipeline.transforms.filterrows;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ConditionEditor;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class FilterRowsDialog extends BaseTransformDialog {
  private static final Class<?> PKG = FilterRowsMeta.class;

  private CCombo wTrueTo;

  private CCombo wFalseTo;

  private ConditionEditor wCondition;

  private final FilterRowsMeta input;
  private final Condition condition;

  private Condition backupCondition;

  public FilterRowsDialog(
      Shell parent, IVariables variables, FilterRowsMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
    condition = new Condition(input.getCondition());
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "FilterRowsDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();
    backupCondition = new Condition(condition);

    Control lastControl = wSpacer;

    // Send 'True' data to...
    Label wlTrueTo = new Label(shell, SWT.RIGHT);
    wlTrueTo.setText(BaseMessages.getString(PKG, "FilterRowsDialog.SendTrueTo.Label"));
    PropsUi.setLook(wlTrueTo);
    FormData fdlTrueTo = new FormData();
    fdlTrueTo.left = new FormAttachment(0, 0);
    fdlTrueTo.right = new FormAttachment(middle, -margin);
    fdlTrueTo.top = new FormAttachment(lastControl, margin);
    wlTrueTo.setLayoutData(fdlTrueTo);
    wTrueTo = new CCombo(shell, SWT.BORDER);
    PropsUi.setLook(wTrueTo);

    TransformMeta transforminfo = pipelineMeta.findTransform(transformName);
    if (transforminfo != null) {
      List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms(transforminfo);
      for (TransformMeta transformMeta : nextTransforms) {
        wTrueTo.add(transformMeta.getName());
      }
    }

    wTrueTo.addModifyListener(lsMod);
    FormData fdTrueTo = new FormData();
    fdTrueTo.left = new FormAttachment(middle, 0);
    fdTrueTo.top = new FormAttachment(lastControl, margin);
    fdTrueTo.right = new FormAttachment(100, 0);
    wTrueTo.setLayoutData(fdTrueTo);

    // Send 'False' data to...
    Label wlFalseTo = new Label(shell, SWT.RIGHT);
    wlFalseTo.setText(BaseMessages.getString(PKG, "FilterRowsDialog.SendFalseTo.Label"));
    PropsUi.setLook(wlFalseTo);
    FormData fdlFalseTo = new FormData();
    fdlFalseTo.left = new FormAttachment(0, 0);
    fdlFalseTo.right = new FormAttachment(middle, -margin);
    fdlFalseTo.top = new FormAttachment(wTrueTo, margin);
    wlFalseTo.setLayoutData(fdlFalseTo);
    wFalseTo = new CCombo(shell, SWT.BORDER);
    PropsUi.setLook(wFalseTo);

    transforminfo = pipelineMeta.findTransform(transformName);
    if (transforminfo != null) {
      List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms(transforminfo);
      for (TransformMeta transformMeta : nextTransforms) {
        wFalseTo.add(transformMeta.getName());
      }
    }

    wFalseTo.addModifyListener(lsMod);
    FormData fdFalseFrom = new FormData();
    fdFalseFrom.left = new FormAttachment(middle, 0);
    fdFalseFrom.top = new FormAttachment(wTrueTo, margin);
    fdFalseFrom.right = new FormAttachment(100, 0);
    wFalseTo.setLayoutData(fdFalseFrom);

    Label wlCondition = new Label(shell, SWT.NONE);
    wlCondition.setText(BaseMessages.getString(PKG, "FilterRowsDialog.Condition.Label"));
    PropsUi.setLook(wlCondition);
    FormData fdlCondition = new FormData();
    fdlCondition.left = new FormAttachment(0, 0);
    fdlCondition.top = new FormAttachment(wFalseTo, margin);
    wlCondition.setLayoutData(fdlCondition);

    IRowMeta inputfields;
    try {
      inputfields = pipelineMeta.getPrevTransformFields(variables, transformName);
    } catch (HopException ke) {
      inputfields = new RowMeta();
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "FilterRowsDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "FilterRowsDialog.FailedToGetFields.DialogMessage"),
          ke);
    }

    wCondition = new ConditionEditor(shell, SWT.BORDER, condition, inputfields);

    FormData fdCondition = new FormData();
    fdCondition.left = new FormAttachment(0, 0);
    fdCondition.top = new FormAttachment(wlCondition, margin);
    fdCondition.right = new FormAttachment(100, 0);
    fdCondition.bottom = new FormAttachment(100, -50);
    wCondition.setLayoutData(fdCondition);
    wCondition.addModifyListener(lsMod);

    getData();
    input.setChanged(backupChanged);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    List<IStream> targetStreams = input.getTransformIOMeta().getTargetStreams();

    wTrueTo.setText(Const.NVL(targetStreams.get(0).getTransformName(), ""));
    wFalseTo.setText(Const.NVL(targetStreams.get(1).getTransformName(), ""));
  }

  private void cancel() {
    transformName = null;
    input.setChanged(backupChanged);
    // Also change the condition back to what it was...
    input.setCondition(backupCondition);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    if (wCondition.getLevel() > 0) {
      wCondition.goUp();
    } else {
      String trueTransformName = wTrueTo.getText();
      if (StringUtils.isEmpty(trueTransformName)) {
        trueTransformName = null;
      }
      String falseTransformName = wFalseTo.getText();
      if (StringUtils.isEmpty(falseTransformName)) {
        falseTransformName = null;
      }

      input.setTrueTransformName(trueTransformName);
      input.setFalseTransformName(falseTransformName);
      input.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());

      transformName = wTransformName.getText(); // return value
      input.setCondition(condition);

      dispose();
    }
  }
}
