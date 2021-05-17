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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.List;

public class FilterDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = FilterMeta.class; // For Translator

  private ComboVar wTrueTo;

  private ComboVar wFalseTo;

  private TextVar wCondition;

  private final FilterMeta input;

  public FilterDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
    super(parent, variables, (BaseTransformMeta) in, tr, sname);
    input = (FilterMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "FilterDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Some buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "FilterDialog.TransformName.Label"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Send 'True' data to...
    Label wlTrueTo = new Label(shell, SWT.RIGHT);
    wlTrueTo.setText(BaseMessages.getString(PKG, "FilterDialog.SendTrueTo.Label"));
    props.setLook(wlTrueTo);
    FormData fdlTrueTo = new FormData();
    fdlTrueTo.left = new FormAttachment(0, 0);
    fdlTrueTo.right = new FormAttachment(middle, -margin);
    fdlTrueTo.top = new FormAttachment(wTransformName, margin);
    wlTrueTo.setLayoutData(fdlTrueTo);
    wTrueTo = new ComboVar(variables, shell, SWT.BORDER);
    props.setLook(wTrueTo);

    TransformMeta transforminfo = pipelineMeta.findTransform(transformName);
    if (transforminfo != null) {
      List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms(transforminfo);
      for (TransformMeta transformMeta : nextTransforms) {
        wTrueTo.add(transformMeta.getName());
      }
    }

    FormData fdTrueTo = new FormData();
    fdTrueTo.left = new FormAttachment(middle, 0);
    fdTrueTo.top = new FormAttachment(wTransformName, margin);
    fdTrueTo.right = new FormAttachment(100, 0);
    wTrueTo.setLayoutData(fdTrueTo);

    // Send 'False' data to...
    Label wlFalseTo = new Label(shell, SWT.RIGHT);
    wlFalseTo.setText(BaseMessages.getString(PKG, "FilterDialog.SendFalseTo.Label"));
    props.setLook(wlFalseTo);
    FormData fdlFalseTo = new FormData();
    fdlFalseTo.left = new FormAttachment(0, 0);
    fdlFalseTo.right = new FormAttachment(middle, -margin);
    fdlFalseTo.top = new FormAttachment(wTrueTo, margin);
    wlFalseTo.setLayoutData(fdlFalseTo);
    wFalseTo = new ComboVar(variables, shell, SWT.BORDER);
    props.setLook(wFalseTo);

    transforminfo = pipelineMeta.findTransform(transformName);
    if (transforminfo != null) {
      List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms(transforminfo);
      for (TransformMeta transformMeta : nextTransforms) {
        wFalseTo.add(transformMeta.getName());
      }
    }

    FormData fdFalseFrom = new FormData();
    fdFalseFrom.left = new FormAttachment(middle, 0);
    fdFalseFrom.top = new FormAttachment(wTrueTo, margin);
    fdFalseFrom.right = new FormAttachment(100, 0);
    wFalseTo.setLayoutData(fdFalseFrom);

    Label wlCondition = new Label(shell, SWT.NONE);
    wlCondition.setText(BaseMessages.getString(PKG, "FilterDialog.Condition.Label"));
    props.setLook(wlCondition);
    FormData fdlCondition = new FormData();
    fdlCondition.left = new FormAttachment(0, 0);
    fdlCondition.top = new FormAttachment(wFalseTo, margin);
    wlCondition.setLayoutData(fdlCondition);

    IRowMeta inputfields = null;
    try {
      inputfields = pipelineMeta.getPrevTransformFields(variables, transformName);
    } catch (HopException ke) {
      inputfields = new RowMeta();
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "FilterDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "FilterDialog.FailedToGetFields.DialogMessage"),
          ke);
    }

    wCondition =
        new TextVar(variables, shell, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, null);

    FormData fdCondition = new FormData();
    fdCondition.left = new FormAttachment(0, 0);
    fdCondition.top = new FormAttachment(wlCondition, margin);
    fdCondition.right = new FormAttachment(100, 0);
    fdCondition.bottom = new FormAttachment(wOk, -2 * margin);
    wCondition.setLayoutData(fdCondition);

    // Add listeners
    wTransformName.addListener(SWT.DefaultSelection, e -> ok());
    wTrueTo.addListener(SWT.DefaultSelection, e -> ok());
    wFalseTo.addListener(SWT.DefaultSelection, e -> ok());

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged(backupChanged);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wTrueTo.setText(Const.NVL(input.getTrueTransformName(), ""));
    wFalseTo.setText(Const.NVL(input.getFalseTransformName(), ""));
    wCondition.setText(Const.NVL(input.getCondition(), ""));

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText();
    input.setTrueTransformName(wTrueTo.getText());
    input.setFalseTransformName(wFalseTo.getText());
    input.setCondition(wCondition.getText());
    dispose();
  }
}
