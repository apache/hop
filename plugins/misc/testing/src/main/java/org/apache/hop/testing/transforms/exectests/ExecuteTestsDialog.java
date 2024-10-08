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

package org.apache.hop.testing.transforms.exectests;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class ExecuteTestsDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ExecuteTestsDialog.class;

  private ExecuteTestsMeta input;

  private Combo wTestNameInputField;
  private Combo wTypeToExecute;
  private TextVar wPipelineNameField;
  private TextVar wUnitTestNameField;
  private TextVar wDataSetNameField;
  private TextVar wTransformNameField;
  private TextVar wErrorField;
  private TextVar wCommentField;

  private boolean hasPreviousTransforms;

  public ExecuteTestsDialog(
      Shell parent,
      IVariables variables,
      ExecuteTestsMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {

    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ExecuteTestsDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    String[] inputFieldNames = new String[] {};
    hasPreviousTransforms = false;
    try {
      TransformMeta[] prevTransforms = pipelineMeta.getPrevTransforms(transformMeta);
      if (prevTransforms.length > 0) {
        IRowMeta prevTransformFields =
            pipelineMeta.getPrevTransformFields(variables, transformMeta);
        inputFieldNames = prevTransformFields.getFieldNames();
        hasPreviousTransforms = true;
      }
    } catch (HopException e) {
      log.logError(
          "Couldn't get input fields for transform "
              + transformMeta.getName()
              + " : "
              + e.getMessage());
    }

    // Transform name...
    //
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "ExecuteTestsDialog.TransformName.Label"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    // Optional test name input field
    //
    Label wlTestNameInputField = new Label(shell, SWT.RIGHT);
    wlTestNameInputField.setText(
        BaseMessages.getString(PKG, "ExecuteTestsDialog.TestNameInputField.Label"));
    PropsUi.setLook(wlTestNameInputField);
    FormData fdlTestNameInputField = new FormData();
    fdlTestNameInputField.left = new FormAttachment(0, 0);
    fdlTestNameInputField.right = new FormAttachment(middle, -margin);
    fdlTestNameInputField.top = new FormAttachment(lastControl, margin);
    wlTestNameInputField.setLayoutData(fdlTestNameInputField);
    wTestNameInputField = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTestNameInputField.setItems(inputFieldNames);
    PropsUi.setLook(wTestNameInputField);
    FormData fdTestNameInputField = new FormData();
    fdTestNameInputField.left = new FormAttachment(middle, 0);
    fdTestNameInputField.top = new FormAttachment(lastControl, margin);
    fdTestNameInputField.right = new FormAttachment(100, 0);
    wTestNameInputField.setLayoutData(fdTestNameInputField);
    wTestNameInputField.addModifyListener(e -> enableFields());
    lastControl = wTestNameInputField;

    // Type to execute
    //
    Label wlTypeToExecute = new Label(shell, SWT.RIGHT);
    wlTypeToExecute.setText(BaseMessages.getString(PKG, "ExecuteTestsDialog.TypeToExecute.Label"));
    PropsUi.setLook(wlTypeToExecute);
    FormData fdlTypeToExecute = new FormData();
    fdlTypeToExecute.left = new FormAttachment(0, 0);
    fdlTypeToExecute.right = new FormAttachment(middle, -margin);
    fdlTypeToExecute.top = new FormAttachment(lastControl, margin);
    wlTypeToExecute.setLayoutData(fdlTypeToExecute);
    wTypeToExecute = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTypeToExecute.setItems(DataSetConst.getTestTypeDescriptions());
    PropsUi.setLook(wTypeToExecute);
    FormData fdTypeToExecute = new FormData();
    fdTypeToExecute.left = new FormAttachment(middle, 0);
    fdTypeToExecute.top = new FormAttachment(lastControl, margin);
    fdTypeToExecute.right = new FormAttachment(100, 0);
    wTypeToExecute.setLayoutData(fdTypeToExecute);
    lastControl = wTypeToExecute;

    // Transformation name field
    //
    Label wlPipelineNameField = new Label(shell, SWT.RIGHT);
    wlPipelineNameField.setText(
        BaseMessages.getString(PKG, "ExecuteTestsDialog.PipelineNameField.Label"));
    PropsUi.setLook(wlPipelineNameField);
    FormData fdlPipelineNameField = new FormData();
    fdlPipelineNameField.left = new FormAttachment(0, 0);
    fdlPipelineNameField.right = new FormAttachment(middle, -margin);
    fdlPipelineNameField.top = new FormAttachment(lastControl, margin);
    wlPipelineNameField.setLayoutData(fdlPipelineNameField);
    wPipelineNameField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPipelineNameField);
    FormData fdPipelineNameField = new FormData();
    fdPipelineNameField.left = new FormAttachment(middle, 0);
    fdPipelineNameField.top = new FormAttachment(lastControl, margin);
    fdPipelineNameField.right = new FormAttachment(100, 0);
    wPipelineNameField.setLayoutData(fdPipelineNameField);
    lastControl = wPipelineNameField;

    // Unit test name field
    //
    Label wlUnitTestNameField = new Label(shell, SWT.RIGHT);
    wlUnitTestNameField.setText(
        BaseMessages.getString(PKG, "ExecuteTestsDialog.UnitTestNameField.Label"));
    PropsUi.setLook(wlUnitTestNameField);
    FormData fdlUnitTestNameField = new FormData();
    fdlUnitTestNameField.left = new FormAttachment(0, 0);
    fdlUnitTestNameField.right = new FormAttachment(middle, -margin);
    fdlUnitTestNameField.top = new FormAttachment(lastControl, margin);
    wlUnitTestNameField.setLayoutData(fdlUnitTestNameField);
    wUnitTestNameField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUnitTestNameField);
    FormData fdUnitTestNameField = new FormData();
    fdUnitTestNameField.left = new FormAttachment(middle, 0);
    fdUnitTestNameField.top = new FormAttachment(lastControl, margin);
    fdUnitTestNameField.right = new FormAttachment(100, 0);
    wUnitTestNameField.setLayoutData(fdUnitTestNameField);
    lastControl = wUnitTestNameField;

    // Data Set Name field
    //
    Label wlDataSetNameField = new Label(shell, SWT.RIGHT);
    wlDataSetNameField.setText(
        BaseMessages.getString(PKG, "ExecuteTestsDialog.DataSetNameField.Label"));
    PropsUi.setLook(wlDataSetNameField);
    FormData fdlDataSetNameField = new FormData();
    fdlDataSetNameField.left = new FormAttachment(0, 0);
    fdlDataSetNameField.right = new FormAttachment(middle, -margin);
    fdlDataSetNameField.top = new FormAttachment(lastControl, margin);
    wlDataSetNameField.setLayoutData(fdlDataSetNameField);
    wDataSetNameField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDataSetNameField);
    FormData fdDataSetNameField = new FormData();
    fdDataSetNameField.left = new FormAttachment(middle, 0);
    fdDataSetNameField.top = new FormAttachment(lastControl, margin);
    fdDataSetNameField.right = new FormAttachment(100, 0);
    wDataSetNameField.setLayoutData(fdDataSetNameField);
    lastControl = wDataSetNameField;

    // Transform Name field
    //
    Label wlTransformNameField = new Label(shell, SWT.RIGHT);
    wlTransformNameField.setText(
        BaseMessages.getString(PKG, "ExecuteTestsDialog.TransformNameField.Label"));
    PropsUi.setLook(wlTransformNameField);
    FormData fdlTransformNameField = new FormData();
    fdlTransformNameField.left = new FormAttachment(0, 0);
    fdlTransformNameField.right = new FormAttachment(middle, -margin);
    fdlTransformNameField.top = new FormAttachment(lastControl, margin);
    wlTransformNameField.setLayoutData(fdlTransformNameField);
    wTransformNameField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTransformNameField);
    FormData fdTransformNameField = new FormData();
    fdTransformNameField.left = new FormAttachment(middle, 0);
    fdTransformNameField.top = new FormAttachment(lastControl, margin);
    fdTransformNameField.right = new FormAttachment(100, 0);
    wTransformNameField.setLayoutData(fdTransformNameField);
    lastControl = wTransformNameField;

    // Error field
    //
    Label wlErrorField = new Label(shell, SWT.RIGHT);
    wlErrorField.setText(BaseMessages.getString(PKG, "ExecuteTestsDialog.ErrorField.Label"));
    PropsUi.setLook(wlErrorField);
    FormData fdlErrorField = new FormData();
    fdlErrorField.left = new FormAttachment(0, 0);
    fdlErrorField.right = new FormAttachment(middle, -margin);
    fdlErrorField.top = new FormAttachment(lastControl, margin);
    wlErrorField.setLayoutData(fdlErrorField);
    wErrorField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wErrorField);
    FormData fdErrorField = new FormData();
    fdErrorField.left = new FormAttachment(middle, 0);
    fdErrorField.top = new FormAttachment(lastControl, margin);
    fdErrorField.right = new FormAttachment(100, 0);
    wErrorField.setLayoutData(fdErrorField);
    lastControl = wErrorField;

    // Comment field
    //
    Label wlCommentField = new Label(shell, SWT.RIGHT);
    wlCommentField.setText(BaseMessages.getString(PKG, "ExecuteTestsDialog.CommentField.Label"));
    PropsUi.setLook(wlCommentField);
    FormData fdlCommentField = new FormData();
    fdlCommentField.left = new FormAttachment(0, 0);
    fdlCommentField.right = new FormAttachment(middle, -margin);
    fdlCommentField.top = new FormAttachment(lastControl, margin);
    wlCommentField.setLayoutData(fdlCommentField);
    wCommentField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCommentField);
    FormData fdCommentField = new FormData();
    fdCommentField.left = new FormAttachment(middle, 0);
    fdCommentField.top = new FormAttachment(lastControl, margin);
    fdCommentField.right = new FormAttachment(100, 0);
    wCommentField.setLayoutData(fdCommentField);
    lastControl = wCommentField;

    // Buttons go at the very bottom
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, margin, lastControl);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void enableFields() {
    boolean previous =
        hasPreviousTransforms && StringUtils.isNotEmpty(wTestNameInputField.getText());
    wTestNameInputField.setEnabled(hasPreviousTransforms);
    wTypeToExecute.setEnabled(!previous);
  }

  private void getData() {

    wTestNameInputField.setText(Const.NVL(input.getTestNameInputField(), ""));
    wTypeToExecute.setText(DataSetConst.getTestTypeDescription(input.getTypeToExecute()));
    wPipelineNameField.setText(Const.NVL(input.getPipelineNameField(), ""));
    wUnitTestNameField.setText(Const.NVL(input.getUnitTestNameField(), ""));
    wDataSetNameField.setText(Const.NVL(input.getDataSetNameField(), ""));
    wTransformNameField.setText(Const.NVL(input.getTransformNameField(), ""));
    wErrorField.setText(Const.NVL(input.getErrorField(), ""));
    wCommentField.setText(Const.NVL(input.getCommentField(), ""));

    enableFields();

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (StringUtil.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    input.setChanged();

    input.setTestNameInputField(wTestNameInputField.getText());
    input.setTypeToExecute(DataSetConst.getTestTypeForDescription(wTypeToExecute.getText()));
    input.setPipelineNameField(wPipelineNameField.getText());
    input.setUnitTestNameField(wUnitTestNameField.getText());
    input.setDataSetNameField(wDataSetNameField.getText());
    input.setTransformNameField(wTransformNameField.getText());
    input.setErrorField(wErrorField.getText());
    input.setCommentField(wCommentField.getText());

    dispose();
  }
}
