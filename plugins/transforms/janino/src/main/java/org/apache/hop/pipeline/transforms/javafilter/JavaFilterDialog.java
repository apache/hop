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

package org.apache.hop.pipeline.transforms.javafilter;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.janino.editor.FormulaEditor;
import org.apache.hop.pipeline.transforms.janino.function.ExpressionLibrary;
import org.apache.hop.pipeline.transforms.util.JaninoCheckerUtil;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.JavaStyledTextComp;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class JavaFilterDialog extends BaseTransformDialog {
  private static final Class<?> PKG = JavaFilterMeta.class;

  private CCombo wTrueTo;
  private CCombo wFalseTo;
  private TextComposite wCondition;

  private final JavaFilterMeta input;

  private final List<String> inputFields = new ArrayList<>();

  public JavaFilterDialog(
      Shell parent, IVariables variables, JavaFilterMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);

    // The order here is important... currentMeta is looked at for changes
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "JavaFilterDialog.DialogTitle"));

    buildButtonBar()
        .ok(e -> ok())
        .custom(BaseMessages.getString(PKG, "JavaFilterDialog.Editor.Button"), e -> editorDialog())
        .custom(BaseMessages.getString(PKG, "JavaFilterDialog.Test.Button"), e -> testCondition())
        .cancel(e -> cancel())
        .build();

    lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    Control lastControl = wSpacer;

    // The transforms this one can send its rows to.
    //
    List<String> nextTransformNames = new ArrayList<>();
    TransformMeta transformInfo = pipelineMeta.findTransform(transformName);
    if (transformInfo != null) {
      for (TransformMeta nextTransform : pipelineMeta.findNextTransforms(transformInfo)) {
        nextTransformNames.add(nextTransform.getName());
      }
    }

    // Send 'True' data to...
    //
    Label wlTrueTo = new Label(shell, SWT.RIGHT);
    wlTrueTo.setText(BaseMessages.getString(PKG, "JavaFilterDialog.SendTrueTo.Label"));
    PropsUi.setLook(wlTrueTo);
    FormData fdlTrueTo = new FormData();
    fdlTrueTo.left = new FormAttachment(0, 0);
    fdlTrueTo.right = new FormAttachment(middle, -margin);
    fdlTrueTo.top = new FormAttachment(lastControl, margin);
    wlTrueTo.setLayoutData(fdlTrueTo);

    wTrueTo = new CCombo(shell, SWT.BORDER);
    PropsUi.setLook(wTrueTo);
    nextTransformNames.forEach(name -> wTrueTo.add(name));
    wTrueTo.addModifyListener(lsMod);
    FormData fdTrueTo = new FormData();
    fdTrueTo.left = new FormAttachment(middle, 0);
    fdTrueTo.top = new FormAttachment(lastControl, margin);
    fdTrueTo.right = new FormAttachment(100, 0);
    wTrueTo.setLayoutData(fdTrueTo);
    lastControl = wTrueTo;

    // Send 'False' data to...
    //
    Label wlFalseTo = new Label(shell, SWT.RIGHT);
    wlFalseTo.setText(BaseMessages.getString(PKG, "JavaFilterDialog.SendFalseTo.Label"));
    PropsUi.setLook(wlFalseTo);
    FormData fdlFalseTo = new FormData();
    fdlFalseTo.left = new FormAttachment(0, 0);
    fdlFalseTo.right = new FormAttachment(middle, -margin);
    fdlFalseTo.top = new FormAttachment(lastControl, margin);
    wlFalseTo.setLayoutData(fdlFalseTo);

    wFalseTo = new CCombo(shell, SWT.BORDER);
    PropsUi.setLook(wFalseTo);
    nextTransformNames.forEach(name -> wFalseTo.add(name));
    wFalseTo.addModifyListener(lsMod);
    FormData fdFalseTo = new FormData();
    fdFalseTo.left = new FormAttachment(middle, 0);
    fdFalseTo.top = new FormAttachment(lastControl, margin);
    fdFalseTo.right = new FormAttachment(100, 0);
    wFalseTo.setLayoutData(fdFalseTo);
    lastControl = wFalseTo;

    // The condition, it takes up the rest of the dialog.
    //
    Label wlCondition = new Label(shell, SWT.LEFT);
    wlCondition.setText(BaseMessages.getString(PKG, "JavaFIlterDialog.Condition.Label"));
    PropsUi.setLook(wlCondition);
    FormData fdlCondition = new FormData();
    fdlCondition.left = new FormAttachment(0, 0);
    fdlCondition.right = new FormAttachment(100, 0);
    fdlCondition.top = new FormAttachment(lastControl, margin);
    wlCondition.setLayoutData(fdlCondition);

    // Hop Web runs on RWT, which has no StyledText: there the editor is the plain variant with the
    // same style type, like every other code editor in Hop.
    //
    if (EnvironmentUtils.getInstance().isWeb()) {
      wCondition =
          new StyledTextComp(
              variables,
              shell,
              SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL,
              TextComposite.STYLE_TYPE_JAVA);
    } else {
      wCondition =
          new JavaStyledTextComp(
              variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
      wCondition.addLineStyleListener();
    }
    wCondition.setToolTipText(BaseMessages.getString(PKG, "JavaFilterDialog.Condition.Tooltip"));
    PropsUi.setLook(wCondition, Props.WIDGET_STYLE_FIXED);
    wCondition.addModifyListener(lsMod);
    FormData fdCondition = new FormData();
    fdCondition.left = new FormAttachment(0, 0);
    fdCondition.right = new FormAttachment(100, 0);
    fdCondition.top = new FormAttachment(wlCondition, margin);
    fdCondition.bottom = new FormAttachment(wOk, -margin);
    wCondition.setLayoutData(fdCondition);

    //
    // Search the fields in the background
    //
    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.add(row.getValueMeta(i).getName());
              }
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "JaninoDialog.Log.UnableToFindInput"));
            }
          }
        };
    BackgroundThreadFacade.start(runnable);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data currentMeta to the dialog fields. */
  public void getData() {

    wTrueTo.setText(Const.NVL(input.getTrueTransform(), ""));
    wFalseTo.setText(Const.NVL(input.getFalseTransform(), ""));
    wCondition.setText(Const.NVL(input.getCondition(), ""));
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

    // Check if code contains content that is not allowed
    JaninoCheckerUtil janinoCheckerUtil = new JaninoCheckerUtil();
    List<String> codeCheck = janinoCheckerUtil.checkCode(wCondition.getText());
    if (!codeCheck.isEmpty()) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText("Invalid Code");
      mb.setMessage("Script contains code that is not allowed : " + codeCheck);
      mb.open();
      return;
    }

    transformName = wTransformName.getText(); // return value

    input.setCondition(wCondition.getText());
    input.setTrueTransform(Const.NVL(wTrueTo.getText(), null));
    input.setFalseTransform(Const.NVL(wFalseTo.getText(), null));

    dispose();
  }

  private void editorDialog() {
    try {
      if (!shell.isDisposed()) {
        FormulaEditor libFormulaEditor =
            new FormulaEditor(
                variables,
                shell,
                SWT.APPLICATION_MODAL | SWT.SHEET,
                Const.NVL(wCondition.getText(), ""),
                inputFields,
                ExpressionLibrary.getFunctionsAndConditionExamples());
        String formula = libFormulaEditor.open();
        if (formula != null) {
          wCondition.setText(formula);
        }
      }
    } catch (Exception ex) {
      new ErrorDialog(shell, "Error", "There was an unexpected error in the formula editor", ex);
    }
  }

  /**
   * Compiles the condition against the fields of the incoming stream and reports the outcome, so a
   * condition that doesn't compile is found here instead of on the first row of a run.
   */
  private void testCondition() {
    try {
      IRowMeta rowMeta = pipelineMeta.getPrevTransformFields(variables, transformName);
      JavaFilterCondition condition =
          JavaFilterCondition.validate(rowMeta, variables.resolve(wCondition.getText()));

      List<String> boundFields = condition.getBoundFieldNames();
      String message =
          boundFields.isEmpty()
              ? BaseMessages.getString(PKG, "JavaFilterDialog.Test.Success.NoFields")
              : BaseMessages.getString(
                  PKG, "JavaFilterDialog.Test.Success.Fields", String.join(", ", boundFields));

      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      mb.setText(BaseMessages.getString(PKG, "JavaFilterDialog.Test.Success.Title"));
      mb.setMessage(message);
      mb.open();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "JavaFilterDialog.Test.Failure.Title"),
          BaseMessages.getString(PKG, "JavaFilterDialog.Test.Failure.Message"),
          e);
    }
  }
}
