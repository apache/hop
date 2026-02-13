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

package org.apache.hop.pipeline.transforms.closure;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class ClosureGeneratorDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ClosureGeneratorDialog.class;

  private Button wRootZero;

  private CCombo wParent;

  private CCombo wChild;

  private Text wDistance;

  private final ClosureGeneratorMeta input;

  private IRowMeta inputFields;

  public ClosureGeneratorDialog(
      Shell parent,
      IVariables variables,
      ClosureGeneratorMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "ClosureGeneratorDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    ScrolledComposite sc = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(sc);
    FormData fdSc = new FormData();
    fdSc.left = new FormAttachment(0, 0);
    fdSc.top = new FormAttachment(wSpacer, 0);
    fdSc.right = new FormAttachment(100, 0);
    fdSc.bottom = new FormAttachment(wOk, -margin);
    sc.setLayoutData(fdSc);
    sc.setLayout(new FillLayout());

    Composite wContent = new Composite(sc, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    // Parent ...
    //
    Label wlParent = new Label(wContent, SWT.RIGHT);
    wlParent.setText(BaseMessages.getString(PKG, "ClosureGeneratorDialog.ParentField.Label"));
    PropsUi.setLook(wlParent);
    FormData fdlParent = new FormData();
    fdlParent.left = new FormAttachment(0, 0);
    fdlParent.right = new FormAttachment(middle, -margin);
    fdlParent.top = new FormAttachment(0, margin);
    wlParent.setLayoutData(fdlParent);

    wParent = new CCombo(wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wParent);
    wParent.addModifyListener(lsMod);
    FormData fdParent = new FormData();
    fdParent.left = new FormAttachment(middle, 0);
    fdParent.right = new FormAttachment(100, 0);
    fdParent.top = new FormAttachment(0, margin);
    wParent.setLayoutData(fdParent);

    // Child ...
    //
    Label wlChild = new Label(wContent, SWT.RIGHT);
    wlChild.setText(BaseMessages.getString(PKG, "ClosureGeneratorDialog.ChildField.Label"));
    PropsUi.setLook(wlChild);
    FormData fdlChild = new FormData();
    fdlChild.left = new FormAttachment(0, 0);
    fdlChild.right = new FormAttachment(middle, -margin);
    fdlChild.top = new FormAttachment(wParent, margin);
    wlChild.setLayoutData(fdlChild);

    wChild = new CCombo(wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wChild);
    wChild.addModifyListener(lsMod);
    FormData fdChild = new FormData();
    fdChild.left = new FormAttachment(middle, 0);
    fdChild.right = new FormAttachment(100, 0);
    fdChild.top = new FormAttachment(wParent, margin);
    wChild.setLayoutData(fdChild);

    // Distance ...
    //
    Label wlDistance = new Label(wContent, SWT.RIGHT);
    wlDistance.setText(BaseMessages.getString(PKG, "ClosureGeneratorDialog.DistanceField.Label"));
    PropsUi.setLook(wlDistance);
    FormData fdlDistance = new FormData();
    fdlDistance.left = new FormAttachment(0, 0);
    fdlDistance.right = new FormAttachment(middle, -margin);
    fdlDistance.top = new FormAttachment(wChild, margin);
    wlDistance.setLayoutData(fdlDistance);

    wDistance = new Text(wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDistance);
    wDistance.addModifyListener(lsMod);
    FormData fdDistance = new FormData();
    fdDistance.left = new FormAttachment(middle, 0);
    fdDistance.right = new FormAttachment(100, 0);
    fdDistance.top = new FormAttachment(wChild, margin);
    wDistance.setLayoutData(fdDistance);

    // Root is zero(Integer)?
    //
    Label wlRootZero = new Label(wContent, SWT.RIGHT);
    wlRootZero.setText(BaseMessages.getString(PKG, "ClosureGeneratorDialog.RootZero.Label"));
    PropsUi.setLook(wlRootZero);
    FormData fdlRootZero = new FormData();
    fdlRootZero.left = new FormAttachment(0, 0);
    fdlRootZero.right = new FormAttachment(middle, -margin);
    fdlRootZero.top = new FormAttachment(wDistance, margin);
    wlRootZero.setLayoutData(fdlRootZero);

    wRootZero = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wRootZero);
    FormData fdRootZero = new FormData();
    fdRootZero.left = new FormAttachment(middle, 0);
    fdRootZero.right = new FormAttachment(100, 0);
    fdRootZero.top = new FormAttachment(wlRootZero, 0, SWT.CENTER);
    wRootZero.setLayoutData(fdRootZero);
    wRootZero.addSelectionListener(new ComponentSelectionListener(input));

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    sc.setContent(wContent);
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);
    sc.setMinWidth(bounds.width);
    sc.setMinHeight(bounds.height);

    // Search the fields in the background
    //
    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              inputFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "ClosureGeneratorDialog.Log.UnableToFindInput"));
            }
          }
        };
    new Thread(runnable).start();

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected void setComboBoxes() {
    shell
        .getDisplay()
        .syncExec(
            () -> {
              if (inputFields != null) {
                String[] fieldNames = inputFields.getFieldNames();
                wParent.setItems(fieldNames);
                wChild.setItems(fieldNames);
              }
            });
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getParentIdFieldName() != null) {
      wParent.setText(input.getParentIdFieldName());
    }
    if (input.getChildIdFieldName() != null) {
      wChild.setText(input.getChildIdFieldName());
    }
    if (input.getDistanceFieldName() != null) {
      wDistance.setText(input.getDistanceFieldName());
    }
    wRootZero.setSelection(input.isRootIdZero());
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getInfo(ClosureGeneratorMeta meta) {
    meta.setParentIdFieldName(wParent.getText());
    meta.setChildIdFieldName(wChild.getText());
    meta.setDistanceFieldName(wDistance.getText());
    meta.setRootIdZero(wRootZero.getSelection());
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value
    getInfo(input);

    dispose();
  }
}
