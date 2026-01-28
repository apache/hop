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

package org.apache.hop.beam.transforms.io;

import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class BeamInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamInput.class;
  private final BeamInputMeta input;

  private boolean getpreviousFields = false;

  private TextVar wInputLocation;
  private MetaSelectionLine<FileDefinition> wFileDefinition;

  public BeamInputDialog(
      Shell parent, IVariables variables, BeamInputMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BeamInputDialog.DialogTitle"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(scrolledComposite);
    FormData fdScrolledComposite = new FormData();
    fdScrolledComposite.left = new FormAttachment(0, 0);
    fdScrolledComposite.top = new FormAttachment(wSpacer, 0);
    fdScrolledComposite.right = new FormAttachment(100, 0);
    fdScrolledComposite.bottom = new FormAttachment(wOk, -margin);
    scrolledComposite.setLayoutData(fdScrolledComposite);
    scrolledComposite.setLayout(new FillLayout());

    Composite wContent = new Composite(scrolledComposite, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    changed = input.hasChanged();

    Control lastControl = null;

    Label wlInputLocation = new Label(wContent, SWT.RIGHT);
    wlInputLocation.setText(BaseMessages.getString(PKG, "BeamInputDialog.InputLocation"));
    PropsUi.setLook(wlInputLocation);
    FormData fdlInputLocation = new FormData();
    fdlInputLocation.left = new FormAttachment(0, 0);
    fdlInputLocation.top = new FormAttachment(0, margin);
    fdlInputLocation.right = new FormAttachment(middle, -margin);
    wlInputLocation.setLayoutData(fdlInputLocation);
    wInputLocation = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInputLocation);
    FormData fdInputLocation = new FormData();
    fdInputLocation.left = new FormAttachment(middle, 0);
    fdInputLocation.top = new FormAttachment(wlInputLocation, 0, SWT.CENTER);
    fdInputLocation.right = new FormAttachment(100, 0);
    wInputLocation.setLayoutData(fdInputLocation);
    lastControl = wInputLocation;

    wFileDefinition =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            FileDefinition.class,
            wContent,
            SWT.NONE,
            BaseMessages.getString(PKG, "BeamInputDialog.FileDefinition"),
            BaseMessages.getString(PKG, "BeamInputDialog.FileDefinition"));
    PropsUi.setLook(wFileDefinition);
    FormData fdFileDefinition = new FormData();
    fdFileDefinition.left = new FormAttachment(0, 0);
    fdFileDefinition.top = new FormAttachment(lastControl, margin);
    fdFileDefinition.right = new FormAttachment(100, 0);
    wFileDefinition.setLayoutData(fdFileDefinition);

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    scrolledComposite.setContent(wContent);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    try {
      wFileDefinition.fillItems();
    } catch (Exception e) {
      log.logError("Error getting file definition items", e);
    }

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Populate the widgets. */
  public void getData() {
    wFileDefinition.setText(Const.NVL(input.getFileDefinitionName(), ""));
    wInputLocation.setText(Const.NVL(input.getInputLocation(), ""));
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

    getInfo(input);

    dispose();
  }

  private void getInfo(BeamInputMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setFileDefinitionName(wFileDefinition.getText());
    in.setInputLocation(wInputLocation.getText());

    input.setChanged();
  }
}
