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
package org.apache.hop.pipeline.transforms.plugincatalog;

import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class PluginCatalogDialog extends BaseTransformDialog {

  private static final Class<?> PKG = PluginCatalogMeta.class;

  private final PluginCatalogMeta input;

  private Button wIncludeTransforms;
  private Button wIncludeActions;
  private Button wIncludeMetadataTypes;
  private CCombo wDetailLevel;

  public PluginCatalogDialog(
      Shell parent,
      IVariables variables,
      PluginCatalogMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Control lastControl =
        createShell(BaseMessages.getString(PKG, "PluginCatalogDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    wIncludeTransforms = addCheckbox("PluginCatalog.includeTransforms", lastControl);
    lastControl = wIncludeTransforms;
    wIncludeActions = addCheckbox("PluginCatalog.includeActions", lastControl);
    lastControl = wIncludeActions;
    wIncludeMetadataTypes = addCheckbox("PluginCatalog.includeMetadataTypes", lastControl);
    lastControl = wIncludeMetadataTypes;

    Label wlDetailLevel = new Label(shell, SWT.RIGHT);
    wlDetailLevel.setText(BaseMessages.getString(PKG, "PluginCatalog.detailLevel.Label"));
    wlDetailLevel.setToolTipText(BaseMessages.getString(PKG, "PluginCatalog.detailLevel.Tooltip"));
    PropsUi.setLook(wlDetailLevel);
    FormData fdlDetail = new FormData();
    fdlDetail.left = new FormAttachment(0, 0);
    fdlDetail.right = new FormAttachment(middle, -margin);
    fdlDetail.top = new FormAttachment(lastControl, margin);
    wlDetailLevel.setLayoutData(fdlDetail);
    wDetailLevel = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wDetailLevel);
    wDetailLevel.setItems(
        new String[] {DetailLevel.PER_PLUGIN.name(), DetailLevel.PER_PROPERTY.name()});
    FormData fdDetail = new FormData();
    fdDetail.left = new FormAttachment(middle, 0);
    fdDetail.top = new FormAttachment(lastControl, margin);
    fdDetail.right = new FormAttachment(100, 0);
    wDetailLevel.setLayoutData(fdDetail);
    wDetailLevel.addModifyListener(lsMod);

    getData();
    input.setChanged(changed);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private Button addCheckbox(String labelKey, Control previous) {
    Button button = new Button(shell, SWT.CHECK);
    button.setText(BaseMessages.getString(PKG, labelKey + ".Label"));
    button.setToolTipText(BaseMessages.getString(PKG, labelKey + ".Tooltip"));
    PropsUi.setLook(button);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(previous, margin);
    fd.right = new FormAttachment(100, 0);
    button.setLayoutData(fd);
    button.addSelectionListener(
        org.eclipse.swt.events.SelectionListener.widgetSelectedAdapter(e -> input.setChanged()));
    return button;
  }

  private void getData() {
    wIncludeTransforms.setSelection(input.isIncludeTransforms());
    wIncludeActions.setSelection(input.isIncludeActions());
    wIncludeMetadataTypes.setSelection(input.isIncludeMetadataTypes());
    wDetailLevel.setText(
        input.getDetailLevel() != null
            ? input.getDetailLevel().name()
            : DetailLevel.PER_PLUGIN.name());
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
    input.setIncludeTransforms(wIncludeTransforms.getSelection());
    input.setIncludeActions(wIncludeActions.getSelection());
    input.setIncludeMetadataTypes(wIncludeMetadataTypes.getSelection());
    input.setDetailLevel(DetailLevel.fromString(wDetailLevel.getText()));
    dispose();
  }
}
