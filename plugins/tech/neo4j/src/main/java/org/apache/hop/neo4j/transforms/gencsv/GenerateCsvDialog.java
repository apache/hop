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

package org.apache.hop.neo4j.transforms.gencsv;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class GenerateCsvDialog extends BaseTransformDialog {

  private static final Class<?> PKG =
      GenerateCsvMeta.class; // for i18n purposes, needed by Translator2!!

  private CCombo wGraphField;
  private TextVar wBaseFolder;
  private CCombo wStrategy;
  private TextVar wFilesPrefix;
  private TextVar wFilenameField;
  private TextVar wFileTypeField;

  private GenerateCsvMeta input;

  public GenerateCsvDialog(
      Shell parent,
      IVariables variables,
      GenerateCsvMeta inputMetadata,
      PipelineMeta pipelineMeta) {
    super(parent, variables, inputMetadata, pipelineMeta);
    input = inputMetadata;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "GenerateCsvMeta.name"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    ScrolledComposite wScrolledComposite =
        new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    FormLayout scFormLayout = new FormLayout();
    wScrolledComposite.setLayout(scFormLayout);
    FormData fdSComposite = new FormData();
    fdSComposite.left = new FormAttachment(0, 0);
    fdSComposite.right = new FormAttachment(100, 0);
    fdSComposite.top = new FormAttachment(wSpacer, 0);
    fdSComposite.bottom = new FormAttachment(wOk, -margin);
    wScrolledComposite.setLayoutData(fdSComposite);

    Composite wComposite = new Composite(wScrolledComposite, SWT.NONE);
    PropsUi.setLook(wComposite);
    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment(0, 0);
    fdComposite.right = new FormAttachment(100, 0);
    fdComposite.top = new FormAttachment(0, 0);
    fdComposite.bottom = new FormAttachment(100, 0);
    wComposite.setLayoutData(fdComposite);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    wComposite.setLayout(formLayout);

    String[] fieldnames = new String[] {};
    try {
      fieldnames = pipelineMeta.getPrevTransformFields(variables, transformMeta).getFieldNames();
    } catch (HopTransformException e) {
      log.logError("error getting input field names: ", e);
    }

    // Graph field
    //
    Label wlGraphField = new Label(wComposite, SWT.RIGHT);
    wlGraphField.setText("Graph field ");
    PropsUi.setLook(wlGraphField);
    FormData fdlGraphField = new FormData();
    fdlGraphField.left = new FormAttachment(0, 0);
    fdlGraphField.right = new FormAttachment(middle, -margin);
    fdlGraphField.top = new FormAttachment(0, margin);
    wlGraphField.setLayoutData(fdlGraphField);
    wGraphField = new CCombo(wComposite, SWT.FLAT | SWT.BORDER);
    wGraphField.setItems(fieldnames);
    PropsUi.setLook(wGraphField);
    FormData fdGraphField = new FormData();
    fdGraphField.left = new FormAttachment(middle, 0);
    fdGraphField.right = new FormAttachment(100, 0);
    fdGraphField.top = new FormAttachment(wlGraphField, 0, SWT.CENTER);
    wGraphField.setLayoutData(fdGraphField);
    Control lastControl = wGraphField;

    // The base folder to run the command from
    //
    Label wlBaseFolder = new Label(wComposite, SWT.RIGHT);
    wlBaseFolder.setText("Base folder (below import/ folder) ");
    PropsUi.setLook(wlBaseFolder);
    FormData fdlBaseFolder = new FormData();
    fdlBaseFolder.left = new FormAttachment(0, 0);
    fdlBaseFolder.right = new FormAttachment(middle, -margin);
    fdlBaseFolder.top = new FormAttachment(lastControl, margin);
    wlBaseFolder.setLayoutData(fdlBaseFolder);
    wBaseFolder = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBaseFolder);
    wBaseFolder.addModifyListener(lsMod);
    FormData fdBaseFolder = new FormData();
    fdBaseFolder.left = new FormAttachment(middle, 0);
    fdBaseFolder.right = new FormAttachment(100, 0);
    fdBaseFolder.top = new FormAttachment(wlBaseFolder, 0, SWT.CENTER);
    wBaseFolder.setLayoutData(fdBaseFolder);
    lastControl = wBaseFolder;

    Label wlFilesPrefix = new Label(wComposite, SWT.RIGHT);
    wlFilesPrefix.setText("CSV files prefix ");
    PropsUi.setLook(wlFilesPrefix);
    FormData fdlFilesPrefix = new FormData();
    fdlFilesPrefix.left = new FormAttachment(0, 0);
    fdlFilesPrefix.right = new FormAttachment(middle, -margin);
    fdlFilesPrefix.top = new FormAttachment(lastControl, margin);
    wlFilesPrefix.setLayoutData(fdlFilesPrefix);
    wFilesPrefix = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilesPrefix);
    wFilesPrefix.addModifyListener(lsMod);
    FormData fdFilesPrefix = new FormData();
    fdFilesPrefix.left = new FormAttachment(middle, 0);
    fdFilesPrefix.right = new FormAttachment(100, 0);
    fdFilesPrefix.top = new FormAttachment(wlFilesPrefix, 0, SWT.CENTER);
    wFilesPrefix.setLayoutData(fdFilesPrefix);
    lastControl = wFilesPrefix;

    Label wlStrategy = new Label(wComposite, SWT.RIGHT);
    wlStrategy.setText("Node/Relationships Uniqueness strategy ");
    PropsUi.setLook(wlStrategy);
    FormData fdlStrategy = new FormData();
    fdlStrategy.left = new FormAttachment(0, 0);
    fdlStrategy.right = new FormAttachment(middle, -margin);
    fdlStrategy.top = new FormAttachment(lastControl, margin);
    wlStrategy.setLayoutData(fdlStrategy);
    wStrategy = new CCombo(wComposite, SWT.FLAT | SWT.BORDER);
    wStrategy.setItems(UniquenessStrategy.getNames());
    PropsUi.setLook(wStrategy);
    FormData fdStrategy = new FormData();
    fdStrategy.left = new FormAttachment(middle, 0);
    fdStrategy.right = new FormAttachment(100, 0);
    fdStrategy.top = new FormAttachment(wlStrategy, 0, SWT.CENTER);
    wStrategy.setLayoutData(fdStrategy);
    lastControl = wStrategy;

    Label wlFilenameField = new Label(wComposite, SWT.RIGHT);
    wlFilenameField.setText("Filename field) ");
    PropsUi.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, 0);
    fdlFilenameField.right = new FormAttachment(middle, -margin);
    fdlFilenameField.top = new FormAttachment(lastControl, margin);
    wlFilenameField.setLayoutData(fdlFilenameField);
    wFilenameField = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilenameField);
    wFilenameField.addModifyListener(lsMod);
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment(middle, 0);
    fdFilenameField.right = new FormAttachment(100, 0);
    fdFilenameField.top = new FormAttachment(wlFilenameField, 0, SWT.CENTER);
    wFilenameField.setLayoutData(fdFilenameField);
    lastControl = wFilenameField;

    Label wlFileTypeField = new Label(wComposite, SWT.RIGHT);
    wlFileTypeField.setText("File type field ");
    PropsUi.setLook(wlFileTypeField);
    FormData fdlFileTypeField = new FormData();
    fdlFileTypeField.left = new FormAttachment(0, 0);
    fdlFileTypeField.right = new FormAttachment(middle, -margin);
    fdlFileTypeField.top = new FormAttachment(lastControl, margin);
    wlFileTypeField.setLayoutData(fdlFileTypeField);
    wFileTypeField = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFileTypeField);
    wFileTypeField.addModifyListener(lsMod);
    FormData fdFileTypeField = new FormData();
    fdFileTypeField.left = new FormAttachment(middle, 0);
    fdFileTypeField.right = new FormAttachment(100, 0);
    fdFileTypeField.top = new FormAttachment(wlFileTypeField, 0, SWT.CENTER);
    wFileTypeField.setLayoutData(fdFileTypeField);
    lastControl = wFileTypeField;

    wComposite.pack();
    Rectangle bounds = wComposite.getBounds();

    wScrolledComposite.setContent(wComposite);

    wScrolledComposite.setExpandHorizontal(true);
    wScrolledComposite.setExpandVertical(true);
    wScrolledComposite.setMinWidth(bounds.width);
    wScrolledComposite.setMinHeight(bounds.height);

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  public void getData() {
    wGraphField.setText(Const.NVL(input.getGraphFieldName(), ""));
    wBaseFolder.setText(Const.NVL(input.getBaseFolder(), ""));
    if (input.getUniquenessStrategy() != null) {
      int idx =
          Const.indexOfString(input.getUniquenessStrategy().name(), UniquenessStrategy.getNames());
      wStrategy.select(idx);
    }
    wFilesPrefix.setText(Const.NVL(input.getFilesPrefix(), ""));
    wFilenameField.setText(Const.NVL(input.getFilenameField(), ""));
    wFileTypeField.setText(Const.NVL(input.getFileTypeField(), ""));
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText(); // return value
    getInfo(input);
    dispose();
  }

  private void getInfo(GenerateCsvMeta meta) {
    meta.setGraphFieldName(wGraphField.getText());
    meta.setBaseFolder(wBaseFolder.getText());
    meta.setUniquenessStrategy(UniquenessStrategy.getStrategyFromName(wStrategy.getText()));
    meta.setFilesPrefix(wFilesPrefix.getText());
    meta.setFilenameField(wFilenameField.getText());
    meta.setFileTypeField(wFileTypeField.getText());
  }
}
