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
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class GenerateCsvDialog extends BaseTransformDialog implements ITransformDialog {

  private static Class<?> PKG = GenerateCsvMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wTransformName;

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
      Object inputMetadata,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) inputMetadata, pipelineMeta, transformName);
    input = (GenerateCsvMeta) inputMetadata;

    metadataProvider = HopGui.getInstance().getMetadataProvider();
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout shellLayout = new FormLayout();
    shell.setLayout(shellLayout);
    shell.setText("Neo4j Generate CSVs");

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    ScrolledComposite wScrolledComposite =
        new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    FormLayout scFormLayout = new FormLayout();
    wScrolledComposite.setLayout(scFormLayout);
    FormData fdSComposite = new FormData();
    fdSComposite.left = new FormAttachment(0, 0);
    fdSComposite.right = new FormAttachment(100, 0);
    fdSComposite.top = new FormAttachment(0, 0);
    fdSComposite.bottom = new FormAttachment(100, 0);
    wScrolledComposite.setLayoutData(fdSComposite);

    Composite wComposite = new Composite(wScrolledComposite, SWT.NONE);
    props.setLook(wComposite);
    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment(0, 0);
    fdComposite.right = new FormAttachment(100, 0);
    fdComposite.top = new FormAttachment(0, 0);
    fdComposite.bottom = new FormAttachment(100, 0);
    wComposite.setLayoutData(fdComposite);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    wComposite.setLayout(formLayout);

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Transform name line
    //
    Label wlTransformName = new Label(wComposite, SWT.RIGHT);
    wlTransformName.setText("Transform name");
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

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
    props.setLook(wlGraphField);
    FormData fdlGraphField = new FormData();
    fdlGraphField.left = new FormAttachment(0, 0);
    fdlGraphField.right = new FormAttachment(middle, -margin);
    fdlGraphField.top = new FormAttachment(lastControl, 2 * margin);
    wlGraphField.setLayoutData(fdlGraphField);
    wGraphField = new CCombo(wComposite, SWT.FLAT | SWT.BORDER);
    wGraphField.setItems(fieldnames);
    props.setLook(wGraphField);
    FormData fdGraphField = new FormData();
    fdGraphField.left = new FormAttachment(middle, 0);
    fdGraphField.right = new FormAttachment(100, 0);
    fdGraphField.top = new FormAttachment(wlGraphField, 0, SWT.CENTER);
    wGraphField.setLayoutData(fdGraphField);
    lastControl = wGraphField;

    // The base folder to run the command from
    //
    Label wlBaseFolder = new Label(wComposite, SWT.RIGHT);
    wlBaseFolder.setText("Base folder (below import/ folder) ");
    props.setLook(wlBaseFolder);
    FormData fdlBaseFolder = new FormData();
    fdlBaseFolder.left = new FormAttachment(0, 0);
    fdlBaseFolder.right = new FormAttachment(middle, -margin);
    fdlBaseFolder.top = new FormAttachment(lastControl, 2 * margin);
    wlBaseFolder.setLayoutData(fdlBaseFolder);
    wBaseFolder = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBaseFolder);
    wBaseFolder.addModifyListener(lsMod);
    FormData fdBaseFolder = new FormData();
    fdBaseFolder.left = new FormAttachment(middle, 0);
    fdBaseFolder.right = new FormAttachment(100, 0);
    fdBaseFolder.top = new FormAttachment(wlBaseFolder, 0, SWT.CENTER);
    wBaseFolder.setLayoutData(fdBaseFolder);
    lastControl = wBaseFolder;

    Label wlFilesPrefix = new Label(wComposite, SWT.RIGHT);
    wlFilesPrefix.setText("CSV files prefix ");
    props.setLook(wlFilesPrefix);
    FormData fdlFilesPrefix = new FormData();
    fdlFilesPrefix.left = new FormAttachment(0, 0);
    fdlFilesPrefix.right = new FormAttachment(middle, -margin);
    fdlFilesPrefix.top = new FormAttachment(lastControl, 2 * margin);
    wlFilesPrefix.setLayoutData(fdlFilesPrefix);
    wFilesPrefix = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilesPrefix);
    wFilesPrefix.addModifyListener(lsMod);
    FormData fdFilesPrefix = new FormData();
    fdFilesPrefix.left = new FormAttachment(middle, 0);
    fdFilesPrefix.right = new FormAttachment(100, 0);
    fdFilesPrefix.top = new FormAttachment(wlFilesPrefix, 0, SWT.CENTER);
    wFilesPrefix.setLayoutData(fdFilesPrefix);
    lastControl = wFilesPrefix;

    Label wlStrategy = new Label(wComposite, SWT.RIGHT);
    wlStrategy.setText("Node/Relationships Uniqueness strategy ");
    props.setLook(wlStrategy);
    FormData fdlStrategy = new FormData();
    fdlStrategy.left = new FormAttachment(0, 0);
    fdlStrategy.right = new FormAttachment(middle, -margin);
    fdlStrategy.top = new FormAttachment(lastControl, 2 * margin);
    wlStrategy.setLayoutData(fdlStrategy);
    wStrategy = new CCombo(wComposite, SWT.FLAT | SWT.BORDER);
    wStrategy.setItems(UniquenessStrategy.getNames());
    props.setLook(wStrategy);
    FormData fdStrategy = new FormData();
    fdStrategy.left = new FormAttachment(middle, 0);
    fdStrategy.right = new FormAttachment(100, 0);
    fdStrategy.top = new FormAttachment(wlStrategy, 0, SWT.CENTER);
    wStrategy.setLayoutData(fdStrategy);
    lastControl = wStrategy;

    Label wlFilenameField = new Label(wComposite, SWT.RIGHT);
    wlFilenameField.setText("Filename field) ");
    props.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, 0);
    fdlFilenameField.right = new FormAttachment(middle, -margin);
    fdlFilenameField.top = new FormAttachment(lastControl, 2 * margin);
    wlFilenameField.setLayoutData(fdlFilenameField);
    wFilenameField = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilenameField);
    wFilenameField.addModifyListener(lsMod);
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment(middle, 0);
    fdFilenameField.right = new FormAttachment(100, 0);
    fdFilenameField.top = new FormAttachment(wlFilenameField, 0, SWT.CENTER);
    wFilenameField.setLayoutData(fdFilenameField);
    lastControl = wFilenameField;

    Label wlFileTypeField = new Label(wComposite, SWT.RIGHT);
    wlFileTypeField.setText("File type field ");
    props.setLook(wlFileTypeField);
    FormData fdlFileTypeField = new FormData();
    fdlFileTypeField.left = new FormAttachment(0, 0);
    fdlFileTypeField.right = new FormAttachment(middle, -margin);
    fdlFileTypeField.top = new FormAttachment(lastControl, 2 * margin);
    wlFileTypeField.setLayoutData(fdlFileTypeField);
    wFileTypeField = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFileTypeField);
    wFileTypeField.addModifyListener(lsMod);
    FormData fdFileTypeField = new FormData();
    fdFileTypeField.left = new FormAttachment(middle, 0);
    fdFileTypeField.right = new FormAttachment(100, 0);
    fdFileTypeField.top = new FormAttachment(wlFileTypeField, 0, SWT.CENTER);
    wFileTypeField.setLayoutData(fdFileTypeField);
    lastControl = wFileTypeField;

    // Some buttons
    wOk = new Button(wComposite, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(wComposite, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    // Position the buttons at the bottom of the dialog.
    //
    setButtonPositions(new Button[] {wOk, wCancel}, margin, lastControl);

    wComposite.pack();
    Rectangle bounds = wComposite.getBounds();

    wScrolledComposite.setContent(wComposite);

    wScrolledComposite.setExpandHorizontal(true);
    wScrolledComposite.setExpandVertical(true);
    wScrolledComposite.setMinWidth(bounds.width);
    wScrolledComposite.setMinHeight(bounds.height);

    // Add listeners
    //
    wCancel.addListener(SWT.Selection, e -> cancel());
    wOk.addListener(SWT.Selection, e -> ok());

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  public void getData() {

    wTransformName.setText(Const.NVL(transformName, ""));
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
