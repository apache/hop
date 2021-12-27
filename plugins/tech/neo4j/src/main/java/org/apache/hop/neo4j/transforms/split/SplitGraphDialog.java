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

package org.apache.hop.neo4j.transforms.split;

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

public class SplitGraphDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG =
      SplitGraphMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wTransformName;

  private CCombo wGraphField;
  private TextVar wTypeField;
  private TextVar wIdField;
  private TextVar wPropertySetField;

  private SplitGraphMeta input;

  public SplitGraphDialog(
      Shell parent,
      IVariables variables,
      Object inputMetadata,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) inputMetadata, pipelineMeta, transformName);
    input = (SplitGraphMeta) inputMetadata;

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
    shell.setText("Neo4j SplitGraph");

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

    // Graph input field
    //
    Label wlGraphField = new Label(wComposite, SWT.RIGHT);
    wlGraphField.setText("Graph field ");
    props.setLook(wlGraphField);
    FormData fdlGraphField = new FormData();
    fdlGraphField.left = new FormAttachment(0, 0);
    fdlGraphField.right = new FormAttachment(middle, -margin);
    fdlGraphField.top = new FormAttachment(lastControl, 2 * margin);
    wlGraphField.setLayoutData(fdlGraphField);
    wGraphField = new CCombo(wComposite, SWT.CHECK | SWT.BORDER);
    wGraphField.setItems(fieldnames);
    props.setLook(wGraphField);
    FormData fdGraphField = new FormData();
    fdGraphField.left = new FormAttachment(middle, 0);
    fdGraphField.right = new FormAttachment(100, 0);
    fdGraphField.top = new FormAttachment(wlGraphField, 0, SWT.CENTER);
    wGraphField.setLayoutData(fdGraphField);
    lastControl = wGraphField;

    // Type output field
    //
    Label wlTypeField = new Label(wComposite, SWT.RIGHT);
    wlTypeField.setText("Type output field (Node/Relationship) ");
    props.setLook(wlTypeField);
    FormData fdlTypeField = new FormData();
    fdlTypeField.left = new FormAttachment(0, 0);
    fdlTypeField.right = new FormAttachment(middle, -margin);
    fdlTypeField.top = new FormAttachment(lastControl, 2 * margin);
    wlTypeField.setLayoutData(fdlTypeField);
    wTypeField = new TextVar(variables, wComposite, SWT.CHECK | SWT.BORDER);
    props.setLook(wTypeField);
    FormData fdTypeField = new FormData();
    fdTypeField.left = new FormAttachment(middle, 0);
    fdTypeField.right = new FormAttachment(100, 0);
    fdTypeField.top = new FormAttachment(wlTypeField, 0, SWT.CENTER);
    wTypeField.setLayoutData(fdTypeField);
    lastControl = wTypeField;

    // The ID of the node or relationship
    //
    Label wlIdField = new Label(wComposite, SWT.RIGHT);
    wlIdField.setText("ID output field ");
    props.setLook(wlIdField);
    FormData fdlIdField = new FormData();
    fdlIdField.left = new FormAttachment(0, 0);
    fdlIdField.right = new FormAttachment(middle, -margin);
    fdlIdField.top = new FormAttachment(lastControl, 2 * margin);
    wlIdField.setLayoutData(fdlIdField);
    wIdField = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wIdField);
    wIdField.addModifyListener(lsMod);
    FormData fdIdField = new FormData();
    fdIdField.left = new FormAttachment(middle, 0);
    fdIdField.right = new FormAttachment(100, 0);
    fdIdField.top = new FormAttachment(wlIdField, 0, SWT.CENTER);
    wIdField.setLayoutData(fdIdField);
    lastControl = wIdField;

    // The property set (type/name of node, relationship, ...)
    //
    Label wlPropertySetField = new Label(wComposite, SWT.RIGHT);
    wlPropertySetField.setText("Property set output field ");
    props.setLook(wlPropertySetField);
    FormData fdlPropertySetField = new FormData();
    fdlPropertySetField.left = new FormAttachment(0, 0);
    fdlPropertySetField.right = new FormAttachment(middle, -margin);
    fdlPropertySetField.top = new FormAttachment(lastControl, 2 * margin);
    wlPropertySetField.setLayoutData(fdlPropertySetField);
    wPropertySetField = new TextVar(variables, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPropertySetField);
    wPropertySetField.addModifyListener(lsMod);
    FormData fdPropertySetField = new FormData();
    fdPropertySetField.left = new FormAttachment(middle, 0);
    fdPropertySetField.right = new FormAttachment(100, 0);
    fdPropertySetField.top = new FormAttachment(wlPropertySetField, 0, SWT.CENTER);
    wPropertySetField.setLayoutData(fdPropertySetField);
    lastControl = wPropertySetField;

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
    wGraphField.setText(Const.NVL(input.getGraphField(), ""));
    wTypeField.setText(Const.NVL(input.getTypeField(), ""));
    wIdField.setText(Const.NVL(input.getIdField(), ""));
    wPropertySetField.setText(Const.NVL(input.getPropertySetField(), ""));
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText(); // return value
    getInfo(input);
    dispose();
  }

  private void getInfo(SplitGraphMeta meta) {
    meta.setGraphField(wGraphField.getText());
    meta.setTypeField(wTypeField.getText());
    meta.setIdField(wIdField.getText());
    meta.setPropertySetField(wPropertySetField.getText());
  }
}
