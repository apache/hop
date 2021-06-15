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

package org.apache.hop.pipeline.transforms.dropbox.output;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.Arrays;
import java.util.List;

public class DropboxOutputDialog extends BaseTransformDialog implements ITransformDialog {

  private static Class<?> PKG =
      DropboxOutputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private static final int MARGIN_SIZE = 15;
  private static final int ELEMENT_SPACING = Const.MARGIN;

  private DropboxOutputMeta meta;

  private Text wTransformNameField;

  private CCombo wSuccessfulToField;

  private CCombo wFailedToField;

  private CCombo wAccessTokenField;

  private CCombo wSourceFilesComboBox;

  private CCombo wTargetFilesComboBox;

  private boolean changed;

  public DropboxOutputDialog(
      Shell parent,
      IVariables variables,
      Object in,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, transformName);
    meta = (DropboxOutputMeta) in;
  }

  public String open() {
    // Set up window
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, meta);
    int middle = props.getMiddlePct();

    // Listeners
    ModifyListener lsMod = e -> meta.setChanged();
    changed = meta.hasChanged();

    // 15 pixel margins
    FormLayout formLayout = new FormLayout();
    formLayout.marginLeft = MARGIN_SIZE;
    formLayout.marginHeight = MARGIN_SIZE;
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "DropboxOutputDialog.Shell.Title"));

    // Build a scrolling composite and a composite for holding all content
    ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL);
    Composite contentComposite = new Composite(scrolledComposite, SWT.NONE);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginRight = MARGIN_SIZE;
    contentComposite.setLayout(contentLayout);
    FormData compositeLayoutData = new FormDataBuilder().fullSize().result();
    contentComposite.setLayoutData(compositeLayoutData);
    props.setLook(contentComposite);

    // transform name label and text field
    // Transform Name.
    Label wTransformNameLabel = new Label(contentComposite, SWT.RIGHT);
    wTransformNameLabel.setText(
        BaseMessages.getString(PKG, "DropboxOutputDialog.TransformName.Label"));
    props.setLook(wTransformNameLabel);
    FormData fdTransformNameLabel =
        new FormDataBuilder().left().top().right(middle, -ELEMENT_SPACING).result();
    wTransformNameLabel.setLayoutData(fdTransformNameLabel);

    wTransformNameField = new Text(contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformNameField.setText(transformName);
    props.setLook(wTransformNameField);
    wTransformNameField.addModifyListener(lsMod);
    FormData fdTransformName = new FormDataBuilder().left(middle, 0).top().right(100, 0).result();
    wTransformNameField.setLayoutData(fdTransformName);

    // Spacer between entry info and content
    Label topSpacer = new Label(contentComposite, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer =
        new FormDataBuilder().fullWidth().top(wTransformNameField, MARGIN_SIZE).result();
    topSpacer.setLayoutData(fdSpacer);

    // Send Successful Transfers to...
    // Target transform for successful transfers.
    Label wlSuccessfulToLabel = new Label(contentComposite, SWT.RIGHT);
    props.setLook(wlSuccessfulToLabel);
    wlSuccessfulToLabel.setText(
        BaseMessages.getString(PKG, "DropboxOutputDialog.SuccessfulTransfersTo.Label"));
    FormData suclTransformation =
        new FormDataBuilder()
            .left()
            .top(topSpacer, ELEMENT_SPACING)
            .right(middle, -ELEMENT_SPACING)
            .result();
    wlSuccessfulToLabel.setLayoutData(suclTransformation);

    wSuccessfulToField = new CCombo(contentComposite, SWT.BORDER);
    props.setLook(wSuccessfulToField);
    wSuccessfulToField.addModifyListener(lsMod);
    FormData sufTransformation =
        new FormDataBuilder()
            .left(middle, 0)
            .top(topSpacer, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wSuccessfulToField.setLayoutData(sufTransformation);

    // Send Failed Transfers to...
    // Target transform for failed transfers.
    Label wlFailedToLabel = new Label(contentComposite, SWT.RIGHT);
    props.setLook(wlFailedToLabel);
    wlFailedToLabel.setText(
        BaseMessages.getString(PKG, "DropboxOutputDialog.FailedTransfersTo.Label"));
    FormData faillTransformation =
        new FormDataBuilder()
            .left()
            .top(wSuccessfulToField, ELEMENT_SPACING)
            .right(middle, -ELEMENT_SPACING)
            .result();
    wlFailedToLabel.setLayoutData(faillTransformation);

    wFailedToField = new CCombo(contentComposite, SWT.BORDER);
    props.setLook(wFailedToField);
    wFailedToField.addModifyListener(lsMod);
    FormData failTransformation =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wSuccessfulToField, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wFailedToField.setLayoutData(failTransformation);

    // Group for Transfer Fields.
    // Group transfer content.
    Group transferGroup = new Group(contentComposite, SWT.SHADOW_ETCHED_IN);
    transferGroup.setText(BaseMessages.getString(PKG, "DropboxOutputDialog.Transfer.GroupText"));
    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = MARGIN_SIZE;
    groupLayout.marginHeight = MARGIN_SIZE;
    transferGroup.setLayout(groupLayout);
    FormData groupLayoutData =
        new FormDataBuilder().fullWidth().top(wFailedToField, MARGIN_SIZE).result();
    transferGroup.setLayoutData(groupLayoutData);
    props.setLook(transferGroup);

    // Access Token Output label/field
    // OAuth access token.
    Label wAccessTokenLabel = new Label(transferGroup, SWT.RIGHT);
    props.setLook(wAccessTokenLabel);
    wAccessTokenLabel.setText(BaseMessages.getString(PKG, "DropboxOutputDialog.AccessToken.Label"));
    FormData fdlTransformation =
        new FormDataBuilder().left().top().right(middle, -ELEMENT_SPACING).result();
    wAccessTokenLabel.setLayoutData(fdlTransformation);

    wAccessTokenField = new CCombo(transferGroup, SWT.BORDER);
    props.setLook(wAccessTokenField);
    wAccessTokenField.addModifyListener(lsMod);
    FormData fdTransformation = new FormDataBuilder().left(middle, 0).top().right(100, 0).result();
    wAccessTokenField.setLayoutData(fdTransformation);

    // Source Files label/field
    // Files to be transferred.
    Label wSourceFilesLabel = new Label(transferGroup, SWT.RIGHT);
    props.setLook(wSourceFilesLabel);
    wSourceFilesLabel.setText(BaseMessages.getString(PKG, "DropboxOutputDialog.SourceFiles.Label"));
    FormData fdlTransformation2 =
        new FormDataBuilder()
            .left()
            .top(wAccessTokenField, ELEMENT_SPACING)
            .right(middle, -ELEMENT_SPACING)
            .result();
    wSourceFilesLabel.setLayoutData(fdlTransformation2);

    wSourceFilesComboBox = new CCombo(transferGroup, SWT.BORDER);
    props.setLook(wSourceFilesComboBox);
    wSourceFilesComboBox.addModifyListener(lsMod);
    FormData fdTransformation2 =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wAccessTokenField, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wSourceFilesComboBox.setLayoutData(fdTransformation2);

    // Target Files label/field
    // Target files to be created.
    Label wTargetFilesLabel = new Label(transferGroup, SWT.RIGHT);
    props.setLook(wTargetFilesLabel);
    wTargetFilesLabel.setText(
        BaseMessages.getString(PKG, "DropboxOutputDialog.TargetFolder.Label"));
    FormData fdlTransformation3 =
        new FormDataBuilder()
            .left()
            .top(wSourceFilesComboBox, ELEMENT_SPACING)
            .right(middle, -ELEMENT_SPACING)
            .result();
    wTargetFilesLabel.setLayoutData(fdlTransformation3);

    wTargetFilesComboBox = new CCombo(transferGroup, SWT.BORDER);
    props.setLook(wTargetFilesComboBox);
    wTargetFilesComboBox.addModifyListener(lsMod);
    FormData fdTransformation3 =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wSourceFilesComboBox, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wTargetFilesComboBox.setLayoutData(fdTransformation3);

    // Cancel, action and OK buttons for the bottom of the window.
    // Footer Buttons
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    FormData fdCancel = new FormDataBuilder().right(100, -MARGIN_SIZE).bottom().result();
    wCancel.setLayoutData(fdCancel);

    Button wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    FormData fdOk = new FormDataBuilder().right(wCancel, -ELEMENT_SPACING).bottom().result();
    wOK.setLayoutData(fdOk);

    // Space between bottom buttons and and group content.
    Label bottomSpacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdhSpacer =
        new FormDataBuilder()
            .left()
            .right(100, -MARGIN_SIZE)
            .bottom(wCancel, -MARGIN_SIZE)
            .result();
    bottomSpacer.setLayoutData(fdhSpacer);

    // Add everything to the scrolling composite
    scrolledComposite.setContent(contentComposite);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setMinSize(contentComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));

    scrolledComposite.setLayout(new FormLayout());
    FormData fdScrolledComposite =
        new FormDataBuilder().fullWidth().top().bottom(bottomSpacer, -MARGIN_SIZE).result();
    scrolledComposite.setLayoutData(fdScrolledComposite);
    props.setLook(scrolledComposite);

    // Listeners
    Listener lsOK = e -> ok();

    wOK.addListener(SWT.Selection, lsOK);
    wCancel.addListener(SWT.Selection, e -> cancel());

    // Populate Window.
    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    // Add target transforms to 'send to' combo box options.
    TransformMeta transformInfo = pipelineMeta.findTransform(transformName);
    if (transformInfo != null) {
      List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms(transformInfo);
      nextTransforms.stream()
          .forEach(
              transformMeta -> {
                wSuccessfulToField.add(transformMeta.getName());
                wFailedToField.add(transformMeta.getName());
              });
    }

    // Get 'send to' transforms.
    wSuccessfulToField.setText(Const.NVL(meta.getSuccessfulTransformName(), ""));
    wFailedToField.setText(Const.NVL(meta.getFailedTransformName(), ""));

    // Add previous fields to transfer combo box options.
    try {
      String[] prevFields =
          pipelineMeta.getPrevTransformFields(variables, transformName).getFieldNames();
      Arrays.stream(prevFields)
          .forEach(
              field -> {
                wAccessTokenField.add(field);
                wSourceFilesComboBox.add(field);
                wTargetFilesComboBox.add(field);
              });
    } catch (HopTransformException e) {
      e.printStackTrace();
    }

    // Get transfer fields values.
    String accessTokenField = meta.getAccessTokenField();
    if (accessTokenField != null) {
      wAccessTokenField.setText(meta.getAccessTokenField());
    }
    String sourceFilesField = meta.getSourceFilesField();
    if (sourceFilesField != null) {
      wSourceFilesComboBox.setText(meta.getSourceFilesField());
    }
    String targetFolderField = meta.getTargetFilesField();
    if (targetFolderField != null) {
      wTargetFilesComboBox.setText(meta.getTargetFilesField());
    }
  }

  /** Save information from dialog fields to the meta-data input. */
  private void getMeta(DropboxOutputMeta meta) {
    // Set target streams.
    List<IStream> targetStreams = meta.getTransformIOMeta().getTargetStreams();
    String successfulStream = wSuccessfulToField.getText();
    String failedStream = wFailedToField.getText();
    targetStreams.get(0).setTransformMeta(pipelineMeta.findTransform(successfulStream));
    targetStreams.get(1).setTransformMeta(pipelineMeta.findTransform(failedStream));
    meta.setSuccessfulTransformName(successfulStream);
    meta.setFailedTransformName(failedStream);

    // Set transfer fields.
    meta.setAccessTokenField(wAccessTokenField.getText());
    meta.setSourceFilesField(wSourceFilesComboBox.getText());
    meta.setTargetFilesField(wTargetFilesComboBox.getText());
  }

  private void cancel() {
    meta.setChanged(changed);
    dispose();
  }

  private void ok() {
    getMeta(meta);
    transformName = wTransformNameField.getText();
    dispose();
  }
}
