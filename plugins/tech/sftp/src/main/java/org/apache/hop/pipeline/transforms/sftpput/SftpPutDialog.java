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
package org.apache.hop.pipeline.transforms.sftpput;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.sftpput.SftpPutMeta.AfterSftpPut;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class SftpPutDialog extends BaseTransformDialog {

  private static final Class<?> PKG = SftpPutMeta.class;

  private final SftpPutMeta input;

  private MetaSelectionLine<SftpConnection> wConnection;

  private CCombo wSourceFileField;
  private Button wInputIsStream;
  private CCombo wRemoteDirectoryField;
  private CCombo wRemoteFilenameField;
  private Button wCreateRemoteFolder;
  private CCombo wAfterSftpPut;
  private Label wlDestinationFolderField;
  private CCombo wDestinationFolderField;
  private Label wlCreateDestinationFolder;
  private Button wCreateDestinationFolder;
  private Label wlAddFilenameToResult;
  private Button wAddFilenameToResult;

  private boolean gotPreviousFields = false;

  public SftpPutDialog(
      Shell parent, IVariables variables, SftpPutMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "SftpPutDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            SftpConnection.class,
            shell,
            SWT.NONE,
            BaseMessages.getString(PKG, "SftpPutDialog.Connection.Label"),
            BaseMessages.getString(PKG, "SftpPutDialog.Connection.Tooltip"));
    PropsUi.setLook(wConnection);
    wConnection.addModifyListener(lsMod);
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, margin);
    fdConnection.top = new FormAttachment(wSpacer, margin);
    fdConnection.right = new FormAttachment(100, -margin);
    wConnection.setLayoutData(fdConnection);

    // ///////////////////////////////
    // The source of the data
    // ///////////////////////////////

    Group wSourceGroup = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wSourceGroup);
    wSourceGroup.setText(BaseMessages.getString(PKG, "SftpPutDialog.Group.Source.Label"));
    FormLayout sourceLayout = new FormLayout();
    sourceLayout.marginWidth = 10;
    sourceLayout.marginHeight = 10;
    wSourceGroup.setLayout(sourceLayout);

    wSourceFileField =
        addFieldCombo(wSourceGroup, null, "SftpPutDialog.SourceFileField.Label", lsMod);
    wInputIsStream =
        addCheckBox(wSourceGroup, wSourceFileField, "SftpPutDialog.InputIsStream.Label");

    FormData fdSourceGroup = new FormData();
    fdSourceGroup.left = new FormAttachment(0, margin);
    fdSourceGroup.top = new FormAttachment(wConnection, margin * 2);
    fdSourceGroup.right = new FormAttachment(100, -margin);
    wSourceGroup.setLayoutData(fdSourceGroup);

    // ///////////////////////////////
    // The target on the server
    // ///////////////////////////////

    Group wTargetGroup = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wTargetGroup);
    wTargetGroup.setText(BaseMessages.getString(PKG, "SftpPutDialog.Group.Target.Label"));
    FormLayout targetLayout = new FormLayout();
    targetLayout.marginWidth = 10;
    targetLayout.marginHeight = 10;
    wTargetGroup.setLayout(targetLayout);

    wRemoteDirectoryField =
        addFieldCombo(wTargetGroup, null, "SftpPutDialog.RemoteDirectoryField.Label", lsMod);
    wRemoteFilenameField =
        addFieldCombo(
            wTargetGroup, wRemoteDirectoryField, "SftpPutDialog.RemoteFilenameField.Label", lsMod);
    wCreateRemoteFolder =
        addCheckBox(wTargetGroup, wRemoteFilenameField, "SftpPutDialog.CreateRemoteFolder.Label");

    FormData fdTargetGroup = new FormData();
    fdTargetGroup.left = new FormAttachment(0, margin);
    fdTargetGroup.top = new FormAttachment(wSourceGroup, margin);
    fdTargetGroup.right = new FormAttachment(100, -margin);
    wTargetGroup.setLayoutData(fdTargetGroup);

    // ///////////////////////////////
    // What to do with the source file afterwards
    // ///////////////////////////////

    Group wAfterGroup = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wAfterGroup);
    wAfterGroup.setText(BaseMessages.getString(PKG, "SftpPutDialog.Group.After.Label"));
    FormLayout afterLayout = new FormLayout();
    afterLayout.marginWidth = 10;
    afterLayout.marginHeight = 10;
    wAfterGroup.setLayout(afterLayout);

    Label wlAfterSftpPut = addLabel(wAfterGroup, null, "SftpPutDialog.AfterSftpPut.Label");
    wAfterSftpPut = new CCombo(wAfterGroup, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wAfterSftpPut);
    wAfterSftpPut.setItems(AfterSftpPut.getDescriptions());
    wAfterSftpPut.setLayoutData(fieldLayout(wlAfterSftpPut));
    wAfterSftpPut.addModifyListener(lsMod);
    wAfterSftpPut.addListener(SWT.Selection, e -> enableFields());

    wlDestinationFolderField =
        addLabel(wAfterGroup, wAfterSftpPut, "SftpPutDialog.DestinationFolderField.Label");
    wDestinationFolderField = new CCombo(wAfterGroup, SWT.BORDER | SWT.READ_ONLY);
    wDestinationFolderField.setEditable(true);
    PropsUi.setLook(wDestinationFolderField);
    wDestinationFolderField.setLayoutData(fieldLayout(wlDestinationFolderField));
    wDestinationFolderField.addModifyListener(lsMod);
    wDestinationFolderField.addListener(SWT.FocusIn, e -> getPreviousFields());

    wlCreateDestinationFolder =
        addLabel(
            wAfterGroup, wDestinationFolderField, "SftpPutDialog.CreateDestinationFolder.Label");
    wCreateDestinationFolder = new Button(wAfterGroup, SWT.CHECK);
    PropsUi.setLook(wCreateDestinationFolder);
    wCreateDestinationFolder.setLayoutData(fieldLayout(wlCreateDestinationFolder));
    wCreateDestinationFolder.addSelectionListener(new ComponentSelectionListener(input));

    wlAddFilenameToResult =
        addLabel(wAfterGroup, wCreateDestinationFolder, "SftpPutDialog.AddFilenameToResult.Label");
    wAddFilenameToResult = new Button(wAfterGroup, SWT.CHECK);
    PropsUi.setLook(wAddFilenameToResult);
    wAddFilenameToResult.setLayoutData(fieldLayout(wlAddFilenameToResult));
    wAddFilenameToResult.addSelectionListener(new ComponentSelectionListener(input));

    FormData fdAfterGroup = new FormData();
    fdAfterGroup.left = new FormAttachment(0, margin);
    fdAfterGroup.top = new FormAttachment(wTargetGroup, margin);
    fdAfterGroup.right = new FormAttachment(100, -margin);
    fdAfterGroup.bottom = new FormAttachment(wOk, -margin * 2);
    wAfterGroup.setLayoutData(fdAfterGroup);

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private Label addLabel(Group parent, Control previous, String messageKey) {
    Label label = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, messageKey));
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(middle, -margin);
    fdLabel.top =
        previous == null ? new FormAttachment(0, margin) : new FormAttachment(previous, margin);
    label.setLayoutData(fdLabel);
    return label;
  }

  private FormData fieldLayout(Label label) {
    FormData fdField = new FormData();
    fdField.left = new FormAttachment(middle, 0);
    fdField.top = new FormAttachment(label, 0, SWT.CENTER);
    fdField.right = new FormAttachment(100, -margin);
    return fdField;
  }

  private CCombo addFieldCombo(
      Group parent, Control previous, String messageKey, ModifyListener lsMod) {
    Label label = addLabel(parent, previous, messageKey);
    CCombo combo = new CCombo(parent, SWT.BORDER | SWT.READ_ONLY);
    combo.setEditable(true);
    PropsUi.setLook(combo);
    combo.setLayoutData(fieldLayout(label));
    combo.addModifyListener(lsMod);
    combo.addListener(SWT.FocusIn, e -> getPreviousFields());
    return combo;
  }

  private Button addCheckBox(Group parent, Control previous, String messageKey) {
    Label label = addLabel(parent, previous, messageKey);
    Button button = new Button(parent, SWT.CHECK);
    PropsUi.setLook(button);
    button.setLayoutData(fieldLayout(label));
    button.addSelectionListener(new ComponentSelectionListener(input));
    return button;
  }

  private void enableFields() {
    boolean move = AfterSftpPut.lookupDescription(wAfterSftpPut.getText()) == AfterSftpPut.MOVE;
    wlDestinationFolderField.setEnabled(move);
    wDestinationFolderField.setEnabled(move);
    wlCreateDestinationFolder.setEnabled(move);
    wCreateDestinationFolder.setEnabled(move);
  }

  private void getPreviousFields() {
    if (gotPreviousFields) {
      return;
    }
    try {
      IRowMeta rowMeta = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (rowMeta != null) {
        String[] fieldNames = rowMeta.getFieldNames();
        setItemsKeepingText(wSourceFileField, fieldNames);
        setItemsKeepingText(wRemoteDirectoryField, fieldNames);
        setItemsKeepingText(wRemoteFilenameField, fieldNames);
        setItemsKeepingText(wDestinationFolderField, fieldNames);
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
          BaseMessages.getString(PKG, "SftpPutDialog.ErrorGettingPreviousFields.DialogMessage"),
          e);
    }
    gotPreviousFields = true;
  }

  private void setItemsKeepingText(CCombo combo, String[] items) {
    String text = combo.getText();
    combo.removeAll();
    combo.setItems(items);
    if (text != null) {
      combo.setText(text);
    }
  }

  private void getData() {
    try {
      wConnection.fillItems();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
          BaseMessages.getString(PKG, "SftpPutDialog.ErrorGettingConnections.DialogMessage"),
          e);
    }
    wConnection.setText(Const.NVL(input.getConnection(), ""));
    wSourceFileField.setText(Const.NVL(input.getSourceFileFieldName(), ""));
    wInputIsStream.setSelection(input.isInputIsStream());
    wRemoteDirectoryField.setText(Const.NVL(input.getRemoteDirectoryFieldName(), ""));
    wRemoteFilenameField.setText(Const.NVL(input.getRemoteFilenameFieldName(), ""));
    wCreateRemoteFolder.setSelection(input.isCreateRemoteFolder());
    wAfterSftpPut.setText(
        input.getAfterSftpPut() == null
            ? AfterSftpPut.NOTHING.getDescription()
            : input.getAfterSftpPut().getDescription());
    wDestinationFolderField.setText(Const.NVL(input.getDestinationFolderFieldName(), ""));
    wCreateDestinationFolder.setSelection(input.isCreateDestinationFolder());
    wAddFilenameToResult.setSelection(input.isAddFilenameToResult());
    enableFields();
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
    input.setConnection(wConnection.getText());
    input.setSourceFileFieldName(wSourceFileField.getText());
    input.setInputIsStream(wInputIsStream.getSelection());
    input.setRemoteDirectoryFieldName(wRemoteDirectoryField.getText());
    input.setRemoteFilenameFieldName(wRemoteFilenameField.getText());
    input.setCreateRemoteFolder(wCreateRemoteFolder.getSelection());
    input.setAfterSftpPut(AfterSftpPut.lookupDescription(wAfterSftpPut.getText()));
    input.setDestinationFolderFieldName(wDestinationFolderField.getText());
    input.setCreateDestinationFolder(wCreateDestinationFolder.getSelection());
    input.setAddFilenameToResult(wAddFilenameToResult.getSelection());

    transformName = wTransformName.getText();
    dispose();
  }
}
