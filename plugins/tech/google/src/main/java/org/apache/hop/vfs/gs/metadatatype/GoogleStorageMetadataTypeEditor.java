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
package org.apache.hop.vfs.gs.metadatatype;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

public class GoogleStorageMetadataTypeEditor extends MetadataEditor<GoogleStorageMetadataType> {

  private static final Class<?> PKG = GoogleStorageMetadataTypeEditor.class;

  // Connection properties
  //
  private Text wName;
  private Text wDescription;
  private TextVar wStorageAccountKey;
  private Combo wStorageCredentialType;

  public GoogleStorageMetadataTypeEditor(
      HopGui hopGui,
      MetadataManager<GoogleStorageMetadataType> manager,
      GoogleStorageMetadataType metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {

    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin() + 2;

    Control lastControl;

    // The name
    //
    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "GoogleStorageMetadataTypeEditor.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, 0);
    fdName.right = new FormAttachment(95, 0);
    wName.setLayoutData(fdName);
    lastControl = wName;

    // The Description
    //
    Label wlDescription = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlDescription);
    wlDescription.setText(
        BaseMessages.getString(PKG, "GoogleStorageMetadataTypeEditor.Description.Label"));
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment(lastControl, margin);
    fdlDescription.left = new FormAttachment(0, 0);
    fdlDescription.right = new FormAttachment(middle, -margin);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment(wlDescription, 0, SWT.CENTER);
    fdDescription.left = new FormAttachment(middle, 0); // To the right of the label
    fdDescription.right = new FormAttachment(95, 0);
    wDescription.setLayoutData(fdDescription);
    lastControl = wDescription;

    // The Storage account key
    //
    Label wlStorageCredentialType = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlStorageCredentialType);
    wlStorageCredentialType.setText(
        BaseMessages.getString(PKG, "GoogleStorageMetadataTypeEditor.StorageAccountKey.Label"));
    FormData fdlStorageCredentialType = new FormData();
    fdlStorageCredentialType.top = new FormAttachment(lastControl, margin);
    fdlStorageCredentialType.left = new FormAttachment(0, 0);
    fdlStorageCredentialType.right = new FormAttachment(middle, -margin);
    wlStorageCredentialType.setLayoutData(fdlStorageCredentialType);
    wStorageCredentialType = new Combo(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wStorageCredentialType);
    FormData fdStorageCredentialType = new FormData();
    fdStorageCredentialType.top = new FormAttachment(wlStorageCredentialType, 0, SWT.CENTER);
    fdStorageCredentialType.left = new FormAttachment(middle, 0);
    fdStorageCredentialType.right = new FormAttachment(95, 0);
    wStorageCredentialType.setLayoutData(fdStorageCredentialType);
    lastControl = wStorageCredentialType;

    List<String> credentialTypeOptions = new ArrayList<>();
    EnumSet.allOf(GoogleStorageCredentialsType.class)
        .forEach(credentialsType -> credentialTypeOptions.add(credentialsType.toString()));
    wStorageCredentialType.setItems(credentialTypeOptions.toArray(new String[0]));

    // The Storage account key
    //
    Label wlStorageAccountKey = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlStorageAccountKey);
    wlStorageAccountKey.setText(
        BaseMessages.getString(PKG, "GoogleStorageMetadataTypeEditor.StorageAccountKey.Label"));
    FormData fdlStorageAccountKey = new FormData();
    fdlStorageAccountKey.top = new FormAttachment(lastControl, margin);
    fdlStorageAccountKey.left = new FormAttachment(0, 0);
    fdlStorageAccountKey.right = new FormAttachment(middle, -margin);
    wlStorageAccountKey.setLayoutData(fdlStorageAccountKey);
    wStorageAccountKey = new TextVar(getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wStorageAccountKey);
    FormData fdStorageAccountKey = new FormData();
    fdStorageAccountKey.top = new FormAttachment(wlStorageAccountKey, 0, SWT.TOP);
    fdStorageAccountKey.left = new FormAttachment(middle, 0);
    fdStorageAccountKey.right = new FormAttachment(95, 0);
    wStorageAccountKey.setLayoutData(fdStorageAccountKey);
    lastControl = wStorageAccountKey;

    setWidgetsContent();

    // Add listener to detect change after loading data
    wName.addModifyListener(e -> setChanged());
    wDescription.addModifyListener(e -> setChanged());
    wStorageCredentialType.addModifyListener(e -> setChanged());
    wStorageAccountKey.addModifyListener(e -> setChanged());
  }

  @Override
  public void setWidgetsContent() {
    GoogleStorageMetadataType GoogleStorageMetadataType = this.getMetadata();
    wName.setText(Const.NVL(GoogleStorageMetadataType.getName(), ""));
    wDescription.setText(Const.NVL(GoogleStorageMetadataType.getDescription(), ""));
    if (GoogleStorageMetadataType.getStorageCredentialsType() != null) {
      wStorageCredentialType.setText(
          GoogleStorageMetadataType.getStorageCredentialsType().toString());
    }
    wStorageAccountKey.setText(Const.NVL(GoogleStorageMetadataType.getStorageAccountKey(), ""));
  }

  @Override
  public void getWidgetsContent(GoogleStorageMetadataType GoogleStorageMetadataType) {
    GoogleStorageMetadataType.setName(wName.getText());
    GoogleStorageMetadataType.setDescription(wDescription.getText());
    GoogleStorageMetadataType.setStorageAccountKey(wStorageAccountKey.getText());
    GoogleStorageMetadataType.setStorageCredentialsType(
        GoogleStorageCredentialsType.getEnum(wStorageCredentialType.getText()));
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }

  @Override
  public void save() throws HopException {
    super.save();
    HopVfs.reset();
  }
}
