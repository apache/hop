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
package org.apache.hop.vfs.azure.metadatatype;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

public class AzureMetadataTypeEditor extends MetadataEditor<AzureMetadataType> {

  private static final Class<?> PKG = AzureMetadataTypeEditor.class;

  // Connection properties
  //
  private Text wName;
  private Text wDescription;
  private TextVar wStorageAccountName;
  private ComboVar wAuthenticationType;
  private Label wlStorageAccountKey;
  private PasswordTextVar wStorageAccountKey;
  private TextVar wStorageAccountEndpoint;

  public AzureMetadataTypeEditor(
      HopGui hopGui, MetadataManager<AzureMetadataType> manager, AzureMetadataType metadata) {
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
    wlName.setText(BaseMessages.getString(PKG, "AzureMetadataTypeEditor.Name.Label"));
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
    wlDescription.setText(BaseMessages.getString(PKG, "AzureMetadataTypeEditor.Description.Label"));
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

    // The storage account endpoint
    //
    Label wlStorageAccountEndpoint = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlStorageAccountEndpoint);
    wlStorageAccountEndpoint.setText(
        BaseMessages.getString(PKG, "AzureMetadataTypeEditor.StorageAccountEndpoint.Label"));
    FormData fdlStorageAccountEndpoint = new FormData();
    fdlStorageAccountEndpoint.top = new FormAttachment(lastControl, margin);
    fdlStorageAccountEndpoint.left = new FormAttachment(0, 0);
    fdlStorageAccountEndpoint.right = new FormAttachment(middle, -margin);
    wlStorageAccountEndpoint.setLayoutData(fdlStorageAccountEndpoint);
    wStorageAccountEndpoint =
        new TextVar(getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wStorageAccountEndpoint);
    FormData fdStorageAccountEndpoint = new FormData();
    fdStorageAccountEndpoint.top = new FormAttachment(wlStorageAccountEndpoint, 0, SWT.CENTER);
    fdStorageAccountEndpoint.left = new FormAttachment(middle, 0);
    fdStorageAccountEndpoint.right = new FormAttachment(95, 0);
    wStorageAccountEndpoint.setLayoutData(fdStorageAccountEndpoint);
    lastControl = wStorageAccountEndpoint;

    // The Storage account name
    //
    Label wlStorageAccountName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlStorageAccountName);
    wlStorageAccountName.setText(
        BaseMessages.getString(PKG, "AzureMetadataTypeEditor.StorageAccountName.Label"));
    FormData fdlStorageAccountName = new FormData();
    fdlStorageAccountName.top = new FormAttachment(lastControl, margin);
    fdlStorageAccountName.left = new FormAttachment(0, 0);
    fdlStorageAccountName.right = new FormAttachment(middle, -margin);
    wlStorageAccountName.setLayoutData(fdlStorageAccountName);
    wStorageAccountName = new TextVar(getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wStorageAccountName);
    FormData fdStorageAccountName = new FormData();
    fdStorageAccountName.top = new FormAttachment(wlStorageAccountName, 0, SWT.CENTER);
    fdStorageAccountName.left = new FormAttachment(middle, 0);
    fdStorageAccountName.right = new FormAttachment(95, 0);
    wStorageAccountName.setLayoutData(fdStorageAccountName);
    lastControl = wStorageAccountName;

    // The Authentication type
    //
    Label wlAuthenticationType = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlAuthenticationType);
    wlAuthenticationType.setText(
        BaseMessages.getString(PKG, "AzureMetadataTypeEditor.AuthenticationType.Label"));
    FormData fdlAuthenticationType = new FormData();
    fdlAuthenticationType.top = new FormAttachment(lastControl, margin);
    fdlAuthenticationType.left = new FormAttachment(0, 0);
    fdlAuthenticationType.right = new FormAttachment(middle, -margin);
    wlAuthenticationType.setLayoutData(fdlAuthenticationType);
    wAuthenticationType = new ComboVar(getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAuthenticationType);
    wAuthenticationType.setItems(new String[] {"Key", "Managed Identity"});
    FormData fdAuthenticationType = new FormData();
    fdAuthenticationType.top = new FormAttachment(wlAuthenticationType, 0, SWT.CENTER);
    fdAuthenticationType.left = new FormAttachment(middle, 0);
    fdAuthenticationType.right = new FormAttachment(95, 0);
    wAuthenticationType.setLayoutData(fdAuthenticationType);
    lastControl = wAuthenticationType;

    // The Storage account key
    //
    wlStorageAccountKey = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlStorageAccountKey);
    wlStorageAccountKey.setText(
        BaseMessages.getString(PKG, "AzureMetadataTypeEditor.StorageAccountKey.Label"));
    FormData fdlStorageAccountKey = new FormData();
    fdlStorageAccountKey.top = new FormAttachment(lastControl, margin);
    fdlStorageAccountKey.left = new FormAttachment(0, 0);
    fdlStorageAccountKey.right = new FormAttachment(middle, -margin);
    wlStorageAccountKey.setLayoutData(fdlStorageAccountKey);
    wStorageAccountKey =
        new PasswordTextVar(getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wStorageAccountKey);
    FormData fdStorageAccountKey = new FormData();
    fdStorageAccountKey.top = new FormAttachment(wlStorageAccountKey, 0, SWT.CENTER);
    fdStorageAccountKey.left = new FormAttachment(middle, 0);
    fdStorageAccountKey.right = new FormAttachment(95, 0);
    wStorageAccountKey.setLayoutData(fdStorageAccountKey);
    lastControl = wStorageAccountKey;

    // Add listener to authentication type to show/hide storage account key
    wAuthenticationType.addModifyListener(
        e -> {
          String authType = wAuthenticationType.getText();
          boolean showKey = "Key".equals(authType);
          wlStorageAccountKey.setVisible(showKey);
          wStorageAccountKey.setVisible(showKey);
          parent.layout(true, true);
        });

    setWidgetsContent();

    // Add listener to detect change after loading data
    wName.addModifyListener(e -> setChanged());
    wDescription.addModifyListener(e -> setChanged());
    wStorageAccountName.addModifyListener(e -> setChanged());
    wAuthenticationType.addModifyListener(e -> setChanged());
    wStorageAccountKey.addModifyListener(e -> setChanged());
    wStorageAccountEndpoint.addModifyListener(e -> setChanged());
  }

  @Override
  public void setWidgetsContent() {
    AzureMetadataType azureMetadataType = this.getMetadata();
    wName.setText(Const.NVL(azureMetadataType.getName(), ""));
    wDescription.setText(Const.NVL(azureMetadataType.getDescription(), ""));
    wStorageAccountName.setText(Const.NVL(azureMetadataType.getStorageAccountName(), ""));
    wAuthenticationType.setText(Const.NVL(azureMetadataType.getAuthenticationType(), "Key"));
    wStorageAccountKey.setText(Const.NVL(azureMetadataType.getStorageAccountKey(), ""));
    wStorageAccountEndpoint.setText(Const.NVL(azureMetadataType.getStorageAccountEndpoint(), ""));

    // Show/hide storage account key based on authentication type
    String authType = wAuthenticationType.getText();
    boolean showKey = "Key".equals(authType);
    wlStorageAccountKey.setVisible(showKey);
    wStorageAccountKey.setVisible(showKey);
  }

  @Override
  public void getWidgetsContent(AzureMetadataType azureMetadataType) {
    azureMetadataType.setName(wName.getText());
    azureMetadataType.setDescription(wDescription.getText());
    azureMetadataType.setStorageAccountName(wStorageAccountName.getText());
    azureMetadataType.setAuthenticationType(wAuthenticationType.getText());
    azureMetadataType.setStorageAccountKey(wStorageAccountKey.getText());
    azureMetadataType.setStorageAccountEndpoint(wStorageAccountEndpoint.getText());
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
