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

package org.apache.hop.pipeline.transforms.jms.shared;

import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.NamingSchemeTypes;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;

@GuiPlugin(description = "Editor for JMS connection metadata")
public class JmsConnectionEditor extends MetadataEditor<JmsConnection> {

  private static final Class<?> PKG = JmsConnection.class;

  private TextVar wName;
  private CCombo wMode;
  private TextVar wBrokerUrl;
  private TextVar wInitialContextFactory;
  private TextVar wProviderUrl;
  private TextVar wConnectionFactoryName;
  private TextVar wUsername;
  private TextVar wPassword;
  private TextVar wClientId;

  public JmsConnectionEditor(
      HopGui hopGui, MetadataManager<JmsConnection> manager, JmsConnection metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin() + 2;

    Composite composite = new Composite(parent, SWT.NONE);
    PropsUi.setLook(composite);
    FormLayout layout = new FormLayout();
    layout.marginWidth = 0;
    layout.marginHeight = 0;
    composite.setLayout(layout);
    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment(0, 0);
    fdComposite.top = new FormAttachment(0, 0);
    fdComposite.right = new FormAttachment(100, 0);
    composite.setLayoutData(fdComposite);

    Label wlName = new Label(composite, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "JmsConnectionEditor.Name"));
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.top = new FormAttachment(0, margin);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName =
        new TextVar(hopGui.getVariables(), composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER)
            .asNameField(NamingSchemeTypes.HOP_METADATA);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);
    Control last = wName;

    wMode =
        (CCombo) labeled(composite, middle, margin, last, "JmsConnectionEditor.Mode", Kind.COMBO);
    wMode.setItems(new String[] {JmsConnection.MODE_DIRECT, JmsConnection.MODE_JNDI});
    last = wMode;

    wBrokerUrl = text(composite, middle, margin, last, "JmsConnectionEditor.BrokerUrl");
    last = wBrokerUrl;
    wInitialContextFactory =
        text(composite, middle, margin, last, "JmsConnectionEditor.InitialContextFactory");
    last = wInitialContextFactory;
    wProviderUrl = text(composite, middle, margin, last, "JmsConnectionEditor.ProviderUrl");
    last = wProviderUrl;
    wConnectionFactoryName =
        text(composite, middle, margin, last, "JmsConnectionEditor.ConnectionFactoryName");
    last = wConnectionFactoryName;
    wUsername = text(composite, middle, margin, last, "JmsConnectionEditor.Username");
    last = wUsername;

    Label wlPassword = new Label(composite, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "JmsConnectionEditor.Password"));
    PropsUi.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.top = new FormAttachment(last, margin);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword =
        new PasswordTextVar(manager.getVariables(), composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPassword);
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.top = new FormAttachment(wlPassword, 0, SWT.CENTER);
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);
    last = wPassword;

    wClientId = text(composite, middle, margin, last, "JmsConnectionEditor.ClientId");

    setWidgetsContent();
    addModifyListeners();
  }

  private enum Kind {
    TEXT,
    COMBO
  }

  private Control labeled(
      Composite composite, int middle, int margin, Control last, String labelKey, Kind kind) {
    Label label = new Label(composite, SWT.RIGHT);
    label.setText(BaseMessages.getString(PKG, labelKey));
    PropsUi.setLook(label);
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.top = new FormAttachment(last, margin);
    fdLabel.right = new FormAttachment(middle, -margin);
    label.setLayoutData(fdLabel);

    Control control =
        kind == Kind.COMBO
            ? new CCombo(composite, SWT.BORDER | SWT.READ_ONLY)
            : new TextVar(manager.getVariables(), composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(control);
    FormData fdControl = new FormData();
    fdControl.left = new FormAttachment(middle, 0);
    fdControl.top = new FormAttachment(label, 0, SWT.CENTER);
    fdControl.right = new FormAttachment(100, 0);
    control.setLayoutData(fdControl);

    String tooltip = BaseMessages.getString(PKG, labelKey + ".Tooltip");
    if (tooltip != null && !tooltip.startsWith("!")) {
      control.setToolTipText(tooltip);
      label.setToolTipText(tooltip);
    }
    return control;
  }

  private TextVar text(Composite composite, int middle, int margin, Control last, String labelKey) {
    return (TextVar) labeled(composite, middle, margin, last, labelKey, Kind.TEXT);
  }

  private void addModifyListeners() {
    wName.addListener(SWT.Modify, e -> setChanged());
    wMode.addListener(SWT.Modify, e -> setChanged());
    for (TextVar field :
        new TextVar[] {
          wBrokerUrl,
          wInitialContextFactory,
          wProviderUrl,
          wConnectionFactoryName,
          wUsername,
          wPassword,
          wClientId
        }) {
      field.addListener(SWT.Modify, e -> setChanged());
    }
  }

  @Override
  public void setWidgetsContent() {
    JmsConnection meta = getMetadata();
    wName.setText(Const.NVL(meta.getName(), ""));
    wMode.setText(Const.NVL(meta.getMode(), JmsConnection.MODE_DIRECT));
    wBrokerUrl.setText(Const.NVL(meta.getBrokerUrl(), ""));
    wInitialContextFactory.setText(Const.NVL(meta.getInitialContextFactory(), ""));
    wProviderUrl.setText(Const.NVL(meta.getProviderUrl(), ""));
    wConnectionFactoryName.setText(Const.NVL(meta.getConnectionFactoryName(), ""));
    wUsername.setText(Const.NVL(meta.getUsername(), ""));
    wPassword.setText(Const.NVL(meta.getPassword(), ""));
    wClientId.setText(Const.NVL(meta.getClientId(), ""));
  }

  @Override
  public void getWidgetsContent(JmsConnection meta) {
    meta.setName(wName.getText());
    meta.setMode(wMode.getText());
    meta.setBrokerUrl(wBrokerUrl.getText());
    meta.setInitialContextFactory(wInitialContextFactory.getText());
    meta.setProviderUrl(wProviderUrl.getText());
    meta.setConnectionFactoryName(wConnectionFactoryName.getText());
    meta.setUsername(wUsername.getText());
    meta.setPassword(wPassword.getText());
    meta.setClientId(wClientId.getText());
  }
}
