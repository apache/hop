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

package org.apache.hop.metadata.rest;

import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

public class RestConnectionEditor extends MetadataEditor<RestConnection> {
  private static final Class<?> PKG = RestConnectionEditor.class;

  private Text wName;

  private TextVar wBaseUrl;
  private TextVar wTestUrl;
  private ComboVar wAuthType;
  private static String[] authTypes =
      new String[] {"No Auth", "API Key", "Basic", "Bearer", "Certificate"};

  private Composite wAuthComp;

  // Bearer token
  private PasswordTextVar wBearerValue;

  // Basic auth token
  private TextVar wUsername;
  private PasswordTextVar wPassword;

  // API Key
  private TextVar wAuthorizationName;
  private TextVar wAuthorizationPrefix;
  private PasswordTextVar wAuthorizationValue;

  // SSL / TrustStore
  private TextVar wTrustStorePassword;
  private TextVar wTrustStoreFile;
  private Button wbTrustStoreFile;
  private Button wIgnoreSsl;

  // Client Certificate / KeyStore
  private TextVar wKeyStoreFile;
  private Button wbKeyStoreFile;
  private PasswordTextVar wKeyStorePassword;
  private ComboVar wKeyStoreType;
  private PasswordTextVar wKeyPassword;
  private TextVar wCertificateAlias;

  private PropsUi props;
  private int middle;
  private int margin;
  private IVariables variables;
  Control lastControl;

  public RestConnectionEditor(
      HopGui hopGui, MetadataManager<RestConnection> manager, RestConnection restConnection) {
    super(hopGui, manager, restConnection);
    props = PropsUi.getInstance();

    middle = props.getMiddlePct();
    margin = props.getMargin();
  }

  @Override
  public void createControl(Composite composite) {

    variables = hopGui.getVariables();

    // The name
    Label wlName = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "RestConnectionEditor.Name"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin);
    fdlName.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, 0); // To the right of the label
    fdName.right = new FormAttachment(95, 0);
    wName.setLayoutData(fdName);
    lastControl = wName;

    // start authentication group (now directly below name)
    Group gAuth = new Group(composite, SWT.SHADOW_ETCHED_IN);
    gAuth.setText(BaseMessages.getString(PKG, "RestConnectionEditor.AuthGroup.Label"));
    FormLayout gAuthLayout = new FormLayout();
    gAuthLayout.marginWidth = 3;
    gAuthLayout.marginHeight = 3;
    gAuth.setLayout(gAuthLayout);
    PropsUi.setLook(gAuth);

    Label wlAuthType = new Label(gAuth, SWT.RIGHT);
    PropsUi.setLook(wlAuthType);
    wlAuthType.setText(BaseMessages.getString(PKG, "RestConnectionEditor.AuthType"));
    FormData fdlAuthType = new FormData();
    fdlAuthType.top = new FormAttachment(0, margin);
    fdlAuthType.left = new FormAttachment(0, 0);
    fdlAuthType.right = new FormAttachment(middle, -margin);
    wlAuthType.setLayoutData(fdlAuthType);
    wAuthType = new ComboVar(variables, gAuth, SWT.READ_ONLY | SWT.BORDER);
    PropsUi.setLook(wAuthType);
    FormData fdAuthType = new FormData();
    fdAuthType.top = new FormAttachment(wlAuthType, 0, SWT.CENTER);
    fdAuthType.left = new FormAttachment(middle, 0);
    fdAuthType.right = new FormAttachment(95, 0);
    wAuthType.setLayoutData(fdAuthType);

    wAuthType.setItems(authTypes);
    wAuthType.addListener(
        SWT.Selection,
        e -> {
          if (wAuthType.getText().equals("No Auth")) {
            addNoAuthFields();
          } else if (wAuthType.getText().equals("API Key")) {
            addApiKeyFields();
          } else if (wAuthType.getText().equals("Basic")) {
            addBasicAuthFields();
          } else if (wAuthType.getText().equals("Bearer")) {
            addBearerFields();
          } else if (wAuthType.getText().equals("Certificate")) {
            addCertificateFields();
          }
        });

    ScrolledComposite wsAuthComp = new ScrolledComposite(gAuth, SWT.V_SCROLL | SWT.H_SCROLL);
    props.setLook(wsAuthComp);
    FormData fdAuthSComp = new FormData();
    fdAuthSComp.top = new FormAttachment(wAuthType, margin);
    fdAuthSComp.left = new FormAttachment(0, 0);
    fdAuthSComp.right = new FormAttachment(95, 0);
    fdAuthSComp.bottom = new FormAttachment(95, 0);
    wsAuthComp.setLayoutData(fdAuthSComp);

    wAuthComp = new Composite(wsAuthComp, SWT.BACKGROUND);
    PropsUi.setLook(wAuthComp);
    wAuthComp.setLayout(new FormLayout());
    FormData fdAuthComp = new FormData();
    fdAuthComp.left = new FormAttachment(0, 0);
    fdAuthComp.right = new FormAttachment(0, 0);
    fdAuthComp.top = new FormAttachment(95, 0);
    fdAuthComp.bottom = new FormAttachment(95, 0);
    wAuthComp.setLayoutData(fdAuthComp);
    wAuthComp.pack();

    wsAuthComp.setContent(wAuthComp);

    wAuthType.select(0);

    wAuthComp.layout();

    FormData fdAuth = new FormData();
    fdAuth.left = new FormAttachment(0, 0);
    fdAuth.top = new FormAttachment(wName, margin);
    fdAuth.right = new FormAttachment(100, 0);
    gAuth.setLayoutData(fdAuth);

    // start of URL group
    Group gUrl = new Group(composite, SWT.SHADOW_ETCHED_IN);
    gUrl.setText(BaseMessages.getString(PKG, "RestConnectionEditor.UrlGroup.Label"));
    FormLayout gUrlLayout = new FormLayout();
    gUrlLayout.marginWidth = 3;
    gUrlLayout.marginHeight = 3;
    gUrl.setLayout(gUrlLayout);
    PropsUi.setLook(gUrl);

    Label wlBaseUrl = new Label(gUrl, SWT.RIGHT);
    PropsUi.setLook(wlBaseUrl);
    wlBaseUrl.setText(BaseMessages.getString(PKG, "RestConnectionEditor.BaseUrl"));
    FormData fdlBaseUrl = new FormData();
    fdlBaseUrl.top = new FormAttachment(0, margin);
    fdlBaseUrl.left = new FormAttachment(0, 0);
    fdlBaseUrl.right = new FormAttachment(middle, -margin);
    wlBaseUrl.setLayoutData(fdlBaseUrl);
    wBaseUrl = new TextVar(variables, gUrl, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBaseUrl);
    FormData fdBaseUrl = new FormData();
    fdBaseUrl.top = new FormAttachment(0, 0);
    fdBaseUrl.left = new FormAttachment(middle, 0);
    fdBaseUrl.right = new FormAttachment(95, 0);
    wBaseUrl.setLayoutData(fdBaseUrl);
    lastControl = wBaseUrl;

    Label wlTestUrl = new Label(gUrl, SWT.RIGHT);
    PropsUi.setLook(wlTestUrl);
    wlTestUrl.setText(BaseMessages.getString(PKG, "RestConnectionEditor.TestUrl"));
    FormData fdlTestUrl = new FormData();
    fdlTestUrl.top = new FormAttachment(lastControl, margin);
    fdlTestUrl.left = new FormAttachment(0, 0);
    fdlTestUrl.right = new FormAttachment(middle, -margin);
    wlTestUrl.setLayoutData(fdlTestUrl);
    wTestUrl = new TextVar(variables, gUrl, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTestUrl);
    FormData fdTestUrl = new FormData();
    fdTestUrl.top = new FormAttachment(wlTestUrl, 0, SWT.CENTER);
    fdTestUrl.left = new FormAttachment(middle, 0);
    fdTestUrl.right = new FormAttachment(95, 0);
    wTestUrl.setLayoutData(fdTestUrl);
    lastControl = wTestUrl;

    FormData fdUrl = new FormData();
    fdUrl.top = new FormAttachment(gAuth, margin);
    fdUrl.left = new FormAttachment(0, 0);
    fdUrl.right = new FormAttachment(95, 0);
    gUrl.setLayoutData(fdUrl);
    // end URL group

    // start SSL group
    Group gSSLTrustStore = new Group(composite, SWT.SHADOW_ETCHED_IN);
    gSSLTrustStore.setText(BaseMessages.getString(PKG, "RestConnectionEditor.SSLGroup.Label"));
    FormLayout gSSLTrustStoreLayout = new FormLayout();
    gSSLTrustStoreLayout.marginWidth = 3;
    gSSLTrustStoreLayout.marginHeight = 3;
    gSSLTrustStore.setLayout(gSSLTrustStoreLayout);
    PropsUi.setLook(gSSLTrustStore);

    Label wlTrustStoreFile = new Label(gSSLTrustStore, SWT.RIGHT);
    wlTrustStoreFile.setText(
        BaseMessages.getString(PKG, "RestConnectionEditor.TrustStoreFile.Label"));
    PropsUi.setLook(wlTrustStoreFile);
    FormData fdlTrustStoreFile = new FormData();
    fdlTrustStoreFile.left = new FormAttachment(0, 0);
    fdlTrustStoreFile.top = new FormAttachment(0, margin);
    fdlTrustStoreFile.right = new FormAttachment(middle, -margin);
    wlTrustStoreFile.setLayoutData(fdlTrustStoreFile);

    wbTrustStoreFile = new Button(gSSLTrustStore, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTrustStoreFile);
    wbTrustStoreFile.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbTrustStoreFile = new FormData();
    fdbTrustStoreFile.right = new FormAttachment(100, 0);
    fdbTrustStoreFile.top = new FormAttachment(0, 0);
    wbTrustStoreFile.setLayoutData(fdbTrustStoreFile);

    wbTrustStoreFile.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                hopGui.getShell(),
                wTrustStoreFile,
                variables,
                new String[] {"*.)"},
                new String[] {BaseMessages.getString(PKG, "System.FileType.AllFiles")},
                true));

    wTrustStoreFile = new TextVar(variables, gSSLTrustStore, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTrustStoreFile);
    //    wTrustStoreFile.addModifyListener(lsMod);
    FormData fdTrustStoreFile = new FormData();
    fdTrustStoreFile.left = new FormAttachment(middle, 0);
    fdTrustStoreFile.top = new FormAttachment(0, margin);
    fdTrustStoreFile.right = new FormAttachment(wbTrustStoreFile, -margin);
    wTrustStoreFile.setLayoutData(fdTrustStoreFile);

    Label wlTrustStorePassword = new Label(gSSLTrustStore, SWT.RIGHT);
    wlTrustStorePassword.setText(
        BaseMessages.getString(PKG, "RestConnectionEditor.TrustStorePassword.Label"));
    PropsUi.setLook(wlTrustStorePassword);
    FormData fdlTrustStorePassword = new FormData();
    fdlTrustStorePassword.left = new FormAttachment(0, 0);
    fdlTrustStorePassword.top = new FormAttachment(wbTrustStoreFile, margin);
    fdlTrustStorePassword.right = new FormAttachment(middle, -margin);
    wlTrustStorePassword.setLayoutData(fdlTrustStorePassword);
    wTrustStorePassword =
        new PasswordTextVar(variables, gSSLTrustStore, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTrustStorePassword);
    //    wTrustStorePassword.addModifyListener(lsMod);
    FormData fdTrustStorePassword = new FormData();
    fdTrustStorePassword.left = new FormAttachment(middle, 0);
    fdTrustStorePassword.top = new FormAttachment(wbTrustStoreFile, margin);
    fdTrustStorePassword.right = new FormAttachment(100, 0);
    wTrustStorePassword.setLayoutData(fdTrustStorePassword);

    Label wlIgnoreSsl = new Label(gSSLTrustStore, SWT.RIGHT);
    wlIgnoreSsl.setText(BaseMessages.getString(PKG, "RestConnectionEditor.IgnoreSsl.Label"));
    PropsUi.setLook(wlIgnoreSsl);
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.top = new FormAttachment(wTrustStorePassword, margin);
    fdLabel.right = new FormAttachment(middle, -margin);
    wlIgnoreSsl.setLayoutData(fdLabel);
    wIgnoreSsl = new Button(gSSLTrustStore, SWT.CHECK);
    PropsUi.setLook(wIgnoreSsl);
    FormData fdButton = new FormData();
    fdButton.left = new FormAttachment(middle, 0);
    fdButton.top = new FormAttachment(wlIgnoreSsl, 0, SWT.CENTER);
    fdButton.right = new FormAttachment(100, 0);
    wIgnoreSsl.setLayoutData(fdButton);
    wIgnoreSsl.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setChanged();
            activateTrustoreFields();
          }
        });

    // Client Certificate section separator
    Label wlClientCert = new Label(gSSLTrustStore, SWT.SEPARATOR | SWT.HORIZONTAL);
    PropsUi.setLook(wlClientCert);
    FormData fdlClientCert = new FormData();
    fdlClientCert.left = new FormAttachment(0, 0);
    fdlClientCert.right = new FormAttachment(100, 0);
    fdlClientCert.top = new FormAttachment(wIgnoreSsl, margin * 2);
    wlClientCert.setLayoutData(fdlClientCert);

    Label wlClientCertLabel = new Label(gSSLTrustStore, SWT.LEFT);
    wlClientCertLabel.setText(
        BaseMessages.getString(PKG, "RestConnectionEditor.ClientCertificate.Label"));
    PropsUi.setLook(wlClientCertLabel);
    FormData fdlClientCertLabel = new FormData();
    fdlClientCertLabel.left = new FormAttachment(0, 0);
    fdlClientCertLabel.top = new FormAttachment(wlClientCert, margin);
    wlClientCertLabel.setLayoutData(fdlClientCertLabel);

    // KeyStore file
    Label wlKeyStoreFile = new Label(gSSLTrustStore, SWT.RIGHT);
    wlKeyStoreFile.setText(BaseMessages.getString(PKG, "RestConnectionEditor.KeyStoreFile.Label"));
    PropsUi.setLook(wlKeyStoreFile);
    FormData fdlKeyStoreFile = new FormData();
    fdlKeyStoreFile.left = new FormAttachment(0, 0);
    fdlKeyStoreFile.top = new FormAttachment(wlClientCertLabel, margin);
    fdlKeyStoreFile.right = new FormAttachment(middle, -margin);
    wlKeyStoreFile.setLayoutData(fdlKeyStoreFile);

    wbKeyStoreFile = new Button(gSSLTrustStore, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbKeyStoreFile);
    wbKeyStoreFile.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbKeyStoreFile = new FormData();
    fdbKeyStoreFile.right = new FormAttachment(100, 0);
    fdbKeyStoreFile.top = new FormAttachment(wlClientCertLabel, margin);
    wbKeyStoreFile.setLayoutData(fdbKeyStoreFile);

    wbKeyStoreFile.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                hopGui.getShell(),
                wKeyStoreFile,
                variables,
                new String[] {"*.p12;*.pfx;*.jks", "*.*"},
                new String[] {
                  BaseMessages.getString(PKG, "RestConnectionEditor.CertificateFiles"),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true));

    wKeyStoreFile = new TextVar(variables, gSSLTrustStore, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wKeyStoreFile);
    FormData fdKeyStoreFile = new FormData();
    fdKeyStoreFile.left = new FormAttachment(middle, 0);
    fdKeyStoreFile.top = new FormAttachment(wlClientCertLabel, margin);
    fdKeyStoreFile.right = new FormAttachment(wbKeyStoreFile, -margin);
    wKeyStoreFile.setLayoutData(fdKeyStoreFile);

    // KeyStore password
    Label wlKeyStorePassword = new Label(gSSLTrustStore, SWT.RIGHT);
    wlKeyStorePassword.setText(
        BaseMessages.getString(PKG, "RestConnectionEditor.KeyStorePassword.Label"));
    PropsUi.setLook(wlKeyStorePassword);
    FormData fdlKeyStorePassword = new FormData();
    fdlKeyStorePassword.left = new FormAttachment(0, 0);
    fdlKeyStorePassword.top = new FormAttachment(wbKeyStoreFile, margin);
    fdlKeyStorePassword.right = new FormAttachment(middle, -margin);
    wlKeyStorePassword.setLayoutData(fdlKeyStorePassword);

    wKeyStorePassword =
        new PasswordTextVar(variables, gSSLTrustStore, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wKeyStorePassword);
    FormData fdKeyStorePassword = new FormData();
    fdKeyStorePassword.left = new FormAttachment(middle, 0);
    fdKeyStorePassword.top = new FormAttachment(wbKeyStoreFile, margin);
    fdKeyStorePassword.right = new FormAttachment(100, 0);
    wKeyStorePassword.setLayoutData(fdKeyStorePassword);

    // KeyStore type
    Label wlKeyStoreType = new Label(gSSLTrustStore, SWT.RIGHT);
    wlKeyStoreType.setText(BaseMessages.getString(PKG, "RestConnectionEditor.KeyStoreType.Label"));
    PropsUi.setLook(wlKeyStoreType);
    FormData fdlKeyStoreType = new FormData();
    fdlKeyStoreType.left = new FormAttachment(0, 0);
    fdlKeyStoreType.top = new FormAttachment(wKeyStorePassword, margin);
    fdlKeyStoreType.right = new FormAttachment(middle, -margin);
    wlKeyStoreType.setLayoutData(fdlKeyStoreType);

    wKeyStoreType = new ComboVar(variables, gSSLTrustStore, SWT.READ_ONLY | SWT.BORDER);
    PropsUi.setLook(wKeyStoreType);
    wKeyStoreType.setItems(new String[] {"PKCS12", "JKS"});
    FormData fdKeyStoreType = new FormData();
    fdKeyStoreType.left = new FormAttachment(middle, 0);
    fdKeyStoreType.top = new FormAttachment(wlKeyStoreType, 0, SWT.CENTER);
    fdKeyStoreType.right = new FormAttachment(100, 0);
    wKeyStoreType.setLayoutData(fdKeyStoreType);

    // Key password (optional)
    Label wlKeyPassword = new Label(gSSLTrustStore, SWT.RIGHT);
    wlKeyPassword.setText(BaseMessages.getString(PKG, "RestConnectionEditor.KeyPassword.Label"));
    PropsUi.setLook(wlKeyPassword);
    FormData fdlKeyPassword = new FormData();
    fdlKeyPassword.left = new FormAttachment(0, 0);
    fdlKeyPassword.top = new FormAttachment(wKeyStoreType, margin);
    fdlKeyPassword.right = new FormAttachment(middle, -margin);
    wlKeyPassword.setLayoutData(fdlKeyPassword);

    wKeyPassword =
        new PasswordTextVar(variables, gSSLTrustStore, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wKeyPassword);
    wKeyPassword.setToolTipText(
        BaseMessages.getString(PKG, "RestConnectionEditor.KeyPassword.Tooltip"));
    FormData fdKeyPassword = new FormData();
    fdKeyPassword.left = new FormAttachment(middle, 0);
    fdKeyPassword.top = new FormAttachment(wKeyStoreType, margin);
    fdKeyPassword.right = new FormAttachment(100, 0);
    wKeyPassword.setLayoutData(fdKeyPassword);

    // Certificate alias (optional)
    Label wlCertificateAlias = new Label(gSSLTrustStore, SWT.RIGHT);
    wlCertificateAlias.setText(
        BaseMessages.getString(PKG, "RestConnectionEditor.CertificateAlias.Label"));
    PropsUi.setLook(wlCertificateAlias);
    FormData fdlCertificateAlias = new FormData();
    fdlCertificateAlias.left = new FormAttachment(0, 0);
    fdlCertificateAlias.top = new FormAttachment(wKeyPassword, margin);
    fdlCertificateAlias.right = new FormAttachment(middle, -margin);
    wlCertificateAlias.setLayoutData(fdlCertificateAlias);

    wCertificateAlias = new TextVar(variables, gSSLTrustStore, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCertificateAlias);
    wCertificateAlias.setToolTipText(
        BaseMessages.getString(PKG, "RestConnectionEditor.CertificateAlias.Tooltip"));
    FormData fdCertificateAlias = new FormData();
    fdCertificateAlias.left = new FormAttachment(middle, 0);
    fdCertificateAlias.top = new FormAttachment(wKeyPassword, margin);
    fdCertificateAlias.right = new FormAttachment(100, 0);
    wCertificateAlias.setLayoutData(fdCertificateAlias);

    FormData fdSSLTrustStore = new FormData();
    fdSSLTrustStore.left = new FormAttachment(0, 0);
    fdSSLTrustStore.right = new FormAttachment(100, 0);
    fdSSLTrustStore.top = new FormAttachment(gUrl, margin);
    gSSLTrustStore.setLayoutData(fdSSLTrustStore);
    // end SSL group

    setWidgetsContent();

    wsAuthComp.setExpandHorizontal(true);
    wsAuthComp.setExpandVertical(true);
    Rectangle authCompBounds = wAuthComp.getBounds();
    wsAuthComp.setMinSize(authCompBounds.width, authCompBounds.height);

    Control[] controls = {
      wName,
      wBaseUrl,
      wTestUrl,
      wAuthComp,
      wAuthType,
      wTrustStoreFile,
      wTrustStorePassword,
      wKeyStoreFile,
      wKeyStorePassword,
      wKeyStoreType,
      wKeyPassword,
      wCertificateAlias
    };
    enableControls(controls);
  }

  private void enableControls(Control[] controls) {
    for (Control control : controls) {
      if (control == null || control.isDisposed()) {
        continue;
      }
      control.addListener(SWT.Modify, e -> setChanged());
      control.addListener(SWT.Selection, e -> setChanged());
    }
  }

  private void clearAuthComp() {
    for (Control child : wAuthComp.getChildren()) {
      child.dispose();
    }
  }

  private void addNoAuthFields() {
    clearAuthComp();
    wAuthComp.pack();
    wAuthComp.redraw();
  }

  private void addBasicAuthFields() {
    clearAuthComp();

    Label wlUsername = new Label(wAuthComp, SWT.RIGHT);
    props.setLook(wlUsername);
    wlUsername.setText(BaseMessages.getString(PKG, "RestConnectionEditor.Basic.Username"));
    FormData fdlUsername = new FormData();
    fdlUsername.top = new FormAttachment(0, margin);
    fdlUsername.left = new FormAttachment(0, 0);
    fdlUsername.right = new FormAttachment(middle, -margin);
    wlUsername.setLayoutData(fdlUsername);

    wUsername = new TextVar(variables, wAuthComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wUsername);
    FormData fdUsername = new FormData();
    fdUsername.top = new FormAttachment(wlUsername, 0, SWT.CENTER);
    fdUsername.left = new FormAttachment(middle, 0);
    fdUsername.right = new FormAttachment(95, margin);
    wUsername.setLayoutData(fdUsername);
    lastControl = wlUsername;

    Label wlPassword = new Label(wAuthComp, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "RestConnectionEditor.Basic.Password"));
    FormData fdlPassword = new FormData();
    fdlPassword.top = new FormAttachment(lastControl, margin);
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);

    wPassword = new PasswordTextVar(variables, wAuthComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPassword);
    FormData fdPassword = new FormData();
    fdPassword.top = new FormAttachment(wlPassword, 0, SWT.CENTER);
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.right = new FormAttachment(95, 0);
    wPassword.setLayoutData(fdPassword);

    wAuthComp.pack();
    wAuthComp.redraw();

    Control[] controls = {wUsername, wPassword};
    enableControls(controls);
  }

  private void addBearerFields() {
    clearAuthComp();

    Label wlBearer = new Label(wAuthComp, SWT.RIGHT);
    PropsUi.setLook(wlBearer);
    wlBearer.setText(BaseMessages.getString(PKG, "RestConnectionEditor.Bearer.Token"));
    FormData fdlBearer = new FormData();
    fdlBearer.top = new FormAttachment(0, margin);
    fdlBearer.left = new FormAttachment(0, 0);
    fdlBearer.right = new FormAttachment(middle, -margin);
    wlBearer.setLayoutData(fdlBearer);

    wBearerValue = new PasswordTextVar(variables, wAuthComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBearerValue);
    FormData fdBearer = new FormData();
    fdBearer.top = new FormAttachment(wlBearer, 0, SWT.CENTER);
    fdBearer.left = new FormAttachment(middle, 0);
    fdBearer.right = new FormAttachment(95, 0);
    wBearerValue.setLayoutData(fdBearer);

    wAuthComp.pack();

    Control[] controls = {wBearerValue};
    enableControls(controls);
  }

  private void addApiKeyFields() {
    clearAuthComp();

    Label wlAuthorizationName = new Label(wAuthComp, SWT.RIGHT);
    PropsUi.setLook(wlAuthorizationName);
    wlAuthorizationName.setText(
        BaseMessages.getString(PKG, "RestConnectionEditor.API.AuthorizationName"));
    FormData fdlAuthorizationName = new FormData();
    fdlAuthorizationName.top = new FormAttachment(0, margin);
    fdlAuthorizationName.left = new FormAttachment(0, 0);
    fdlAuthorizationName.right = new FormAttachment(middle, -margin);
    wlAuthorizationName.setLayoutData(fdlAuthorizationName);

    wAuthorizationName = new TextVar(variables, wAuthComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAuthorizationName);
    FormData fdAuthorizationName = new FormData();
    fdAuthorizationName.top = new FormAttachment(wlAuthorizationName, 0, SWT.CENTER);
    fdAuthorizationName.left = new FormAttachment(middle, 0);
    fdAuthorizationName.right = new FormAttachment(95, 0);
    wAuthorizationName.setLayoutData(fdAuthorizationName);
    lastControl = wAuthorizationName;

    Label wlAuthorizationPrefix = new Label(wAuthComp, SWT.RIGHT);
    PropsUi.setLook(wlAuthorizationPrefix);
    wlAuthorizationPrefix.setText(
        BaseMessages.getString(PKG, "RestConnectionEditor.API.AuthorizationPrefix"));
    FormData fdlAuthorizationPrefix = new FormData();
    fdlAuthorizationPrefix.top = new FormAttachment(lastControl, margin);
    fdlAuthorizationPrefix.left = new FormAttachment(0, 0);
    fdlAuthorizationPrefix.right = new FormAttachment(middle, -margin);
    wlAuthorizationPrefix.setLayoutData(fdlAuthorizationPrefix);

    wAuthorizationPrefix = new TextVar(variables, wAuthComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAuthorizationPrefix);
    FormData fdAuthorizationPrefix = new FormData();
    fdAuthorizationPrefix.top = new FormAttachment(wlAuthorizationPrefix, 0, SWT.CENTER);
    fdAuthorizationPrefix.left = new FormAttachment(middle, 0);
    fdAuthorizationPrefix.right = new FormAttachment(95, 0);
    wAuthorizationPrefix.setLayoutData(fdAuthorizationPrefix);
    lastControl = wAuthorizationPrefix;

    Label wlAuthorizationValue = new Label(wAuthComp, SWT.RIGHT);
    PropsUi.setLook(wlAuthorizationValue);
    wlAuthorizationValue.setText(
        BaseMessages.getString(PKG, "RestConnectionEditor.API.AuthorizationValue"));
    FormData fdlAuthorizationValue = new FormData();
    fdlAuthorizationValue.top = new FormAttachment(lastControl, margin);
    fdlAuthorizationValue.left = new FormAttachment(0, 0);
    fdlAuthorizationValue.right = new FormAttachment(middle, -margin);
    wlAuthorizationValue.setLayoutData(fdlAuthorizationValue);
    wAuthorizationValue =
        new PasswordTextVar(variables, wAuthComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAuthorizationValue);
    FormData fdAuthorizationValue = new FormData();
    fdAuthorizationValue.top = new FormAttachment(wlAuthorizationValue, 0, SWT.CENTER);
    fdAuthorizationValue.left = new FormAttachment(middle, 0);
    fdAuthorizationValue.right = new FormAttachment(95, 0);
    wAuthorizationValue.setLayoutData(fdAuthorizationValue);

    wAuthComp.pack();

    Control[] controls = {wAuthorizationName, wAuthorizationPrefix, wAuthorizationValue};
    enableControls(controls);
  }

  private void addCertificateFields() {
    clearAuthComp();

    Label wlInfo = new Label(wAuthComp, SWT.LEFT | SWT.WRAP);
    wlInfo.setText(BaseMessages.getString(PKG, "RestConnectionEditor.Certificate.Info"));
    PropsUi.setLook(wlInfo);
    FormData fdlInfo = new FormData();
    fdlInfo.top = new FormAttachment(0, margin);
    fdlInfo.left = new FormAttachment(0, 0);
    fdlInfo.right = new FormAttachment(100, 0);
    wlInfo.setLayoutData(fdlInfo);

    wAuthComp.pack();
    wAuthComp.redraw();
  }

  @Override
  public Button[] createButtonsForButtonBar(Composite composite) {
    Button wTest = new Button(composite, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "System.Button.Test"));
    wTest.addListener(SWT.Selection, e -> test());

    return new Button[] {wTest};
  }

  private void test() {
    IVariables variables = hopGui.getVariables();
    RestConnection restConnection = new RestConnection(variables);
    restConnection.setName(wName.getText());
    if (StringUtils.isEmpty(wTestUrl.getText())) {
      restConnection.setTestUrl(wBaseUrl.getText());
    } else {
      restConnection.setTestUrl(wTestUrl.getText());
    }
    restConnection.setBaseUrl(wBaseUrl.getText());
    restConnection.setTestUrl(wTestUrl.getText());

    // SSL/TLS configuration
    restConnection.setTrustStoreFile(wTrustStoreFile.getText());
    restConnection.setTrustStorePassword(wTrustStorePassword.getText());
    restConnection.setIgnoreSsl(wIgnoreSsl.getSelection());
    restConnection.setKeyStoreFile(wKeyStoreFile.getText());
    restConnection.setKeyStorePassword(wKeyStorePassword.getText());
    restConnection.setKeyStoreType(wKeyStoreType.getText());
    restConnection.setKeyPassword(wKeyPassword.getText());
    restConnection.setCertificateAlias(wCertificateAlias.getText());

    // Authentication configuration
    restConnection.setAuthType(wAuthType.getText());
    if (wAuthType.getText().equals("No Auth")) {
      // nothing required
    } else if (wAuthType.getText().equals("Basic")) {
      restConnection.setUsername(wUsername.getText());
      restConnection.setPassword(wPassword.getText());
    } else if (wAuthType.getText().equals("Bearer")) {
      restConnection.setBearerToken(wBearerValue.getText());
    } else if (wAuthType.getText().equals("API Key")) {
      restConnection.setAuthorizationHeaderName(wAuthorizationName.getText());
      restConnection.setAuthorizationPrefix(wAuthorizationPrefix.getText());
      restConnection.setAuthorizationHeaderValue(wAuthorizationValue.getText());
    } else if (wAuthType.getText().equals("Certificate")) {
      // Certificate fields already set in SSL section above
    }

    // TODO: remove this temporary debug dialog when the SSL configuration issue is resolved.
    String resolvedTestUrl = variables.resolve(restConnection.getTestUrl());
    String responsePreview;
    try {
      responsePreview = restConnection.getResponse(resolvedTestUrl);
      if (responsePreview != null && responsePreview.length() > 500) {
        responsePreview = responsePreview.substring(0, 500) + "... (truncated)";
      }
    } catch (Exception e) {
      responsePreview = "<error retrieving response: " + Const.NVL(e.getMessage(), "unknown") + ">";
    }

    String debugSummary =
        "ignoreSsl = "
            + restConnection.isIgnoreSsl()
            + "\ntrustStore (raw) = "
            + Const.NVL(restConnection.getTrustStoreFile(), "<empty>")
            + "\ntrustStore (resolved) = "
            + (StringUtils.isEmpty(restConnection.getTrustStoreFile())
                ? "<empty>"
                : variables.resolve(restConnection.getTrustStoreFile()))
            + "\nkeyStore (raw) = "
            + Const.NVL(restConnection.getKeyStoreFile(), "<empty>")
            + "\nkeyStore (resolved) = "
            + (StringUtils.isEmpty(restConnection.getKeyStoreFile())
                ? "<empty>"
                : variables.resolve(restConnection.getKeyStoreFile()))
            + "\nkeyStore type = "
            + Const.NVL(restConnection.getKeyStoreType(), "<empty>")
            + "\nkey password set = "
            + (!StringUtils.isEmpty(restConnection.getKeyPassword()))
            + "\nauthType = "
            + Const.NVL(restConnection.getAuthType(), "<empty>")
            + "\ntest URL (resolved) = "
            + resolvedTestUrl
            + "\nresponse preview = "
            + Const.NVL(responsePreview, "<null>");
    MessageBox debugBox = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
    debugBox.setText("REST Connection Test - Debug Info");
    debugBox.setMessage(debugSummary + "\n\nClick OK to continue the test.");
    debugBox.open();

    try {
      restConnection.testConnection();
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK);
      box.setText("OK");
      String message =
          BaseMessages.getString(PKG, "RestConnectionEditor.ConnectionTestSuccess") + Const.CR;
      message += Const.CR;
      message += "URL : " + restConnection.getTestUrl();
      box.setMessage(message);
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(), "Error", "Error connecting to REST URL : " + wTestUrl.getText(), e);
    }
  }

  @Override
  public void dispose() {}

  @Override
  public void setWidgetsContent() {
    // backwards compatibility: if we have authorization header values but no Auth Type,
    // consider this to be an API Key auth
    if (!StringUtils.isEmpty(metadata.getAuthorizationHeaderName())
        && !StringUtils.isEmpty(metadata.getAuthorizationHeaderValue())
        && StringUtils.isEmpty(metadata.getAuthType())) {
      metadata.setAuthType("API Key");
    }

    wName.setText(Const.NVL(metadata.getName(), ""));
    wBaseUrl.setText(Const.NVL(metadata.getBaseUrl(), ""));
    wTestUrl.setText(Const.NVL(metadata.getTestUrl(), ""));

    wTrustStoreFile.setText(Const.NVL(metadata.getTrustStoreFile(), ""));
    wTrustStorePassword.setText(Const.NVL(metadata.getTrustStorePassword(), ""));
    wIgnoreSsl.setSelection(metadata.isIgnoreSsl());

    wKeyStoreFile.setText(Const.NVL(metadata.getKeyStoreFile(), ""));
    wKeyStorePassword.setText(Const.NVL(metadata.getKeyStorePassword(), ""));
    wKeyStoreType.setText(Const.NVL(metadata.getKeyStoreType(), "PKCS12"));
    wKeyPassword.setText(Const.NVL(metadata.getKeyPassword(), ""));
    wCertificateAlias.setText(Const.NVL(metadata.getCertificateAlias(), ""));

    if (StringUtils.isEmpty(metadata.getAuthType())) {
      metadata.setAuthType("No Auth");
      wAuthType.select(0);
    } else {
      wAuthType.select(Arrays.asList(authTypes).indexOf(metadata.getAuthType()));
    }
    switch (metadata.getAuthType()) {
      case "Basic" -> {
        addBasicAuthFields();
        wUsername.setText(Const.NVL(metadata.getUsername(), ""));
        wPassword.setText(Const.NVL(metadata.getPassword(), ""));
      }
      case "Bearer" -> {
        addBearerFields();
        wBearerValue.setText(metadata.getBearerToken());
      }
      case "API Key" -> {
        addApiKeyFields();
        wAuthorizationName.setText(Const.NVL(metadata.getAuthorizationHeaderName(), ""));
        wAuthorizationPrefix.setText(Const.NVL(metadata.getAuthorizationPrefix(), ""));
        wAuthorizationValue.setText(Const.NVL(metadata.getAuthorizationHeaderValue(), ""));
      }
      case "Certificate" -> addCertificateFields();
    }
  }

  @Override
  public void getWidgetsContent(RestConnection connection) {
    connection.setName(wName.getText());
    connection.setBaseUrl(wBaseUrl.getText());
    connection.setTestUrl(wTestUrl.getText());

    connection.setTrustStoreFile(wTrustStoreFile.getText());
    connection.setTrustStorePassword(wTrustStorePassword.getText());
    connection.setIgnoreSsl(wIgnoreSsl.getSelection());

    connection.setKeyStoreFile(wKeyStoreFile.getText());
    connection.setKeyStorePassword(wKeyStorePassword.getText());
    connection.setKeyStoreType(wKeyStoreType.getText());
    connection.setKeyPassword(wKeyPassword.getText());
    connection.setCertificateAlias(wCertificateAlias.getText());

    connection.setAuthType(wAuthType.getText());
    if (wAuthType.getText().equals("Basic")) {
      connection.setUsername(wUsername.getText());
      connection.setPassword(wPassword.getText());
    } else if (wAuthType.getText().equals("Bearer")) {
      connection.setBearerToken(wBearerValue.getText());
    } else if (wAuthType.getText().equals("API Key")) {
      connection.setAuthorizationHeaderName(wAuthorizationName.getText());
      connection.setAuthorizationPrefix(wAuthorizationPrefix.getText());
      connection.setAuthorizationHeaderValue(wAuthorizationValue.getText());
    }
    // Note: Certificate auth doesn't have additional fields in auth section
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }

  private void activateTrustoreFields() {
    wTrustStoreFile.setEnabled(!wIgnoreSsl.getSelection());
    wbTrustStoreFile.setEnabled(!wIgnoreSsl.getSelection());
    wTrustStorePassword.setEnabled(!wIgnoreSsl.getSelection());
  }
}
