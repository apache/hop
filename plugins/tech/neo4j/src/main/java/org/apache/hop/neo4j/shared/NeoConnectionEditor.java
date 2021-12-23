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
 *
 */

package org.apache.hop.neo4j.shared;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.*;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

/** Dialog that allows you to edit the settings of a Neo4j connection */
public class NeoConnectionEditor extends MetadataEditor<NeoConnection> {
  private static final Class<?> PKG = NeoConnectionEditor.class; // for Translator2

  // Connection properties
  //
  private Text wName;
  private Label wlServer;
  private TextVar wServer;
  private Label wlDatabaseName;
  private TextVar wDatabaseName;
  private CheckBoxVar wVersion4;
  private Label wlBoltPort;
  private TextVar wBoltPort;
  private TextVar wBrowserPort;
  private Label wlPolicy;
  private TextVar wPolicy;
  private TextVar wUsername;
  private TextVar wPassword;
  private Label wlRouting;
  private CheckBoxVar wRouting;
  private Label wlEncryption;
  private CheckBoxVar wEncryption;
  private Label wlTrustAllCertificates;
  private CheckBoxVar wTrustAllCertificates;

  private Group gAdvanced;
  private TextVar wConnectionLivenessCheckTimeout;
  private TextVar wMaxConnectionLifetime;
  private TextVar wMaxConnectionPoolSize;
  private TextVar wConnectionAcquisitionTimeout;
  private TextVar wConnectionTimeout;
  private TextVar wMaxTransactionRetryTime;

  private TableView wUrls;

  public NeoConnectionEditor(
      HopGui hopGui, MetadataManager<NeoConnection> manager, NeoConnection neoConnection) {
    super(hopGui, manager, neoConnection);
  }

  @Override
  public void createControl(Composite composite) {
    PropsUi props = PropsUi.getInstance();

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN + 2;

    IVariables variables = getHopGui().getVariables();

    // The name
    Label wlName = new Label(composite, SWT.RIGHT);
    props.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin);
    fdlName.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, 0); // To the right of the label
    fdName.right = new FormAttachment(95, 0);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    // The server
    wlServer = new Label(composite, SWT.RIGHT);
    props.setLook(wlServer);
    wlServer.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Server.Label"));
    FormData fdlServer = new FormData();
    fdlServer.top = new FormAttachment(lastControl, margin);
    fdlServer.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlServer.right = new FormAttachment(middle, -margin);
    wlServer.setLayoutData(fdlServer);
    wServer = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wServer);
    FormData fdServer = new FormData();
    fdServer.top = new FormAttachment(wlServer, 0, SWT.CENTER);
    fdServer.left = new FormAttachment(middle, 0); // To the right of the label
    fdServer.right = new FormAttachment(95, 0);
    wServer.setLayoutData(fdServer);
    lastControl = wServer;

    // The DatabaseName
    wlDatabaseName = new Label(composite, SWT.RIGHT);
    props.setLook(wlDatabaseName);
    wlDatabaseName.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.DatabaseName.Label"));
    FormData fdlDatabaseName = new FormData();
    fdlDatabaseName.top = new FormAttachment(lastControl, margin);
    fdlDatabaseName.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlDatabaseName.right = new FormAttachment(middle, -margin);
    wlDatabaseName.setLayoutData(fdlDatabaseName);
    wDatabaseName = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wDatabaseName);
    FormData fdDatabaseName = new FormData();
    fdDatabaseName.top = new FormAttachment(wlDatabaseName, 0, SWT.CENTER);
    fdDatabaseName.left = new FormAttachment(middle, 0); // To the right of the label
    fdDatabaseName.right = new FormAttachment(95, 0);
    wDatabaseName.setLayoutData(fdDatabaseName);
    lastControl = wDatabaseName;

    // Version4?
    Label wlVersion4 = new Label(composite, SWT.RIGHT);
    wlVersion4.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Version4.Label"));
    props.setLook(wlVersion4);
    FormData fdlVersion4 = new FormData();
    fdlVersion4.top = new FormAttachment(lastControl, margin);
    fdlVersion4.left = new FormAttachment(0, 0);
    fdlVersion4.right = new FormAttachment(middle, -margin);
    wlVersion4.setLayoutData(fdlVersion4);
    wVersion4 = new CheckBoxVar(variables, composite, SWT.CHECK);
    props.setLook(wVersion4);
    FormData fdVersion4 = new FormData();
    fdVersion4.top = new FormAttachment(wlVersion4, 0, SWT.CENTER);
    fdVersion4.left = new FormAttachment(middle, 0);
    fdVersion4.right = new FormAttachment(95, 0);
    wVersion4.setLayoutData(fdVersion4);
    lastControl = wVersion4;

    // Bolt port?
    wlBoltPort = new Label(composite, SWT.RIGHT);
    props.setLook(wlBoltPort);
    wlBoltPort.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.BoltPort.Label"));
    FormData fdlBoltPort = new FormData();
    fdlBoltPort.top = new FormAttachment(lastControl, margin);
    fdlBoltPort.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlBoltPort.right = new FormAttachment(middle, -margin);
    wlBoltPort.setLayoutData(fdlBoltPort);
    wBoltPort = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBoltPort);
    FormData fdBoltPort = new FormData();
    fdBoltPort.top = new FormAttachment(wlBoltPort, 0, SWT.CENTER);
    fdBoltPort.left = new FormAttachment(middle, 0); // To the right of the label
    fdBoltPort.right = new FormAttachment(95, 0);
    wBoltPort.setLayoutData(fdBoltPort);
    lastControl = wBoltPort;

    // Browser port?
    Label wlBrowserPort = new Label(composite, SWT.RIGHT);
    props.setLook(wlBrowserPort);
    wlBrowserPort.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.BrowserPort.Label"));
    FormData fdlBrowserPort = new FormData();
    fdlBrowserPort.top = new FormAttachment(lastControl, margin);
    fdlBrowserPort.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlBrowserPort.right = new FormAttachment(middle, -margin);
    wlBrowserPort.setLayoutData(fdlBrowserPort);
    wBrowserPort = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBrowserPort);
    FormData fdBrowserPort = new FormData();
    fdBrowserPort.top = new FormAttachment(wlBrowserPort, 0, SWT.CENTER);
    fdBrowserPort.left = new FormAttachment(middle, 0); // To the right of the label
    fdBrowserPort.right = new FormAttachment(95, 0);
    wBrowserPort.setLayoutData(fdBrowserPort);
    lastControl = wBrowserPort;

    // Https
    wlRouting = new Label(composite, SWT.RIGHT);
    wlRouting.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Routing.Label"));
    props.setLook(wlRouting);
    FormData fdlRouting = new FormData();
    fdlRouting.top = new FormAttachment(lastControl, margin);
    fdlRouting.left = new FormAttachment(0, 0);
    fdlRouting.right = new FormAttachment(middle, -margin);
    wlRouting.setLayoutData(fdlRouting);
    wRouting = new CheckBoxVar(variables, composite, SWT.CHECK);
    props.setLook(wRouting);
    FormData fdRouting = new FormData();
    fdRouting.top = new FormAttachment(wlRouting, 0, SWT.CENTER);
    fdRouting.left = new FormAttachment(middle, 0);
    fdRouting.right = new FormAttachment(95, 0);
    wRouting.setLayoutData(fdRouting);
    lastControl = wRouting;

    // Policy
    wlPolicy = new Label(composite, SWT.RIGHT);
    wlPolicy.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Policy.Label"));
    props.setLook(wlPolicy);
    FormData fdlPolicy = new FormData();
    fdlPolicy.top = new FormAttachment(lastControl, margin);
    fdlPolicy.left = new FormAttachment(0, 0);
    fdlPolicy.right = new FormAttachment(middle, -margin);
    wlPolicy.setLayoutData(fdlPolicy);
    wPolicy = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPolicy);
    FormData fdPolicy = new FormData();
    fdPolicy.top = new FormAttachment(wlPolicy, 0, SWT.CENTER);
    fdPolicy.left = new FormAttachment(middle, 0);
    fdPolicy.right = new FormAttachment(95, 0);
    wPolicy.setLayoutData(fdPolicy);
    lastControl = wPolicy;

    wRouting.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            enableFields();
          }
        });

    // Username
    Label wlUsername = new Label(composite, SWT.RIGHT);
    wlUsername.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.UserName.Label"));
    props.setLook(wlUsername);
    FormData fdlUsername = new FormData();
    fdlUsername.top = new FormAttachment(lastControl, margin);
    fdlUsername.left = new FormAttachment(0, 0);
    fdlUsername.right = new FormAttachment(middle, -margin);
    wlUsername.setLayoutData(fdlUsername);
    wUsername = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wUsername);
    FormData fdUsername = new FormData();
    fdUsername.top = new FormAttachment(wlUsername, 0, SWT.CENTER);
    fdUsername.left = new FormAttachment(middle, 0);
    fdUsername.right = new FormAttachment(95, 0);
    wUsername.setLayoutData(fdUsername);
    lastControl = wUsername;

    // Password
    Label wlPassword = new Label(composite, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Password.Label"));
    props.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.top = new FormAttachment(wUsername, margin);
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new PasswordTextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPassword);
    FormData fdPassword = new FormData();
    fdPassword.top = new FormAttachment(wlPassword, 0, SWT.CENTER);
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.right = new FormAttachment(95, 0);
    wPassword.setLayoutData(fdPassword);
    lastControl = wPassword;

    // Encryption?
    wlEncryption = new Label(composite, SWT.RIGHT);
    wlEncryption.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Encryption.Label"));
    props.setLook(wlEncryption);
    FormData fdlEncryption = new FormData();
    fdlEncryption.top = new FormAttachment(lastControl, margin);
    fdlEncryption.left = new FormAttachment(0, 0);
    fdlEncryption.right = new FormAttachment(middle, -margin);
    wlEncryption.setLayoutData(fdlEncryption);
    wEncryption = new CheckBoxVar(variables, composite, SWT.CHECK);
    props.setLook(wEncryption);
    FormData fdEncryption = new FormData();
    fdEncryption.top = new FormAttachment(wlEncryption, 0, SWT.CENTER);
    fdEncryption.left = new FormAttachment(middle, 0);
    fdEncryption.right = new FormAttachment(95, 0);
    wEncryption.setLayoutData(fdEncryption);
    wEncryption.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            enableFields();
          }
        });
    lastControl = wEncryption;

    // Trust Level?
    wlTrustAllCertificates = new Label(composite, SWT.RIGHT);
    wlTrustAllCertificates.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.TrustAllCertificates.Label"));
    props.setLook(wlTrustAllCertificates);
    FormData fdlTrustAllCertificates = new FormData();
    fdlTrustAllCertificates.top = new FormAttachment(lastControl, margin);
    fdlTrustAllCertificates.left = new FormAttachment(0, 0);
    fdlTrustAllCertificates.right = new FormAttachment(middle, -margin);
    wlTrustAllCertificates.setLayoutData(fdlTrustAllCertificates);
    wTrustAllCertificates = new CheckBoxVar(variables, composite, SWT.CHECK);
    props.setLook(wEncryption);
    FormData fdTrustAllCertificates = new FormData();
    fdTrustAllCertificates.top = new FormAttachment(wlTrustAllCertificates, 0, SWT.CENTER);
    fdTrustAllCertificates.left = new FormAttachment(middle, 0);
    fdTrustAllCertificates.right = new FormAttachment(95, 0);
    wTrustAllCertificates.setLayoutData(fdTrustAllCertificates);
    lastControl = wlTrustAllCertificates;

    gAdvanced = new Group(composite, SWT.SHADOW_ETCHED_IN);
    props.setLook(gAdvanced);
    FormLayout advancedLayout = new FormLayout();
    advancedLayout.marginTop = margin;
    advancedLayout.marginBottom = margin;
    gAdvanced.setLayout(advancedLayout);
    gAdvanced.setText("Advanced options");

    // ConnectionLivenessCheckTimeout
    Label wlConnectionLivenessCheckTimeout = new Label(gAdvanced, SWT.RIGHT);
    wlConnectionLivenessCheckTimeout.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.ConnectionLivenessCheckTimeout.Label"));
    props.setLook(wlConnectionLivenessCheckTimeout);
    FormData fdlConnectionLivenessCheckTimeout = new FormData();
    fdlConnectionLivenessCheckTimeout.top = new FormAttachment(0, 0);
    fdlConnectionLivenessCheckTimeout.left = new FormAttachment(0, 0);
    fdlConnectionLivenessCheckTimeout.right = new FormAttachment(middle, -margin);
    wlConnectionLivenessCheckTimeout.setLayoutData(fdlConnectionLivenessCheckTimeout);
    wConnectionLivenessCheckTimeout =
        new TextVar(variables, gAdvanced, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wConnectionLivenessCheckTimeout);
    FormData fdConnectionLivenessCheckTimeout = new FormData();
    fdConnectionLivenessCheckTimeout.top =
        new FormAttachment(wlConnectionLivenessCheckTimeout, 0, SWT.CENTER);
    fdConnectionLivenessCheckTimeout.left = new FormAttachment(middle, 0);
    fdConnectionLivenessCheckTimeout.right = new FormAttachment(95, 0);
    wConnectionLivenessCheckTimeout.setLayoutData(fdConnectionLivenessCheckTimeout);
    Control lastGroupControl = wConnectionLivenessCheckTimeout;

    // MaxConnectionLifetime
    Label wlMaxConnectionLifetime = new Label(gAdvanced, SWT.RIGHT);
    wlMaxConnectionLifetime.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.MaxConnectionLifetime.Label"));
    props.setLook(wlMaxConnectionLifetime);
    FormData fdlMaxConnectionLifetime = new FormData();
    fdlMaxConnectionLifetime.top = new FormAttachment(lastGroupControl, margin);
    fdlMaxConnectionLifetime.left = new FormAttachment(0, 0);
    fdlMaxConnectionLifetime.right = new FormAttachment(middle, -margin);
    wlMaxConnectionLifetime.setLayoutData(fdlMaxConnectionLifetime);
    wMaxConnectionLifetime = new TextVar(variables, gAdvanced, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMaxConnectionLifetime);
    FormData fdMaxConnectionLifetime = new FormData();
    fdMaxConnectionLifetime.top = new FormAttachment(wlMaxConnectionLifetime, 0, SWT.CENTER);
    fdMaxConnectionLifetime.left = new FormAttachment(middle, 0);
    fdMaxConnectionLifetime.right = new FormAttachment(95, 0);
    wMaxConnectionLifetime.setLayoutData(fdMaxConnectionLifetime);
    lastGroupControl = wMaxConnectionLifetime;

    // MaxConnectionPoolSize
    Label wlMaxConnectionPoolSize = new Label(gAdvanced, SWT.RIGHT);
    wlMaxConnectionPoolSize.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.MaxConnectionPoolSize.Label"));
    props.setLook(wlMaxConnectionPoolSize);
    FormData fdlMaxConnectionPoolSize = new FormData();
    fdlMaxConnectionPoolSize.top = new FormAttachment(lastGroupControl, margin);
    fdlMaxConnectionPoolSize.left = new FormAttachment(0, 0);
    fdlMaxConnectionPoolSize.right = new FormAttachment(middle, -margin);
    wlMaxConnectionPoolSize.setLayoutData(fdlMaxConnectionPoolSize);
    wMaxConnectionPoolSize = new TextVar(variables, gAdvanced, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMaxConnectionPoolSize);
    FormData fdMaxConnectionPoolSize = new FormData();
    fdMaxConnectionPoolSize.top = new FormAttachment(wlMaxConnectionPoolSize, 0, SWT.CENTER);
    fdMaxConnectionPoolSize.left = new FormAttachment(middle, 0);
    fdMaxConnectionPoolSize.right = new FormAttachment(95, 0);
    wMaxConnectionPoolSize.setLayoutData(fdMaxConnectionPoolSize);
    lastGroupControl = wMaxConnectionPoolSize;

    // ConnectionAcquisitionTimeout
    Label wlConnectionAcquisitionTimeout = new Label(gAdvanced, SWT.RIGHT);
    wlConnectionAcquisitionTimeout.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.ConnectionAcquisitionTimeout.Label"));
    props.setLook(wlConnectionAcquisitionTimeout);
    FormData fdlConnectionAcquisitionTimeout = new FormData();
    fdlConnectionAcquisitionTimeout.top = new FormAttachment(lastGroupControl, margin);
    fdlConnectionAcquisitionTimeout.left = new FormAttachment(0, 0);
    fdlConnectionAcquisitionTimeout.right = new FormAttachment(middle, -margin);
    wlConnectionAcquisitionTimeout.setLayoutData(fdlConnectionAcquisitionTimeout);
    wConnectionAcquisitionTimeout =
        new TextVar(variables, gAdvanced, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wConnectionAcquisitionTimeout);
    FormData fdConnectionAcquisitionTimeout = new FormData();
    fdConnectionAcquisitionTimeout.top =
        new FormAttachment(wlConnectionAcquisitionTimeout, 0, SWT.CENTER);
    fdConnectionAcquisitionTimeout.left = new FormAttachment(middle, 0);
    fdConnectionAcquisitionTimeout.right = new FormAttachment(95, 0);
    wConnectionAcquisitionTimeout.setLayoutData(fdConnectionAcquisitionTimeout);
    lastGroupControl = wConnectionAcquisitionTimeout;

    // ConnectionTimeout
    Label wlConnectionTimeout = new Label(gAdvanced, SWT.RIGHT);
    wlConnectionTimeout.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.ConnectionTimeout.Label"));
    props.setLook(wlConnectionTimeout);
    FormData fdlConnectionTimeout = new FormData();
    fdlConnectionTimeout.top = new FormAttachment(lastGroupControl, margin);
    fdlConnectionTimeout.left = new FormAttachment(0, 0);
    fdlConnectionTimeout.right = new FormAttachment(middle, -margin);
    wlConnectionTimeout.setLayoutData(fdlConnectionTimeout);
    wConnectionTimeout = new TextVar(variables, gAdvanced, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wConnectionTimeout);
    FormData fdConnectionTimeout = new FormData();
    fdConnectionTimeout.top = new FormAttachment(wlConnectionTimeout, 0, SWT.CENTER);
    fdConnectionTimeout.left = new FormAttachment(middle, 0);
    fdConnectionTimeout.right = new FormAttachment(95, 0);
    wConnectionTimeout.setLayoutData(fdConnectionTimeout);
    lastGroupControl = wConnectionTimeout;

    // MaxTransactionRetryTime
    Label wlMaxTransactionRetryTime = new Label(gAdvanced, SWT.RIGHT);
    wlMaxTransactionRetryTime.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.MaxTransactionRetryTime.Label"));
    props.setLook(wlMaxTransactionRetryTime);
    FormData fdlMaxTransactionRetryTime = new FormData();
    fdlMaxTransactionRetryTime.top = new FormAttachment(lastGroupControl, margin);
    fdlMaxTransactionRetryTime.left = new FormAttachment(0, 0);
    fdlMaxTransactionRetryTime.right = new FormAttachment(middle, -margin);
    wlMaxTransactionRetryTime.setLayoutData(fdlMaxTransactionRetryTime);
    wMaxTransactionRetryTime =
        new TextVar(variables, gAdvanced, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMaxTransactionRetryTime);
    FormData fdMaxTransactionRetryTime = new FormData();
    fdMaxTransactionRetryTime.top = new FormAttachment(wlMaxTransactionRetryTime, 0, SWT.CENTER);
    fdMaxTransactionRetryTime.left = new FormAttachment(middle, 0);
    fdMaxTransactionRetryTime.right = new FormAttachment(95, 0);
    wMaxTransactionRetryTime.setLayoutData(fdMaxTransactionRetryTime);
    lastGroupControl = wMaxTransactionRetryTime;

    // End of Advanced group
    //
    FormData fdgAdvanced = new FormData();
    fdgAdvanced.left = new FormAttachment(0, 0);
    fdgAdvanced.right = new FormAttachment(100, 0);
    fdgAdvanced.top = new FormAttachment(lastControl, margin * 2);
    gAdvanced.setLayoutData(fdgAdvanced);
    lastControl = gAdvanced;

    // The URLs group...
    //
    Group gUrls = new Group(composite, SWT.SHADOW_ETCHED_IN);
    props.setLook(gUrls);
    FormLayout urlsLayout = new FormLayout();
    urlsLayout.marginTop = margin;
    urlsLayout.marginBottom = margin;
    gUrls.setLayout(urlsLayout);
    gUrls.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.URLs.Label"));

    // URLs
    wUrls =
        new TableView(
            variables,
            gUrls,
            SWT.NONE,
            new ColumnInfo[] {
              new ColumnInfo(
                  BaseMessages.getString(PKG, "NeoConnectionEditor.URLColumn.Label"),
                  ColumnInfo.COLUMN_TYPE_TEXT)
            },
            getMetadata().getManualUrls().size(),
            e -> setChanged(),
            props);
    wUrls.table.addListener(
        SWT.Selection,
        e -> {
          enableFields();
        });
    wUrls.table.addListener(
        SWT.MouseDown,
        e -> {
          enableFields();
        });
    wUrls.table.addListener(
        SWT.MouseUp,
        e -> {
          enableFields();
        });
    wUrls.table.addListener(
        SWT.FocusIn,
        e -> {
          enableFields();
        });
    wUrls.table.addListener(
        SWT.FocusOut,
        e -> {
          enableFields();
        });
    wUrls.addModifyListener(
        e -> {
          enableFields();
        });

    FormData fdUrls = new FormData();
    fdUrls.top = new FormAttachment(0, 0);
    fdUrls.left = new FormAttachment(0, 0);
    fdUrls.right = new FormAttachment(100, 0);
    fdUrls.bottom = new FormAttachment(100, 0);
    wUrls.setLayoutData(fdUrls);

    // End of Advanced group
    //
    FormData fdgUrls = new FormData();
    fdgUrls.left = new FormAttachment(0, 0);
    fdgUrls.right = new FormAttachment(100, 0);
    fdgUrls.top = new FormAttachment(lastControl, margin * 2);
    fdgUrls.bottom = new FormAttachment(100, -margin * 2);
    gUrls.setLayoutData(fdgUrls);

    setWidgetsContent();

    // Add modify listeners to all controls.
    // This will inform the Metadata perspective in the Hop GUI that this object was modified and
    // needs to be saved.
    //
    Listener modifyListener = e -> setChanged();
    wName.addListener(SWT.Modify, modifyListener);
    wServer.addListener(SWT.Modify, modifyListener);
    wDatabaseName.addListener(SWT.Modify, modifyListener);
    wVersion4.addListener(SWT.Selection, modifyListener);
    wBoltPort.addListener(SWT.Selection, modifyListener);
    wBrowserPort.addListener(SWT.Modify, modifyListener);
    wPolicy.addListener(SWT.Modify, modifyListener);
    wUsername.addListener(SWT.Modify, modifyListener);
    wPassword.addListener(SWT.Modify, modifyListener);
    wRouting.addListener(SWT.Selection, modifyListener);
    wEncryption.addListener(SWT.Selection, modifyListener);
    wTrustAllCertificates.addListener(SWT.Selection, modifyListener);
    wConnectionLivenessCheckTimeout.addListener(SWT.Modify, modifyListener);
    wMaxConnectionLifetime.addListener(SWT.Modify, modifyListener);
    wMaxConnectionPoolSize.addListener(SWT.Modify, modifyListener);
    wConnectionAcquisitionTimeout.addListener(SWT.Modify, modifyListener);
    wConnectionTimeout.addListener(SWT.Modify, modifyListener);
    wMaxTransactionRetryTime.addListener(SWT.Modify, modifyListener);
  }

  private void enableFields() {
    wlPolicy.setEnabled(wRouting.getSelection());
    wPolicy.setEnabled(wRouting.getSelection());

    boolean hasNoUrls = wUrls.nrNonEmpty() == 0;
    for (Control control :
        new Control[] {
          wlServer,
          wServer,
          wlDatabaseName,
          wDatabaseName,
          wlBoltPort,
          wBoltPort,
          wlRouting,
          wRouting,
          wlPolicy,
          wPolicy,
          wlEncryption,
          wEncryption,
          gAdvanced
        }) {
      control.setEnabled(hasNoUrls);
    }

    boolean encryption = wEncryption.getSelection();
    wlTrustAllCertificates.setEnabled(encryption);
    wTrustAllCertificates.setEnabled(encryption);
    wTrustAllCertificates.getTextVar().setEnabled(encryption);
  }

  @Override
  public void setWidgetsContent() {
    wName.setText(Const.NVL(metadata.getName(), ""));
    wServer.setText(Const.NVL(metadata.getServer(), ""));
    wDatabaseName.setText(Const.NVL(metadata.getDatabaseName(), ""));
    wVersion4.setSelection(metadata.isVersion4());
    wVersion4.setVariableName(Const.NVL(metadata.getVersion4Variable(), ""));
    wBoltPort.setText(Const.NVL(metadata.getBoltPort(), ""));
    wBrowserPort.setText(Const.NVL(metadata.getBrowserPort(), ""));
    wRouting.setSelection(metadata.isRouting());
    wRouting.setVariableName(Const.NVL(metadata.getRoutingVariable(), ""));
    wPolicy.setText(Const.NVL(metadata.getRoutingPolicy(), ""));
    wUsername.setText(Const.NVL(metadata.getUsername(), ""));
    wPassword.setText(Const.NVL(metadata.getPassword(), ""));
    wEncryption.setSelection(metadata.isUsingEncryption());
    wEncryption.setVariableName(Const.NVL(metadata.getUsingEncryptionVariable(), ""));
    wTrustAllCertificates.setSelection(metadata.isTrustAllCertificates());
    wTrustAllCertificates.setVariableName(
        Const.NVL(metadata.getTrustAllCertificatesVariable(), ""));
    wConnectionLivenessCheckTimeout.setText(
        Const.NVL(metadata.getConnectionLivenessCheckTimeout(), ""));
    wMaxConnectionLifetime.setText(Const.NVL(metadata.getMaxConnectionLifetime(), ""));
    wMaxConnectionPoolSize.setText(Const.NVL(metadata.getMaxConnectionPoolSize(), ""));
    wConnectionAcquisitionTimeout.setText(
        Const.NVL(metadata.getConnectionAcquisitionTimeout(), ""));
    wConnectionTimeout.setText(Const.NVL(metadata.getConnectionTimeout(), ""));
    wMaxTransactionRetryTime.setText(Const.NVL(metadata.getMaxTransactionRetryTime(), ""));
    for (int i = 0; i < metadata.getManualUrls().size(); i++) {
      TableItem item = wUrls.table.getItem(i);
      item.setText(1, Const.NVL(metadata.getManualUrls().get(i), ""));
    }
    wUrls.setRowNums();
    wUrls.optWidth(true);

    enableFields();
    wName.setFocus();
  }

  @Override
  public void getWidgetsContent(NeoConnection neoConnection) {
    neoConnection.setName(wName.getText());
    neoConnection.setServer(wServer.getText());
    neoConnection.setDatabaseName(wDatabaseName.getText());
    neoConnection.setVersion4(wVersion4.getSelection());
    neoConnection.setVersion4Variable(wVersion4.getVariableName());
    neoConnection.setBoltPort(wBoltPort.getText());
    neoConnection.setBrowserPort(wBrowserPort.getText());
    neoConnection.setRouting(wRouting.getSelection());
    neoConnection.setRoutingVariable(wRouting.getVariableName());
    neoConnection.setRoutingPolicy(wPolicy.getText());
    neoConnection.setUsername(wUsername.getText());
    neoConnection.setPassword(wPassword.getText());
    neoConnection.setUsingEncryption(wEncryption.getSelection());
    neoConnection.setUsingEncryptionVariable(wEncryption.getVariableName());
    neoConnection.setTrustAllCertificates(wTrustAllCertificates.getSelection());
    neoConnection.setTrustAllCertificatesVariable(wTrustAllCertificates.getVariableName());

    neoConnection.setConnectionLivenessCheckTimeout(wConnectionLivenessCheckTimeout.getText());
    neoConnection.setMaxConnectionLifetime(wMaxConnectionLifetime.getText());
    neoConnection.setMaxConnectionPoolSize(wMaxConnectionPoolSize.getText());
    neoConnection.setConnectionAcquisitionTimeout(wConnectionAcquisitionTimeout.getText());
    neoConnection.setConnectionTimeout(wConnectionTimeout.getText());
    neoConnection.setMaxTransactionRetryTime(wMaxTransactionRetryTime.getText());

    neoConnection.getManualUrls().clear();
    for (int i = 0; i < wUrls.nrNonEmpty(); i++) {
      TableItem item = wUrls.getNonEmpty(i);
      neoConnection.getManualUrls().add(item.getText(1));
    }
  }

  public void test() {
    IVariables variables = manager.getVariables();
    NeoConnection neo =
        new NeoConnection(manager.getVariables(), metadata); // parent as variable space
    try {
      getWidgetsContent(neo);
      neo.test(variables);
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK);
      box.setText("OK");
      String message = "Connection successful!" + Const.CR;
      message += Const.CR;
      message += "URL : " + neo.getUrl(variables);
      box.setMessage(message);
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          "Error",
          "Error connecting to Neo with URL : " + neo.getUrl(variables),
          e);
    }
  }

  @Override
  public Button[] createButtonsForButtonBar(Composite composite) {
    Button wTest = new Button(composite, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "System.Button.Test"));
    wTest.addListener(SWT.Selection, e -> test());

    return new Button[] {wTest};
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }

  @Override
  public void setChanged() {
    if (this.isChanged == false) {
      this.isChanged = true;
      MetadataPerspective.getInstance().updateEditor(this);
    }
  }
}
