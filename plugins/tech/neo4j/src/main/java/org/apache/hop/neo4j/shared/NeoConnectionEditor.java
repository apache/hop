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
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.CheckBoxVar;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** Dialog that allows you to edit the settings of a Neo4j connection */
public class NeoConnectionEditor extends MetadataEditor<NeoConnection> {
  private static final Class<?> PKG = NeoConnectionEditor.class;

  private CTabFolder wTabFolder;

  // Connection properties
  //
  private Text wName;
  private Label wlAutomatic;
  private CheckBoxVar wAutomatic;
  private TextVar wProtocol;
  private Label wlServer;
  private TextVar wServer;
  private Label wlDatabaseName;
  private TextVar wDatabaseName;
  private Label wlDatabasePort;
  private TextVar wDatabasePort;
  private TextVar wUsername;
  private TextVar wPassword;

  // Advanced
  //
  private TextVar wBrowserPort;
  private Label wlPolicy;
  private TextVar wPolicy;
  private Label wlRouting;
  private CheckBoxVar wRouting;
  private Label wlEncryption;
  private CheckBoxVar wEncryption;
  private Label wlTrustAllCertificates;
  private CheckBoxVar wTrustAllCertificates;

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
    int margin = PropsUi.getMargin() + 2;

    IVariables variables = getHopGui().getVariables();

    // The name
    Label wlName = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Name.Label"));
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

    wTabFolder = new CTabFolder(composite, SWT.BORDER);
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.top = new FormAttachment(wName, 2 * margin);
    fdTabFolder.bottom = new FormAttachment(100, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    addBasicTab(props, variables, middle, margin);
    addProtocolTab(props, variables, middle, margin);
    addAdvancedTab(props, variables, middle, margin);
    addUrlsTab(props, variables);

    // Always select the basic tab
    wTabFolder.setSelection(0);

    setWidgetsContent();
    enableFields();

    clearChanged();

    // Add modify listeners to all controls.
    // This will inform the Metadata perspective in the Hop GUI that this object was modified and
    // needs to be saved.
    //
    Control[] controls = {
      wName,
      wAutomatic,
      wServer,
      wDatabaseName,
      wDatabasePort,
      wBrowserPort,
      wPolicy,
      wUsername,
      wPassword,
      wRouting,
      wEncryption,
      wTrustAllCertificates,
      wConnectionLivenessCheckTimeout,
      wMaxConnectionLifetime,
      wMaxConnectionPoolSize,
      wConnectionAcquisitionTimeout,
      wConnectionTimeout,
      wMaxTransactionRetryTime,
      wProtocol
    };
    for (Control control : controls) {
      control.addListener(SWT.Modify, e -> setChanged());
      control.addListener(SWT.Selection, e -> setChanged());
    }
  }

  private void addBasicTab(PropsUi props, IVariables variables, int middle, int margin) {
    CTabItem wModelTab = new CTabItem(wTabFolder, SWT.NONE);
    wModelTab.setFont(GuiResource.getInstance().getFontDefault());
    wModelTab.setText("Basic   ");
    ScrolledComposite wBasicSComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    wBasicSComp.setLayout(new FillLayout());

    Composite wBasicComp = new Composite(wBasicSComp, SWT.NONE);
    PropsUi.setLook(wBasicComp);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 3;
    formLayout.marginHeight = 3;
    wBasicComp.setLayout(formLayout);

    // Automatic?
    wlAutomatic = new Label(wBasicComp, SWT.RIGHT);
    wlAutomatic.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Automatic.Label"));
    wlAutomatic.setToolTipText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.Automatic.Tooltip"));
    PropsUi.setLook(wlAutomatic);
    FormData fdlAutomatic = new FormData();
    fdlAutomatic.top = new FormAttachment(0, margin);
    fdlAutomatic.left = new FormAttachment(0, 0);
    fdlAutomatic.right = new FormAttachment(middle, -margin);
    wlAutomatic.setLayoutData(fdlAutomatic);
    wAutomatic = new CheckBoxVar(variables, wBasicComp, SWT.CHECK);
    wAutomatic.setToolTipText(BaseMessages.getString(PKG, "NeoConnectionEditor.Automatic.Tooltip"));
    PropsUi.setLook(wAutomatic);
    FormData fdAutomatic = new FormData();
    fdAutomatic.top = new FormAttachment(wlAutomatic, 0, SWT.CENTER);
    fdAutomatic.left = new FormAttachment(middle, 0);
    fdAutomatic.right = new FormAttachment(95, 0);
    wAutomatic.setLayoutData(fdAutomatic);
    wAutomatic.addListener(SWT.Selection, e -> enableFields());
    Control lastControl = wAutomatic;

    // Protocol
    Label wlProtocol = new Label(wBasicComp, SWT.RIGHT);
    wlProtocol.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Protocol.Label"));
    wlProtocol.setToolTipText(BaseMessages.getString(PKG, "NeoConnectionEditor.Protocol.Tooltip"));
    PropsUi.setLook(wlProtocol);
    FormData fdlProtocol = new FormData();
    fdlProtocol.top = new FormAttachment(lastControl, margin);
    fdlProtocol.left = new FormAttachment(0, 0);
    fdlProtocol.right = new FormAttachment(middle, -margin);
    wlProtocol.setLayoutData(fdlProtocol);
    wProtocol = new TextVar(variables, wBasicComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wProtocol.setToolTipText(BaseMessages.getString(PKG, "NeoConnectionEditor.Protocol.Tooltip"));
    PropsUi.setLook(wProtocol);
    FormData fdProtocol = new FormData();
    fdProtocol.top = new FormAttachment(wlProtocol, 0, SWT.CENTER);
    fdProtocol.left = new FormAttachment(middle, 0);
    fdProtocol.right = new FormAttachment(95, 0);
    wProtocol.setLayoutData(fdProtocol);
    lastControl = wProtocol;

    // The server
    wlServer = new Label(wBasicComp, SWT.RIGHT);
    PropsUi.setLook(wlServer);
    wlServer.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Server.Label"));
    FormData fdlServer = new FormData();
    fdlServer.top = new FormAttachment(lastControl, margin);
    fdlServer.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlServer.right = new FormAttachment(middle, -margin);
    wlServer.setLayoutData(fdlServer);
    wServer = new TextVar(variables, wBasicComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wServer);
    FormData fdServer = new FormData();
    fdServer.top = new FormAttachment(wlServer, 0, SWT.CENTER);
    fdServer.left = new FormAttachment(middle, 0); // To the right of the label
    fdServer.right = new FormAttachment(95, 0);
    wServer.setLayoutData(fdServer);
    lastControl = wServer;

    // The DatabaseName
    wlDatabaseName = new Label(wBasicComp, SWT.RIGHT);
    PropsUi.setLook(wlDatabaseName);
    wlDatabaseName.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.DatabaseName.Label"));
    FormData fdlDatabaseName = new FormData();
    fdlDatabaseName.top = new FormAttachment(lastControl, margin);
    fdlDatabaseName.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlDatabaseName.right = new FormAttachment(middle, -margin);
    wlDatabaseName.setLayoutData(fdlDatabaseName);
    wDatabaseName = new TextVar(variables, wBasicComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDatabaseName);
    FormData fdDatabaseName = new FormData();
    fdDatabaseName.top = new FormAttachment(wlDatabaseName, 0, SWT.CENTER);
    fdDatabaseName.left = new FormAttachment(middle, 0); // To the right of the label
    fdDatabaseName.right = new FormAttachment(95, 0);
    wDatabaseName.setLayoutData(fdDatabaseName);
    lastControl = wDatabaseName;

    // Database port?
    wlDatabasePort = new Label(wBasicComp, SWT.RIGHT);
    PropsUi.setLook(wlDatabasePort);
    wlDatabasePort.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.DatabasePort.Label"));
    FormData fdlDatabasePort = new FormData();
    fdlDatabasePort.top = new FormAttachment(lastControl, margin);
    fdlDatabasePort.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlDatabasePort.right = new FormAttachment(middle, -margin);
    wlDatabasePort.setLayoutData(fdlDatabasePort);
    wDatabasePort = new TextVar(variables, wBasicComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDatabasePort);
    FormData fdDatabasePort = new FormData();
    fdDatabasePort.top = new FormAttachment(wlDatabasePort, 0, SWT.CENTER);
    fdDatabasePort.left = new FormAttachment(middle, 0); // To the right of the label
    fdDatabasePort.right = new FormAttachment(95, 0);
    wDatabasePort.setLayoutData(fdDatabasePort);
    lastControl = wDatabasePort;

    // Username
    Label wlUsername = new Label(wBasicComp, SWT.RIGHT);
    wlUsername.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.UserName.Label"));
    PropsUi.setLook(wlUsername);
    FormData fdlUsername = new FormData();
    fdlUsername.top = new FormAttachment(lastControl, margin);
    fdlUsername.left = new FormAttachment(0, 0);
    fdlUsername.right = new FormAttachment(middle, -margin);
    wlUsername.setLayoutData(fdlUsername);
    wUsername = new TextVar(variables, wBasicComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUsername);
    FormData fdUsername = new FormData();
    fdUsername.top = new FormAttachment(wlUsername, 0, SWT.CENTER);
    fdUsername.left = new FormAttachment(middle, 0);
    fdUsername.right = new FormAttachment(95, 0);
    wUsername.setLayoutData(fdUsername);
    lastControl = wUsername;

    // Password
    Label wlPassword = new Label(wBasicComp, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Password.Label"));
    PropsUi.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.top = new FormAttachment(lastControl, margin);
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new PasswordTextVar(variables, wBasicComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPassword);
    FormData fdPassword = new FormData();
    fdPassword.top = new FormAttachment(wlPassword, 0, SWT.CENTER);
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.right = new FormAttachment(95, 0);
    wPassword.setLayoutData(fdPassword);

    // End of the basic tab...
    //
    wBasicComp.pack();

    Rectangle bounds = wBasicComp.getBounds();

    wBasicSComp.setContent(wBasicComp);
    wBasicSComp.setExpandHorizontal(true);
    wBasicSComp.setExpandVertical(true);
    wBasicSComp.setMinWidth(bounds.width);
    wBasicSComp.setMinHeight(bounds.height);

    wModelTab.setControl(wBasicSComp);
  }

  private void addProtocolTab(PropsUi props, IVariables variables, int middle, int margin) {
    CTabItem wProtocolTab = new CTabItem(wTabFolder, SWT.NONE);
    wProtocolTab.setFont(GuiResource.getInstance().getFontDefault());
    wProtocolTab.setText("Protocol   ");
    ScrolledComposite wProtocolSComp =
        new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    wProtocolSComp.setLayout(new FillLayout());

    Composite wProtocolComp = new Composite(wProtocolSComp, SWT.NONE);
    PropsUi.setLook(wProtocolComp);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 3;
    formLayout.marginHeight = 3;
    wProtocolComp.setLayout(formLayout);

    Control lastControl = null;

    // Browser port?
    Label wlBrowserPort = new Label(wProtocolComp, SWT.RIGHT);
    PropsUi.setLook(wlBrowserPort);
    wlBrowserPort.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.BrowserPort.Label"));
    FormData fdlBrowserPort = new FormData();
    fdlBrowserPort.top = new FormAttachment(lastControl, margin);
    fdlBrowserPort.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlBrowserPort.right = new FormAttachment(middle, -margin);
    wlBrowserPort.setLayoutData(fdlBrowserPort);
    wBrowserPort = new TextVar(variables, wProtocolComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBrowserPort);
    FormData fdBrowserPort = new FormData();
    fdBrowserPort.top = new FormAttachment(wlBrowserPort, 0, SWT.CENTER);
    fdBrowserPort.left = new FormAttachment(middle, 0); // To the right of the label
    fdBrowserPort.right = new FormAttachment(95, 0);
    wBrowserPort.setLayoutData(fdBrowserPort);
    lastControl = wBrowserPort;

    // Routing
    wlRouting = new Label(wProtocolComp, SWT.RIGHT);
    wlRouting.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Routing.Label"));
    PropsUi.setLook(wlRouting);
    FormData fdlRouting = new FormData();
    fdlRouting.top = new FormAttachment(lastControl, margin);
    fdlRouting.left = new FormAttachment(0, 0);
    fdlRouting.right = new FormAttachment(middle, -margin);
    wlRouting.setLayoutData(fdlRouting);
    wRouting = new CheckBoxVar(variables, wProtocolComp, SWT.CHECK);
    PropsUi.setLook(wRouting);
    FormData fdRouting = new FormData();
    fdRouting.top = new FormAttachment(wlRouting, 0, SWT.CENTER);
    fdRouting.left = new FormAttachment(middle, 0);
    fdRouting.right = new FormAttachment(95, 0);
    wRouting.setLayoutData(fdRouting);
    wRouting.addListener(SWT.Selection, e -> enableFields());
    lastControl = wRouting;

    // Policy
    wlPolicy = new Label(wProtocolComp, SWT.RIGHT);
    wlPolicy.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Policy.Label"));
    PropsUi.setLook(wlPolicy);
    FormData fdlPolicy = new FormData();
    fdlPolicy.top = new FormAttachment(lastControl, margin);
    fdlPolicy.left = new FormAttachment(0, 0);
    fdlPolicy.right = new FormAttachment(middle, -margin);
    wlPolicy.setLayoutData(fdlPolicy);
    wPolicy = new TextVar(variables, wProtocolComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPolicy);
    FormData fdPolicy = new FormData();
    fdPolicy.top = new FormAttachment(wlPolicy, 0, SWT.CENTER);
    fdPolicy.left = new FormAttachment(middle, 0);
    fdPolicy.right = new FormAttachment(95, 0);
    wPolicy.setLayoutData(fdPolicy);
    lastControl = wPolicy;

    // Encryption?
    wlEncryption = new Label(wProtocolComp, SWT.RIGHT);
    wlEncryption.setText(BaseMessages.getString(PKG, "NeoConnectionEditor.Encryption.Label"));
    PropsUi.setLook(wlEncryption);
    FormData fdlEncryption = new FormData();
    fdlEncryption.top = new FormAttachment(lastControl, margin);
    fdlEncryption.left = new FormAttachment(0, 0);
    fdlEncryption.right = new FormAttachment(middle, -margin);
    wlEncryption.setLayoutData(fdlEncryption);
    wEncryption = new CheckBoxVar(variables, wProtocolComp, SWT.CHECK);
    PropsUi.setLook(wEncryption);
    FormData fdEncryption = new FormData();
    fdEncryption.top = new FormAttachment(wlEncryption, 0, SWT.CENTER);
    fdEncryption.left = new FormAttachment(middle, 0);
    fdEncryption.right = new FormAttachment(95, 0);
    wEncryption.setLayoutData(fdEncryption);
    wEncryption.addListener(SWT.Selection, e -> enableFields());
    lastControl = wEncryption;

    // Trust Level?
    wlTrustAllCertificates = new Label(wProtocolComp, SWT.RIGHT);
    wlTrustAllCertificates.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.TrustAllCertificates.Label"));
    PropsUi.setLook(wlTrustAllCertificates);
    FormData fdlTrustAllCertificates = new FormData();
    fdlTrustAllCertificates.top = new FormAttachment(lastControl, margin);
    fdlTrustAllCertificates.left = new FormAttachment(0, 0);
    fdlTrustAllCertificates.right = new FormAttachment(middle, -margin);
    wlTrustAllCertificates.setLayoutData(fdlTrustAllCertificates);
    wTrustAllCertificates = new CheckBoxVar(variables, wProtocolComp, SWT.CHECK);
    PropsUi.setLook(wEncryption);
    FormData fdTrustAllCertificates = new FormData();
    fdTrustAllCertificates.top = new FormAttachment(wlTrustAllCertificates, 0, SWT.CENTER);
    fdTrustAllCertificates.left = new FormAttachment(middle, 0);
    fdTrustAllCertificates.right = new FormAttachment(95, 0);
    wTrustAllCertificates.setLayoutData(fdTrustAllCertificates);

    // End of the basic tab...
    //
    wProtocolComp.pack();

    Rectangle bounds = wProtocolComp.getBounds();

    wProtocolSComp.setContent(wProtocolComp);
    wProtocolSComp.setExpandHorizontal(true);
    wProtocolSComp.setExpandVertical(true);
    wProtocolSComp.setMinWidth(bounds.width);
    wProtocolSComp.setMinHeight(bounds.height);

    wProtocolTab.setControl(wProtocolSComp);
  }

  private void addUrlsTab(PropsUi props, IVariables variables) {
    CTabItem wUrlsTab = new CTabItem(wTabFolder, SWT.NONE);
    wUrlsTab.setFont(GuiResource.getInstance().getFontDefault());
    wUrlsTab.setText("Manual URLs");
    ScrolledComposite wUrlsSComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    wUrlsSComp.setLayout(new FillLayout());

    Composite wUrlsComp = new Composite(wUrlsSComp, SWT.NONE);
    PropsUi.setLook(wUrlsComp);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 3;
    formLayout.marginHeight = 3;
    wUrlsComp.setLayout(formLayout);

    // URLs
    wUrls =
        new TableView(
            variables,
            wUrlsComp,
            SWT.NONE,
            new ColumnInfo[] {
              new ColumnInfo(
                  BaseMessages.getString(PKG, "NeoConnectionEditor.URLColumn.Label"),
                  ColumnInfo.COLUMN_TYPE_TEXT)
            },
            getMetadata().getManualUrls().size(),
            e -> {
              setChanged();
              enableFields();
            },
            props);

    FormData fdUrls = new FormData();
    fdUrls.top = new FormAttachment(0, 0);
    fdUrls.left = new FormAttachment(0, 0);
    fdUrls.right = new FormAttachment(100, 0);
    fdUrls.bottom = new FormAttachment(100, 0);
    wUrls.setLayoutData(fdUrls);

    // End of the basic tab...
    //
    wUrlsComp.pack();

    Rectangle bounds = wUrlsComp.getBounds();

    wUrlsSComp.setContent(wUrlsComp);
    wUrlsSComp.setExpandHorizontal(true);
    wUrlsSComp.setExpandVertical(true);
    wUrlsSComp.setMinWidth(bounds.width);
    wUrlsSComp.setMinHeight(bounds.height);

    wUrlsTab.setControl(wUrlsSComp);
  }

  private void addAdvancedTab(PropsUi props, IVariables variables, int middle, int margin) {
    CTabItem wAdvancedTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdvancedTab.setFont(GuiResource.getInstance().getFontDefault());
    wAdvancedTab.setText("Advanced  ");
    ScrolledComposite wAdvancedSComp =
        new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    wAdvancedSComp.setLayout(new FillLayout());

    Composite wAdvancedComp = new Composite(wAdvancedSComp, SWT.NONE);
    PropsUi.setLook(wAdvancedComp);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 3;
    formLayout.marginHeight = 3;
    wAdvancedComp.setLayout(formLayout);

    // ConnectionLivenessCheckTimeout
    Label wlConnectionLivenessCheckTimeout = new Label(wAdvancedComp, SWT.RIGHT);
    wlConnectionLivenessCheckTimeout.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.ConnectionLivenessCheckTimeout.Label"));
    PropsUi.setLook(wlConnectionLivenessCheckTimeout);
    FormData fdlConnectionLivenessCheckTimeout = new FormData();
    fdlConnectionLivenessCheckTimeout.top = new FormAttachment(0, 0);
    fdlConnectionLivenessCheckTimeout.left = new FormAttachment(0, 0);
    fdlConnectionLivenessCheckTimeout.right = new FormAttachment(middle, -margin);
    wlConnectionLivenessCheckTimeout.setLayoutData(fdlConnectionLivenessCheckTimeout);
    wConnectionLivenessCheckTimeout =
        new TextVar(variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wConnectionLivenessCheckTimeout);
    FormData fdConnectionLivenessCheckTimeout = new FormData();
    fdConnectionLivenessCheckTimeout.top =
        new FormAttachment(wlConnectionLivenessCheckTimeout, 0, SWT.CENTER);
    fdConnectionLivenessCheckTimeout.left = new FormAttachment(middle, 0);
    fdConnectionLivenessCheckTimeout.right = new FormAttachment(95, 0);
    wConnectionLivenessCheckTimeout.setLayoutData(fdConnectionLivenessCheckTimeout);
    Control lastGroupControl = wConnectionLivenessCheckTimeout;

    // MaxConnectionLifetime
    Label wlMaxConnectionLifetime = new Label(wAdvancedComp, SWT.RIGHT);
    wlMaxConnectionLifetime.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.MaxConnectionLifetime.Label"));
    PropsUi.setLook(wlMaxConnectionLifetime);
    FormData fdlMaxConnectionLifetime = new FormData();
    fdlMaxConnectionLifetime.top = new FormAttachment(lastGroupControl, margin);
    fdlMaxConnectionLifetime.left = new FormAttachment(0, 0);
    fdlMaxConnectionLifetime.right = new FormAttachment(middle, -margin);
    wlMaxConnectionLifetime.setLayoutData(fdlMaxConnectionLifetime);
    wMaxConnectionLifetime =
        new TextVar(variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMaxConnectionLifetime);
    FormData fdMaxConnectionLifetime = new FormData();
    fdMaxConnectionLifetime.top = new FormAttachment(wlMaxConnectionLifetime, 0, SWT.CENTER);
    fdMaxConnectionLifetime.left = new FormAttachment(middle, 0);
    fdMaxConnectionLifetime.right = new FormAttachment(95, 0);
    wMaxConnectionLifetime.setLayoutData(fdMaxConnectionLifetime);
    lastGroupControl = wMaxConnectionLifetime;

    // MaxConnectionPoolSize
    Label wlMaxConnectionPoolSize = new Label(wAdvancedComp, SWT.RIGHT);
    wlMaxConnectionPoolSize.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.MaxConnectionPoolSize.Label"));
    PropsUi.setLook(wlMaxConnectionPoolSize);
    FormData fdlMaxConnectionPoolSize = new FormData();
    fdlMaxConnectionPoolSize.top = new FormAttachment(lastGroupControl, margin);
    fdlMaxConnectionPoolSize.left = new FormAttachment(0, 0);
    fdlMaxConnectionPoolSize.right = new FormAttachment(middle, -margin);
    wlMaxConnectionPoolSize.setLayoutData(fdlMaxConnectionPoolSize);
    wMaxConnectionPoolSize =
        new TextVar(variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMaxConnectionPoolSize);
    FormData fdMaxConnectionPoolSize = new FormData();
    fdMaxConnectionPoolSize.top = new FormAttachment(wlMaxConnectionPoolSize, 0, SWT.CENTER);
    fdMaxConnectionPoolSize.left = new FormAttachment(middle, 0);
    fdMaxConnectionPoolSize.right = new FormAttachment(95, 0);
    wMaxConnectionPoolSize.setLayoutData(fdMaxConnectionPoolSize);
    lastGroupControl = wMaxConnectionPoolSize;

    // ConnectionAcquisitionTimeout
    Label wlConnectionAcquisitionTimeout = new Label(wAdvancedComp, SWT.RIGHT);
    wlConnectionAcquisitionTimeout.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.ConnectionAcquisitionTimeout.Label"));
    PropsUi.setLook(wlConnectionAcquisitionTimeout);
    FormData fdlConnectionAcquisitionTimeout = new FormData();
    fdlConnectionAcquisitionTimeout.top = new FormAttachment(lastGroupControl, margin);
    fdlConnectionAcquisitionTimeout.left = new FormAttachment(0, 0);
    fdlConnectionAcquisitionTimeout.right = new FormAttachment(middle, -margin);
    wlConnectionAcquisitionTimeout.setLayoutData(fdlConnectionAcquisitionTimeout);
    wConnectionAcquisitionTimeout =
        new TextVar(variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wConnectionAcquisitionTimeout);
    FormData fdConnectionAcquisitionTimeout = new FormData();
    fdConnectionAcquisitionTimeout.top =
        new FormAttachment(wlConnectionAcquisitionTimeout, 0, SWT.CENTER);
    fdConnectionAcquisitionTimeout.left = new FormAttachment(middle, 0);
    fdConnectionAcquisitionTimeout.right = new FormAttachment(95, 0);
    wConnectionAcquisitionTimeout.setLayoutData(fdConnectionAcquisitionTimeout);
    lastGroupControl = wConnectionAcquisitionTimeout;

    // ConnectionTimeout
    Label wlConnectionTimeout = new Label(wAdvancedComp, SWT.RIGHT);
    wlConnectionTimeout.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.ConnectionTimeout.Label"));
    PropsUi.setLook(wlConnectionTimeout);
    FormData fdlConnectionTimeout = new FormData();
    fdlConnectionTimeout.top = new FormAttachment(lastGroupControl, margin);
    fdlConnectionTimeout.left = new FormAttachment(0, 0);
    fdlConnectionTimeout.right = new FormAttachment(middle, -margin);
    wlConnectionTimeout.setLayoutData(fdlConnectionTimeout);
    wConnectionTimeout = new TextVar(variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wConnectionTimeout);
    FormData fdConnectionTimeout = new FormData();
    fdConnectionTimeout.top = new FormAttachment(wlConnectionTimeout, 0, SWT.CENTER);
    fdConnectionTimeout.left = new FormAttachment(middle, 0);
    fdConnectionTimeout.right = new FormAttachment(95, 0);
    wConnectionTimeout.setLayoutData(fdConnectionTimeout);
    lastGroupControl = wConnectionTimeout;

    // MaxTransactionRetryTime
    Label wlMaxTransactionRetryTime = new Label(wAdvancedComp, SWT.RIGHT);
    wlMaxTransactionRetryTime.setText(
        BaseMessages.getString(PKG, "NeoConnectionEditor.MaxTransactionRetryTime.Label"));
    PropsUi.setLook(wlMaxTransactionRetryTime);
    FormData fdlMaxTransactionRetryTime = new FormData();
    fdlMaxTransactionRetryTime.top = new FormAttachment(lastGroupControl, margin);
    fdlMaxTransactionRetryTime.left = new FormAttachment(0, 0);
    fdlMaxTransactionRetryTime.right = new FormAttachment(middle, -margin);
    wlMaxTransactionRetryTime.setLayoutData(fdlMaxTransactionRetryTime);
    wMaxTransactionRetryTime =
        new TextVar(variables, wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMaxTransactionRetryTime);
    FormData fdMaxTransactionRetryTime = new FormData();
    fdMaxTransactionRetryTime.top = new FormAttachment(wlMaxTransactionRetryTime, 0, SWT.CENTER);
    fdMaxTransactionRetryTime.left = new FormAttachment(middle, 0);
    fdMaxTransactionRetryTime.right = new FormAttachment(95, 0);
    wMaxTransactionRetryTime.setLayoutData(fdMaxTransactionRetryTime);

    // End of the basic tab...
    //
    wAdvancedComp.pack();

    Rectangle bounds = wAdvancedComp.getBounds();

    wAdvancedSComp.setContent(wAdvancedComp);
    wAdvancedSComp.setExpandHorizontal(true);
    wAdvancedSComp.setExpandVertical(true);
    wAdvancedSComp.setMinWidth(bounds.width);
    wAdvancedSComp.setMinHeight(bounds.height);

    wAdvancedTab.setControl(wAdvancedSComp);
  }

  private void enableFields() {

    // If you specify URLs manually a lot of things are no longer available...
    //
    boolean hasNoUrls = wUrls.nrNonEmpty() == 0;
    for (Control control :
        new Control[] {
          wlServer,
          wServer,
          wlDatabaseName,
          wDatabaseName,
          wlDatabasePort,
          wDatabasePort,
          wlRouting,
          wRouting,
          wlPolicy,
          wPolicy,
          wlEncryption,
          wEncryption,
        }) {
      control.setEnabled(hasNoUrls);
    }

    // For the normal scenarios without manual URLs we consider the automatic flag.
    // If things are configured automatically a number of flags are no longer applicable:
    // Version 4, routing, routing policy, encryption & trust all certificates.
    //
    NeoConnection neo = new NeoConnection();
    getWidgetsContent(neo);

    boolean automatic = neo.isAutomatic();
    boolean routing = neo.isRouting();
    boolean encryption = neo.isUsingEncryption();

    wRouting.setEnabled(!automatic);
    wlRouting.setEnabled(!automatic);
    wRouting.setEnabled(!automatic);
    wlEncryption.setEnabled(!automatic);
    wEncryption.setEnabled(!automatic);

    wlPolicy.setEnabled(!automatic && routing);
    wPolicy.setEnabled(!automatic && routing);

    wlTrustAllCertificates.setEnabled(!automatic && encryption);
    wTrustAllCertificates.setEnabled(!automatic && encryption);
    wTrustAllCertificates.getTextVar().setEnabled(!automatic && encryption);
  }

  @Override
  public void setWidgetsContent() {
    wName.setText(Const.NVL(metadata.getName(), ""));
    wAutomatic.setSelection(metadata.isAutomatic());
    wAutomatic.setVariableName(Const.NVL(metadata.getAutomaticVariable(), ""));
    wProtocol.setText(Const.NVL(metadata.getProtocol(), ""));
    wServer.setText(Const.NVL(metadata.getServer(), ""));
    wDatabaseName.setText(Const.NVL(metadata.getDatabaseName(), ""));
    wDatabasePort.setText(Const.NVL(metadata.getBoltPort(), ""));
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
    neoConnection.setAutomatic(wAutomatic.getSelection());
    neoConnection.setAutomaticVariable(wAutomatic.getVariableName());
    neoConnection.setProtocol(wProtocol.getText());
    neoConnection.setServer(wServer.getText());
    neoConnection.setDatabaseName(wDatabaseName.getText());
    neoConnection.setBoltPort(wDatabasePort.getText());
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
    NeoConnection neo = new NeoConnection(metadata);
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

  public void clearChanged() {
    resetChanged();
    MetadataPerspective.getInstance().updateEditor(this);
  }
}
