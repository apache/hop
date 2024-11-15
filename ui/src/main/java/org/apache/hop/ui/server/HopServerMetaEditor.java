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

package org.apache.hop.ui.server;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.server.HopServerMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.www.RegisterPipelineServlet;
import org.apache.hop.www.RemoteHopServer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

/**
 * Editor that allows you to edit the settings of the Hop server
 *
 * @see HopServerMeta
 */
public class HopServerMetaEditor extends MetadataEditor<HopServerMeta> {
  private static final Class<?> PKG = HopServerMetaEditor.class;

  private CTabFolder wTabFolder;

  // Service
  private Text wName;
  private TextVar wHostname;
  private TextVar wPort;
  private TextVar wWebAppName;
  private TextVar wUsername;
  private TextVar wPassword;
  private Button wSSL;

  // Proxy
  private TextVar wProxyHost;
  private TextVar wProxyPort;
  private TextVar wNonProxyHosts;

  private int middle;
  private int margin;

  public HopServerMetaEditor(
      HopGui hopGui, MetadataManager<HopServerMeta> manager, HopServerMeta metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {

    PropsUi props = PropsUi.getInstance();

    middle = props.getMiddlePct();
    margin = PropsUi.getMargin();

    Label wIcon = new Label(parent, SWT.RIGHT);
    wIcon.setImage(getImage());
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment(0, 0);
    fdlicon.right = new FormAttachment(100, 0);
    wIcon.setLayoutData(fdlicon);
    PropsUi.setLook(wIcon);

    // What's the name
    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "HopServerDialog.ServerName.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, 0);
    fdlName.left = new FormAttachment(0, 0);
    wlName.setLayoutData(fdlName);

    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 5);
    fdName.left = new FormAttachment(0, 0);
    fdName.right = new FormAttachment(wIcon, -5);
    wName.setLayoutData(fdName);

    Label spacer = new Label(parent, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wName, 15);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    wTabFolder = new CTabFolder(parent, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(spacer, 15);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(100, -15);
    wTabFolder.setLayoutData(fdTabFolder);

    createServiceTab();
    createProxyTab();
    wTabFolder.setSelection(0);

    setWidgetsContent();

    // Add listener to detect change after loading data
    ModifyListener lsMod = e -> setChanged();
    wName.addModifyListener(lsMod);
    wHostname.addModifyListener(lsMod);
    wPort.addModifyListener(lsMod);
    wWebAppName.addModifyListener(lsMod);
    wUsername.addModifyListener(lsMod);
    wPassword.addModifyListener(lsMod);
    wProxyHost.addModifyListener(lsMod);
    wProxyPort.addModifyListener(lsMod);
    wNonProxyHosts.addModifyListener(lsMod);
    wSSL.addListener(SWT.Selection, e -> setChanged());
  }

  private void createServiceTab() {
    PropsUi props = PropsUi.getInstance();
    HopServerMeta hopServer = getMetadata();

    // ////////////////////////
    // START OF DB TAB ///
    // ////////////////////////
    CTabItem wServiceTab = new CTabItem(wTabFolder, SWT.NONE);
    wServiceTab.setFont(GuiResource.getInstance().getFontDefault());
    wServiceTab.setText(BaseMessages.getString(PKG, "HopServerDialog.USER_TAB_SERVICE"));

    Composite wServiceComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wServiceComp);

    FormLayout genLayout = new FormLayout();
    genLayout.marginWidth = PropsUi.getFormMargin();
    genLayout.marginHeight = PropsUi.getFormMargin();
    wServiceComp.setLayout(genLayout);

    // What's the hostname
    Label wlHostname = new Label(wServiceComp, SWT.RIGHT);
    PropsUi.setLook(wlHostname);
    wlHostname.setText(BaseMessages.getString(PKG, "HopServerDialog.HostIP.Label"));
    FormData fdlHostname = new FormData();
    fdlHostname.top = new FormAttachment(0, margin * 2);
    fdlHostname.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlHostname.right = new FormAttachment(middle, -margin);
    wlHostname.setLayoutData(fdlHostname);

    wHostname =
        new TextVar(manager.getVariables(), wServiceComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

    PropsUi.setLook(wHostname);
    FormData fdHostname = new FormData();
    fdHostname.top = new FormAttachment(wName, margin * 2);
    fdHostname.left = new FormAttachment(middle, 0); // To the right of the label
    fdHostname.right = new FormAttachment(95, 0);
    wHostname.setLayoutData(fdHostname);

    // What's the service URL?
    Label wlPort = new Label(wServiceComp, SWT.RIGHT);
    PropsUi.setLook(wlPort);
    wlPort.setText(BaseMessages.getString(PKG, "HopServerDialog.Port.Label"));
    FormData fdlPort = new FormData();
    fdlPort.top = new FormAttachment(wHostname, margin);
    fdlPort.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlPort.right = new FormAttachment(middle, -margin);
    wlPort.setLayoutData(fdlPort);

    wPort = new TextVar(manager.getVariables(), wServiceComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

    PropsUi.setLook(wPort);
    FormData fdPort = new FormData();
    fdPort.top = new FormAttachment(wHostname, margin);
    fdPort.left = new FormAttachment(middle, 0); // To the right of the label
    fdPort.right = new FormAttachment(95, 0);
    wPort.setLayoutData(fdPort);

    // webapp name (optional)
    Label wlWebAppName = new Label(wServiceComp, SWT.RIGHT);
    wlWebAppName.setText(BaseMessages.getString(PKG, "HopServerDialog.WebAppName.Label"));
    PropsUi.setLook(wlWebAppName);
    FormData fdlWebAppName = new FormData();
    fdlWebAppName.top = new FormAttachment(wPort, margin);
    fdlWebAppName.left = new FormAttachment(0, 0);
    fdlWebAppName.right = new FormAttachment(middle, -margin);
    wlWebAppName.setLayoutData(fdlWebAppName);

    wWebAppName =
        new TextVar(manager.getVariables(), wServiceComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

    PropsUi.setLook(wWebAppName);
    FormData fdWebAppName = new FormData();
    fdWebAppName.top = new FormAttachment(wPort, margin);
    fdWebAppName.left = new FormAttachment(middle, 0);
    fdWebAppName.right = new FormAttachment(95, 0);
    wWebAppName.setLayoutData(fdWebAppName);

    // Username
    Label wlUsername = new Label(wServiceComp, SWT.RIGHT);
    wlUsername.setText(BaseMessages.getString(PKG, "HopServerDialog.UserName.Label"));
    PropsUi.setLook(wlUsername);
    FormData fdlUsername = new FormData();
    fdlUsername.top = new FormAttachment(wWebAppName, margin);
    fdlUsername.left = new FormAttachment(0, 0);
    fdlUsername.right = new FormAttachment(middle, -margin);
    wlUsername.setLayoutData(fdlUsername);

    wUsername =
        new TextVar(manager.getVariables(), wServiceComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

    PropsUi.setLook(wUsername);
    FormData fdUsername = new FormData();
    fdUsername.top = new FormAttachment(wWebAppName, margin);
    fdUsername.left = new FormAttachment(middle, 0);
    fdUsername.right = new FormAttachment(95, 0);
    wUsername.setLayoutData(fdUsername);

    // Password
    Label wlPassword = new Label(wServiceComp, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "HopServerDialog.Password.Label"));
    PropsUi.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.top = new FormAttachment(wUsername, margin);
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);

    wPassword =
        new PasswordTextVar(
            manager.getVariables(), wServiceComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

    PropsUi.setLook(wPassword);
    FormData fdPassword = new FormData();
    fdPassword.top = new FormAttachment(wUsername, margin);
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.right = new FormAttachment(95, 0);
    wPassword.setLayoutData(fdPassword);

    // Https
    Label wlSSL = new Label(wServiceComp, SWT.RIGHT);
    wlSSL.setText(BaseMessages.getString(PKG, "HopServerDialog.UseSsl.Label"));
    PropsUi.setLook(wlSSL);
    FormData fd = new FormData();
    fd.top = new FormAttachment(wPassword, margin);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    wlSSL.setLayoutData(fd);
    wlSSL.setVisible(true);

    wSSL = new Button(wServiceComp, SWT.CHECK);

    PropsUi.setLook(wSSL);
    FormData bfd = new FormData();
    bfd.top = new FormAttachment(wlSSL, 0, SWT.CENTER);
    bfd.left = new FormAttachment(middle, 0);
    bfd.right = new FormAttachment(95, 0);
    wSSL.setLayoutData(bfd);
    wSSL.setVisible(true);

    FormData fdServiceComp = new FormData();
    fdServiceComp.left = new FormAttachment(0, 0);
    fdServiceComp.top = new FormAttachment(0, 0);
    fdServiceComp.right = new FormAttachment(100, 0);
    fdServiceComp.bottom = new FormAttachment(100, 0);
    wServiceComp.setLayoutData(fdServiceComp);

    wServiceComp.layout();
    wServiceTab.setControl(wServiceComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GEN TAB
    // ///////////////////////////////////////////////////////////
  }

  private void createProxyTab() {
    PropsUi props = PropsUi.getInstance();
    HopServerMeta hopServer = getMetadata();

    // ////////////////////////
    // START OF POOL TAB///
    // /
    CTabItem wProxyTab = new CTabItem(wTabFolder, SWT.NONE);
    wProxyTab.setFont(GuiResource.getInstance().getFontDefault());
    wProxyTab.setText(BaseMessages.getString(PKG, "HopServerDialog.USER_TAB_PROXY"));

    FormLayout poolLayout = new FormLayout();
    poolLayout.marginWidth = PropsUi.getFormMargin();
    poolLayout.marginHeight = PropsUi.getFormMargin();

    Composite wProxyComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wProxyComp);
    wProxyComp.setLayout(poolLayout);

    // What's the data tablespace name?
    Label wlProxyHost = new Label(wProxyComp, SWT.RIGHT);
    PropsUi.setLook(wlProxyHost);
    wlProxyHost.setText(BaseMessages.getString(PKG, "HopServerDialog.ProxyServerName.Label"));
    FormData fdlProxyHost = new FormData();
    fdlProxyHost.top = new FormAttachment(0, 0);
    fdlProxyHost.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlProxyHost.right = new FormAttachment(middle, -margin);
    wlProxyHost.setLayoutData(fdlProxyHost);

    wProxyHost =
        new TextVar(manager.getVariables(), wProxyComp, SWT.BORDER | SWT.LEFT | SWT.SINGLE);

    PropsUi.setLook(wProxyHost);
    FormData fdProxyHost = new FormData();
    fdProxyHost.top = new FormAttachment(0, 0);
    fdProxyHost.left = new FormAttachment(middle, 0); // To the right of the label
    fdProxyHost.right = new FormAttachment(95, 0);
    wProxyHost.setLayoutData(fdProxyHost);

    // What's the initial pool size
    Label wlProxyPort = new Label(wProxyComp, SWT.RIGHT);
    PropsUi.setLook(wlProxyPort);
    wlProxyPort.setText(BaseMessages.getString(PKG, "HopServerDialog.ProxyServerPort.Label"));
    FormData fdlProxyPort = new FormData();
    fdlProxyPort.top = new FormAttachment(wProxyHost, margin);
    fdlProxyPort.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlProxyPort.right = new FormAttachment(middle, -margin);
    wlProxyPort.setLayoutData(fdlProxyPort);

    wProxyPort =
        new TextVar(manager.getVariables(), wProxyComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wProxyPort);
    FormData fdProxyPort = new FormData();
    fdProxyPort.top = new FormAttachment(wProxyHost, margin);
    fdProxyPort.left = new FormAttachment(middle, 0); // To the right of the label
    fdProxyPort.right = new FormAttachment(95, 0);
    wProxyPort.setLayoutData(fdProxyPort);

    // What's the maximum pool size
    Label wlNonProxyHosts = new Label(wProxyComp, SWT.RIGHT);
    PropsUi.setLook(wlNonProxyHosts);
    wlNonProxyHosts.setText(
        BaseMessages.getString(PKG, "HopServerDialog.IgnoreProxyForHosts.Label"));
    FormData fdlNonProxyHosts = new FormData();
    fdlNonProxyHosts.top = new FormAttachment(wProxyPort, margin);
    fdlNonProxyHosts.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlNonProxyHosts.right = new FormAttachment(middle, -margin);
    wlNonProxyHosts.setLayoutData(fdlNonProxyHosts);

    wNonProxyHosts =
        new TextVar(manager.getVariables(), wProxyComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wNonProxyHosts);
    FormData fdNonProxyHosts = new FormData();
    fdNonProxyHosts.top = new FormAttachment(wProxyPort, margin);
    fdNonProxyHosts.left = new FormAttachment(middle, 0); // To the right of the label
    fdNonProxyHosts.right = new FormAttachment(95, 0);
    wNonProxyHosts.setLayoutData(fdNonProxyHosts);

    FormData fdProxyComp = new FormData();
    fdProxyComp.left = new FormAttachment(0, 0);
    fdProxyComp.top = new FormAttachment(0, 0);
    fdProxyComp.right = new FormAttachment(100, 0);
    fdProxyComp.bottom = new FormAttachment(100, 0);
    wProxyComp.setLayoutData(fdProxyComp);

    wProxyComp.layout();
    wProxyTab.setControl(wProxyComp);
  }

  @Override
  public void setWidgetsContent() {
    HopServerMeta server = getMetadata();

    wName.setText(Const.NVL(server.getName(), ""));
    wHostname.setText(Const.NVL(server.getHostname(), ""));
    wPort.setText(Const.NVL(server.getPort(), ""));
    wWebAppName.setText(Const.NVL(server.getWebAppName(), ""));
    wUsername.setText(Const.NVL(server.getUsername(), ""));
    wPassword.setText(Const.NVL(server.getPassword(), ""));
    wProxyHost.setText(Const.NVL(server.getProxyHostname(), ""));
    wProxyPort.setText(Const.NVL(server.getProxyPort(), ""));
    wNonProxyHosts.setText(Const.NVL(server.getNonProxyHosts(), ""));
    wSSL.setSelection(server.isSslMode());
  }

  @Override
  public void getWidgetsContent(HopServerMeta server) {
    server.setName(wName.getText());
    server.setHostname(wHostname.getText());
    server.setPort(wPort.getText());
    server.setWebAppName(wWebAppName.getText());
    server.setUsername(wUsername.getText());
    server.setPassword(wPassword.getText());
    server.setProxyHostname(wProxyHost.getText());
    server.setProxyPort(wProxyPort.getText());
    server.setNonProxyHosts(wNonProxyHosts.getText());
    server.setSslMode(wSSL.getSelection());
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }

  public void test() {

    HopServerMeta serverMeta = getMetadata();

    getWidgetsContent(serverMeta);

    RemoteHopServer server = new RemoteHopServer(serverMeta);

    try {
      String xml = "<sample/>";
      String reply =
          server.sendXml(manager.getVariables(), xml, RegisterPipelineServlet.CONTEXT_PATH);

      String message =
          BaseMessages.getString(PKG, "HopServer.Replay.Info1")
              + server.createUrl(manager.getVariables(), RegisterPipelineServlet.CONTEXT_PATH)
              + Const.CR
              + BaseMessages.getString(PKG, "HopServer.Replay.Info2")
              + Const.CR
              + Const.CR;
      message += xml;
      message += Const.CR + Const.CR;
      message += "Reply was:" + Const.CR + Const.CR;
      message += reply + Const.CR;

      EnterTextDialog dialog =
          new EnterTextDialog(
              getShell(),
              "XML",
              BaseMessages.getString(PKG, "HopServer.RetournedXMLInfo"),
              message);
      dialog.open();
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "HopServer.ExceptionError"),
          BaseMessages.getString(PKG, "HopServer.ExceptionUnableGetReplay.Error1")
              + getVariables().resolve(serverMeta.getHostname())
              + BaseMessages.getString(PKG, "HopServer.ExceptionUnableGetReplay.Error2"),
          e);
    }
  }
}
