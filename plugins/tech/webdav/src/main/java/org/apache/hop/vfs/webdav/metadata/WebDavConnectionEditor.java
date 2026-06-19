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
package org.apache.hop.vfs.webdav.metadata;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

@GuiPlugin(description = "Editor for WebDAV connection metadata")
public class WebDavConnectionEditor extends MetadataEditor<WebDavConnection> {

  private static final Class<?> PKG = WebDavConnectionEditor.class;

  private Text wName;
  private Text wDescription;
  private TextVar wRootUrl;
  private TextVar wUsername;
  private PasswordTextVar wPassword;
  private Button wFollowRedirects;
  private Button wPreemptiveAuth;

  public WebDavConnectionEditor(
      HopGui hopGui, MetadataManager<WebDavConnection> manager, WebDavConnection metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin() + 2;

    Control lastControl;

    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "WebDavConnectionEditor.Name.Label"));
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

    Label wlDescription = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlDescription);
    wlDescription.setText(BaseMessages.getString(PKG, "WebDavConnectionEditor.Description.Label"));
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment(lastControl, margin);
    fdlDescription.left = new FormAttachment(0, 0);
    fdlDescription.right = new FormAttachment(middle, -margin);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment(wlDescription, 0, SWT.CENTER);
    fdDescription.left = new FormAttachment(middle, 0);
    fdDescription.right = new FormAttachment(95, 0);
    wDescription.setLayoutData(fdDescription);
    lastControl = wDescription;

    Label wlRootUrl = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlRootUrl);
    wlRootUrl.setText(BaseMessages.getString(PKG, "WebDavConnectionEditor.RootUrl.Label"));
    FormData fdlRootUrl = new FormData();
    fdlRootUrl.top = new FormAttachment(lastControl, margin);
    fdlRootUrl.left = new FormAttachment(0, 0);
    fdlRootUrl.right = new FormAttachment(middle, -margin);
    wlRootUrl.setLayoutData(fdlRootUrl);
    wRootUrl = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRootUrl);
    FormData fdRootUrl = new FormData();
    fdRootUrl.top = new FormAttachment(wlRootUrl, 0, SWT.CENTER);
    fdRootUrl.left = new FormAttachment(middle, 0);
    fdRootUrl.right = new FormAttachment(95, 0);
    wRootUrl.setLayoutData(fdRootUrl);
    lastControl = wRootUrl;

    Label wlUsername = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlUsername);
    wlUsername.setText(BaseMessages.getString(PKG, "WebDavConnectionEditor.Username.Label"));
    FormData fdlUsername = new FormData();
    fdlUsername.top = new FormAttachment(lastControl, margin);
    fdlUsername.left = new FormAttachment(0, 0);
    fdlUsername.right = new FormAttachment(middle, -margin);
    wlUsername.setLayoutData(fdlUsername);
    wUsername = new TextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUsername);
    FormData fdUsername = new FormData();
    fdUsername.top = new FormAttachment(wlUsername, 0, SWT.CENTER);
    fdUsername.left = new FormAttachment(middle, 0);
    fdUsername.right = new FormAttachment(95, 0);
    wUsername.setLayoutData(fdUsername);
    lastControl = wUsername;

    Label wlPassword = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlPassword);
    wlPassword.setText(BaseMessages.getString(PKG, "WebDavConnectionEditor.Password.Label"));
    FormData fdlPassword = new FormData();
    fdlPassword.top = new FormAttachment(lastControl, margin);
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword =
        new PasswordTextVar(manager.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPassword);
    FormData fdPassword = new FormData();
    fdPassword.top = new FormAttachment(wlPassword, 0, SWT.CENTER);
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.right = new FormAttachment(95, 0);
    wPassword.setLayoutData(fdPassword);
    lastControl = wPassword;

    Label wlFollowRedirects = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlFollowRedirects);
    wlFollowRedirects.setText(
        BaseMessages.getString(PKG, "WebDavConnectionEditor.FollowRedirects.Label"));
    FormData fdlFollowRedirects = new FormData();
    fdlFollowRedirects.top = new FormAttachment(lastControl, margin);
    fdlFollowRedirects.left = new FormAttachment(0, 0);
    fdlFollowRedirects.right = new FormAttachment(middle, -margin);
    wlFollowRedirects.setLayoutData(fdlFollowRedirects);
    wFollowRedirects = new Button(parent, SWT.CHECK);
    PropsUi.setLook(wFollowRedirects);
    FormData fdFollowRedirects = new FormData();
    fdFollowRedirects.top = new FormAttachment(wlFollowRedirects, 0, SWT.CENTER);
    fdFollowRedirects.left = new FormAttachment(middle, 0);
    fdFollowRedirects.right = new FormAttachment(95, 0);
    wFollowRedirects.setLayoutData(fdFollowRedirects);
    lastControl = wFollowRedirects;

    Label wlPreemptive = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlPreemptive);
    wlPreemptive.setText(
        BaseMessages.getString(PKG, "WebDavConnectionEditor.PreemptiveAuth.Label"));
    FormData fdlPreemptive = new FormData();
    fdlPreemptive.top = new FormAttachment(lastControl, margin);
    fdlPreemptive.left = new FormAttachment(0, 0);
    fdlPreemptive.right = new FormAttachment(middle, -margin);
    wlPreemptive.setLayoutData(fdlPreemptive);
    wPreemptiveAuth = new Button(parent, SWT.CHECK);
    PropsUi.setLook(wPreemptiveAuth);
    FormData fdPreemptive = new FormData();
    fdPreemptive.top = new FormAttachment(wlPreemptive, 0, SWT.CENTER);
    fdPreemptive.left = new FormAttachment(middle, 0);
    fdPreemptive.right = new FormAttachment(95, 0);
    wPreemptiveAuth.setLayoutData(fdPreemptive);

    setWidgetsContent();

    wName.addModifyListener(e -> setChanged());
    wDescription.addModifyListener(e -> setChanged());
    wRootUrl.addModifyListener(e -> setChanged());
    wUsername.addModifyListener(e -> setChanged());
    wPassword.addModifyListener(e -> setChanged());
    wFollowRedirects.addListener(SWT.Selection, e -> setChanged());
    wPreemptiveAuth.addListener(SWT.Selection, e -> setChanged());
  }

  @Override
  public void setWidgetsContent() {
    WebDavConnection c = getMetadata();
    wName.setText(Const.NVL(c.getName(), ""));
    wDescription.setText(Const.NVL(c.getDescription(), ""));
    wRootUrl.setText(Const.NVL(c.getRootUrl(), ""));
    wUsername.setText(Const.NVL(c.getUsername(), ""));
    wPassword.setText(Const.NVL(c.getPassword(), ""));
    wFollowRedirects.setSelection(c.isFollowRedirects());
    wPreemptiveAuth.setSelection(c.isPreemptiveAuth());
  }

  @Override
  public void getWidgetsContent(WebDavConnection c) {
    c.setName(wName.getText());
    c.setDescription(wDescription.getText());
    c.setRootUrl(wRootUrl.getText());
    c.setUsername(wUsername.getText());
    c.setPassword(wPassword.getText());
    c.setFollowRedirects(wFollowRedirects.getSelection());
    c.setPreemptiveAuth(wPreemptiveAuth.getSelection());
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
