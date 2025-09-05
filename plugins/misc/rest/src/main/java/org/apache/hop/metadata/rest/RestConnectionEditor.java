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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
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

public class RestConnectionEditor extends MetadataEditor<RestConnection> {
  private static final Class<?> PKG = RestConnectionEditor.class;

  private Text wName;

  private TextVar wBaseUrl;
  private TextVar wTestUrl;
  private TextVar wAuthorizationName;
  private TextVar wAuthorizationPrefix;
  private PasswordTextVar wAuthorizationValue;

  public RestConnectionEditor(
      HopGui hopGui, MetadataManager<RestConnection> manager, RestConnection restConnection) {
    super(hopGui, manager, restConnection);
  }

  @Override
  public void createControl(Composite composite) {
    PropsUi props = PropsUi.getInstance();

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    IVariables variables = hopGui.getVariables();

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
    Control lastControl = wName;

    Label wlBaseUrl = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlBaseUrl);
    wlBaseUrl.setText(BaseMessages.getString(PKG, "RestConnectionEditor.BaseUrl"));
    FormData fdlBaseUrl = new FormData();
    fdlBaseUrl.top = new FormAttachment(lastControl, margin);
    fdlBaseUrl.left = new FormAttachment(0, 0);
    fdlBaseUrl.right = new FormAttachment(middle, -margin);
    wlBaseUrl.setLayoutData(fdlBaseUrl);
    wBaseUrl = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBaseUrl);
    FormData fdBaseUrl = new FormData();
    fdBaseUrl.top = new FormAttachment(wlBaseUrl, 0, SWT.CENTER);
    fdBaseUrl.left = new FormAttachment(middle, 0);
    fdBaseUrl.right = new FormAttachment(95, 0);
    wBaseUrl.setLayoutData(fdBaseUrl);
    lastControl = wBaseUrl;

    Label wlTestUrl = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlTestUrl);
    wlTestUrl.setText(BaseMessages.getString(PKG, "RestConnectionEditor.TestUrl"));
    FormData fdlTestUrl = new FormData();
    fdlTestUrl.top = new FormAttachment(lastControl, margin);
    fdlTestUrl.left = new FormAttachment(0, 0);
    fdlTestUrl.right = new FormAttachment(middle, -margin);
    wlTestUrl.setLayoutData(fdlTestUrl);
    wTestUrl = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTestUrl);
    FormData fdTestUrl = new FormData();
    fdTestUrl.top = new FormAttachment(wlTestUrl, 0, SWT.CENTER);
    fdTestUrl.left = new FormAttachment(middle, 0);
    fdTestUrl.right = new FormAttachment(95, 0);
    wTestUrl.setLayoutData(fdTestUrl);
    lastControl = wTestUrl;

    Label wlAuthorizationName = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlAuthorizationName);
    wlAuthorizationName.setText(
        BaseMessages.getString(PKG, "RestConnectionEditor.AuthorizationName"));
    FormData fdlAuthorizationName = new FormData();
    fdlAuthorizationName.top = new FormAttachment(lastControl, margin);
    fdlAuthorizationName.left = new FormAttachment(0, 0);
    fdlAuthorizationName.right = new FormAttachment(middle, -margin);
    wlAuthorizationName.setLayoutData(fdlAuthorizationName);
    wAuthorizationName = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAuthorizationName);
    FormData fdAuthorizationName = new FormData();
    fdAuthorizationName.top = new FormAttachment(wlAuthorizationName, 0, SWT.CENTER);
    fdAuthorizationName.left = new FormAttachment(middle, 0);
    fdAuthorizationName.right = new FormAttachment(95, 0);
    wAuthorizationName.setLayoutData(fdAuthorizationName);
    lastControl = wAuthorizationName;

    Label wlAuthorizationPrefix = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlAuthorizationPrefix);
    wlAuthorizationPrefix.setText(
        BaseMessages.getString(PKG, "RestConnectionEditor.AuthorizationPrefix"));
    FormData fdlAuthorizationPrefix = new FormData();
    fdlAuthorizationPrefix.top = new FormAttachment(lastControl, margin);
    fdlAuthorizationPrefix.left = new FormAttachment(0, 0);
    fdlAuthorizationPrefix.right = new FormAttachment(middle, -margin);
    wlAuthorizationPrefix.setLayoutData(fdlAuthorizationPrefix);

    wAuthorizationPrefix = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAuthorizationPrefix);
    FormData fdAuthorizationPrefix = new FormData();
    fdAuthorizationPrefix.top = new FormAttachment(wlAuthorizationPrefix, 0, SWT.CENTER);
    fdAuthorizationPrefix.left = new FormAttachment(middle, 0);
    fdAuthorizationPrefix.right = new FormAttachment(95, 0);
    wAuthorizationPrefix.setLayoutData(fdAuthorizationPrefix);
    lastControl = wAuthorizationPrefix;

    Label wlAuthorizationValue = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlAuthorizationValue);
    wlAuthorizationValue.setText(
        BaseMessages.getString(PKG, "RestConnectionEditor.AuthorizationValue"));
    FormData fdlAuthorizationValue = new FormData();
    fdlAuthorizationValue.top = new FormAttachment(lastControl, margin);
    fdlAuthorizationValue.left = new FormAttachment(0, 0);
    fdlAuthorizationValue.right = new FormAttachment(middle, -margin);
    wlAuthorizationValue.setLayoutData(fdlAuthorizationValue);
    wAuthorizationValue =
        new PasswordTextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAuthorizationValue);
    FormData fdAuthorizationValue = new FormData();
    fdAuthorizationValue.top = new FormAttachment(wlAuthorizationValue, 0, SWT.CENTER);
    fdAuthorizationValue.left = new FormAttachment(middle, 0);
    fdAuthorizationValue.right = new FormAttachment(95, 0);
    wAuthorizationValue.setLayoutData(fdAuthorizationValue);
    lastControl = wAuthorizationValue;

    setWidgetsContent();

    Control[] controls = {wName, wAuthorizationName, wAuthorizationValue, wBaseUrl, wTestUrl};
    for (Control control : controls) {
      control.addListener(SWT.Modify, e -> setChanged());
      control.addListener(SWT.Selection, e -> setChanged());
    }
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
    restConnection.setAuthorizationHeaderName(wAuthorizationName.getText());
    restConnection.setAuthorizationPrefix(wAuthorizationPrefix.getText());
    restConnection.setAuthorizationHeaderValue(wAuthorizationValue.getText());
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
    wName.setText(Const.NVL(metadata.getName(), ""));
    wBaseUrl.setText(Const.NVL(metadata.getBaseUrl(), ""));
    wTestUrl.setText(Const.NVL(metadata.getTestUrl(), ""));
    wAuthorizationName.setText(Const.NVL(metadata.getAuthorizationHeaderName(), ""));
    wAuthorizationPrefix.setText(Const.NVL(metadata.getAuthorizationPrefix(), ""));
    wAuthorizationValue.setText(Const.NVL(metadata.getAuthorizationHeaderValue(), ""));
  }

  @Override
  public void getWidgetsContent(RestConnection connection) {
    connection.setName(wName.getText());
    connection.setBaseUrl(wBaseUrl.getText());
    connection.setTestUrl(wTestUrl.getText());
    connection.setAuthorizationHeaderName(wAuthorizationName.getText());
    connection.setAuthorizationPrefix(wAuthorizationPrefix.getText());
    connection.setAuthorizationHeaderValue(wAuthorizationValue.getText());
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }
}
