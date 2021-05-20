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

package org.apache.hop.pipeline.transforms.mqtt.subscriber;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.List;

public class MQTTSubscriberDialog extends BaseTransformDialog implements ITransformDialog {

  protected MQTTSubscriberMeta subscriberMeta;

  private TextVar wBroker;
  private TextVar wClientID;
  private TextVar wTimeout;
  private TextVar wkeepAlive;
  private TextVar wQOS;
  private TextVar wExecuteForDuration;

  private Button wRequiresAuth;
  private Label wlUsername;
  private TextVar wUsername;
  private Label wlPassword;
  private TextVar wPassword;

  private TextVar wCAFile;
  private TextVar wCertFile;
  private TextVar wKeyFile;
  private TextVar wKeyPassword;

  private TableView wTopicsTable;
  private CCombo wTopicMessageTypeCombo;
  private Button wAllowObjectMessages;

  public MQTTSubscriberDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String transformname) {
    super(parent, variables, (BaseTransformMeta) in, tr, transformname);
    subscriberMeta = (MQTTSubscriberMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, subscriberMeta);

    ModifyListener lsMod = e -> subscriberMeta.setChanged();
    changed = subscriberMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Step name
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.StepName.Label"));
    props.setLook(wlTransformName);

    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    // ====================
    // START OF TAB FOLDER
    // ====================
    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ====================
    // GENERAL TAB
    // ====================

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.GeneralTab.Label"));

    FormLayout mainLayout = new FormLayout();
    mainLayout.marginWidth = 3;
    mainLayout.marginHeight = 3;

    Composite wGeneralTabComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralTabComp);
    wGeneralTabComp.setLayout(mainLayout);

    // Broker URL
    Label wlBroker = new Label(wGeneralTabComp, SWT.RIGHT);
    wlBroker.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.Broker.Label"));
    props.setLook(wlBroker);
    FormData fdlBroker = new FormData();
    fdlBroker.top = new FormAttachment(0, margin * 2);
    fdlBroker.left = new FormAttachment(0, 0);
    fdlBroker.right = new FormAttachment(middle, -margin);
    wlBroker.setLayoutData(fdlBroker);
    wBroker = new TextVar(variables, wGeneralTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBroker);
    wBroker.addModifyListener(lsMod);
    FormData fdBroker = new FormData();
    fdBroker.top = new FormAttachment(0, margin * 2);
    fdBroker.left = new FormAttachment(middle, 0);
    fdBroker.right = new FormAttachment(100, 0);
    wBroker.setLayoutData(fdBroker);
    lastControl = wBroker;

    // Connection timeout
    Label wlConnectionTimeout = new Label(wGeneralTabComp, SWT.RIGHT);
    wlConnectionTimeout.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.ConnectionTimeout.Label"));
    wlConnectionTimeout.setToolTipText(
        BaseMessages.getString(
            MQTTSubscriberMeta.PKG, "MQTTClientDialog.ConnectionTimeout.ToolTip"));
    props.setLook(wlConnectionTimeout);
    FormData fdlConnectionTimeout = new FormData();
    fdlConnectionTimeout.top = new FormAttachment(lastControl, margin);
    fdlConnectionTimeout.left = new FormAttachment(0, 0);
    fdlConnectionTimeout.right = new FormAttachment(middle, -margin);
    wlConnectionTimeout.setLayoutData(fdlConnectionTimeout);
    wTimeout = new TextVar(variables, wGeneralTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTimeout);
    wTimeout.addModifyListener(lsMod);
    FormData fdConnectionTimeout = new FormData();
    fdConnectionTimeout.top = new FormAttachment(lastControl, margin);
    fdConnectionTimeout.left = new FormAttachment(middle, 0);
    fdConnectionTimeout.right = new FormAttachment(100, 0);
    wTimeout.setLayoutData(fdConnectionTimeout);
    lastControl = wTimeout;

    // Keep alive interval
    Label wKeepAliveLab = new Label(wGeneralTabComp, SWT.RIGHT);
    wKeepAliveLab.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.KeepAlive.Label"));
    wKeepAliveLab.setToolTipText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.KeepAlive.ToolTip"));
    props.setLook(wKeepAliveLab);
    FormData fd = new FormData();
    fd.top = new FormAttachment(lastControl, margin);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    wKeepAliveLab.setLayoutData(fd);
    wkeepAlive = new TextVar(variables, wGeneralTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wkeepAlive);
    wkeepAlive.addModifyListener(lsMod);
    fd = new FormData();
    fd.top = new FormAttachment(lastControl, margin);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wkeepAlive.setLayoutData(fd);
    lastControl = wkeepAlive;

    // Client ID
    Label wlClientID = new Label(wGeneralTabComp, SWT.RIGHT);
    wlClientID.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.ClientID.Label"));
    props.setLook(wlClientID);
    FormData fdlClientID = new FormData();
    fdlClientID.top = new FormAttachment(lastControl, margin);
    fdlClientID.left = new FormAttachment(0, 0);
    fdlClientID.right = new FormAttachment(middle, -margin);
    wlClientID.setLayoutData(fdlClientID);
    wClientID = new TextVar(variables, wGeneralTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wClientID);
    wClientID.addModifyListener(lsMod);
    FormData fdClientID = new FormData();
    fdClientID.top = new FormAttachment(lastControl, margin);
    fdClientID.left = new FormAttachment(middle, 0);
    fdClientID.right = new FormAttachment(100, 0);
    wClientID.setLayoutData(fdClientID);
    lastControl = wClientID;

    // QOS
    Label wlQOS = new Label(wGeneralTabComp, SWT.RIGHT);
    wlQOS.setText(BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.QOS.Label"));
    props.setLook(wlQOS);
    FormData fdlQOS = new FormData();
    fdlQOS.top = new FormAttachment(lastControl, margin);
    fdlQOS.left = new FormAttachment(0, 0);
    fdlQOS.right = new FormAttachment(middle, -margin);
    wlQOS.setLayoutData(fdlQOS);
    wQOS = new TextVar(variables, wGeneralTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wQOS);
    wQOS.addModifyListener(lsMod);
    FormData fdQOS = new FormData();
    fdQOS.top = new FormAttachment(lastControl, margin);
    fdQOS.left = new FormAttachment(middle, 0);
    fdQOS.right = new FormAttachment(100, 0);
    wQOS.setLayoutData(fdQOS);
    lastControl = wQOS;

    // Execute for duration
    Label wExecuteForLab = new Label(wGeneralTabComp, SWT.RIGHT);
    wExecuteForLab.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.ExecuteFor.Label"));
    wExecuteForLab.setToolTipText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.ExecuteFor.ToolTip"));
    props.setLook(wExecuteForLab);
    fd = new FormData();
    fd.top = new FormAttachment(lastControl, margin);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    wExecuteForLab.setLayoutData(fd);

    wExecuteForDuration =
        new TextVar(variables, wGeneralTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wExecuteForDuration);
    fd = new FormData();
    fd.top = new FormAttachment(lastControl, margin);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wExecuteForDuration.setLayoutData(fd);
    lastControl = wExecuteForDuration;

    FormData fdGeneralTabComp = new FormData();
    fdGeneralTabComp.left = new FormAttachment(0, 0);
    fdGeneralTabComp.top = new FormAttachment(0, 0);
    fdGeneralTabComp.right = new FormAttachment(100, 0);
    fdGeneralTabComp.bottom = new FormAttachment(100, 0);
    wGeneralTabComp.setLayoutData(fdGeneralTabComp);

    wGeneralTabComp.layout();
    wGeneralTab.setControl(wGeneralTabComp);

    // ====================
    // CREDENTIALS TAB
    // ====================
    CTabItem wCredentialsTab = new CTabItem(wTabFolder, SWT.NONE);
    wCredentialsTab.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.CredentialsTab.Title"));

    Composite wCredentialsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wCredentialsComp);

    FormLayout fieldsCompLayout = new FormLayout();
    fieldsCompLayout.marginWidth = Const.FORM_MARGIN;
    fieldsCompLayout.marginHeight = Const.FORM_MARGIN;
    wCredentialsComp.setLayout(fieldsCompLayout);

    Label wlRequiresAuth = new Label(wCredentialsComp, SWT.RIGHT);
    wlRequiresAuth.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.RequireAuth.Label"));
    props.setLook(wlRequiresAuth);
    FormData fdlRequriesAuth = new FormData();
    fdlRequriesAuth.left = new FormAttachment(0, 0);
    fdlRequriesAuth.top = new FormAttachment(0, margin * 2);
    fdlRequriesAuth.right = new FormAttachment(middle, -margin);
    wlRequiresAuth.setLayoutData(fdlRequriesAuth);
    wRequiresAuth = new Button(wCredentialsComp, SWT.CHECK);
    props.setLook(wRequiresAuth);
    FormData fdRequiresAuth = new FormData();
    fdRequiresAuth.left = new FormAttachment(middle, 0);
    fdRequiresAuth.top = new FormAttachment(wlRequiresAuth, 0, SWT.CENTER);
    fdRequiresAuth.right = new FormAttachment(100, 0);
    wRequiresAuth.setLayoutData(fdRequiresAuth);
    wRequiresAuth.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            boolean enabled = wRequiresAuth.getSelection();
            wlUsername.setEnabled(enabled);
            wUsername.setEnabled(enabled);
            wlPassword.setEnabled(enabled);
            wPassword.setEnabled(enabled);
          }
        });
    lastControl = wlRequiresAuth;

    // Username field
    wlUsername = new Label(wCredentialsComp, SWT.RIGHT);
    wlUsername.setEnabled(false);
    wlUsername.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.Username.Label"));
    props.setLook(wlUsername);
    FormData fdlUsername = new FormData();
    fdlUsername.left = new FormAttachment(0, -margin);
    fdlUsername.right = new FormAttachment(middle, -2 * margin);
    fdlUsername.top = new FormAttachment(lastControl, 2 * margin);
    wlUsername.setLayoutData(fdlUsername);

    wUsername = new TextVar(variables, wCredentialsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wUsername.setEnabled(false);
    wUsername.setToolTipText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.Username.Tooltip"));
    props.setLook(wUsername);
    wUsername.addModifyListener(lsMod);
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment(middle, -margin);
    fdResult.top = new FormAttachment(wlUsername, 0, SWT.CENTER);
    fdResult.right = new FormAttachment(100, 0);
    wUsername.setLayoutData(fdResult);
    lastControl = wUsername;

    // Password field
    wlPassword = new Label(wCredentialsComp, SWT.RIGHT);
    wlPassword.setEnabled(false);
    wlPassword.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.Password.Label"));
    props.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment(0, -margin);
    fdlPassword.right = new FormAttachment(middle, -2 * margin);
    fdlPassword.top = new FormAttachment(lastControl, margin);
    wlPassword.setLayoutData(fdlPassword);

    wPassword =
        new TextVar(variables, wCredentialsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    wPassword.setEnabled(false);
    wPassword.setToolTipText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.Password.Tooltip"));
    props.setLook(wPassword);
    wPassword.addModifyListener(lsMod);
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(middle, -margin);
    fdPassword.top = new FormAttachment(lastControl, margin);
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);

    FormData fdCredentialsComp = new FormData();
    fdCredentialsComp.left = new FormAttachment(0, 0);
    fdCredentialsComp.top = new FormAttachment(0, 0);
    fdCredentialsComp.right = new FormAttachment(100, 0);
    fdCredentialsComp.bottom = new FormAttachment(100, 0);
    wCredentialsComp.setLayoutData(fdCredentialsComp);

    wCredentialsComp.layout();
    wCredentialsTab.setControl(wCredentialsComp);

    // ====================
    // SSL TAB
    // ====================
    CTabItem wSSLTab = new CTabItem(wTabFolder, SWT.NONE);
    wSSLTab.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.SSLTab.Label"));

    Composite wSSLComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wSSLComp);

    FormLayout sslCompLayout = new FormLayout();
    sslCompLayout.marginWidth = Const.FORM_MARGIN;
    sslCompLayout.marginHeight = Const.FORM_MARGIN;
    wSSLComp.setLayout(sslCompLayout);

    // Server CA file path
    Label wlCAFile = new Label(wSSLComp, SWT.RIGHT);
    wlCAFile.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.CAFile.Label"));
    props.setLook(wlCAFile);
    FormData fdlCAFile = new FormData();
    fdlCAFile.left = new FormAttachment(0, -margin);
    fdlCAFile.right = new FormAttachment(middle, -2 * margin);
    fdlCAFile.top = new FormAttachment(0, 2 * margin);
    wlCAFile.setLayoutData(fdlCAFile);

    wCAFile = new TextVar(variables, wSSLComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wCAFile.setToolTipText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.CAFile.Tooltip"));
    props.setLook(wCAFile);
    wCAFile.addModifyListener(lsMod);
    FormData fdCAFile = new FormData();
    fdCAFile.left = new FormAttachment(middle, -margin);
    fdCAFile.top = new FormAttachment(0, 2 * margin);
    fdCAFile.right = new FormAttachment(100, 0);
    wCAFile.setLayoutData(fdCAFile);
    lastControl = wCAFile;

    // Client certificate file path
    Label wlCertFile = new Label(wSSLComp, SWT.RIGHT);
    wlCertFile.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.CertFile.Label"));
    props.setLook(wlCertFile);
    FormData fdlCertFile = new FormData();
    fdlCertFile.left = new FormAttachment(0, -margin);
    fdlCertFile.right = new FormAttachment(middle, -2 * margin);
    fdlCertFile.top = new FormAttachment(lastControl, margin);
    wlCertFile.setLayoutData(fdlCertFile);

    wCertFile = new TextVar(variables, wSSLComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wCertFile.setToolTipText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.CertFile.Tooltip"));
    props.setLook(wCertFile);
    wCertFile.addModifyListener(lsMod);
    FormData fdCertFile = new FormData();
    fdCertFile.left = new FormAttachment(middle, -margin);
    fdCertFile.top = new FormAttachment(lastControl, margin);
    fdCertFile.right = new FormAttachment(100, 0);
    wCertFile.setLayoutData(fdCertFile);
    lastControl = wCertFile;

    // Client key file path
    Label wlKeyFile = new Label(wSSLComp, SWT.RIGHT);
    wlKeyFile.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.KeyFile.Label"));
    props.setLook(wlKeyFile);
    FormData fdlKeyFile = new FormData();
    fdlKeyFile.left = new FormAttachment(0, -margin);
    fdlKeyFile.right = new FormAttachment(middle, -2 * margin);
    fdlKeyFile.top = new FormAttachment(lastControl, margin);
    wlKeyFile.setLayoutData(fdlKeyFile);

    wKeyFile = new TextVar(variables, wSSLComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wKeyFile.setToolTipText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.KeyFile.Tooltip"));
    props.setLook(wKeyFile);
    wKeyFile.addModifyListener(lsMod);
    FormData fdKeyFile = new FormData();
    fdKeyFile.left = new FormAttachment(middle, -margin);
    fdKeyFile.top = new FormAttachment(lastControl, margin);
    fdKeyFile.right = new FormAttachment(100, 0);
    wKeyFile.setLayoutData(fdKeyFile);
    lastControl = wKeyFile;

    // Client key file password path
    Label wlKeyPassword = new Label(wSSLComp, SWT.RIGHT);
    wlKeyPassword.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.KeyPassword.Label"));
    props.setLook(wlKeyPassword);
    FormData fdlKeyPassword = new FormData();
    fdlKeyPassword.left = new FormAttachment(0, -margin);
    fdlKeyPassword.right = new FormAttachment(middle, -2 * margin);
    fdlKeyPassword.top = new FormAttachment(lastControl, margin);
    wlKeyPassword.setLayoutData(fdlKeyPassword);

    wKeyPassword =
        new TextVar(variables, wSSLComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
    wKeyPassword.setToolTipText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.KeyPassword.Tooltip"));
    props.setLook(wKeyPassword);
    wKeyPassword.addModifyListener(lsMod);
    FormData fdKeyPassword = new FormData();
    fdKeyPassword.left = new FormAttachment(middle, -margin);
    fdKeyPassword.top = new FormAttachment(lastControl, margin);
    fdKeyPassword.right = new FormAttachment(100, 0);
    wKeyPassword.setLayoutData(fdKeyPassword);
    lastControl = wKeyPassword;

    FormData fdSSLComp = new FormData();
    fdSSLComp.left = new FormAttachment(0, 0);
    fdSSLComp.top = new FormAttachment(0, 0);
    fdSSLComp.right = new FormAttachment(100, 0);
    fdSSLComp.bottom = new FormAttachment(100, 0);
    wSSLComp.setLayoutData(fdSSLComp);

    wSSLComp.layout();
    wSSLTab.setControl(wSSLComp);

    // ====================
    // Topics TAB
    // ====================
    CTabItem wTopicsTab = new CTabItem(wTabFolder, SWT.NONE);
    wTopicsTab.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.TopicsTab.Label"));
    Composite wTopicsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wTopicsComp);
    FormLayout topicsLayout = new FormLayout();
    topicsLayout.marginWidth = Const.FORM_MARGIN;
    topicsLayout.marginHeight = Const.FORM_MARGIN;
    wTopicsComp.setLayout(topicsLayout);

    Label wTopicTypeLab = new Label(wTopicsComp, SWT.RIGHT);
    props.setLook(wTopicTypeLab);
    wTopicTypeLab.setText(
        BaseMessages.getString(MQTTSubscriberMeta.PKG, "MQTTClientDialog.TopicMessageType.Label"));
    wTopicTypeLab.setToolTipText(
        BaseMessages.getString(
            MQTTSubscriberMeta.PKG, "MQTTClientDialog.TopicMessageType.ToolTip"));
    fd = new FormData();
    fd.top = new FormAttachment(0, margin * 2);
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    wTopicTypeLab.setLayoutData(fd);

    wTopicMessageTypeCombo = new CCombo(wTopicsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTopicMessageTypeCombo);
    wTopicMessageTypeCombo.setItems(ValueMetaFactory.getAllValueMetaNames());
    fd = new FormData();
    fd.top = new FormAttachment(0, margin * 2);
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    wTopicMessageTypeCombo.setLayoutData(fd);
    lastControl = wTopicMessageTypeCombo;

    Label wlAllowObjectMessages = new Label(wTopicsComp, SWT.RIGHT);
    wlAllowObjectMessages.setText(
        BaseMessages.getString(
            MQTTSubscriberMeta.PKG, "MQTTClientDialog.AllowObjectMessages.Label"));
    props.setLook(wlAllowObjectMessages);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(wTopicMessageTypeCombo, margin * 2);
    fd.right = new FormAttachment(middle, -margin);
    wlAllowObjectMessages.setLayoutData(fd);

    wAllowObjectMessages = new Button(wTopicsComp, SWT.CHECK);
    props.setLook(wAllowObjectMessages);
    fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(wlAllowObjectMessages, 0, SWT.CENTER);
    fd.right = new FormAttachment(100, 0);
    wAllowObjectMessages.setLayoutData(fd);
    lastControl = wlAllowObjectMessages;

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo("Topic", ColumnInfo.COLUMN_TYPE_TEXT),
        };

    // colinf[ 1 ].setComboValues( ValueMetaFactory.getAllValueMetaNames() );
    wTopicsTable =
        new TableView(
            variables,
            wTopicsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            5,
            lsMod,
            props);
    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(lastControl, 2 * margin);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wTopicsTable.setLayoutData(fd);

    fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment(100, 0);
    wTopicsComp.setLayoutData(fd);
    wTopicsComp.layout();
    wTopicsTab.setControl(wTopicsComp);

    // ====================
    // BUTTONS
    // ====================
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(MQTTSubscriberMeta.PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(MQTTSubscriberMeta.PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // ====================
    // END OF TAB FOLDER
    // ====================
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners
    wCancel.addListener(SWT.Selection, e -> cancel());
    wOk.addListener(SWT.Selection, e -> ok());

    wTabFolder.setSelection(0);

    getData(subscriberMeta, true);
    subscriberMeta.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void ok() {
    if (!Utils.isEmpty(wTransformName.getText())) {
      setData(subscriberMeta);
      transformName = wTransformName.getText();
      dispose();
    }
  }

  private void cancel() {
    transformName = null;
    subscriberMeta.setChanged(changed);
    dispose();
  }

  private void setData(MQTTSubscriberMeta subscriberMeta) {
    subscriberMeta.setBroker(wBroker.getText());

    subscriberMeta.setClientId(wClientID.getText());
    subscriberMeta.setTimeout(wTimeout.getText());
    subscriberMeta.setKeepAliveInterval(wkeepAlive.getText());
    subscriberMeta.setExecuteForDuration(wExecuteForDuration.getText());
    subscriberMeta.setQoS(wQOS.getText());

    boolean requiresAuth = wRequiresAuth.getSelection();
    subscriberMeta.setRequiresAuth(requiresAuth);
    if (requiresAuth) {
      subscriberMeta.setUsername(wUsername.getText());
      subscriberMeta.setPassword(wPassword.getText());
    }

    subscriberMeta.setAllowReadMessageOfTypeObject(wAllowObjectMessages.getSelection());

    subscriberMeta.setSSLCaFile(wCAFile.getText());
    subscriberMeta.setSSLCertFile(wCertFile.getText());
    subscriberMeta.setSSLKeyFile(wKeyFile.getText());
    subscriberMeta.setSSLKeyFilePass(wKeyPassword.getText());
    subscriberMeta.setMessageType(
        Const.NVL(
            wTopicMessageTypeCombo.getText(),
            ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING)));

    // fields
    int nrNonEmptyFields = wTopicsTable.nrNonEmpty();
    List<String> topics = new ArrayList<String>();
    for (int i = 0; i < nrNonEmptyFields; i++) {
      TableItem item = wTopicsTable.getNonEmpty(i);
      topics.add(item.getText(1).trim());
    }
    subscriberMeta.setTopics(topics);

    subscriberMeta.setChanged();
  }

  private void getData(MQTTSubscriberMeta subscriberMeta, boolean copyStepname) {
    if (copyStepname) {
      wTransformName.setText(transformName);
    }
    wBroker.setText(Const.NVL(subscriberMeta.getBroker(), ""));
    wClientID.setText(Const.NVL(subscriberMeta.getClientId(), ""));
    wTimeout.setText(Const.NVL(subscriberMeta.getTimeout(), "30"));
    wkeepAlive.setText(Const.NVL(subscriberMeta.getKeepAliveInterval(), "60"));
    wQOS.setText(Const.NVL(subscriberMeta.getQoS(), "0"));
    wExecuteForDuration.setText(Const.NVL(subscriberMeta.getExecuteForDuration(), "0"));

    wRequiresAuth.setSelection(subscriberMeta.isRequiresAuth());
    wRequiresAuth.notifyListeners(SWT.Selection, new Event());

    wUsername.setText(Const.NVL(subscriberMeta.getUsername(), ""));
    wPassword.setText(Const.NVL(subscriberMeta.getPassword(), ""));

    wAllowObjectMessages.setSelection(subscriberMeta.getAllowReadMessageOfTypeObject());

    wCAFile.setText(Const.NVL(subscriberMeta.getSSLCaFile(), ""));
    wCertFile.setText(Const.NVL(subscriberMeta.getSSLCertFile(), ""));
    wKeyFile.setText(Const.NVL(subscriberMeta.getSSLKeyFile(), ""));
    wKeyPassword.setText(Const.NVL(subscriberMeta.getSSLKeyFilePass(), ""));
    wTopicMessageTypeCombo.setText(
        Const.NVL(
            subscriberMeta.getMessageType(),
            ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING)));

    List<String> topics = subscriberMeta.getTopics();
    if (topics.size() > 0) {
      wTopicsTable.clearAll(false);
      Table table = wTopicsTable.getTable();
      for (String t : topics) {
        TableItem item = new TableItem(table, SWT.NONE);
        item.setText(1, t.trim());
      }

      wTopicsTable.removeEmptyRows();
      wTopicsTable.setRowNums();
      wTopicsTable.optWidth(true);
    }
  }
}
