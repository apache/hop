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

package org.apache.hop.pipeline.transforms.mqtt.publisher;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;


public class MQTTPublisherDialog extends BaseTransformDialog implements ITransformDialog {

    private MQTTPublisherMeta producerMeta;

    private CCombo m_wInputField;

    private CTabFolder m_wTabFolder;

    private CTabItem m_wGeneralTab;
    private TextVar m_wBroker;
    private Label m_wlTopicName;
    private CCombo m_wTopicName;
    private TextVar m_wClientID;
    private TextVar m_wTimeout;
    private TextVar m_wQOS;

    private Button m_wTopicFromIncomingField;

    private CTabItem m_wCredentialsTab;
    private Button m_wRequiresAuth;
    private Label m_wlUsername;
    private TextVar m_wUsername;
    private Label m_wlPassword;
    private TextVar m_wPassword;

    private CTabItem m_wSSLTab;
    private TextVar m_wCAFile;
    private TextVar m_wCertFile;
    private TextVar m_wKeyFile;
    private TextVar m_wKeyPassword;

    public MQTTPublisherDialog(Shell parent, IVariables variables, Object in, PipelineMeta tr, String transformname) {
        super(parent, variables, (BaseTransformMeta) in, tr, transformname);
        producerMeta = (MQTTPublisherMeta) in;
    }

  /*public MQTTPublisherDialog( Shell parent, BaseStepMeta baseStepMeta, TransMeta transMeta, String stepname ) {
    super( parent, baseStepMeta, transMeta, stepname );
    producerMeta = (MQTTPublisherMeta) baseStepMeta;
  }

  public MQTTPublisherDialog( Shell parent, int nr, BaseStepMeta in, TransMeta tr ) {
    super( parent, nr, in, tr );
    producerMeta = (MQTTPublisherMeta) in;
  }*/

    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        props.setLook(shell);
        setShellImage(shell, producerMeta);

        ModifyListener lsMod = new ModifyListener() {
            public void modifyText(ModifyEvent e) {
                producerMeta.setChanged();
            }
        };
        changed = producerMeta.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "MQTTClientDialog.Shell.Title"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        // Step name
        wlTransformName = new Label(shell, SWT.RIGHT);
        wlTransformName.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "MQTTClientDialog.StepName.Label"));
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
        m_wTabFolder = new CTabFolder(shell, SWT.BORDER);
        props.setLook(m_wTabFolder, Props.WIDGET_STYLE_TAB);

        // ====================
        // GENERAL TAB
        // ====================

        m_wGeneralTab = new CTabItem(m_wTabFolder, SWT.NONE);
        m_wGeneralTab.setText(BaseMessages
                .getString(MQTTPublisherMeta.PKG,
                        "MQTTClientDialog.GeneralTab.Label"));

        FormLayout mainLayout = new FormLayout();
        mainLayout.marginWidth = 3;
        mainLayout.marginHeight = 3;

        Composite wGeneralTabComp = new Composite(m_wTabFolder, SWT.NONE);
        props.setLook(wGeneralTabComp);
        wGeneralTabComp.setLayout(mainLayout);

        // Broker URL
        Label wlBroker = new Label(wGeneralTabComp, SWT.RIGHT);
        wlBroker.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "MQTTClientDialog.Broker.Label"));
        props.setLook(wlBroker);
        FormData fdlBroker = new FormData();
        fdlBroker.top = new FormAttachment(0, margin * 2);
        fdlBroker.left = new FormAttachment(0, 0);
        fdlBroker.right = new FormAttachment(middle, -margin);
        wlBroker.setLayoutData(fdlBroker);
        m_wBroker = new TextVar(variables, wGeneralTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_wBroker);
        m_wBroker.addModifyListener(lsMod);
        FormData fdBroker = new FormData();
        fdBroker.top = new FormAttachment(0, margin * 2);
        fdBroker.left = new FormAttachment(middle, 0);
        fdBroker.right = new FormAttachment(100, 0);
        m_wBroker.setLayoutData(fdBroker);
        lastControl = m_wBroker;

        // Topic name
        m_wlTopicName = new Label(wGeneralTabComp, SWT.RIGHT);
        m_wlTopicName.setText(BaseMessages
                .getString(MQTTPublisherMeta.PKG,
                        "MQTTClientDialog.TopicName.Label"));
        props.setLook(m_wlTopicName);
        FormData fdlTopicName = new FormData();
        fdlTopicName.top = new FormAttachment(lastControl, margin);
        fdlTopicName.left = new FormAttachment(0, 0);
        fdlTopicName.right = new FormAttachment(middle, -margin);
        m_wlTopicName.setLayoutData(fdlTopicName);
        m_wTopicName = new CCombo(wGeneralTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_wTopicName);
        m_wTopicName.addModifyListener(lsMod);
        FormData fdTopicName = new FormData();
        fdTopicName.top = new FormAttachment(lastControl, margin);
        fdTopicName.left = new FormAttachment(middle, 0);
        fdTopicName.right = new FormAttachment(100, 0);
        m_wTopicName.setLayoutData(fdTopicName);
        lastControl = m_wTopicName;

        // Topic is from field
        Label wlTopicIsFromFieldLab = new Label(wGeneralTabComp, SWT.RIGHT);
        wlTopicIsFromFieldLab.setText(
                BaseMessages.getString(MQTTPublisherMeta.PKG, "MQTTClientDialog.TopicNameIsInIncomingField.Label"));
        props.setLook(wlTopicIsFromFieldLab);
        FormData fd = new FormData();
        fd.top = new FormAttachment(lastControl, margin);
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -margin);
        wlTopicIsFromFieldLab.setLayoutData(fd);

        m_wTopicFromIncomingField = new Button(wGeneralTabComp, SWT.CHECK);
        props.setLook(m_wTopicFromIncomingField);
        m_wTopicFromIncomingField.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent selectionEvent) {
                super.widgetSelected(selectionEvent);
                producerMeta.setChanged();
                updateTopicCombo(getPreviousFields());
            }
        });
        fd = new FormData();
        fd.top = new FormAttachment(lastControl, margin);
        fd.left = new FormAttachment(middle, 0);
        fd.right = new FormAttachment(100, 0);
        m_wTopicFromIncomingField.setLayoutData(fd);
        lastControl = m_wTopicFromIncomingField;

        // Input field
        IRowMeta previousFields = getPreviousFields();

        Label wlInputField = new Label(wGeneralTabComp, SWT.RIGHT);
        wlInputField.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "MQTTClientDialog.FieldName.Label"));
        props.setLook(wlInputField);
        FormData fdlInputField = new FormData();
        fdlInputField.top = new FormAttachment(lastControl, margin);
        fdlInputField.left = new FormAttachment(0, 0);
        fdlInputField.right = new FormAttachment(middle, -margin);
        wlInputField.setLayoutData(fdlInputField);
        m_wInputField = new CCombo(wGeneralTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        m_wInputField.setToolTipText(BaseMessages
                .getString(MQTTPublisherMeta.PKG,
                        "MQTTClientDialog.FieldName.Tooltip"));
        m_wInputField.setItems(previousFields.getFieldNames());
        props.setLook(m_wInputField);
        m_wInputField.addModifyListener(lsMod);
        FormData fdFilename = new FormData();
        fdFilename.top = new FormAttachment(lastControl, margin);
        fdFilename.left = new FormAttachment(middle, 0);
        fdFilename.right = new FormAttachment(100, 0);
        m_wInputField.setLayoutData(fdFilename);
        lastControl = m_wInputField;

        // Client ID
        Label wlClientID = new Label(wGeneralTabComp, SWT.RIGHT);
        wlClientID.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "MQTTClientDialog.ClientID.Label"));
        props.setLook(wlClientID);
        FormData fdlClientID = new FormData();
        fdlClientID.top = new FormAttachment(lastControl, margin);
        fdlClientID.left = new FormAttachment(0, 0);
        fdlClientID.right = new FormAttachment(middle, -margin);
        wlClientID.setLayoutData(fdlClientID);
        m_wClientID = new TextVar(variables, wGeneralTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_wClientID);
        m_wClientID.addModifyListener(lsMod);
        FormData fdClientID = new FormData();
        fdClientID.top = new FormAttachment(lastControl, margin);
        fdClientID.left = new FormAttachment(middle, 0);
        fdClientID.right = new FormAttachment(100, 0);
        m_wClientID.setLayoutData(fdClientID);
        lastControl = m_wClientID;

        // Connection timeout
        Label wlConnectionTimeout = new Label(wGeneralTabComp, SWT.RIGHT);
        wlConnectionTimeout.setText(BaseMessages
                .getString(MQTTPublisherMeta.PKG,
                        "MQTTClientDialog.ConnectionTimeout.Label"));
        wlConnectionTimeout.setToolTipText(
                BaseMessages.getString(MQTTPublisherMeta.PKG, "MQTTClientDialog.ConnectionTimeout.ToolTip"));
        props.setLook(wlConnectionTimeout);
        FormData fdlConnectionTimeout = new FormData();
        fdlConnectionTimeout.top = new FormAttachment(lastControl, margin);
        fdlConnectionTimeout.left = new FormAttachment(0, 0);
        fdlConnectionTimeout.right = new FormAttachment(middle, -margin);
        wlConnectionTimeout.setLayoutData(fdlConnectionTimeout);
        m_wTimeout = new TextVar(variables, wGeneralTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_wTimeout);
        m_wTimeout.addModifyListener(lsMod);
        FormData fdConnectionTimeout = new FormData();
        fdConnectionTimeout.top = new FormAttachment(lastControl, margin);
        fdConnectionTimeout.left = new FormAttachment(middle, 0);
        fdConnectionTimeout.right = new FormAttachment(100, 0);
        m_wTimeout.setLayoutData(fdConnectionTimeout);
        lastControl = m_wTimeout;

        // QOS
        Label wlQOS = new Label(wGeneralTabComp, SWT.RIGHT);
        wlQOS.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "MQTTClientDialog.QOS.Label"));
        props.setLook(wlQOS);
        FormData fdlQOS = new FormData();
        fdlQOS.top = new FormAttachment(lastControl, margin);
        fdlQOS.left = new FormAttachment(0, 0);
        fdlQOS.right = new FormAttachment(middle, -margin);
        wlQOS.setLayoutData(fdlQOS);
        m_wQOS = new TextVar(variables, wGeneralTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_wQOS);
        m_wQOS.addModifyListener(lsMod);
        FormData fdQOS = new FormData();
        fdQOS.top = new FormAttachment(lastControl, margin);
        fdQOS.left = new FormAttachment(middle, 0);
        fdQOS.right = new FormAttachment(100, 0);
        m_wQOS.setLayoutData(fdQOS);
        lastControl = m_wQOS;

        FormData fdGeneralTabComp = new FormData();
        fdGeneralTabComp.left = new FormAttachment(0, 0);
        fdGeneralTabComp.top = new FormAttachment(0, 0);
        fdGeneralTabComp.right = new FormAttachment(100, 0);
        fdGeneralTabComp.bottom = new FormAttachment(100, 0);
        wGeneralTabComp.setLayoutData(fdGeneralTabComp);

        wGeneralTabComp.layout();
        m_wGeneralTab.setControl(wGeneralTabComp);

        // ====================
        // CREDENTIALS TAB
        // ====================
        m_wCredentialsTab = new CTabItem(m_wTabFolder, SWT.NONE);
        m_wCredentialsTab.setText(BaseMessages
                .getString(MQTTPublisherMeta.PKG,
                        "MQTTClientDialog.CredentialsTab.Title"));

        Composite wCredentialsComp = new Composite(m_wTabFolder, SWT.NONE);
        props.setLook(wCredentialsComp);

        FormLayout fieldsCompLayout = new FormLayout();
        fieldsCompLayout.marginWidth = Const.FORM_MARGIN;
        fieldsCompLayout.marginHeight = Const.FORM_MARGIN;
        wCredentialsComp.setLayout(fieldsCompLayout);

        Label wlRequiresAuth = new Label(wCredentialsComp, SWT.RIGHT);
        wlRequiresAuth.setText(BaseMessages
                .getString(MQTTPublisherMeta.PKG,
                        "MQTTClientDialog.RequireAuth.Label"));
        props.setLook(wlRequiresAuth);
        FormData fdlRequriesAuth = new FormData();
        fdlRequriesAuth.left = new FormAttachment(0, 0);
        fdlRequriesAuth.top = new FormAttachment(0, margin * 2);
        fdlRequriesAuth.right = new FormAttachment(middle, -margin);
        wlRequiresAuth.setLayoutData(fdlRequriesAuth);
        m_wRequiresAuth = new Button(wCredentialsComp, SWT.CHECK);
        props.setLook(m_wRequiresAuth);
        FormData fdRequiresAuth = new FormData();
        fdRequiresAuth.left = new FormAttachment(middle, 0);
        fdRequiresAuth.top = new FormAttachment(0, margin * 2);
        fdRequiresAuth.right = new FormAttachment(100, 0);
        m_wRequiresAuth.setLayoutData(fdRequiresAuth);

        m_wRequiresAuth.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent arg0) {
                boolean enabled = m_wRequiresAuth.getSelection();
                m_wlUsername.setEnabled(enabled);
                m_wUsername.setEnabled(enabled);
                m_wlPassword.setEnabled(enabled);
                m_wPassword.setEnabled(enabled);
            }
        });
        lastControl = m_wRequiresAuth;

        // Username field
        m_wlUsername = new Label(wCredentialsComp, SWT.RIGHT);
        m_wlUsername.setEnabled(false);
        m_wlUsername.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "MQTTClientDialog.Username.Label"));
        props.setLook(m_wlUsername);
        FormData fdlUsername = new FormData();
        fdlUsername.left = new FormAttachment(0, -margin);
        fdlUsername.right = new FormAttachment(middle, -2 * margin);
        fdlUsername.top = new FormAttachment(lastControl, 2 * margin);
        m_wlUsername.setLayoutData(fdlUsername);

        m_wUsername = new TextVar(variables, wCredentialsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        m_wUsername.setEnabled(false);
        m_wUsername.setToolTipText(BaseMessages
                .getString(MQTTPublisherMeta.PKG,
                        "MQTTClientDialog.Username.Tooltip"));
        props.setLook(m_wUsername);
        m_wUsername.addModifyListener(lsMod);
        FormData fdResult = new FormData();
        fdResult.left = new FormAttachment(middle, -margin);
        fdResult.top = new FormAttachment(lastControl, 2 * margin);
        fdResult.right = new FormAttachment(100, 0);
        m_wUsername.setLayoutData(fdResult);
        lastControl = m_wUsername;

        // Password field
        m_wlPassword = new Label(wCredentialsComp, SWT.RIGHT);
        m_wlPassword.setEnabled(false);
        m_wlPassword.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "MQTTClientDialog.Password.Label"));
        props.setLook(m_wlPassword);
        FormData fdlPassword = new FormData();
        fdlPassword.left = new FormAttachment(0, -margin);
        fdlPassword.right = new FormAttachment(middle, -2 * margin);
        fdlPassword.top = new FormAttachment(lastControl, margin);
        m_wlPassword.setLayoutData(fdlPassword);

        m_wPassword = new TextVar(variables, wCredentialsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
        m_wPassword.setEnabled(false);
        m_wPassword.setToolTipText(BaseMessages
                .getString(MQTTPublisherMeta.PKG,
                        "MQTTClientDialog.Password.Tooltip"));
        props.setLook(m_wPassword);
        m_wPassword.addModifyListener(lsMod);
        FormData fdPassword = new FormData();
        fdPassword.left = new FormAttachment(middle, -margin);
        fdPassword.top = new FormAttachment(lastControl, margin);
        fdPassword.right = new FormAttachment(100, 0);
        m_wPassword.setLayoutData(fdPassword);

        FormData fdCredentialsComp = new FormData();
        fdCredentialsComp.left = new FormAttachment(0, 0);
        fdCredentialsComp.top = new FormAttachment(0, 0);
        fdCredentialsComp.right = new FormAttachment(100, 0);
        fdCredentialsComp.bottom = new FormAttachment(100, 0);
        wCredentialsComp.setLayoutData(fdCredentialsComp);

        wCredentialsComp.layout();
        m_wCredentialsTab.setControl(wCredentialsComp);

        // ====================
        // SSL TAB
        // ====================
        m_wSSLTab = new CTabItem(m_wTabFolder, SWT.NONE);
        m_wSSLTab.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "MQTTClientDialog.SSLTab.Label"));

        Composite wSSLComp = new Composite(m_wTabFolder, SWT.NONE);
        props.setLook(wSSLComp);

        FormLayout sslCompLayout = new FormLayout();
        sslCompLayout.marginWidth = Const.FORM_MARGIN;
        sslCompLayout.marginHeight = Const.FORM_MARGIN;
        wSSLComp.setLayout(sslCompLayout);

        // Server CA file path
        Label wlCAFile = new Label(wSSLComp, SWT.RIGHT);
        wlCAFile.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "MQTTClientDialog.CAFile.Label"));
        props.setLook(wlCAFile);
        FormData fdlCAFile = new FormData();
        fdlCAFile.left = new FormAttachment(0, -margin);
        fdlCAFile.right = new FormAttachment(middle, -2 * margin);
        fdlCAFile.top = new FormAttachment(0, 2 * margin);
        wlCAFile.setLayoutData(fdlCAFile);

        m_wCAFile = new TextVar(variables, wSSLComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        m_wCAFile.setToolTipText(BaseMessages
                .getString(MQTTPublisherMeta.PKG,
                        "MQTTClientDialog.CAFile.Tooltip"));
        props.setLook(m_wCAFile);
        m_wCAFile.addModifyListener(lsMod);
        FormData fdCAFile = new FormData();
        fdCAFile.left = new FormAttachment(middle, -margin);
        fdCAFile.top = new FormAttachment(0, 2 * margin);
        fdCAFile.right = new FormAttachment(100, 0);
        m_wCAFile.setLayoutData(fdCAFile);
        lastControl = m_wCAFile;

        // Client certificate file path
        Label wlCertFile = new Label(wSSLComp, SWT.RIGHT);
        wlCertFile.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "MQTTClientDialog.CertFile.Label"));
        props.setLook(wlCertFile);
        FormData fdlCertFile = new FormData();
        fdlCertFile.left = new FormAttachment(0, -margin);
        fdlCertFile.right = new FormAttachment(middle, -2 * margin);
        fdlCertFile.top = new FormAttachment(lastControl, margin);
        wlCertFile.setLayoutData(fdlCertFile);

        m_wCertFile = new TextVar(variables, wSSLComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        m_wCertFile.setToolTipText(BaseMessages
                .getString(MQTTPublisherMeta.PKG,
                        "MQTTClientDialog.CertFile.Tooltip"));
        props.setLook(m_wCertFile);
        m_wCertFile.addModifyListener(lsMod);
        FormData fdCertFile = new FormData();
        fdCertFile.left = new FormAttachment(middle, -margin);
        fdCertFile.top = new FormAttachment(lastControl, margin);
        fdCertFile.right = new FormAttachment(100, 0);
        m_wCertFile.setLayoutData(fdCertFile);
        lastControl = m_wCertFile;

        // Client key file path
        Label wlKeyFile = new Label(wSSLComp, SWT.RIGHT);
        wlKeyFile.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "MQTTClientDialog.KeyFile.Label"));
        props.setLook(wlKeyFile);
        FormData fdlKeyFile = new FormData();
        fdlKeyFile.left = new FormAttachment(0, -margin);
        fdlKeyFile.right = new FormAttachment(middle, -2 * margin);
        fdlKeyFile.top = new FormAttachment(lastControl, margin);
        wlKeyFile.setLayoutData(fdlKeyFile);

        m_wKeyFile = new TextVar(variables, wSSLComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        m_wKeyFile.setToolTipText(BaseMessages
                .getString(MQTTPublisherMeta.PKG,
                        "MQTTClientDialog.KeyFile.Tooltip"));
        props.setLook(m_wKeyFile);
        m_wKeyFile.addModifyListener(lsMod);
        FormData fdKeyFile = new FormData();
        fdKeyFile.left = new FormAttachment(middle, -margin);
        fdKeyFile.top = new FormAttachment(lastControl, margin);
        fdKeyFile.right = new FormAttachment(100, 0);
        m_wKeyFile.setLayoutData(fdKeyFile);
        lastControl = m_wKeyFile;

        // Client key file password path
        Label wlKeyPassword = new Label(wSSLComp, SWT.RIGHT);
        wlKeyPassword.setText(BaseMessages
                .getString(MQTTPublisherMeta.PKG,
                        "MQTTClientDialog.KeyPassword.Label"));
        props.setLook(wlKeyPassword);
        FormData fdlKeyPassword = new FormData();
        fdlKeyPassword.left = new FormAttachment(0, -margin);
        fdlKeyPassword.right = new FormAttachment(middle, -2 * margin);
        fdlKeyPassword.top = new FormAttachment(lastControl, margin);
        wlKeyPassword.setLayoutData(fdlKeyPassword);

        m_wKeyPassword = new TextVar(variables, wSSLComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD);
        m_wKeyPassword.setToolTipText(BaseMessages
                .getString(MQTTPublisherMeta.PKG,
                        "MQTTClientDialog.KeyPassword.Tooltip"));
        props.setLook(m_wKeyPassword);
        m_wKeyPassword.addModifyListener(lsMod);
        FormData fdKeyPassword = new FormData();
        fdKeyPassword.left = new FormAttachment(middle, -margin);
        fdKeyPassword.top = new FormAttachment(lastControl, margin);
        fdKeyPassword.right = new FormAttachment(100, 0);
        m_wKeyPassword.setLayoutData(fdKeyPassword);
        lastControl = m_wKeyPassword;

        FormData fdSSLComp = new FormData();
        fdSSLComp.left = new FormAttachment(0, 0);
        fdSSLComp.top = new FormAttachment(0, 0);
        fdSSLComp.right = new FormAttachment(100, 0);
        fdSSLComp.bottom = new FormAttachment(100, 0);
        wSSLComp.setLayoutData(fdSSLComp);

        wSSLComp.layout();
        m_wSSLTab.setControl(wSSLComp);

        // ====================
        // BUTTONS
        // ====================
        wOk = new Button(shell, SWT.PUSH);
        wOk.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "System.Button.OK"));
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(MQTTPublisherMeta.PKG,
                "System.Button.Cancel"));

        setButtonPositions(new Button[]{wOk, wCancel}, margin, null);

        // ====================
        // END OF TAB FOLDER
        // ====================
        FormData fdTabFolder = new FormData();
        fdTabFolder.left = new FormAttachment(0, 0);
        fdTabFolder.top = new FormAttachment(wTransformName, margin);
        fdTabFolder.right = new FormAttachment(100, 0);
        fdTabFolder.bottom = new FormAttachment(wOk, -margin);
        m_wTabFolder.setLayoutData(fdTabFolder);

        // Add listeners
        lsCancel = new Listener() {
            public void handleEvent(Event e) {
                cancel();
            }
        };
        lsOk = new Listener() {
            public void handleEvent(Event e) {
                ok();
            }
        };
        wCancel.addListener(SWT.Selection, lsCancel);
        wOk.addListener(SWT.Selection, lsOk);

        lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };
        wTransformName.addSelectionListener(lsDef);
        m_wTopicName.addSelectionListener(lsDef);
        m_wInputField.addSelectionListener(lsDef);

        m_wTabFolder.setSelection(0);

        // Detect X or ALT-F4 or something that kills this window...
        shell.addShellListener(new ShellAdapter() {
            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });

        // Set the shell size, based upon previous time...
        setSize(shell, 440, 350, true);

        getData(producerMeta, true);
        producerMeta.setChanged(changed);

        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        return transformName;
    }

    private IRowMeta getPreviousFields() {
        IRowMeta previousFields = null;

        try {
            previousFields = pipelineMeta.getPrevTransformFields(variables, transformName);
        } catch (HopTransformException e) {
            new ErrorDialog(shell, BaseMessages
                    .getString(MQTTPublisherMeta.PKG,
                            "System.Dialog.Error.Title"), BaseMessages
                    .getString(MQTTPublisherMeta.PKG,
                            "MQTTClientDialog.ErrorDialog.UnableToGetInputFields.Message"), e);
        }

        return previousFields;
    }

    private void updateTopicCombo(IRowMeta previousFields) {
        if (m_wTopicFromIncomingField.getSelection()) {
            m_wlTopicName.setText(BaseMessages.getString(MQTTPublisherMeta.PKG, "MQTTClientDialog.TopicNameFromField"));
        } else {
            m_wlTopicName.setText(BaseMessages
                    .getString(MQTTPublisherMeta.PKG,
                            "MQTTClientDialog.TopicName.Label"));
        }

        String current = m_wTopicName.getText();
        m_wTopicName.removeAll();
        if (m_wTopicFromIncomingField.getSelection()) {
            m_wTopicName.setItems(previousFields.getFieldNames());
        }

        if (!Utils.isEmpty(current)) {
            m_wTopicName.setText(current);
        }
    }

    /**
     * Copy information from the meta-data input to the dialog fields.
     */
    private void getData(MQTTPublisherMeta producerMeta, boolean copyStepname) {
        if (copyStepname) {
            wTransformName.setText(transformName);
        }
        m_wBroker.setText(Const.NVL(producerMeta.getBroker(), ""));
        m_wTopicName.setText(Const.NVL(producerMeta.getTopic(), ""));
        m_wTopicFromIncomingField.setSelection(producerMeta.getTopicIsFromField());
        m_wInputField.setText(Const.NVL(producerMeta.getField(), ""));
        m_wClientID.setText(Const.NVL(producerMeta.getClientId(), ""));
        m_wTimeout.setText(Const.NVL(producerMeta.getTimeout(), "10000"));
        m_wQOS.setText(Const.NVL(producerMeta.getQoS(), "0"));

        m_wRequiresAuth.setSelection(producerMeta.isRequiresAuth());
        m_wRequiresAuth.notifyListeners(SWT.Selection, new Event());

        m_wUsername.setText(Const.NVL(producerMeta.getUsername(), ""));
        m_wPassword.setText(Const.NVL(producerMeta.getPassword(), ""));

        m_wCAFile.setText(Const.NVL(producerMeta.getSSLCaFile(), ""));
        m_wCertFile.setText(Const.NVL(producerMeta.getSSLCertFile(), ""));
        m_wKeyFile.setText(Const.NVL(producerMeta.getSSLKeyFile(), ""));
        m_wKeyPassword.setText(Const.NVL(producerMeta.getSSLKeyFilePass(), ""));

        updateTopicCombo(getPreviousFields());

        wTransformName.selectAll();
    }

    private void cancel() {
        transformName = null;
        producerMeta.setChanged(changed);
        dispose();
    }

    /**
     * Copy information from the dialog fields to the meta-data input
     */
    private void setData(MQTTPublisherMeta producerMeta) {
        producerMeta.setBroker(m_wBroker.getText());
        producerMeta.setTopic(m_wTopicName.getText());
        producerMeta.setTopicIsFromField(m_wTopicFromIncomingField.getSelection());
        producerMeta.setField(m_wInputField.getText());
        producerMeta.setClientId(m_wClientID.getText());
        producerMeta.setTimeout(m_wTimeout.getText());
        producerMeta.setQoS(m_wQOS.getText());

        boolean requiresAuth = m_wRequiresAuth.getSelection();
        producerMeta.setRequiresAuth(requiresAuth);
        if (requiresAuth) {
            producerMeta.setUsername(m_wUsername.getText());
            producerMeta.setPassword(m_wPassword.getText());
        }

        producerMeta.setSSLCaFile(m_wCAFile.getText());
        producerMeta.setSSLCertFile(m_wCertFile.getText());
        producerMeta.setSSLKeyFile(m_wKeyFile.getText());
        producerMeta.setSSLKeyFilePass(m_wKeyPassword.getText());

        producerMeta.setChanged();
    }

    private void ok() {
        if (Utils.isEmpty(wTransformName.getText())) {
            return;
        }
        setData(producerMeta);
        transformName = wTransformName.getText();
        dispose();
    }
}
