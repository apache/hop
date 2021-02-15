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
package org.apache.hop.pipeline.transforms.ldapoutput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.ldapinput.LdapConnection;
import org.apache.hop.pipeline.transforms.ldapinput.LdapProtocol;
import org.apache.hop.pipeline.transforms.ldapinput.LdapProtocolFactory;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.window.Window;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LdapOutputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = LdapOutputMeta.class; // For Translator

  private Button wUsingAuthentication;

  private final LdapOutputMeta input;

  private TextVar wHost;

  private Label wlUserName;
  private TextVar wUserName;

  private Label wlPassword;
  private TextVar wPassword;

  private TextVar wPort;

  private ComboVar wDnField;
  private Label wlDnField;

  private ComboVar wOldDnField;
  private Label wlOldDnField;

  private ComboVar wNewDnField;
  private Label wlNewDnField;

  private Label wlFailIfNotExist;
  private Button wFailIfNotExist;
  private Label wlDeleteRDN;
  private Button wDeleteRDN;

  private final Map<String, Integer> inputFields;

  private ColumnInfo[] ciReturn;

  private Button wDoMapping;

  private Label wlReturn;
  private TableView wReturn;

  private Button wGetLU;

  private CCombo wOperation;

  private CCombo wReferral;

  private CCombo wDerefAliases;

  private Label wlMultiValuedSeparator;
  private TextVar wMultiValuedSeparator;

  private Label wlBaseDN;
  private TextVar wBaseDN;

  private ComboVar wProtocol;

  private Label wlTrustStorePath;
  private TextVar wTrustStorePath;

  private Label wlTrustStorePassword;
  private TextVar wTrustStorePassword;

  private Label wlSetTrustStore;
  private Button wSetTrustStore;

  private Label wlTrustAll;
  private Button wTrustAll;

  /** List of ColumnInfo that should have the field names of the selected base dn */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  private boolean gotPrevious = false;

  private Button wbbFilename;

  public static final int[] dateLengths = new int[] {23, 19, 14, 10, 10, 10, 10, 8, 8, 8, 8, 6, 6};

  public LdapOutputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (LdapOutputMeta) in;
    inputFields = new HashMap<>();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "LdapOutputDialog.DialogTitle"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Buttons go at the bottom
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText(BaseMessages.getString(PKG, "LdapOutputDialog.General.Tab"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wGeneralComp.setLayout(fileLayout);

    // /////////////////////////////////
    // START OF Connection GROUP
    // /////////////////////////////////

    Group wConnectionGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wConnectionGroup);
    wConnectionGroup.setText(
        BaseMessages.getString(PKG, "LdapOutputDialog.Group.ConnectionGroup.Label"));

    FormLayout connectiongroupLayout = new FormLayout();
    connectiongroupLayout.marginWidth = 10;
    connectiongroupLayout.marginHeight = 10;
    wConnectionGroup.setLayout(connectiongroupLayout);

    // Host line
    Label wlHost = new Label(wConnectionGroup, SWT.RIGHT);
    wlHost.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Host.Label"));
    props.setLook(wlHost);
    FormData fdlHost = new FormData();
    fdlHost.left = new FormAttachment(0, 0);
    fdlHost.top = new FormAttachment(wTransformName, margin);
    fdlHost.right = new FormAttachment(middle, -margin);
    wlHost.setLayoutData(fdlHost);
    wHost = new TextVar(variables, wConnectionGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHost.setToolTipText(BaseMessages.getString(PKG, "LdapOutputDialog.Host.Tooltip"));
    props.setLook(wHost);
    wHost.addModifyListener(lsMod);
    FormData fdHost = new FormData();
    fdHost.left = new FormAttachment(middle, 0);
    fdHost.top = new FormAttachment(wTransformName, margin);
    fdHost.right = new FormAttachment(100, 0);
    wHost.setLayoutData(fdHost);

    // Port line
    Label wlPort = new Label(wConnectionGroup, SWT.RIGHT);
    wlPort.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Port.Label"));
    props.setLook(wlPort);
    FormData fdlPort = new FormData();
    fdlPort.left = new FormAttachment(0, 0);
    fdlPort.top = new FormAttachment(wHost, margin);
    fdlPort.right = new FormAttachment(middle, -margin);
    wlPort.setLayoutData(fdlPort);
    wPort = new TextVar(variables, wConnectionGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPort);
    wPort.setToolTipText(BaseMessages.getString(PKG, "LdapOutputDialog.Port.Tooltip"));
    wPort.addModifyListener(lsMod);
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment(middle, 0);
    fdPort.top = new FormAttachment(wHost, margin);
    fdPort.right = new FormAttachment(100, 0);
    wPort.setLayoutData(fdPort);

    // Referral
    Label wlReferral = new Label(wConnectionGroup, SWT.RIGHT);
    wlReferral.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Referral.Label"));
    props.setLook(wlReferral);
    FormData fdlReferral = new FormData();
    fdlReferral.left = new FormAttachment(0, 0);
    fdlReferral.right = new FormAttachment(middle, -margin);
    fdlReferral.top = new FormAttachment(wPort, margin);
    wlReferral.setLayoutData(fdlReferral);

    wReferral = new CCombo(wConnectionGroup, SWT.BORDER | SWT.READ_ONLY);
    props.setLook(wReferral);
    wReferral.addModifyListener(lsMod);
    FormData fdReferral = new FormData();
    fdReferral.left = new FormAttachment(middle, 0);
    fdReferral.top = new FormAttachment(wPort, margin);
    fdReferral.right = new FormAttachment(100, -margin);
    wReferral.setLayoutData(fdReferral);
    wReferral.setItems(LdapOutputMeta.referralTypeDesc);
    wReferral.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // DerefAliases
    Label wlDerefAliases = new Label(wConnectionGroup, SWT.RIGHT);
    wlDerefAliases.setText(BaseMessages.getString(PKG, "LdapOutputDialog.DerefAliases.Label"));
    props.setLook(wlDerefAliases);
    FormData fdlDerefAliases = new FormData();
    fdlDerefAliases.left = new FormAttachment(0, 0);
    fdlDerefAliases.right = new FormAttachment(middle, -margin);
    fdlDerefAliases.top = new FormAttachment(wReferral, margin);
    wlDerefAliases.setLayoutData(fdlDerefAliases);

    wDerefAliases = new CCombo(wConnectionGroup, SWT.BORDER | SWT.READ_ONLY);
    props.setLook(wDerefAliases);
    wDerefAliases.addModifyListener(lsMod);
    FormData fdDerefAliases = new FormData();
    fdDerefAliases.left = new FormAttachment(middle, 0);
    fdDerefAliases.top = new FormAttachment(wReferral, margin);
    fdDerefAliases.right = new FormAttachment(100, -margin);
    wDerefAliases.setLayoutData(fdDerefAliases);
    wDerefAliases.setItems(LdapOutputMeta.derefAliasesTypeDesc);
    wDerefAliases.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Protocol Line
    Label wlProtocol = new Label(wConnectionGroup, SWT.RIGHT);
    wlProtocol.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Protocol.Label"));
    props.setLook(wlProtocol);
    FormData fdlProtocol = new FormData();
    fdlProtocol.left = new FormAttachment(0, 0);
    fdlProtocol.right = new FormAttachment(middle, -margin);
    fdlProtocol.top = new FormAttachment(wDerefAliases, margin);
    wlProtocol.setLayoutData(fdlProtocol);

    wProtocol = new ComboVar(variables, wConnectionGroup, SWT.BORDER | SWT.READ_ONLY);
    wProtocol.setEditable(true);
    props.setLook(wProtocol);
    wProtocol.addModifyListener(lsMod);
    FormData fdProtocol = new FormData();
    fdProtocol.left = new FormAttachment(middle, 0);
    fdProtocol.top = new FormAttachment(wDerefAliases, margin);
    fdProtocol.right = new FormAttachment(100, -margin);
    wProtocol.setLayoutData(fdProtocol);
    wProtocol.setItems(LdapProtocolFactory.getConnectionTypes(log).toArray(new String[] {}));
    wProtocol.addSelectionListener(
        new SelectionAdapter() {

          public void widgetSelected(SelectionEvent e) {
            setProtocol();
          }
        });

    FormData fdConnectionGroup = new FormData();
    fdConnectionGroup.left = new FormAttachment(0, margin);
    fdConnectionGroup.top = new FormAttachment(0, margin);
    fdConnectionGroup.right = new FormAttachment(100, -margin);
    wConnectionGroup.setLayoutData(fdConnectionGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF CONNECTION GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Authentication GROUP
    // /////////////////////////////////

    Group wAuthenticationGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wAuthenticationGroup);
    wAuthenticationGroup.setText(
        BaseMessages.getString(PKG, "LdapOutputDialog.Group.AuthenticationGroup.Label"));

    FormLayout AuthenticationGroupLayout = new FormLayout();
    AuthenticationGroupLayout.marginWidth = 10;
    AuthenticationGroupLayout.marginHeight = 10;
    wAuthenticationGroup.setLayout(AuthenticationGroupLayout);

    // using authentication ?
    Label wlUsingAuthentication = new Label(wAuthenticationGroup, SWT.RIGHT);
    wlUsingAuthentication.setText(
        BaseMessages.getString(PKG, "LdapOutputDialog.usingAuthentication.Label"));
    props.setLook(wlUsingAuthentication);
    FormData fdlUsingAuthentication = new FormData();
    fdlUsingAuthentication.left = new FormAttachment(0, 0);
    fdlUsingAuthentication.top = new FormAttachment(wConnectionGroup, margin);
    fdlUsingAuthentication.right = new FormAttachment(middle, -margin);
    wlUsingAuthentication.setLayoutData(fdlUsingAuthentication);
    wUsingAuthentication = new Button(wAuthenticationGroup, SWT.CHECK);
    props.setLook(wUsingAuthentication);
    wUsingAuthentication.setToolTipText(
        BaseMessages.getString(PKG, "LdapOutputDialog.usingAuthentication.Tooltip"));
    FormData fdUsingAuthentication = new FormData();
    fdUsingAuthentication.left = new FormAttachment(middle, 0);
    fdUsingAuthentication.top = new FormAttachment(wlUsingAuthentication, 0, SWT.CENTER);
    wUsingAuthentication.setLayoutData(fdUsingAuthentication);

    wUsingAuthentication.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            useAuthentication();
            input.setChanged();
          }
        });

    // UserName line
    wlUserName = new Label(wAuthenticationGroup, SWT.RIGHT);
    wlUserName.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Username.Label"));
    props.setLook(wlUserName);
    FormData fdlUserName = new FormData();
    fdlUserName.left = new FormAttachment(0, 0);
    fdlUserName.top = new FormAttachment(wUsingAuthentication, margin);
    fdlUserName.right = new FormAttachment(middle, -margin);
    wlUserName.setLayoutData(fdlUserName);
    wUserName = new TextVar(variables, wAuthenticationGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wUserName);
    wUserName.setToolTipText(BaseMessages.getString(PKG, "LdapOutputDialog.Username.Tooltip"));
    wUserName.addModifyListener(lsMod);
    FormData fdUserName = new FormData();
    fdUserName.left = new FormAttachment(middle, 0);
    fdUserName.top = new FormAttachment(wUsingAuthentication, margin);
    fdUserName.right = new FormAttachment(100, 0);
    wUserName.setLayoutData(fdUserName);

    // Password line
    wlPassword = new Label(wAuthenticationGroup, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Password.Label"));
    props.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.top = new FormAttachment(wUserName, margin);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword =
        new PasswordTextVar(variables, wAuthenticationGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wPassword.setToolTipText(BaseMessages.getString(PKG, "LdapOutputDialog.Password.Tooltip"));
    props.setLook(wPassword);
    wPassword.addModifyListener(lsMod);
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.top = new FormAttachment(wUserName, margin);
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);

    FormData fdAuthenticationGroup = new FormData();
    fdAuthenticationGroup.left = new FormAttachment(0, margin);
    fdAuthenticationGroup.top = new FormAttachment(wConnectionGroup, margin);
    fdAuthenticationGroup.right = new FormAttachment(100, -margin);
    wAuthenticationGroup.setLayoutData(fdAuthenticationGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Authentication GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Certificate GROUP
    // /////////////////////////////////

    Group wCertificateGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wCertificateGroup);
    wCertificateGroup.setText(
        BaseMessages.getString(PKG, "LdapOutputDialog.Group.CertificateGroup.Label"));

    FormLayout certificateGroupLayout = new FormLayout();
    certificateGroupLayout.marginWidth = 10;
    certificateGroupLayout.marginHeight = 10;
    wCertificateGroup.setLayout(certificateGroupLayout);

    // set TrustStore?
    wlSetTrustStore = new Label(wCertificateGroup, SWT.RIGHT);
    wlSetTrustStore.setText(BaseMessages.getString(PKG, "LdapOutputDialog.setTrustStore.Label"));
    props.setLook(wlSetTrustStore);
    FormData fdlsetTrustStore = new FormData();
    fdlsetTrustStore.left = new FormAttachment(0, 0);
    fdlsetTrustStore.top = new FormAttachment(wAuthenticationGroup, margin);
    fdlsetTrustStore.right = new FormAttachment(middle, -margin);
    wlSetTrustStore.setLayoutData(fdlsetTrustStore);
    wSetTrustStore = new Button(wCertificateGroup, SWT.CHECK);
    props.setLook(wSetTrustStore);
    wSetTrustStore.setToolTipText(
        BaseMessages.getString(PKG, "LdapOutputDialog.setTrustStore.Tooltip"));
    FormData fdsetTrustStore = new FormData();
    fdsetTrustStore.left = new FormAttachment(middle, 0);
    fdsetTrustStore.top = new FormAttachment(wlSetTrustStore, 0, SWT.CENTER);
    wSetTrustStore.setLayoutData(fdsetTrustStore);

    wSetTrustStore.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            setTrustStore();
          }
        });

    // TrustStorePath line
    wlTrustStorePath = new Label(wCertificateGroup, SWT.RIGHT);
    wlTrustStorePath.setText(BaseMessages.getString(PKG, "LdapOutputDialog.TrustStorePath.Label"));
    props.setLook(wlTrustStorePath);
    FormData fdlTrustStorePath = new FormData();
    fdlTrustStorePath.left = new FormAttachment(0, -margin);
    fdlTrustStorePath.top = new FormAttachment(wSetTrustStore, margin);
    fdlTrustStorePath.right = new FormAttachment(middle, -margin);
    wlTrustStorePath.setLayoutData(fdlTrustStorePath);

    wbbFilename = new Button(wCertificateGroup, SWT.PUSH | SWT.CENTER);
    props.setLook(wbbFilename);
    wbbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbbFilename.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wSetTrustStore, margin);
    wbbFilename.setLayoutData(fdbFilename);
    // Listen to the Browse... button

    wbbFilename.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wTrustStorePath, variables));

    wTrustStorePath = new TextVar(variables, wCertificateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTrustStorePath);
    wTrustStorePath.setToolTipText(
        BaseMessages.getString(PKG, "LdapOutputDialog.TrustStorePath.Tooltip"));
    wTrustStorePath.addModifyListener(lsMod);
    FormData fdTrustStorePath = new FormData();
    fdTrustStorePath.left = new FormAttachment(middle, 0);
    fdTrustStorePath.top = new FormAttachment(wSetTrustStore, margin);
    fdTrustStorePath.right = new FormAttachment(wbbFilename, -margin);
    wTrustStorePath.setLayoutData(fdTrustStorePath);

    // TrustStorePassword line
    wlTrustStorePassword = new Label(wCertificateGroup, SWT.RIGHT);
    wlTrustStorePassword.setText(
        BaseMessages.getString(PKG, "LdapOutputDialog.TrustStorePassword.Label"));
    props.setLook(wlTrustStorePassword);
    FormData fdlTrustStorePassword = new FormData();
    fdlTrustStorePassword.left = new FormAttachment(0, -margin);
    fdlTrustStorePassword.top = new FormAttachment(wTrustStorePath, margin);
    fdlTrustStorePassword.right = new FormAttachment(middle, -margin);
    wlTrustStorePassword.setLayoutData(fdlTrustStorePassword);
    wTrustStorePassword =
        new PasswordTextVar(variables, wCertificateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTrustStorePassword);
    wTrustStorePassword.setToolTipText(
        BaseMessages.getString(PKG, "LdapOutputDialog.TrustStorePassword.Tooltip"));
    wTrustStorePassword.addModifyListener(lsMod);
    FormData fdTrustStorePassword = new FormData();
    fdTrustStorePassword.left = new FormAttachment(middle, 0);
    fdTrustStorePassword.top = new FormAttachment(wTrustStorePath, margin);
    fdTrustStorePassword.right = new FormAttachment(100, -margin);
    wTrustStorePassword.setLayoutData(fdTrustStorePassword);

    // Trust all certificate?
    wlTrustAll = new Label(wCertificateGroup, SWT.RIGHT);
    wlTrustAll.setText(BaseMessages.getString(PKG, "LdapOutputDialog.TrustAll.Label"));
    props.setLook(wlTrustAll);
    FormData fdlTrustAll = new FormData();
    fdlTrustAll.left = new FormAttachment(0, 0);
    fdlTrustAll.top = new FormAttachment(wTrustStorePassword, margin);
    fdlTrustAll.right = new FormAttachment(middle, -margin);
    wlTrustAll.setLayoutData(fdlTrustAll);
    wTrustAll = new Button(wCertificateGroup, SWT.CHECK);
    props.setLook(wTrustAll);
    wTrustAll.setToolTipText(BaseMessages.getString(PKG, "LdapOutputDialog.TrustAll.Tooltip"));
    FormData fdTrustAll = new FormData();
    fdTrustAll.left = new FormAttachment(middle, 0);
    fdTrustAll.top = new FormAttachment(wlTrustAll, 0, SWT.CENTER);
    wTrustAll.setLayoutData(fdTrustAll);
    wTrustAll.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            trustAll();
          }
        });

    FormData fdCertificateGroup = new FormData();
    fdCertificateGroup.left = new FormAttachment(0, margin);
    fdCertificateGroup.top = new FormAttachment(wAuthenticationGroup, margin);
    fdCertificateGroup.right = new FormAttachment(100, -margin);
    wCertificateGroup.setLayoutData(fdCertificateGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Certificate GROUP
    // ///////////////////////////////////////////////////////////

    // Test LDAP connection button
    Button wTest = new Button(wGeneralComp, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "LdapOutputDialog.TestConnection.Label"));
    props.setLook(wTest);
    FormData fdTest = new FormData();
    wTest.setToolTipText(BaseMessages.getString(PKG, "LdapOutputDialog.TestConnection.Tooltip"));
    fdTest.top = new FormAttachment(wCertificateGroup, margin);
    fdTest.right = new FormAttachment(100, 0);
    wTest.setLayoutData(fdTest);

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Settings TAB ///
    // ////////////////////////
    CTabItem wSettingsTab = new CTabItem(wTabFolder, SWT.NONE);
    wSettingsTab.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Settings.Tab"));

    Composite wSettingsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wSettingsComp);

    FormLayout settLayout = new FormLayout();
    settLayout.marginWidth = 3;
    settLayout.marginHeight = 3;
    wSettingsComp.setLayout(settLayout);

    // /////////////////////////////////
    // START OF Search GROUP
    // /////////////////////////////////

    Group wSettings = new Group(wSettingsComp, SWT.SHADOW_NONE);
    props.setLook(wSettings);
    wSettings.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Group.Settings.Label"));

    FormLayout SettingsLayout = new FormLayout();
    SettingsLayout.marginWidth = 10;
    SettingsLayout.marginHeight = 10;
    wSettings.setLayout(SettingsLayout);

    // Operation
    Label wlOperation = new Label(wSettings, SWT.RIGHT);
    wlOperation.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Operation.Label"));
    props.setLook(wlOperation);
    FormData fdlOperation = new FormData();
    fdlOperation.left = new FormAttachment(0, 0);
    fdlOperation.right = new FormAttachment(middle, -margin);
    fdlOperation.top = new FormAttachment(wTransformName, margin);
    wlOperation.setLayoutData(fdlOperation);

    wOperation = new CCombo(wSettings, SWT.BORDER | SWT.READ_ONLY);
    props.setLook(wOperation);
    wOperation.addModifyListener(lsMod);
    FormData fdOperation = new FormData();
    fdOperation.left = new FormAttachment(middle, 0);
    fdOperation.top = new FormAttachment(wTransformName, margin);
    fdOperation.right = new FormAttachment(100, -margin);
    wOperation.setLayoutData(fdOperation);
    wOperation.setItems(LdapOutputMeta.operationTypeDesc);
    wOperation.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            updateOperation();
            input.setChanged();
          }
        });

    // Multi valued field separator
    wlMultiValuedSeparator = new Label(wSettings, SWT.RIGHT);
    wlMultiValuedSeparator.setText(
        BaseMessages.getString(PKG, "LdapOutputDialog.MultiValuedSeparator.Label"));
    props.setLook(wlMultiValuedSeparator);
    FormData fdlMultiValuedSeparator = new FormData();
    fdlMultiValuedSeparator.left = new FormAttachment(0, 0);
    fdlMultiValuedSeparator.top = new FormAttachment(wOperation, margin);
    fdlMultiValuedSeparator.right = new FormAttachment(middle, -margin);
    wlMultiValuedSeparator.setLayoutData(fdlMultiValuedSeparator);
    wMultiValuedSeparator = new TextVar(variables, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMultiValuedSeparator);
    wMultiValuedSeparator.setToolTipText(
        BaseMessages.getString(PKG, "LdapOutputDialog.MultiValuedSeparator.Tooltip"));
    wMultiValuedSeparator.addModifyListener(lsMod);
    FormData fdMultiValuedSeparator = new FormData();
    fdMultiValuedSeparator.left = new FormAttachment(middle, 0);
    fdMultiValuedSeparator.top = new FormAttachment(wOperation, margin);
    fdMultiValuedSeparator.right = new FormAttachment(100, 0);
    wMultiValuedSeparator.setLayoutData(fdMultiValuedSeparator);

    // Fail id not exist
    wlFailIfNotExist = new Label(wSettings, SWT.RIGHT);
    wlFailIfNotExist.setText(BaseMessages.getString(PKG, "LdapOutputDialog.FailIfNotExist.Label"));
    props.setLook(wlFailIfNotExist);
    FormData fdlFailIfNotExist = new FormData();
    fdlFailIfNotExist.left = new FormAttachment(0, 0);
    fdlFailIfNotExist.top = new FormAttachment(wMultiValuedSeparator, margin);
    fdlFailIfNotExist.right = new FormAttachment(middle, -margin);
    wlFailIfNotExist.setLayoutData(fdlFailIfNotExist);
    wFailIfNotExist = new Button(wSettings, SWT.CHECK);
    wFailIfNotExist.setToolTipText(
        BaseMessages.getString(PKG, "LdapOutputDialog.FailIfNotExist.Tooltip"));
    props.setLook(wFailIfNotExist);
    FormData fdFailIfNotExist = new FormData();
    fdFailIfNotExist.left = new FormAttachment(middle, 0);
    fdFailIfNotExist.top = new FormAttachment(wlFailIfNotExist, 0, SWT.CENTER);
    fdFailIfNotExist.right = new FormAttachment(100, 0);
    wFailIfNotExist.setLayoutData(fdFailIfNotExist);
    SelectionAdapter lsSelR =
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wFailIfNotExist.addSelectionListener(lsSelR);

    // Dn fieldname
    wlDnField = new Label(wSettings, SWT.RIGHT);
    wlDnField.setText(BaseMessages.getString(PKG, "LdapOutputDialog.DnField.Label"));
    props.setLook(wlDnField);
    FormData fdlDnField = new FormData();
    fdlDnField.left = new FormAttachment(0, 0);
    fdlDnField.top = new FormAttachment(wFailIfNotExist, margin);
    fdlDnField.right = new FormAttachment(middle, -margin);
    wlDnField.setLayoutData(fdlDnField);
    wDnField = new ComboVar(variables, wSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wDnField.setEditable(true);
    props.setLook(wDnField);
    wDnField.addModifyListener(lsMod);
    FormData fdDnField = new FormData();
    fdDnField.left = new FormAttachment(middle, 0);
    fdDnField.top = new FormAttachment(wFailIfNotExist, margin);
    fdDnField.right = new FormAttachment(100, -margin);
    wDnField.setLayoutData(fdDnField);
    wDnField.addFocusListener(
        new FocusListener() {
          public void focusLost(org.eclipse.swt.events.FocusEvent e) {}

          public void focusGained(org.eclipse.swt.events.FocusEvent e) {
            getPreviousFields();
          }
        });

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment(0, margin);
    fdSettings.top = new FormAttachment(wConnectionGroup, margin);
    fdSettings.right = new FormAttachment(100, -margin);
    wSettings.setLayoutData(fdSettings);

    // ///////////////////////////////////////////////////////////
    // / END OF Search GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Rename GROUP
    // /////////////////////////////////

    Group wRenameGroup = new Group(wSettingsComp, SWT.SHADOW_NONE);
    props.setLook(wRenameGroup);
    wRenameGroup.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Group.RenameGroup.Label"));

    FormLayout RenameGroupLayout = new FormLayout();
    RenameGroupLayout.marginWidth = 10;
    RenameGroupLayout.marginHeight = 10;
    wRenameGroup.setLayout(RenameGroupLayout);

    // OldDn fieldname
    wlOldDnField = new Label(wRenameGroup, SWT.RIGHT);
    wlOldDnField.setText(BaseMessages.getString(PKG, "LdapOutputDialog.OldDnField.Label"));
    props.setLook(wlOldDnField);
    FormData fdlOldDnField = new FormData();
    fdlOldDnField.left = new FormAttachment(0, 0);
    fdlOldDnField.top = new FormAttachment(wSettings, margin);
    fdlOldDnField.right = new FormAttachment(middle, -margin);
    wlOldDnField.setLayoutData(fdlOldDnField);
    wOldDnField = new ComboVar(variables, wRenameGroup, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wOldDnField.setEditable(true);
    props.setLook(wOldDnField);
    wOldDnField.addModifyListener(lsMod);
    FormData fdOldDnField = new FormData();
    fdOldDnField.left = new FormAttachment(middle, 0);
    fdOldDnField.top = new FormAttachment(wSettings, margin);
    fdOldDnField.right = new FormAttachment(100, -margin);
    wOldDnField.setLayoutData(fdOldDnField);
    wOldDnField.addFocusListener(
        new FocusListener() {
          public void focusLost(org.eclipse.swt.events.FocusEvent e) {}

          public void focusGained(org.eclipse.swt.events.FocusEvent e) {
            getPreviousFields();
          }
        });

    // NewDn fieldname
    wlNewDnField = new Label(wRenameGroup, SWT.RIGHT);
    wlNewDnField.setText(BaseMessages.getString(PKG, "LdapOutputDialog.NewDnField.Label"));
    props.setLook(wlNewDnField);
    FormData fdlNewDnField = new FormData();
    fdlNewDnField.left = new FormAttachment(0, 0);
    fdlNewDnField.top = new FormAttachment(wOldDnField, margin);
    fdlNewDnField.right = new FormAttachment(middle, -margin);
    wlNewDnField.setLayoutData(fdlNewDnField);
    wNewDnField = new ComboVar(variables, wRenameGroup, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wNewDnField.setEditable(true);
    props.setLook(wNewDnField);
    wNewDnField.addModifyListener(lsMod);
    FormData fdNewDnField = new FormData();
    fdNewDnField.left = new FormAttachment(middle, 0);
    fdNewDnField.top = new FormAttachment(wOldDnField, margin);
    fdNewDnField.right = new FormAttachment(100, -margin);
    wNewDnField.setLayoutData(fdNewDnField);
    wNewDnField.addFocusListener(
        new FocusListener() {
          public void focusLost(org.eclipse.swt.events.FocusEvent e) {}

          public void focusGained(org.eclipse.swt.events.FocusEvent e) {
            getPreviousFields();
          }
        });

    wlDeleteRDN = new Label(wRenameGroup, SWT.RIGHT);
    wlDeleteRDN.setText(BaseMessages.getString(PKG, "LdapOutputDialog.DeleteRDN.Label"));
    props.setLook(wlDeleteRDN);
    FormData fdlDeleteRDN = new FormData();
    fdlDeleteRDN.left = new FormAttachment(0, 0);
    fdlDeleteRDN.top = new FormAttachment(wNewDnField, margin);
    fdlDeleteRDN.right = new FormAttachment(middle, -margin);
    wlDeleteRDN.setLayoutData(fdlDeleteRDN);
    wDeleteRDN = new Button(wRenameGroup, SWT.CHECK);
    wDeleteRDN.setToolTipText(BaseMessages.getString(PKG, "LdapOutputDialog.DeleteRDN.Tooltip"));
    props.setLook(wDeleteRDN);
    FormData fdDeleteRDN = new FormData();
    fdDeleteRDN.left = new FormAttachment(middle, 0);
    fdDeleteRDN.top = new FormAttachment(wlDeleteRDN, 0, SWT.CENTER);
    fdDeleteRDN.right = new FormAttachment(100, 0);
    wDeleteRDN.setLayoutData(fdDeleteRDN);
    SelectionAdapter lsSeld =
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wDeleteRDN.addSelectionListener(lsSeld);

    FormData fdRenameGroup = new FormData();
    fdRenameGroup.left = new FormAttachment(0, margin);
    fdRenameGroup.top = new FormAttachment(wSettings, margin);
    fdRenameGroup.right = new FormAttachment(100, -margin);
    wRenameGroup.setLayoutData(fdRenameGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Rename GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdSettingsComp = new FormData();
    fdSettingsComp.left = new FormAttachment(0, 0);
    fdSettingsComp.top = new FormAttachment(0, 0);
    fdSettingsComp.right = new FormAttachment(100, 0);
    fdSettingsComp.bottom = new FormAttachment(100, 0);
    wSettingsComp.setLayoutData(fdSettingsComp);

    wSettingsComp.layout();
    wSettingsTab.setControl(wSettingsComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Settings TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Fields TAB ///
    // ////////////////////////
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Fields.Tab"));

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFieldsComp);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wFieldsComp.setLayout(fieldsLayout);

    // /////////////////////////////////
    // START OF Fields GROUP
    // /////////////////////////////////

    Group wFields = new Group(wFieldsComp, SWT.SHADOW_NONE);
    props.setLook(wFields);
    wFields.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Group.Fields.Label"));

    FormLayout FieldsLayout = new FormLayout();
    FieldsLayout.marginWidth = 10;
    FieldsLayout.marginHeight = 10;
    wFields.setLayout(FieldsLayout);

    // Basedn line
    wlBaseDN = new Label(wFields, SWT.RIGHT);
    wlBaseDN.setText(BaseMessages.getString(PKG, "LdapOutputDialog.BaseDN.Label"));
    props.setLook(wlBaseDN);
    FormData fdlBaseDN = new FormData();
    fdlBaseDN.left = new FormAttachment(0, 0);
    fdlBaseDN.top = new FormAttachment(wSettings, margin);
    fdlBaseDN.right = new FormAttachment(middle, -margin);
    wlBaseDN.setLayoutData(fdlBaseDN);
    wBaseDN = new TextVar(variables, wFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBaseDN.setToolTipText(BaseMessages.getString(PKG, "LdapOutputDialog.BaseDN.Tooltip"));
    props.setLook(wBaseDN);
    wBaseDN.addModifyListener(lsMod);
    FormData fdBaseDN = new FormData();
    fdBaseDN.left = new FormAttachment(middle, 0);
    fdBaseDN.top = new FormAttachment(wSettings, margin);
    fdBaseDN.right = new FormAttachment(100, 0);
    wBaseDN.setLayoutData(fdBaseDN);
    wBaseDN.addModifyListener(
        e -> {
          input.setChanged();
          if (Utils.isEmpty(wBaseDN.getText())) {
            wDoMapping.setEnabled(false);
          } else {
            setFieldsCombo();
            wDoMapping.setEnabled(true);
          }
        });
    // THE UPDATE/INSERT TABLE
    wlReturn = new Label(wFields, SWT.NONE);
    wlReturn.setText(BaseMessages.getString(PKG, "LdapOutputUpdateDialog.UpdateFields.Label"));
    props.setLook(wlReturn);
    FormData fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment(0, 0);
    fdlReturn.top = new FormAttachment(wBaseDN, margin);
    wlReturn.setLayoutData(fdlReturn);

    int UpInsCols = 3;
    int UpInsRows = (input.getUpdateLookup() != null ? input.getUpdateLookup().length : 1);

    ciReturn = new ColumnInfo[UpInsCols];
    ciReturn[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "LdapOutputUpdateDialog.ColumnInfo.TableField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "LdapOutputUpdateDialog.ColumnInfo.StreamField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "LdapOutputUpdateDialog.ColumnInfo.Update"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {"Y", "N"});

    tableFieldColumns.add(ciReturn[0]);
    wReturn =
        new TableView(
            variables,
            wFields,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciReturn,
            UpInsRows,
            lsMod,
            props);

    wGetLU = new Button(wFields, SWT.PUSH);
    wGetLU.setText(BaseMessages.getString(PKG, "LdapOutputUpdateDialog.GetAndUpdateFields.Label"));
    FormData fdGetLU = new FormData();
    fdGetLU.top = new FormAttachment(wlReturn, margin);
    fdGetLU.right = new FormAttachment(100, 0);
    wGetLU.setLayoutData(fdGetLU);

    wDoMapping = new Button(wFields, SWT.PUSH);
    wDoMapping.setText(BaseMessages.getString(PKG, "LdapOutputUpdateDialog.EditMapping.Label"));
    FormData fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment(wGetLU, margin);
    fdDoMapping.right = new FormAttachment(100, 0);
    wDoMapping.setLayoutData(fdDoMapping);

    wDoMapping.addListener(SWT.Selection, arg0 -> generateMappings());

    FormData fdReturn = new FormData();
    fdReturn.left = new FormAttachment(0, 0);
    fdReturn.top = new FormAttachment(wlReturn, margin);
    fdReturn.right = new FormAttachment(wGetLU, -5 * margin);
    fdReturn.bottom = new FormAttachment(100, -2 * margin);
    wReturn.setLayoutData(fdReturn);

    //
    // Search the fields in the background
    //

    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.put(row.getValueMeta(i).getName(), i);
              }

              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, margin);
    fdFields.top = new FormAttachment(wSettings, margin);
    fdFields.right = new FormAttachment(100, -margin);
    fdFields.bottom = new FormAttachment(100, -margin);
    wFields.setLayoutData(fdFields);

    // ///////////////////////////////////////////////////////////
    // / END OF Fields GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Fields TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners
    wTest.addListener(SWT.Selection, e -> test());
    wGetLU.addListener(SWT.Selection, e -> getUpdate());

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    wTabFolder.setSelection(0);

    // Set the shell size, based upon previous time...
    setSize();
    getData(input);
    useAuthentication();
    setProtocol();
    setTrustStore();
    updateOperation();
    input.setChanged(changed);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void test() {
    LdapConnection connection = null;
    try {

      LdapOutputMeta meta = new LdapOutputMeta();
      getInfo(meta);

      // Defined a LDAP connection
      connection = new LdapConnection(log, variables, meta, null);
      // connect...
      if (wUsingAuthentication.getSelection()) {
        connection.connect(
            variables.resolve(meta.getUserName()),
            Encr.decryptPasswordOptionallyEncrypted(variables.resolve(meta.getPassword())));
      } else {
        connection.connect();
      }
      // We are successfully connected

      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      mb.setMessage(BaseMessages.getString(PKG, "LdapOutputDialog.Connected.OK") + Const.CR);
      mb.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Connected.Title.Ok"));
      mb.open();

    } catch (Exception e) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "LdapOutputDialog.Connected.NOK", e.getMessage()));
      mb.setText(BaseMessages.getString(PKG, "LdapOutputDialog.Connected.Title.Error"));
      mb.open();
    } finally {
      if (connection != null) {
        // Disconnect ...
        try {
          connection.close();
        } catch (Exception e) {
          /* Ignore */
        }
      }
    }
  }

  /**
   * Read the data from the LdapOutputMeta object and show it in this dialog.
   *
   * @param in The LdapOutputMeta object to obtain the data from.
   */
  public void getData(LdapOutputMeta in) {
    wProtocol.setText(
        Const.NVL(in.getProtocol(), LdapProtocolFactory.getConnectionTypes(log).get(0)));
    wSetTrustStore.setSelection(in.isUseCertificate());
    if (in.getTrustStorePath() != null) {
      wTrustStorePath.setText(in.getTrustStorePath());
    }
    if (in.getTrustStorePassword() != null) {
      wTrustStorePassword.setText(in.getTrustStorePassword());
    }
    wTrustAll.setSelection(in.isTrustAllCertificates());

    wUsingAuthentication.setSelection(in.isUseAuthentication());

    if (in.getHost() != null) {
      wHost.setText(in.getHost());
    }
    if (in.getUserName() != null) {
      wUserName.setText(in.getUserName());
    }
    if (in.getPassword() != null) {
      wPassword.setText(in.getPassword());
    }
    if (in.getPort() != null) {
      wPort.setText(in.getPort());
    }
    if (in.getDnField() != null) {
      wDnField.setText(in.getDnField());
    }
    wFailIfNotExist.setSelection(in.isFailIfNotExist());
    wOperation.setText(LdapOutputMeta.getOperationTypeDesc(input.getOperationType()));
    if (in.getMultiValuedSeparator() != null) {
      wMultiValuedSeparator.setText(in.getMultiValuedSeparator());
    }
    if (in.getSearchBaseDN() != null) {
      wBaseDN.setText(in.getSearchBaseDN());
    }

    wReferral.setText(LdapOutputMeta.getReferralTypeDesc(input.getReferralType()));
    wDerefAliases.setText(LdapOutputMeta.getDerefAliasesTypeDesc(input.getDerefAliasesType()));

    if (in.getOldDnFieldName() != null) {
      wOldDnField.setText(in.getOldDnFieldName());
    }
    if (in.getNewDnFieldName() != null) {
      wNewDnField.setText(in.getNewDnFieldName());
    }
    wDeleteRDN.setSelection(in.isDeleteRDN());

    if (input.getUpdateLookup() != null) {
      for (int i = 0; i < input.getUpdateLookup().length; i++) {
        TableItem item = wReturn.table.getItem(i);
        if (input.getUpdateLookup()[i] != null) {
          item.setText(1, input.getUpdateLookup()[i]);
        }
        if (input.getUpdateStream()[i] != null) {
          item.setText(2, input.getUpdateStream()[i]);
        }
        if (input.getUpdate()[i] == null || input.getUpdate()[i]) {
          item.setText(3, "Y");
        } else {
          item.setText(3, "N");
        }
      }
    }

    wReturn.removeEmptyRows();
    wReturn.setRowNums();
    wReturn.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText();
    try {
      getInfo(input);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "LdapOutputDialog.ErrorParsingData.DialogTitle"),
          BaseMessages.getString(PKG, "LdapOutputDialog.ErrorParsingData.DialogMessage"),
          e);
    }
    dispose();
  }

  private void getInfo(LdapOutputMeta in) throws HopException {
    transformName = wTransformName.getText(); // return value
    in.setProtocol(wProtocol.getText());
    in.setUseCertificate(wSetTrustStore.getSelection());
    in.setTrustStorePath(wTrustStorePath.getText());
    in.setTrustStorePassword(wTrustStorePassword.getText());
    in.setTrustAllCertificates(wTrustAll.getSelection());

    in.setUseAuthentication(wUsingAuthentication.getSelection());
    in.setHost(wHost.getText());
    in.setUserName(wUserName.getText());
    in.setPassword(wPassword.getText());
    in.setPort(wPort.getText());
    in.setDnField(wDnField.getText());
    in.setFailIfNotExist(wFailIfNotExist.getSelection());
    in.setOperationType(LdapOutputMeta.getOperationTypeByDesc(wOperation.getText()));
    in.setMultiValuedSeparator(wMultiValuedSeparator.getText());
    in.setSearchBaseDN(wBaseDN.getText());
    in.setReferralType(LdapOutputMeta.getReferralTypeByDesc(wReferral.getText()));
    in.setDerefAliasesType(LdapOutputMeta.getDerefAliasesTypeByDesc(wDerefAliases.getText()));

    in.setOldDnFieldName(wOldDnField.getText());
    in.setNewDnFieldName(wNewDnField.getText());
    in.setDeleteRDN(wDeleteRDN.getSelection());

    int nrFields = wReturn.nrNonEmpty();

    in.allocate(nrFields);

    // CHECKSTYLE:Indentation:OFF
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wReturn.getNonEmpty(i);
      in.getUpdateLookup()[i] = item.getText(1);
      in.getUpdateStream()[i] = item.getText(2);
      in.getUpdate()[i] = "Y".equals(item.getText(3));
    }
  }

  private void useAuthentication() {
    wUserName.setEnabled(wUsingAuthentication.getSelection());
    wlUserName.setEnabled(wUsingAuthentication.getSelection());
    wPassword.setEnabled(wUsingAuthentication.getSelection());
    wlPassword.setEnabled(wUsingAuthentication.getSelection());
  }

  private void getPreviousFields() {
    if (!gotPrevious) {
      try {
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          String dn = wDnField.getText();
          String olddn = wOldDnField.getText();
          String newdn = wNewDnField.getText();
          wDnField.removeAll();
          wOldDnField.removeAll();
          wNewDnField.removeAll();
          wDnField.setItems(r.getFieldNames());
          wOldDnField.setItems(r.getFieldNames());
          wNewDnField.setItems(r.getFieldNames());
          if (dn != null) {
            wDnField.setText(dn);
          }
          if (olddn != null) {
            wOldDnField.setText(olddn);
          }
          if (newdn != null) {
            wNewDnField.setText(newdn);
          }
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "LdapOutputDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "LdapOutputDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
      gotPrevious = true;
    }
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll(inputFields);

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>(keySet);

    String[] fieldNames = entries.toArray(new String[entries.size()]);
    Const.sortStrings(fieldNames);
    // return fields
    ciReturn[1].setComboValues(fieldNames);
  }

  private void getUpdate() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              tableItem.setText(3, "Y");
              return true;
            };
        BaseTransformDialog.getFieldsFromPrevious(
            r, wReturn, 1, new int[] {1, 2}, new int[] {}, -1, -1, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "LdapOutputUpdateDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "LdapOutputUpdateDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void updateOperation() {
    boolean activateFields =
        (LdapOutputMeta.getOperationTypeByDesc(wOperation.getText())
                != LdapOutputMeta.OPERATION_TYPE_DELETE
            && LdapOutputMeta.getOperationTypeByDesc(wOperation.getText())
                != LdapOutputMeta.OPERATION_TYPE_RENAME);

    wlReturn.setEnabled(activateFields);
    wReturn.setEnabled(activateFields);
    wGetLU.setEnabled(activateFields);
    wBaseDN.setEnabled(activateFields);
    wlBaseDN.setEnabled(activateFields);
    wDoMapping.setEnabled(activateFields && !Utils.isEmpty(wBaseDN.getText()));

    boolean activateMulTiValueSeparator =
        (LdapOutputMeta.getOperationTypeByDesc(wOperation.getText())
                != LdapOutputMeta.OPERATION_TYPE_DELETE)
            && (LdapOutputMeta.getOperationTypeByDesc(wOperation.getText())
                != LdapOutputMeta.OPERATION_TYPE_UPDATE)
            && (LdapOutputMeta.getOperationTypeByDesc(wOperation.getText())
                != LdapOutputMeta.OPERATION_TYPE_RENAME);
    wlMultiValuedSeparator.setEnabled(activateMulTiValueSeparator);
    wMultiValuedSeparator.setEnabled(activateMulTiValueSeparator);

    boolean activateFailIfNotExist =
        (LdapOutputMeta.getOperationTypeByDesc(wOperation.getText())
                != LdapOutputMeta.OPERATION_TYPE_UPSERT)
            && (LdapOutputMeta.getOperationTypeByDesc(wOperation.getText())
                != LdapOutputMeta.OPERATION_TYPE_INSERT)
            && (LdapOutputMeta.getOperationTypeByDesc(wOperation.getText())
                != LdapOutputMeta.OPERATION_TYPE_RENAME);
    wlFailIfNotExist.setEnabled(activateFailIfNotExist);
    wFailIfNotExist.setEnabled(activateFailIfNotExist);

    boolean activateRename =
        (LdapOutputMeta.getOperationTypeByDesc(wOperation.getText())
            == LdapOutputMeta.OPERATION_TYPE_RENAME);
    wlOldDnField.setEnabled(activateRename);
    wOldDnField.setEnabled(activateRename);
    wlNewDnField.setEnabled(activateRename);
    wNewDnField.setEnabled(activateRename);
    wlDeleteRDN.setEnabled(activateRename);
    wDeleteRDN.setEnabled(activateRename);
    wlDnField.setEnabled(!activateRename);
    wDnField.setEnabled(!activateRename);
  }

  public IRowMeta getLDAPFields() throws HopException {
    LdapConnection connection = null;
    try {
      LdapOutputMeta meta = new LdapOutputMeta();
      getInfo(meta);
      // Defined a LDAP connection
      connection = new LdapConnection(log, variables, meta, null);
      // connect ...
      if (wUsingAuthentication.getSelection()) {
        String username = variables.resolve(wUserName.getText());
        String password =
            Encr.decryptPasswordOptionallyEncrypted(variables.resolve(wPassword.getText()));
        connection.connect(username, password);
      } else {
        connection.connect();
      }
      return connection.getFields(variables.resolve(wBaseDN.getText()));
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (Exception e) {
          /* Ignore */
        }
      }
    }
  }

  /**
   * Reads in the fields from the previous transforms and from the ONE next transform and opens an
   * EnterMappingDialog with this information. After the user did the mapping, those information is
   * put into the Select/Rename table.
   */
  private void generateMappings() {

    // Determine the source and target fields...
    //
    IRowMeta sourceFields;
    IRowMeta targetFields;

    try {
      sourceFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "LdapOutputDialog.DoMapping.UnableToFindSourceFields.Title"),
          BaseMessages.getString(
              PKG, "LdapOutputDialog.DoMapping.UnableToFindSourceFields.Message"),
          e);
      return;
    }
    LdapConnection connection = null;
    try {

      // return fields
      targetFields = getLDAPFields();

    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "LdapOutputDialog.DoMapping.UnableToFindTargetFields.Title"),
          BaseMessages.getString(
              PKG, "LdapOutputDialog.DoMapping.UnableToFindTargetFields.Message"),
          e);
      return;
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (Exception e) {
          /* Ignore */
        }
      }
    }

    String[] inputNames = new String[sourceFields.size()];
    for (int i = 0; i < sourceFields.size(); i++) {
      IValueMeta value = sourceFields.getValueMeta(i);
      inputNames[i] = value.getName();
    }

    // Create the existing mapping list...
    //
    List<SourceToTargetMapping> mappings = new ArrayList<>();
    StringBuilder missingSourceFields = new StringBuilder();
    StringBuilder missingTargetFields = new StringBuilder();

    int nrFields = wReturn.nrNonEmpty();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wReturn.getNonEmpty(i);
      String source = item.getText(2);
      String target = item.getText(1);

      int sourceIndex = sourceFields.indexOfValue(source);
      if (sourceIndex < 0) {
        missingSourceFields.append(Const.CR + "   " + source + " --> " + target);
      }
      int targetIndex = targetFields.indexOfValue(target);
      if (targetIndex < 0) {
        missingTargetFields.append(Const.CR + "   " + source + " --> " + target);
      }
      if (sourceIndex < 0 || targetIndex < 0) {
        continue;
      }

      SourceToTargetMapping mapping = new SourceToTargetMapping(sourceIndex, targetIndex);
      mappings.add(mapping);
    }

    // show a confirm dialog if some missing field was found
    //
    if (missingSourceFields.length() > 0 || missingTargetFields.length() > 0) {

      String message = "";
      if (missingSourceFields.length() > 0) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "LdapOutputDialog.DoMapping.SomeSourceFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      if (missingTargetFields.length() > 0) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "LdapOutputDialog.DoMapping.SomeTargetFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      message += Const.CR;
      message +=
          BaseMessages.getString(PKG, "LdapOutputDialog.DoMapping.SomeFieldsNotFoundContinue")
              + Const.CR;
      Window.setDefaultImage(GuiResource.getInstance().getImageHopUi());
      boolean goOn =
          MessageDialog.openConfirm(
              shell,
              BaseMessages.getString(PKG, "LdapOutputDialog.DoMapping.SomeFieldsNotFoundTitle"),
              message);
      if (!goOn) {
        return;
      }
    }
    EnterMappingDialog d =
        new EnterMappingDialog(
            LdapOutputDialog.this.shell,
            sourceFields.getFieldNames(),
            targetFields.getFieldNames(),
            mappings);
    mappings = d.open();

    // mappings == null if the user pressed cancel
    //
    if (mappings != null) {
      // Clear and re-populate!
      //
      wReturn.table.removeAll();
      wReturn.table.setItemCount(mappings.size());
      for (int i = 0; i < mappings.size(); i++) {
        SourceToTargetMapping mapping = mappings.get(i);
        TableItem item = wReturn.table.getItem(i);
        item.setText(2, sourceFields.getValueMeta(mapping.getSourcePosition()).getName());
        item.setText(1, targetFields.getValueMeta(mapping.getTargetPosition()).getName());
      }
      wReturn.setRowNums();
      wReturn.optWidth(true);
    }
  }

  public void setFieldsCombo() {
    Display display = shell.getDisplay();
    if (!(display == null || display.isDisposed())) {
      display.asyncExec(
          () -> {
            // clear
            for (ColumnInfo colInfo : tableFieldColumns) {
              colInfo.setComboValues(new String[] {});
            }
            if (wBaseDN.isDisposed()) {
              return;
            }
            String baseDn = variables.resolve(wBaseDN.getText());
            if (!Utils.isEmpty(baseDn)) {
              try {
                IRowMeta fields = getLDAPFields();
                // loop through the objects and find build the list of fields
                String[] fieldsName = new String[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                  fieldsName[i] = fields.getValueMeta(i).getName();
                }

                if (fieldsName != null) {
                  for (ColumnInfo colInfo : tableFieldColumns) {
                    colInfo.setComboValues(fieldsName);
                  }
                }
              } catch (Exception e) {
                for (ColumnInfo colInfo : tableFieldColumns) {
                  colInfo.setComboValues(new String[] {});
                }
                // ignore any errors here. drop downs will not be
                // filled, but no problem for the user
              }
            }
          });
    }
  }

  private void setProtocol() {
    boolean enable = !LdapProtocol.getName().equals(wProtocol.getText());
    wlSetTrustStore.setEnabled(enable);
    wSetTrustStore.setEnabled(enable);
    setTrustStore();
  }

  private void setTrustStore() {
    boolean enable =
        wSetTrustStore.getSelection() && !LdapProtocol.getName().equals(wProtocol.getText());
    wlTrustAll.setEnabled(enable);
    wTrustAll.setEnabled(enable);
    trustAll();
  }

  private void trustAll() {
    boolean enable =
        wSetTrustStore.getSelection()
            && !LdapProtocol.getName().equals(wProtocol.getText())
            && !wTrustAll.getSelection();
    wlTrustStorePath.setEnabled(enable);
    wTrustStorePath.setEnabled(enable);
    wlTrustStorePassword.setEnabled(enable);
    wTrustStorePassword.setEnabled(enable);
    wbbFilename.setEnabled(enable);
  }
}
