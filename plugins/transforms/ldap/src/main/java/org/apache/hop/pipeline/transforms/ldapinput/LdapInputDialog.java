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
package org.apache.hop.pipeline.transforms.ldapinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.ControlSpaceKeyAdapter;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
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

public class LdapInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = LdapInputMeta.class; // For Translator

  private Button wInclRownum;

  private Button wSetPaging;

  private Label wlPageSize;
  private TextVar wPageSize;

  private Button wUsingAuthentication;

  private Label wlInclRownumField;
  private TextVar wInclRownumField;

  private Text wLimit;

  private TextVar wTimeLimit;

  private TextVar wMultiValuedSeparator;

  private TableView wFields;

  private final LdapInputMeta input;

  private TextVar wHost;

  private Label wlUserName;
  private TextVar wUserName;

  private Label wlPassword;
  private TextVar wPassword;

  private TextVar wPort;

  private Label wlSearchBase;
  private TextVar wSearchBase;

  private Label wlTrustStorePath;
  private TextVar wTrustStorePath;

  private Label wlTrustStorePassword;
  private TextVar wTrustStorePassword;

  private Label wlSetTrustStore;
  private Button wSetTrustStore;

  private Label wlTrustAll;
  private Button wTrustAll;

  private Label wlFilterString;
  private StyledTextComp wFilterString;

  private Button wDynamicBase;

  private Label wlSearchBaseField;
  private CCombo wSearchBaseField;

  private Button wDynamicFilter;

  private Label wlFilterField;
  private CCombo wFilterField;

  private CCombo wSearchScope;

  public static final int[] dateLengths = new int[] {23, 19, 14, 10, 10, 10, 10, 8, 8, 8, 8, 6, 6};
  private ColumnInfo[] colinf;
  private boolean gotPreviousFields = false;

  private ComboVar wProtocol;

  private Button wbbFilename;

  public LdapInputDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (LdapInputMeta) in;
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
    shell.setText(BaseMessages.getString(PKG, "LdapInputDialog.DialogTitle"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "LdapInputDialog.Button.PreviewRows"));
    wPreview.addListener(SWT.Selection, e -> preview());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wPreview, wCancel}, margin, null);

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
    wGeneralTab.setText(BaseMessages.getString(PKG, "LdapInputDialog.General.Tab"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wGeneralComp.setLayout(fileLayout);

    // /////////////////////////////////
    // START OF Host GROUP
    // /////////////////////////////////

    Group wHostGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wHostGroup);
    wHostGroup.setText(BaseMessages.getString(PKG, "LdapInputDialog.Group.HostGroup.Label"));

    FormLayout HostGroupLayout = new FormLayout();
    HostGroupLayout.marginWidth = 10;
    HostGroupLayout.marginHeight = 10;
    wHostGroup.setLayout(HostGroupLayout);

    // Host line
    Label wlHost = new Label(wHostGroup, SWT.RIGHT);
    wlHost.setText(BaseMessages.getString(PKG, "LdapInputDialog.Host.Label"));
    props.setLook(wlHost);
    FormData fdlHost = new FormData();
    fdlHost.left = new FormAttachment(0, 0);
    fdlHost.top = new FormAttachment(wTransformName, margin);
    fdlHost.right = new FormAttachment(middle, -margin);
    wlHost.setLayoutData(fdlHost);
    wHost = new TextVar(variables, wHostGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHost.setToolTipText(BaseMessages.getString(PKG, "LdapInputDialog.Host.Tooltip"));
    props.setLook(wHost);
    wHost.addModifyListener(lsMod);
    FormData fdHost = new FormData();
    fdHost.left = new FormAttachment(middle, 0);
    fdHost.top = new FormAttachment(wTransformName, margin);
    fdHost.right = new FormAttachment(100, 0);
    wHost.setLayoutData(fdHost);

    // Port line
    Label wlPort = new Label(wHostGroup, SWT.RIGHT);
    wlPort.setText(BaseMessages.getString(PKG, "LdapInputDialog.Port.Label"));
    props.setLook(wlPort);
    FormData fdlPort = new FormData();
    fdlPort.left = new FormAttachment(0, 0);
    fdlPort.top = new FormAttachment(wHost, margin);
    fdlPort.right = new FormAttachment(middle, -margin);
    wlPort.setLayoutData(fdlPort);
    wPort = new TextVar(variables, wHostGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPort);
    wPort.setToolTipText(BaseMessages.getString(PKG, "LdapInputDialog.Port.Tooltip"));
    wPort.addModifyListener(lsMod);
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment(middle, 0);
    fdPort.top = new FormAttachment(wHost, margin);
    fdPort.right = new FormAttachment(100, 0);
    wPort.setLayoutData(fdPort);

    // Protocol Line
    Label wlProtocol = new Label(wHostGroup, SWT.RIGHT);
    wlProtocol.setText(BaseMessages.getString(PKG, "LdapInputDialog.Protocol.Label"));
    props.setLook(wlProtocol);
    FormData fdlProtocol = new FormData();
    fdlProtocol.left = new FormAttachment(0, 0);
    fdlProtocol.right = new FormAttachment(middle, -margin);
    fdlProtocol.top = new FormAttachment(wPort, margin);
    wlProtocol.setLayoutData(fdlProtocol);

    wProtocol = new ComboVar(variables, wHostGroup, SWT.BORDER | SWT.READ_ONLY);
    wProtocol.setEditable(true);
    props.setLook(wProtocol);
    wProtocol.addModifyListener(lsMod);
    FormData fdProtocol = new FormData();
    fdProtocol.left = new FormAttachment(middle, 0);
    fdProtocol.top = new FormAttachment(wPort, margin);
    fdProtocol.right = new FormAttachment(100, -margin);
    wProtocol.setLayoutData(fdProtocol);
    wProtocol.setItems(LdapProtocolFactory.getConnectionTypes(log).toArray(new String[] {}));
    wProtocol.addSelectionListener(
        new SelectionAdapter() {

          public void widgetSelected(SelectionEvent e) {
            setProtocol();
          }
        });

    FormData fdHostGroup = new FormData();
    fdHostGroup.left = new FormAttachment(0, margin);
    fdHostGroup.top = new FormAttachment(0, margin);
    fdHostGroup.right = new FormAttachment(100, -margin);
    wHostGroup.setLayoutData(fdHostGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Host GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Authentication GROUP
    // /////////////////////////////////

    Group wAuthenticationGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wAuthenticationGroup);
    wAuthenticationGroup.setText(
        BaseMessages.getString(PKG, "LdapInputDialog.Group.AuthenticationGroup.Label"));

    FormLayout AuthenticationGroupLayout = new FormLayout();
    AuthenticationGroupLayout.marginWidth = 10;
    AuthenticationGroupLayout.marginHeight = 10;
    wAuthenticationGroup.setLayout(AuthenticationGroupLayout);

    // using authentication ?
    Label wlUsingAuthentication = new Label(wAuthenticationGroup, SWT.RIGHT);
    wlUsingAuthentication.setText(
        BaseMessages.getString(PKG, "LdapInputDialog.usingAuthentication.Label"));
    props.setLook(wlUsingAuthentication);
    FormData fdlUsingAuthentication = new FormData();
    fdlUsingAuthentication.left = new FormAttachment(0, 0);
    fdlUsingAuthentication.top = new FormAttachment(wHostGroup, margin);
    fdlUsingAuthentication.right = new FormAttachment(middle, -margin);
    wlUsingAuthentication.setLayoutData(fdlUsingAuthentication);
    wUsingAuthentication = new Button(wAuthenticationGroup, SWT.CHECK);
    props.setLook(wUsingAuthentication);
    wUsingAuthentication.setToolTipText(
        BaseMessages.getString(PKG, "LdapInputDialog.usingAuthentication.Tooltip"));
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
    wlUserName.setText(BaseMessages.getString(PKG, "LdapInputDialog.Username.Label"));
    props.setLook(wlUserName);
    FormData fdlUserName = new FormData();
    fdlUserName.left = new FormAttachment(0, 0);
    fdlUserName.top = new FormAttachment(wUsingAuthentication, margin);
    fdlUserName.right = new FormAttachment(middle, -margin);
    wlUserName.setLayoutData(fdlUserName);
    wUserName = new TextVar(variables, wAuthenticationGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wUserName);
    wUserName.setToolTipText(BaseMessages.getString(PKG, "LdapInputDialog.Username.Tooltip"));
    wUserName.addModifyListener(lsMod);
    FormData fdUserName = new FormData();
    fdUserName.left = new FormAttachment(middle, 0);
    fdUserName.top = new FormAttachment(wUsingAuthentication, margin);
    fdUserName.right = new FormAttachment(100, 0);
    wUserName.setLayoutData(fdUserName);

    // Password line
    wlPassword = new Label(wAuthenticationGroup, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "LdapInputDialog.Password.Label"));
    props.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.top = new FormAttachment(wUserName, margin);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword =
        new PasswordTextVar( variables, wAuthenticationGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wPassword.setToolTipText(BaseMessages.getString(PKG, "LdapInputDialog.Password.Tooltip"));
    props.setLook(wPassword);
    wPassword.addModifyListener(lsMod);
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.top = new FormAttachment(wUserName, margin);
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);

    FormData fdAuthenticationGroup = new FormData();
    fdAuthenticationGroup.left = new FormAttachment(0, margin);
    fdAuthenticationGroup.top = new FormAttachment(wHostGroup, margin);
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
        BaseMessages.getString(PKG, "LdapInputDialog.Group.CertificateGroup.Label"));

    FormLayout CertificateGroupLayout = new FormLayout();
    CertificateGroupLayout.marginWidth = 10;
    CertificateGroupLayout.marginHeight = 10;
    wCertificateGroup.setLayout(CertificateGroupLayout);

    // set TrustStore?
    wlSetTrustStore = new Label(wCertificateGroup, SWT.RIGHT);
    wlSetTrustStore.setText(BaseMessages.getString(PKG, "LdapInputDialog.setTrustStore.Label"));
    props.setLook(wlSetTrustStore);
    FormData fdlsetTrustStore = new FormData();
    fdlsetTrustStore.left = new FormAttachment(0, 0);
    fdlsetTrustStore.top = new FormAttachment(wAuthenticationGroup, margin);
    fdlsetTrustStore.right = new FormAttachment(middle, -margin);
    wlSetTrustStore.setLayoutData(fdlsetTrustStore);
    wSetTrustStore = new Button(wCertificateGroup, SWT.CHECK);
    props.setLook(wSetTrustStore);
    wSetTrustStore.setToolTipText(
        BaseMessages.getString(PKG, "LdapInputDialog.setTrustStore.Tooltip"));
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
    wlTrustStorePath.setText(BaseMessages.getString(PKG, "LdapInputDialog.TrustStorePath.Label"));
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
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wTrustStorePath,
                variables,
                new String[] {"*"},
                new String[] {"All files"},
                true));

    wTrustStorePath =
        new TextVar(variables, wCertificateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTrustStorePath);
    wTrustStorePath.setToolTipText(
        BaseMessages.getString(PKG, "LdapInputDialog.TrustStorePath.Tooltip"));
    wTrustStorePath.addModifyListener(lsMod);
    FormData fdTrustStorePath = new FormData();
    fdTrustStorePath.left = new FormAttachment(middle, 0);
    fdTrustStorePath.top = new FormAttachment(wSetTrustStore, margin);
    fdTrustStorePath.right = new FormAttachment(wbbFilename, -margin);
    wTrustStorePath.setLayoutData(fdTrustStorePath);

    // TrustStorePassword line
    wlTrustStorePassword = new Label(wCertificateGroup, SWT.RIGHT);
    wlTrustStorePassword.setText(
        BaseMessages.getString(PKG, "LdapInputDialog.TrustStorePassword.Label"));
    props.setLook(wlTrustStorePassword);
    FormData fdlTrustStorePassword = new FormData();
    fdlTrustStorePassword.left = new FormAttachment(0, -margin);
    fdlTrustStorePassword.top = new FormAttachment(wbbFilename, margin);
    fdlTrustStorePassword.right = new FormAttachment(middle, -margin);
    wlTrustStorePassword.setLayoutData(fdlTrustStorePassword);
    wTrustStorePassword =
        new PasswordTextVar( variables, wCertificateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTrustStorePassword);
    wTrustStorePassword.setToolTipText(
        BaseMessages.getString(PKG, "LdapInputDialog.TrustStorePassword.Tooltip"));
    wTrustStorePassword.addModifyListener(lsMod);
    FormData fdTrustStorePassword = new FormData();
    fdTrustStorePassword.left = new FormAttachment(middle, 0);
    fdTrustStorePassword.top = new FormAttachment(wbbFilename, margin);
    fdTrustStorePassword.right = new FormAttachment(100, -margin);
    wTrustStorePassword.setLayoutData(fdTrustStorePassword);

    // Trust all certificate?
    wlTrustAll = new Label(wCertificateGroup, SWT.RIGHT);
    wlTrustAll.setText(BaseMessages.getString(PKG, "LdapInputDialog.TrustAll.Label"));
    props.setLook(wlTrustAll);
    FormData fdlTrustAll = new FormData();
    fdlTrustAll.left = new FormAttachment(0, 0);
    fdlTrustAll.top = new FormAttachment(wTrustStorePassword, margin);
    fdlTrustAll.right = new FormAttachment(middle, -margin);
    wlTrustAll.setLayoutData(fdlTrustAll);
    wTrustAll = new Button(wCertificateGroup, SWT.CHECK);
    props.setLook(wTrustAll);
    wTrustAll.setToolTipText(BaseMessages.getString(PKG, "LdapInputDialog.TrustAll.Tooltip"));
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
    wTest.setText(BaseMessages.getString(PKG, "LdapInputDialog.TestConnection.Label"));
    props.setLook(wTest);
    FormData fdTest = new FormData();
    wTest.setToolTipText(BaseMessages.getString(PKG, "LdapInputDialog.TestConnection.Tooltip"));
    // fdTest.left = new FormAttachment(middle, 0);
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
    // START OF Search TAB///
    // /
    CTabItem wSearchTab = new CTabItem(wTabFolder, SWT.NONE);
    wSearchTab.setText(BaseMessages.getString(PKG, "LdapInputDialog.Search.Tab"));

    FormLayout SearchLayout = new FormLayout();
    SearchLayout.marginWidth = 3;
    SearchLayout.marginHeight = 3;

    Composite wSearchComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wSearchComp);
    wSearchComp.setLayout(SearchLayout);

    // /////////////////////////////////
    // START OF Search GROUP
    // /////////////////////////////////

    Group wSearchGroup = new Group(wSearchComp, SWT.SHADOW_NONE);
    props.setLook(wSearchGroup);
    wSearchGroup.setText(BaseMessages.getString(PKG, "LdapInputDialog.Group.SearchGroup.Label"));

    FormLayout searchgroupLayout = new FormLayout();
    searchgroupLayout.marginWidth = 10;
    searchgroupLayout.marginHeight = 10;
    wSearchGroup.setLayout(searchgroupLayout);

    // Is base defined in a Field
    Label wlDynamicBase = new Label(wSearchGroup, SWT.RIGHT);
    wlDynamicBase.setText(BaseMessages.getString(PKG, "LdapInputDialog.dynamicBase.Label"));
    props.setLook(wlDynamicBase);
    FormData fdlDynamicBase = new FormData();
    fdlDynamicBase.left = new FormAttachment(0, -margin);
    fdlDynamicBase.top = new FormAttachment(wTransformName, margin);
    fdlDynamicBase.right = new FormAttachment(middle, -2 * margin);
    wlDynamicBase.setLayoutData(fdlDynamicBase);
    wDynamicBase = new Button(wSearchGroup, SWT.CHECK);
    props.setLook(wDynamicBase);
    wDynamicBase.setToolTipText(BaseMessages.getString(PKG, "LdapInputDialog.dynamicBase.Tooltip"));
    FormData fdDynamicBase = new FormData();
    fdDynamicBase.left = new FormAttachment(middle, -margin);
    fdDynamicBase.top = new FormAttachment(wlDynamicBase, 0, SWT.CENTER);
    wDynamicBase.setLayoutData(fdDynamicBase);
    SelectionAdapter ldynamicBase =
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            ActiveDynamicBase();
            input.setChanged();
          }
        };
    wDynamicBase.addSelectionListener(ldynamicBase);

    // dynamic search base field
    wlSearchBaseField = new Label(wSearchGroup, SWT.RIGHT);
    wlSearchBaseField.setText(
        BaseMessages.getString(PKG, "LdapInputDialog.wsearchBaseField.Label"));
    props.setLook(wlSearchBaseField);
    FormData fdlsearchBaseField = new FormData();
    fdlsearchBaseField.left = new FormAttachment(0, -margin);
    fdlsearchBaseField.top = new FormAttachment(wDynamicBase, margin);
    fdlsearchBaseField.right = new FormAttachment(middle, -2 * margin);
    wlSearchBaseField.setLayoutData(fdlsearchBaseField);

    wSearchBaseField = new CCombo(wSearchGroup, SWT.BORDER | SWT.READ_ONLY);
    wSearchBaseField.setEditable(true);
    props.setLook(wSearchBaseField);
    wSearchBaseField.addModifyListener(lsMod);
    FormData fdsearchBaseField = new FormData();
    fdsearchBaseField.left = new FormAttachment(middle, -margin);
    fdsearchBaseField.top = new FormAttachment(wDynamicBase, margin);
    fdsearchBaseField.right = new FormAttachment(100, -2 * margin);
    wSearchBaseField.setLayoutData(fdsearchBaseField);
    wSearchBaseField.addFocusListener(
        new FocusListener() {
          public void focusLost(org.eclipse.swt.events.FocusEvent e) {}

          public void focusGained(org.eclipse.swt.events.FocusEvent e) {
            setSearchBaseField();
          }
        });

    // SearchBase line
    wlSearchBase = new Label(wSearchGroup, SWT.RIGHT);
    wlSearchBase.setText(BaseMessages.getString(PKG, "LdapInputDialog.SearchBase.Label"));
    props.setLook(wlSearchBase);
    FormData fdlSearchBase = new FormData();
    fdlSearchBase.left = new FormAttachment(0, -margin);
    fdlSearchBase.top = new FormAttachment(wSearchBaseField, margin);
    fdlSearchBase.right = new FormAttachment(middle, -2 * margin);
    wlSearchBase.setLayoutData(fdlSearchBase);
    wSearchBase = new TextVar(variables, wSearchGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSearchBase);
    wSearchBase.setToolTipText(BaseMessages.getString(PKG, "LdapInputDialog.SearchBase.Tooltip"));
    wSearchBase.addModifyListener(lsMod);
    FormData fdSearchBase = new FormData();
    fdSearchBase.left = new FormAttachment(middle, -margin);
    fdSearchBase.top = new FormAttachment(wSearchBaseField, margin);
    fdSearchBase.right = new FormAttachment(100, -2 * margin);
    wSearchBase.setLayoutData(fdSearchBase);

    // Is filter defined in a Field
    Label wlDynamicFilter = new Label(wSearchGroup, SWT.RIGHT);
    wlDynamicFilter.setText(BaseMessages.getString(PKG, "LdapInputDialog.dynamicFilter.Label"));
    props.setLook(wlDynamicFilter);
    FormData fdlDynamicFilter = new FormData();
    fdlDynamicFilter.left = new FormAttachment(0, -margin);
    fdlDynamicFilter.top = new FormAttachment(wSearchBase, margin);
    fdlDynamicFilter.right = new FormAttachment(middle, -2 * margin);
    wlDynamicFilter.setLayoutData(fdlDynamicFilter);
    wDynamicFilter = new Button(wSearchGroup, SWT.CHECK);
    props.setLook(wDynamicFilter);
    wDynamicFilter.setToolTipText(
        BaseMessages.getString(PKG, "LdapInputDialog.dynamicFilter.Tooltip"));
    FormData fdynamicFilter = new FormData();
    fdynamicFilter.left = new FormAttachment(middle, -margin);
    fdynamicFilter.top = new FormAttachment(wlDynamicFilter, 0, SWT.CENTER);
    wDynamicFilter.setLayoutData(fdynamicFilter);
    SelectionAdapter ldynamicFilter =
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent arg0) {
            ActivedynamicFilter();
            input.setChanged();
          }
        };
    wDynamicFilter.addSelectionListener(ldynamicFilter);

    // dynamic search base field
    wlFilterField = new Label(wSearchGroup, SWT.RIGHT);
    wlFilterField.setText(BaseMessages.getString(PKG, "LdapInputDialog.filterField.Label"));
    props.setLook(wlFilterField);
    FormData fdlfilterField = new FormData();
    fdlfilterField.left = new FormAttachment(0, -margin);
    fdlfilterField.top = new FormAttachment(wDynamicFilter, margin);
    fdlfilterField.right = new FormAttachment(middle, -2 * margin);
    wlFilterField.setLayoutData(fdlfilterField);
    wFilterField = new CCombo(wSearchGroup, SWT.BORDER | SWT.READ_ONLY);
    wFilterField.setEditable(true);
    props.setLook(wFilterField);
    wFilterField.addModifyListener(lsMod);
    FormData fdfilterField = new FormData();
    fdfilterField.left = new FormAttachment(middle, -margin);
    fdfilterField.top = new FormAttachment(wDynamicFilter, margin);
    fdfilterField.right = new FormAttachment(100, -2 * margin);
    wFilterField.setLayoutData(fdfilterField);
    wFilterField.addFocusListener(
        new FocusListener() {
          public void focusLost(org.eclipse.swt.events.FocusEvent e) {}

          public void focusGained(org.eclipse.swt.events.FocusEvent e) {
            setSearchBaseField();
          }
        });

    // Filter String
    wlFilterString = new Label(wSearchGroup, SWT.RIGHT);
    wlFilterString.setText(BaseMessages.getString(PKG, "LdapInputDialog.FilterString.Label"));
    props.setLook(wlFilterString);
    FormData fdlFilterString = new FormData();
    fdlFilterString.left = new FormAttachment(0, 0);
    fdlFilterString.top = new FormAttachment(wFilterField, margin);
    fdlFilterString.right = new FormAttachment(middle, -2 * margin);
    wlFilterString.setLayoutData(fdlFilterString);

    wFilterString =
        new StyledTextComp(
            variables,
            wSearchGroup,
            SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL
        );
    wFilterString.setToolTipText(
        BaseMessages.getString(PKG, "LdapInputDialog.FilterString.Tooltip"));
    props.setLook(wFilterString);
    wFilterString.addModifyListener(lsMod);
    FormData fdFilterString = new FormData();
    fdFilterString.left = new FormAttachment(middle, -margin);
    fdFilterString.top = new FormAttachment(wFilterField, margin);
    fdFilterString.right = new FormAttachment(100, -2 * margin);
    fdFilterString.bottom = new FormAttachment(100, -margin);
    wFilterString.setLayoutData(fdFilterString);
    wFilterString.addKeyListener(new ControlSpaceKeyAdapter(variables, wFilterString));

    FormData fdSearchGroup = new FormData();
    fdSearchGroup.left = new FormAttachment(0, margin);
    fdSearchGroup.top = new FormAttachment(wTransformName, margin);
    fdSearchGroup.right = new FormAttachment(100, -margin);
    fdSearchGroup.bottom = new FormAttachment(100, -margin);
    wSearchGroup.setLayoutData(fdSearchGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Search GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdSearchComp = new FormData();
    fdSearchComp.left = new FormAttachment(0, 0);
    fdSearchComp.top = new FormAttachment(0, 0);
    fdSearchComp.right = new FormAttachment(100, 0);
    fdSearchComp.bottom = new FormAttachment(100, 0);
    wSearchComp.setLayoutData(fdSearchComp);

    wSearchComp.layout();
    wSearchTab.setControl(wSearchComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Search TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setText(BaseMessages.getString(PKG, "LdapInputDialog.Content.Tab"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wContentComp);
    wContentComp.setLayout(contentLayout);

    // /////////////////////////////////
    // START OF Additional Fields GROUP
    // /////////////////////////////////

    Group wAdditionalGroup = new Group(wContentComp, SWT.SHADOW_NONE);
    props.setLook(wAdditionalGroup);
    wAdditionalGroup.setText(
        BaseMessages.getString(PKG, "LdapInputDialog.Group.AdditionalGroup.Label"));

    FormLayout additionalgroupLayout = new FormLayout();
    additionalgroupLayout.marginWidth = 10;
    additionalgroupLayout.marginHeight = 10;
    wAdditionalGroup.setLayout(additionalgroupLayout);

    Label wlInclRownum = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclRownum.setText(BaseMessages.getString(PKG, "LdapInputDialog.InclRownum.Label"));
    props.setLook(wlInclRownum);
    FormData fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment(0, 0);
    fdlInclRownum.top = new FormAttachment(0, margin);
    fdlInclRownum.right = new FormAttachment(middle, -margin);
    wlInclRownum.setLayoutData(fdlInclRownum);
    wInclRownum = new Button(wAdditionalGroup, SWT.CHECK);
    props.setLook(wInclRownum);
    wInclRownum.setToolTipText(BaseMessages.getString(PKG, "LdapInputDialog.InclRownum.Tooltip"));
    FormData fdRownum = new FormData();
    fdRownum.left = new FormAttachment(middle, 0);
    fdRownum.top = new FormAttachment(wlInclRownum, 0, SWT.CENTER);
    wInclRownum.setLayoutData(fdRownum);
    wInclRownum.addSelectionListener(new ComponentSelectionListener(input));

    wlInclRownumField = new Label(wAdditionalGroup, SWT.RIGHT);
    wlInclRownumField.setText(BaseMessages.getString(PKG, "LdapInputDialog.InclRownumField.Label"));
    props.setLook(wlInclRownumField);
    FormData fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment(wInclRownum, margin);
    fdlInclRownumField.top = new FormAttachment(0, margin);
    wlInclRownumField.setLayoutData(fdlInclRownumField);
    wInclRownumField =
        new TextVar(variables, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wInclRownumField);
    wInclRownumField.addModifyListener(lsMod);
    FormData fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment(wlInclRownumField, margin);
    fdInclRownumField.top = new FormAttachment(0, margin);
    fdInclRownumField.right = new FormAttachment(100, 0);
    wInclRownumField.setLayoutData(fdInclRownumField);

    FormData fdAdditionalGroup = new FormData();
    fdAdditionalGroup.left = new FormAttachment(0, margin);
    fdAdditionalGroup.top = new FormAttachment(0, margin);
    fdAdditionalGroup.right = new FormAttachment(100, -margin);
    wAdditionalGroup.setLayoutData(fdAdditionalGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF DESTINATION ADDRESS GROUP
    // ///////////////////////////////////////////////////////////

    Label wlLimit = new Label(wContentComp, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "LdapInputDialog.Limit.Label"));
    props.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.top = new FormAttachment(wAdditionalGroup, 2 * margin);
    fdlLimit.right = new FormAttachment(middle, -margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLimit);
    wLimit.addModifyListener(lsMod);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.top = new FormAttachment(wAdditionalGroup, 2 * margin);
    fdLimit.right = new FormAttachment(100, 0);
    wLimit.setLayoutData(fdLimit);

    // TimeLimit
    Label wlTimeLimit = new Label(wContentComp, SWT.RIGHT);
    wlTimeLimit.setText(BaseMessages.getString(PKG, "LdapInputDialog.TimeLimit.Label"));
    props.setLook(wlTimeLimit);
    FormData fdlTimeLimit = new FormData();
    fdlTimeLimit.left = new FormAttachment(0, 0);
    fdlTimeLimit.top = new FormAttachment(wLimit, margin);
    fdlTimeLimit.right = new FormAttachment(middle, -margin);
    wlTimeLimit.setLayoutData(fdlTimeLimit);
    wTimeLimit = new TextVar(variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTimeLimit);
    wTimeLimit.setToolTipText(BaseMessages.getString(PKG, "LdapInputDialog.TimeLimit.Tooltip"));
    wTimeLimit.addModifyListener(lsMod);

    FormData fdTimeLimit = new FormData();
    fdTimeLimit.left = new FormAttachment(middle, 0);
    fdTimeLimit.top = new FormAttachment(wLimit, margin);
    fdTimeLimit.right = new FormAttachment(100, 0);
    wTimeLimit.setLayoutData(fdTimeLimit);

    // Multi valued field separator
    Label wlMultiValuedSeparator = new Label(wContentComp, SWT.RIGHT);
    wlMultiValuedSeparator.setText(
        BaseMessages.getString(PKG, "LdapInputDialog.MultiValuedSeparator.Label"));
    props.setLook(wlMultiValuedSeparator);
    FormData fdlMultiValuedSeparator = new FormData();
    fdlMultiValuedSeparator.left = new FormAttachment(0, 0);
    fdlMultiValuedSeparator.top = new FormAttachment(wTimeLimit, margin);
    fdlMultiValuedSeparator.right = new FormAttachment(middle, -margin);
    wlMultiValuedSeparator.setLayoutData(fdlMultiValuedSeparator);
    wMultiValuedSeparator =
        new TextVar(variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMultiValuedSeparator);
    wMultiValuedSeparator.setToolTipText(
        BaseMessages.getString(PKG, "LdapInputDialog.MultiValuedSeparator.Tooltip"));
    wMultiValuedSeparator.addModifyListener(lsMod);
    FormData fdMultiValuedSeparator = new FormData();
    fdMultiValuedSeparator.left = new FormAttachment(middle, 0);
    fdMultiValuedSeparator.top = new FormAttachment(wTimeLimit, margin);
    fdMultiValuedSeparator.right = new FormAttachment(100, 0);
    wMultiValuedSeparator.setLayoutData(fdMultiValuedSeparator);

    // Use page ranging?
    Label wlSetPaging = new Label(wContentComp, SWT.RIGHT);
    wlSetPaging.setText(BaseMessages.getString(PKG, "LdapInputDialog.setPaging.Label"));
    props.setLook(wlSetPaging);
    FormData fdlSetPaging = new FormData();
    fdlSetPaging.left = new FormAttachment(0, 0);
    fdlSetPaging.top = new FormAttachment(wMultiValuedSeparator, margin);
    fdlSetPaging.right = new FormAttachment(middle, -margin);
    wlSetPaging.setLayoutData(fdlSetPaging);
    wSetPaging = new Button(wContentComp, SWT.CHECK);
    props.setLook(wSetPaging);
    wSetPaging.setToolTipText(BaseMessages.getString(PKG, "LdapInputDialog.setPaging.Tooltip"));
    FormData fdSetPaging = new FormData();
    fdSetPaging.left = new FormAttachment(middle, 0);
    fdSetPaging.top = new FormAttachment(wlSetPaging, 0, SWT.CENTER);
    wSetPaging.setLayoutData(fdSetPaging);
    wSetPaging.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            setPaging();
            input.setChanged();
          }
        });
    wlPageSize = new Label(wContentComp, SWT.RIGHT);
    wlPageSize.setText(BaseMessages.getString(PKG, "LdapInputDialog.PageSize.Label"));
    props.setLook(wlPageSize);
    FormData fdlPageSize = new FormData();
    fdlPageSize.left = new FormAttachment(wSetPaging, margin);
    fdlPageSize.top = new FormAttachment(wMultiValuedSeparator, margin);
    wlPageSize.setLayoutData(fdlPageSize);
    wPageSize = new TextVar(variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPageSize);
    wPageSize.addModifyListener(lsMod);
    FormData fdPageSize = new FormData();
    fdPageSize.left = new FormAttachment(wlPageSize, margin);
    fdPageSize.top = new FormAttachment(wMultiValuedSeparator, margin);
    fdPageSize.right = new FormAttachment(100, 0);
    wPageSize.setLayoutData(fdPageSize);

    // searchScope
    Label wlsearchScope = new Label(wContentComp, SWT.RIGHT);
    wlsearchScope.setText(BaseMessages.getString(PKG, "LdapInputDialog.SearchScope.Label"));
    props.setLook(wlsearchScope);
    FormData fdlsearchScope = new FormData();
    fdlsearchScope.left = new FormAttachment(0, 0);
    fdlsearchScope.right = new FormAttachment(middle, -margin);
    fdlsearchScope.top = new FormAttachment(wPageSize, margin);
    wlsearchScope.setLayoutData(fdlsearchScope);

    wSearchScope = new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY);
    props.setLook(wSearchScope);
    wSearchScope.addModifyListener(lsMod);
    FormData fdsearchScope = new FormData();
    fdsearchScope.left = new FormAttachment(middle, 0);
    fdsearchScope.top = new FormAttachment(wPageSize, margin);
    fdsearchScope.right = new FormAttachment(100, -margin);
    wSearchScope.setLayoutData(fdsearchScope);
    wSearchScope.setItems(LdapInputMeta.searchScopeDesc);
    wSearchScope.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {}
        });

    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment(0, 0);
    fdContentComp.top = new FormAttachment(0, 0);
    fdContentComp.right = new FormAttachment(100, 0);
    fdContentComp.bottom = new FormAttachment(100, 0);
    wContentComp.setLayoutData(fdContentComp);

    wContentComp.layout();
    wContentTab.setControl(wContentComp);

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText(BaseMessages.getString(PKG, "LdapInputDialog.Fields.Tab"));

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    wFieldsComp.setLayout(fieldsLayout);
    props.setLook(wFieldsComp);

    wGet = new Button(wFieldsComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "LdapInputDialog.GetFields.Button"));
    fdGet = new FormData();
    fdGet.left = new FormAttachment(50, 0);
    fdGet.bottom = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);

    final int FieldsRows = input.getInputFields().length;

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.Name.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.Attribute.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.FetchAttributeAs.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              LdapInputField.FetchAttributeAsDesc,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.IsSortedKey.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, "System.Combo.Yes"),
                BaseMessages.getString(PKG, "System.Combo.No")
              },
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.Type.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.Format.Column"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              3),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.Length.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.Precision.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.Currency.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.Decimal.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.Group.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.TrimType.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              LdapInputField.trimTypeDesc,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.Repeat.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, "System.Combo.Yes"),
                BaseMessages.getString(PKG, "System.Combo.No")
              },
              true),
        };

    colinf[0].setUsingVariables(true);
    colinf[0].setToolTip(
        BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.Name.Column.Tooltip"));
    colinf[1].setUsingVariables(true);
    colinf[1].setToolTip(
        BaseMessages.getString(PKG, "LdapInputDialog.FieldsTable.Attribute.Column.Tooltip"));

    wFields =
        new TableView(
            variables,
            wFieldsComp,
            SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wGet, -margin);
    wFields.setLayoutData(fdFields);

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners

    wGet.addListener(SWT.Selection, e -> get());
    wTest.addListener(SWT.Selection, e -> test());

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);
    wLimit.addSelectionListener(lsDef);
    wInclRownumField.addSelectionListener(lsDef);

    // Enable/disable the right fields to allow a row number to be added to each row...
    wInclRownum.addSelectionListener(
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            setIncludeRownum();
          }
        });

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
    setProtocol();
    setTrustStore();
    useAuthentication();
    setPaging();
    ActiveDynamicBase();
    ActivedynamicFilter();
    input.setChanged(changed);

    wFields.optWidth(true);

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

      LdapInputMeta meta = new LdapInputMeta();
      getInfo(meta);

      // Defined a LDAP connection
      connection = new LdapConnection(log, variables, meta, null);

      // connect...
      if (wUsingAuthentication.getSelection()) {
        connection.connect(
            variables.resolve(meta.getUserName()),
            Encr.decryptPasswordOptionallyEncrypted(
                variables.resolve(meta.getPassword())));
      } else {
        connection.connect();
      }
      // We are successfully connected

      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      mb.setMessage(BaseMessages.getString(PKG, "LdapInputDialog.Connected.OK") + Const.CR);
      mb.setText(BaseMessages.getString(PKG, "LdapInputDialog.Connected.Title.Ok"));
      mb.open();

    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "LdapInputDialog.Connected.Title.Error"),
          BaseMessages.getString(PKG, "LdapInputDialog.Connected.NOK"),
          e);
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

  private void get() {
    LdapConnection connection = null;
    try {

      LdapInputMeta meta = new LdapInputMeta();
      getInfo(meta);

      // Clear Fields Grid
      wFields.removeAll();

      // Defined a LDAP connection
      connection = new LdapConnection(log, variables, meta, null);

      // connect ...
      if (meta.isUseAuthentication()) {
        String username = variables.resolve(meta.getUserName());
        String password =
            Encr.decryptPasswordOptionallyEncrypted(
                variables.resolve(meta.getPassword()));
        connection.connect(username, password);
      } else {
        connection.connect();
      }

      // return fields
      RowMeta listattributes =
          connection.getFields(variables.resolve(meta.getSearchBase()));
      String[] fieldsName = new String[listattributes.size()];
      for (int i = 0; i < listattributes.size(); i++) {

        IValueMeta v = listattributes.getValueMeta(i);
        fieldsName[i] = v.getName();
        // Get Column Name
        TableItem item = new TableItem(wFields.table, SWT.NONE);
        item.setText(1, v.getName());
        item.setText(2, v.getName());

        if (LdapInputField.binaryAttributes.contains(v.getName())) {
          item.setText(3, BaseMessages.getString(PKG, "LdapInputField.FetchAttributeAs.Binary"));
        } else {
          item.setText(3, BaseMessages.getString(PKG, "LdapInputField.FetchAttributeAs.String"));
        }
        item.setText(4, BaseMessages.getString(PKG, "System.Combo.No"));
        item.setText(5, v.getTypeDesc());
      }
      colinf[1].setComboValues(fieldsName);
      wFields.removeEmptyRows();
      wFields.setRowNums();
      wFields.optWidth(true);

    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "LdapInputDialog.ErrorGettingColums.DialogTitle"),
          BaseMessages.getString(PKG, "LdapInputDialog.ErrorGettingColums.DialogMessage"),
          e);
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "LdapInputDialog.ErrorGettingColums.DialogTitle"),
          BaseMessages.getString(PKG, "LdapInputDialog.ErrorGettingColums.DialogMessage"),
          e);

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

  private void setPaging() {
    wlPageSize.setEnabled(wSetPaging.getSelection());
    wPageSize.setEnabled(wSetPaging.getSelection());
  }

  public void setIncludeRownum() {
    wlInclRownumField.setEnabled(wInclRownum.getSelection());
    wInclRownumField.setEnabled(wInclRownum.getSelection());
  }

  /**
   * Read the data from the LdapInputMeta object and show it in this dialog.
   *
   * @param in The LdapInputMeta object to obtain the data from.
   */
  public void getData(LdapInputMeta in) {
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

    wInclRownum.setSelection(in.isIncludeRowNumber());
    if (in.getRowNumberField() != null) {
      wInclRownumField.setText(in.getRowNumberField());
    }

    wUsingAuthentication.setSelection(in.isUseAuthentication());
    wSetPaging.setSelection(in.isPaging());
    if (in.getPageSize() != null) {
      wPageSize.setText(in.getPageSize());
    }

    wLimit.setText("" + in.getRowLimit());
    wTimeLimit.setText("" + in.getTimeLimit());
    if (in.getMultiValuedSeparator() != null) {
      wMultiValuedSeparator.setText(in.getMultiValuedSeparator());
    }

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

    if (in.getFilterString() != null) {
      wFilterString.setText(in.getFilterString());
    }
    if (in.getSearchBase() != null) {
      wSearchBase.setText(in.getSearchBase());
    }
    wDynamicBase.setSelection(in.isDynamicSearch());
    if (in.getDynamicSearchFieldName() != null) {
      wSearchBaseField.setText(in.getDynamicSearchFieldName());
    }

    wDynamicFilter.setSelection(in.isDynamicFilter());
    if (in.getDynamicFilterFieldName() != null) {
      wFilterField.setText(in.getDynamicFilterFieldName());
    }
    wSearchScope.setText(LdapInputMeta.getSearchScopeDesc(in.getSearchScope()));
    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "LdapInputDialog.Log.GettingFieldsInfo"));
    }
    for (int i = 0; i < in.getInputFields().length; i++) {
      LdapInputField field = in.getInputFields()[i];

      if (field != null) {
        TableItem item = wFields.table.getItem(i);
        String name = field.getName();
        String path = field.getAttribute();
        String issortedkey =
            field.isSortedKey()
                ? BaseMessages.getString(PKG, "System.Combo.Yes")
                : BaseMessages.getString(PKG, "System.Combo.No");
        String returntype = field.getFetchAttributeAsDesc();
        String type = field.getTypeDesc();
        String format = field.getFormat();
        String length = "" + field.getLength();
        String prec = "" + field.getPrecision();
        String curr = field.getCurrencySymbol();
        String group = field.getGroupSymbol();
        String decim = field.getDecimalSymbol();
        String trim = field.getTrimTypeDesc();
        String rep =
            field.isRepeated()
                ? BaseMessages.getString(PKG, "System.Combo.Yes")
                : BaseMessages.getString(PKG, "System.Combo.No");

        if (name != null) {
          item.setText(1, name);
        }
        if (path != null) {
          item.setText(2, path);
        }
        if (returntype != null) {
          item.setText(3, returntype);
        }
        if (issortedkey != null) {
          item.setText(4, issortedkey);
        }
        if (type != null) {
          item.setText(5, type);
        }
        if (format != null) {
          item.setText(6, format);
        }
        if (length != null && !"-1".equals(length)) {
          item.setText(5, length);
        }
        if (prec != null && !"-1".equals(prec)) {
          item.setText(8, prec);
        }
        if (curr != null) {
          item.setText(9, curr);
        }
        if (decim != null) {
          item.setText(10, decim);
        }
        if (group != null) {
          item.setText(11, group);
        }
        if (trim != null) {
          item.setText(12, trim);
        }
        if (rep != null) {
          item.setText(13, rep);
        }
      }
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);

    setIncludeRownum();

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
          BaseMessages.getString(PKG, "LdapInputDialog.ErrorParsingData.DialogTitle"),
          BaseMessages.getString(PKG, "LdapInputDialog.ErrorParsingData.DialogMessage"),
          e);
    }
    dispose();
  }

  private void getInfo(LdapInputMeta in) throws HopException {
    transformName = wTransformName.getText(); // return value
    in.setProtocol(wProtocol.getText());
    in.setUseCertificate(wSetTrustStore.getSelection());
    in.setTrustStorePath(wTrustStorePath.getText());
    in.setTrustStorePassword(wTrustStorePassword.getText());
    in.setTrustAllCertificates(wTrustAll.getSelection());

    // copy info to TextFileInputMeta class (input)
    in.setRowLimit(Const.toInt(wLimit.getText(), 0));
    in.setTimeLimit(Const.toInt(wTimeLimit.getText(), 0));
    in.setMultiValuedSeparator(wMultiValuedSeparator.getText());
    in.setIncludeRowNumber(wInclRownum.getSelection());
    in.setUseAuthentication(wUsingAuthentication.getSelection());
    in.setPaging(wSetPaging.getSelection());
    in.setPageSize(wPageSize.getText());
    in.setRowNumberField(wInclRownumField.getText());
    in.setHost(wHost.getText());
    in.setUserName(wUserName.getText());
    in.setPassword(wPassword.getText());
    in.setPort(wPort.getText());
    in.setFilterString(wFilterString.getText());
    in.setSearchBase(wSearchBase.getText());
    in.setDynamicSearch(wDynamicBase.getSelection());
    in.setDynamicSearchFieldName(wSearchBaseField.getText());
    in.setDynamicFilter(wDynamicFilter.getSelection());
    in.setDynamicFilterFieldName(wFilterField.getText());

    int nrFields = wFields.nrNonEmpty();

    in.allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      LdapInputField field = new LdapInputField();

      TableItem item = wFields.getNonEmpty(i);

      field.setName(item.getText(1));
      field.setAttribute(item.getText(2));
      field.setFetchAttributeAs(LdapInputField.getFetchAttributeAsByDesc(item.getText(3)));
      field.setSortedKey(
          BaseMessages.getString(PKG, "System.Combo.Yes").equalsIgnoreCase(item.getText(4)));
      field.setType(ValueMetaFactory.getIdForValueMeta(item.getText(5)));
      field.setFormat(item.getText(6));
      field.setLength(Const.toInt(item.getText(7), -1));
      field.setPrecision(Const.toInt(item.getText(8), -1));
      field.setCurrencySymbol(item.getText(9));
      field.setDecimalSymbol(item.getText(10));
      field.setGroupSymbol(item.getText(11));
      field.setTrimType(LdapInputField.getTrimTypeByDesc(item.getText(12)));
      field.setRepeated(
          BaseMessages.getString(PKG, "System.Combo.Yes").equalsIgnoreCase(item.getText(13)));

      // CHECKSTYLE:Indentation:OFF
      in.getInputFields()[i] = field;
    }
    in.setSearchScope(LdapInputMeta.getSearchScopeByDesc(wSearchScope.getText()));
  }

  private void useAuthentication() {
    wUserName.setEnabled(wUsingAuthentication.getSelection());
    wlUserName.setEnabled(wUsingAuthentication.getSelection());
    wPassword.setEnabled(wUsingAuthentication.getSelection());
    wlPassword.setEnabled(wUsingAuthentication.getSelection());
  }

  // Preview the data
  private void preview() {
    try {
      // Create the XML input transform
      LdapInputMeta oneMeta = new LdapInputMeta();
      getInfo(oneMeta);

      PipelineMeta previewMeta =
          PipelinePreviewFactory.generatePreviewPipeline(
            variables, pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

      EnterNumberDialog numberDialog =
          new EnterNumberDialog(
              shell,
              props.getDefaultPreviewSize(),
              BaseMessages.getString(PKG, "LdapInputDialog.NumberRows.DialogTitle"),
              BaseMessages.getString(PKG, "LdapInputDialog.NumberRows.DialogMessage"));
      int previewSize = numberDialog.open();
      if (previewSize > 0) {
        PipelinePreviewProgressDialog progressDialog =
            new PipelinePreviewProgressDialog(
                shell,
              variables,
                previewMeta,
                new String[] {wTransformName.getText()},
                new int[] {previewSize});
        progressDialog.open();

        if (!progressDialog.isCancelled()) {
          Pipeline pipeline = progressDialog.getPipeline();
          String loggingText = progressDialog.getLoggingText();

          if (pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0) {
            EnterTextDialog etd =
                new EnterTextDialog(
                    shell,
                    BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
                    BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"),
                    loggingText,
                    true);
            etd.setReadOnly();
            etd.open();
          }

          PreviewRowsDialog prd =
              new PreviewRowsDialog(
                  shell,
                variables,
                  SWT.NONE,
                  wTransformName.getText(),
                  progressDialog.getPreviewRowsMeta(wTransformName.getText()),
                  progressDialog.getPreviewRows(wTransformName.getText()),
                  loggingText);
          prd.open();
        }
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "LdapInputDialog.ErrorPreviewingData.DialogTitle"),
          BaseMessages.getString(PKG, "LdapInputDialog.ErrorPreviewingData.DialogMessage"),
          e);
    }
  }

  private void ActiveDynamicBase() {
    wSearchBase.setEnabled(!wDynamicBase.getSelection());
    wlSearchBase.setEnabled(!wDynamicBase.getSelection());
    wlSearchBaseField.setEnabled(wDynamicBase.getSelection());
    wSearchBaseField.setEnabled(wDynamicBase.getSelection());
    activatePreview();
    activateGetFields();
  }

  private void ActivedynamicFilter() {
    wlFilterString.setEnabled(!wDynamicFilter.getSelection());
    wFilterString.setEnabled(!wDynamicFilter.getSelection());
    wlFilterField.setEnabled(wDynamicFilter.getSelection());
    wFilterField.setEnabled(wDynamicFilter.getSelection());
    activatePreview();
  }

  private void activatePreview() {
    wPreview.setEnabled(!wDynamicBase.getSelection() && !wDynamicFilter.getSelection());
  }

  private void activateGetFields() {
    wGet.setEnabled(!wDynamicBase.getSelection());
  }

  private void setSearchBaseField() {
    if (!gotPreviousFields) {
      try {
        String basefield = wSearchBaseField.getText();
        String filterfield = wFilterField.getText();
        wSearchBaseField.removeAll();

        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wSearchBaseField.setItems(r.getFieldNames());
          wFilterField.setItems(r.getFieldNames());
        }
        if (basefield != null) {
          wSearchBaseField.setText(basefield);
        }
        if (filterfield != null) {
          wFilterField.setText(basefield);
        }

      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "LdapInputDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "LdapInputDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
      gotPreviousFields = true;
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
