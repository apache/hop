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

package org.apache.hop.pipeline.transforms.mailinput;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.workflow.actions.getpop.MailConnection;
import org.apache.hop.workflow.actions.getpop.MailConnectionMeta;
import org.apache.hop.workflow.actions.getpop.SelectFolderDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.*;
import org.eclipse.swt.widgets.*;

import javax.mail.Folder;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class MailInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = MailInputMeta.class; // For Translator

  private final MailInputMeta input;
  private TextVar wServerName;

  private TextVar wSender;

  private TextVar wReceipient;

  private TextVar wSubject;

  private Label wlUserName;
  private TextVar wUserName;

  private Label wlIMAPFolder;
  private TextVar wIMAPFolder;

  private Label wlPassword;
  private TextVar wPassword;

  private Label wlUseProxy;
  private Button wUseProxy;

  private Label wlProxyUsername;
  private TextVar wProxyUsername;

  private Label wlListmails;
  private CCombo wListmails;

  private Label wlIMAPListmails;
  private CCombo wIMAPListmails;

  private Label wlFirstmails;
  private TextVar wFirstmails;

  private Label wlIMAPFirstmails;
  private TextVar wIMAPFirstmails;

  private Label wlPort;
  private TextVar wPort;

  private Label wlUseSSL;
  private Button wUseSSL;

  private Label wlIncludeSubFolders;
  private Button wIncludeSubFolders;

  private Button wNegateSender;

  private Button wNegateReceipient;

  private Button wNegateSubject;

  private Button wNegateReceivedDate;

  private Label wlPOP3Message;

  private Label wlLimit;
  private Text wLimit;

  private Label wlReadFrom;
  private TextVar wReadFrom;
  private Button open;

  private Label wlConditionOnReceivedDate;
  private CCombo wConditionOnReceivedDate;

  private Label wlReadTo;
  private TextVar wReadTo;
  private Button opento;

  private CCombo wProtocol;

  private Button wTestIMAPFolder;

  private Button wSelectFolder;

  private Label wlFolderField, wlDynamicFolder;
  private CCombo wFolderField;
  private Button wDynamicFolder;

  private TableView wFields;

  private MailConnection mailConn = null;

  private boolean gotPreviousfields = false;

  private Button wUseBatch;
  private Text wBatchSize;
  private TextVar wStartMessage;
  private TextVar wEndMessage;
  private Button wIgnoreFieldErrors;

  public MailInputDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (MailInputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> {
      closeMailConnection();
      input.setChanged();
    };

    SelectionListener lsSelection = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        closeMailConnection();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "MailInputdialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Buttons go at the buttom
    //
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "MailInputDialog.Preview" ) );
    wPreview.addListener( SWT.Selection, e -> preview() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wPreview, wCancel }, margin, null);

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "MailInputdialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "MailInput.Tab.General.Label" ) );
    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);
    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // ////////////////////////
    // START OF SERVER SETTINGS GROUP///
    // /
    Group wServerSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wServerSettings);
    wServerSettings.setText( BaseMessages.getString( PKG, "MailInput.ServerSettings.Group.Label" ) );

    FormLayout ServerSettingsgroupLayout = new FormLayout();
    ServerSettingsgroupLayout.marginWidth = 10;
    ServerSettingsgroupLayout.marginHeight = 10;
    wServerSettings.setLayout( ServerSettingsgroupLayout );

    // ServerName line
    Label wlServerName = new Label(wServerSettings, SWT.RIGHT);
    wlServerName.setText( BaseMessages.getString( PKG, "MailInput.Server.Label" ) );
    props.setLook(wlServerName);
    FormData fdlServerName = new FormData();
    fdlServerName.left = new FormAttachment( 0, 0 );
    fdlServerName.top = new FormAttachment( 0, 2 * margin );
    fdlServerName.right = new FormAttachment( middle, -margin );
    wlServerName.setLayoutData(fdlServerName);
    wServerName = new TextVar( variables, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wServerName );
    wServerName.addModifyListener( lsMod );
    FormData fdServerName = new FormData();
    fdServerName.left = new FormAttachment( middle, 0 );
    fdServerName.top = new FormAttachment( 0, 2 * margin );
    fdServerName.right = new FormAttachment( 100, 0 );
    wServerName.setLayoutData(fdServerName);

    // USE connection with SSL
    wlUseSSL = new Label(wServerSettings, SWT.RIGHT );
    wlUseSSL.setText( BaseMessages.getString( PKG, "MailInput.UseSSLMails.Label" ) );
    props.setLook( wlUseSSL );
    FormData fdlUseSSL = new FormData();
    fdlUseSSL.left = new FormAttachment( 0, 0 );
    fdlUseSSL.top = new FormAttachment( wServerName, margin );
    fdlUseSSL.right = new FormAttachment( middle, -margin );
    wlUseSSL.setLayoutData(fdlUseSSL);
    wUseSSL = new Button(wServerSettings, SWT.CHECK );
    props.setLook( wUseSSL );
    FormData fdUseSSL = new FormData();
    wUseSSL.setToolTipText( BaseMessages.getString( PKG, "MailInput.UseSSLMails.Tooltip" ) );
    fdUseSSL.left = new FormAttachment( middle, 0 );
    fdUseSSL.top = new FormAttachment( wlUseSSL, 0, SWT.CENTER );
    fdUseSSL.right = new FormAttachment( 100, 0 );
    wUseSSL.setLayoutData(fdUseSSL);

    wUseSSL.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        closeMailConnection();
        refreshPort( true );
      }
    } );

    // port
    wlPort = new Label(wServerSettings, SWT.RIGHT );
    wlPort.setText( BaseMessages.getString( PKG, "MailInput.SSLPort.Label" ) );
    props.setLook( wlPort );
    FormData fdlPort = new FormData();
    fdlPort.left = new FormAttachment( 0, 0 );
    fdlPort.top = new FormAttachment( wUseSSL, margin );
    fdlPort.right = new FormAttachment( middle, -margin );
    wlPort.setLayoutData(fdlPort);
    wPort = new TextVar( variables, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPort );
    wPort.setToolTipText( BaseMessages.getString( PKG, "MailInput.SSLPort.Tooltip" ) );
    wPort.addModifyListener( lsMod );
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment( middle, 0 );
    fdPort.top = new FormAttachment( wUseSSL, margin );
    fdPort.right = new FormAttachment( 100, 0 );
    wPort.setLayoutData(fdPort);

    // UserName line
    wlUserName = new Label(wServerSettings, SWT.RIGHT );
    wlUserName.setText( BaseMessages.getString( PKG, "MailInput.Username.Label" ) );
    props.setLook( wlUserName );
    FormData fdlUserName = new FormData();
    fdlUserName.left = new FormAttachment( 0, 0 );
    fdlUserName.top = new FormAttachment( wPort, margin );
    fdlUserName.right = new FormAttachment( middle, -margin );
    wlUserName.setLayoutData(fdlUserName);
    wUserName = new TextVar( variables, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUserName );
    wUserName.setToolTipText( BaseMessages.getString( PKG, "MailInput.Username.Tooltip" ) );
    wUserName.addModifyListener( lsMod );
    FormData fdUserName = new FormData();
    fdUserName.left = new FormAttachment( middle, 0 );
    fdUserName.top = new FormAttachment( wPort, margin );
    fdUserName.right = new FormAttachment( 100, 0 );
    wUserName.setLayoutData(fdUserName);

    // Password line
    wlPassword = new Label(wServerSettings, SWT.RIGHT );
    wlPassword.setText( BaseMessages.getString( PKG, "MailInput.Password.Label" ) );
    props.setLook( wlPassword );
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment( 0, 0 );
    fdlPassword.top = new FormAttachment( wUserName, margin );
    fdlPassword.right = new FormAttachment( middle, -margin );
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new PasswordTextVar( variables, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPassword );
    wPassword.addModifyListener( lsMod );
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment( middle, 0 );
    fdPassword.top = new FormAttachment( wUserName, margin );
    fdPassword.right = new FormAttachment( 100, 0 );
    wPassword.setLayoutData(fdPassword);

    // USE proxy
    wlUseProxy = new Label(wServerSettings, SWT.RIGHT );
    wlUseProxy.setText( BaseMessages.getString( PKG, "MailInput.UseProxyMails.Label" ) );
    props.setLook( wlUseProxy );
    FormData fdlUseProxy = new FormData();
    fdlUseProxy.left = new FormAttachment( 0, 0 );
    fdlUseProxy.top = new FormAttachment( wPassword, 2 * margin );
    fdlUseProxy.right = new FormAttachment( middle, -margin );
    wlUseProxy.setLayoutData(fdlUseProxy);
    wUseProxy = new Button(wServerSettings, SWT.CHECK );
    props.setLook( wUseProxy );
    FormData fdUseProxy = new FormData();
    wUseProxy.setToolTipText( BaseMessages.getString( PKG, "MailInput.UseProxyMails.Tooltip" ) );
    fdUseProxy.left = new FormAttachment( middle, 0 );
    fdUseProxy.top = new FormAttachment( wlUseProxy, 0, SWT.CENTER );
    fdUseProxy.right = new FormAttachment( 100, 0 );
    wUseProxy.setLayoutData(fdUseProxy);

    wUseProxy.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setUserProxy();
        input.setChanged();
      }
    } );

    // ProxyUsername line
    wlProxyUsername = new Label(wServerSettings, SWT.RIGHT );
    wlProxyUsername.setText( BaseMessages.getString( PKG, "MailInput.ProxyUsername.Label" ) );
    wProxyUsername = new TextVar( variables, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wProxyUsername.setToolTipText( BaseMessages.getString( PKG, "MailInput.ProxyUsername.Tooltip" ) );
    wProxyUsername.addModifyListener( lsMod );
    addLabelInputPairBelow( wlProxyUsername, wProxyUsername, wUseProxy );

    // Use Batch label/checkbox
    Label wlUseBatch = new Label(wServerSettings, SWT.RIGHT );
    wlUseBatch.setText( BaseMessages.getString( PKG, "MailInputDialog.UseBatch.Label" ) );
    wUseBatch = new Button(wServerSettings, SWT.CHECK );
    wUseBatch.setToolTipText( BaseMessages.getString( PKG, "MailInputDialog.UseBatch.Tooltip" ) );
    wUseBatch.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setBatchSettingsEnabled();
      }
    } );
    addLabelInputPairBelow( wlUseBatch, wUseBatch, wProxyUsername );
    // ignore field errors
    Label wlIgnoreFieldErrors = new Label(wServerSettings, SWT.RIGHT);
    wlIgnoreFieldErrors.setText( BaseMessages.getString( PKG, "MailInput.IgnoreFieldErrors.Label" ) );
    wIgnoreFieldErrors = new Button(wServerSettings, SWT.CHECK );
    wIgnoreFieldErrors.setToolTipText( BaseMessages.getString( PKG, "MailInput.IgnoreFieldErrors.Tooltip" ) );
    addLabelInputPairBelow(wlIgnoreFieldErrors, wIgnoreFieldErrors, wUseBatch );

    // Protocol
    Label wlProtocol = new Label(wServerSettings, SWT.RIGHT);
    wlProtocol.setText( BaseMessages.getString( PKG, "MailInput.Protocol.Label" ) );
    wProtocol = new CCombo(wServerSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wProtocol.setItems( MailConnectionMeta.protocolCodes );
    wProtocol.select( 0 );
    wProtocol.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        refreshProtocol( true );

      }
    } );
    addLabelInputPairBelow(wlProtocol, wProtocol, wIgnoreFieldErrors );

    // Test connection button
    Button wTest = new Button(wServerSettings, SWT.PUSH);
    wTest.setText( BaseMessages.getString( PKG, "MailInput.TestConnection.Label" ) );
    props.setLook(wTest);
    FormData fdTest = new FormData();
    wTest.setToolTipText( BaseMessages.getString( PKG, "MailInput.TestConnection.Tooltip" ) );
    fdTest.top = new FormAttachment( wProtocol, margin );
    fdTest.right = new FormAttachment( 100, 0 );
    wTest.setLayoutData(fdTest);

    FormData fdServerSettings = new FormData();
    fdServerSettings.left = new FormAttachment( 0, margin );
    fdServerSettings.top = new FormAttachment( wProtocol, margin );
    fdServerSettings.right = new FormAttachment( 100, -margin );
    wServerSettings.setLayoutData(fdServerSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF SERVER SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( wTransformName, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    props.setLook(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF SETTINGS TAB ///
    // ////////////////////////

    CTabItem wSettingsTab = new CTabItem(wTabFolder, SWT.NONE);
    wSettingsTab.setText( BaseMessages.getString( PKG, "MailInput.Tab.Pop.Label" ) );
    Composite wSettingsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wSettingsComp);
    FormLayout PopLayout = new FormLayout();
    PopLayout.marginWidth = 3;
    PopLayout.marginHeight = 3;
    wSettingsComp.setLayout( PopLayout );

    // Message: for POP3, only INBOX folder is available!
    wlPOP3Message = new Label(wSettingsComp, SWT.RIGHT );
    wlPOP3Message.setText( BaseMessages.getString( PKG, "MailInput.POP3Message.Label" ) );
    props.setLook( wlPOP3Message );
    FormData fdlPOP3Message = new FormData();
    fdlPOP3Message.left = new FormAttachment( 0, margin );
    fdlPOP3Message.top = new FormAttachment( 0, 3 * margin );
    wlPOP3Message.setLayoutData(fdlPOP3Message);
    wlPOP3Message.setForeground( GuiResource.getInstance().getColorOrange() );

    // ////////////////////////
    // START OF POP3 Settings GROUP///
    // /
    Group wPOP3Settings = new Group(wSettingsComp, SWT.SHADOW_NONE);
    props.setLook(wPOP3Settings);
    wPOP3Settings.setText( BaseMessages.getString( PKG, "MailInput.POP3Settings.Group.Label" ) );

    FormLayout POP3SettingsgroupLayout = new FormLayout();
    POP3SettingsgroupLayout.marginWidth = 10;
    POP3SettingsgroupLayout.marginHeight = 10;
    wPOP3Settings.setLayout( POP3SettingsgroupLayout );

    // List of mails of retrieve
    wlListmails = new Label(wPOP3Settings, SWT.RIGHT );
    wlListmails.setText( BaseMessages.getString( PKG, "MailInput.Listmails.Label" ) );
    props.setLook( wlListmails );
    FormData fdlListmails = new FormData();
    fdlListmails.left = new FormAttachment( 0, 0 );
    fdlListmails.right = new FormAttachment( middle, 0 );
    fdlListmails.top = new FormAttachment( wlPOP3Message, 2 * margin );
    wlListmails.setLayoutData(fdlListmails);
    wListmails = new CCombo(wPOP3Settings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wListmails.add( BaseMessages.getString( PKG, "MailInput.RetrieveAllMails.Label" ) );
    // [PDI-7241] pop3 does not support retrive unread option
    // wListmails.add( BaseMessages.getString( PKG, "MailInput.RetrieveUnreadMails.Label" ) );
    wListmails.add( BaseMessages.getString( PKG, "MailInput.RetrieveFirstMails.Label" ) );
    wListmails.select( 0 ); // +1: starts at -1

    props.setLook( wListmails );
    FormData fdListmails = new FormData();
    fdListmails.left = new FormAttachment( middle, 0 );
    fdListmails.top = new FormAttachment( wlPOP3Message, 2 * margin );
    fdListmails.right = new FormAttachment( 100, 0 );
    wListmails.setLayoutData(fdListmails);

    wListmails.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        chooseListMails();

      }
    } );

    // Retrieve the first ... mails
    wlFirstmails = new Label(wPOP3Settings, SWT.RIGHT );
    wlFirstmails.setText( BaseMessages.getString( PKG, "MailInput.Firstmails.Label" ) );
    props.setLook( wlFirstmails );
    FormData fdlFirstmails = new FormData();
    fdlFirstmails.left = new FormAttachment( 0, 0 );
    fdlFirstmails.right = new FormAttachment( middle, -margin );
    fdlFirstmails.top = new FormAttachment( wListmails, margin );
    wlFirstmails.setLayoutData(fdlFirstmails);

    wFirstmails = new TextVar( variables, wPOP3Settings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFirstmails );
    wFirstmails.addModifyListener( lsMod );
    FormData fdFirstmails = new FormData();
    fdFirstmails.left = new FormAttachment( middle, 0 );
    fdFirstmails.top = new FormAttachment( wListmails, margin );
    fdFirstmails.right = new FormAttachment( 100, 0 );
    wFirstmails.setLayoutData(fdFirstmails);

    FormData fdPOP3Settings = new FormData();
    fdPOP3Settings.left = new FormAttachment( 0, margin );
    fdPOP3Settings.top = new FormAttachment( wlPOP3Message, 2 * margin );
    fdPOP3Settings.right = new FormAttachment( 100, -margin );
    wPOP3Settings.setLayoutData(fdPOP3Settings);
    // ///////////////////////////////////////////////////////////
    // / END OF POP3 SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF IMAP Settings GROUP///
    // /
    Group wIMAPSettings = new Group(wSettingsComp, SWT.SHADOW_NONE);
    props.setLook(wIMAPSettings);
    wIMAPSettings.setText( BaseMessages.getString( PKG, "MailInput.IMAPSettings.Groupp.Label" ) );

    FormLayout IMAPSettingsgroupLayout = new FormLayout();
    IMAPSettingsgroupLayout.marginWidth = 10;
    IMAPSettingsgroupLayout.marginHeight = 10;
    wIMAPSettings.setLayout( IMAPSettingsgroupLayout );

    // Is folder name defined in a Field
    wlDynamicFolder = new Label(wIMAPSettings, SWT.RIGHT );
    wlDynamicFolder.setText( BaseMessages.getString( PKG, "MailInput.dynamicFolder.Label" ) );
    props.setLook( wlDynamicFolder );
    FormData fdldynamicFolder = new FormData();
    fdldynamicFolder.left = new FormAttachment( 0, 0 );
    fdldynamicFolder.top = new FormAttachment( 0, margin );
    fdldynamicFolder.right = new FormAttachment( middle, -margin );
    wlDynamicFolder.setLayoutData(fdldynamicFolder);

    wDynamicFolder = new Button(wIMAPSettings, SWT.CHECK );
    props.setLook( wDynamicFolder );
    wDynamicFolder.setToolTipText( BaseMessages.getString( PKG, "MailInput.dynamicFolder.Tooltip" ) );
    FormData fddynamicFolder = new FormData();
    fddynamicFolder.left = new FormAttachment( middle, 0 );
    fddynamicFolder.top = new FormAttachment( wlDynamicFolder, 0, SWT.CENTER );
    wDynamicFolder.setLayoutData(fddynamicFolder);
    SelectionAdapter lsxmlstream = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        activedynamicFolder();
        input.setChanged();
      }
    };
    wDynamicFolder.addSelectionListener( lsxmlstream );

    // Folder field
    wlFolderField = new Label(wIMAPSettings, SWT.RIGHT );
    wlFolderField.setText( BaseMessages.getString( PKG, "MailInput.wlFolderField.Label" ) );
    props.setLook( wlFolderField );
    FormData fdlFolderField = new FormData();
    fdlFolderField.left = new FormAttachment( 0, 0 );
    fdlFolderField.top = new FormAttachment( wDynamicFolder, margin );
    fdlFolderField.right = new FormAttachment( middle, -margin );
    wlFolderField.setLayoutData(fdlFolderField);

    wFolderField = new CCombo(wIMAPSettings, SWT.BORDER | SWT.READ_ONLY );
    wFolderField.setEditable( true );
    props.setLook( wFolderField );
    wFolderField.addModifyListener( lsMod );
    FormData fdFolderField = new FormData();
    fdFolderField.left = new FormAttachment( middle, 0 );
    fdFolderField.top = new FormAttachment( wDynamicFolder, margin );
    fdFolderField.right = new FormAttachment( 100, -margin );
    wFolderField.setLayoutData(fdFolderField);
    wFolderField.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        setFolderField();
      }
    } );

    // SelectFolder button
    wSelectFolder = new Button(wIMAPSettings, SWT.PUSH );
    wSelectFolder.setImage( GuiResource.getInstance().getImageBol() );
    wSelectFolder.setToolTipText( BaseMessages.getString( PKG, "MailInput.SelectFolderConnection.Label" ) );
    props.setLook( wSelectFolder );
    FormData fdSelectFolder = new FormData();
    wSelectFolder.setToolTipText( BaseMessages.getString( PKG, "MailInput.SelectFolderConnection.Tooltip" ) );
    fdSelectFolder.top = new FormAttachment( wFolderField, margin );
    fdSelectFolder.right = new FormAttachment( 100, 0 );
    wSelectFolder.setLayoutData(fdSelectFolder);

    // TestIMAPFolder button
    wTestIMAPFolder = new Button(wIMAPSettings, SWT.PUSH );
    wTestIMAPFolder.setText( BaseMessages.getString( PKG, "MailInput.TestIMAPFolderConnection.Label" ) );
    props.setLook( wTestIMAPFolder );
    FormData fdTestIMAPFolder = new FormData();
    wTestIMAPFolder.setToolTipText( BaseMessages.getString( PKG, "MailInput.TestIMAPFolderConnection.Tooltip" ) );
    fdTestIMAPFolder.top = new FormAttachment( wFolderField, margin );
    fdTestIMAPFolder.right = new FormAttachment( wSelectFolder, -margin );
    wTestIMAPFolder.setLayoutData(fdTestIMAPFolder);

    // IMAPFolder line
    wlIMAPFolder = new Label(wIMAPSettings, SWT.RIGHT );
    wlIMAPFolder.setText( BaseMessages.getString( PKG, "MailInput.IMAPFolder.Label" ) );
    props.setLook( wlIMAPFolder );
    FormData fdlIMAPFolder = new FormData();
    fdlIMAPFolder.left = new FormAttachment( 0, 0 );
    fdlIMAPFolder.top = new FormAttachment( wFolderField, margin );
    fdlIMAPFolder.right = new FormAttachment( middle, -margin );
    wlIMAPFolder.setLayoutData(fdlIMAPFolder);
    wIMAPFolder = new TextVar( variables, wIMAPSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wIMAPFolder );
    wIMAPFolder.setToolTipText( BaseMessages.getString( PKG, "MailInput.IMAPFolder.Tooltip" ) );
    wIMAPFolder.addModifyListener( lsMod );
    FormData fdIMAPFolder = new FormData();
    fdIMAPFolder.left = new FormAttachment( middle, 0 );
    fdIMAPFolder.top = new FormAttachment( wFolderField, margin );
    fdIMAPFolder.right = new FormAttachment( wTestIMAPFolder, -margin );
    wIMAPFolder.setLayoutData(fdIMAPFolder);

    // Include subfolders?
    wlIncludeSubFolders = new Label(wIMAPSettings, SWT.RIGHT );
    wlIncludeSubFolders.setText( BaseMessages.getString( PKG, "MailInput.IncludeSubFoldersMails.Label" ) );
    props.setLook( wlIncludeSubFolders );
    FormData fdlIncludeSubFolders = new FormData();
    fdlIncludeSubFolders.left = new FormAttachment( 0, 0 );
    fdlIncludeSubFolders.top = new FormAttachment( wIMAPFolder, margin );
    fdlIncludeSubFolders.right = new FormAttachment( middle, -margin );
    wlIncludeSubFolders.setLayoutData(fdlIncludeSubFolders);
    wIncludeSubFolders = new Button(wIMAPSettings, SWT.CHECK );
    props.setLook( wIncludeSubFolders );
    FormData fdIncludeSubFolders = new FormData();
    wIncludeSubFolders.setToolTipText( BaseMessages.getString( PKG, "MailInput.IncludeSubFoldersMails.Tooltip" ) );
    fdIncludeSubFolders.left = new FormAttachment( middle, 0 );
    fdIncludeSubFolders.top = new FormAttachment( wlIncludeSubFolders, 0, SWT.CENTER );
    fdIncludeSubFolders.right = new FormAttachment( 100, 0 );
    wIncludeSubFolders.setLayoutData(fdIncludeSubFolders);
    wIncludeSubFolders.addSelectionListener( lsSelection );

    // List of mails of retrieve
    wlIMAPListmails = new Label(wIMAPSettings, SWT.RIGHT );
    wlIMAPListmails.setText( BaseMessages.getString( PKG, "MailInput.IMAPListmails.Label" ) );
    props.setLook( wlIMAPListmails );
    FormData fdlIMAPListmails = new FormData();
    fdlIMAPListmails.left = new FormAttachment( 0, 0 );
    fdlIMAPListmails.right = new FormAttachment( middle, -margin );
    fdlIMAPListmails.top = new FormAttachment( wIncludeSubFolders, margin );
    wlIMAPListmails.setLayoutData(fdlIMAPListmails);
    wIMAPListmails = new CCombo(wIMAPSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wIMAPListmails.setItems( MailConnectionMeta.valueIMAPListDesc );
    wIMAPListmails.select( 0 ); // +1: starts at -1

    props.setLook( wIMAPListmails );
    FormData fdIMAPListmails = new FormData();
    fdIMAPListmails.left = new FormAttachment( middle, 0 );
    fdIMAPListmails.top = new FormAttachment( wIncludeSubFolders, margin );
    fdIMAPListmails.right = new FormAttachment( 100, 0 );
    wIMAPListmails.setLayoutData(fdIMAPListmails);

    wIMAPListmails.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        // ChooseIMAPListmails();

      }
    } );

    // Retrieve the first ... mails
    wlIMAPFirstmails = new Label(wIMAPSettings, SWT.RIGHT );
    wlIMAPFirstmails.setText( BaseMessages.getString( PKG, "MailInput.IMAPFirstmails.Label" ) );
    props.setLook( wlIMAPFirstmails );
    FormData fdlIMAPFirstmails = new FormData();
    fdlIMAPFirstmails.left = new FormAttachment( 0, 0 );
    fdlIMAPFirstmails.right = new FormAttachment( middle, -margin );
    fdlIMAPFirstmails.top = new FormAttachment( wIMAPListmails, margin );
    wlIMAPFirstmails.setLayoutData(fdlIMAPFirstmails);

    wIMAPFirstmails = new TextVar( variables, wIMAPSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wIMAPFirstmails );
    wIMAPFirstmails.addModifyListener( lsMod );
    FormData fdIMAPFirstmails = new FormData();
    fdIMAPFirstmails.left = new FormAttachment( middle, 0 );
    fdIMAPFirstmails.top = new FormAttachment( wIMAPListmails, margin );
    fdIMAPFirstmails.right = new FormAttachment( 100, 0 );
    wIMAPFirstmails.setLayoutData(fdIMAPFirstmails);

    FormData fdIMAPSettings = new FormData();
    fdIMAPSettings.left = new FormAttachment( 0, margin );
    fdIMAPSettings.top = new FormAttachment(wPOP3Settings, 2 * margin );
    fdIMAPSettings.right = new FormAttachment( 100, -margin );
    wIMAPSettings.setLayoutData(fdIMAPSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF IMAP SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////////////
    // START OF Batch Settings GROUP///
    //
    Group wBatchSettingsGroup = createGroup(wSettingsComp, wIMAPSettings, BaseMessages.getString(
            PKG, "MailInputDialog.BatchSettingsGroup.Label"));

    // Batch size
    Label wlBatchSize = new Label(wBatchSettingsGroup, SWT.RIGHT );
    wlBatchSize.setText( BaseMessages.getString( PKG, "MailInputDialog.BatchSize.Label" ) );
    wBatchSize = new Text(wBatchSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    addLabelInputPairBelow( wlBatchSize, wBatchSize, wBatchSettingsGroup);

    // Starting message
    Label wlStartMessage = new Label(wBatchSettingsGroup, SWT.RIGHT );
    wlStartMessage.setText( BaseMessages.getString( PKG, "MailInputDialog.StartMessage.Label" ) );
    wStartMessage = new TextVar( variables, wBatchSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    addLabelInputPairBelow( wlStartMessage, wStartMessage, wBatchSize );

    // Last message
    Label wlEndMessage = new Label(wBatchSettingsGroup, SWT.RIGHT );
    wlEndMessage.setText( BaseMessages.getString( PKG, "MailInputDialog.EndMessage.Label" ) );
    wEndMessage = new TextVar( variables, wBatchSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    addLabelInputPairBelow( wlEndMessage, wEndMessage, wStartMessage );

    //
    // / END OF Batch Settings GROUP
    // ///////////////////////////////

    FormData fdSettingsComp = new FormData();
    fdSettingsComp.left = new FormAttachment( 0, 0 );
    fdSettingsComp.top = new FormAttachment( wTransformName, 0 );
    fdSettingsComp.right = new FormAttachment( 100, 0 );
    fdSettingsComp.bottom = new FormAttachment( 100, 0 );
    wSettingsComp.setLayoutData(fdSettingsComp);

    wSettingsComp.layout();
    wSettingsTab.setControl(wSettingsComp);
    props.setLook(wSettingsComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Pop TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF SEARCH TAB ///
    // ////////////////////////

    CTabItem wSearchTab = new CTabItem(wTabFolder, SWT.NONE);
    wSearchTab.setText( BaseMessages.getString( PKG, "MailInput.Tab.Search.Label" ) );
    Composite wSearchComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wSearchComp);
    FormLayout searchLayout = new FormLayout();
    searchLayout.marginWidth = 3;
    searchLayout.marginHeight = 3;
    wSearchComp.setLayout( searchLayout );

    // ////////////////////////
    // START OF HEADER ROUP///
    // /
    Group wHeader = new Group(wSearchComp, SWT.SHADOW_NONE);
    props.setLook(wHeader);
    wHeader.setText( BaseMessages.getString( PKG, "MailInput.Header.Group.Label" ) );

    FormLayout HeadergroupLayout = new FormLayout();
    HeadergroupLayout.marginWidth = 10;
    HeadergroupLayout.marginHeight = 10;
    wHeader.setLayout( HeadergroupLayout );



    // From line
    Label wlSender = new Label(wHeader, SWT.RIGHT);
    wlSender.setText( BaseMessages.getString( PKG, "MailInput.wSender.Label" ) );
    props.setLook(wlSender);
    FormData fdlSender = new FormData();
    fdlSender.left = new FormAttachment( 0, 0 );
    fdlSender.top = new FormAttachment( 0, margin );
    fdlSender.right = new FormAttachment( middle, -margin );
    wlSender.setLayoutData(fdlSender);
    wNegateSender = new Button(wHeader, SWT.CHECK );
    props.setLook( wNegateSender );
    FormData fdNegateSender = new FormData();
    wNegateSender.setToolTipText( BaseMessages.getString( PKG, "MailInput.NegateSender.Tooltip" ) );
    fdNegateSender.top = new FormAttachment( wlSender, 0, SWT.CENTER );
    fdNegateSender.right = new FormAttachment( 100, -margin );
    wNegateSender.setLayoutData(fdNegateSender);
    wSender = new TextVar( variables, wHeader, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSender );
    wSender.addModifyListener( lsMod );
    FormData fdSender = new FormData();
    fdSender.left = new FormAttachment( middle, 0 );
    fdSender.top = new FormAttachment( wlSender, 0, SWT.CENTER );
    fdSender.right = new FormAttachment( wNegateSender, -margin );
    wSender.setLayoutData(fdSender);


    // Receipient line
    Label wlReceipient = new Label(wHeader, SWT.RIGHT);
    wlReceipient.setText( BaseMessages.getString( PKG, "MailInput.Receipient.Label" ) );
    props.setLook(wlReceipient);
    FormData fdlReceipient = new FormData();
    fdlReceipient.left = new FormAttachment( 0, 0 );
    fdlReceipient.top = new FormAttachment( wSender, margin );
    fdlReceipient.right = new FormAttachment( middle, -margin );
    wlReceipient.setLayoutData(fdlReceipient);
    wNegateReceipient = new Button(wHeader, SWT.CHECK );
    props.setLook( wNegateReceipient );
    FormData fdNegateReceipient = new FormData();
    wNegateReceipient.setToolTipText( BaseMessages.getString( PKG, "MailInput.NegateReceipient.Tooltip" ) );
    fdNegateReceipient.top = new FormAttachment( wlReceipient, 0, SWT.CENTER );
    fdNegateReceipient.right = new FormAttachment( 100, -margin );
    wNegateReceipient.setLayoutData(fdNegateReceipient);
    wReceipient = new TextVar( variables, wHeader, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wReceipient );
    wReceipient.addModifyListener( lsMod );
    FormData fdReceipient = new FormData();
    fdReceipient.left = new FormAttachment( middle, 0 );
    fdReceipient.top = new FormAttachment( wlReceipient, 0, SWT.CENTER );
    fdReceipient.right = new FormAttachment( wNegateReceipient, -margin );
    wReceipient.setLayoutData(fdReceipient);


    // Subject line
    Label wlSubject = new Label(wHeader, SWT.RIGHT);
    wlSubject.setText( BaseMessages.getString( PKG, "MailInput.Subject.Label" ) );
    props.setLook(wlSubject);
    FormData fdlSubject = new FormData();
    fdlSubject.left = new FormAttachment( 0, 0 );
    fdlSubject.top = new FormAttachment( wReceipient, margin );
    fdlSubject.right = new FormAttachment( middle, -margin );
    wlSubject.setLayoutData(fdlSubject);
    wNegateSubject = new Button(wHeader, SWT.CHECK );
    props.setLook( wNegateSubject );
    FormData fdNegateSubject = new FormData();
    wNegateSubject.setToolTipText( BaseMessages.getString( PKG, "MailInput.NegateSubject.Tooltip" ) );
    fdNegateSubject.top = new FormAttachment( wlSubject, 0, SWT.CENTER );
    fdNegateSubject.right = new FormAttachment( 100, -margin );
    wNegateSubject.setLayoutData(fdNegateSubject);
    wSubject = new TextVar( variables, wHeader, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSubject );
    wSubject.addModifyListener( lsMod );
    FormData fdSubject = new FormData();
    fdSubject.left = new FormAttachment( middle, 0 );
    fdSubject.top = new FormAttachment( wlSubject, 0, SWT.CENTER );
    fdSubject.right = new FormAttachment( wNegateSubject, -margin );
    wSubject.setLayoutData(fdSubject);


    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment( 0, margin );
    fdHeader.top = new FormAttachment( wReceipient, 2 * margin );
    fdHeader.right = new FormAttachment( 100, -margin );
    wHeader.setLayoutData(fdHeader);
    // ///////////////////////////////////////////////////////////
    // / END OF HEADER GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF RECEIVED DATE ROUP///
    // /
    Group wReceivedDate = new Group(wSearchComp, SWT.SHADOW_NONE);
    props.setLook(wReceivedDate);
    wReceivedDate.setText( BaseMessages.getString( PKG, "MailInput.ReceivedDate.Group.Label" ) );

    FormLayout ReceivedDategroupLayout = new FormLayout();
    ReceivedDategroupLayout.marginWidth = 10;
    ReceivedDategroupLayout.marginHeight = 10;
    wReceivedDate.setLayout( ReceivedDategroupLayout );

    wNegateReceivedDate = new Button(wReceivedDate, SWT.CHECK );
    props.setLook( wNegateReceivedDate );
    FormData fdNegateReceivedDate = new FormData();
    wNegateReceivedDate.setToolTipText( BaseMessages.getString( PKG, "MailInput.NegateReceivedDate.Tooltip" ) );
    fdNegateReceivedDate.top = new FormAttachment(wHeader, margin );
    fdNegateReceivedDate.right = new FormAttachment( 100, -margin );
    wNegateReceivedDate.setLayoutData(fdNegateReceivedDate);

    // Received Date Condition
    wlConditionOnReceivedDate = new Label(wReceivedDate, SWT.RIGHT );
    wlConditionOnReceivedDate.setText( BaseMessages.getString( PKG, "MailInput.ConditionOnReceivedDate.Label" ) );
    props.setLook( wlConditionOnReceivedDate );
    FormData fdlConditionOnReceivedDate = new FormData();
    fdlConditionOnReceivedDate.left = new FormAttachment( 0, 0 );
    fdlConditionOnReceivedDate.right = new FormAttachment( middle, -margin );
    fdlConditionOnReceivedDate.top = new FormAttachment(wHeader, margin );
    wlConditionOnReceivedDate.setLayoutData(fdlConditionOnReceivedDate);

    wConditionOnReceivedDate = new CCombo(wReceivedDate, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wConditionOnReceivedDate.setItems( MailConnectionMeta.conditionDateDesc );
    wConditionOnReceivedDate.select( 0 ); // +1: starts at -1

    props.setLook( wConditionOnReceivedDate );
    FormData fdConditionOnReceivedDate = new FormData();
    fdConditionOnReceivedDate.left = new FormAttachment( middle, 0 );
    fdConditionOnReceivedDate.top = new FormAttachment(wHeader, margin );
    fdConditionOnReceivedDate.right = new FormAttachment( wNegateReceivedDate, -margin );
    wConditionOnReceivedDate.setLayoutData(fdConditionOnReceivedDate);
    wConditionOnReceivedDate.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        conditionReceivedDate();
        input.setChanged();
      }
    } );

    open = new Button(wReceivedDate, SWT.PUSH );
    open.setImage( GuiResource.getInstance().getImageCalendar() );
    open.setToolTipText( BaseMessages.getString( PKG, "MailInput.OpenCalendar" ) );
    FormData fdlButton = new FormData();
    fdlButton.top = new FormAttachment( wConditionOnReceivedDate, margin );
    fdlButton.right = new FormAttachment( 100, 0 );
    open.setLayoutData( fdlButton );
    open.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        final Shell dialog = new Shell( shell, SWT.DIALOG_TRIM );
        dialog.setText( BaseMessages.getString( PKG, "MailInput.SelectDate" ) );
        dialog.setImage( GuiResource.getInstance().getImageHopUi() );
        dialog.setLayout( new GridLayout( 3, false ) );

        final DateTime calendar = new DateTime( dialog, SWT.CALENDAR );
        final DateTime time = new DateTime( dialog, SWT.TIME | SWT.TIME );
        new Label( dialog, SWT.NONE );
        new Label( dialog, SWT.NONE );

        Button ok = new Button( dialog, SWT.PUSH );
        ok.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
        ok.setLayoutData( new GridData( SWT.FILL, SWT.CENTER, false, false ) );
        ok.addSelectionListener( new SelectionAdapter() {
          public void widgetSelected( SelectionEvent e ) {
            Calendar cal = Calendar.getInstance();
            cal.set( Calendar.YEAR, calendar.getYear() );
            cal.set( Calendar.MONTH, calendar.getMonth() );
            cal.set( Calendar.DAY_OF_MONTH, calendar.getDay() );

            cal.set( Calendar.HOUR_OF_DAY, time.getHours() );
            cal.set( Calendar.MINUTE, time.getMinutes() );
            cal.set( Calendar.SECOND, time.getSeconds() );

            wReadFrom.setText( new SimpleDateFormat( MailInputMeta.DATE_PATTERN ).format( cal.getTime() ) );

            dialog.close();
          }
        } );
        dialog.setDefaultButton( ok );
        dialog.pack();
        dialog.open();
      }
    } );

    wlReadFrom = new Label(wReceivedDate, SWT.RIGHT );
    wlReadFrom.setText( BaseMessages.getString( PKG, "MailInput.ReadFrom.Label" ) );
    props.setLook( wlReadFrom );
    FormData fdlReadFrom = new FormData();
    fdlReadFrom.left = new FormAttachment( 0, 0 );
    fdlReadFrom.top = new FormAttachment( wConditionOnReceivedDate, margin );
    fdlReadFrom.right = new FormAttachment( middle, -margin );
    wlReadFrom.setLayoutData(fdlReadFrom);
    wReadFrom = new TextVar( variables, wReceivedDate, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wReadFrom.setToolTipText( BaseMessages.getString( PKG, "MailInput.ReadFrom.Tooltip" ) );
    props.setLook( wReadFrom );
    wReadFrom.addModifyListener( lsMod );
    FormData fdReadFrom = new FormData();
    fdReadFrom.left = new FormAttachment( middle, 0 );
    fdReadFrom.top = new FormAttachment( wConditionOnReceivedDate, margin );
    fdReadFrom.right = new FormAttachment( open, -margin );
    wReadFrom.setLayoutData(fdReadFrom);

    opento = new Button(wReceivedDate, SWT.PUSH );
    opento.setImage( GuiResource.getInstance().getImageCalendar() );
    opento.setToolTipText( BaseMessages.getString( PKG, "MailInput.OpenCalendar" ) );
    FormData fdlButtonto = new FormData();
    fdlButtonto.top = new FormAttachment( wReadFrom, 2 * margin );
    fdlButtonto.right = new FormAttachment( 100, 0 );
    opento.setLayoutData( fdlButtonto );
    opento.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        final Shell dialogto = new Shell( shell, SWT.DIALOG_TRIM );
        dialogto.setText( BaseMessages.getString( PKG, "MailInput.SelectDate" ) );
        dialogto.setImage( GuiResource.getInstance().getImageHopUi() );
        dialogto.setLayout( new GridLayout( 3, false ) );

        final DateTime calendarto = new DateTime( dialogto, SWT.CALENDAR | SWT.BORDER );
        final DateTime timeto = new DateTime( dialogto, SWT.TIME | SWT.TIME );
        new Label( dialogto, SWT.NONE );
        new Label( dialogto, SWT.NONE );
        Button okto = new Button( dialogto, SWT.PUSH );
        okto.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
        okto.setLayoutData( new GridData( SWT.FILL, SWT.CENTER, false, false ) );
        okto.addSelectionListener( new SelectionAdapter() {
          public void widgetSelected( SelectionEvent e ) {
            Calendar cal = Calendar.getInstance();
            cal.set( Calendar.YEAR, calendarto.getYear() );
            cal.set( Calendar.MONTH, calendarto.getMonth() );
            cal.set( Calendar.DAY_OF_MONTH, calendarto.getDay() );

            cal.set( Calendar.HOUR_OF_DAY, timeto.getHours() );
            cal.set( Calendar.MINUTE, timeto.getMinutes() );
            cal.set( Calendar.SECOND, timeto.getSeconds() );

            wReadTo.setText( new SimpleDateFormat( MailInputMeta.DATE_PATTERN ).format( cal.getTime() ) );

            dialogto.close();
          }
        } );
        dialogto.setDefaultButton( okto );
        dialogto.pack();
        dialogto.open();
      }
    } );

    wlReadTo = new Label(wReceivedDate, SWT.RIGHT );
    wlReadTo.setText( BaseMessages.getString( PKG, "MailInput.ReadTo.Label" ) );
    props.setLook( wlReadTo );
    FormData fdlReadTo = new FormData();
    fdlReadTo.left = new FormAttachment( 0, 0 );
    fdlReadTo.top = new FormAttachment( wReadFrom, 2 * margin );
    fdlReadTo.right = new FormAttachment( middle, -margin );
    wlReadTo.setLayoutData(fdlReadTo);
    wReadTo = new TextVar( variables, wReceivedDate, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wReadTo.setToolTipText( BaseMessages.getString( PKG, "MailInput.ReadTo.Tooltip" ) );
    props.setLook( wReadTo );
    wReadTo.addModifyListener( lsMod );
    FormData fdReadTo = new FormData();
    fdReadTo.left = new FormAttachment( middle, 0 );
    fdReadTo.top = new FormAttachment( wReadFrom, 2 * margin );
    fdReadTo.right = new FormAttachment( opento, -margin );
    wReadTo.setLayoutData(fdReadTo);

    FormData fdReceivedDate = new FormData();
    fdReceivedDate.left = new FormAttachment( 0, margin );
    fdReceivedDate.top = new FormAttachment(wHeader, margin );
    fdReceivedDate.right = new FormAttachment( 100, -margin );
    wReceivedDate.setLayoutData(fdReceivedDate);
    // ///////////////////////////////////////////////////////////
    // / END OF RECEIVED DATE GROUP
    // ///////////////////////////////////////////////////////////

    wlLimit = new Label(wSearchComp, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "MailInput.Limit.Label" ) );
    props.setLook( wlLimit );
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.top = new FormAttachment(wReceivedDate, 2 * margin );
    fdlLimit.right = new FormAttachment( middle, -margin );
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(wSearchComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.top = new FormAttachment(wReceivedDate, 2 * margin );
    fdLimit.right = new FormAttachment( 100, 0 );
    wLimit.setLayoutData(fdLimit);

    FormData fdSearchComp = new FormData();
    fdSearchComp.left = new FormAttachment( 0, 0 );
    fdSearchComp.top = new FormAttachment( wTransformName, 0 );
    fdSearchComp.right = new FormAttachment( 100, 0 );
    fdSearchComp.bottom = new FormAttachment( 100, 0 );
    wSearchComp.setLayoutData(fdSearchComp);

    wSearchComp.layout();
    wSearchTab.setControl(wSearchComp);
    props.setLook(wSearchComp);

    // ////////////////////////////////
    // / END OF SEARCH TAB
    // ////////////////////////////////

    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText( BaseMessages.getString( PKG, "MailInputdialog.Fields.Tab" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook(wFieldsComp);

    wGet = new Button(wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "MailInputdialog.GetFields.Button" ) );
    fdGet = new FormData();
    fdGet.left = new FormAttachment( 50, 0 );
    fdGet.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    final int FieldsRows = input.getInputFields().length;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "MailInputdialog.FieldsTable.Name.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "MailInputdialog.FieldsTable.Column.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, MailInputField.ColumnDesc, true ), };

    colinf[ 0 ].setUsingVariables( true );
    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "MailInputdialog.FieldsTable.Name.Column.Tooltip" ) );
    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "MailInputdialog.FieldsTable.Column.Column.Tooltip" ) );

    wFields =
      new TableView( variables, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -margin );
    wFields.setLayoutData(fdFields);

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData(fdTabFolder);



    // Add listeners
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    Listener lsTest = e -> test();
    wTest.addListener( SWT.Selection, lsTest);
    wGet.addListener( SWT.Selection, e -> getFields() );
    Listener lsTestIMAPFolder = e -> checkFolder(variables.resolve(wIMAPFolder.getText()));
    wTestIMAPFolder.addListener( SWT.Selection, lsTestIMAPFolder);

    Listener lsSelectFolder = e -> selectFolder(wIMAPFolder);
    wSelectFolder.addListener( SWT.Selection, lsSelectFolder);

    wTransformName.addSelectionListener( lsDef );
    wServerName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    setUserProxy();
    chooseListMails();
    refreshProtocol( false );
    conditionReceivedDate();
    wTabFolder.setSelection( 0 );
    BaseTransformDialog.setSize( shell );

    shell.open();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void activedynamicFolder() {
    wlFolderField.setEnabled( wDynamicFolder.getSelection() );
    wFolderField.setEnabled( wDynamicFolder.getSelection() );
    wlIMAPFolder.setEnabled( !wDynamicFolder.getSelection() );
    wIMAPFolder.setEnabled( !wDynamicFolder.getSelection() );
    wPreview.setEnabled( !wDynamicFolder.getSelection() );
    if ( wDynamicFolder.getSelection() ) {
      wLimit.setText( "0" );
    }
    wlLimit.setEnabled( !wDynamicFolder.getSelection() );
    wLimit.setEnabled( !wDynamicFolder.getSelection() );
    boolean activePOP3 = wProtocol.getText().equals( MailConnectionMeta.PROTOCOL_STRING_POP3 );
    wlIMAPFolder.setEnabled( !wDynamicFolder.getSelection() && !activePOP3 );
    wIMAPFolder.setEnabled( !wDynamicFolder.getSelection() && !activePOP3 );
    wTestIMAPFolder.setEnabled( !wDynamicFolder.getSelection() && !activePOP3 );
    wSelectFolder.setEnabled( !wDynamicFolder.getSelection() && !activePOP3 );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getServerName() != null ) {
      wServerName.setText( input.getServerName() );
    }
    if ( input.getUserName() != null ) {
      wUserName.setText( input.getUserName() );
    }
    if ( input.getPassword() != null ) {
      wPassword.setText( input.getPassword() );
    }

    wUseSSL.setSelection( input.isUseSSL() );

    if ( input.getPort() != null ) {
      wPort.setText( input.getPort() );
    }

    String protocol = input.getProtocol();

    boolean isPop3 = StringUtils.equals( protocol, MailConnectionMeta.PROTOCOL_STRING_POP3 );
    wProtocol.setText( protocol );
    int iRet = input.getRetrievemails();

    // [PDI-7241] POP3 does not support retrieve email flags.
    // if anyone already used 'unread' for POP3 in pipeline or 'retrieve... first'
    // now they realize that all this time it was 'retrieve all mails'.
    if ( iRet > 0 ) {
      if ( isPop3 ) {
        wListmails.select( iRet - 1 );
      } else {
        wListmails.select( iRet );
      }
    } else {
      wListmails.select( 0 ); // Retrieve All Mails
    }

    if ( input.getFirstMails() != null ) {
      wFirstmails.setText( input.getFirstMails() );
    }

    wIMAPListmails.setText( MailConnectionMeta.getValueImapListDesc( input.getValueImapList() ) );
    if ( input.getFirstIMAPMails() != null ) {
      wIMAPFirstmails.setText( input.getFirstIMAPMails() );
    }
    if ( input.getIMAPFolder() != null ) {
      wIMAPFolder.setText( input.getIMAPFolder() );
    }
    // search term
    if ( input.getSenderSearchTerm() != null ) {
      wSender.setText( input.getSenderSearchTerm() );
    }
    wNegateSender.setSelection( input.isNotTermSenderSearch() );
    if ( input.getRecipientSearch() != null ) {
      wReceipient.setText( input.getRecipientSearch() );
    }
    wNegateReceipient.setSelection( input.isNotTermRecipientSearch() );
    if ( input.getSubjectSearch() != null ) {
      wSubject.setText( input.getSubjectSearch() );
    }
    wNegateSubject.setSelection( input.isNotTermSubjectSearch() );
    wConditionOnReceivedDate
      .setText( MailConnectionMeta.getConditionDateDesc( input.getConditionOnReceivedDate() ) );
    wNegateReceivedDate.setSelection( input.isNotTermReceivedDateSearch() );
    if ( input.getReceivedDate1() != null ) {
      wReadFrom.setText( input.getReceivedDate1() );
    }
    if ( input.getReceivedDate2() != null ) {
      wReadTo.setText( input.getReceivedDate2() );
    }
    wIncludeSubFolders.setSelection( input.isIncludeSubFolders() );
    wUseProxy.setSelection( input.isUseProxy() );
    if ( input.getProxyUsername() != null ) {
      wProxyUsername.setText( input.getProxyUsername() );
    }
    wDynamicFolder.setSelection( input.isDynamicFolder() );
    if ( input.getFolderField() != null ) {
      wFolderField.setText( input.getFolderField() );
    }
    wLimit.setText( Const.NVL( input.getRowLimit(), "0" ) );
    for ( int i = 0; i < input.getInputFields().length; i++ ) {
      MailInputField field = input.getInputFields()[ i ];

      if ( field != null ) {
        TableItem item = wFields.table.getItem( i );
        String name = field.getName();
        String column = field.getColumnDesc();
        if ( name != null ) {
          item.setText( 1, name );
        }
        if ( column != null ) {
          item.setText( 2, column );
        }
      }
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );

    wUseBatch.setSelection( input.isUseBatch() );
    wBatchSize.setText(
      input.getBatchSize() == null
        ? String.valueOf( MailInputMeta.DEFAULT_BATCH_SIZE )
        : input.getBatchSize().toString() );
    wStartMessage.setText( input.getStart() == null ? "" : input.getStart().toString() );
    wEndMessage.setText( input.getEnd() == null ? "" : input.getEnd().toString() );
    wIgnoreFieldErrors.setSelection( input.isStopOnError() );
    setBatchSettingsEnabled();

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    try {
      getInfo( input );
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "MailInputDialog.ErrorParsingData.DialogTitle" ), BaseMessages
        .getString( PKG, "MailInputDialog.ErrorParsingData.DialogMessage" ), e );
    }
    dispose();
  }

  private void getInfo( MailInputMeta in ) throws HopException {
    transformName = wTransformName.getText(); // return value

    in.setServerName( wServerName.getText() );
    in.setUserName( wUserName.getText() );
    in.setPassword( wPassword.getText() );
    in.setUseSSL( wUseSSL.getSelection() );
    in.setPort( wPort.getText() );

    // [PDI-7241] Option 'retrieve unread' is removed and there is only 2 options.
    // for backward compatibility: 0 is 'retrieve all', 2 is 'retrieve first...'
    int actualIndex = wListmails.getSelectionIndex();
    in.setRetrievemails( actualIndex > 0 ? 2 : 0 );

    //Set first... emails for POP3
    in.setFirstMails( wFirstmails.getText() );
    in.setProtocol( wProtocol.getText() );
    in.setValueImapList( MailConnectionMeta.getValueImapListByDesc( wIMAPListmails.getText() ) );
    //Set first... emails for IMAP
    in.setFirstIMAPMails( wIMAPFirstmails.getText() );
    in.setIMAPFolder( wIMAPFolder.getText() );
    // search term
    in.setSenderSearchTerm( wSender.getText() );
    in.setNotTermSenderSearch( wNegateSender.getSelection() );

    in.setRecipientSearch( wReceipient.getText() );
    in.setNotTermRecipientSearch( wNegateReceipient.getSelection() );
    in.setSubjectSearch( wSubject.getText() );
    in.setNotTermSubjectSearch( wNegateSubject.getSelection() );
    in.setConditionOnReceivedDate( MailConnectionMeta.getConditionDateByDesc( wConditionOnReceivedDate.getText() ) );
    in.setNotTermReceivedDateSearch( wNegateReceivedDate.getSelection() );
    in.setReceivedDate1( wReadFrom.getText() );
    in.setReceivedDate2( wReadTo.getText() );
    in.setIncludeSubFolders( wIncludeSubFolders.getSelection() );
    in.setUseProxy( wUseProxy.getSelection() );
    in.setProxyUsername( wProxyUsername.getText() );
    in.setDynamicFolder( wDynamicFolder.getSelection() );
    in.setFolderField( wFolderField.getText() );
    in.setRowLimit( wLimit.getText() );
    int nrFields = wFields.nrNonEmpty();
    in.allocate( nrFields );
    for ( int i = 0; i < nrFields; i++ ) {
      MailInputField field = new MailInputField();

      TableItem item = wFields.getNonEmpty( i );

      field.setName( item.getText( 1 ) );
      field.setColumn( MailInputField.getColumnByDesc( item.getText( 2 ) ) );
      //CHECKSTYLE:Indentation:OFF
      in.getInputFields()[ i ] = field;
    }

    in.setUseBatch( wUseBatch.getSelection() );
    Integer batchSize = getInteger( wBatchSize.getText() );
    in.setBatchSize( batchSize == null ? MailInputMeta.DEFAULT_BATCH_SIZE : batchSize );
    in.setStart( wStartMessage.getText() );
    in.setEnd( wEndMessage.getText() );
    in.setStopOnError( wIgnoreFieldErrors.getSelection() );
  }

  private void setFolderField() {
    if ( !gotPreviousfields ) {
      try {
        String field = wFolderField.getText();
        wFolderField.removeAll();

        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wFolderField.setItems( r.getFieldNames() );
        }
        if ( field != null ) {
          wFolderField.setText( field );
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "MailInput.FailedToGetFields.DialogTitle" ), BaseMessages
          .getString( PKG, "MailInput.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousfields = true;
    }
  }

  private void closeMailConnection() {
    try {
      if ( mailConn != null ) {
        mailConn.disconnect();
        mailConn = null;
      }
    } catch ( Exception e ) { /* Ignore */
    }
  }

  private void conditionReceivedDate() {
    boolean activeReceivedDate =
      !( MailConnectionMeta.getConditionDateByDesc( wConditionOnReceivedDate.getText() )
        == MailConnectionMeta.CONDITION_DATE_IGNORE );
    boolean useBetween =
      ( MailConnectionMeta.getConditionDateByDesc( wConditionOnReceivedDate.getText() )
        == MailConnectionMeta.CONDITION_DATE_BETWEEN );
    wlReadFrom.setVisible( activeReceivedDate );
    wReadFrom.setVisible( activeReceivedDate );
    open.setVisible( activeReceivedDate );
    wlReadTo.setVisible( activeReceivedDate && useBetween );
    wReadTo.setVisible( activeReceivedDate && useBetween );
    opento.setVisible( activeReceivedDate && useBetween );
    if ( !activeReceivedDate ) {
      wReadFrom.setText( "" );
      wReadTo.setText( "" );
      wNegateReceivedDate.setSelection( false );
    }
  }

  private void refreshPort( boolean refreshport ) {
    if ( refreshport ) {
      if ( wProtocol.getText().equals( MailConnectionMeta.PROTOCOL_STRING_POP3 ) ) {
        if ( wUseSSL.getSelection() ) {
          if ( Utils.isEmpty( wPort.getText() )
            || wPort.getText().equals( "" + MailConnectionMeta.DEFAULT_SSL_IMAP_PORT ) ) {
            wPort.setText( "" + MailConnectionMeta.DEFAULT_SSL_POP3_PORT );
          }
        } else {
          if ( Utils.isEmpty( wPort.getText() ) || wPort.getText().equals( MailConnectionMeta.DEFAULT_IMAP_PORT ) ) {
            wPort.setText( "" + MailConnectionMeta.DEFAULT_POP3_PORT );
          }
        }
      } else if ( wProtocol.getText().equals( MailConnectionMeta.PROTOCOL_STRING_IMAP ) ) {
        if ( wUseSSL.getSelection() ) {
          if ( Utils.isEmpty( wPort.getText() )
            || wPort.getText().equals( "" + MailConnectionMeta.DEFAULT_SSL_POP3_PORT ) ) {
            wPort.setText( "" + MailConnectionMeta.DEFAULT_SSL_IMAP_PORT );
          }
        } else {
          if ( Utils.isEmpty( wPort.getText() ) || wPort.getText().equals( MailConnectionMeta.DEFAULT_POP3_PORT ) ) {
            wPort.setText( "" + MailConnectionMeta.DEFAULT_IMAP_PORT );
          }
        }
      } else {
        wPort.setText( "" );
      }
    }
  }

  private void refreshProtocol( boolean refreshport ) {
    boolean activePOP3 = wProtocol.getText().equals( MailConnectionMeta.PROTOCOL_STRING_POP3 );
    boolean activeIMAP = wProtocol.getText().equals( MailConnectionMeta.PROTOCOL_STRING_IMAP );

    wlPOP3Message.setEnabled( activePOP3 );
    wlListmails.setEnabled( activePOP3 );
    wListmails.setEnabled( activePOP3 );
    wlFirstmails.setEnabled( activePOP3 );

    wlIMAPFirstmails.setEnabled( activeIMAP );
    wIMAPFirstmails.setEnabled( activeIMAP );

    wlIncludeSubFolders.setEnabled( !activePOP3 );
    wIncludeSubFolders.setEnabled( !activePOP3 );
    wlIMAPListmails.setEnabled( activeIMAP );
    wIMAPListmails.setEnabled( activeIMAP );
    if ( activePOP3 && wDynamicFolder.getSelection() ) {
      wDynamicFolder.setSelection( false );
    }
    wlDynamicFolder.setEnabled( !activePOP3 );
    wDynamicFolder.setEnabled( !activePOP3 );

    if ( activePOP3 ) {
      // clear out selections
      wConditionOnReceivedDate.select( 0 );
      conditionReceivedDate();
    }
    // POP3/MBOX protocols do not provide information about when a message was received
    wConditionOnReceivedDate.setEnabled( activeIMAP );
    wNegateReceivedDate.setEnabled( activeIMAP );
    wlConditionOnReceivedDate.setEnabled( activeIMAP );

    setRemoteOptionsEnabled( activePOP3 || activeIMAP );

    activedynamicFolder();
    chooseListMails();
    refreshPort( refreshport );
  }

  private void setRemoteOptionsEnabled( boolean enableRemoteOpts ) {
    wlUserName.setEnabled( enableRemoteOpts );
    wUserName.setEnabled( enableRemoteOpts );
    wPort.setEnabled( enableRemoteOpts );
    wlPort.setEnabled( enableRemoteOpts );
    wPassword.setEnabled( enableRemoteOpts );
    wlPassword.setEnabled( enableRemoteOpts );
    if ( !enableRemoteOpts && wUseProxy.getSelection() ) {
      wUseProxy.setSelection( false );
      setUserProxy();
    }
    wUseProxy.setEnabled( enableRemoteOpts );
    wlUseProxy.setEnabled( enableRemoteOpts );
    wUseSSL.setEnabled( enableRemoteOpts );
    wlUseSSL.setEnabled( enableRemoteOpts );
  }

  public void dispose() {
    closeMailConnection();
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  public void chooseListMails() {
    boolean ok =
      ( wProtocol.getText().equals( MailConnectionMeta.PROTOCOL_STRING_POP3 ) && wListmails.getSelectionIndex() == 1 );
    wlFirstmails.setEnabled( ok );
    wFirstmails.setEnabled( ok );
  }

  private void selectFolder( TextVar input ) {
    if ( connect() ) {
      try {
        Folder folder = mailConn.getStore().getDefaultFolder();
        SelectFolderDialog s = new SelectFolderDialog( shell, SWT.NONE, folder );
        String folderName = s.open();
        if ( folderName != null ) {
          input.setText( folderName );
        }
      } catch ( Exception e ) {
        // Ignore errors
      }
    }
  }

  private boolean connect() {
    String errordescription = null;
    boolean retval = false;
    if ( mailConn != null && mailConn.isConnected() ) {
      retval = mailConn.isConnected();
    }

    if ( !retval ) {
      String realserver = variables.resolve( wServerName.getText() );
      String realuser = variables.resolve( wUserName.getText() );
      String realpass = Utils.resolvePassword( variables, wPassword.getText() );
      String realProxyUsername = variables.resolve( wProxyUsername.getText() );
      int realport = Const.toInt( variables.resolve( wPort.getText() ), -1 );

      try {
        mailConn =
          new MailConnection(
            LogChannel.UI, MailConnectionMeta.getProtocolFromString(
            wProtocol.getText(), MailConnectionMeta.PROTOCOL_IMAP ), realserver, realport, realuser,
            realpass, wUseSSL.getSelection(), wUseProxy.getSelection(), realProxyUsername );
        mailConn.connect();

        retval = true;
      } catch ( Exception e ) {
        errordescription = e.getMessage();
      }
    }

    if ( !retval ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "MailInput.Connected.NOK.ConnectionBad", wServerName.getText() )
        + Const.CR + Const.NVL( errordescription, "" ) );
      mb.setText( BaseMessages.getString( PKG, "MailInput.Connected.Title.Bad" ) );
      mb.open();
    }

    return ( mailConn.isConnected() );

  }

  private void test() {
    if ( connect() ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "MailInput.Connected.OK", wServerName.getText() ) + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "MailInput.Connected.Title.Ok" ) );
      mb.open();
    }
  }

  private void checkFolder( String folderName ) {
    if ( !Utils.isEmpty( folderName ) ) {
      if ( connect() ) {
        // check folder
        if ( mailConn.folderExists( folderName ) ) {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "MailInput.IMAPFolderExists.OK", folderName ) + Const.CR );
          mb.setText( BaseMessages.getString( PKG, "MailInput.IMAPFolderExists.Title.Ok" ) );
          mb.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "MailInput.Connected.NOK.IMAPFolderExists", folderName )
            + Const.CR );
          mb.setText( BaseMessages.getString( PKG, "MailInput.IMAPFolderExists.Title.Bad" ) );
          mb.open();
        }
      }
    }
  }

  // Preview the data
  private void preview() {
    try {
      MailInputMeta oneMeta = new MailInputMeta();
      getInfo( oneMeta );

      PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline( variables, metadataProvider, oneMeta, wTransformName.getText() );

      EnterNumberDialog numberDialog = new EnterNumberDialog( shell, props.getDefaultPreviewSize(),
        BaseMessages.getString( PKG, "MailInputDialog.NumberRows.DialogTitle" ),
        BaseMessages.getString( PKG, "MailInputDialog.NumberRows.DialogMessage" ) );

      int previewSize = numberDialog.open();
      if ( previewSize > 0 ) {
        PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
            shell, variables, previewMeta, new String[] { wTransformName.getText() }, new int[] { previewSize } );
        progressDialog.open();

        if ( !progressDialog.isCancelled() ) {
          Pipeline pipeline = progressDialog.getPipeline();
          String loggingText = progressDialog.getLoggingText();

          if ( pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0 ) {
            EnterTextDialog etd = new EnterTextDialog( shell,
              BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ),
              BaseMessages.getString( PKG, "System.Dialog.PreviewError.Message" ), loggingText, true );
            etd.setReadOnly();
            etd.open();
          }
          PreviewRowsDialog prd =
            new PreviewRowsDialog(
              shell, variables, SWT.NONE, wTransformName.getText(), progressDialog.getPreviewRowsMeta( wTransformName
              .getText() ), progressDialog.getPreviewRows( wTransformName.getText() ), loggingText );
          prd.open();

        }
      }
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "MailInputDialog.ErrorPreviewingData.DialogTitle" ), BaseMessages
        .getString( PKG, "MailInputDialog.ErrorPreviewingData.DialogMessage" ), e );
    }
  }

  private void setUserProxy() {
    wlProxyUsername.setEnabled( wUseProxy.getSelection() );
    wProxyUsername.setEnabled( wUseProxy.getSelection() );
  }

  private void getFields() {

    // Clear Fields Grid
    wFields.removeAll();
    for ( int i = 0; i < MailInputField.ColumnDesc.length; i++ ) {
      wFields.add( new String[] { MailInputField.ColumnDesc[ i ], MailInputField.ColumnDesc[ i ] } );
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );
  }

  private void setBatchSettingsEnabled() {
    boolean enabled = wUseBatch.getSelection();
    wBatchSize.setEnabled( enabled );
    wStartMessage.setEnabled( enabled );
    wEndMessage.setEnabled( enabled );
  }

  private Group createGroup( Composite parentTab, Control top, String label ) {
    Group group = new Group( parentTab, SWT.SHADOW_NONE );
    props.setLook( group );
    group.setText( label );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    group.setLayout( groupLayout );

    FormData fdGroup = new FormData();
    fdGroup.left = new FormAttachment( 0, props.getMargin() );
    fdGroup.top = new FormAttachment( top, props.getMargin() );
    fdGroup.right = new FormAttachment( 100, -props.getMargin() );
    group.setLayoutData( fdGroup );

    return group;
  }

  private void addLabelInputPairBelow( Control label, Control input, Control widgetAbove ) {
    addLabelBelow( label, widgetAbove );
    addControlBelow( label, input );
  }

  private void addLabelBelow( Control label, Control widgetAbove ) {
    props.setLook( label );
    FormData fData = new FormData();
    fData.top = new FormAttachment( widgetAbove, props.getMargin() );
    fData.right = new FormAttachment( Const.MIDDLE_PCT, -props.getMargin() );
    label.setLayoutData( fData );
  }

  private void addControlBelow( Control label, Control control ) {
    props.setLook( control );
    FormData fData = new FormData();
    fData.top = new FormAttachment( label, 0, SWT.CENTER );
    fData.left = new FormAttachment( Const.MIDDLE_PCT, props.getMargin() );
    fData.right = new FormAttachment( 100, -props.getMargin() );
    control.setLayoutData( fData );
  }

  private Integer getInteger( String toParse ) {
    if ( Utils.isEmpty( toParse ) ) {
      return null;
    }

    try {
      return new Integer( toParse );
    } catch ( NumberFormatException e ) {
      return null;
    }
  }

}
