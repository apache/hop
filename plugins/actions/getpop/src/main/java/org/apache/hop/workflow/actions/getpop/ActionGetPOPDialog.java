// CHECKSTYLE:FileLength:OFF
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

package org.apache.hop.workflow.actions.getpop;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
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

/**
 * This dialog allows you to edit the Get POP action settings.
 *
 * @author Matt
 * @since 19-06-2003
 */
public class ActionGetPOPDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionGetPOP.class; // For Translator

  private Text wName;

  private TextVar wServerName;

  private TextVar wSender;

  private TextVar wReceipient;

  private TextVar wSubject;

  private TextVar wBody;

  private Label wlAttachmentFolder;

  private TextVar wAttachmentFolder;

  private Button wbAttachmentFolder;

  private Label wlAttachmentWildcard;

  private TextVar wAttachmentWildcard;

  private TextVar wUserName;

  private Label wlIMAPFolder;

  private TextVar wIMAPFolder;

  private Label wlMoveToFolder;

  private TextVar wMoveToFolder;

  private Button wSelectMoveToFolder;

  private Button wTestMoveToFolder;

  private TextVar wPassword;

  private Label wlOutputDirectory;

  private TextVar wOutputDirectory;

  private Label wlFilenamePattern;

  private TextVar wFilenamePattern;

  private Button wbDirectory;

  private Label wlListmails;

  private CCombo wListmails;

  private Label wlIMAPListmails;

  private CCombo wIMAPListmails;

  private Label wlAfterGetIMAP;

  private CCombo wAfterGetIMAP;

  private Label wlFirstmails;

  private TextVar wFirstmails;

  private Label wlIMAPFirstmails;

  private TextVar wIMAPFirstmails;

  private TextVar wPort;

  private Button wUseSSL;

  private Button wUseProxy;

  private Label wlProxyUsername;

  private TextVar wProxyUsername;

  private Label wlIncludeSubFolders;

  private Button wIncludeSubFolders;

  private Label wlcreateMoveToFolder;

  private Button wcreateMoveToFolder;

  private Label wlcreateLocalFolder;

  private Button wcreateLocalFolder;

  private Button wNegateSender;

  private Button wNegateReceipient;

  private Button wNegateSubject;

  private Button wNegateBody;

  private Button wNegateReceivedDate;

  private Label wlGetAttachment;

  private Button wGetAttachment;

  private Label wlGetMessage;

  private Button wGetMessage;

  private Label wlDifferentFolderForAttachment;

  private Button wDifferentFolderForAttachment;

  private Label wlPOP3Message;

  private Label wlDelete;

  private Button wDelete;

  private ActionGetPOP action;

  private Shell shell;

  private boolean changed;

  private Label wlReadFrom;

  private TextVar wReadFrom;

  private Button open;

  private Label wlConditionOnReceivedDate;

  private CCombo wConditionOnReceivedDate;

  private CCombo wActionType;

  private Label wlReadTo;

  private TextVar wReadTo;

  private Button opento;

  private CCombo wProtocol;

  private Button wTestIMAPFolder;

  private Button wSelectFolder;

  private MailConnection mailConn = null;

  public ActionGetPOPDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionGetPOP) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "ActionGetPOP.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    WorkflowMeta workflowMeta = getWorkflowMeta();
    
    ModifyListener lsMod = e -> {
      closeMailConnection();
      action.setChanged();
    };

    SelectionListener lsSelection = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        closeMailConnection();
      }
    };
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ActionGetPOP.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "ActionGetPOP.Name.Label" ) );
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData(fdlName);
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData(fdName);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "ActionGetPOP.Tab.General.Label" ) );
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
    wServerSettings.setText( BaseMessages.getString( PKG, "ActionGetPOP.ServerSettings.Group.Label" ) );

    FormLayout ServerSettingsgroupLayout = new FormLayout();
    ServerSettingsgroupLayout.marginWidth = 10;
    ServerSettingsgroupLayout.marginHeight = 10;
    wServerSettings.setLayout( ServerSettingsgroupLayout );

    // ServerName line
    Label wlServerName = new Label(wServerSettings, SWT.RIGHT);
    wlServerName.setText( BaseMessages.getString( PKG, "ActionGetPOP.Server.Label" ) );
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
    Label wlUseSSL = new Label(wServerSettings, SWT.RIGHT);
    wlUseSSL.setText( BaseMessages.getString( PKG, "ActionGetPOP.UseSSLMails.Label" ) );
    props.setLook(wlUseSSL);
    FormData fdlUseSSL = new FormData();
    fdlUseSSL.left = new FormAttachment( 0, 0 );
    fdlUseSSL.top = new FormAttachment( wServerName, margin );
    fdlUseSSL.right = new FormAttachment( middle, -margin );
    wlUseSSL.setLayoutData(fdlUseSSL);
    wUseSSL = new Button(wServerSettings, SWT.CHECK );
    props.setLook( wUseSSL );
    FormData fdUseSSL = new FormData();
    wUseSSL.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.UseSSLMails.Tooltip" ) );
    fdUseSSL.left = new FormAttachment( middle, 0 );
    fdUseSSL.top = new FormAttachment( wServerName, margin );
    fdUseSSL.right = new FormAttachment( 100, 0 );
    wUseSSL.setLayoutData(fdUseSSL);

    wUseSSL.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        closeMailConnection();
        refreshPort( true );
      }
    } );

    // port
    Label wlPort = new Label(wServerSettings, SWT.RIGHT);
    wlPort.setText( BaseMessages.getString( PKG, "ActionGetPOP.SSLPort.Label" ) );
    props.setLook(wlPort);
    FormData fdlPort = new FormData();
    fdlPort.left = new FormAttachment( 0, 0 );
    fdlPort.top = new FormAttachment( wUseSSL, margin );
    fdlPort.right = new FormAttachment( middle, -margin );
    wlPort.setLayoutData(fdlPort);
    wPort = new TextVar( variables, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPort );
    wPort.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.SSLPort.Tooltip" ) );
    wPort.addModifyListener( lsMod );
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment( middle, 0 );
    fdPort.top = new FormAttachment( wUseSSL, margin );
    fdPort.right = new FormAttachment( 100, 0 );
    wPort.setLayoutData(fdPort);

    // UserName line
    Label wlUserName = new Label(wServerSettings, SWT.RIGHT);
    wlUserName.setText( BaseMessages.getString( PKG, "ActionGetPOP.Username.Label" ) );
    props.setLook(wlUserName);
    FormData fdlUserName = new FormData();
    fdlUserName.left = new FormAttachment( 0, 0 );
    fdlUserName.top = new FormAttachment( wPort, margin );
    fdlUserName.right = new FormAttachment( middle, -margin );
    wlUserName.setLayoutData(fdlUserName);
    wUserName = new TextVar( variables, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUserName );
    wUserName.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.Username.Tooltip" ) );
    wUserName.addModifyListener( lsMod );
    FormData fdUserName = new FormData();
    fdUserName.left = new FormAttachment( middle, 0 );
    fdUserName.top = new FormAttachment( wPort, margin );
    fdUserName.right = new FormAttachment( 100, 0 );
    wUserName.setLayoutData(fdUserName);

    // Password line
    Label wlPassword = new Label(wServerSettings, SWT.RIGHT);
    wlPassword.setText( BaseMessages.getString( PKG, "ActionGetPOP.Password.Label" ) );
    props.setLook(wlPassword);
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
    Label wlUseProxy = new Label(wServerSettings, SWT.RIGHT);
    wlUseProxy.setText( BaseMessages.getString( PKG, "ActionGetPOP.UseProxyMails.Label" ) );
    props.setLook(wlUseProxy);
    FormData fdlUseProxy = new FormData();
    fdlUseProxy.left = new FormAttachment( 0, 0 );
    fdlUseProxy.top = new FormAttachment( wPassword, 2 * margin );
    fdlUseProxy.right = new FormAttachment( middle, -margin );
    wlUseProxy.setLayoutData(fdlUseProxy);
    wUseProxy = new Button(wServerSettings, SWT.CHECK );
    props.setLook( wUseProxy );
    FormData fdUseProxy = new FormData();
    wUseProxy.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.UseProxyMails.Tooltip" ) );
    fdUseProxy.left = new FormAttachment( middle, 0 );
    fdUseProxy.top = new FormAttachment( wPassword, 2 * margin );
    fdUseProxy.right = new FormAttachment( 100, 0 );
    wUseProxy.setLayoutData(fdUseProxy);

    wUseProxy.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setUserProxy();
        action.setChanged();
      }
    } );

    // ProxyUsername line
    wlProxyUsername = new Label(wServerSettings, SWT.RIGHT );
    wlProxyUsername.setText( BaseMessages.getString( PKG, "ActionGetPOP.ProxyUsername.Label" ) );
    props.setLook( wlProxyUsername );
    FormData fdlProxyUsername = new FormData();
    fdlProxyUsername.left = new FormAttachment( 0, 0 );
    fdlProxyUsername.top = new FormAttachment( wUseProxy, margin );
    fdlProxyUsername.right = new FormAttachment( middle, -margin );
    wlProxyUsername.setLayoutData(fdlProxyUsername);
    wProxyUsername = new TextVar( variables, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wProxyUsername );
    wProxyUsername.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.ProxyUsername.Tooltip" ) );
    wProxyUsername.addModifyListener( lsMod );
    FormData fdProxyUsername = new FormData();
    fdProxyUsername.left = new FormAttachment( middle, 0 );
    fdProxyUsername.top = new FormAttachment( wUseProxy, margin );
    fdProxyUsername.right = new FormAttachment( 100, 0 );
    wProxyUsername.setLayoutData(fdProxyUsername);

    // Protocol
    Label wlProtocol = new Label(wServerSettings, SWT.RIGHT);
    wlProtocol.setText( BaseMessages.getString( PKG, "ActionGetPOP.Protocol.Label" ) );
    props.setLook(wlProtocol);
    FormData fdlProtocol = new FormData();
    fdlProtocol.left = new FormAttachment( 0, 0 );
    fdlProtocol.right = new FormAttachment( middle, -margin );
    fdlProtocol.top = new FormAttachment( wProxyUsername, margin );
    wlProtocol.setLayoutData(fdlProtocol);
    wProtocol = new CCombo(wServerSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wProtocol.setItems( MailConnectionMeta.protocolCodes );
    wProtocol.select( 0 );
    props.setLook( wProtocol );
    FormData fdProtocol = new FormData();
    fdProtocol.left = new FormAttachment( middle, 0 );
    fdProtocol.top = new FormAttachment( wProxyUsername, margin );
    fdProtocol.right = new FormAttachment( 100, 0 );
    wProtocol.setLayoutData(fdProtocol);
    wProtocol.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        refreshProtocol( true );

      }
    } );

    // Test connection button
    Button wTest = new Button(wServerSettings, SWT.PUSH);
    wTest.setText( BaseMessages.getString( PKG, "ActionGetPOP.TestConnection.Label" ) );
    props.setLook(wTest);
    FormData fdTest = new FormData();
    wTest.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.TestConnection.Tooltip" ) );
    // fdTest.left = new FormAttachment(middle, 0);
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

    // ////////////////////////
    // START OF Target Folder GROUP///
    // /
    Group wTargetFolder = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wTargetFolder);
    wTargetFolder.setText( BaseMessages.getString( PKG, "ActionGetPOP.TargetFolder.Group.Label" ) );

    FormLayout TargetFoldergroupLayout = new FormLayout();
    TargetFoldergroupLayout.marginWidth = 10;
    TargetFoldergroupLayout.marginHeight = 10;
    wTargetFolder.setLayout( TargetFoldergroupLayout );

    // OutputDirectory line
    wlOutputDirectory = new Label(wTargetFolder, SWT.RIGHT );
    wlOutputDirectory.setText( BaseMessages.getString( PKG, "ActionGetPOP.OutputDirectory.Label" ) );
    props.setLook( wlOutputDirectory );
    FormData fdlOutputDirectory = new FormData();
    fdlOutputDirectory.left = new FormAttachment( 0, 0 );
    fdlOutputDirectory.top = new FormAttachment(wServerSettings, margin );
    fdlOutputDirectory.right = new FormAttachment( middle, -margin );
    wlOutputDirectory.setLayoutData(fdlOutputDirectory);

    // Browse Source folders button ...
    wbDirectory = new Button(wTargetFolder, SWT.PUSH | SWT.CENTER );
    props.setLook( wbDirectory );
    wbDirectory.setText( BaseMessages.getString( PKG, "ActionGetPOP.BrowseFolders.Label" ) );
    FormData fdbDirectory = new FormData();
    fdbDirectory.right = new FormAttachment( 100, -margin );
    fdbDirectory.top = new FormAttachment(wServerSettings, margin );
    wbDirectory.setLayoutData(fdbDirectory);
    wbDirectory.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wOutputDirectory, variables ) );

    wOutputDirectory = new TextVar( variables, wTargetFolder, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wOutputDirectory );
    wOutputDirectory.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.OutputDirectory.Tooltip" ) );
    wOutputDirectory.addModifyListener( lsMod );
    FormData fdOutputDirectory = new FormData();
    fdOutputDirectory.left = new FormAttachment( middle, 0 );
    fdOutputDirectory.top = new FormAttachment(wServerSettings, margin );
    fdOutputDirectory.right = new FormAttachment( wbDirectory, -margin );
    wOutputDirectory.setLayoutData(fdOutputDirectory);

    // Create local folder
    wlcreateLocalFolder = new Label(wTargetFolder, SWT.RIGHT );
    wlcreateLocalFolder.setText( BaseMessages.getString( PKG, "ActionGetPOP.createLocalFolder.Label" ) );
    props.setLook( wlcreateLocalFolder );
    FormData fdlcreateLocalFolder = new FormData();
    fdlcreateLocalFolder.left = new FormAttachment( 0, 0 );
    fdlcreateLocalFolder.top = new FormAttachment( wOutputDirectory, margin );
    fdlcreateLocalFolder.right = new FormAttachment( middle, -margin );
    wlcreateLocalFolder.setLayoutData(fdlcreateLocalFolder);
    wcreateLocalFolder = new Button(wTargetFolder, SWT.CHECK );
    props.setLook( wcreateLocalFolder );
    FormData fdcreateLocalFolder = new FormData();
    wcreateLocalFolder.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.createLocalFolder.Tooltip" ) );
    fdcreateLocalFolder.left = new FormAttachment( middle, 0 );
    fdcreateLocalFolder.top = new FormAttachment( wOutputDirectory, margin );
    fdcreateLocalFolder.right = new FormAttachment( 100, 0 );
    wcreateLocalFolder.setLayoutData(fdcreateLocalFolder);

    // Filename pattern line
    wlFilenamePattern = new Label(wTargetFolder, SWT.RIGHT );
    wlFilenamePattern.setText( BaseMessages.getString( PKG, "ActionGetPOP.FilenamePattern.Label" ) );
    props.setLook( wlFilenamePattern );
    FormData fdlFilenamePattern = new FormData();
    fdlFilenamePattern.left = new FormAttachment( 0, 0 );
    fdlFilenamePattern.top = new FormAttachment( wcreateLocalFolder, margin );
    fdlFilenamePattern.right = new FormAttachment( middle, -margin );
    wlFilenamePattern.setLayoutData(fdlFilenamePattern);
    wFilenamePattern = new TextVar( variables, wTargetFolder, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilenamePattern );
    wFilenamePattern.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.FilenamePattern.Tooltip" ) );
    wFilenamePattern.addModifyListener( lsMod );
    FormData fdFilenamePattern = new FormData();
    fdFilenamePattern.left = new FormAttachment( middle, 0 );
    fdFilenamePattern.top = new FormAttachment( wcreateLocalFolder, margin );
    fdFilenamePattern.right = new FormAttachment( 100, 0 );
    wFilenamePattern.setLayoutData(fdFilenamePattern);

    // Whenever something changes, set the tooltip to the expanded version:
    wFilenamePattern.addModifyListener( e -> wFilenamePattern.setToolTipText( variables.resolve( wFilenamePattern.getText() ) ) );

    // Get message?
    wlGetMessage = new Label(wTargetFolder, SWT.RIGHT );
    wlGetMessage.setText( BaseMessages.getString( PKG, "ActionGetPOP.GetMessageMails.Label" ) );
    props.setLook( wlGetMessage );
    FormData fdlGetMessage = new FormData();
    fdlGetMessage.left = new FormAttachment( 0, 0 );
    fdlGetMessage.top = new FormAttachment( wFilenamePattern, margin );
    fdlGetMessage.right = new FormAttachment( middle, -margin );
    wlGetMessage.setLayoutData(fdlGetMessage);
    wGetMessage = new Button(wTargetFolder, SWT.CHECK );
    props.setLook( wGetMessage );
    FormData fdGetMessage = new FormData();
    wGetMessage.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.GetMessageMails.Tooltip" ) );
    fdGetMessage.left = new FormAttachment( middle, 0 );
    fdGetMessage.top = new FormAttachment( wFilenamePattern, margin );
    fdGetMessage.right = new FormAttachment( 100, 0 );
    wGetMessage.setLayoutData(fdGetMessage);

    wGetMessage.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        if ( !wGetAttachment.getSelection() && !wGetMessage.getSelection() ) {
          wGetAttachment.setSelection( true );
        }
      }
    } );

    // Get attachment?
    wlGetAttachment = new Label(wTargetFolder, SWT.RIGHT );
    wlGetAttachment.setText( BaseMessages.getString( PKG, "ActionGetPOP.GetAttachmentMails.Label" ) );
    props.setLook( wlGetAttachment );
    FormData fdlGetAttachment = new FormData();
    fdlGetAttachment.left = new FormAttachment( 0, 0 );
    fdlGetAttachment.top = new FormAttachment( wGetMessage, margin );
    fdlGetAttachment.right = new FormAttachment( middle, -margin );
    wlGetAttachment.setLayoutData(fdlGetAttachment);
    wGetAttachment = new Button(wTargetFolder, SWT.CHECK );
    props.setLook( wGetAttachment );
    FormData fdGetAttachment = new FormData();
    wGetAttachment.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.GetAttachmentMails.Tooltip" ) );
    fdGetAttachment.left = new FormAttachment( middle, 0 );
    fdGetAttachment.top = new FormAttachment( wGetMessage, margin );
    fdGetAttachment.right = new FormAttachment( 100, 0 );
    wGetAttachment.setLayoutData(fdGetAttachment);

    wGetAttachment.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeAttachmentFolder();
      }
    } );

    // different folder for attachment?
    wlDifferentFolderForAttachment = new Label(wTargetFolder, SWT.RIGHT );
    wlDifferentFolderForAttachment.setText( BaseMessages.getString(
      PKG, "ActionGetPOP.DifferentFolderForAttachmentMails.Label" ) );
    props.setLook( wlDifferentFolderForAttachment );
    FormData fdlDifferentFolderForAttachment = new FormData();
    fdlDifferentFolderForAttachment.left = new FormAttachment( 0, 0 );
    fdlDifferentFolderForAttachment.top = new FormAttachment( wGetAttachment, margin );
    fdlDifferentFolderForAttachment.right = new FormAttachment( middle, -margin );
    wlDifferentFolderForAttachment.setLayoutData(fdlDifferentFolderForAttachment);
    wDifferentFolderForAttachment = new Button(wTargetFolder, SWT.CHECK );
    props.setLook( wDifferentFolderForAttachment );
    FormData fdDifferentFolderForAttachment = new FormData();
    wDifferentFolderForAttachment.setToolTipText( BaseMessages.getString(
      PKG, "ActionGetPOP.DifferentFolderForAttachmentMails.Tooltip" ) );
    fdDifferentFolderForAttachment.left = new FormAttachment( middle, 0 );
    fdDifferentFolderForAttachment.top = new FormAttachment( wGetAttachment, margin );
    fdDifferentFolderForAttachment.right = new FormAttachment( 100, 0 );
    wDifferentFolderForAttachment.setLayoutData(fdDifferentFolderForAttachment);

    wDifferentFolderForAttachment.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeAttachmentFolder();
      }
    } );

    // AttachmentFolder line
    wlAttachmentFolder = new Label(wTargetFolder, SWT.RIGHT );
    wlAttachmentFolder.setText( BaseMessages.getString( PKG, "ActionGetPOP.AttachmentFolder.Label" ) );
    props.setLook( wlAttachmentFolder );
    FormData fdlAttachmentFolder = new FormData();
    fdlAttachmentFolder.left = new FormAttachment( 0, 0 );
    fdlAttachmentFolder.top = new FormAttachment( wDifferentFolderForAttachment, margin );
    fdlAttachmentFolder.right = new FormAttachment( middle, -margin );
    wlAttachmentFolder.setLayoutData(fdlAttachmentFolder);

    // Browse Source folders button ...
    wbAttachmentFolder = new Button(wTargetFolder, SWT.PUSH | SWT.CENTER );
    props.setLook( wbAttachmentFolder );
    wbAttachmentFolder.setText( BaseMessages.getString( PKG, "ActionGetPOP.BrowseFolders.Label" ) );
    FormData fdbAttachmentFolder = new FormData();
    fdbAttachmentFolder.right = new FormAttachment( 100, -margin );
    fdbAttachmentFolder.top = new FormAttachment( wDifferentFolderForAttachment, margin );
    wbAttachmentFolder.setLayoutData(fdbAttachmentFolder);
    wbAttachmentFolder.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wAttachmentFolder, variables ) );

    wAttachmentFolder = new TextVar( variables, wTargetFolder, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAttachmentFolder );
    wAttachmentFolder.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.AttachmentFolder.Tooltip" ) );
    wAttachmentFolder.addModifyListener( lsMod );
    FormData fdAttachmentFolder = new FormData();
    fdAttachmentFolder.left = new FormAttachment( middle, 0 );
    fdAttachmentFolder.top = new FormAttachment( wDifferentFolderForAttachment, margin );
    fdAttachmentFolder.right = new FormAttachment( wbAttachmentFolder, -margin );
    wAttachmentFolder.setLayoutData(fdAttachmentFolder);

    // Limit attached files
    wlAttachmentWildcard = new Label(wTargetFolder, SWT.RIGHT );
    wlAttachmentWildcard.setText( BaseMessages.getString( PKG, "ActionGetPOP.AttachmentWildcard.Label" ) );
    props.setLook( wlAttachmentWildcard );
    FormData fdlAttachmentWildcard = new FormData();
    fdlAttachmentWildcard.left = new FormAttachment( 0, 0 );
    fdlAttachmentWildcard.top = new FormAttachment( wbAttachmentFolder, margin );
    fdlAttachmentWildcard.right = new FormAttachment( middle, -margin );
    wlAttachmentWildcard.setLayoutData(fdlAttachmentWildcard);
    wAttachmentWildcard = new TextVar( variables, wTargetFolder, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAttachmentWildcard );
    wAttachmentWildcard.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.AttachmentWildcard.Tooltip" ) );
    wAttachmentWildcard.addModifyListener( lsMod );
    FormData fdAttachmentWildcard = new FormData();
    fdAttachmentWildcard.left = new FormAttachment( middle, 0 );
    fdAttachmentWildcard.top = new FormAttachment( wbAttachmentFolder, margin );
    fdAttachmentWildcard.right = new FormAttachment( 100, 0 );
    wAttachmentWildcard.setLayoutData(fdAttachmentWildcard);

    // Whenever something changes, set the tooltip to the expanded version:
    wAttachmentWildcard.addModifyListener( e -> wAttachmentWildcard.setToolTipText( variables.resolve( wAttachmentWildcard.getText() ) ) );

    FormData fdTargetFolder = new FormData();
    fdTargetFolder.left = new FormAttachment( 0, margin );
    fdTargetFolder.top = new FormAttachment(wServerSettings, margin );
    fdTargetFolder.right = new FormAttachment( 100, -margin );
    wTargetFolder.setLayoutData(fdTargetFolder);
    // ///////////////////////////////////////////////////////////
    // / END OF SERVER SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( wName, 0 );
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
    wSettingsTab.setText( BaseMessages.getString( PKG, "ActionGetPOP.Tab.Pop.Label" ) );
    Composite wSettingsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wSettingsComp);
    FormLayout PopLayout = new FormLayout();
    PopLayout.marginWidth = 3;
    PopLayout.marginHeight = 3;
    wSettingsComp.setLayout( PopLayout );

    // Action type
    Label wlActionType = new Label(wSettingsComp, SWT.RIGHT);
    wlActionType.setText( BaseMessages.getString( PKG, "ActionGetPOP.ActionType.Label" ) );
    props.setLook(wlActionType);
    FormData fdlActionType = new FormData();
    fdlActionType.left = new FormAttachment( 0, 0 );
    fdlActionType.right = new FormAttachment( middle, -margin );
    fdlActionType.top = new FormAttachment( 0, 3 * margin );
    wlActionType.setLayoutData(fdlActionType);

    wActionType = new CCombo(wSettingsComp, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wActionType.setItems( MailConnectionMeta.actionTypeDesc );
    wActionType.select( 0 ); // +1: starts at -1

    props.setLook( wActionType );
    FormData fdActionType = new FormData();
    fdActionType.left = new FormAttachment( middle, 0 );
    fdActionType.top = new FormAttachment( 0, 3 * margin );
    fdActionType.right = new FormAttachment( 100, 0 );
    wActionType.setLayoutData(fdActionType);
    wActionType.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setActionType();
        action.setChanged();
      }
    } );

    // Message: for POP3, only INBOX folder is available!
    wlPOP3Message = new Label(wSettingsComp, SWT.RIGHT );
    wlPOP3Message.setText( BaseMessages.getString( PKG, "ActionGetPOP.POP3Message.Label" ) );
    props.setLook( wlPOP3Message );
    FormData fdlPOP3Message = new FormData();
    fdlPOP3Message.left = new FormAttachment( 0, margin );
    fdlPOP3Message.top = new FormAttachment( wActionType, 3 * margin );
    wlPOP3Message.setLayoutData(fdlPOP3Message);
    wlPOP3Message.setForeground( GuiResource.getInstance().getColorOrange() );

    // ////////////////////////
    // START OF POP3 Settings GROUP///
    // /
    Group wPOP3Settings = new Group(wSettingsComp, SWT.SHADOW_NONE);
    props.setLook(wPOP3Settings);
    wPOP3Settings.setText( BaseMessages.getString( PKG, "ActionGetPOP.POP3Settings.Group.Label" ) );

    FormLayout POP3SettingsgroupLayout = new FormLayout();
    POP3SettingsgroupLayout.marginWidth = 10;
    POP3SettingsgroupLayout.marginHeight = 10;
    wPOP3Settings.setLayout( POP3SettingsgroupLayout );

    // List of mails of retrieve
    wlListmails = new Label(wPOP3Settings, SWT.RIGHT );
    wlListmails.setText( BaseMessages.getString( PKG, "ActionGetPOP.Listmails.Label" ) );
    props.setLook( wlListmails );
    FormData fdlListmails = new FormData();
    fdlListmails.left = new FormAttachment( 0, 0 );
    fdlListmails.right = new FormAttachment( middle, 0 );
    fdlListmails.top = new FormAttachment( wlPOP3Message, 2 * margin );
    wlListmails.setLayoutData(fdlListmails);
    wListmails = new CCombo(wPOP3Settings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wListmails.add( BaseMessages.getString( PKG, "ActionGetPOP.RetrieveAllMails.Label" ) );
    // PDI-7241 POP3 does not support retrive unread
    // wListmails.add( BaseMessages.getString( PKG, "ActionGetPOP.RetrieveUnreadMails.Label" ) );
    wListmails.add( BaseMessages.getString( PKG, "ActionGetPOP.RetrieveFirstMails.Label" ) );
    wListmails.select( 0 ); // +1: starts at -1

    props.setLook( wListmails );
    FormData fdListmails = new FormData();
    fdListmails.left = new FormAttachment( middle, 0 );
    fdListmails.top = new FormAttachment( wlPOP3Message, 2 * margin );
    fdListmails.right = new FormAttachment( 100, 0 );
    wListmails.setLayoutData(fdListmails);

    wListmails.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        chooseListMails();

      }
    } );

    // Retrieve the first ... mails
    wlFirstmails = new Label(wPOP3Settings, SWT.RIGHT );
    wlFirstmails.setText( BaseMessages.getString( PKG, "ActionGetPOP.Firstmails.Label" ) );
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

    // Delete mails after retrieval...
    wlDelete = new Label(wPOP3Settings, SWT.RIGHT );
    wlDelete.setText( BaseMessages.getString( PKG, "ActionGetPOP.DeleteMails.Label" ) );
    props.setLook( wlDelete );
    FormData fdlDelete = new FormData();
    fdlDelete.left = new FormAttachment( 0, 0 );
    fdlDelete.top = new FormAttachment( wFirstmails, margin );
    fdlDelete.right = new FormAttachment( middle, -margin );
    wlDelete.setLayoutData(fdlDelete);
    wDelete = new Button(wPOP3Settings, SWT.CHECK );
    props.setLook( wDelete );
    FormData fdDelete = new FormData();
    wDelete.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.DeleteMails.Tooltip" ) );
    fdDelete.left = new FormAttachment( middle, 0 );
    fdDelete.top = new FormAttachment( wFirstmails, margin );
    fdDelete.right = new FormAttachment( 100, 0 );
    wDelete.setLayoutData(fdDelete);

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
    wIMAPSettings.setText( BaseMessages.getString( PKG, "ActionGetPOP.IMAPSettings.Groupp.Label" ) );

    FormLayout IMAPSettingsgroupLayout = new FormLayout();
    IMAPSettingsgroupLayout.marginWidth = 10;
    IMAPSettingsgroupLayout.marginHeight = 10;
    wIMAPSettings.setLayout( IMAPSettingsgroupLayout );

    // SelectFolder button
    wSelectFolder = new Button(wIMAPSettings, SWT.PUSH );
    wSelectFolder.setText( BaseMessages.getString( PKG, "ActionGetPOP.SelectFolderConnection.Label" ) );
    props.setLook( wSelectFolder );
    FormData fdSelectFolder = new FormData();
    wSelectFolder.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.SelectFolderConnection.Tooltip" ) );
    fdSelectFolder.top = new FormAttachment(wPOP3Settings, margin );
    fdSelectFolder.right = new FormAttachment( 100, 0 );
    wSelectFolder.setLayoutData(fdSelectFolder);

    // TestIMAPFolder button
    wTestIMAPFolder = new Button(wIMAPSettings, SWT.PUSH );
    wTestIMAPFolder.setText( BaseMessages.getString( PKG, "ActionGetPOP.TestIMAPFolderConnection.Label" ) );
    props.setLook( wTestIMAPFolder );
    FormData fdTestIMAPFolder = new FormData();
    wTestIMAPFolder.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.TestIMAPFolderConnection.Tooltip" ) );
    fdTestIMAPFolder.top = new FormAttachment(wPOP3Settings, margin );
    fdTestIMAPFolder.right = new FormAttachment( wSelectFolder, -margin );
    wTestIMAPFolder.setLayoutData(fdTestIMAPFolder);

    // IMAPFolder line
    wlIMAPFolder = new Label(wIMAPSettings, SWT.RIGHT );
    wlIMAPFolder.setText( BaseMessages.getString( PKG, "ActionGetPOP.IMAPFolder.Label" ) );
    props.setLook( wlIMAPFolder );
    FormData fdlIMAPFolder = new FormData();
    fdlIMAPFolder.left = new FormAttachment( 0, 0 );
    fdlIMAPFolder.top = new FormAttachment(wPOP3Settings, margin );
    fdlIMAPFolder.right = new FormAttachment( middle, -margin );
    wlIMAPFolder.setLayoutData(fdlIMAPFolder);
    wIMAPFolder = new TextVar( variables, wIMAPSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wIMAPFolder );
    wIMAPFolder.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.IMAPFolder.Tooltip" ) );
    wIMAPFolder.addModifyListener( lsMod );
    FormData fdIMAPFolder = new FormData();
    fdIMAPFolder.left = new FormAttachment( middle, 0 );
    fdIMAPFolder.top = new FormAttachment(wPOP3Settings, margin );
    fdIMAPFolder.right = new FormAttachment( wTestIMAPFolder, -margin );
    wIMAPFolder.setLayoutData(fdIMAPFolder);

    // Include subfolders?
    wlIncludeSubFolders = new Label(wIMAPSettings, SWT.RIGHT );
    wlIncludeSubFolders.setText( BaseMessages.getString( PKG, "ActionGetPOP.IncludeSubFoldersMails.Label" ) );
    props.setLook( wlIncludeSubFolders );
    FormData fdlIncludeSubFolders = new FormData();
    fdlIncludeSubFolders.left = new FormAttachment( 0, 0 );
    fdlIncludeSubFolders.top = new FormAttachment( wIMAPFolder, margin );
    fdlIncludeSubFolders.right = new FormAttachment( middle, -margin );
    wlIncludeSubFolders.setLayoutData(fdlIncludeSubFolders);
    wIncludeSubFolders = new Button(wIMAPSettings, SWT.CHECK );
    props.setLook( wIncludeSubFolders );
    FormData fdIncludeSubFolders = new FormData();
    wIncludeSubFolders.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.IncludeSubFoldersMails.Tooltip" ) );
    fdIncludeSubFolders.left = new FormAttachment( middle, 0 );
    fdIncludeSubFolders.top = new FormAttachment( wIMAPFolder, margin );
    fdIncludeSubFolders.right = new FormAttachment( 100, 0 );
    wIncludeSubFolders.setLayoutData(fdIncludeSubFolders);
    wIncludeSubFolders.addSelectionListener( lsSelection );

    // List of mails of retrieve
    wlIMAPListmails = new Label(wIMAPSettings, SWT.RIGHT );
    wlIMAPListmails.setText( BaseMessages.getString( PKG, "ActionGetPOP.IMAPListmails.Label" ) );
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
    wlIMAPFirstmails.setText( BaseMessages.getString( PKG, "ActionGetPOP.IMAPFirstmails.Label" ) );
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

    // After get IMAP
    wlAfterGetIMAP = new Label(wIMAPSettings, SWT.RIGHT );
    wlAfterGetIMAP.setText( BaseMessages.getString( PKG, "ActionGetPOP.AfterGetIMAP.Label" ) );
    props.setLook( wlAfterGetIMAP );
    FormData fdlAfterGetIMAP = new FormData();
    fdlAfterGetIMAP.left = new FormAttachment( 0, 0 );
    fdlAfterGetIMAP.right = new FormAttachment( middle, -margin );
    fdlAfterGetIMAP.top = new FormAttachment( wIMAPFirstmails, 2 * margin );
    wlAfterGetIMAP.setLayoutData(fdlAfterGetIMAP);
    wAfterGetIMAP = new CCombo(wIMAPSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wAfterGetIMAP.setItems( MailConnectionMeta.afterGetIMAPDesc );
    wAfterGetIMAP.select( 0 ); // +1: starts at -1

    props.setLook( wAfterGetIMAP );
    FormData fdAfterGetIMAP = new FormData();
    fdAfterGetIMAP.left = new FormAttachment( middle, 0 );
    fdAfterGetIMAP.top = new FormAttachment( wIMAPFirstmails, 2 * margin );
    fdAfterGetIMAP.right = new FormAttachment( 100, 0 );
    wAfterGetIMAP.setLayoutData(fdAfterGetIMAP);

    wAfterGetIMAP.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setAfterIMAPRetrived();
        action.setChanged();
      }
    } );

    // MoveToFolder line
    wlMoveToFolder = new Label(wIMAPSettings, SWT.RIGHT );
    wlMoveToFolder.setText( BaseMessages.getString( PKG, "ActionGetPOP.MoveToFolder.Label" ) );
    props.setLook( wlMoveToFolder );
    FormData fdlMoveToFolder = new FormData();
    fdlMoveToFolder.left = new FormAttachment( 0, 0 );
    fdlMoveToFolder.top = new FormAttachment( wAfterGetIMAP, margin );
    fdlMoveToFolder.right = new FormAttachment( middle, -margin );
    wlMoveToFolder.setLayoutData(fdlMoveToFolder);

    // SelectMoveToFolder button
    wSelectMoveToFolder = new Button(wIMAPSettings, SWT.PUSH );
    wSelectMoveToFolder.setText( BaseMessages.getString( PKG, "ActionGetPOP.SelectMoveToFolderConnection.Label" ) );
    props.setLook( wSelectMoveToFolder );
    FormData fdSelectMoveToFolder = new FormData();
    wSelectMoveToFolder.setToolTipText( BaseMessages.getString(
      PKG, "ActionGetPOP.SelectMoveToFolderConnection.Tooltip" ) );
    fdSelectMoveToFolder.top = new FormAttachment( wAfterGetIMAP, margin );
    fdSelectMoveToFolder.right = new FormAttachment( 100, 0 );
    wSelectMoveToFolder.setLayoutData(fdSelectMoveToFolder);

    // TestMoveToFolder button
    wTestMoveToFolder = new Button(wIMAPSettings, SWT.PUSH );
    wTestMoveToFolder.setText( BaseMessages.getString( PKG, "ActionGetPOP.TestMoveToFolderConnection.Label" ) );
    props.setLook( wTestMoveToFolder );
    FormData fdTestMoveToFolder = new FormData();
    wTestMoveToFolder
      .setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.TestMoveToFolderConnection.Tooltip" ) );
    fdTestMoveToFolder.top = new FormAttachment( wAfterGetIMAP, margin );
    fdTestMoveToFolder.right = new FormAttachment( wSelectMoveToFolder, -margin );
    wTestMoveToFolder.setLayoutData(fdTestMoveToFolder);

    wMoveToFolder = new TextVar( variables, wIMAPSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMoveToFolder );
    wMoveToFolder.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.MoveToFolder.Tooltip" ) );
    wMoveToFolder.addModifyListener( lsMod );
    FormData fdMoveToFolder = new FormData();
    fdMoveToFolder.left = new FormAttachment( middle, 0 );
    fdMoveToFolder.top = new FormAttachment( wAfterGetIMAP, margin );
    fdMoveToFolder.right = new FormAttachment( wTestMoveToFolder, -margin );
    wMoveToFolder.setLayoutData(fdMoveToFolder);

    // Create move to folder
    wlcreateMoveToFolder = new Label(wIMAPSettings, SWT.RIGHT );
    wlcreateMoveToFolder.setText( BaseMessages.getString( PKG, "ActionGetPOP.createMoveToFolderMails.Label" ) );
    props.setLook( wlcreateMoveToFolder );
    FormData fdlcreateMoveToFolder = new FormData();
    fdlcreateMoveToFolder.left = new FormAttachment( 0, 0 );
    fdlcreateMoveToFolder.top = new FormAttachment( wMoveToFolder, margin );
    fdlcreateMoveToFolder.right = new FormAttachment( middle, -margin );
    wlcreateMoveToFolder.setLayoutData(fdlcreateMoveToFolder);
    wcreateMoveToFolder = new Button(wIMAPSettings, SWT.CHECK );
    props.setLook( wcreateMoveToFolder );
    FormData fdcreateMoveToFolder = new FormData();
    wcreateMoveToFolder
      .setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.createMoveToFolderMails.Tooltip" ) );
    fdcreateMoveToFolder.left = new FormAttachment( middle, 0 );
    fdcreateMoveToFolder.top = new FormAttachment( wMoveToFolder, margin );
    fdcreateMoveToFolder.right = new FormAttachment( 100, 0 );
    wcreateMoveToFolder.setLayoutData(fdcreateMoveToFolder);

    FormData fdIMAPSettings = new FormData();
    fdIMAPSettings.left = new FormAttachment( 0, margin );
    fdIMAPSettings.top = new FormAttachment(wPOP3Settings, 2 * margin );
    fdIMAPSettings.right = new FormAttachment( 100, -margin );
    wIMAPSettings.setLayoutData(fdIMAPSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF IMAP SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdSettingsComp = new FormData();
    fdSettingsComp.left = new FormAttachment( 0, 0 );
    fdSettingsComp.top = new FormAttachment( wName, 0 );
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
    wSearchTab.setText( BaseMessages.getString( PKG, "ActionGetPOP.Tab.Search.Label" ) );
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
    wHeader.setText( BaseMessages.getString( PKG, "ActionGetPOP.Header.Group.Label" ) );

    FormLayout HeadergroupLayout = new FormLayout();
    HeadergroupLayout.marginWidth = 10;
    HeadergroupLayout.marginHeight = 10;
    wHeader.setLayout( HeadergroupLayout );

    wNegateSender = new Button(wHeader, SWT.CHECK );
    props.setLook( wNegateSender );
    FormData fdNegateSender = new FormData();
    wNegateSender.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.NegateSender.Tooltip" ) );
    fdNegateSender.top = new FormAttachment( 0, margin );
    fdNegateSender.right = new FormAttachment( 100, -margin );
    wNegateSender.setLayoutData(fdNegateSender);

    // From line
    Label wlSender = new Label(wHeader, SWT.RIGHT);
    wlSender.setText( BaseMessages.getString( PKG, "ActionGetPOP.wSender.Label" ) );
    props.setLook(wlSender);
    FormData fdlSender = new FormData();
    fdlSender.left = new FormAttachment( 0, 0 );
    fdlSender.top = new FormAttachment( 0, margin );
    fdlSender.right = new FormAttachment( middle, -margin );
    wlSender.setLayoutData(fdlSender);
    wSender = new TextVar( variables, wHeader, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSender );
    wSender.addModifyListener( lsMod );
    FormData fdSender = new FormData();
    fdSender.left = new FormAttachment( middle, 0 );
    fdSender.top = new FormAttachment( 0, margin );
    fdSender.right = new FormAttachment( wNegateSender, -margin );
    wSender.setLayoutData(fdSender);

    wNegateReceipient = new Button(wHeader, SWT.CHECK );
    props.setLook( wNegateReceipient );
    FormData fdNegateReceipient = new FormData();
    wNegateReceipient.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.NegateReceipient.Tooltip" ) );
    fdNegateReceipient.top = new FormAttachment( wSender, margin );
    fdNegateReceipient.right = new FormAttachment( 100, -margin );
    wNegateReceipient.setLayoutData(fdNegateReceipient);

    // Receipient line
    Label wlReceipient = new Label(wHeader, SWT.RIGHT);
    wlReceipient.setText( BaseMessages.getString( PKG, "ActionGetPOP.Receipient.Label" ) );
    props.setLook(wlReceipient);
    FormData fdlReceipient = new FormData();
    fdlReceipient.left = new FormAttachment( 0, 0 );
    fdlReceipient.top = new FormAttachment( wSender, margin );
    fdlReceipient.right = new FormAttachment( middle, -margin );
    wlReceipient.setLayoutData(fdlReceipient);
    wReceipient = new TextVar( variables, wHeader, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wReceipient );
    wReceipient.addModifyListener( lsMod );
    FormData fdReceipient = new FormData();
    fdReceipient.left = new FormAttachment( middle, 0 );
    fdReceipient.top = new FormAttachment( wSender, margin );
    fdReceipient.right = new FormAttachment( wNegateReceipient, -margin );
    wReceipient.setLayoutData(fdReceipient);

    wNegateSubject = new Button(wHeader, SWT.CHECK );
    props.setLook( wNegateSubject );
    FormData fdNegateSubject = new FormData();
    wNegateSubject.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.NegateSubject.Tooltip" ) );
    fdNegateSubject.top = new FormAttachment( wReceipient, margin );
    fdNegateSubject.right = new FormAttachment( 100, -margin );
    wNegateSubject.setLayoutData(fdNegateSubject);

    // Subject line
    Label wlSubject = new Label(wHeader, SWT.RIGHT);
    wlSubject.setText( BaseMessages.getString( PKG, "ActionGetPOP.Subject.Label" ) );
    props.setLook(wlSubject);
    FormData fdlSubject = new FormData();
    fdlSubject.left = new FormAttachment( 0, 0 );
    fdlSubject.top = new FormAttachment( wReceipient, margin );
    fdlSubject.right = new FormAttachment( middle, -margin );
    wlSubject.setLayoutData(fdlSubject);
    wSubject = new TextVar( variables, wHeader, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSubject );
    wSubject.addModifyListener( lsMod );
    FormData fdSubject = new FormData();
    fdSubject.left = new FormAttachment( middle, 0 );
    fdSubject.top = new FormAttachment( wReceipient, margin );
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
    // START OF CONTENT GROUP///
    // /
    Group wContent = new Group(wSearchComp, SWT.SHADOW_NONE);
    props.setLook(wContent);
    wContent.setText( BaseMessages.getString( PKG, "ActionGetPOP.Content.Group.Label" ) );

    FormLayout ContentgroupLayout = new FormLayout();
    ContentgroupLayout.marginWidth = 10;
    ContentgroupLayout.marginHeight = 10;
    wContent.setLayout( ContentgroupLayout );

    wNegateBody = new Button(wContent, SWT.CHECK );
    props.setLook( wNegateBody );
    FormData fdNegateBody = new FormData();
    wNegateBody.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.NegateBody.Tooltip" ) );
    fdNegateBody.top = new FormAttachment(wHeader, margin );
    fdNegateBody.right = new FormAttachment( 100, -margin );
    wNegateBody.setLayoutData(fdNegateBody);

    // Body line
    Label wlBody = new Label(wContent, SWT.RIGHT);
    wlBody.setText( BaseMessages.getString( PKG, "ActionGetPOP.Body.Label" ) );
    props.setLook(wlBody);
    FormData fdlBody = new FormData();
    fdlBody.left = new FormAttachment( 0, 0 );
    fdlBody.top = new FormAttachment(wHeader, margin );
    fdlBody.right = new FormAttachment( middle, -margin );
    wlBody.setLayoutData(fdlBody);
    wBody = new TextVar( variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBody );
    wBody.addModifyListener( lsMod );
    FormData fdBody = new FormData();
    fdBody.left = new FormAttachment( middle, 0 );
    fdBody.top = new FormAttachment(wHeader, margin );
    fdBody.right = new FormAttachment( wNegateBody, -margin );
    wBody.setLayoutData(fdBody);

    FormData fdContent = new FormData();
    fdContent.left = new FormAttachment( 0, margin );
    fdContent.top = new FormAttachment(wHeader, margin );
    fdContent.right = new FormAttachment( 100, -margin );
    wContent.setLayoutData(fdContent);
    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF RECEIVED DATE ROUP///
    // /
    Group wReceivedDate = new Group(wSearchComp, SWT.SHADOW_NONE);
    props.setLook(wReceivedDate);
    wReceivedDate.setText( BaseMessages.getString( PKG, "ActionGetPOP.ReceivedDate.Group.Label" ) );

    FormLayout ReceivedDategroupLayout = new FormLayout();
    ReceivedDategroupLayout.marginWidth = 10;
    ReceivedDategroupLayout.marginHeight = 10;
    wReceivedDate.setLayout( ReceivedDategroupLayout );

    wNegateReceivedDate = new Button(wReceivedDate, SWT.CHECK );
    props.setLook( wNegateReceivedDate );
    FormData fdNegateReceivedDate = new FormData();
    wNegateReceivedDate.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.NegateReceivedDate.Tooltip" ) );
    fdNegateReceivedDate.top = new FormAttachment(wContent, margin );
    fdNegateReceivedDate.right = new FormAttachment( 100, -margin );
    wNegateReceivedDate.setLayoutData(fdNegateReceivedDate);

    // Received Date Condition
    wlConditionOnReceivedDate = new Label(wReceivedDate, SWT.RIGHT );
    wlConditionOnReceivedDate.setText( BaseMessages.getString( PKG, "ActionGetPOP.ConditionOnReceivedDate.Label" ) );
    props.setLook( wlConditionOnReceivedDate );
    FormData fdlConditionOnReceivedDate = new FormData();
    fdlConditionOnReceivedDate.left = new FormAttachment( 0, 0 );
    fdlConditionOnReceivedDate.right = new FormAttachment( middle, -margin );
    fdlConditionOnReceivedDate.top = new FormAttachment(wContent, margin );
    wlConditionOnReceivedDate.setLayoutData(fdlConditionOnReceivedDate);

    wConditionOnReceivedDate = new CCombo(wReceivedDate, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wConditionOnReceivedDate.setItems( MailConnectionMeta.conditionDateDesc );
    wConditionOnReceivedDate.select( 0 ); // +1: starts at -1

    props.setLook( wConditionOnReceivedDate );
    FormData fdConditionOnReceivedDate = new FormData();
    fdConditionOnReceivedDate.left = new FormAttachment( middle, 0 );
    fdConditionOnReceivedDate.top = new FormAttachment(wContent, margin );
    fdConditionOnReceivedDate.right = new FormAttachment( wNegateReceivedDate, -margin );
    wConditionOnReceivedDate.setLayoutData(fdConditionOnReceivedDate);
    wConditionOnReceivedDate.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        conditionReceivedDate();
        action.setChanged();
      }
    } );

    open = new Button(wReceivedDate, SWT.PUSH );
    open.setImage( GuiResource.getInstance().getImageCalendar() );
    open.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.OpenCalendar" ) );
    FormData fdlButton = new FormData();
    fdlButton.top = new FormAttachment( wConditionOnReceivedDate, margin );
    fdlButton.right = new FormAttachment( 100, 0 );
    open.setLayoutData( fdlButton );
    open.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        final Shell dialog = new Shell( shell, SWT.DIALOG_TRIM );
        dialog.setText( BaseMessages.getString( PKG, "ActionGetPOP.SelectDate" ) );
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

            wReadFrom.setText( new SimpleDateFormat( ActionGetPOP.DATE_PATTERN ).format( cal.getTime() ) );

            dialog.close();
          }
        } );
        dialog.setDefaultButton( ok );
        dialog.pack();
        dialog.open();
      }
    } );

    wlReadFrom = new Label(wReceivedDate, SWT.RIGHT );
    wlReadFrom.setText( BaseMessages.getString( PKG, "ActionGetPOP.ReadFrom.Label" ) );
    props.setLook( wlReadFrom );
    FormData fdlReadFrom = new FormData();
    fdlReadFrom.left = new FormAttachment( 0, 0 );
    fdlReadFrom.top = new FormAttachment( wConditionOnReceivedDate, margin );
    fdlReadFrom.right = new FormAttachment( middle, -margin );
    wlReadFrom.setLayoutData(fdlReadFrom);
    wReadFrom = new TextVar( variables, wReceivedDate, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wReadFrom.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.ReadFrom.Tooltip" ) );
    props.setLook( wReadFrom );
    wReadFrom.addModifyListener( lsMod );
    FormData fdReadFrom = new FormData();
    fdReadFrom.left = new FormAttachment( middle, 0 );
    fdReadFrom.top = new FormAttachment( wConditionOnReceivedDate, margin );
    fdReadFrom.right = new FormAttachment( open, -margin );
    wReadFrom.setLayoutData(fdReadFrom);

    opento = new Button(wReceivedDate, SWT.PUSH );
    opento.setImage( GuiResource.getInstance().getImageCalendar() );
    opento.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.OpenCalendar" ) );
    FormData fdlButtonto = new FormData();
    fdlButtonto.top = new FormAttachment( wReadFrom, 2 * margin );
    fdlButtonto.right = new FormAttachment( 100, 0 );
    opento.setLayoutData( fdlButtonto );
    opento.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        final Shell dialogto = new Shell( shell, SWT.DIALOG_TRIM );
        dialogto.setText( BaseMessages.getString( PKG, "ActionGetPOP.SelectDate" ) );
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

            wReadTo.setText( new SimpleDateFormat( ActionGetPOP.DATE_PATTERN ).format( cal.getTime() ) );
            dialogto.close();
          }
        } );
        dialogto.setDefaultButton( okto );
        dialogto.pack();
        dialogto.open();
      }
    } );

    wlReadTo = new Label(wReceivedDate, SWT.RIGHT );
    wlReadTo.setText( BaseMessages.getString( PKG, "ActionGetPOP.ReadTo.Label" ) );
    props.setLook( wlReadTo );
    FormData fdlReadTo = new FormData();
    fdlReadTo.left = new FormAttachment( 0, 0 );
    fdlReadTo.top = new FormAttachment( wReadFrom, 2 * margin );
    fdlReadTo.right = new FormAttachment( middle, -margin );
    wlReadTo.setLayoutData(fdlReadTo);
    wReadTo = new TextVar( variables, wReceivedDate, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wReadTo.setToolTipText( BaseMessages.getString( PKG, "ActionGetPOP.ReadTo.Tooltip" ) );
    props.setLook( wReadTo );
    wReadTo.addModifyListener( lsMod );
    FormData fdReadTo = new FormData();
    fdReadTo.left = new FormAttachment( middle, 0 );
    fdReadTo.top = new FormAttachment( wReadFrom, 2 * margin );
    fdReadTo.right = new FormAttachment( opento, -margin );
    wReadTo.setLayoutData(fdReadTo);

    FormData fdReceivedDate = new FormData();
    fdReceivedDate.left = new FormAttachment( 0, margin );
    fdReceivedDate.top = new FormAttachment(wContent, margin );
    fdReceivedDate.right = new FormAttachment( 100, -margin );
    wReceivedDate.setLayoutData(fdReceivedDate);
    // ///////////////////////////////////////////////////////////
    // / END OF RECEIVED DATE GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdSearchComp = new FormData();
    fdSearchComp.left = new FormAttachment( 0, 0 );
    fdSearchComp.top = new FormAttachment( wName, 0 );
    fdSearchComp.right = new FormAttachment( 100, 0 );
    fdSearchComp.bottom = new FormAttachment( 100, 0 );
    wSearchComp.setLayoutData(fdSearchComp);

    wSearchComp.layout();
    wSearchTab.setControl(wSearchComp);
    props.setLook(wSearchComp);

    // ////////////////////////////////
    // / END OF SEARCH TAB
    // ////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData(fdTabFolder);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, wTabFolder);

    // Add listeners
    Listener lsCancel = e -> cancel();
    Listener lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel);
    wOk.addListener( SWT.Selection, lsOk);

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };
    Listener lsTest = e -> test();
    wTest.addListener( SWT.Selection, lsTest);

    Listener lsTestIMAPFolder = e -> checkFolder(variables.resolve(wIMAPFolder.getText()));
    wTestIMAPFolder.addListener( SWT.Selection, lsTestIMAPFolder);

    Listener lsTestMoveToFolder = e -> checkFolder(variables.resolve(wMoveToFolder.getText()));
    wTestMoveToFolder.addListener( SWT.Selection, lsTestMoveToFolder);

    Listener lsSelectFolder = e -> selectFolder(wIMAPFolder);
    wSelectFolder.addListener( SWT.Selection, lsSelectFolder);

    Listener lsSelectMoveToFolder = e -> selectFolder(wMoveToFolder);
    wSelectMoveToFolder.addListener( SWT.Selection, lsSelectMoveToFolder);

    wName.addSelectionListener(lsDef);
    wServerName.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    setUserProxy();
    chooseListMails();
    activeAttachmentFolder();
    refreshProtocol( false );
    conditionReceivedDate();
    wTabFolder.setSelection( 0 );
    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "ActionGetPOPDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  private void setUserProxy() {
    wlProxyUsername.setEnabled( wUseProxy.getSelection() );
    wProxyUsername.setEnabled( wUseProxy.getSelection() );
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
      String realpass = action.getRealPassword( variables.resolve( wPassword.getText() ) );
      int realport = Const.toInt( variables.resolve( wPort.getText() ), -1 );
      String realproxyuser = variables.resolve( wProxyUsername.getText() );
      try {
        mailConn =
          new MailConnection(
            LogChannel.UI, MailConnectionMeta.getProtocolFromString(
            wProtocol.getText(), MailConnectionMeta.PROTOCOL_IMAP ), realserver, realport, realuser,
            realpass, wUseSSL.getSelection(), wUseProxy.getSelection(), realproxyuser );
        mailConn.connect();

        retval = true;
      } catch ( Exception e ) {
        errordescription = e.getMessage();
      }
    }

    if ( !retval ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "ActionGetPOP.Connected.NOK.ConnectionBad", wServerName.getText() )
        + Const.CR + Const.NVL( errordescription, "" ) );
      mb.setText( BaseMessages.getString( PKG, "ActionGetPOP.Connected.Title.Bad" ) );
      mb.open();
    }

    return ( mailConn.isConnected() );

  }

  private void test() {
    if ( connect() ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "ActionGetPOP.Connected.OK", wServerName.getText() ) + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "ActionGetPOP.Connected.Title.Ok" ) );
      mb.open();
    }
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

  private void checkFolder( String folderName ) {
    if ( !Utils.isEmpty( folderName ) ) {
      if ( connect() ) {
        // check folder
        if ( mailConn.folderExists( folderName ) ) {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "ActionGetPOP.IMAPFolderExists.OK", folderName ) + Const.CR );
          mb.setText( BaseMessages.getString( PKG, "ActionGetPOP.IMAPFolderExists.Title.Ok" ) );
          mb.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "ActionGetPOP.Connected.NOK.IMAPFolderExists", folderName )
            + Const.CR );
          mb.setText( BaseMessages.getString( PKG, "ActionGetPOP.IMAPFolderExists.Title.Bad" ) );
          mb.open();
        }
      }
    }
  }

  private void closeMailConnection() {
    try {
      if ( mailConn != null ) {
        mailConn.disconnect();
        mailConn = null;
      }
    } catch ( Exception e ) {
      // Ignore
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

  private void activeAttachmentFolder() {
    boolean getmessages =
      MailConnectionMeta.getActionTypeByDesc( wActionType.getText() ) == MailConnectionMeta.ACTION_TYPE_GET;
    wlDifferentFolderForAttachment.setEnabled( getmessages && wGetAttachment.getSelection() );
    wDifferentFolderForAttachment.setEnabled( getmessages && wGetAttachment.getSelection() );
    boolean activeattachmentfolder =
      ( wGetAttachment.getSelection() && wDifferentFolderForAttachment.getSelection() );
    wlAttachmentFolder.setEnabled( getmessages && activeattachmentfolder );
    wAttachmentFolder.setEnabled( getmessages && activeattachmentfolder );
    wbAttachmentFolder.setEnabled( getmessages && activeattachmentfolder );
    if ( !wGetAttachment.getSelection() && !wGetMessage.getSelection() ) {
      wGetMessage.setSelection( true );
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
      } else {
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
      }

    }
  }

  private void refreshProtocol( boolean refreshport ) {
    checkUnavailableMode();
    boolean activePOP3 = wProtocol.getText().equals( MailConnectionMeta.PROTOCOL_STRING_POP3 );
    wlPOP3Message.setEnabled( activePOP3 );
    wlListmails.setEnabled( activePOP3 );
    wListmails.setEnabled( activePOP3 );
    wlFirstmails.setEnabled( activePOP3 );
    wlDelete.setEnabled( activePOP3 );
    wDelete.setEnabled( activePOP3 );

    wlIMAPFirstmails.setEnabled( !activePOP3 );
    wIMAPFirstmails.setEnabled( !activePOP3 );
    wlIMAPFolder.setEnabled( !activePOP3 );
    wIMAPFolder.setEnabled( !activePOP3 );
    wlIncludeSubFolders.setEnabled( !activePOP3 );
    wIncludeSubFolders.setEnabled( !activePOP3 );
    wlIMAPListmails.setEnabled( !activePOP3 );
    wIMAPListmails.setEnabled( !activePOP3 );
    wTestIMAPFolder.setEnabled( !activePOP3 );
    wSelectFolder.setEnabled( !activePOP3 );
    wlAfterGetIMAP.setEnabled( !activePOP3 );
    wAfterGetIMAP.setEnabled( !activePOP3 );

    if ( activePOP3 ) {
      // clear out selections
      wConditionOnReceivedDate.select( 0 );
      conditionReceivedDate();
    }
    // POP3 protocol does not provide information about when a message was received
    wConditionOnReceivedDate.setEnabled( !activePOP3 );
    wNegateReceivedDate.setEnabled( !activePOP3 );
    wlConditionOnReceivedDate.setEnabled( !activePOP3 );

    chooseListMails();
    refreshPort( refreshport );
    setActionType();
  }

  private void checkUnavailableMode() {
    if ( wProtocol.getText().equals( MailConnectionMeta.PROTOCOL_STRING_POP3 )
      && MailConnectionMeta.getActionTypeByDesc( wActionType.getText() ) == MailConnectionMeta.ACTION_TYPE_MOVE ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( "This action is not available for POP3!"
        + Const.CR + "Only one Folder (INBOX) is available in POP3." + Const.CR
        + "If you want to move messages to another folder," + Const.CR + "please use IMAP protocol." );
      mb.setText( "ERROR" );
      mb.open();
      wActionType.setText( MailConnectionMeta.getActionTypeDesc( MailConnectionMeta.ACTION_TYPE_GET ) );
    }
  }

  private void setActionType() {
    checkUnavailableMode();
    if ( MailConnectionMeta.getActionTypeByDesc( wActionType.getText() ) != MailConnectionMeta.ACTION_TYPE_GET ) {
      wAfterGetIMAP.setText( MailConnectionMeta.getAfterGetIMAPDesc( MailConnectionMeta.AFTER_GET_IMAP_NOTHING ) );
    }

    boolean getmessages =
      MailConnectionMeta.getActionTypeByDesc( wActionType.getText() ) == MailConnectionMeta.ACTION_TYPE_GET;

    wlOutputDirectory.setEnabled( getmessages );
    wOutputDirectory.setEnabled( getmessages );
    wbDirectory.setEnabled( getmessages );
    wlcreateLocalFolder.setEnabled( getmessages );
    wcreateLocalFolder.setEnabled( getmessages );
    wFilenamePattern.setEnabled( getmessages );
    wlFilenamePattern.setEnabled( getmessages );
    wlAttachmentWildcard.setEnabled( getmessages );
    wAttachmentWildcard.setEnabled( getmessages );
    wlDifferentFolderForAttachment.setEnabled( getmessages );
    wDifferentFolderForAttachment.setEnabled( getmessages );
    wlGetAttachment.setEnabled( getmessages );
    wGetAttachment.setEnabled( getmessages );
    wlGetMessage.setEnabled( getmessages );
    wGetMessage.setEnabled( getmessages );

    wlAfterGetIMAP
      .setEnabled( getmessages && wProtocol.getText().equals( MailConnectionMeta.PROTOCOL_STRING_IMAP ) );
    wAfterGetIMAP
      .setEnabled( getmessages && wProtocol.getText().equals( MailConnectionMeta.PROTOCOL_STRING_IMAP ) );

    setAfterIMAPRetrived();
  }

  private void setAfterIMAPRetrived() {
    boolean activeMoveToFolfer =
      ( ( ( wProtocol.getText().equals( MailConnectionMeta.PROTOCOL_STRING_IMAP ) ) && ( MailConnectionMeta
        .getActionTypeByDesc( wActionType.getText() ) == MailConnectionMeta.ACTION_TYPE_MOVE ) ) || ( MailConnectionMeta
        .getAfterGetIMAPByDesc( wAfterGetIMAP.getText() ) == MailConnectionMeta.AFTER_GET_IMAP_MOVE ) );
    wlMoveToFolder.setEnabled( activeMoveToFolfer );
    wMoveToFolder.setEnabled( activeMoveToFolfer );
    wTestMoveToFolder.setEnabled( activeMoveToFolfer );
    wSelectMoveToFolder.setEnabled( activeMoveToFolfer );
    wlcreateMoveToFolder.setEnabled( activeMoveToFolfer );
    wcreateMoveToFolder.setEnabled( activeMoveToFolfer );
  }

  public void chooseListMails() {
    boolean ok =
      ( wProtocol.getText().equals( MailConnectionMeta.PROTOCOL_STRING_POP3 ) && wListmails.getSelectionIndex() == 1 );
    wlFirstmails.setEnabled( ok );
    wFirstmails.setEnabled( ok );
  }

  public void dispose() {
    closeMailConnection();
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( action.getName() != null ) {
      wName.setText( action.getName() );
    }
    if ( action.getServerName() != null ) {
      wServerName.setText( action.getServerName() );
    }
    if ( action.getUserName() != null ) {
      wUserName.setText( action.getUserName() );
    }
    if ( action.getPassword() != null ) {
      wPassword.setText( action.getPassword() );
    }

    wUseSSL.setSelection( action.isUseSSL() );
    wGetMessage.setSelection( action.isSaveMessage() );
    wGetAttachment.setSelection( action.isSaveAttachment() );
    wDifferentFolderForAttachment.setSelection( action.isDifferentFolderForAttachment() );
    if ( action.getAttachmentFolder() != null ) {
      wAttachmentFolder.setText( action.getAttachmentFolder() );
    }

    if ( action.getPort() != null ) {
      wPort.setText( action.getPort() );
    }

    if ( action.getOutputDirectory() != null ) {
      wOutputDirectory.setText( action.getOutputDirectory() );
    }
    if ( action.getFilenamePattern() != null ) {
      wFilenamePattern.setText( action.getFilenamePattern() );
    }
    if ( action.getAttachmentWildcard() != null ) {
      wAttachmentWildcard.setText( action.getAttachmentWildcard() );
    }

    String protocol = action.getProtocol();
    boolean isPop3 = StringUtils.equals( protocol, MailConnectionMeta.PROTOCOL_STRING_POP3 );
    wProtocol.setText( protocol );
    int i = action.getRetrievemails();

    if ( i > 0 ) {
      if ( isPop3 ) {
        wListmails.select( i - 1 );
      } else {
        wListmails.select( i );
      }
    } else {
      wListmails.select( 0 ); // Retrieve All Mails
    }

    if ( action.getFirstMails() != null ) {
      wFirstmails.setText( action.getFirstMails() );
    }

    wDelete.setSelection( action.getDelete() );
    wIMAPListmails.setText( MailConnectionMeta.getValueImapListDesc( action.getValueImapList() ) );
    if ( action.getIMAPFolder() != null ) {
      wIMAPFolder.setText( action.getIMAPFolder() );
    }
    // search term
    if ( action.getSenderSearchTerm() != null ) {
      wSender.setText( action.getSenderSearchTerm() );
    }
    wNegateSender.setSelection( action.isNotTermSenderSearch() );
    if ( action.getReceipientSearch() != null ) {
      wReceipient.setText( action.getReceipientSearch() );
    }
    wNegateReceipient.setSelection( action.isNotTermReceipientSearch() );
    if ( action.getSubjectSearch() != null ) {
      wSubject.setText( action.getSubjectSearch() );
    }
    wNegateSubject.setSelection( action.isNotTermSubjectSearch() );
    if ( action.getBodySearch() != null ) {
      wBody.setText( action.getBodySearch() );
    }
    wNegateBody.setSelection( action.isNotTermBodySearch() );
    wConditionOnReceivedDate.setText( MailConnectionMeta.getConditionDateDesc( action.getConditionOnReceivedDate() ) );
    wNegateReceivedDate.setSelection( action.isNotTermReceivedDateSearch() );
    if ( action.getReceivedDate1() != null ) {
      wReadFrom.setText( action.getReceivedDate1() );
    }
    if ( action.getReceivedDate2() != null ) {
      wReadTo.setText( action.getReceivedDate2() );
    }
    wActionType.setText( MailConnectionMeta.getActionTypeDesc( action.getActionType() ) );
    wcreateMoveToFolder.setSelection( action.isCreateMoveToFolder() );
    wcreateLocalFolder.setSelection( action.isCreateLocalFolder() );
    if ( action.getMoveToIMAPFolder() != null ) {
      wMoveToFolder.setText( action.getMoveToIMAPFolder() );
    }
    wAfterGetIMAP.setText( MailConnectionMeta.getAfterGetIMAPDesc( action.getAfterGetIMAP() ) );
    wIncludeSubFolders.setSelection( action.isIncludeSubFolders() );
    wUseProxy.setSelection( action.isUseProxy() );
    if ( action.getProxyUsername() != null ) {
      wProxyUsername.setText( action.getProxyUsername() );
    }
    if ( action.getFirstIMAPMails() != null ) {
      wIMAPFirstmails.setText( action.getFirstIMAPMails() );
    }

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged( changed );
    action = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "ActionGetPOP.NoNameMessageBox.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "ActionGetPOP.NoNameMessageBox.Text" ) );
      mb.open();
      return;
    }
    action.setName( wName.getText() );
    action.setServerName( wServerName.getText() );
    action.setUserName( wUserName.getText() );
    action.setPassword( wPassword.getText() );
    action.setUseSSL( wUseSSL.getSelection() );
    action.setSaveAttachment( wGetAttachment.getSelection() );
    action.setSaveMessage( wGetMessage.getSelection() );
    action.setDifferentFolderForAttachment( wDifferentFolderForAttachment.getSelection() );
    action.setAttachmentFolder( wAttachmentFolder.getText() );
    action.setPort( wPort.getText() );
    action.setOutputDirectory( wOutputDirectory.getText() );
    action.setFilenamePattern( wFilenamePattern.getText() );

    // [PDI-7241] Option 'retrieve unread' is removed and there is only 2 options.
    // for backward compatibility: 0 is 'retrieve all', 1 is 'retrieve first...'
    int actualIndex = wListmails.getSelectionIndex();
    action.setRetrievemails( actualIndex > 0 ? 2 : 0 );

    action.setFirstMails( wFirstmails.getText() );
    action.setDelete( wDelete.getSelection() );
    action.setProtocol( wProtocol.getText() );
    action.setAttachmentWildcard( wAttachmentWildcard.getText() );
    action.setValueImapList( MailConnectionMeta.getValueImapListByDesc( wIMAPListmails.getText() ) );
    action.setFirstIMAPMails( wIMAPFirstmails.getText() );
    action.setIMAPFolder( wIMAPFolder.getText() );
    // search term
    action.setSenderSearchTerm( wSender.getText() );
    action.setNotTermSenderSearch( wNegateSender.getSelection() );

    action.setReceipientSearch( wReceipient.getText() );
    action.setNotTermReceipientSearch( wNegateReceipient.getSelection() );
    action.setSubjectSearch( wSubject.getText() );
    action.setNotTermSubjectSearch( wNegateSubject.getSelection() );
    action.setBodySearch( wBody.getText() );
    action.setNotTermBodySearch( wNegateBody.getSelection() );
    action.setConditionOnReceivedDate( MailConnectionMeta.getConditionDateByDesc( wConditionOnReceivedDate
      .getText() ) );
    action.setNotTermReceivedDateSearch( wNegateReceivedDate.getSelection() );
    action.setReceivedDate1( wReadFrom.getText() );
    action.setReceivedDate2( wReadTo.getText() );
    action.setActionType( MailConnectionMeta.getActionTypeByDesc( wActionType.getText() ) );
    action.setMoveToIMAPFolder( wMoveToFolder.getText() );
    action.setCreateMoveToFolder( wcreateMoveToFolder.getSelection() );
    action.setCreateLocalFolder( wcreateLocalFolder.getSelection() );
    action.setAfterGetIMAP( MailConnectionMeta.getAfterGetIMAPByDesc( wAfterGetIMAP.getText() ) );
    action.setIncludeSubFolders( wIncludeSubFolders.getSelection() );
    action.setUseProxy( wUseProxy.getSelection() );
    action.setProxyUsername( wProxyUsername.getText() );
    dispose();
  }
}
