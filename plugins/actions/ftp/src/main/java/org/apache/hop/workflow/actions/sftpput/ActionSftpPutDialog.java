/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.workflow.actions.sftpput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.actions.sftp.SftpClient;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.net.InetAddress;

/**
 * This dialog allows you to edit the FTP Put action settings.
 *
 * @author Matt
 * @since 19-06-2003
 */
public class ActionSftpPutDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionSftpPut.class; // for i18n purposes, needed by Translator!!
  private static final String[] FILETYPES = new String[] {
    BaseMessages.getString( PKG, "JobSFTPPUT.Filetype.Pem" ),
    BaseMessages.getString( PKG, "JobSFTPPUT.Filetype.All" ) };

  private Text wName;

  private TextVar wServerName;

  private TextVar wServerPort;

  private TextVar wUserName;

  private TextVar wPassword;

  private TextVar wScpDirectory;

  private Label wlLocalDirectory;
  private TextVar wLocalDirectory;

  private Label wlWildcard;
  private TextVar wWildcard;

  private ActionSftpPut action;
  private Shell shell;

  private Button wCreateRemoteFolder;

  private Button wbLocalDirectory;

  private boolean changed;

  private Button wbTestChangeFolderExists;

  private Button wgetPrevious;

  private Button wgetPreviousFiles;

  private Button wSuccessWhenNoFile;

  private Label wlAddFilenameToResult;

  private Button wAddFilenameToResult;

  private LabelTextVar wkeyfilePass;

  private Button wusePublicKey;

  private Label wlKeyFilename;

  private Button wbKeyFilename;

  private TextVar wKeyFilename;

  private CCombo wCompression;

  private CCombo wProxyType;

  private LabelTextVar wProxyHost;
  private LabelTextVar wProxyPort;
  private LabelTextVar wProxyUsername;
  private LabelTextVar wProxyPassword;

  private CCombo wAfterFtpPut;

  private Label wlCreateDestinationFolder;
  private Button wCreateDestinationFolder;

  private Label wlDestinationFolder;
  private TextVar wDestinationFolder;

  private Button wbMovetoDirectory;

  private SftpClient sftpclient = null;

  public ActionSftpPutDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionSftpPut) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobSFTPPUT.Title" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    WorkflowMeta workflowMeta = getWorkflowMeta();
    
    ModifyListener lsMod = e -> {
      sftpclient = null;
      action.setChanged();
    };
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobSFTPPUT.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobSFTPPUT.Name.Label" ) );
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
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobSFTPPUT.Tab.General.Label" ) );

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
    wServerSettings.setText( BaseMessages.getString( PKG, "JobSFTPPUT.ServerSettings.Group.Label" ) );
    FormLayout ServerSettingsgroupLayout = new FormLayout();
    ServerSettingsgroupLayout.marginWidth = 10;
    ServerSettingsgroupLayout.marginHeight = 10;
    wServerSettings.setLayout( ServerSettingsgroupLayout );

    // ServerName line
    Label wlServerName = new Label(wServerSettings, SWT.RIGHT);
    wlServerName.setText( BaseMessages.getString( PKG, "JobSFTPPUT.Server.Label" ) );
    props.setLook(wlServerName);
    FormData fdlServerName = new FormData();
    fdlServerName.left = new FormAttachment( 0, 0 );
    fdlServerName.top = new FormAttachment( wName, margin );
    fdlServerName.right = new FormAttachment( middle, -margin );
    wlServerName.setLayoutData(fdlServerName);
    wServerName = new TextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wServerName );
    wServerName.addModifyListener( lsMod );
    FormData fdServerName = new FormData();
    fdServerName.left = new FormAttachment( middle, 0 );
    fdServerName.top = new FormAttachment( wName, margin );
    fdServerName.right = new FormAttachment( 100, 0 );
    wServerName.setLayoutData(fdServerName);

    // ServerPort line
    Label wlServerPort = new Label(wServerSettings, SWT.RIGHT);
    wlServerPort.setText( BaseMessages.getString( PKG, "JobSFTPPUT.Port.Label" ) );
    props.setLook(wlServerPort);
    FormData fdlServerPort = new FormData();
    fdlServerPort.left = new FormAttachment( 0, 0 );
    fdlServerPort.top = new FormAttachment( wServerName, margin );
    fdlServerPort.right = new FormAttachment( middle, -margin );
    wlServerPort.setLayoutData(fdlServerPort);
    wServerPort = new TextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wServerPort );
    wServerPort.setToolTipText( BaseMessages.getString( PKG, "JobSFTPPUT.Port.Tooltip" ) );
    wServerPort.addModifyListener( lsMod );
    FormData fdServerPort = new FormData();
    fdServerPort.left = new FormAttachment( middle, 0 );
    fdServerPort.top = new FormAttachment( wServerName, margin );
    fdServerPort.right = new FormAttachment( 100, 0 );
    wServerPort.setLayoutData(fdServerPort);

    // UserName line
    Label wlUserName = new Label(wServerSettings, SWT.RIGHT);
    wlUserName.setText( BaseMessages.getString( PKG, "JobSFTPPUT.Username.Label" ) );
    props.setLook(wlUserName);
    FormData fdlUserName = new FormData();
    fdlUserName.left = new FormAttachment( 0, 0 );
    fdlUserName.top = new FormAttachment( wServerPort, margin );
    fdlUserName.right = new FormAttachment( middle, -margin );
    wlUserName.setLayoutData(fdlUserName);
    wUserName = new TextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUserName );
    wUserName.setToolTipText( BaseMessages.getString( PKG, "JobSFTPPUT.Username.Tooltip" ) );
    wUserName.addModifyListener( lsMod );
    FormData fdUserName = new FormData();
    fdUserName.left = new FormAttachment( middle, 0 );
    fdUserName.top = new FormAttachment( wServerPort, margin );
    fdUserName.right = new FormAttachment( 100, 0 );
    wUserName.setLayoutData(fdUserName);

    // Password line
    Label wlPassword = new Label(wServerSettings, SWT.RIGHT);
    wlPassword.setText( BaseMessages.getString( PKG, "JobSFTPPUT.Password.Label" ) );
    props.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment( 0, 0 );
    fdlPassword.top = new FormAttachment( wUserName, margin );
    fdlPassword.right = new FormAttachment( middle, -margin );
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new PasswordTextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPassword );
    wPassword.addModifyListener( lsMod );
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment( middle, 0 );
    fdPassword.top = new FormAttachment( wUserName, margin );
    fdPassword.right = new FormAttachment( 100, 0 );
    wPassword.setLayoutData(fdPassword);

    // usePublicKey
    Label wlusePublicKey = new Label(wServerSettings, SWT.RIGHT);
    wlusePublicKey.setText( BaseMessages.getString( PKG, "JobSFTPPUT.useKeyFile.Label" ) );
    props.setLook(wlusePublicKey);
    FormData fdlusePublicKey = new FormData();
    fdlusePublicKey.left = new FormAttachment( 0, 0 );
    fdlusePublicKey.top = new FormAttachment( wPassword, margin );
    fdlusePublicKey.right = new FormAttachment( middle, -margin );
    wlusePublicKey.setLayoutData(fdlusePublicKey);
    wusePublicKey = new Button(wServerSettings, SWT.CHECK );
    wusePublicKey.setToolTipText( BaseMessages.getString( PKG, "JobSFTPPUT.useKeyFile.Tooltip" ) );
    props.setLook( wusePublicKey );
    FormData fdusePublicKey = new FormData();
    fdusePublicKey.left = new FormAttachment( middle, 0 );
    fdusePublicKey.top = new FormAttachment( wPassword, margin );
    fdusePublicKey.right = new FormAttachment( 100, 0 );
    wusePublicKey.setLayoutData(fdusePublicKey);
    wusePublicKey.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeUseKey();
        action.setChanged();
      }
    } );

    // Key File
    wlKeyFilename = new Label(wServerSettings, SWT.RIGHT );
    wlKeyFilename.setText( BaseMessages.getString( PKG, "JobSFTPPUT.KeyFilename.Label" ) );
    props.setLook( wlKeyFilename );
    FormData fdlKeyFilename = new FormData();
    fdlKeyFilename.left = new FormAttachment( 0, 0 );
    fdlKeyFilename.top = new FormAttachment( wusePublicKey, margin );
    fdlKeyFilename.right = new FormAttachment( middle, -margin );
    wlKeyFilename.setLayoutData(fdlKeyFilename);

    wbKeyFilename = new Button(wServerSettings, SWT.PUSH | SWT.CENTER );
    props.setLook( wbKeyFilename );
    wbKeyFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbKeyFilename = new FormData();
    fdbKeyFilename.right = new FormAttachment( 100, 0 );
    fdbKeyFilename.top = new FormAttachment( wusePublicKey, 0 );
    // fdbKeyFilename.height = 22;
    wbKeyFilename.setLayoutData(fdbKeyFilename);

    wKeyFilename = new TextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wKeyFilename.setToolTipText( BaseMessages.getString( PKG, "JobSFTPPUT.KeyFilename.Tooltip" ) );
    props.setLook( wKeyFilename );
    wKeyFilename.addModifyListener( lsMod );
    FormData fdKeyFilename = new FormData();
    fdKeyFilename.left = new FormAttachment( middle, 0 );
    fdKeyFilename.top = new FormAttachment( wusePublicKey, margin );
    fdKeyFilename.right = new FormAttachment( wbKeyFilename, -margin );
    wKeyFilename.setLayoutData(fdKeyFilename);

    wbKeyFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wKeyFilename, workflowMeta,
      new String[] { "*.pem", "*" }, FILETYPES, true )
    );

    // keyfilePass line
    wkeyfilePass =
      new LabelTextVar(
        workflowMeta, wServerSettings, BaseMessages.getString( PKG, "JobSFTPPUT.keyfilePass.Label" ), BaseMessages
        .getString( PKG, "JobSFTPPUT.keyfilePass.Tooltip" ), true );
    props.setLook( wkeyfilePass );
    wkeyfilePass.addModifyListener( lsMod );
    FormData fdkeyfilePass = new FormData();
    fdkeyfilePass.left = new FormAttachment( 0, -margin );
    fdkeyfilePass.top = new FormAttachment( wKeyFilename, margin );
    fdkeyfilePass.right = new FormAttachment( 100, 0 );
    wkeyfilePass.setLayoutData(fdkeyfilePass);

    Label wlProxyType = new Label(wServerSettings, SWT.RIGHT);
    wlProxyType.setText( BaseMessages.getString( PKG, "JobSFTPPUT.ProxyType.Label" ) );
    props.setLook(wlProxyType);
    FormData fdlProxyType = new FormData();
    fdlProxyType.left = new FormAttachment( 0, 0 );
    fdlProxyType.right = new FormAttachment( middle, -margin );
    fdlProxyType.top = new FormAttachment( wkeyfilePass, 2 * margin );
    wlProxyType.setLayoutData(fdlProxyType);

    wProxyType = new CCombo(wServerSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wProxyType.add( SftpClient.PROXY_TYPE_HTTP );
    wProxyType.add( SftpClient.PROXY_TYPE_SOCKS5 );
    wProxyType.select( 0 ); // +1: starts at -1
    props.setLook( wProxyType );
    FormData fdProxyType = new FormData();
    fdProxyType.left = new FormAttachment( middle, 0 );
    fdProxyType.top = new FormAttachment( wkeyfilePass, 2 * margin );
    fdProxyType.right = new FormAttachment( 100, 0 );
    wProxyType.setLayoutData(fdProxyType);
    wProxyType.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setDefaulProxyPort();
      }
    } );

    // Proxy host line
    wProxyHost = new LabelTextVar( workflowMeta, wServerSettings,
      BaseMessages.getString( PKG, "JobSFTPPUT.ProxyHost.Label" ),
      BaseMessages.getString( PKG, "JobSFTPPUT.ProxyHost.Tooltip" ) );
    props.setLook( wProxyHost );
    wProxyHost.addModifyListener( lsMod );
    FormData fdProxyHost = new FormData();
    fdProxyHost.left = new FormAttachment( 0, -2 * margin );
    fdProxyHost.top = new FormAttachment( wProxyType, margin );
    fdProxyHost.right = new FormAttachment( 100, 0 );
    wProxyHost.setLayoutData(fdProxyHost);

    // Proxy port line
    wProxyPort =
      new LabelTextVar(
        workflowMeta, wServerSettings, BaseMessages.getString( PKG, "JobSFTPPUT.ProxyPort.Label" ), BaseMessages
        .getString( PKG, "JobSFTPPUT.ProxyPort.Tooltip" ) );
    props.setLook( wProxyPort );
    wProxyPort.addModifyListener( lsMod );
    FormData fdProxyPort = new FormData();
    fdProxyPort.left = new FormAttachment( 0, -2 * margin );
    fdProxyPort.top = new FormAttachment( wProxyHost, margin );
    fdProxyPort.right = new FormAttachment( 100, 0 );
    wProxyPort.setLayoutData(fdProxyPort);

    // Proxy username line
    wProxyUsername =
      new LabelTextVar(
        workflowMeta, wServerSettings, BaseMessages.getString( PKG, "JobSFTPPUT.ProxyUsername.Label" ),
        BaseMessages.getString( PKG, "JobSFTPPUT.ProxyUsername.Tooltip" ) );
    props.setLook( wProxyUsername );
    wProxyUsername.addModifyListener( lsMod );
    FormData fdProxyUsername = new FormData();
    fdProxyUsername.left = new FormAttachment( 0, -2 * margin );
    fdProxyUsername.top = new FormAttachment( wProxyPort, margin );
    fdProxyUsername.right = new FormAttachment( 100, 0 );
    wProxyUsername.setLayoutData(fdProxyUsername);

    // Proxy password line
    wProxyPassword =
      new LabelTextVar(
        workflowMeta, wServerSettings, BaseMessages.getString( PKG, "JobSFTPPUT.ProxyPassword.Label" ),
        BaseMessages.getString( PKG, "JobSFTPPUT.ProxyPassword.Tooltip" ), true );
    props.setLook( wProxyPassword );
    wProxyPassword.addModifyListener( lsMod );
    FormData fdProxyPasswd = new FormData();
    fdProxyPasswd.left = new FormAttachment( 0, -2 * margin );
    fdProxyPasswd.top = new FormAttachment( wProxyUsername, margin );
    fdProxyPasswd.right = new FormAttachment( 100, 0 );
    wProxyPassword.setLayoutData(fdProxyPasswd);

    // Test connection button
    Button wTest = new Button(wServerSettings, SWT.PUSH);
    wTest.setText( BaseMessages.getString( PKG, "JobSFTPPUT.TestConnection.Label" ) );
    props.setLook(wTest);
    FormData fdTest = new FormData();
    wTest.setToolTipText( BaseMessages.getString( PKG, "JobSFTPPUT.TestConnection.Tooltip" ) );
    fdTest.top = new FormAttachment( wProxyPassword, margin );
    fdTest.right = new FormAttachment( 100, 0 );
    wTest.setLayoutData(fdTest);

    FormData fdServerSettings = new FormData();
    fdServerSettings.left = new FormAttachment( 0, margin );
    fdServerSettings.top = new FormAttachment( wName, margin );
    fdServerSettings.right = new FormAttachment( 100, -margin );
    wServerSettings.setLayoutData(fdServerSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF SERVER SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    Label wlCompression = new Label(wGeneralComp, SWT.RIGHT);
    wlCompression.setText( BaseMessages.getString( PKG, "JobSFTPPUT.Compression.Label" ) );
    props.setLook(wlCompression);
    FormData fdlCompression = new FormData();
    fdlCompression.left = new FormAttachment( 0, -margin );
    fdlCompression.right = new FormAttachment( middle, 0 );
    fdlCompression.top = new FormAttachment(wServerSettings, margin );
    wlCompression.setLayoutData(fdlCompression);

    wCompression = new CCombo(wGeneralComp, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wCompression.add( "none" );
    wCompression.add( "zlib" );
    wCompression.select( 0 ); // +1: starts at -1

    props.setLook( wCompression );
    FormData fdCompression = new FormData();
    fdCompression.left = new FormAttachment( middle, margin );
    fdCompression.top = new FormAttachment(wServerSettings, margin );
    fdCompression.right = new FormAttachment( 100, 0 );
    wCompression.setLayoutData(fdCompression);

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
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
    // START OF Files TAB ///
    // ////////////////////////

    CTabItem wFilesTab = new CTabItem(wTabFolder, SWT.NONE);
    wFilesTab.setText( BaseMessages.getString( PKG, "JobSFTPPUT.Tab.Files.Label" ) );

    Composite wFilesComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFilesComp);

    FormLayout FilesLayout = new FormLayout();
    FilesLayout.marginWidth = 3;
    FilesLayout.marginHeight = 3;
    wFilesComp.setLayout( FilesLayout );

    // ////////////////////////
    // START OF Source files GROUP///
    // /
    Group wSourceFiles = new Group(wFilesComp, SWT.SHADOW_NONE);
    props.setLook(wSourceFiles);
    wSourceFiles.setText( BaseMessages.getString( PKG, "JobSFTPPUT.SourceFiles.Group.Label" ) );
    FormLayout SourceFilesgroupLayout = new FormLayout();
    SourceFilesgroupLayout.marginWidth = 10;
    SourceFilesgroupLayout.marginHeight = 10;
    wSourceFiles.setLayout( SourceFilesgroupLayout );

    // Get arguments from previous result...
    Label wlgetPrevious = new Label(wSourceFiles, SWT.RIGHT);
    wlgetPrevious.setText( BaseMessages.getString( PKG, "JobSFTPPUT.getPrevious.Label" ) );
    props.setLook(wlgetPrevious);
    FormData fdlgetPrevious = new FormData();
    fdlgetPrevious.left = new FormAttachment( 0, 0 );
    fdlgetPrevious.top = new FormAttachment(wServerSettings, 2 * margin );
    fdlgetPrevious.right = new FormAttachment( middle, -margin );
    wlgetPrevious.setLayoutData(fdlgetPrevious);
    wgetPrevious = new Button(wSourceFiles, SWT.CHECK );
    props.setLook( wgetPrevious );
    wgetPrevious.setToolTipText( BaseMessages.getString( PKG, "JobSFTPPUT.getPrevious.Tooltip" ) );
    FormData fdgetPrevious = new FormData();
    fdgetPrevious.left = new FormAttachment( middle, 0 );
    fdgetPrevious.top = new FormAttachment(wServerSettings, 2 * margin );
    fdgetPrevious.right = new FormAttachment( 100, 0 );
    wgetPrevious.setLayoutData(fdgetPrevious);
    wgetPrevious.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        if ( wgetPrevious.getSelection() ) {
          wgetPreviousFiles.setSelection( false ); // only one is allowed
        }
        activeCopyFromPrevious();
        action.setChanged();
      }
    } );

    // Get arguments from previous files result...
    Label wlgetPreviousFiles = new Label(wSourceFiles, SWT.RIGHT);
    wlgetPreviousFiles.setText( BaseMessages.getString( PKG, "JobSFTPPUT.getPreviousFiles.Label" ) );
    props.setLook(wlgetPreviousFiles);
    FormData fdlgetPreviousFiles = new FormData();
    fdlgetPreviousFiles.left = new FormAttachment( 0, 0 );
    fdlgetPreviousFiles.top = new FormAttachment( wgetPrevious, 2 * margin );
    fdlgetPreviousFiles.right = new FormAttachment( middle, -margin );
    wlgetPreviousFiles.setLayoutData(fdlgetPreviousFiles);
    wgetPreviousFiles = new Button(wSourceFiles, SWT.CHECK );
    props.setLook( wgetPreviousFiles );
    wgetPreviousFiles.setToolTipText( BaseMessages.getString( PKG, "JobSFTPPUT.getPreviousFiles.Tooltip" ) );
    FormData fdgetPreviousFiles = new FormData();
    fdgetPreviousFiles.left = new FormAttachment( middle, 0 );
    fdgetPreviousFiles.top = new FormAttachment( wgetPrevious, 2 * margin );
    fdgetPreviousFiles.right = new FormAttachment( 100, 0 );
    wgetPreviousFiles.setLayoutData(fdgetPreviousFiles);
    wgetPreviousFiles.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        if ( wgetPreviousFiles.getSelection() ) {
          wgetPrevious.setSelection( false ); // only one is allowed
        }
        activeCopyFromPrevious();
        action.setChanged();
      }
    } );

    // Local Directory line
    wlLocalDirectory = new Label(wSourceFiles, SWT.RIGHT );
    wlLocalDirectory.setText( BaseMessages.getString( PKG, "JobSFTPPUT.LocalDir.Label" ) );
    props.setLook( wlLocalDirectory );
    FormData fdlLocalDirectory = new FormData();
    fdlLocalDirectory.left = new FormAttachment( 0, 0 );
    fdlLocalDirectory.top = new FormAttachment( wgetPreviousFiles, margin );
    fdlLocalDirectory.right = new FormAttachment( middle, -margin );
    wlLocalDirectory.setLayoutData(fdlLocalDirectory);

    // Browse folders button ...
    wbLocalDirectory = new Button(wSourceFiles, SWT.PUSH | SWT.CENTER );
    props.setLook( wbLocalDirectory );
    wbLocalDirectory.setText( BaseMessages.getString( PKG, "JobSFTPPUT.BrowseFolders.Label" ) );
    FormData fdbLocalDirectory = new FormData();
    fdbLocalDirectory.right = new FormAttachment( 100, 0 );
    fdbLocalDirectory.top = new FormAttachment( wgetPreviousFiles, margin );
    wbLocalDirectory.setLayoutData(fdbLocalDirectory);
    wbLocalDirectory.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wLocalDirectory, workflowMeta ) );

    wLocalDirectory = new TextVar( workflowMeta, wSourceFiles, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLocalDirectory );
    wLocalDirectory.setToolTipText( BaseMessages.getString( PKG, "JobSFTPPUT.LocalDir.Tooltip" ) );
    wLocalDirectory.addModifyListener( lsMod );
    FormData fdLocalDirectory = new FormData();
    fdLocalDirectory.left = new FormAttachment( middle, 0 );
    fdLocalDirectory.top = new FormAttachment( wgetPreviousFiles, margin );
    fdLocalDirectory.right = new FormAttachment( wbLocalDirectory, -margin );
    wLocalDirectory.setLayoutData(fdLocalDirectory);

    // Wildcard line
    wlWildcard = new Label(wSourceFiles, SWT.RIGHT );
    wlWildcard.setText( BaseMessages.getString( PKG, "JobSFTPPUT.Wildcard.Label" ) );
    props.setLook( wlWildcard );
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment( 0, 0 );
    fdlWildcard.top = new FormAttachment( wbLocalDirectory, margin );
    fdlWildcard.right = new FormAttachment( middle, -margin );
    wlWildcard.setLayoutData(fdlWildcard);
    wWildcard = new TextVar( workflowMeta, wSourceFiles, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wWildcard );
    wWildcard.setToolTipText( BaseMessages.getString( PKG, "JobSFTPPUT.Wildcard.Tooltip" ) );
    wWildcard.addModifyListener( lsMod );
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment( middle, 0 );
    fdWildcard.top = new FormAttachment( wbLocalDirectory, margin );
    fdWildcard.right = new FormAttachment( 100, 0 );
    wWildcard.setLayoutData(fdWildcard);

    // Success when there is no file...
    Label wlSuccessWhenNoFile = new Label(wSourceFiles, SWT.RIGHT);
    wlSuccessWhenNoFile.setText( BaseMessages.getString( PKG, "JobSFTPPUT.SuccessWhenNoFile.Label" ) );
    props.setLook(wlSuccessWhenNoFile);
    FormData fdlSuccessWhenNoFile = new FormData();
    fdlSuccessWhenNoFile.left = new FormAttachment( 0, 0 );
    fdlSuccessWhenNoFile.top = new FormAttachment( wWildcard, margin );
    fdlSuccessWhenNoFile.right = new FormAttachment( middle, -margin );
    wlSuccessWhenNoFile.setLayoutData(fdlSuccessWhenNoFile);
    wSuccessWhenNoFile = new Button(wSourceFiles, SWT.CHECK );
    props.setLook( wSuccessWhenNoFile );
    wSuccessWhenNoFile.setToolTipText( BaseMessages.getString( PKG, "JobSFTPPUT.SuccessWhenNoFile.Tooltip" ) );
    FormData fdSuccessWhenNoFile = new FormData();
    fdSuccessWhenNoFile.left = new FormAttachment( middle, 0 );
    fdSuccessWhenNoFile.top = new FormAttachment( wWildcard, margin );
    fdSuccessWhenNoFile.right = new FormAttachment( 100, 0 );
    wSuccessWhenNoFile.setLayoutData(fdSuccessWhenNoFile);
    wSuccessWhenNoFile.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // After FTP Put
    Label wlAfterFtpPut = new Label(wSourceFiles, SWT.RIGHT);
    wlAfterFtpPut.setText( BaseMessages.getString( PKG, "JobSFTPPUT.AfterFTPPut.Label" ) );
    props.setLook(wlAfterFtpPut);
    FormData fdlAfterFtpPut = new FormData();
    fdlAfterFtpPut.left = new FormAttachment( 0, 0 );
    fdlAfterFtpPut.right = new FormAttachment( middle, -margin );
    fdlAfterFtpPut.top = new FormAttachment( wSuccessWhenNoFile, 2 * margin );
    wlAfterFtpPut.setLayoutData(fdlAfterFtpPut);
    wAfterFtpPut = new CCombo(wSourceFiles, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wAfterFtpPut.add( BaseMessages.getString( PKG, "JobSFTPPUT.AfterSFTP.DoNothing.Label" ) );
    wAfterFtpPut.add( BaseMessages.getString( PKG, "JobSFTPPUT.AfterSFTP.Delete.Label" ) );
    wAfterFtpPut.add( BaseMessages.getString( PKG, "JobSFTPPUT.AfterSFTP.Move.Label" ) );
    wAfterFtpPut.select( 0 ); // +1: starts at -1
    props.setLook(wAfterFtpPut);
    FormData fdAfterFtpPut = new FormData();
    fdAfterFtpPut.left = new FormAttachment( middle, 0 );
    fdAfterFtpPut.top = new FormAttachment( wSuccessWhenNoFile, 2 * margin );
    fdAfterFtpPut.right = new FormAttachment( 100, -margin );
    wAfterFtpPut.setLayoutData(fdAfterFtpPut);
    wAfterFtpPut.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        AfterFtpPutActivate();

      }
    } );

    // moveTo Directory
    wlDestinationFolder = new Label(wSourceFiles, SWT.RIGHT );
    wlDestinationFolder.setText( BaseMessages.getString( PKG, "JobSFTPPUT.DestinationFolder.Label" ) );
    props.setLook( wlDestinationFolder );
    FormData fdlDestinationFolder = new FormData();
    fdlDestinationFolder.left = new FormAttachment( 0, 0 );
    fdlDestinationFolder.top = new FormAttachment(wAfterFtpPut, margin );
    fdlDestinationFolder.right = new FormAttachment( middle, -margin );
    wlDestinationFolder.setLayoutData(fdlDestinationFolder);

    // Browse folders button ...
    wbMovetoDirectory = new Button(wSourceFiles, SWT.PUSH | SWT.CENTER );
    props.setLook( wbMovetoDirectory );
    wbMovetoDirectory.setText( BaseMessages.getString( PKG, "JobSFTPPUT.BrowseFolders.Label" ) );
    FormData fdbMovetoDirectory = new FormData();
    fdbMovetoDirectory.right = new FormAttachment( 100, 0 );
    fdbMovetoDirectory.top = new FormAttachment(wAfterFtpPut, margin );
    wbMovetoDirectory.setLayoutData(fdbMovetoDirectory);

    wbMovetoDirectory.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wDestinationFolder, workflowMeta ) );

    wDestinationFolder = new TextVar( workflowMeta, wSourceFiles, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobSFTPPUT.DestinationFolder.Tooltip" ) );
    props.setLook( wDestinationFolder );
    wDestinationFolder.addModifyListener( lsMod );
    FormData fdDestinationFolder = new FormData();
    fdDestinationFolder.left = new FormAttachment( middle, 0 );
    fdDestinationFolder.top = new FormAttachment(wAfterFtpPut, margin );
    fdDestinationFolder.right = new FormAttachment( wbMovetoDirectory, -margin );
    wDestinationFolder.setLayoutData(fdDestinationFolder);

    // Whenever something changes, set the tooltip to the expanded version:
    wDestinationFolder.addModifyListener( e -> wDestinationFolder.setToolTipText( workflowMeta.environmentSubstitute( wDestinationFolder.getText() ) ) );

    // Create destination folder if necessary ...
    wlCreateDestinationFolder = new Label(wSourceFiles, SWT.RIGHT );
    wlCreateDestinationFolder.setText( BaseMessages.getString( PKG, "JobSFTPPUT.CreateDestinationFolder.Label" ) );
    props.setLook( wlCreateDestinationFolder );
    FormData fdlCreateDestinationFolder = new FormData();
    fdlCreateDestinationFolder.left = new FormAttachment( 0, 0 );
    fdlCreateDestinationFolder.top = new FormAttachment( wDestinationFolder, margin );
    fdlCreateDestinationFolder.right = new FormAttachment( middle, -margin );
    wlCreateDestinationFolder.setLayoutData(fdlCreateDestinationFolder);
    wCreateDestinationFolder = new Button(wSourceFiles, SWT.CHECK );
    wCreateDestinationFolder.setToolTipText( BaseMessages.getString(
      PKG, "JobSFTPPUT.CreateDestinationFolder.Tooltip" ) );
    props.setLook( wCreateDestinationFolder );
    FormData fdCreateDestinationFolder = new FormData();
    fdCreateDestinationFolder.left = new FormAttachment( middle, 0 );
    fdCreateDestinationFolder.top = new FormAttachment( wDestinationFolder, margin );
    fdCreateDestinationFolder.right = new FormAttachment( 100, 0 );
    wCreateDestinationFolder.setLayoutData(fdCreateDestinationFolder);

    // Add filenames to result filenames...
    wlAddFilenameToResult = new Label(wSourceFiles, SWT.RIGHT );
    wlAddFilenameToResult.setText( BaseMessages.getString( PKG, "JobSFTPPUT.AddfilenametoResult.Label" ) );
    props.setLook( wlAddFilenameToResult );
    FormData fdlAddFilenameToResult = new FormData();
    fdlAddFilenameToResult.left = new FormAttachment( 0, 0 );
    fdlAddFilenameToResult.top = new FormAttachment( wCreateDestinationFolder, margin );
    fdlAddFilenameToResult.right = new FormAttachment( middle, -margin );
    wlAddFilenameToResult.setLayoutData(fdlAddFilenameToResult);
    wAddFilenameToResult = new Button(wSourceFiles, SWT.CHECK );
    wAddFilenameToResult.setToolTipText( BaseMessages.getString( PKG, "JobSFTPPUT.AddfilenametoResult.Tooltip" ) );
    props.setLook( wAddFilenameToResult );
    FormData fdAddFilenameToResult = new FormData();
    fdAddFilenameToResult.left = new FormAttachment( middle, 0 );
    fdAddFilenameToResult.top = new FormAttachment( wCreateDestinationFolder, margin );
    fdAddFilenameToResult.right = new FormAttachment( 100, 0 );
    wAddFilenameToResult.setLayoutData(fdAddFilenameToResult);

    FormData fdSourceFiles = new FormData();
    fdSourceFiles.left = new FormAttachment( 0, margin );
    fdSourceFiles.top = new FormAttachment(wServerSettings, 2 * margin );
    fdSourceFiles.right = new FormAttachment( 100, -margin );
    wSourceFiles.setLayoutData(fdSourceFiles);
    // ///////////////////////////////////////////////////////////
    // / END OF Source files GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Target files GROUP///
    // /
    Group wTargetFiles = new Group(wFilesComp, SWT.SHADOW_NONE);
    props.setLook(wTargetFiles);
    wTargetFiles.setText( BaseMessages.getString( PKG, "JobSFTPPUT.TargetFiles.Group.Label" ) );
    FormLayout TargetFilesgroupLayout = new FormLayout();
    TargetFilesgroupLayout.marginWidth = 10;
    TargetFilesgroupLayout.marginHeight = 10;
    wTargetFiles.setLayout( TargetFilesgroupLayout );

    // FtpDirectory line
    Label wlScpDirectory = new Label(wTargetFiles, SWT.RIGHT);
    wlScpDirectory.setText( BaseMessages.getString( PKG, "JobSFTPPUT.RemoteDir.Label" ) );
    props.setLook(wlScpDirectory);
    FormData fdlScpDirectory = new FormData();
    fdlScpDirectory.left = new FormAttachment( 0, 0 );
    fdlScpDirectory.top = new FormAttachment(wSourceFiles, margin );
    fdlScpDirectory.right = new FormAttachment( middle, -margin );
    wlScpDirectory.setLayoutData(fdlScpDirectory);

    // Test remote folder button ...
    wbTestChangeFolderExists = new Button(wTargetFiles, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTestChangeFolderExists );
    wbTestChangeFolderExists.setText( BaseMessages.getString( PKG, "JobSFTPPUT.TestFolderExists.Label" ) );
    FormData fdbTestChangeFolderExists = new FormData();
    fdbTestChangeFolderExists.right = new FormAttachment( 100, 0 );
    fdbTestChangeFolderExists.top = new FormAttachment(wSourceFiles, margin );
    wbTestChangeFolderExists.setLayoutData(fdbTestChangeFolderExists);

    // Target (remote) folder
    wScpDirectory = new TextVar( workflowMeta, wTargetFiles, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wScpDirectory );
    wScpDirectory.setToolTipText( BaseMessages.getString( PKG, "JobSFTPPUT.RemoteDir.Tooltip" ) );
    wScpDirectory.addModifyListener( lsMod );
    FormData fdScpDirectory = new FormData();
    fdScpDirectory.left = new FormAttachment( middle, 0 );
    fdScpDirectory.top = new FormAttachment(wSourceFiles, margin );
    fdScpDirectory.right = new FormAttachment( wbTestChangeFolderExists, -margin );
    wScpDirectory.setLayoutData(fdScpDirectory);

    // CreateRemoteFolder files after retrieval...
    Label wlCreateRemoteFolder = new Label(wTargetFiles, SWT.RIGHT);
    wlCreateRemoteFolder.setText( BaseMessages.getString( PKG, "JobSFTPPUT.CreateRemoteFolderFiles.Label" ) );
    props.setLook(wlCreateRemoteFolder);
    FormData fdlCreateRemoteFolder = new FormData();
    fdlCreateRemoteFolder.left = new FormAttachment( 0, 0 );
    fdlCreateRemoteFolder.top = new FormAttachment( wScpDirectory, margin );
    fdlCreateRemoteFolder.right = new FormAttachment( middle, -margin );
    wlCreateRemoteFolder.setLayoutData(fdlCreateRemoteFolder);
    wCreateRemoteFolder = new Button(wTargetFiles, SWT.CHECK );
    props.setLook( wCreateRemoteFolder );
    FormData fdCreateRemoteFolder = new FormData();
    wCreateRemoteFolder
      .setToolTipText( BaseMessages.getString( PKG, "JobSFTPPUT.CreateRemoteFolderFiles.Tooltip" ) );
    fdCreateRemoteFolder.left = new FormAttachment( middle, 0 );
    fdCreateRemoteFolder.top = new FormAttachment( wScpDirectory, margin );
    fdCreateRemoteFolder.right = new FormAttachment( 100, 0 );
    wCreateRemoteFolder.setLayoutData(fdCreateRemoteFolder);
    wCreateRemoteFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    FormData fdTargetFiles = new FormData();
    fdTargetFiles.left = new FormAttachment( 0, margin );
    fdTargetFiles.top = new FormAttachment(wSourceFiles, margin );
    fdTargetFiles.right = new FormAttachment( 100, -margin );
    wTargetFiles.setLayoutData(fdTargetFiles);
    // ///////////////////////////////////////////////////////////
    // / END OF Target files GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdFilesComp = new FormData();
    fdFilesComp.left = new FormAttachment( 0, 0 );
    fdFilesComp.top = new FormAttachment( 0, 0 );
    fdFilesComp.right = new FormAttachment( 100, 0 );
    fdFilesComp.bottom = new FormAttachment( 100, 0 );
    wFilesComp.setLayoutData(fdFilesComp);

    wFilesComp.layout();
    wFilesTab.setControl(wFilesComp);
    props.setLook(wFilesComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Files TAB
    // ///////////////////////////////////////////////////////////

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
    Listener lsTest = e -> test();
    Listener lsCheckChangeFolder = e -> checkRemoteFolder();

    wCancel.addListener( SWT.Selection, lsCancel);
    wOk.addListener( SWT.Selection, lsOk);
    wTest.addListener( SWT.Selection, lsTest);
    wbTestChangeFolderExists.addListener( SWT.Selection, lsCheckChangeFolder);

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener(lsDef);
    wServerName.addSelectionListener(lsDef);
    wUserName.addSelectionListener(lsDef);
    wPassword.addSelectionListener(lsDef);
    wScpDirectory.addSelectionListener(lsDef);
    wLocalDirectory.addSelectionListener(lsDef);
    wWildcard.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );
    wTabFolder.setSelection( 0 );

    getData();
    activeCopyFromPrevious();
    activeUseKey();
    AfterFtpPutActivate();
    BaseTransformDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  private void activeCopyFromPrevious() {
    boolean enabled = !wgetPrevious.getSelection() && !wgetPreviousFiles.getSelection();
    wLocalDirectory.setEnabled( enabled );
    wlLocalDirectory.setEnabled( enabled );
    wbLocalDirectory.setEnabled( enabled );
    wlWildcard.setEnabled( enabled );
    wWildcard.setEnabled( enabled );
    wbTestChangeFolderExists.setEnabled( enabled );
  }

  private void test() {

    if ( connectToSftp( false, null ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "JobSFTPPUT.Connected.OK", wServerName.getText() ) + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "JobSFTPPUT.Connected.Title.Ok" ) );
      mb.open();
    }
  }

  private void closeFtpConnections() {
    // Close SecureFTP connection if necessary
    if ( sftpclient != null ) {
      try {
        sftpclient.disconnect();
        sftpclient = null;
      } catch ( Exception e ) {
        // Ignore errors
      }
    }
  }

  private boolean connectToSftp(boolean checkFolder, String Remotefoldername ) {
    boolean retval = false;
    try {
      WorkflowMeta workflowMeta = getWorkflowMeta();
    	
      if ( sftpclient == null ) {
        // Create sftp client to host ...
        sftpclient = new SftpClient(
          InetAddress.getByName( workflowMeta.environmentSubstitute( wServerName.getText() ) ),
          Const.toInt( workflowMeta.environmentSubstitute( wServerPort.getText() ), 22 ),
          workflowMeta.environmentSubstitute( wUserName.getText() ),
          workflowMeta.environmentSubstitute( wKeyFilename.getText() ),
          workflowMeta.environmentSubstitute( wkeyfilePass.getText() ) );
        // Set proxy?
        String realProxyHost = workflowMeta.environmentSubstitute( wProxyHost.getText() );
        if ( !Utils.isEmpty( realProxyHost ) ) {
          // Set proxy
          sftpclient.setProxy(
            realProxyHost,
            workflowMeta.environmentSubstitute( wProxyPort.getText() ),
            workflowMeta.environmentSubstitute( wProxyUsername.getText() ),
            workflowMeta.environmentSubstitute( wProxyPassword.getText() ),
            wProxyType.getText() );
        }
        // login to ftp host ...
        sftpclient.login( Utils.resolvePassword( workflowMeta, wPassword.getText() ) );

        retval = true;
      }
      if ( checkFolder ) {
        retval = sftpclient.folderExists( Remotefoldername );
      }

    } catch ( Exception e ) {
      if ( sftpclient != null ) {
        try {
          sftpclient.disconnect();
        } catch ( Exception ignored ) {
          // We've tried quitting the SFTP Client exception
          // nothing else to be done if the SFTP Client was already disconnected
        }
        sftpclient = null;
      }
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "JobSFTPPUT.ErrorConnect.NOK", wServerName.getText(), e
        .getMessage() )
        + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "JobSFTPPUT.ErrorConnect.Title.Bad" ) );
      mb.open();
    }
    return retval;
  }

  private void checkRemoteFolder() {
    String changeFtpFolder = getWorkflowMeta().environmentSubstitute( wScpDirectory.getText() );
    if ( !Utils.isEmpty( changeFtpFolder ) ) {
      if ( connectToSftp( true, changeFtpFolder ) ) {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
        mb.setMessage( BaseMessages.getString( PKG, "JobSFTPPUT.FolderExists.OK", changeFtpFolder ) + Const.CR );
        mb.setText( BaseMessages.getString( PKG, "JobSFTPPUT.FolderExists.Title.Ok" ) );
        mb.open();
      }
    }
  }

  public void dispose() {
    // Close open connections
    closeFtpConnections();
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.nullToEmpty( action.getName() ) );
    wServerName.setText( Const.NVL( action.getServerName(), "" ) );
    wServerPort.setText( action.getServerPort() );
    wUserName.setText( Const.NVL( action.getUserName(), "" ) );
    wPassword.setText( Const.NVL( action.getPassword(), "" ) );
    wScpDirectory.setText( Const.NVL( action.getScpDirectory(), "" ) );
    wLocalDirectory.setText( Const.NVL( action.getLocalDirectory(), "" ) );
    wWildcard.setText( Const.NVL( action.getWildcard(), "" ) );
    wgetPrevious.setSelection( action.isCopyPrevious() );
    wgetPreviousFiles.setSelection( action.isCopyPreviousFiles() );
    wAddFilenameToResult.setSelection( action.isAddFilenameResut() );
    wusePublicKey.setSelection( action.isUseKeyFile() );
    wKeyFilename.setText( Const.NVL( action.getKeyFilename(), "" ) );
    wkeyfilePass.setText( Const.NVL( action.getKeyPassPhrase(), "" ) );
    wCompression.setText( Const.NVL( action.getCompression(), "none" ) );

    wProxyType.setText( Const.NVL( action.getProxyType(), "" ) );
    wProxyHost.setText( Const.NVL( action.getProxyHost(), "" ) );
    wProxyPort.setText( Const.NVL( action.getProxyPort(), "" ) );
    wProxyUsername.setText( Const.NVL( action.getProxyUsername(), "" ) );
    wProxyPassword.setText( Const.NVL( action.getProxyPassword(), "" ) );
    wCreateRemoteFolder.setSelection( action.isCreateRemoteFolder() );

    wAfterFtpPut.setText( ActionSftpPut.getAfterSftpPutDesc( action.getAfterFtps() ) );
    wDestinationFolder.setText( Const.NVL( action.getDestinationFolder(), "" ) );
    wCreateDestinationFolder.setSelection( action.isCreateDestinationFolder() );
    wSuccessWhenNoFile.setSelection( action.isSuccessWhenNoFile() );

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
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    action.setName( wName.getText() );
    action.setServerName( wServerName.getText() );
    action.setServerPort( wServerPort.getText() );
    action.setUserName( wUserName.getText() );
    action.setPassword( wPassword.getText() );
    action.setScpDirectory( wScpDirectory.getText() );
    action.setLocalDirectory( wLocalDirectory.getText() );
    action.setWildcard( wWildcard.getText() );
    action.setCopyPrevious( wgetPrevious.getSelection() );
    action.setCopyPreviousFiles( wgetPreviousFiles.getSelection() );
    action.setAddFilenameResut( wAddFilenameToResult.getSelection() );
    action.setUseKeyFile( wusePublicKey.getSelection() );
    action.setKeyFilename( wKeyFilename.getText() );
    action.setKeyPassPhrase( wkeyfilePass.getText() );
    action.setCompression( wCompression.getText() );

    action.setProxyType( wProxyType.getText() );
    action.setProxyHost( wProxyHost.getText() );
    action.setProxyPort( wProxyPort.getText() );
    action.setProxyUsername( wProxyUsername.getText() );
    action.setProxyPassword( wProxyPassword.getText() );
    action.setCreateRemoteFolder( wCreateRemoteFolder.getSelection() );
    action.setAfterFtps( ActionSftpPut.getAfterSftpPutByDesc( wAfterFtpPut.getText() ) );
    action.setCreateDestinationFolder( wCreateDestinationFolder.getSelection() );
    action.setDestinationFolder( wDestinationFolder.getText() );
    action.setSuccessWhenNoFile( wSuccessWhenNoFile.getSelection() );
    dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

  private void activeUseKey() {
    wlKeyFilename.setEnabled( wusePublicKey.getSelection() );
    wKeyFilename.setEnabled( wusePublicKey.getSelection() );
    wbKeyFilename.setEnabled( wusePublicKey.getSelection() );
    wkeyfilePass.setEnabled( wusePublicKey.getSelection() );
  }

  private void setDefaulProxyPort() {
    if ( wProxyType.getText().equals( SftpClient.PROXY_TYPE_HTTP ) ) {
      if ( Utils.isEmpty( wProxyPort.getText() )
        || ( !Utils.isEmpty( wProxyPort.getText() ) && wProxyPort.getText().equals(
        SftpClient.SOCKS5_DEFAULT_PORT ) ) ) {
        wProxyPort.setText( SftpClient.HTTP_DEFAULT_PORT );
      }
    } else {
      if ( Utils.isEmpty( wProxyPort.getText() )
        || ( !Utils.isEmpty( wProxyPort.getText() ) && wProxyPort
        .getText().equals( SftpClient.HTTP_DEFAULT_PORT ) ) ) {
        wProxyPort.setText( SftpClient.SOCKS5_DEFAULT_PORT );
      }
    }
  }

  private void AfterFtpPutActivate() {
    boolean moveFile =
      ActionSftpPut.getAfterSftpPutByDesc( wAfterFtpPut.getText() ) == ActionSftpPut.AFTER_FTPSPUT_MOVE;
    boolean doNothing =
      ActionSftpPut.getAfterSftpPutByDesc( wAfterFtpPut.getText() ) == ActionSftpPut.AFTER_FTPSPUT_NOTHING;

    wlDestinationFolder.setEnabled( moveFile );
    wDestinationFolder.setEnabled( moveFile );
    wbMovetoDirectory.setEnabled( moveFile );
    wlCreateDestinationFolder.setEnabled( moveFile );
    wCreateDestinationFolder.setEnabled( moveFile );
    wlAddFilenameToResult.setEnabled( doNothing );
    wAddFilenameToResult.setEnabled( doNothing );

  }
}
