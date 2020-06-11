/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
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

package org.apache.hop.workflow.actions.sftp;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
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
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.net.InetAddress;

/**
 * This dialog allows you to edit the SFTP action settings.
 *
 * @author Matt
 * @since 19-06-2003
 */
@PluginDialog( 
		  id = "SFTP", 
		  image = "SFTP.svg", 
		  pluginType = PluginDialog.PluginType.ACTION,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/sftp.html"
)
public class ActionSftpDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionSftp.class; // for i18n purposes, needed by Translator!!
  private static final String[] FILETYPES =
    new String[] {
      BaseMessages.getString( PKG, "JobSFTP.Filetype.Pem" ),
      BaseMessages.getString( PKG, "JobSFTP.Filetype.All" ) };

  private Label wlName;

  private Text wName;

  private FormData fdlName, fdName;

  private Label wlServerName;

  private TextVar wServerName;

  private FormData fdlServerName, fdServerName;

  private Label wlServerPort;

  private TextVar wServerPort;

  private FormData fdlServerPort, fdServerPort;

  private Label wlUserName;

  private TextVar wUserName;

  private FormData fdlUserName, fdUserName;

  private Label wlPassword;

  private TextVar wPassword;

  private FormData fdlPassword, fdPassword;

  private Label wlScpDirectory;

  private TextVar wScpDirectory;

  private FormData fdlScpDirectory, fdScpDirectory;

  private Label wlTargetDirectory;

  private TextVar wTargetDirectory;

  private FormData fdlTargetDirectory, fdTargetDirectory;

  private Label wlWildcard;

  private TextVar wWildcard;

  private FormData fdlWildcard, fdWildcard;

  private Label wlRemove;

  private Button wRemove;

  private FormData fdlRemove, fdRemove;

  private Button wOk, wCancel;

  private Listener lsOk, lsCancel;

  private Listener lsCheckChangeFolder;

  private ActionSftp action;

  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  private Button wTest;

  private FormData fdTest;

  private Listener lsTest;

  private Button wbTestChangeFolderExists;

  private FormData fdbTestChangeFolderExists;

  private Button wbTargetDirectory;

  private FormData fdbTargetDirectory;

  private Group wServerSettings;

  private FormData fdServerSettings;

  private Group wSourceFiles;

  private FormData fdSourceFiles;

  private Group wTargetFiles;

  private FormData fdTargetFiles;

  private Label wlAddFilenameToResult;

  private Button wAddFilenameToResult;

  private FormData fdlAddFilenameToResult, fdAddFilenameToResult;

  private Label wlCreateTargetFolder;

  private Button wCreateTargetFolder;

  private FormData fdlCreateTargetFolder, fdCreateTargetFolder;

  private Label wlgetPrevious;

  private Button wgetPrevious;

  private FormData fdlgetPrevious, fdgetPrevious;

  private LabelTextVar wkeyfilePass;

  private FormData fdkeyfilePass;

  private Label wlusePublicKey;

  private Button wusePublicKey;

  private FormData fdlusePublicKey, fdusePublicKey;

  private Label wlKeyFilename;

  private Button wbKeyFilename;

  private TextVar wKeyFilename;

  private FormData fdlKeyFilename, fdbKeyFilename, fdKeyFilename;

  private CTabFolder wTabFolder;
  private Composite wGeneralComp, wFilesComp;
  private CTabItem wGeneralTab, wFilesTab;
  private FormData fdGeneralComp, fdFilesComp;
  private FormData fdTabFolder;

  private SftpClient sftpclient = null;

  private Label wlCompression;
  private FormData fdlCompression;
  private CCombo wCompression;
  private FormData fdCompression;

  private Label wlProxyType;
  private FormData fdlProxyType;
  private CCombo wProxyType;
  private FormData fdProxyType;

  private LabelTextVar wProxyHost;
  private FormData fdProxyHost;
  private LabelTextVar wProxyPort;
  private FormData fdProxyPort;
  private LabelTextVar wProxyUsername;
  private FormData fdProxyUsername;
  private LabelTextVar wProxyPassword;
  private FormData fdProxyPasswd;

  public ActionSftpDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionSftp) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobSFTP.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        sftpclient = null;
        action.setChanged();
      }
    };
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobSFTP.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobSFTP.Name.Label" ) );
    props.setLook( wlName );
    fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobSFTP.Tab.General.Label" ) );

    wGeneralComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wGeneralComp );

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // ////////////////////////
    // START OF SERVER SETTINGS GROUP///
    // /
    wServerSettings = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wServerSettings );
    wServerSettings.setText( BaseMessages.getString( PKG, "JobSFTP.ServerSettings.Group.Label" ) );
    FormLayout ServerSettingsgroupLayout = new FormLayout();
    ServerSettingsgroupLayout.marginWidth = 10;
    ServerSettingsgroupLayout.marginHeight = 10;
    wServerSettings.setLayout( ServerSettingsgroupLayout );

    // ServerName line
    wlServerName = new Label( wServerSettings, SWT.RIGHT );
    wlServerName.setText( BaseMessages.getString( PKG, "JobSFTP.Server.Label" ) );
    props.setLook( wlServerName );
    fdlServerName = new FormData();
    fdlServerName.left = new FormAttachment( 0, 0 );
    fdlServerName.top = new FormAttachment( wName, margin );
    fdlServerName.right = new FormAttachment( middle, -margin );
    wlServerName.setLayoutData( fdlServerName );
    wServerName = new TextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wServerName );
    wServerName.addModifyListener( lsMod );
    fdServerName = new FormData();
    fdServerName.left = new FormAttachment( middle, 0 );
    fdServerName.top = new FormAttachment( wName, margin );
    fdServerName.right = new FormAttachment( 100, 0 );
    wServerName.setLayoutData( fdServerName );

    // ServerPort line
    wlServerPort = new Label( wServerSettings, SWT.RIGHT );
    wlServerPort.setText( BaseMessages.getString( PKG, "JobSFTP.Port.Label" ) );
    props.setLook( wlServerPort );
    fdlServerPort = new FormData();
    fdlServerPort.left = new FormAttachment( 0, 0 );
    fdlServerPort.top = new FormAttachment( wServerName, margin );
    fdlServerPort.right = new FormAttachment( middle, -margin );
    wlServerPort.setLayoutData( fdlServerPort );
    wServerPort = new TextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wServerPort );
    wServerPort.setToolTipText( BaseMessages.getString( PKG, "JobSFTP.Port.Tooltip" ) );
    wServerPort.addModifyListener( lsMod );
    fdServerPort = new FormData();
    fdServerPort.left = new FormAttachment( middle, 0 );
    fdServerPort.top = new FormAttachment( wServerName, margin );
    fdServerPort.right = new FormAttachment( 100, 0 );
    wServerPort.setLayoutData( fdServerPort );

    // UserName line
    wlUserName = new Label( wServerSettings, SWT.RIGHT );
    wlUserName.setText( BaseMessages.getString( PKG, "JobSFTP.Username.Label" ) );
    props.setLook( wlUserName );
    fdlUserName = new FormData();
    fdlUserName.left = new FormAttachment( 0, 0 );
    fdlUserName.top = new FormAttachment( wServerPort, margin );
    fdlUserName.right = new FormAttachment( middle, -margin );
    wlUserName.setLayoutData( fdlUserName );
    wUserName = new TextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUserName );
    wUserName.addModifyListener( lsMod );
    fdUserName = new FormData();
    fdUserName.left = new FormAttachment( middle, 0 );
    fdUserName.top = new FormAttachment( wServerPort, margin );
    fdUserName.right = new FormAttachment( 100, 0 );
    wUserName.setLayoutData( fdUserName );

    // Password line
    wlPassword = new Label( wServerSettings, SWT.RIGHT );
    wlPassword.setText( BaseMessages.getString( PKG, "JobSFTP.Password.Label" ) );
    props.setLook( wlPassword );
    fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment( 0, 0 );
    fdlPassword.top = new FormAttachment( wUserName, margin );
    fdlPassword.right = new FormAttachment( middle, -margin );
    wlPassword.setLayoutData( fdlPassword );
    wPassword = new PasswordTextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPassword );
    wPassword.addModifyListener( lsMod );
    fdPassword = new FormData();
    fdPassword.left = new FormAttachment( middle, 0 );
    fdPassword.top = new FormAttachment( wUserName, margin );
    fdPassword.right = new FormAttachment( 100, 0 );
    wPassword.setLayoutData( fdPassword );

    // usePublicKey
    wlusePublicKey = new Label( wServerSettings, SWT.RIGHT );
    wlusePublicKey.setText( BaseMessages.getString( PKG, "JobSFTP.useKeyFile.Label" ) );
    props.setLook( wlusePublicKey );
    fdlusePublicKey = new FormData();
    fdlusePublicKey.left = new FormAttachment( 0, 0 );
    fdlusePublicKey.top = new FormAttachment( wPassword, margin );
    fdlusePublicKey.right = new FormAttachment( middle, -margin );
    wlusePublicKey.setLayoutData( fdlusePublicKey );
    wusePublicKey = new Button( wServerSettings, SWT.CHECK );
    wusePublicKey.setToolTipText( BaseMessages.getString( PKG, "JobSFTP.useKeyFile.Tooltip" ) );
    props.setLook( wusePublicKey );
    fdusePublicKey = new FormData();
    fdusePublicKey.left = new FormAttachment( middle, 0 );
    fdusePublicKey.top = new FormAttachment( wPassword, margin );
    fdusePublicKey.right = new FormAttachment( 100, 0 );
    wusePublicKey.setLayoutData( fdusePublicKey );
    wusePublicKey.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeUseKey();
        action.setChanged();
      }
    } );

    // Key File
    wlKeyFilename = new Label( wServerSettings, SWT.RIGHT );
    wlKeyFilename.setText( BaseMessages.getString( PKG, "JobSFTP.KeyFilename.Label" ) );
    props.setLook( wlKeyFilename );
    fdlKeyFilename = new FormData();
    fdlKeyFilename.left = new FormAttachment( 0, 0 );
    fdlKeyFilename.top = new FormAttachment( wusePublicKey, margin );
    fdlKeyFilename.right = new FormAttachment( middle, -margin );
    wlKeyFilename.setLayoutData( fdlKeyFilename );

    wbKeyFilename = new Button( wServerSettings, SWT.PUSH | SWT.CENTER );
    props.setLook( wbKeyFilename );
    wbKeyFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbKeyFilename = new FormData();
    fdbKeyFilename.right = new FormAttachment( 100, 0 );
    fdbKeyFilename.top = new FormAttachment( wusePublicKey, 0 );
    // fdbKeyFilename.height = 22;
    wbKeyFilename.setLayoutData( fdbKeyFilename );

    wKeyFilename = new TextVar( workflowMeta, wServerSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wKeyFilename.setToolTipText( BaseMessages.getString( PKG, "JobSFTP.KeyFilename.Tooltip" ) );
    props.setLook( wKeyFilename );
    wKeyFilename.addModifyListener( lsMod );
    fdKeyFilename = new FormData();
    fdKeyFilename.left = new FormAttachment( middle, 0 );
    fdKeyFilename.top = new FormAttachment( wusePublicKey, margin );
    fdKeyFilename.right = new FormAttachment( wbKeyFilename, -margin );
    wKeyFilename.setLayoutData( fdKeyFilename );

    wbKeyFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wKeyFilename, workflowMeta,
      new String[] { "*.pem", "*" }, FILETYPES, true )
    );

    // keyfilePass line
    wkeyfilePass = new LabelTextVar( workflowMeta, wServerSettings, BaseMessages.getString( PKG, "JobSFTP.keyfilePass.Label" ),
      BaseMessages.getString( PKG, "JobSFTP.keyfilePass.Tooltip" ), true );
    props.setLook( wkeyfilePass );
    wkeyfilePass.addModifyListener( lsMod );
    fdkeyfilePass = new FormData();
    fdkeyfilePass.left = new FormAttachment( 0, -2 * margin );
    fdkeyfilePass.top = new FormAttachment( wKeyFilename, margin );
    fdkeyfilePass.right = new FormAttachment( 100, 0 );
    wkeyfilePass.setLayoutData( fdkeyfilePass );

    wlProxyType = new Label( wServerSettings, SWT.RIGHT );
    wlProxyType.setText( BaseMessages.getString( PKG, "JobSFTP.ProxyType.Label" ) );
    props.setLook( wlProxyType );
    fdlProxyType = new FormData();
    fdlProxyType.left = new FormAttachment( 0, 0 );
    fdlProxyType.right = new FormAttachment( middle, -margin );
    fdlProxyType.top = new FormAttachment( wkeyfilePass, 2 * margin );
    wlProxyType.setLayoutData( fdlProxyType );

    wProxyType = new CCombo( wServerSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wProxyType.add( SftpClient.PROXY_TYPE_HTTP );
    wProxyType.add( SftpClient.PROXY_TYPE_SOCKS5 );
    wProxyType.select( 0 ); // +1: starts at -1
    props.setLook( wProxyType );
    fdProxyType = new FormData();
    fdProxyType.left = new FormAttachment( middle, 0 );
    fdProxyType.top = new FormAttachment( wkeyfilePass, 2 * margin );
    fdProxyType.right = new FormAttachment( 100, 0 );
    wProxyType.setLayoutData( fdProxyType );
    wProxyType.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setDefaulProxyPort();
      }
    } );

    // Proxy host line
    wProxyHost =
      new LabelTextVar(
        workflowMeta, wServerSettings, BaseMessages.getString( PKG, "JobSFTP.ProxyHost.Label" ), BaseMessages
        .getString( PKG, "JobSFTP.ProxyHost.Tooltip" ) );
    props.setLook( wProxyHost );
    wProxyHost.addModifyListener( lsMod );
    fdProxyHost = new FormData();
    fdProxyHost.left = new FormAttachment( 0, -2 * margin );
    fdProxyHost.top = new FormAttachment( wProxyType, margin );
    fdProxyHost.right = new FormAttachment( 100, 0 );
    wProxyHost.setLayoutData( fdProxyHost );

    // Proxy port line
    wProxyPort =
      new LabelTextVar(
        workflowMeta, wServerSettings, BaseMessages.getString( PKG, "JobSFTP.ProxyPort.Label" ), BaseMessages
        .getString( PKG, "JobSFTP.ProxyPort.Tooltip" ) );
    props.setLook( wProxyPort );
    wProxyPort.addModifyListener( lsMod );
    fdProxyPort = new FormData();
    fdProxyPort.left = new FormAttachment( 0, -2 * margin );
    fdProxyPort.top = new FormAttachment( wProxyHost, margin );
    fdProxyPort.right = new FormAttachment( 100, 0 );
    wProxyPort.setLayoutData( fdProxyPort );

    // Proxy username line
    wProxyUsername =
      new LabelTextVar(
        workflowMeta, wServerSettings, BaseMessages.getString( PKG, "JobSFTP.ProxyUsername.Label" ), BaseMessages
        .getString( PKG, "JobSFTP.ProxyUsername.Tooltip" ) );
    props.setLook( wProxyUsername );
    wProxyUsername.addModifyListener( lsMod );
    fdProxyUsername = new FormData();
    fdProxyUsername.left = new FormAttachment( 0, -2 * margin );
    fdProxyUsername.top = new FormAttachment( wProxyPort, margin );
    fdProxyUsername.right = new FormAttachment( 100, 0 );
    wProxyUsername.setLayoutData( fdProxyUsername );

    // Proxy password line
    wProxyPassword =
      new LabelTextVar(
        workflowMeta, wServerSettings, BaseMessages.getString( PKG, "JobSFTP.ProxyPassword.Label" ), BaseMessages
        .getString( PKG, "JobSFTP.ProxyPassword.Tooltip" ), true );
    props.setLook( wProxyPassword );
    wProxyPassword.addModifyListener( lsMod );
    fdProxyPasswd = new FormData();
    fdProxyPasswd.left = new FormAttachment( 0, -2 * margin );
    fdProxyPasswd.top = new FormAttachment( wProxyUsername, margin );
    fdProxyPasswd.right = new FormAttachment( 100, 0 );
    wProxyPassword.setLayoutData( fdProxyPasswd );

    // Test connection button
    wTest = new Button( wServerSettings, SWT.PUSH );
    wTest.setText( BaseMessages.getString( PKG, "JobSFTP.TestConnection.Label" ) );
    props.setLook( wTest );
    fdTest = new FormData();
    wTest.setToolTipText( BaseMessages.getString( PKG, "JobSFTP.TestConnection.Tooltip" ) );
    fdTest.top = new FormAttachment( wProxyPassword, margin );
    fdTest.right = new FormAttachment( 100, 0 );
    wTest.setLayoutData( fdTest );

    fdServerSettings = new FormData();
    fdServerSettings.left = new FormAttachment( 0, margin );
    fdServerSettings.top = new FormAttachment( wName, margin );
    fdServerSettings.right = new FormAttachment( 100, -margin );
    wServerSettings.setLayoutData( fdServerSettings );
    // ///////////////////////////////////////////////////////////
    // / END OF SERVER SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    wlCompression = new Label( wGeneralComp, SWT.RIGHT );
    wlCompression.setText( BaseMessages.getString( PKG, "JobSFTP.Compression.Label" ) );
    props.setLook( wlCompression );
    fdlCompression = new FormData();
    fdlCompression.left = new FormAttachment( 0, -margin );
    fdlCompression.right = new FormAttachment( middle, 0 );
    fdlCompression.top = new FormAttachment( wServerSettings, margin );
    wlCompression.setLayoutData( fdlCompression );

    wCompression = new CCombo( wGeneralComp, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wCompression.add( "none" );
    wCompression.add( "zlib" );
    wCompression.select( 0 ); // +1: starts at -1

    props.setLook( wCompression );
    fdCompression = new FormData();
    fdCompression.left = new FormAttachment( middle, margin );
    fdCompression.top = new FormAttachment( wServerSettings, margin );
    fdCompression.right = new FormAttachment( 100, 0 );
    wCompression.setLayoutData( fdCompression );

    fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.layout();
    wGeneralTab.setControl( wGeneralComp );
    props.setLook( wGeneralComp );

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Files TAB ///
    // ////////////////////////

    wFilesTab = new CTabItem( wTabFolder, SWT.NONE );
    wFilesTab.setText( BaseMessages.getString( PKG, "JobSFTP.Tab.Files.Label" ) );

    wFilesComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFilesComp );

    FormLayout FilesLayout = new FormLayout();
    FilesLayout.marginWidth = 3;
    FilesLayout.marginHeight = 3;
    wFilesComp.setLayout( FilesLayout );

    // ////////////////////////
    // START OF Source files GROUP///
    // /
    wSourceFiles = new Group( wFilesComp, SWT.SHADOW_NONE );
    props.setLook( wSourceFiles );
    wSourceFiles.setText( BaseMessages.getString( PKG, "JobSFTP.SourceFiles.Group.Label" ) );
    FormLayout SourceFilesgroupLayout = new FormLayout();
    SourceFilesgroupLayout.marginWidth = 10;
    SourceFilesgroupLayout.marginHeight = 10;
    wSourceFiles.setLayout( SourceFilesgroupLayout );

    // Get arguments from previous result...
    wlgetPrevious = new Label( wSourceFiles, SWT.RIGHT );
    wlgetPrevious.setText( BaseMessages.getString( PKG, "JobSFTP.getPrevious.Label" ) );
    props.setLook( wlgetPrevious );
    fdlgetPrevious = new FormData();
    fdlgetPrevious.left = new FormAttachment( 0, 0 );
    fdlgetPrevious.top = new FormAttachment( wServerSettings, 2 * margin );
    fdlgetPrevious.right = new FormAttachment( middle, -margin );
    wlgetPrevious.setLayoutData( fdlgetPrevious );
    wgetPrevious = new Button( wSourceFiles, SWT.CHECK );
    props.setLook( wgetPrevious );
    wgetPrevious.setToolTipText( BaseMessages.getString( PKG, "JobSFTP.getPrevious.Tooltip" ) );
    fdgetPrevious = new FormData();
    fdgetPrevious.left = new FormAttachment( middle, 0 );
    fdgetPrevious.top = new FormAttachment( wServerSettings, 2 * margin );
    fdgetPrevious.right = new FormAttachment( 100, 0 );
    wgetPrevious.setLayoutData( fdgetPrevious );
    wgetPrevious.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeCopyFromPrevious();
        action.setChanged();
      }
    } );

    // FtpDirectory line
    wlScpDirectory = new Label( wSourceFiles, SWT.RIGHT );
    wlScpDirectory.setText( BaseMessages.getString( PKG, "JobSFTP.RemoteDir.Label" ) );
    props.setLook( wlScpDirectory );
    fdlScpDirectory = new FormData();
    fdlScpDirectory.left = new FormAttachment( 0, 0 );
    fdlScpDirectory.top = new FormAttachment( wgetPrevious, 2 * margin );
    fdlScpDirectory.right = new FormAttachment( middle, -margin );
    wlScpDirectory.setLayoutData( fdlScpDirectory );

    // Test remote folder button ...
    wbTestChangeFolderExists = new Button( wSourceFiles, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTestChangeFolderExists );
    wbTestChangeFolderExists.setText( BaseMessages.getString( PKG, "JobSFTP.TestFolderExists.Label" ) );
    fdbTestChangeFolderExists = new FormData();
    fdbTestChangeFolderExists.right = new FormAttachment( 100, 0 );
    fdbTestChangeFolderExists.top = new FormAttachment( wgetPrevious, 2 * margin );
    wbTestChangeFolderExists.setLayoutData( fdbTestChangeFolderExists );

    wScpDirectory =
      new TextVar( workflowMeta, wSourceFiles, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobSFTP.RemoteDir.Tooltip" ) );
    props.setLook( wScpDirectory );
    wScpDirectory.addModifyListener( lsMod );
    fdScpDirectory = new FormData();
    fdScpDirectory.left = new FormAttachment( middle, 0 );
    fdScpDirectory.top = new FormAttachment( wgetPrevious, 2 * margin );
    fdScpDirectory.right = new FormAttachment( wbTestChangeFolderExists, -margin );
    wScpDirectory.setLayoutData( fdScpDirectory );

    // Wildcard line
    wlWildcard = new Label( wSourceFiles, SWT.RIGHT );
    wlWildcard.setText( BaseMessages.getString( PKG, "JobSFTP.Wildcard.Label" ) );
    props.setLook( wlWildcard );
    fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment( 0, 0 );
    fdlWildcard.top = new FormAttachment( wScpDirectory, margin );
    fdlWildcard.right = new FormAttachment( middle, -margin );
    wlWildcard.setLayoutData( fdlWildcard );
    wWildcard =
      new TextVar( workflowMeta, wSourceFiles, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobSFTP.Wildcard.Tooltip" ) );
    props.setLook( wWildcard );
    wWildcard.addModifyListener( lsMod );
    fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment( middle, 0 );
    fdWildcard.top = new FormAttachment( wScpDirectory, margin );
    fdWildcard.right = new FormAttachment( 100, 0 );
    wWildcard.setLayoutData( fdWildcard );

    // Remove files after retrieval...
    wlRemove = new Label( wSourceFiles, SWT.RIGHT );
    wlRemove.setText( BaseMessages.getString( PKG, "JobSFTP.RemoveFiles.Label" ) );
    props.setLook( wlRemove );
    fdlRemove = new FormData();
    fdlRemove.left = new FormAttachment( 0, 0 );
    fdlRemove.top = new FormAttachment( wWildcard, margin );
    fdlRemove.right = new FormAttachment( middle, -margin );
    wlRemove.setLayoutData( fdlRemove );
    wRemove = new Button( wSourceFiles, SWT.CHECK );
    props.setLook( wRemove );
    wRemove.setToolTipText( BaseMessages.getString( PKG, "JobSFTP.RemoveFiles.Tooltip" ) );
    fdRemove = new FormData();
    fdRemove.left = new FormAttachment( middle, 0 );
    fdRemove.top = new FormAttachment( wWildcard, margin );
    fdRemove.right = new FormAttachment( 100, 0 );
    wRemove.setLayoutData( fdRemove );

    fdSourceFiles = new FormData();
    fdSourceFiles.left = new FormAttachment( 0, margin );
    fdSourceFiles.top = new FormAttachment( wServerSettings, 2 * margin );
    fdSourceFiles.right = new FormAttachment( 100, -margin );
    wSourceFiles.setLayoutData( fdSourceFiles );
    // ///////////////////////////////////////////////////////////
    // / END OF Source files GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Target files GROUP///
    // /
    wTargetFiles = new Group( wFilesComp, SWT.SHADOW_NONE );
    props.setLook( wTargetFiles );
    wTargetFiles.setText( BaseMessages.getString( PKG, "JobSFTP.TargetFiles.Group.Label" ) );
    FormLayout TargetFilesgroupLayout = new FormLayout();
    TargetFilesgroupLayout.marginWidth = 10;
    TargetFilesgroupLayout.marginHeight = 10;
    wTargetFiles.setLayout( TargetFilesgroupLayout );

    // TargetDirectory line
    wlTargetDirectory = new Label( wTargetFiles, SWT.RIGHT );
    wlTargetDirectory.setText( BaseMessages.getString( PKG, "JobSFTP.TargetDir.Label" ) );
    props.setLook( wlTargetDirectory );
    fdlTargetDirectory = new FormData();
    fdlTargetDirectory.left = new FormAttachment( 0, 0 );
    fdlTargetDirectory.top = new FormAttachment( wSourceFiles, margin );
    fdlTargetDirectory.right = new FormAttachment( middle, -margin );
    wlTargetDirectory.setLayoutData( fdlTargetDirectory );

    // Browse folders button ...
    wbTargetDirectory = new Button( wTargetFiles, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTargetDirectory );
    wbTargetDirectory.setText( BaseMessages.getString( PKG, "JobSFTP.BrowseFolders.Label" ) );
    fdbTargetDirectory = new FormData();
    fdbTargetDirectory.right = new FormAttachment( 100, 0 );
    fdbTargetDirectory.top = new FormAttachment( wSourceFiles, margin );
    wbTargetDirectory.setLayoutData( fdbTargetDirectory );
    wbTargetDirectory.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wTargetDirectory, workflowMeta ) );

    wTargetDirectory = new TextVar( workflowMeta, wTargetFiles, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobSFTP.TargetDir.Tooltip" ) );
    props.setLook( wTargetDirectory );
    wTargetDirectory.addModifyListener( lsMod );
    fdTargetDirectory = new FormData();
    fdTargetDirectory.left = new FormAttachment( middle, 0 );
    fdTargetDirectory.top = new FormAttachment( wSourceFiles, margin );
    fdTargetDirectory.right = new FormAttachment( wbTargetDirectory, -margin );
    wTargetDirectory.setLayoutData( fdTargetDirectory );

    // Create target folder if necessary...
    wlCreateTargetFolder = new Label( wTargetFiles, SWT.RIGHT );
    wlCreateTargetFolder.setText( BaseMessages.getString( PKG, "JobSFTP.CreateTargetFolder.Label" ) );
    props.setLook( wlCreateTargetFolder );
    fdlCreateTargetFolder = new FormData();
    fdlCreateTargetFolder.left = new FormAttachment( 0, 0 );
    fdlCreateTargetFolder.top = new FormAttachment( wTargetDirectory, margin );
    fdlCreateTargetFolder.right = new FormAttachment( middle, -margin );
    wlCreateTargetFolder.setLayoutData( fdlCreateTargetFolder );
    wCreateTargetFolder = new Button( wTargetFiles, SWT.CHECK );
    wCreateTargetFolder.setToolTipText( BaseMessages.getString( PKG, "JobSFTP.CreateTargetFolder.Tooltip" ) );
    props.setLook( wCreateTargetFolder );
    fdCreateTargetFolder = new FormData();
    fdCreateTargetFolder.left = new FormAttachment( middle, 0 );
    fdCreateTargetFolder.top = new FormAttachment( wTargetDirectory, margin );
    fdCreateTargetFolder.right = new FormAttachment( 100, 0 );
    wCreateTargetFolder.setLayoutData( fdCreateTargetFolder );

    // Add filenames to result filenames...
    wlAddFilenameToResult = new Label( wTargetFiles, SWT.RIGHT );
    wlAddFilenameToResult.setText( BaseMessages.getString( PKG, "JobSFTP.AddfilenametoResult.Label" ) );
    props.setLook( wlAddFilenameToResult );
    fdlAddFilenameToResult = new FormData();
    fdlAddFilenameToResult.left = new FormAttachment( 0, 0 );
    fdlAddFilenameToResult.top = new FormAttachment( wCreateTargetFolder, margin );
    fdlAddFilenameToResult.right = new FormAttachment( middle, -margin );
    wlAddFilenameToResult.setLayoutData( fdlAddFilenameToResult );
    wAddFilenameToResult = new Button( wTargetFiles, SWT.CHECK );
    wAddFilenameToResult.setToolTipText( BaseMessages.getString( PKG, "JobSFTP.AddfilenametoResult.Tooltip" ) );
    props.setLook( wAddFilenameToResult );
    fdAddFilenameToResult = new FormData();
    fdAddFilenameToResult.left = new FormAttachment( middle, 0 );
    fdAddFilenameToResult.top = new FormAttachment( wCreateTargetFolder, margin );
    fdAddFilenameToResult.right = new FormAttachment( 100, 0 );
    wAddFilenameToResult.setLayoutData( fdAddFilenameToResult );

    fdTargetFiles = new FormData();
    fdTargetFiles.left = new FormAttachment( 0, margin );
    fdTargetFiles.top = new FormAttachment( wSourceFiles, margin );
    fdTargetFiles.right = new FormAttachment( 100, -margin );
    wTargetFiles.setLayoutData( fdTargetFiles );
    // ///////////////////////////////////////////////////////////
    // / END OF Target files GROUP
    // ///////////////////////////////////////////////////////////

    fdFilesComp = new FormData();
    fdFilesComp.left = new FormAttachment( 0, 0 );
    fdFilesComp.top = new FormAttachment( 0, 0 );
    fdFilesComp.right = new FormAttachment( 100, 0 );
    fdFilesComp.bottom = new FormAttachment( 100, 0 );
    wFilesComp.setLayoutData( fdFilesComp );

    wFilesComp.layout();
    wFilesTab.setControl( wFilesComp );
    props.setLook( wFilesComp );

    // ///////////////////////////////////////////////////////////
    // / END OF Files TAB
    // ///////////////////////////////////////////////////////////

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wTabFolder );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsTest = new Listener() {
      public void handleEvent( Event e ) {
        test();
      }
    };
    lsCheckChangeFolder = new Listener() {
      public void handleEvent( Event e ) {
        checkRemoteFolder();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );
    wTest.addListener( SWT.Selection, lsTest );
    wbTestChangeFolderExists.addListener( SWT.Selection, lsCheckChangeFolder );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener( lsDef );
    wServerName.addSelectionListener( lsDef );
    wUserName.addSelectionListener( lsDef );
    wPassword.addSelectionListener( lsDef );
    wScpDirectory.addSelectionListener( lsDef );
    wTargetDirectory.addSelectionListener( lsDef );
    wWildcard.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    activeCopyFromPrevious();
    activeUseKey();

    BaseTransformDialog.setSize( shell );
    wTabFolder.setSelection( 0 );
    shell.open();
    props.setDialogSize( shell, "JobSFTPDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  private void test() {
    if ( connectToSftp( false, null ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "JobSFTP.Connected.OK", wServerName.getText() ) + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "JobSFTP.Connected.Title.Ok" ) );
      mb.open();
    }
  }

  private void activeCopyFromPrevious() {
    wlWildcard.setEnabled( !wgetPrevious.getSelection() );
    wWildcard.setEnabled( !wgetPrevious.getSelection() );
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
            Utils.resolvePassword( workflowMeta, wProxyPassword.getText() ),
            wProxyType.getText() );
        }
        // login to ftp host ...
        sftpclient.login( action.getRealPassword( workflowMeta.environmentSubstitute( wPassword.getText() ) ) );

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
      mb.setMessage( BaseMessages.getString( PKG, "JobSFTP.ErrorConnect.NOK", wServerName.getText(), e
        .getMessage() )
        + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "JobSFTP.ErrorConnect.Title.Bad" ) );
      mb.open();
    }
    return retval;
  }

  private void checkRemoteFolder() {
    String changeFtpFolder = workflowMeta.environmentSubstitute( wScpDirectory.getText() );
    if ( !Utils.isEmpty( changeFtpFolder ) ) {
      if ( connectToSftp( true, changeFtpFolder ) ) {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
        mb.setMessage( BaseMessages.getString( PKG, "JobSFTP.FolderExists.OK", changeFtpFolder ) + Const.CR );
        mb.setText( BaseMessages.getString( PKG, "JobSFTP.FolderExists.Title.Ok" ) );
        mb.open();
      }
    }
  }

  public void dispose() {
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
    wTargetDirectory.setText( Const.NVL( action.getTargetDirectory(), "" ) );
    wWildcard.setText( Const.NVL( action.getWildcard(), "" ) );
    wRemove.setSelection( action.getRemove() );
    wAddFilenameToResult.setSelection( action.isAddToResult() );
    wCreateTargetFolder.setSelection( action.iscreateTargetFolder() );
    wgetPrevious.setSelection( action.isCopyPrevious() );
    wusePublicKey.setSelection( action.isUseKeyFile() );
    wKeyFilename.setText( Const.NVL( action.getKeyFilename(), "" ) );
    wkeyfilePass.setText( Const.NVL( action.getKeyPassPhrase(), "" ) );
    wCompression.setText( Const.NVL( action.getCompression(), "none" ) );

    wProxyType.setText( Const.NVL( action.getProxyType(), "" ) );
    wProxyHost.setText( Const.NVL( action.getProxyHost(), "" ) );
    wProxyPort.setText( Const.NVL( action.getProxyPort(), "" ) );
    wProxyUsername.setText( Const.NVL( action.getProxyUsername(), "" ) );
    wProxyPassword.setText( Const.NVL( action.getProxyPassword(), "" ) );

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
    action.setTargetDirectory( wTargetDirectory.getText() );
    action.setWildcard( wWildcard.getText() );
    action.setRemove( wRemove.getSelection() );
    action.setAddToResult( wAddFilenameToResult.getSelection() );
    action.setcreateTargetFolder( wCreateTargetFolder.getSelection() );
    action.setCopyPrevious( wgetPrevious.getSelection() );
    action.setUseKeyFile( wusePublicKey.getSelection() );
    action.setKeyFilename( wKeyFilename.getText() );
    action.setKeyPassPhrase( wkeyfilePass.getText() );
    action.setCompression( wCompression.getText() );

    action.setProxyType( wProxyType.getText() );
    action.setProxyHost( wProxyHost.getText() );
    action.setProxyPort( wProxyPort.getText() );
    action.setProxyUsername( wProxyUsername.getText() );
    action.setProxyPassword( wProxyPassword.getText() );
    dispose();
  }

  private void activeUseKey() {
    wlKeyFilename.setEnabled( wusePublicKey.getSelection() );
    wKeyFilename.setEnabled( wusePublicKey.getSelection() );
    wbKeyFilename.setEnabled( wusePublicKey.getSelection() );
    wkeyfilePass.setEnabled( wusePublicKey.getSelection() );
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
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
}
