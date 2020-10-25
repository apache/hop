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

package org.apache.hop.ui.server;

import org.apache.hop.server.HopServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.metastore.IMetadataDialog;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.www.RegisterPipelineServlet;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
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
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * Dialog that allows you to edit the settings of the security service connection
 *
 * @author Matt
 * @see HopServer
 * @since 31-10-2006
 */

public class HopServerDialog extends Dialog implements IMetadataDialog {
  private static Class<?> PKG = HopServerDialog.class; // for i18n purposes, needed by Translator!!
  private final IHopMetadataProvider metadataProvider;

  private HopServer hopServer;

  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wServiceTab, wProxyTab;

  private Composite wServiceComp, wProxyComp;
  private FormData fdServiceComp, fdProxyComp;

  private Shell shell;

  // Service
  private Text wName;
  private TextVar wHostname, wPort, wWebAppName, wUsername, wPassword;
  private Button wSSL;

  // Proxy
  private TextVar wProxyHost, wProxyPort, wNonProxyHosts;

  private Button wOk, wCancel;

  private ModifyListener lsMod;

  private PropsUi props;

  private int middle;
  private int margin;

  private HopServer originalServer;
  private String result;

  public HopServerDialog( Shell par, IHopMetadataProvider metadataProvider, HopServer hopServer ) {
    super( par, SWT.NONE );
    this.metadataProvider = metadataProvider;
    this.hopServer = (HopServer) hopServer.clone();
    this.hopServer.shareVariablesWith( hopServer );
    this.originalServer = hopServer;
    props = PropsUi.getInstance();
    result = null;
  }

  public String open() {
    Shell parent = getParent();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImageServer() );

    lsMod = e -> hopServer.setChanged();

    middle = props.getMiddlePct();
    margin = props.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "HopServerDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    // First, add the buttons...

    // Buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    Button[] buttons = new Button[] { wOk, wCancel };
    BaseTransformDialog.positionBottomButtons( shell, buttons, margin, null );

    // The rest stays above the buttons...

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    addServiceTab();
    addProxyTab();

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( 0, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -margin );
    wTabFolder.setLayoutData( fdTabFolder );

    // Add listeners
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wUsername.addSelectionListener( selAdapter );
    wPassword.addSelectionListener( selAdapter );
    wHostname.addSelectionListener( selAdapter );
    wPort.addSelectionListener( selAdapter );
    wWebAppName.addSelectionListener( selAdapter );
    wProxyHost.addSelectionListener( selAdapter );
    wProxyPort.addSelectionListener( selAdapter );
    wNonProxyHosts.addSelectionListener( selAdapter );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wTabFolder.setSelection( 0 );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    Display display = parent.getDisplay();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return result;
  }

  private void addServiceTab() {
    // ////////////////////////
    // START OF DB TAB ///
    // ////////////////////////
    wServiceTab = new CTabItem( wTabFolder, SWT.NONE );
    wServiceTab.setText( BaseMessages.getString( PKG, "HopServerDialog.USER_TAB_SERVICE" ) );

    wServiceComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wServiceComp );

    FormLayout GenLayout = new FormLayout();
    GenLayout.marginWidth = Const.FORM_MARGIN;
    GenLayout.marginHeight = Const.FORM_MARGIN;
    wServiceComp.setLayout( GenLayout );

    // What's the name
    Label wlName = new Label( wServiceComp, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "HopServerDialog.ServerName.Label" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, 0 );
    fdlName.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlName.right = new FormAttachment( middle, -margin );
    wlName.setLayoutData( fdlName );

    wName = new Text( wServiceComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( 0, 0 );
    fdName.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdName.right = new FormAttachment( 95, 0 );
    wName.setLayoutData( fdName );

    // What's the hostname
    Label wlHostname = new Label( wServiceComp, SWT.RIGHT );
    props.setLook( wlHostname );
    wlHostname.setText( BaseMessages.getString( PKG, "HopServerDialog.HostIP.Label" ) );
    FormData fdlHostname = new FormData();
    fdlHostname.top = new FormAttachment( wName, margin * 2 );
    fdlHostname.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlHostname.right = new FormAttachment( middle, -margin );
    wlHostname.setLayoutData( fdlHostname );

    wHostname = new TextVar( hopServer, wServiceComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHostname );
    wHostname.addModifyListener( lsMod );
    FormData fdHostname = new FormData();
    fdHostname.top = new FormAttachment( wName, margin * 2 );
    fdHostname.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdHostname.right = new FormAttachment( 95, 0 );
    wHostname.setLayoutData( fdHostname );

    // What's the service URL?
    Label wlPort = new Label( wServiceComp, SWT.RIGHT );
    props.setLook( wlPort );
    wlPort.setText( BaseMessages.getString( PKG, "HopServerDialog.Port.Label" ) );
    FormData fdlPort = new FormData();
    fdlPort.top = new FormAttachment( wHostname, margin );
    fdlPort.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlPort.right = new FormAttachment( middle, -margin );
    wlPort.setLayoutData( fdlPort );

    wPort = new TextVar( hopServer, wServiceComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPort );
    wPort.addModifyListener( lsMod );
    FormData fdPort = new FormData();
    fdPort.top = new FormAttachment( wHostname, margin );
    fdPort.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdPort.right = new FormAttachment( 95, 0 );
    wPort.setLayoutData( fdPort );

    // webapp name (optional)
    Label wlWebAppName = new Label( wServiceComp, SWT.RIGHT );
    wlWebAppName.setText( BaseMessages.getString( PKG, "HopServerDialog.WebAppName.Label" ) );
    props.setLook( wlWebAppName );
    FormData fdlWebAppName = new FormData();
    fdlWebAppName.top = new FormAttachment( wPort, margin );
    fdlWebAppName.left = new FormAttachment( 0, 0 );
    fdlWebAppName.right = new FormAttachment( middle, -margin );
    wlWebAppName.setLayoutData( fdlWebAppName );

    wWebAppName = new TextVar( hopServer, wServiceComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wWebAppName );
    wWebAppName.addModifyListener( lsMod );
    FormData fdWebAppName = new FormData();
    fdWebAppName.top = new FormAttachment( wPort, margin );
    fdWebAppName.left = new FormAttachment( middle, 0 );
    fdWebAppName.right = new FormAttachment( 95, 0 );
    wWebAppName.setLayoutData( fdWebAppName );

    // Username
    Label wlUsername = new Label( wServiceComp, SWT.RIGHT );
    wlUsername.setText( BaseMessages.getString( PKG, "HopServerDialog.UserName.Label" ) );
    props.setLook( wlUsername );
    FormData fdlUsername = new FormData();
    fdlUsername.top = new FormAttachment( wWebAppName, margin );
    fdlUsername.left = new FormAttachment( 0, 0 );
    fdlUsername.right = new FormAttachment( middle, -margin );
    wlUsername.setLayoutData( fdlUsername );

    wUsername = new TextVar( hopServer, wServiceComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUsername );
    wUsername.addModifyListener( lsMod );
    FormData fdUsername = new FormData();
    fdUsername.top = new FormAttachment( wWebAppName, margin );
    fdUsername.left = new FormAttachment( middle, 0 );
    fdUsername.right = new FormAttachment( 95, 0 );
    wUsername.setLayoutData( fdUsername );

    // Password
    Label wlPassword = new Label( wServiceComp, SWT.RIGHT );
    wlPassword.setText( BaseMessages.getString( PKG, "HopServerDialog.Password.Label" ) );
    props.setLook( wlPassword );
    FormData fdlPassword = new FormData();
    fdlPassword.top = new FormAttachment( wUsername, margin );
    fdlPassword.left = new FormAttachment( 0, 0 );
    fdlPassword.right = new FormAttachment( middle, -margin );
    wlPassword.setLayoutData( fdlPassword );

    wPassword = new PasswordTextVar( hopServer, wServiceComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPassword );
    wPassword.addModifyListener( lsMod );
    FormData fdPassword = new FormData();
    fdPassword.top = new FormAttachment( wUsername, margin );
    fdPassword.left = new FormAttachment( middle, 0 );
    fdPassword.right = new FormAttachment( 95, 0 );
    wPassword.setLayoutData( fdPassword );

    // Https
    Label wlSSL = new Label( wServiceComp, SWT.RIGHT );
    wlSSL.setText( BaseMessages.getString( PKG, "HopServerDialog.UseSsl.Label" ) );
    props.setLook( wlSSL );
    FormData fd = new FormData();
    fd.top = new FormAttachment( wPassword, margin );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    wlSSL.setLayoutData( fd );
    wlSSL.setVisible( true );

    wSSL = new Button( wServiceComp, SWT.CHECK );
    props.setLook( wSSL );
    FormData bfd = new FormData();
    bfd.top = new FormAttachment( wlSSL, 0, SWT.CENTER );
    bfd.left = new FormAttachment( middle, 0 );
    bfd.right = new FormAttachment( 95, 0 );
    wSSL.setLayoutData( bfd );
    wSSL.setVisible( true );

    fdServiceComp = new FormData();
    fdServiceComp.left = new FormAttachment( 0, 0 );
    fdServiceComp.top = new FormAttachment( 0, 0 );
    fdServiceComp.right = new FormAttachment( 100, 0 );
    fdServiceComp.bottom = new FormAttachment( 100, 0 );
    wServiceComp.setLayoutData( fdServiceComp );

    wServiceComp.layout();
    wServiceTab.setControl( wServiceComp );

    // ///////////////////////////////////////////////////////////
    // / END OF GEN TAB
    // ///////////////////////////////////////////////////////////
  }

  private void addProxyTab() {
    // ////////////////////////
    // START OF POOL TAB///
    // /
    wProxyTab = new CTabItem( wTabFolder, SWT.NONE );
    wProxyTab.setText( BaseMessages.getString( PKG, "HopServerDialog.USER_TAB_PROXY" ) );

    FormLayout poolLayout = new FormLayout();
    poolLayout.marginWidth = Const.FORM_MARGIN;
    poolLayout.marginHeight = Const.FORM_MARGIN;

    wProxyComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wProxyComp );
    wProxyComp.setLayout( poolLayout );

    // What's the data tablespace name?
    Label wlProxyHost = new Label( wProxyComp, SWT.RIGHT );
    props.setLook( wlProxyHost );
    wlProxyHost.setText( BaseMessages.getString( PKG, "HopServerDialog.ProxyServerName.Label" ) );
    FormData fdlProxyHost = new FormData();
    fdlProxyHost.top = new FormAttachment( 0, 0 );
    fdlProxyHost.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlProxyHost.right = new FormAttachment( middle, -margin );
    wlProxyHost.setLayoutData( fdlProxyHost );

    wProxyHost = new TextVar( hopServer, wProxyComp, SWT.BORDER | SWT.LEFT | SWT.SINGLE );
    props.setLook( wProxyHost );
    wProxyHost.addModifyListener( lsMod );
    FormData fdProxyHost = new FormData();
    fdProxyHost.top = new FormAttachment( 0, 0 );
    fdProxyHost.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdProxyHost.right = new FormAttachment( 95, 0 );
    wProxyHost.setLayoutData( fdProxyHost );

    // What's the initial pool size
    Label wlProxyPort = new Label( wProxyComp, SWT.RIGHT );
    props.setLook( wlProxyPort );
    wlProxyPort.setText( BaseMessages.getString( PKG, "HopServerDialog.ProxyServerPort.Label" ) );
    FormData fdlProxyPort = new FormData();
    fdlProxyPort.top = new FormAttachment( wProxyHost, margin );
    fdlProxyPort.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlProxyPort.right = new FormAttachment( middle, -margin );
    wlProxyPort.setLayoutData( fdlProxyPort );

    wProxyPort = new TextVar( hopServer, wProxyComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wProxyPort );
    wProxyPort.addModifyListener( lsMod );
    FormData fdProxyPort = new FormData();
    fdProxyPort.top = new FormAttachment( wProxyHost, margin );
    fdProxyPort.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdProxyPort.right = new FormAttachment( 95, 0 );
    wProxyPort.setLayoutData( fdProxyPort );

    // What's the maximum pool size
    Label wlNonProxyHosts = new Label( wProxyComp, SWT.RIGHT );
    props.setLook( wlNonProxyHosts );
    wlNonProxyHosts.setText( BaseMessages.getString( PKG, "HopServerDialog.IgnoreProxyForHosts.Label" ) );
    FormData fdlNonProxyHosts = new FormData();
    fdlNonProxyHosts.top = new FormAttachment( wProxyPort, margin );
    fdlNonProxyHosts.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlNonProxyHosts.right = new FormAttachment( middle, -margin );
    wlNonProxyHosts.setLayoutData( fdlNonProxyHosts );

    wNonProxyHosts = new TextVar( hopServer, wProxyComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wNonProxyHosts );
    wNonProxyHosts.addModifyListener( lsMod );
    FormData fdNonProxyHosts = new FormData();
    fdNonProxyHosts.top = new FormAttachment( wProxyPort, margin );
    fdNonProxyHosts.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdNonProxyHosts.right = new FormAttachment( 95, 0 );
    wNonProxyHosts.setLayoutData( fdNonProxyHosts );

    fdProxyComp = new FormData();
    fdProxyComp.left = new FormAttachment( 0, 0 );
    fdProxyComp.top = new FormAttachment( 0, 0 );
    fdProxyComp.right = new FormAttachment( 100, 0 );
    fdProxyComp.bottom = new FormAttachment( 100, 0 );
    wProxyComp.setLayoutData( fdProxyComp );

    wProxyComp.layout();
    wProxyTab.setControl( wProxyComp );
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    wName.setText( Const.NVL( hopServer.getName(), "" ) );
    wHostname.setText( Const.NVL( hopServer.getHostname(), "" ) );
    wPort.setText( Const.NVL( hopServer.getPort(), "" ) );
    wWebAppName.setText( Const.NVL( hopServer.getWebAppName(), "" ) );
    wUsername.setText( Const.NVL( hopServer.getUsername(), "" ) );
    wPassword.setText( Const.NVL( hopServer.getPassword(), "" ) );

    wProxyHost.setText( Const.NVL( hopServer.getProxyHostname(), "" ) );
    wProxyPort.setText( Const.NVL( hopServer.getProxyPort(), "" ) );
    wNonProxyHosts.setText( Const.NVL( hopServer.getNonProxyHosts(), "" ) );

    wSSL.setSelection( hopServer.isSslMode() );

    wName.setFocus();
  }

  private void cancel() {
    originalServer = null;
    dispose();
  }

  public void ok() {
    getInfo();

    if ( !hopServer.getName().equals( originalServer.getName() ) ) {

      // See if the name collides with an existing one...
      // TODO: provide name changes utilities
      //
      try {
        IHopMetadataSerializer<HopServer> serializer = metadataProvider.getSerializer( HopServer.class );
        if ( serializer.exists( hopServer.getName() ) ) {
          String title = BaseMessages.getString( PKG, "HopServerDialog.HopServerNameExists.Title" );
          String message =
            BaseMessages.getString( PKG, "HopServerDialog.HopServerNameExists", hopServer.getName() );
          String okButton = BaseMessages.getString( PKG, "System.Button.OK" );
          MessageDialog dialog =
            new MessageDialog( shell, title, null, message, MessageDialog.ERROR, new String[] { okButton }, 0 );

          dialog.open();
          return;
        }
      } catch ( Exception e ) {
        new ErrorDialog( shell, "Error", "Error checking for name collisions after rename", e );
      }
    }

    originalServer.setName( hopServer.getName() );
    originalServer.setHostname( hopServer.getHostname() );
    originalServer.setPort( hopServer.getPort() );
    originalServer.setWebAppName( hopServer.getWebAppName() );
    originalServer.setUsername( hopServer.getUsername() );
    originalServer.setPassword( hopServer.getPassword() );

    originalServer.setProxyHostname( hopServer.getProxyHostname() );
    originalServer.setProxyPort( hopServer.getProxyPort() );
    originalServer.setNonProxyHosts( hopServer.getNonProxyHosts() );

    originalServer.setSslMode( hopServer.isSslMode() );

    originalServer.setChanged();

    result = hopServer.getName();

    dispose();
  }

  // Get dialog info in securityService
  private void getInfo() {
    hopServer.setName( wName.getText() );
    hopServer.setHostname( wHostname.getText() );
    hopServer.setPort( wPort.getText() );
    hopServer.setWebAppName( wWebAppName.getText() );
    hopServer.setUsername( wUsername.getText() );
    hopServer.setPassword( wPassword.getText() );

    hopServer.setProxyHostname( wProxyHost.getText() );
    hopServer.setProxyPort( wProxyPort.getText() );
    hopServer.setNonProxyHosts( wNonProxyHosts.getText() );

    hopServer.setSslMode( wSSL.getSelection() );
  }

  public void test() {
    try {
      getInfo();

      String xml = "<sample/>";

      String reply = hopServer.sendXml( xml, RegisterPipelineServlet.CONTEXT_PATH );

      String message =
        BaseMessages.getString( PKG, "HopServer.Replay.Info1" )
          + hopServer.constructUrl( RegisterPipelineServlet.CONTEXT_PATH ) + Const.CR
          + BaseMessages.getString( PKG, "HopServer.Replay.Info2" ) + Const.CR + Const.CR;
      message += xml;
      message += Const.CR + Const.CR;
      message += "Reply was:" + Const.CR + Const.CR;
      message += reply + Const.CR;

      EnterTextDialog dialog =
        new EnterTextDialog(
          shell, "XML", BaseMessages.getString( PKG, "HopServer.RetournedXMLInfo" ), message );
      dialog.open();
    } catch ( Exception e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "HopServer.ExceptionError" ), BaseMessages.getString(
        PKG, "HopServer.ExceptionUnableGetReplay.Error1" )
        + hopServer.getHostname()
        + BaseMessages.getString( PKG, "HopServer.ExceptionUnableGetReplay.Error2" ), e );
    }
  }
}
