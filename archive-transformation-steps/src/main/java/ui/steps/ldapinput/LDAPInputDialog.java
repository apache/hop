/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.trans.steps.ldapinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.TransPreviewFactory;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.steps.ldapinput.LDAPConnection;
import org.apache.hop.trans.steps.ldapinput.LDAPInputField;
import org.apache.hop.trans.steps.ldapinput.LDAPInputMeta;
import org.apache.hop.trans.steps.ldapinput.LdapProtocol;
import org.apache.hop.trans.steps.ldapinput.LdapProtocolFactory;
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
import org.apache.hop.ui.trans.dialog.TransPreviewProgressDialog;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.apache.hop.ui.trans.step.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
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
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class LDAPInputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = LDAPInputMeta.class; // for i18n purposes, needed by Translator2!!

  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wGeneralTab, wContentTab, wFieldsTab, wSearchTab;

  private Composite wGeneralComp, wContentComp, wFieldsComp, wSearchComp;
  private FormData fdGeneralComp, fdContentComp, fdFieldsComp, fdSearchComp;

  private Label wlInclRownum;
  private Button wInclRownum;
  private FormData fdlInclRownum, fdRownum;

  private Label wlsetPaging;
  private Button wsetPaging;
  private FormData fdlsetPaging, fdsetPaging;

  private Label wlPageSize;
  private TextVar wPageSize;
  private FormData fdlPageSize, fdPageSize;

  private Label wlusingAuthentication;
  private Button wusingAuthentication;
  private FormData fdlusingAuthentication;

  private Label wlInclRownumField;
  private TextVar wInclRownumField;
  private FormData fdlInclRownumField, fdInclRownumField;

  private Label wlLimit;
  private Text wLimit;
  private FormData fdlLimit, fdLimit;

  private Label wlTimeLimit;
  private TextVar wTimeLimit;
  private FormData fdlTimeLimit, fdTimeLimit;

  private Label wlMultiValuedSeparator;
  private TextVar wMultiValuedSeparator;
  private FormData fdlMultiValuedSeparator, fdMultiValuedSeparator;

  private TableView wFields;
  private FormData fdFields;

  private LDAPInputMeta input;

  private Group wAdditionalGroup;
  private FormData fdAdditionalGroup;

  private Group wHostGroup, wAuthenticationGroup, wCertificateGroup;
  private FormData fdHostGroup, fdAuthenticationGroup, fdCertificateGroup;

  private Group wSearchGroup;
  private FormData fdSearchGroup;

  private Label wlHost;
  private TextVar wHost;
  private FormData fdlHost, fdHost;

  private Label wlUserName;
  private TextVar wUserName;
  private FormData fdlUserName, fdUserName;

  private Label wlPassword;
  private TextVar wPassword;
  private FormData fdlPassword, fdPassword;

  private Label wlPort;
  private TextVar wPort;
  private FormData fdlPort, fdPort;

  private Button wTest;
  private FormData fdTest;

  private Label wlSearchBase;
  private TextVar wSearchBase;
  private FormData fdlSearchBase, fdSearchBase;

  private Label wlTrustStorePath;
  private TextVar wTrustStorePath;
  private FormData fdlTrustStorePath, fdTrustStorePath;

  private Label wlTrustStorePassword;
  private TextVar wTrustStorePassword;
  private FormData fdlTrustStorePassword, fdTrustStorePassword;

  private Label wlsetTrustStore;
  private FormData fdlsetTrustStore;
  private Button wsetTrustStore;
  private FormData fdsetTrustStore;

  private Label wlTrustAll;
  private FormData fdlTrustAll;
  private Button wTrustAll;
  private FormData fdTrustAll;

  private Label wlFilterString;
  private StyledTextComp wFilterString;
  private FormData fdlFilterString, fdFilterString;

  private Label wldynamicBase;
  private FormData fdlynamicBase;
  private Button wdynamicBase;
  private FormData fdynamicBase;

  private Label wlsearchBaseField;
  private FormData fdlsearchBaseField;
  private CCombo wsearchBaseField;
  private FormData fdsearchBaseField;

  private Label wldynamicFilter;
  private FormData fdldynamicFilter;
  private Button wdynamicFilter;
  private FormData fdynamicFilter;

  private Label wlfilterField;
  private FormData fdlfilterField;
  private CCombo wfilterField;
  private FormData fdfilterField;

  private Listener lsTest;

  private Label wlsearchScope;
  private CCombo wsearchScope;
  private FormData fdlsearchScope;
  private FormData fdsearchScope;

  public static final int[] dateLengths = new int[] { 23, 19, 14, 10, 10, 10, 10, 8, 8, 8, 8, 6, 6 };
  private ColumnInfo[] colinf;
  private boolean gotPreviousFields = false;

  private Label wlProtocol;
  private ComboVar wProtocol;
  private FormData fdlProtocol, fdProtocol;

  private Button wbbFilename;
  private FormData fdbFilename;

  public LDAPInputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (LDAPInputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "LDAPInputDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, margin );
    fdlStepname.right = new FormAttachment( middle, -margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////
    wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab.setText( BaseMessages.getString( PKG, "LDAPInputDialog.General.Tab" ) );

    wGeneralComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wGeneralComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wGeneralComp.setLayout( fileLayout );

    // /////////////////////////////////
    // START OF Host GROUP
    // /////////////////////////////////

    wHostGroup = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wHostGroup );
    wHostGroup.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Group.HostGroup.Label" ) );

    FormLayout HostGroupLayout = new FormLayout();
    HostGroupLayout.marginWidth = 10;
    HostGroupLayout.marginHeight = 10;
    wHostGroup.setLayout( HostGroupLayout );

    // Host line
    wlHost = new Label( wHostGroup, SWT.RIGHT );
    wlHost.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Host.Label" ) );
    props.setLook( wlHost );
    fdlHost = new FormData();
    fdlHost.left = new FormAttachment( 0, 0 );
    fdlHost.top = new FormAttachment( wStepname, margin );
    fdlHost.right = new FormAttachment( middle, -margin );
    wlHost.setLayoutData( fdlHost );
    wHost = new TextVar( transMeta, wHostGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wHost.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.Host.Tooltip" ) );
    props.setLook( wHost );
    wHost.addModifyListener( lsMod );
    fdHost = new FormData();
    fdHost.left = new FormAttachment( middle, 0 );
    fdHost.top = new FormAttachment( wStepname, margin );
    fdHost.right = new FormAttachment( 100, 0 );
    wHost.setLayoutData( fdHost );

    // Port line
    wlPort = new Label( wHostGroup, SWT.RIGHT );
    wlPort.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Port.Label" ) );
    props.setLook( wlPort );
    fdlPort = new FormData();
    fdlPort.left = new FormAttachment( 0, 0 );
    fdlPort.top = new FormAttachment( wHost, margin );
    fdlPort.right = new FormAttachment( middle, -margin );
    wlPort.setLayoutData( fdlPort );
    wPort = new TextVar( transMeta, wHostGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPort );
    wPort.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.Port.Tooltip" ) );
    wPort.addModifyListener( lsMod );
    fdPort = new FormData();
    fdPort.left = new FormAttachment( middle, 0 );
    fdPort.top = new FormAttachment( wHost, margin );
    fdPort.right = new FormAttachment( 100, 0 );
    wPort.setLayoutData( fdPort );

    // Protocol Line
    wlProtocol = new Label( wHostGroup, SWT.RIGHT );
    wlProtocol.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Protocol.Label" ) );
    props.setLook( wlProtocol );
    fdlProtocol = new FormData();
    fdlProtocol.left = new FormAttachment( 0, 0 );
    fdlProtocol.right = new FormAttachment( middle, -margin );
    fdlProtocol.top = new FormAttachment( wPort, margin );
    wlProtocol.setLayoutData( fdlProtocol );

    wProtocol = new ComboVar( transMeta, wHostGroup, SWT.BORDER | SWT.READ_ONLY );
    wProtocol.setEditable( true );
    props.setLook( wProtocol );
    wProtocol.addModifyListener( lsMod );
    fdProtocol = new FormData();
    fdProtocol.left = new FormAttachment( middle, 0 );
    fdProtocol.top = new FormAttachment( wPort, margin );
    fdProtocol.right = new FormAttachment( 100, -margin );
    wProtocol.setLayoutData( fdProtocol );
    wProtocol.setItems( LdapProtocolFactory.getConnectionTypes( log ).toArray( new String[] {} ) );
    wProtocol.addSelectionListener( new SelectionAdapter() {

      public void widgetSelected( SelectionEvent e ) {
        setProtocol();
      }
    } );

    fdHostGroup = new FormData();
    fdHostGroup.left = new FormAttachment( 0, margin );
    fdHostGroup.top = new FormAttachment( 0, margin );
    fdHostGroup.right = new FormAttachment( 100, -margin );
    wHostGroup.setLayoutData( fdHostGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF Host GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Authentication GROUP
    // /////////////////////////////////

    wAuthenticationGroup = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wAuthenticationGroup );
    wAuthenticationGroup
      .setText( BaseMessages.getString( PKG, "LDAPInputDialog.Group.AuthenticationGroup.Label" ) );

    FormLayout AuthenticationGroupLayout = new FormLayout();
    AuthenticationGroupLayout.marginWidth = 10;
    AuthenticationGroupLayout.marginHeight = 10;
    wAuthenticationGroup.setLayout( AuthenticationGroupLayout );

    // using authentication ?
    wlusingAuthentication = new Label( wAuthenticationGroup, SWT.RIGHT );
    wlusingAuthentication.setText( BaseMessages.getString( PKG, "LDAPInputDialog.usingAuthentication.Label" ) );
    props.setLook( wlusingAuthentication );
    fdlusingAuthentication = new FormData();
    fdlusingAuthentication.left = new FormAttachment( 0, 0 );
    fdlusingAuthentication.top = new FormAttachment( wHostGroup, margin );
    fdlusingAuthentication.right = new FormAttachment( middle, -margin );
    wlusingAuthentication.setLayoutData( fdlusingAuthentication );
    wusingAuthentication = new Button( wAuthenticationGroup, SWT.CHECK );
    props.setLook( wusingAuthentication );
    wusingAuthentication.setToolTipText( BaseMessages.getString(
      PKG, "LDAPInputDialog.usingAuthentication.Tooltip" ) );
    FormData fdusingAuthentication = new FormData();
    fdusingAuthentication.left = new FormAttachment( middle, 0 );
    fdusingAuthentication.top = new FormAttachment( wHostGroup, margin );
    wusingAuthentication.setLayoutData( fdusingAuthentication );

    wusingAuthentication.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        useAuthentication();
        input.setChanged();
      }
    } );

    // UserName line
    wlUserName = new Label( wAuthenticationGroup, SWT.RIGHT );
    wlUserName.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Username.Label" ) );
    props.setLook( wlUserName );
    fdlUserName = new FormData();
    fdlUserName.left = new FormAttachment( 0, 0 );
    fdlUserName.top = new FormAttachment( wusingAuthentication, margin );
    fdlUserName.right = new FormAttachment( middle, -margin );
    wlUserName.setLayoutData( fdlUserName );
    wUserName = new TextVar( transMeta, wAuthenticationGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUserName );
    wUserName.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.Username.Tooltip" ) );
    wUserName.addModifyListener( lsMod );
    fdUserName = new FormData();
    fdUserName.left = new FormAttachment( middle, 0 );
    fdUserName.top = new FormAttachment( wusingAuthentication, margin );
    fdUserName.right = new FormAttachment( 100, 0 );
    wUserName.setLayoutData( fdUserName );

    // Password line
    wlPassword = new Label( wAuthenticationGroup, SWT.RIGHT );
    wlPassword.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Password.Label" ) );
    props.setLook( wlPassword );
    fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment( 0, 0 );
    fdlPassword.top = new FormAttachment( wUserName, margin );
    fdlPassword.right = new FormAttachment( middle, -margin );
    wlPassword.setLayoutData( fdlPassword );
    wPassword = new PasswordTextVar( transMeta, wAuthenticationGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wPassword.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.Password.Tooltip" ) );
    props.setLook( wPassword );
    wPassword.addModifyListener( lsMod );
    fdPassword = new FormData();
    fdPassword.left = new FormAttachment( middle, 0 );
    fdPassword.top = new FormAttachment( wUserName, margin );
    fdPassword.right = new FormAttachment( 100, 0 );
    wPassword.setLayoutData( fdPassword );

    fdAuthenticationGroup = new FormData();
    fdAuthenticationGroup.left = new FormAttachment( 0, margin );
    fdAuthenticationGroup.top = new FormAttachment( wHostGroup, margin );
    fdAuthenticationGroup.right = new FormAttachment( 100, -margin );
    wAuthenticationGroup.setLayoutData( fdAuthenticationGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF Authentication GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Certificate GROUP
    // /////////////////////////////////

    wCertificateGroup = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wCertificateGroup );
    wCertificateGroup.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Group.CertificateGroup.Label" ) );

    FormLayout CertificateGroupLayout = new FormLayout();
    CertificateGroupLayout.marginWidth = 10;
    CertificateGroupLayout.marginHeight = 10;
    wCertificateGroup.setLayout( CertificateGroupLayout );

    // set TrustStore?
    wlsetTrustStore = new Label( wCertificateGroup, SWT.RIGHT );
    wlsetTrustStore.setText( BaseMessages.getString( PKG, "LDAPInputDialog.setTrustStore.Label" ) );
    props.setLook( wlsetTrustStore );
    fdlsetTrustStore = new FormData();
    fdlsetTrustStore.left = new FormAttachment( 0, 0 );
    fdlsetTrustStore.top = new FormAttachment( wAuthenticationGroup, margin );
    fdlsetTrustStore.right = new FormAttachment( middle, -margin );
    wlsetTrustStore.setLayoutData( fdlsetTrustStore );
    wsetTrustStore = new Button( wCertificateGroup, SWT.CHECK );
    props.setLook( wsetTrustStore );
    wsetTrustStore.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.setTrustStore.Tooltip" ) );
    fdsetTrustStore = new FormData();
    fdsetTrustStore.left = new FormAttachment( middle, 0 );
    fdsetTrustStore.top = new FormAttachment( wAuthenticationGroup, margin );
    wsetTrustStore.setLayoutData( fdsetTrustStore );

    wsetTrustStore.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setTrustStore();
      }
    } );

    // TrustStorePath line
    wlTrustStorePath = new Label( wCertificateGroup, SWT.RIGHT );
    wlTrustStorePath.setText( BaseMessages.getString( PKG, "LDAPInputDialog.TrustStorePath.Label" ) );
    props.setLook( wlTrustStorePath );
    fdlTrustStorePath = new FormData();
    fdlTrustStorePath.left = new FormAttachment( 0, -margin );
    fdlTrustStorePath.top = new FormAttachment( wsetTrustStore, margin );
    fdlTrustStorePath.right = new FormAttachment( middle, -margin );
    wlTrustStorePath.setLayoutData( fdlTrustStorePath );

    wbbFilename = new Button( wCertificateGroup, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbFilename );
    wbbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    wbbFilename.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wsetTrustStore, margin );
    wbbFilename.setLayoutData( fdbFilename );
    // Listen to the Browse... button
    wbbFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        if ( wTrustStorePath.getText() != null ) {
          String fpath = transMeta.environmentSubstitute( wTrustStorePath.getText() );
          dialog.setFileName( fpath );
        }

        if ( dialog.open() != null ) {
          String str = dialog.getFilterPath() + System.getProperty( "file.separator" ) + dialog.getFileName();
          wTrustStorePath.setText( str );
        }
      }
    } );

    wTrustStorePath = new TextVar( transMeta, wCertificateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTrustStorePath );
    wTrustStorePath.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.TrustStorePath.Tooltip" ) );
    wTrustStorePath.addModifyListener( lsMod );
    fdTrustStorePath = new FormData();
    fdTrustStorePath.left = new FormAttachment( middle, 0 );
    fdTrustStorePath.top = new FormAttachment( wsetTrustStore, margin );
    fdTrustStorePath.right = new FormAttachment( wbbFilename, -margin );
    wTrustStorePath.setLayoutData( fdTrustStorePath );

    // TrustStorePassword line
    wlTrustStorePassword = new Label( wCertificateGroup, SWT.RIGHT );
    wlTrustStorePassword.setText( BaseMessages.getString( PKG, "LDAPInputDialog.TrustStorePassword.Label" ) );
    props.setLook( wlTrustStorePassword );
    fdlTrustStorePassword = new FormData();
    fdlTrustStorePassword.left = new FormAttachment( 0, -margin );
    fdlTrustStorePassword.top = new FormAttachment( wbbFilename, margin );
    fdlTrustStorePassword.right = new FormAttachment( middle, -margin );
    wlTrustStorePassword.setLayoutData( fdlTrustStorePassword );
    wTrustStorePassword = new PasswordTextVar( transMeta, wCertificateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTrustStorePassword );
    wTrustStorePassword
      .setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.TrustStorePassword.Tooltip" ) );
    wTrustStorePassword.addModifyListener( lsMod );
    fdTrustStorePassword = new FormData();
    fdTrustStorePassword.left = new FormAttachment( middle, 0 );
    fdTrustStorePassword.top = new FormAttachment( wbbFilename, margin );
    fdTrustStorePassword.right = new FormAttachment( 100, -margin );
    wTrustStorePassword.setLayoutData( fdTrustStorePassword );

    // Trust all certificate?
    wlTrustAll = new Label( wCertificateGroup, SWT.RIGHT );
    wlTrustAll.setText( BaseMessages.getString( PKG, "LDAPInputDialog.TrustAll.Label" ) );
    props.setLook( wlTrustAll );
    fdlTrustAll = new FormData();
    fdlTrustAll.left = new FormAttachment( 0, 0 );
    fdlTrustAll.top = new FormAttachment( wTrustStorePassword, margin );
    fdlTrustAll.right = new FormAttachment( middle, -margin );
    wlTrustAll.setLayoutData( fdlTrustAll );
    wTrustAll = new Button( wCertificateGroup, SWT.CHECK );
    props.setLook( wTrustAll );
    wTrustAll.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.TrustAll.Tooltip" ) );
    fdTrustAll = new FormData();
    fdTrustAll.left = new FormAttachment( middle, 0 );
    fdTrustAll.top = new FormAttachment( wTrustStorePassword, margin );
    wTrustAll.setLayoutData( fdTrustAll );

    wTrustAll.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        trustAll();
      }
    } );

    fdCertificateGroup = new FormData();
    fdCertificateGroup.left = new FormAttachment( 0, margin );
    fdCertificateGroup.top = new FormAttachment( wAuthenticationGroup, margin );
    fdCertificateGroup.right = new FormAttachment( 100, -margin );
    wCertificateGroup.setLayoutData( fdCertificateGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF Certificate GROUP
    // ///////////////////////////////////////////////////////////

    // Test LDAP connection button
    wTest = new Button( wGeneralComp, SWT.PUSH );
    wTest.setText( BaseMessages.getString( PKG, "LDAPInputDialog.TestConnection.Label" ) );
    props.setLook( wTest );
    fdTest = new FormData();
    wTest.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.TestConnection.Tooltip" ) );
    // fdTest.left = new FormAttachment(middle, 0);
    fdTest.top = new FormAttachment( wCertificateGroup, margin );
    fdTest.right = new FormAttachment( 100, 0 );
    wTest.setLayoutData( fdTest );

    fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.layout();
    wGeneralTab.setControl( wGeneralComp );

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Search TAB///
    // /
    wSearchTab = new CTabItem( wTabFolder, SWT.NONE );
    wSearchTab.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Search.Tab" ) );

    FormLayout SearchLayout = new FormLayout();
    SearchLayout.marginWidth = 3;
    SearchLayout.marginHeight = 3;

    wSearchComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wSearchComp );
    wSearchComp.setLayout( SearchLayout );

    // /////////////////////////////////
    // START OF Search GROUP
    // /////////////////////////////////

    wSearchGroup = new Group( wSearchComp, SWT.SHADOW_NONE );
    props.setLook( wSearchGroup );
    wSearchGroup.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Group.SearchGroup.Label" ) );

    FormLayout searchgroupLayout = new FormLayout();
    searchgroupLayout.marginWidth = 10;
    searchgroupLayout.marginHeight = 10;
    wSearchGroup.setLayout( searchgroupLayout );

    // Is base defined in a Field
    wldynamicBase = new Label( wSearchGroup, SWT.RIGHT );
    wldynamicBase.setText( BaseMessages.getString( PKG, "LDAPInputDialog.dynamicBase.Label" ) );
    props.setLook( wldynamicBase );
    fdlynamicBase = new FormData();
    fdlynamicBase.left = new FormAttachment( 0, -margin );
    fdlynamicBase.top = new FormAttachment( wStepname, margin );
    fdlynamicBase.right = new FormAttachment( middle, -2 * margin );
    wldynamicBase.setLayoutData( fdlynamicBase );

    wdynamicBase = new Button( wSearchGroup, SWT.CHECK );
    props.setLook( wdynamicBase );
    wdynamicBase.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.dynamicBase.Tooltip" ) );
    fdynamicBase = new FormData();
    fdynamicBase.left = new FormAttachment( middle, -margin );
    fdynamicBase.top = new FormAttachment( wStepname, margin );
    wdynamicBase.setLayoutData( fdynamicBase );
    SelectionAdapter ldynamicBase = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        ActiveDynamicBase();
        input.setChanged();
      }
    };
    wdynamicBase.addSelectionListener( ldynamicBase );

    // dynamic search base field
    wlsearchBaseField = new Label( wSearchGroup, SWT.RIGHT );
    wlsearchBaseField.setText( BaseMessages.getString( PKG, "LDAPInputDialog.wsearchBaseField.Label" ) );
    props.setLook( wlsearchBaseField );
    fdlsearchBaseField = new FormData();
    fdlsearchBaseField.left = new FormAttachment( 0, -margin );
    fdlsearchBaseField.top = new FormAttachment( wdynamicBase, margin );
    fdlsearchBaseField.right = new FormAttachment( middle, -2 * margin );
    wlsearchBaseField.setLayoutData( fdlsearchBaseField );

    wsearchBaseField = new CCombo( wSearchGroup, SWT.BORDER | SWT.READ_ONLY );
    wsearchBaseField.setEditable( true );
    props.setLook( wsearchBaseField );
    wsearchBaseField.addModifyListener( lsMod );
    fdsearchBaseField = new FormData();
    fdsearchBaseField.left = new FormAttachment( middle, -margin );
    fdsearchBaseField.top = new FormAttachment( wdynamicBase, margin );
    fdsearchBaseField.right = new FormAttachment( 100, -2 * margin );
    wsearchBaseField.setLayoutData( fdsearchBaseField );
    wsearchBaseField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        setSearchBaseField();
      }
    } );

    // SearchBase line
    wlSearchBase = new Label( wSearchGroup, SWT.RIGHT );
    wlSearchBase.setText( BaseMessages.getString( PKG, "LDAPInputDialog.SearchBase.Label" ) );
    props.setLook( wlSearchBase );
    fdlSearchBase = new FormData();
    fdlSearchBase.left = new FormAttachment( 0, -margin );
    fdlSearchBase.top = new FormAttachment( wsearchBaseField, margin );
    fdlSearchBase.right = new FormAttachment( middle, -2 * margin );
    wlSearchBase.setLayoutData( fdlSearchBase );
    wSearchBase = new TextVar( transMeta, wSearchGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSearchBase );
    wSearchBase.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.SearchBase.Tooltip" ) );
    wSearchBase.addModifyListener( lsMod );
    fdSearchBase = new FormData();
    fdSearchBase.left = new FormAttachment( middle, -margin );
    fdSearchBase.top = new FormAttachment( wsearchBaseField, margin );
    fdSearchBase.right = new FormAttachment( 100, -2 * margin );
    wSearchBase.setLayoutData( fdSearchBase );

    // Is filter defined in a Field
    wldynamicFilter = new Label( wSearchGroup, SWT.RIGHT );
    wldynamicFilter.setText( BaseMessages.getString( PKG, "LDAPInputDialog.dynamicFilter.Label" ) );
    props.setLook( wldynamicFilter );
    fdldynamicFilter = new FormData();
    fdldynamicFilter.left = new FormAttachment( 0, -margin );
    fdldynamicFilter.top = new FormAttachment( wSearchBase, margin );
    fdldynamicFilter.right = new FormAttachment( middle, -2 * margin );
    wldynamicFilter.setLayoutData( fdldynamicFilter );

    wdynamicFilter = new Button( wSearchGroup, SWT.CHECK );
    props.setLook( wdynamicFilter );
    wdynamicFilter.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.dynamicFilter.Tooltip" ) );
    fdynamicFilter = new FormData();
    fdynamicFilter.left = new FormAttachment( middle, -margin );
    fdynamicFilter.top = new FormAttachment( wSearchBase, margin );
    wdynamicFilter.setLayoutData( fdynamicFilter );
    SelectionAdapter ldynamicFilter = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        ActivedynamicFilter();
        input.setChanged();
      }
    };
    wdynamicFilter.addSelectionListener( ldynamicFilter );

    // dynamic search base field
    wlfilterField = new Label( wSearchGroup, SWT.RIGHT );
    wlfilterField.setText( BaseMessages.getString( PKG, "LDAPInputDialog.filterField.Label" ) );
    props.setLook( wlfilterField );
    fdlfilterField = new FormData();
    fdlfilterField.left = new FormAttachment( 0, -margin );
    fdlfilterField.top = new FormAttachment( wdynamicFilter, margin );
    fdlfilterField.right = new FormAttachment( middle, -2 * margin );
    wlfilterField.setLayoutData( fdlfilterField );

    wfilterField = new CCombo( wSearchGroup, SWT.BORDER | SWT.READ_ONLY );
    wfilterField.setEditable( true );
    props.setLook( wfilterField );
    wfilterField.addModifyListener( lsMod );
    fdfilterField = new FormData();
    fdfilterField.left = new FormAttachment( middle, -margin );
    fdfilterField.top = new FormAttachment( wdynamicFilter, margin );
    fdfilterField.right = new FormAttachment( 100, -2 * margin );
    wfilterField.setLayoutData( fdfilterField );
    wfilterField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        setSearchBaseField();
      }
    } );

    // Filter String
    wlFilterString = new Label( wSearchGroup, SWT.RIGHT );
    wlFilterString.setText( BaseMessages.getString( PKG, "LDAPInputDialog.FilterString.Label" ) );
    props.setLook( wlFilterString );
    fdlFilterString = new FormData();
    fdlFilterString.left = new FormAttachment( 0, 0 );
    fdlFilterString.top = new FormAttachment( wfilterField, margin );
    fdlFilterString.right = new FormAttachment( middle, -2 * margin );
    wlFilterString.setLayoutData( fdlFilterString );

    wFilterString =
      new StyledTextComp( transMeta, wSearchGroup, SWT.MULTI
        | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "" );
    wFilterString.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.FilterString.Tooltip" ) );
    props.setLook( wFilterString );
    wFilterString.addModifyListener( lsMod );
    fdFilterString = new FormData();
    fdFilterString.left = new FormAttachment( middle, -margin );
    fdFilterString.top = new FormAttachment( wfilterField, margin );
    fdFilterString.right = new FormAttachment( 100, -2 * margin );
    fdFilterString.bottom = new FormAttachment( 100, -margin );
    wFilterString.setLayoutData( fdFilterString );
    wFilterString.addKeyListener( new ControlSpaceKeyAdapter( transMeta, wFilterString ) );

    fdSearchGroup = new FormData();
    fdSearchGroup.left = new FormAttachment( 0, margin );
    fdSearchGroup.top = new FormAttachment( wStepname, margin );
    fdSearchGroup.right = new FormAttachment( 100, -margin );
    fdSearchGroup.bottom = new FormAttachment( 100, -margin );
    wSearchGroup.setLayoutData( fdSearchGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF Search GROUP
    // ///////////////////////////////////////////////////////////

    fdSearchComp = new FormData();
    fdSearchComp.left = new FormAttachment( 0, 0 );
    fdSearchComp.top = new FormAttachment( 0, 0 );
    fdSearchComp.right = new FormAttachment( 100, 0 );
    fdSearchComp.bottom = new FormAttachment( 100, 0 );
    wSearchComp.setLayoutData( fdSearchComp );

    wSearchComp.layout();
    wSearchTab.setControl( wSearchComp );

    // ///////////////////////////////////////////////////////////
    // / END OF Search TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    wContentTab = new CTabItem( wTabFolder, SWT.NONE );
    wContentTab.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Content.Tab" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    wContentComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wContentComp );
    wContentComp.setLayout( contentLayout );

    // /////////////////////////////////
    // START OF Additional Fields GROUP
    // /////////////////////////////////

    wAdditionalGroup = new Group( wContentComp, SWT.SHADOW_NONE );
    props.setLook( wAdditionalGroup );
    wAdditionalGroup.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Group.AdditionalGroup.Label" ) );

    FormLayout additionalgroupLayout = new FormLayout();
    additionalgroupLayout.marginWidth = 10;
    additionalgroupLayout.marginHeight = 10;
    wAdditionalGroup.setLayout( additionalgroupLayout );

    wlInclRownum = new Label( wAdditionalGroup, SWT.RIGHT );
    wlInclRownum.setText( BaseMessages.getString( PKG, "LDAPInputDialog.InclRownum.Label" ) );
    props.setLook( wlInclRownum );
    fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment( 0, 0 );
    fdlInclRownum.top = new FormAttachment( 0, margin );
    fdlInclRownum.right = new FormAttachment( middle, -margin );
    wlInclRownum.setLayoutData( fdlInclRownum );
    wInclRownum = new Button( wAdditionalGroup, SWT.CHECK );
    props.setLook( wInclRownum );
    wInclRownum.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.InclRownum.Tooltip" ) );
    fdRownum = new FormData();
    fdRownum.left = new FormAttachment( middle, 0 );
    fdRownum.top = new FormAttachment( 0, margin );
    wInclRownum.setLayoutData( fdRownum );
    wInclRownum.addSelectionListener( new ComponentSelectionListener( input ) );

    wlInclRownumField = new Label( wAdditionalGroup, SWT.RIGHT );
    wlInclRownumField.setText( BaseMessages.getString( PKG, "LDAPInputDialog.InclRownumField.Label" ) );
    props.setLook( wlInclRownumField );
    fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment( wInclRownum, margin );
    fdlInclRownumField.top = new FormAttachment( 0, margin );
    wlInclRownumField.setLayoutData( fdlInclRownumField );
    wInclRownumField = new TextVar( transMeta, wAdditionalGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclRownumField );
    wInclRownumField.addModifyListener( lsMod );
    fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment( wlInclRownumField, margin );
    fdInclRownumField.top = new FormAttachment( 0, margin );
    fdInclRownumField.right = new FormAttachment( 100, 0 );
    wInclRownumField.setLayoutData( fdInclRownumField );

    fdAdditionalGroup = new FormData();
    fdAdditionalGroup.left = new FormAttachment( 0, margin );
    fdAdditionalGroup.top = new FormAttachment( 0, margin );
    fdAdditionalGroup.right = new FormAttachment( 100, -margin );
    wAdditionalGroup.setLayoutData( fdAdditionalGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF DESTINATION ADDRESS GROUP
    // ///////////////////////////////////////////////////////////

    wlLimit = new Label( wContentComp, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Limit.Label" ) );
    props.setLook( wlLimit );
    fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.top = new FormAttachment( wAdditionalGroup, 2 * margin );
    fdlLimit.right = new FormAttachment( middle, -margin );
    wlLimit.setLayoutData( fdlLimit );
    wLimit = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.top = new FormAttachment( wAdditionalGroup, 2 * margin );
    fdLimit.right = new FormAttachment( 100, 0 );
    wLimit.setLayoutData( fdLimit );

    // TimeLimit
    wlTimeLimit = new Label( wContentComp, SWT.RIGHT );
    wlTimeLimit.setText( BaseMessages.getString( PKG, "LDAPInputDialog.TimeLimit.Label" ) );
    props.setLook( wlTimeLimit );
    fdlTimeLimit = new FormData();
    fdlTimeLimit.left = new FormAttachment( 0, 0 );
    fdlTimeLimit.top = new FormAttachment( wLimit, margin );
    fdlTimeLimit.right = new FormAttachment( middle, -margin );
    wlTimeLimit.setLayoutData( fdlTimeLimit );
    wTimeLimit = new TextVar( transMeta, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTimeLimit );
    wTimeLimit.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.TimeLimit.Tooltip" ) );
    wTimeLimit.addModifyListener( lsMod );

    fdTimeLimit = new FormData();
    fdTimeLimit.left = new FormAttachment( middle, 0 );
    fdTimeLimit.top = new FormAttachment( wLimit, margin );
    fdTimeLimit.right = new FormAttachment( 100, 0 );
    wTimeLimit.setLayoutData( fdTimeLimit );

    // Multi valued field separator
    wlMultiValuedSeparator = new Label( wContentComp, SWT.RIGHT );
    wlMultiValuedSeparator.setText( BaseMessages.getString( PKG, "LDAPInputDialog.MultiValuedSeparator.Label" ) );
    props.setLook( wlMultiValuedSeparator );
    fdlMultiValuedSeparator = new FormData();
    fdlMultiValuedSeparator.left = new FormAttachment( 0, 0 );
    fdlMultiValuedSeparator.top = new FormAttachment( wTimeLimit, margin );
    fdlMultiValuedSeparator.right = new FormAttachment( middle, -margin );
    wlMultiValuedSeparator.setLayoutData( fdlMultiValuedSeparator );
    wMultiValuedSeparator = new TextVar( transMeta, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMultiValuedSeparator );
    wMultiValuedSeparator.setToolTipText( BaseMessages.getString(
      PKG, "LDAPInputDialog.MultiValuedSeparator.Tooltip" ) );
    wMultiValuedSeparator.addModifyListener( lsMod );
    fdMultiValuedSeparator = new FormData();
    fdMultiValuedSeparator.left = new FormAttachment( middle, 0 );
    fdMultiValuedSeparator.top = new FormAttachment( wTimeLimit, margin );
    fdMultiValuedSeparator.right = new FormAttachment( 100, 0 );
    wMultiValuedSeparator.setLayoutData( fdMultiValuedSeparator );

    // Use page ranging?
    wlsetPaging = new Label( wContentComp, SWT.RIGHT );
    wlsetPaging.setText( BaseMessages.getString( PKG, "LDAPInputDialog.setPaging.Label" ) );
    props.setLook( wlsetPaging );
    fdlsetPaging = new FormData();
    fdlsetPaging.left = new FormAttachment( 0, 0 );
    fdlsetPaging.top = new FormAttachment( wMultiValuedSeparator, margin );
    fdlsetPaging.right = new FormAttachment( middle, -margin );
    wlsetPaging.setLayoutData( fdlsetPaging );
    wsetPaging = new Button( wContentComp, SWT.CHECK );
    props.setLook( wsetPaging );
    wsetPaging.setToolTipText( BaseMessages.getString( PKG, "LDAPInputDialog.setPaging.Tooltip" ) );
    fdsetPaging = new FormData();
    fdsetPaging.left = new FormAttachment( middle, 0 );
    fdsetPaging.top = new FormAttachment( wMultiValuedSeparator, margin );
    wsetPaging.setLayoutData( fdsetPaging );
    wsetPaging.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setPaging();
        input.setChanged();
      }
    } );
    wlPageSize = new Label( wContentComp, SWT.RIGHT );
    wlPageSize.setText( BaseMessages.getString( PKG, "LDAPInputDialog.PageSize.Label" ) );
    props.setLook( wlPageSize );
    fdlPageSize = new FormData();
    fdlPageSize.left = new FormAttachment( wsetPaging, margin );
    fdlPageSize.top = new FormAttachment( wMultiValuedSeparator, margin );
    wlPageSize.setLayoutData( fdlPageSize );
    wPageSize = new TextVar( transMeta, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPageSize );
    wPageSize.addModifyListener( lsMod );
    fdPageSize = new FormData();
    fdPageSize.left = new FormAttachment( wlPageSize, margin );
    fdPageSize.top = new FormAttachment( wMultiValuedSeparator, margin );
    fdPageSize.right = new FormAttachment( 100, 0 );
    wPageSize.setLayoutData( fdPageSize );

    // searchScope
    wlsearchScope = new Label( wContentComp, SWT.RIGHT );
    wlsearchScope.setText( BaseMessages.getString( PKG, "LDAPInputDialog.SearchScope.Label" ) );
    props.setLook( wlsearchScope );
    fdlsearchScope = new FormData();
    fdlsearchScope.left = new FormAttachment( 0, 0 );
    fdlsearchScope.right = new FormAttachment( middle, -margin );
    fdlsearchScope.top = new FormAttachment( wPageSize, margin );
    wlsearchScope.setLayoutData( fdlsearchScope );

    wsearchScope = new CCombo( wContentComp, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wsearchScope );
    wsearchScope.addModifyListener( lsMod );
    fdsearchScope = new FormData();
    fdsearchScope.left = new FormAttachment( middle, 0 );
    fdsearchScope.top = new FormAttachment( wPageSize, margin );
    fdsearchScope.right = new FormAttachment( 100, -margin );
    wsearchScope.setLayoutData( fdsearchScope );
    wsearchScope.setItems( LDAPInputMeta.searchScopeDesc );
    wsearchScope.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

      }
    } );

    fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment( 0, 0 );
    fdContentComp.top = new FormAttachment( 0, 0 );
    fdContentComp.right = new FormAttachment( 100, 0 );
    fdContentComp.bottom = new FormAttachment( 100, 0 );
    wContentComp.setLayoutData( fdContentComp );

    wContentComp.layout();
    wContentTab.setControl( wContentComp );

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    // Fields tab...
    //
    wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Fields.Tab" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook( wFieldsComp );

    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "LDAPInputDialog.GetFields.Button" ) );
    fdGet = new FormData();
    fdGet.left = new FormAttachment( 50, 0 );
    fdGet.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    final int FieldsRows = input.getInputFields().length;

    colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.Name.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.Attribute.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.FetchAttributeAs.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, LDAPInputField.FetchAttributeAsDesc, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.IsSortedKey.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {
          BaseMessages.getString( PKG, "System.Combo.Yes" ),
          BaseMessages.getString( PKG, "System.Combo.No" ) }, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.Type.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames(), true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.Format.Column" ),
          ColumnInfo.COLUMN_TYPE_FORMAT, 3 ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.Length.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.Precision.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.Currency.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.Decimal.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.Group.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.TrimType.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, LDAPInputField.trimTypeDesc, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.Repeat.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {
          BaseMessages.getString( PKG, "System.Combo.Yes" ),
          BaseMessages.getString( PKG, "System.Combo.No" ) }, true ),

      };

    colinf[ 0 ].setUsingVariables( true );
    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.Name.Column.Tooltip" ) );
    colinf[ 1 ].setUsingVariables( true );
    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "LDAPInputDialog.FieldsTable.Attribute.Column.Tooltip" ) );

    wFields =
      new TableView( transMeta, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -margin );
    wFields.setLayoutData( fdFields );

    fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wStepname, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Button.PreviewRows" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wPreview, wCancel }, margin, wTabFolder );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };
    lsTest = new Listener() {
      public void handleEvent( Event e ) {
        test();
      }
    };
    lsPreview = new Listener() {
      public void handleEvent( Event e ) {
        preview();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wGet.addListener( SWT.Selection, lsGet );
    wTest.addListener( SWT.Selection, lsTest );
    wPreview.addListener( SWT.Selection, lsPreview );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wLimit.addSelectionListener( lsDef );
    wInclRownumField.addSelectionListener( lsDef );

    // Enable/disable the right fields to allow a row number to be added to each row...
    wInclRownum.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setIncludeRownum();
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();
    getData( input );
    setProtocol();
    setTrustStore();
    useAuthentication();
    setPaging();
    ActiveDynamicBase();
    ActivedynamicFilter();
    input.setChanged( changed );

    wFields.optWidth( true );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private void test() {
    LDAPConnection connection = null;
    try {

      LDAPInputMeta meta = new LDAPInputMeta();
      getInfo( meta );

      // Defined a LDAP connection
      connection = new LDAPConnection( log, transMeta, meta, null );

      // connect...
      if ( wusingAuthentication.getSelection() ) {
        connection.connect( transMeta.environmentSubstitute( meta.getUserName() ), Encr
          .decryptPasswordOptionallyEncrypted( transMeta.environmentSubstitute( meta.getPassword() ) ) );
      } else {
        connection.connect();
      }
      // We are successfully connected

      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "LDAPInputDialog.Connected.OK" ) + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "LDAPInputDialog.Connected.Title.Ok" ) );
      mb.open();

    } catch ( Exception e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "LDAPInputDialog.Connected.Title.Error" ), BaseMessages
        .getString( PKG, "LDAPInputDialog.Connected.NOK" ), e );
    } finally {
      if ( connection != null ) {
        // Disconnect ...
        try {
          connection.close();
        } catch ( Exception e ) { /* Ignore */
        }
      }
    }
  }

  private void get() {
    LDAPConnection connection = null;
    try {

      LDAPInputMeta meta = new LDAPInputMeta();
      getInfo( meta );

      // Clear Fields Grid
      wFields.removeAll();

      // Defined a LDAP connection
      connection = new LDAPConnection( log, transMeta, meta, null );

      // connect ...
      if ( meta.UseAuthentication() ) {
        String username = transMeta.environmentSubstitute( meta.getUserName() );
        String password =
          Encr.decryptPasswordOptionallyEncrypted( transMeta.environmentSubstitute( meta.getPassword() ) );
        connection.connect( username, password );
      } else {
        connection.connect();
      }

      // return fields
      RowMeta listattributes = connection.getFields( transMeta.environmentSubstitute( meta.getSearchBase() ) );
      String[] fieldsName = new String[ listattributes.size() ];
      for ( int i = 0; i < listattributes.size(); i++ ) {

        ValueMetaInterface v = listattributes.getValueMeta( i );
        fieldsName[ i ] = v.getName();
        // Get Column Name
        TableItem item = new TableItem( wFields.table, SWT.NONE );
        item.setText( 1, v.getName() );
        item.setText( 2, v.getName() );

        if ( LDAPInputField.binaryAttributes.contains( v.getName() ) ) {
          item.setText( 3, BaseMessages.getString( PKG, "LDAPInputField.FetchAttributeAs.Binary" ) );
        } else {
          item.setText( 3, BaseMessages.getString( PKG, "LDAPInputField.FetchAttributeAs.String" ) );
        }
        item.setText( 4, BaseMessages.getString( PKG, "System.Combo.No" ) );
        item.setText( 5, v.getTypeDesc() );
      }
      colinf[ 1 ].setComboValues( fieldsName );
      wFields.removeEmptyRows();
      wFields.setRowNums();
      wFields.optWidth( true );

    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "LDAPInputDialog.ErrorGettingColums.DialogTitle" ), BaseMessages
        .getString( PKG, "LDAPInputDialog.ErrorGettingColums.DialogMessage" ), e );
    } catch ( Exception e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "LDAPInputDialog.ErrorGettingColums.DialogTitle" ), BaseMessages
        .getString( PKG, "LDAPInputDialog.ErrorGettingColums.DialogMessage" ), e );

    } finally {
      if ( connection != null ) {
        try {
          connection.close();
        } catch ( Exception e ) { /* Ignore */
        }
      }
    }
  }

  private void setPaging() {
    wlPageSize.setEnabled( wsetPaging.getSelection() );
    wPageSize.setEnabled( wsetPaging.getSelection() );
  }

  public void setIncludeRownum() {
    wlInclRownumField.setEnabled( wInclRownum.getSelection() );
    wInclRownumField.setEnabled( wInclRownum.getSelection() );
  }

  /**
   * Read the data from the LDAPInputMeta object and show it in this dialog.
   *
   * @param in The LDAPInputMeta object to obtain the data from.
   */
  public void getData( LDAPInputMeta in ) {
    wProtocol.setText( Const.NVL( in.getProtocol(), LdapProtocolFactory.getConnectionTypes( log ).get( 0 ) ) );
    wsetTrustStore.setSelection( in.isUseCertificate() );
    if ( in.getTrustStorePath() != null ) {
      wTrustStorePath.setText( in.getTrustStorePath() );
    }
    if ( in.getTrustStorePassword() != null ) {
      wTrustStorePassword.setText( in.getTrustStorePassword() );
    }
    wTrustAll.setSelection( in.isTrustAllCertificates() );

    wInclRownum.setSelection( in.includeRowNumber() );
    if ( in.getRowNumberField() != null ) {
      wInclRownumField.setText( in.getRowNumberField() );
    }

    wusingAuthentication.setSelection( in.UseAuthentication() );
    wsetPaging.setSelection( in.isPaging() );
    if ( in.getPageSize() != null ) {
      wPageSize.setText( in.getPageSize() );
    }

    wLimit.setText( "" + in.getRowLimit() );
    wTimeLimit.setText( "" + in.getTimeLimit() );
    if ( in.getMultiValuedSeparator() != null ) {
      wMultiValuedSeparator.setText( in.getMultiValuedSeparator() );
    }

    if ( in.getHost() != null ) {
      wHost.setText( in.getHost() );
    }
    if ( in.getUserName() != null ) {
      wUserName.setText( in.getUserName() );
    }
    if ( in.getPassword() != null ) {
      wPassword.setText( in.getPassword() );
    }
    if ( in.getPort() != null ) {
      wPort.setText( in.getPort() );
    }

    if ( in.getFilterString() != null ) {
      wFilterString.setText( in.getFilterString() );
    }
    if ( in.getSearchBase() != null ) {
      wSearchBase.setText( in.getSearchBase() );
    }
    wdynamicBase.setSelection( in.isDynamicSearch() );
    if ( in.getDynamicSearchFieldName() != null ) {
      wsearchBaseField.setText( in.getDynamicSearchFieldName() );
    }

    wdynamicFilter.setSelection( in.isDynamicFilter() );
    if ( in.getDynamicFilterFieldName() != null ) {
      wfilterField.setText( in.getDynamicFilterFieldName() );
    }
    wsearchScope.setText( LDAPInputMeta.getSearchScopeDesc( in.getSearchScope() ) );
    if ( isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "LDAPInputDialog.Log.GettingFieldsInfo" ) );
    }
    for ( int i = 0; i < in.getInputFields().length; i++ ) {
      LDAPInputField field = in.getInputFields()[ i ];

      if ( field != null ) {
        TableItem item = wFields.table.getItem( i );
        String name = field.getName();
        String path = field.getAttribute();
        String issortedkey =
          field.isSortedKey() ? BaseMessages.getString( PKG, "System.Combo.Yes" ) : BaseMessages.getString(
            PKG, "System.Combo.No" );
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
          field.isRepeated() ? BaseMessages.getString( PKG, "System.Combo.Yes" ) : BaseMessages.getString(
            PKG, "System.Combo.No" );

        if ( name != null ) {
          item.setText( 1, name );
        }
        if ( path != null ) {
          item.setText( 2, path );
        }
        if ( returntype != null ) {
          item.setText( 3, returntype );
        }
        if ( issortedkey != null ) {
          item.setText( 4, issortedkey );
        }
        if ( type != null ) {
          item.setText( 5, type );
        }
        if ( format != null ) {
          item.setText( 6, format );
        }
        if ( length != null && !"-1".equals( length ) ) {
          item.setText( 5, length );
        }
        if ( prec != null && !"-1".equals( prec ) ) {
          item.setText( 8, prec );
        }
        if ( curr != null ) {
          item.setText( 9, curr );
        }
        if ( decim != null ) {
          item.setText( 10, decim );
        }
        if ( group != null ) {
          item.setText( 11, group );
        }
        if ( trim != null ) {
          item.setText( 12, trim );
        }
        if ( rep != null ) {
          item.setText( 13, rep );
        }
      }
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );

    setIncludeRownum();

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText();
    try {
      getInfo( input );
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "LDAPInputDialog.ErrorParsingData.DialogTitle" ), BaseMessages
        .getString( PKG, "LDAPInputDialog.ErrorParsingData.DialogMessage" ), e );
    }
    dispose();
  }

  private void getInfo( LDAPInputMeta in ) throws HopException {
    stepname = wStepname.getText(); // return value
    in.setProtocol( wProtocol.getText() );
    in.setUseCertificate( wsetTrustStore.getSelection() );
    in.setTrustStorePath( wTrustStorePath.getText() );
    in.setTrustStorePassword( wTrustStorePassword.getText() );
    in.setTrustAllCertificates( wTrustAll.getSelection() );

    // copy info to TextFileInputMeta class (input)
    in.setRowLimit( Const.toInt( wLimit.getText(), 0 ) );
    in.setTimeLimit( Const.toInt( wTimeLimit.getText(), 0 ) );
    in.setMultiValuedSeparator( wMultiValuedSeparator.getText() );
    in.setIncludeRowNumber( wInclRownum.getSelection() );
    in.setUseAuthentication( wusingAuthentication.getSelection() );
    in.setPaging( wsetPaging.getSelection() );
    in.setPageSize( wPageSize.getText() );
    in.setRowNumberField( wInclRownumField.getText() );
    in.setHost( wHost.getText() );
    in.setUserName( wUserName.getText() );
    in.setPassword( wPassword.getText() );
    in.setPort( wPort.getText() );
    in.setFilterString( wFilterString.getText() );
    in.setSearchBase( wSearchBase.getText() );
    in.setDynamicSearch( wdynamicBase.getSelection() );
    in.setDynamicSearchFieldName( wsearchBaseField.getText() );
    in.setDynamicFilter( wdynamicFilter.getSelection() );
    in.setDynamicFilterFieldName( wfilterField.getText() );

    int nrFields = wFields.nrNonEmpty();

    in.allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      LDAPInputField field = new LDAPInputField();

      TableItem item = wFields.getNonEmpty( i );

      field.setName( item.getText( 1 ) );
      field.setAttribute( item.getText( 2 ) );
      field.setFetchAttributeAs( LDAPInputField.getFetchAttributeAsByDesc( item.getText( 3 ) ) );
      field.setSortedKey( BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase( item.getText( 4 ) ) );
      field.setType( ValueMetaFactory.getIdForValueMeta( item.getText( 5 ) ) );
      field.setFormat( item.getText( 6 ) );
      field.setLength( Const.toInt( item.getText( 7 ), -1 ) );
      field.setPrecision( Const.toInt( item.getText( 8 ), -1 ) );
      field.setCurrencySymbol( item.getText( 9 ) );
      field.setDecimalSymbol( item.getText( 10 ) );
      field.setGroupSymbol( item.getText( 11 ) );
      field.setTrimType( LDAPInputField.getTrimTypeByDesc( item.getText( 12 ) ) );
      field.setRepeated( BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase( item.getText( 13 ) ) );

      //CHECKSTYLE:Indentation:OFF
      in.getInputFields()[ i ] = field;
    }
    in.setSearchScope( LDAPInputMeta.getSearchScopeByDesc( wsearchScope.getText() ) );
  }

  private void useAuthentication() {
    wUserName.setEnabled( wusingAuthentication.getSelection() );
    wlUserName.setEnabled( wusingAuthentication.getSelection() );
    wPassword.setEnabled( wusingAuthentication.getSelection() );
    wlPassword.setEnabled( wusingAuthentication.getSelection() );
  }

  // Preview the data
  private void preview() {
    try {
      // Create the XML input step
      LDAPInputMeta oneMeta = new LDAPInputMeta();
      getInfo( oneMeta );

      TransMeta previewMeta =
        TransPreviewFactory.generatePreviewTransformation( transMeta, oneMeta, wStepname.getText() );

      EnterNumberDialog numberDialog = new EnterNumberDialog( shell, props.getDefaultPreviewSize(),
        BaseMessages.getString( PKG, "LDAPInputDialog.NumberRows.DialogTitle" ),
        BaseMessages.getString( PKG, "LDAPInputDialog.NumberRows.DialogMessage" ) );
      int previewSize = numberDialog.open();
      if ( previewSize > 0 ) {
        TransPreviewProgressDialog progressDialog =
          new TransPreviewProgressDialog(
            shell, previewMeta, new String[] { wStepname.getText() }, new int[] { previewSize } );
        progressDialog.open();

        if ( !progressDialog.isCancelled() ) {
          Trans trans = progressDialog.getTrans();
          String loggingText = progressDialog.getLoggingText();

          if ( trans.getResult() != null && trans.getResult().getNrErrors() > 0 ) {
            EnterTextDialog etd =
              new EnterTextDialog(
                shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ), BaseMessages
                .getString( PKG, "System.Dialog.PreviewError.Message" ), loggingText, true );
            etd.setReadOnly();
            etd.open();
          }

          PreviewRowsDialog prd =
            new PreviewRowsDialog(
              shell, transMeta, SWT.NONE, wStepname.getText(), progressDialog.getPreviewRowsMeta( wStepname
              .getText() ), progressDialog.getPreviewRows( wStepname.getText() ), loggingText );
          prd.open();

        }
      }
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "LDAPInputDialog.ErrorPreviewingData.DialogTitle" ), BaseMessages
        .getString( PKG, "LDAPInputDialog.ErrorPreviewingData.DialogMessage" ), e );
    }
  }

  private void ActiveDynamicBase() {
    wSearchBase.setEnabled( !wdynamicBase.getSelection() );
    wlSearchBase.setEnabled( !wdynamicBase.getSelection() );
    wlsearchBaseField.setEnabled( wdynamicBase.getSelection() );
    wsearchBaseField.setEnabled( wdynamicBase.getSelection() );
    activatePreview();
    activateGetFields();
  }

  private void ActivedynamicFilter() {
    wlFilterString.setEnabled( !wdynamicFilter.getSelection() );
    wFilterString.setEnabled( !wdynamicFilter.getSelection() );
    wlfilterField.setEnabled( wdynamicFilter.getSelection() );
    wfilterField.setEnabled( wdynamicFilter.getSelection() );
    activatePreview();
  }

  private void activatePreview() {
    wPreview.setEnabled( !wdynamicBase.getSelection() && !wdynamicFilter.getSelection() );
  }

  private void activateGetFields() {
    wGet.setEnabled( !wdynamicBase.getSelection() );
  }

  private void setSearchBaseField() {
    if ( !gotPreviousFields ) {
      try {
        String basefield = wsearchBaseField.getText();
        String filterfield = wfilterField.getText();
        wsearchBaseField.removeAll();

        RowMetaInterface r = transMeta.getPrevStepFields( stepname );
        if ( r != null ) {
          wsearchBaseField.setItems( r.getFieldNames() );
          wfilterField.setItems( r.getFieldNames() );
        }
        if ( basefield != null ) {
          wsearchBaseField.setText( basefield );
        }
        if ( filterfield != null ) {
          wfilterField.setText( basefield );
        }

      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "LDAPInputDialog.FailedToGetFields.DialogTitle" ), BaseMessages
          .getString( PKG, "LDAPInputDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
  }

  private void setProtocol() {
    boolean enable = !LdapProtocol.getName().equals( wProtocol.getText() );
    wlsetTrustStore.setEnabled( enable );
    wsetTrustStore.setEnabled( enable );
    setTrustStore();
  }

  private void setTrustStore() {
    boolean enable = wsetTrustStore.getSelection() && !LdapProtocol.getName().equals( wProtocol.getText() );
    wlTrustAll.setEnabled( enable );
    wTrustAll.setEnabled( enable );
    trustAll();
  }

  private void trustAll() {
    boolean enable =
      wsetTrustStore.getSelection()
        && !LdapProtocol.getName().equals( wProtocol.getText() ) && !wTrustAll.getSelection();
    wlTrustStorePath.setEnabled( enable );
    wTrustStorePath.setEnabled( enable );
    wlTrustStorePassword.setEnabled( enable );
    wTrustStorePassword.setEnabled( enable );
    wbbFilename.setEnabled( enable );
  }
}
