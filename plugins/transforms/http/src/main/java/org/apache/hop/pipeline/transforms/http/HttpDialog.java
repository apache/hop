/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.http;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.*;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.nio.charset.Charset;
import java.util.List;
import java.util.*;

public class HttpDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = HttpMeta.class; // Needed by Translator

  private Label wlUrl;
  private TextVar wUrl;

  private TextVar wResult;

  private TableView wFields;

  private TableView wHeaders;

  private Button wUrlInField;

  private Label wlUrlField;
  private ComboVar wUrlField;

  private ComboVar wEncoding;

  private TextVar wHttpLogin;

  private TextVar wHttpPassword;

  private TextVar wProxyHost;

  private TextVar wProxyPort;

  private TextVar wResultCode;

  private TextVar wResponseTime;

  private TextVar wResponseHeader;

  private final HttpMeta input;

  private ColumnInfo[] colinf;
  private ColumnInfo[] colinfHeaders;

  private final Map<String, Integer> inputFields;

  private boolean gotEncodings = false;

  private TextVar wConnectionTimeOut;

  private TextVar wSocketTimeOut;

  private TextVar wCloseIdleConnectionsTime;

  public HttpDialog( Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (HttpMeta) in;
    inputFields = new HashMap<>();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "HTTPDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null);

    // TransformName line
    //
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "HTTPDialog.TransformName.Label" ) );
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
    Control lastControl = wTransformName;

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, PropsUi.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "HTTPDialog.GeneralTab.Title" ) );

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wGeneralComp.setLayout( fileLayout );

    // ////////////////////////
    // START Settings GROUP

    Group gSettings = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN );
    gSettings.setText( BaseMessages.getString( PKG, "HTTPDialog.SettingsGroup.Label" ) );
    FormLayout SettingsLayout = new FormLayout();
    SettingsLayout.marginWidth = 3;
    SettingsLayout.marginHeight = 3;
    gSettings.setLayout( SettingsLayout );
    props.setLook( gSettings );

    // The URL to use
    //
    wlUrl = new Label( gSettings, SWT.RIGHT );
    wlUrl.setText( BaseMessages.getString( PKG, "HTTPDialog.URL.Label" ) );
    props.setLook( wlUrl );
    FormData fdlUrl = new FormData();
    fdlUrl.left = new FormAttachment( 0, 0 );
    fdlUrl.right = new FormAttachment( middle, -margin );
    fdlUrl.top = new FormAttachment( lastControl, margin );
    wlUrl.setLayoutData(fdlUrl);

    wUrl = new TextVar( pipelineMeta, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUrl );
    wUrl.addModifyListener( lsMod );
    FormData fdUrl = new FormData();
    fdUrl.left = new FormAttachment( middle, 0 );
    fdUrl.top = new FormAttachment( lastControl, margin );
    fdUrl.right = new FormAttachment( 100, 0 );
    wUrl.setLayoutData(fdUrl);
    lastControl = wUrl;

    // UrlInField line
    //
    Label wlUrlInField = new Label(gSettings, SWT.RIGHT);
    wlUrlInField.setText( BaseMessages.getString( PKG, "HTTPDialog.UrlInField.Label" ) );
    props.setLook(wlUrlInField);
    FormData fdlUrlInField = new FormData();
    fdlUrlInField.left = new FormAttachment( 0, 0 );
    fdlUrlInField.top = new FormAttachment( lastControl, margin );
    fdlUrlInField.right = new FormAttachment( middle, -margin );
    wlUrlInField.setLayoutData(fdlUrlInField);
    wUrlInField = new Button( gSettings, SWT.CHECK );
    props.setLook( wUrlInField );
    FormData fdUrlInField = new FormData();
    fdUrlInField.left = new FormAttachment( middle, 0 );
    fdUrlInField.top = new FormAttachment( wlUrlInField, 0, SWT.CENTER );
    fdUrlInField.right = new FormAttachment( 100, 0 );
    wUrlInField.setLayoutData(fdUrlInField);
    wUrlInField.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        activeUrlInfield();
      }
    } );
    lastControl = wUrlInField;

    // UrlField Line
    //
    wlUrlField = new Label( gSettings, SWT.RIGHT );
    wlUrlField.setText( BaseMessages.getString( PKG, "HTTPDialog.UrlField.Label" ) );
    props.setLook( wlUrlField );
    FormData fdlUrlField = new FormData();
    fdlUrlField.left = new FormAttachment( 0, 0 );
    fdlUrlField.right = new FormAttachment( middle, -margin );
    fdlUrlField.top = new FormAttachment( lastControl, margin );
    wlUrlField.setLayoutData(fdlUrlField);

    wUrlField = new ComboVar( pipelineMeta, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wUrlField.setToolTipText( BaseMessages.getString( PKG, "HTTPDialog.UrlField.Tooltip" ) );
    props.setLook( wUrlField );
    wUrlField.addModifyListener( lsMod );
    FormData fdUrlField = new FormData();
    fdUrlField.left = new FormAttachment( middle, 0 );
    fdUrlField.top = new FormAttachment( lastControl, margin );
    fdUrlField.right = new FormAttachment( 100, 0 );
    wUrlField.setLayoutData(fdUrlField);
    wUrlField.setEnabled( false );
    wUrlField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        BaseTransformDialog.getFieldsFromPrevious( wUrlField, pipelineMeta, transformMeta );
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    lastControl = wUrlField;

    // Encoding
    //
    Label wlEncoding = new Label( gSettings, SWT.RIGHT );
    wlEncoding.setText( BaseMessages.getString( PKG, "HTTPDialog.Encoding.Label" ) );
    props.setLook( wlEncoding );
    FormData fdlEncoding = new FormData();
    fdlEncoding.top = new FormAttachment( lastControl, margin );
    fdlEncoding.left = new FormAttachment( 0, 0 );
    fdlEncoding.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData( fdlEncoding );
    wEncoding = new ComboVar( pipelineMeta, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEncoding );
    wEncoding.addModifyListener( lsMod );
    FormData fdEncoding = new FormData();
    fdEncoding.top = new FormAttachment( lastControl, margin );
    fdEncoding.left = new FormAttachment( middle, 0 );
    fdEncoding.right = new FormAttachment( 100, 0 );
    wEncoding.setLayoutData( fdEncoding );
    lastControl = wEncoding;
    wEncoding.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setEncodings();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    Label wlConnectionTimeOut = new Label(gSettings, SWT.RIGHT);
    wlConnectionTimeOut.setText( BaseMessages.getString( PKG, "HTTPDialog.ConnectionTimeOut.Label" ) );
    props.setLook(wlConnectionTimeOut);
    FormData fdlConnectionTimeOut = new FormData();
    fdlConnectionTimeOut.top = new FormAttachment( wEncoding, margin );
    fdlConnectionTimeOut.left = new FormAttachment( 0, 0 );
    fdlConnectionTimeOut.right = new FormAttachment( middle, -margin );
    wlConnectionTimeOut.setLayoutData( fdlConnectionTimeOut );
    wConnectionTimeOut = new TextVar( pipelineMeta, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wConnectionTimeOut.addModifyListener( lsMod );
    wConnectionTimeOut.setToolTipText( BaseMessages.getString( PKG, "HTTPDialog.ConnectionTimeOut.Tooltip" ) );
    props.setLook( wConnectionTimeOut );
    FormData fdConnectionTimeOut = new FormData();
    fdConnectionTimeOut.top = new FormAttachment( wEncoding, margin );
    fdConnectionTimeOut.left = new FormAttachment( middle, 0 );
    fdConnectionTimeOut.right = new FormAttachment( 100, 0 );
    wConnectionTimeOut.setLayoutData( fdConnectionTimeOut );

    Label wlSocketTimeOut = new Label(gSettings, SWT.RIGHT);
    wlSocketTimeOut.setText( BaseMessages.getString( PKG, "HTTPDialog.SocketTimeOut.Label" ) );
    props.setLook(wlSocketTimeOut);
    FormData fdlSocketTimeOut = new FormData();
    fdlSocketTimeOut.top = new FormAttachment( wConnectionTimeOut, margin );
    fdlSocketTimeOut.left = new FormAttachment( 0, 0 );
    fdlSocketTimeOut.right = new FormAttachment( middle, -margin );
    wlSocketTimeOut.setLayoutData( fdlSocketTimeOut );
    wSocketTimeOut = new TextVar( pipelineMeta, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSocketTimeOut.addModifyListener( lsMod );
    wSocketTimeOut.setToolTipText( BaseMessages.getString( PKG, "HTTPDialog.SocketTimeOut.Tooltip" ) );
    props.setLook( wSocketTimeOut );
    FormData fdSocketTimeOut = new FormData();
    fdSocketTimeOut.top = new FormAttachment( wConnectionTimeOut, margin );
    fdSocketTimeOut.left = new FormAttachment( middle, 0 );
    fdSocketTimeOut.right = new FormAttachment( 100, 0 );
    wSocketTimeOut.setLayoutData( fdSocketTimeOut );

    Label wlCloseIdleConnectionsTime = new Label(gSettings, SWT.RIGHT);
    wlCloseIdleConnectionsTime
      .setText( BaseMessages.getString( PKG, "HTTPDialog.CloseIdleConnectionsTime.Label" ) );
    props.setLook(wlCloseIdleConnectionsTime);
    FormData fdlCloseIdleConnectionsTime = new FormData();
    fdlCloseIdleConnectionsTime.top = new FormAttachment( wSocketTimeOut, margin );
    fdlCloseIdleConnectionsTime.left = new FormAttachment( 0, 0 );
    fdlCloseIdleConnectionsTime.right = new FormAttachment( middle, -margin );
    wlCloseIdleConnectionsTime.setLayoutData( fdlCloseIdleConnectionsTime );
    wCloseIdleConnectionsTime = new TextVar( pipelineMeta, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wCloseIdleConnectionsTime.addModifyListener( lsMod );
    wCloseIdleConnectionsTime.setToolTipText( BaseMessages.getString(
      PKG, "HTTPDialog.CloseIdleConnectionsTime.Tooltip" ) );
    props.setLook( wCloseIdleConnectionsTime );
    FormData fdCloseIdleConnectionsTime = new FormData();
    fdCloseIdleConnectionsTime.top = new FormAttachment( wSocketTimeOut, margin );
    fdCloseIdleConnectionsTime.left = new FormAttachment( middle, 0 );
    fdCloseIdleConnectionsTime.right = new FormAttachment( 100, 0 );
    wCloseIdleConnectionsTime.setLayoutData( fdCloseIdleConnectionsTime );

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment( 0, 0 );
    fdSettings.right = new FormAttachment( 100, 0 );
    fdSettings.top = new FormAttachment( lastControl, margin );
    gSettings.setLayoutData( fdSettings );

    // END Output Settings GROUP
    // ////////////////////////
    lastControl = gSettings;
    // ////////////////////////
    // START Output Fields GROUP

    Group gOutputFields = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN );
    gOutputFields.setText( BaseMessages.getString( PKG, "HTTPDialog.OutputFieldsGroup.Label" ) );
    FormLayout OutputFieldsLayout = new FormLayout();
    OutputFieldsLayout.marginWidth = 3;
    OutputFieldsLayout.marginHeight = 3;
    gOutputFields.setLayout( OutputFieldsLayout );
    props.setLook( gOutputFields );

    // Result line...
    //
    Label wlResult = new Label(gOutputFields, SWT.RIGHT);
    wlResult.setText( BaseMessages.getString( PKG, "HTTPDialog.Result.Label" ) );
    props.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment( 0, 0 );
    fdlResult.right = new FormAttachment( middle, -margin );
    fdlResult.top = new FormAttachment( lastControl, margin );
    wlResult.setLayoutData(fdlResult);
    wResult = new TextVar( pipelineMeta, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResult );
    wResult.addModifyListener( lsMod );
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, 0 );
    fdResult.top = new FormAttachment( lastControl, margin );
    fdResult.right = new FormAttachment( 100, 0 );
    wResult.setLayoutData(fdResult);

    // Resultcode line...
    Label wlResultCode = new Label(gOutputFields, SWT.RIGHT);
    wlResultCode.setText( BaseMessages.getString( PKG, "HTTPDialog.ResultCode.Label" ) );
    props.setLook(wlResultCode);
    FormData fdlResultCode = new FormData();
    fdlResultCode.left = new FormAttachment( 0, 0 );
    fdlResultCode.right = new FormAttachment( middle, -margin );
    fdlResultCode.top = new FormAttachment( wResult, margin );
    wlResultCode.setLayoutData(fdlResultCode);
    wResultCode = new TextVar( pipelineMeta, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResultCode );
    wResultCode.addModifyListener( lsMod );
    FormData fdResultCode = new FormData();
    fdResultCode.left = new FormAttachment( middle, 0 );
    fdResultCode.top = new FormAttachment( wResult, margin );
    fdResultCode.right = new FormAttachment( 100, 0 );
    wResultCode.setLayoutData(fdResultCode);

    // Response time line...
    Label wlResponseTime = new Label(gOutputFields, SWT.RIGHT);
    wlResponseTime.setText( BaseMessages.getString( PKG, "HTTPDialog.ResponseTime.Label" ) );
    props.setLook(wlResponseTime);
    FormData fdlResponseTime = new FormData();
    fdlResponseTime.left = new FormAttachment( 0, 0 );
    fdlResponseTime.right = new FormAttachment( middle, -margin );
    fdlResponseTime.top = new FormAttachment( wResultCode, margin );
    wlResponseTime.setLayoutData(fdlResponseTime);
    wResponseTime = new TextVar( pipelineMeta, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResponseTime );
    wResponseTime.addModifyListener( lsMod );
    FormData fdResponseTime = new FormData();
    fdResponseTime.left = new FormAttachment( middle, 0 );
    fdResponseTime.top = new FormAttachment( wResultCode, margin );
    fdResponseTime.right = new FormAttachment( 100, 0 );
    wResponseTime.setLayoutData(fdResponseTime);
    // Response header line...
    Label wlResponseHeader = new Label(gOutputFields, SWT.RIGHT);
    wlResponseHeader.setText( BaseMessages.getString( PKG, "HTTPDialog.ResponseHeader.Label" ) );
    props.setLook(wlResponseHeader);
    FormData fdlResponseHeader = new FormData();
    fdlResponseHeader.left = new FormAttachment( 0, 0 );
    fdlResponseHeader.right = new FormAttachment( middle, -margin );
    fdlResponseHeader.top = new FormAttachment( wResponseTime, margin );
    wlResponseHeader.setLayoutData(fdlResponseHeader);
    wResponseHeader = new TextVar( pipelineMeta, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResponseHeader );
    wResponseHeader.addModifyListener( lsMod );
    FormData fdResponseHeader = new FormData();
    fdResponseHeader.left = new FormAttachment( middle, 0 );
    fdResponseHeader.top = new FormAttachment( wResponseTime, margin );
    fdResponseHeader.right = new FormAttachment( 100, 0 );
    wResponseHeader.setLayoutData(fdResponseHeader);

    FormData fdOutputFields = new FormData();
    fdOutputFields.left = new FormAttachment( 0, 0 );
    fdOutputFields.right = new FormAttachment( 100, 0 );
    fdOutputFields.top = new FormAttachment( lastControl, margin );
    gOutputFields.setLayoutData( fdOutputFields );

    // END Output Fields GROUP
    // ////////////////////////

    lastControl = gOutputFields;

    // ////////////////////////
    // START Http AUTH GROUP

    Group gHttpAuth = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN );
    gHttpAuth.setText( BaseMessages.getString( PKG, "HTTPDialog.HttpAuthGroup.Label" ) );
    FormLayout httpAuthLayout = new FormLayout();
    httpAuthLayout.marginWidth = 3;
    httpAuthLayout.marginHeight = 3;
    gHttpAuth.setLayout( httpAuthLayout );
    props.setLook( gHttpAuth );

    // Http Login
    Label wlHttpLogin = new Label(gHttpAuth, SWT.RIGHT);
    wlHttpLogin.setText( BaseMessages.getString( PKG, "HTTPDialog.HttpLogin.Label" ) );
    props.setLook(wlHttpLogin);
    FormData fdlHttpLogin = new FormData();
    fdlHttpLogin.top = new FormAttachment( 0, margin );
    fdlHttpLogin.left = new FormAttachment( 0, 0 );
    fdlHttpLogin.right = new FormAttachment( middle, -margin );
    wlHttpLogin.setLayoutData( fdlHttpLogin );
    wHttpLogin = new TextVar( pipelineMeta, gHttpAuth, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wHttpLogin.addModifyListener( lsMod );
    wHttpLogin.setToolTipText( BaseMessages.getString( PKG, "HTTPDialog.HttpLogin.Tooltip" ) );
    props.setLook( wHttpLogin );
    FormData fdHttpLogin = new FormData();
    fdHttpLogin.top = new FormAttachment( 0, margin );
    fdHttpLogin.left = new FormAttachment( middle, 0 );
    fdHttpLogin.right = new FormAttachment( 100, 0 );
    wHttpLogin.setLayoutData( fdHttpLogin );

    // Http Password
    Label wlHttpPassword = new Label(gHttpAuth, SWT.RIGHT);
    wlHttpPassword.setText( BaseMessages.getString( PKG, "HTTPDialog.HttpPassword.Label" ) );
    props.setLook(wlHttpPassword);
    FormData fdlHttpPassword = new FormData();
    fdlHttpPassword.top = new FormAttachment( wHttpLogin, margin );
    fdlHttpPassword.left = new FormAttachment( 0, 0 );
    fdlHttpPassword.right = new FormAttachment( middle, -margin );
    wlHttpPassword.setLayoutData( fdlHttpPassword );
    wHttpPassword = new PasswordTextVar( pipelineMeta, gHttpAuth, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wHttpPassword.addModifyListener( lsMod );
    wHttpPassword.setToolTipText( BaseMessages.getString( PKG, "HTTPDialog.HttpPassword.Tooltip" ) );
    props.setLook( wHttpPassword );
    FormData fdHttpPassword = new FormData();
    fdHttpPassword.top = new FormAttachment( wHttpLogin, margin );
    fdHttpPassword.left = new FormAttachment( middle, 0 );
    fdHttpPassword.right = new FormAttachment( 100, 0 );
    wHttpPassword.setLayoutData( fdHttpPassword );

    FormData fdHttpAuth = new FormData();
    fdHttpAuth.left = new FormAttachment( 0, 0 );
    fdHttpAuth.right = new FormAttachment( 100, 0 );
    fdHttpAuth.top = new FormAttachment( lastControl, margin );
    gHttpAuth.setLayoutData( fdHttpAuth );

    // END Http AUTH GROUP
    // ////////////////////////

    lastControl = gHttpAuth;

    // ////////////////////////
    // START PROXY GROUP

    Group gProxy = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN );
    gProxy.setText( BaseMessages.getString( PKG, "HTTPDialog.ProxyGroup.Label" ) );
    FormLayout proxyLayout = new FormLayout();
    proxyLayout.marginWidth = 3;
    proxyLayout.marginHeight = 3;
    gProxy.setLayout( proxyLayout );
    props.setLook( gProxy );

    // Http Login
    Label wlProxyHost = new Label(gProxy, SWT.RIGHT);
    wlProxyHost.setText( BaseMessages.getString( PKG, "HTTPDialog.ProxyHost.Label" ) );
    props.setLook(wlProxyHost);
    FormData fdlProxyHost = new FormData();
    fdlProxyHost.top = new FormAttachment( 0, margin );
    fdlProxyHost.left = new FormAttachment( 0, 0 );
    fdlProxyHost.right = new FormAttachment( middle, -margin );
    wlProxyHost.setLayoutData( fdlProxyHost );
    wProxyHost = new TextVar( pipelineMeta, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wProxyHost.addModifyListener( lsMod );
    wProxyHost.setToolTipText( BaseMessages.getString( PKG, "HTTPDialog.ProxyHost.Tooltip" ) );
    props.setLook( wProxyHost );
    FormData fdProxyHost = new FormData();
    fdProxyHost.top = new FormAttachment( 0, margin );
    fdProxyHost.left = new FormAttachment( middle, 0 );
    fdProxyHost.right = new FormAttachment( 100, 0 );
    wProxyHost.setLayoutData( fdProxyHost );

    // Http Password
    Label wlProxyPort = new Label(gProxy, SWT.RIGHT);
    wlProxyPort.setText( BaseMessages.getString( PKG, "HTTPDialog.ProxyPort.Label" ) );
    props.setLook(wlProxyPort);
    FormData fdlProxyPort = new FormData();
    fdlProxyPort.top = new FormAttachment( wProxyHost, margin );
    fdlProxyPort.left = new FormAttachment( 0, 0 );
    fdlProxyPort.right = new FormAttachment( middle, -margin );
    wlProxyPort.setLayoutData( fdlProxyPort );
    wProxyPort = new TextVar( pipelineMeta, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wProxyPort.addModifyListener( lsMod );
    wProxyPort.setToolTipText( BaseMessages.getString( PKG, "HTTPDialog.ProxyPort.Tooltip" ) );
    props.setLook( wProxyPort );
    FormData fdProxyPort = new FormData();
    fdProxyPort.top = new FormAttachment( wProxyHost, margin );
    fdProxyPort.left = new FormAttachment( middle, 0 );
    fdProxyPort.right = new FormAttachment( 100, 0 );
    wProxyPort.setLayoutData( fdProxyPort );

    FormData fdProxy = new FormData();
    fdProxy.left = new FormAttachment( 0, 0 );
    fdProxy.right = new FormAttachment( 100, 0 );
    fdProxy.top = new FormAttachment( lastControl, margin );
    gProxy.setLayoutData( fdProxy );

    // END Http Proxy GROUP
    // ////////////////////////
    lastControl = gProxy;

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( wTransformName, margin );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////
    // Additional tab...
    //
    CTabItem wAdditionalTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdditionalTab.setText( BaseMessages.getString( PKG, "HTTPDialog.FieldsTab.Title" ) );

    FormLayout addLayout = new FormLayout();
    addLayout.marginWidth = Const.FORM_MARGIN;
    addLayout.marginHeight = Const.FORM_MARGIN;

    Composite wAdditionalComp = new Composite(wTabFolder, SWT.NONE);
    wAdditionalComp.setLayout( addLayout );
    props.setLook(wAdditionalComp);

    Label wlFields = new Label(wAdditionalComp, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "HTTPDialog.Parameters.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( lastControl, margin );
    wlFields.setLayoutData(fdlFields);
    lastControl = wlFields;

    Button wGet = new Button(wAdditionalComp, SWT.PUSH);
    wGet.setText( BaseMessages.getString( PKG, "HTTPDialog.GetFields.Button" ) );
    FormData fdGet = new FormData();
    fdGet.top = new FormAttachment(wlFields, margin );
    fdGet.right = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    final int FieldsRows = input.getArgumentField().length;

    colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "HTTPDialog.ColumnInfo.Name" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          new String[] { "" }, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "HTTPDialog.ColumnInfo.Parameter" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ), };

    wFields =
      new TableView(
        pipelineMeta, wAdditionalComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod,
        props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment(wGet, -margin );
    fdFields.bottom = new FormAttachment(wlFields, 200 );
    wFields.setLayoutData(fdFields);

    Label wlHeaders = new Label(wAdditionalComp, SWT.NONE);
    wlHeaders.setText( BaseMessages.getString( PKG, "HTTPDialog.Headers.Label" ) );
    props.setLook(wlHeaders);
    FormData fdlHeaders = new FormData();
    fdlHeaders.left = new FormAttachment( 0, 0 );
    fdlHeaders.top = new FormAttachment( wFields, margin );
    wlHeaders.setLayoutData(fdlHeaders);

    final int HeadersRows = input.getHeaderParameter().length;

    colinfHeaders =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "HTTPDialog.ColumnInfo.Field" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          new String[] { "" }, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "HTTPDialog.ColumnInfo.Header" ), ColumnInfo.COLUMN_TYPE_TEXT, false ), };
    colinfHeaders[ 1 ].setUsingVariables( true );
    wHeaders =
      new TableView(
        pipelineMeta, wAdditionalComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinfHeaders, HeadersRows,
        lsMod, props );

    Button wGetHeaders = new Button(wAdditionalComp, SWT.PUSH);
    wGetHeaders.setText( BaseMessages.getString( PKG, "HTTPDialog.GetHeaders.Button" ) );
    FormData fdGetHeaders = new FormData();
    fdGetHeaders.top = new FormAttachment(wlHeaders, margin );
    fdGetHeaders.right = new FormAttachment( 100, 0 );
    wGetHeaders.setLayoutData( fdGetHeaders );

    FormData fdHeaders = new FormData();
    fdHeaders.left = new FormAttachment( 0, 0 );
    fdHeaders.top = new FormAttachment(wlHeaders, margin );
    fdHeaders.right = new FormAttachment(wGetHeaders, -margin );
    fdHeaders.bottom = new FormAttachment( 100, -margin );
    wHeaders.setLayoutData(fdHeaders);

    //
    // Search the fields in the background

    final Runnable runnable = () -> {
      TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
      if ( transformMeta != null ) {
        try {
          IRowMeta row = pipelineMeta.getPrevTransformFields( transformMeta );

          // Remember these fields...
          for ( int i = 0; i < row.size(); i++ ) {
            inputFields.put( row.getValueMeta( i ).getName(), i);
          }
          setComboBoxes();
        } catch ( HopException e ) {
          logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
        }
      }
    };
    new Thread( runnable ).start();

    FormData fdAdditionalComp = new FormData();
    fdAdditionalComp.left = new FormAttachment( 0, 0 );
    fdAdditionalComp.top = new FormAttachment( wTransformName, margin );
    fdAdditionalComp.right = new FormAttachment( 100, 0 );
    fdAdditionalComp.bottom = new FormAttachment( 100, 0 );
    wAdditionalComp.setLayoutData(fdAdditionalComp);

    wAdditionalComp.layout();
    wAdditionalTab.setControl(wAdditionalComp);
    // ////// END of Additional Tab

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners
    wGet.addListener( SWT.Selection, e -> get());
    wGetHeaders.addListener( SWT.Selection, e -> getHeadersFields());

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wUrl.addSelectionListener( lsDef );
    wResult.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    lsResize = event -> {
      Point size = shell.getSize();
      wFields.setSize( size.x - 10, size.y - 50 );
      wFields.table.setSize( size.x - 10, size.y - 50 );
      wFields.redraw();
    };
    shell.addListener( SWT.Resize, lsResize );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    wTabFolder.setSelection( 0 );
    activeUrlInfield();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void setEncodings() {
    // Encoding of the text file:
    if ( !gotEncodings ) {
      gotEncodings = true;

      wEncoding.removeAll();
      List<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
      for (Charset charSet : values) {
        wEncoding.add(charSet.displayName());
      }

      // Now select the default!
      String defEncoding = Const.getEnvironmentVariable( "file.encoding", "UTF-8" );
      int idx = Const.indexOfString( defEncoding, wEncoding.getItems() );
      if ( idx >= 0 ) {
        wEncoding.select( idx );
      }
    }
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[ entries.size() ] );

    Const.sortStrings( fieldNames );
    colinf[ 0 ].setComboValues( fieldNames );
    colinfHeaders[ 0 ].setComboValues( fieldNames );
  }

  private void activeUrlInfield() {
    wlUrlField.setEnabled( wUrlInField.getSelection() );
    wUrlField.setEnabled( wUrlInField.getSelection() );
    wlUrl.setEnabled( !wUrlInField.getSelection() );
    wUrl.setEnabled( !wUrlInField.getSelection() );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "HTTPDialog.Log.GettingKeyInfo" ) );
    }

    if ( input.getArgumentField() != null ) {
      for ( int i = 0; i < input.getArgumentField().length; i++ ) {
        TableItem item = wFields.table.getItem( i );
        item.setText( 1, Const.NVL( input.getArgumentField()[ i ], "" ) );
        item.setText( 2, Const.NVL( input.getArgumentParameter()[ i ], "" ) );
      }
    }

    if ( input.getHeaderField() != null ) {
      for ( int i = 0; i < input.getHeaderField().length; i++ ) {
        TableItem item = wHeaders.table.getItem( i );
        if ( input.getHeaderField()[ i ] != null ) {
          item.setText( 1, input.getHeaderField()[ i ] );
        }
        if ( input.getHeaderParameter()[ i ] != null ) {
          item.setText( 2, input.getHeaderParameter()[ i ] );
        }
      }
    }
    wSocketTimeOut.setText( Const.NVL( input.getSocketTimeout(), "" ) );
    wConnectionTimeOut.setText( Const.NVL( input.getConnectionTimeout(), "" ) );
    wCloseIdleConnectionsTime.setText( Const.NVL( input.getCloseIdleConnectionsTime(), "" ) );

    wUrl.setText( Const.NVL( input.getUrl(), "" ) );
    wUrlInField.setSelection( input.isUrlInField() );
    wUrlField.setText( Const.NVL( input.getUrlField(), "" ) );
    wEncoding.setText( Const.NVL( input.getEncoding(), "" ) );

    wResult.setText( Const.NVL( input.getFieldName(), "" ) );
    if ( input.getHttpLogin() != null ) {
      wHttpLogin.setText( input.getHttpLogin() );
    }
    if ( input.getHttpPassword() != null ) {
      wHttpPassword.setText( input.getHttpPassword() );
    }
    if ( input.getProxyHost() != null ) {
      wProxyHost.setText( input.getProxyHost() );
    }
    if ( input.getProxyPort() != null ) {
      wProxyPort.setText( input.getProxyPort() );
    }
    if ( input.getResultCodeFieldName() != null ) {
      wResultCode.setText( input.getResultCodeFieldName() );
    }
    if ( input.getResponseTimeFieldName() != null ) {
      wResponseTime.setText( input.getResponseTimeFieldName() );
    }
    if ( input.getResponseHeaderFieldName() != null ) {
      wResponseHeader.setText( input.getResponseHeaderFieldName() );
    }

    wFields.setRowNums();
    wFields.optWidth( true );
    wHeaders.setRowNums();
    wHeaders.optWidth( true );

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

    int nrargs = wFields.nrNonEmpty();
    int nrheaders = wHeaders.nrNonEmpty();

    input.allocate( nrargs, nrheaders );

    if ( isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "HTTPDialog.Log.FoundArguments", String.valueOf( nrargs ) ) );
    }
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrargs; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      input.getArgumentField()[ i ] = item.getText( 1 );
      input.getArgumentParameter()[ i ] = item.getText( 2 );
    }

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "HTTPDialog.Log.FoundHeaders", String.valueOf( nrheaders ) ) );
    }
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrheaders; i++ ) {
      TableItem item = wHeaders.getNonEmpty( i );
      input.getHeaderField()[ i ] = item.getText( 1 );
      input.getHeaderParameter()[ i ] = item.getText( 2 );
    }

    input.setUrl( wUrl.getText() );
    input.setUrlField( wUrlField.getText() );
    input.setUrlInField( wUrlInField.getSelection() );
    input.setFieldName( wResult.getText() );
    input.setEncoding( wEncoding.getText() );
    input.setHttpLogin( wHttpLogin.getText() );
    input.setHttpPassword( wHttpPassword.getText() );
    input.setProxyHost( wProxyHost.getText() );
    input.setProxyPort( wProxyPort.getText() );
    input.setResultCodeFieldName( wResultCode.getText() );
    input.setResponseTimeFieldName( wResponseTime.getText() );
    input.setResponseHeaderFieldName( wResponseHeader.getText() );
    input.setSocketTimeout( wSocketTimeOut.getText() );
    input.setConnectionTimeout( wConnectionTimeOut.getText() );
    input.setCloseIdleConnectionsTime( wCloseIdleConnectionsTime.getText() );

    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
      if ( r != null && !r.isEmpty() ) {
        BaseTransformDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1, 2 }, new int[] { 3 }, -1, -1, null );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "HTTPDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "HTTPDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  private void getHeadersFields() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
      if ( r != null && !r.isEmpty() ) {
        BaseTransformDialog.getFieldsFromPrevious( r, wHeaders, 1, new int[] { 1, 2 }, new int[] { 3 }, -1, -1, null );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "HTTPDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "HTTPDialog.FailedToGetFields.DialogMessage" ), ke );
    }

  }
}
