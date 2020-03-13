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

package org.apache.hop.ui.trans.steps.httppost;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.steps.httppost.HTTPPOSTMeta;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.apache.hop.ui.trans.step.ComponentSelectionListener;
import org.apache.hop.ui.trans.step.TableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Point;
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
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HTTPPOSTDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = HTTPPOSTMeta.class; // for i18n purposes, needed by Translator2!!

  private static final String[] YES_NO_COMBO = new String[] {
    BaseMessages.getString( PKG, "System.Combo.No" ), BaseMessages.getString( PKG, "System.Combo.Yes" ) };
  private static final String YES = BaseMessages.getString( PKG, "System.Combo.Yes" );
  private static final String NO = BaseMessages.getString( PKG, "System.Combo.No" );

  private Label wlUrl;
  private TextVar wUrl;
  private FormData fdlUrl, fdUrl;

  private Label wlResult;
  private TextVar wResult;
  private FormData fdlResult, fdResult;

  private Label wlResultCode;
  private TextVar wResultCode;
  private FormData fdlResultCode, fdResultCode;

  private Label wlResponseTime;
  private TextVar wResponseTime;
  private FormData fdlResponseTime, fdResponseTime;
  private Label wlResponseHeader;
  private TextVar wResponseHeader;
  private FormData fdlResponseHeader, fdResponseHeader;

  private Label wlFields;
  private TableView wFields;
  private FormData fdlFields, fdFields;

  private Label wlQuery;
  private TableView wQuery;
  private FormData fdlQuery, fdQuery;

  private Label wlUrlInField;
  private Button wUrlInField;
  private FormData fdlUrlInField, fdUrlInField;

  private Label wlUrlField;
  private ComboVar wUrlField;
  private FormData fdlUrlField, fdUrlField;

  private Label wlrequestEntity;
  private ComboVar wrequestEntity;
  private FormData fdlrequestEntity, fdrequestEntity;

  private Label wlHttpLogin;
  private TextVar wHttpLogin;

  private Label wlHttpPassword;
  private TextVar wHttpPassword;

  private Label wlProxyHost;
  private TextVar wProxyHost;

  private Label wlProxyPort;
  private TextVar wProxyPort;

  private HTTPPOSTMeta input;

  private Map<String, Integer> inputFields;

  private ColumnInfo[] colinf;
  private ColumnInfo[] colinfquery;

  private String[] fieldNames;

  private boolean gotPreviousFields = false;

  private Button wGetBodyParam;
  private FormData fdGetBodyParam;
  private Listener lsGetBodyParam;

  private Label wlEncoding;
  private ComboVar wEncoding;
  private FormData fdlEncoding, fdEncoding;

  private Label wlPostAFile;
  private Button wPostAFile;

  private CTabFolder wTabFolder;

  private CTabItem wGeneralTab, wAdditionalTab;
  private FormData fdTabFolder;

  private Composite wGeneralComp, wAdditionalComp;
  private FormData fdGeneralComp, fdAdditionalComp;

  private boolean gotEncodings = false;

  private Label wlConnectionTimeOut;
  private TextVar wConnectionTimeOut;

  private Label wlSocketTimeOut;
  private TextVar wSocketTimeOut;

  private Label wlCloseIdleConnectionsTime;
  private TextVar wCloseIdleConnectionsTime;

  public HTTPPOSTDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (HTTPPOSTMeta) in;
    inputFields = new HashMap<String, Integer>();
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
    shell.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
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
    props.setLook( wTabFolder, PropsUI.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////
    wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.GeneralTab.Title" ) );

    wGeneralComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wGeneralComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wGeneralComp.setLayout( fileLayout );

    // ////////////////////////
    // START Settings GROUP

    Group gSettings = new Group( wGeneralComp, SWT.SHADOW_ETCHED_IN );
    gSettings.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.SettingsGroup.Label" ) );
    FormLayout SettingsLayout = new FormLayout();
    SettingsLayout.marginWidth = 3;
    SettingsLayout.marginHeight = 3;
    gSettings.setLayout( SettingsLayout );
    props.setLook( gSettings );

    wlUrl = new Label( gSettings, SWT.RIGHT );
    wlUrl.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.URL.Label" ) );
    props.setLook( wlUrl );
    fdlUrl = new FormData();
    fdlUrl.left = new FormAttachment( 0, 0 );
    fdlUrl.right = new FormAttachment( middle, -margin );
    fdlUrl.top = new FormAttachment( wStepname, margin );
    wlUrl.setLayoutData( fdlUrl );

    wUrl = new TextVar( transMeta, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUrl );
    wUrl.addModifyListener( lsMod );
    fdUrl = new FormData();
    fdUrl.left = new FormAttachment( middle, 0 );
    fdUrl.top = new FormAttachment( wStepname, margin );
    fdUrl.right = new FormAttachment( 100, 0 );
    wUrl.setLayoutData( fdUrl );

    // UrlInField line
    wlUrlInField = new Label( gSettings, SWT.RIGHT );
    wlUrlInField.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.UrlInField.Label" ) );
    props.setLook( wlUrlInField );
    fdlUrlInField = new FormData();
    fdlUrlInField.left = new FormAttachment( 0, 0 );
    fdlUrlInField.top = new FormAttachment( wUrl, margin );
    fdlUrlInField.right = new FormAttachment( middle, -margin );
    wlUrlInField.setLayoutData( fdlUrlInField );
    wUrlInField = new Button( gSettings, SWT.CHECK );
    props.setLook( wUrlInField );
    fdUrlInField = new FormData();
    fdUrlInField.left = new FormAttachment( middle, 0 );
    fdUrlInField.top = new FormAttachment( wUrl, margin );
    fdUrlInField.right = new FormAttachment( 100, 0 );
    wUrlInField.setLayoutData( fdUrlInField );
    wUrlInField.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        activeUrlInfield();
      }
    } );

    // UrlField Line
    wlUrlField = new Label( gSettings, SWT.RIGHT );
    wlUrlField.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.UrlField.Label" ) );
    props.setLook( wlUrlField );
    fdlUrlField = new FormData();
    fdlUrlField.left = new FormAttachment( 0, 0 );
    fdlUrlField.right = new FormAttachment( middle, -margin );
    fdlUrlField.top = new FormAttachment( wUrlInField, margin );
    wlUrlField.setLayoutData( fdlUrlField );

    wUrlField = new ComboVar( transMeta, gSettings, SWT.BORDER | SWT.READ_ONLY );
    wUrlField.setEditable( true );
    props.setLook( wUrlField );
    wUrlField.addModifyListener( lsMod );
    fdUrlField = new FormData();
    fdUrlField.left = new FormAttachment( middle, 0 );
    fdUrlField.top = new FormAttachment( wUrlInField, margin );
    fdUrlField.right = new FormAttachment( 100, -margin );
    wUrlField.setLayoutData( fdUrlField );
    wUrlField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setStreamFields();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    wlEncoding = new Label( gSettings, SWT.RIGHT );
    wlEncoding.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.Encoding.Label" ) );
    props.setLook( wlEncoding );
    fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment( 0, 0 );
    fdlEncoding.top = new FormAttachment( wUrlField, margin );
    fdlEncoding.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData( fdlEncoding );
    wEncoding = new ComboVar( transMeta, gSettings, SWT.BORDER | SWT.READ_ONLY );
    wEncoding.setEditable( true );
    props.setLook( wEncoding );
    wEncoding.addModifyListener( lsMod );
    fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment( middle, 0 );
    fdEncoding.top = new FormAttachment( wUrlField, margin );
    fdEncoding.right = new FormAttachment( 100, -margin );
    wEncoding.setLayoutData( fdEncoding );
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

    // requestEntity Line
    wlrequestEntity = new Label( gSettings, SWT.RIGHT );
    wlrequestEntity.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.requestEntity.Label" ) );
    props.setLook( wlrequestEntity );
    fdlrequestEntity = new FormData();
    fdlrequestEntity.left = new FormAttachment( 0, 0 );
    fdlrequestEntity.right = new FormAttachment( middle, -margin );
    fdlrequestEntity.top = new FormAttachment( wEncoding, margin );
    wlrequestEntity.setLayoutData( fdlrequestEntity );

    wrequestEntity = new ComboVar( transMeta, gSettings, SWT.BORDER | SWT.READ_ONLY );
    wrequestEntity.setEditable( true );
    props.setLook( wrequestEntity );
    wrequestEntity.addModifyListener( lsMod );
    fdrequestEntity = new FormData();
    fdrequestEntity.left = new FormAttachment( middle, 0 );
    fdrequestEntity.top = new FormAttachment( wEncoding, margin );
    fdrequestEntity.right = new FormAttachment( 100, -margin );
    wrequestEntity.setLayoutData( fdrequestEntity );
    wrequestEntity.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setStreamFields();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // Post file?
    wlPostAFile = new Label( gSettings, SWT.RIGHT );
    wlPostAFile.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.postAFile.Label" ) );
    props.setLook( wlPostAFile );
    FormData fdlPostAFile = new FormData();
    fdlPostAFile.left = new FormAttachment( 0, 0 );
    fdlPostAFile.right = new FormAttachment( middle, -margin );
    fdlPostAFile.top = new FormAttachment( wrequestEntity, margin );
    wlPostAFile.setLayoutData( fdlPostAFile );
    wPostAFile = new Button( gSettings, SWT.CHECK );
    wPostAFile.setToolTipText( BaseMessages.getString( PKG, "HTTPPOSTDialog.postAFile.Tooltip" ) );
    props.setLook( wPostAFile );
    FormData fdPostAFile = new FormData();
    fdPostAFile.left = new FormAttachment( middle, 0 );
    fdPostAFile.top = new FormAttachment( wrequestEntity, margin );
    fdPostAFile.right = new FormAttachment( 100, 0 );
    wPostAFile.setLayoutData( fdPostAFile );
    wPostAFile.addSelectionListener( new ComponentSelectionListener( input ) );

    wlConnectionTimeOut = new Label( gSettings, SWT.RIGHT );
    wlConnectionTimeOut.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.ConnectionTimeOut.Label" ) );
    props.setLook( wlConnectionTimeOut );
    FormData fdlConnectionTimeOut = new FormData();
    fdlConnectionTimeOut.top = new FormAttachment( wPostAFile, margin );
    fdlConnectionTimeOut.left = new FormAttachment( 0, 0 );
    fdlConnectionTimeOut.right = new FormAttachment( middle, -margin );
    wlConnectionTimeOut.setLayoutData( fdlConnectionTimeOut );
    wConnectionTimeOut = new TextVar( transMeta, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wConnectionTimeOut.addModifyListener( lsMod );
    wConnectionTimeOut.setToolTipText( BaseMessages.getString( PKG, "HTTPPOSTDialog.ConnectionTimeOut.Tooltip" ) );
    props.setLook( wConnectionTimeOut );
    FormData fdConnectionTimeOut = new FormData();
    fdConnectionTimeOut.top = new FormAttachment( wPostAFile, margin );
    fdConnectionTimeOut.left = new FormAttachment( middle, 0 );
    fdConnectionTimeOut.right = new FormAttachment( 100, 0 );
    wConnectionTimeOut.setLayoutData( fdConnectionTimeOut );

    wlSocketTimeOut = new Label( gSettings, SWT.RIGHT );
    wlSocketTimeOut.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.SocketTimeOut.Label" ) );
    props.setLook( wlSocketTimeOut );
    FormData fdlSocketTimeOut = new FormData();
    fdlSocketTimeOut.top = new FormAttachment( wConnectionTimeOut, margin );
    fdlSocketTimeOut.left = new FormAttachment( 0, 0 );
    fdlSocketTimeOut.right = new FormAttachment( middle, -margin );
    wlSocketTimeOut.setLayoutData( fdlSocketTimeOut );
    wSocketTimeOut = new TextVar( transMeta, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSocketTimeOut.addModifyListener( lsMod );
    wSocketTimeOut.setToolTipText( BaseMessages.getString( PKG, "HTTPPOSTDialog.SocketTimeOut.Tooltip" ) );
    props.setLook( wSocketTimeOut );
    FormData fdSocketTimeOut = new FormData();
    fdSocketTimeOut.top = new FormAttachment( wConnectionTimeOut, margin );
    fdSocketTimeOut.left = new FormAttachment( middle, 0 );
    fdSocketTimeOut.right = new FormAttachment( 100, 0 );
    wSocketTimeOut.setLayoutData( fdSocketTimeOut );

    wlCloseIdleConnectionsTime = new Label( gSettings, SWT.RIGHT );
    wlCloseIdleConnectionsTime.setText( BaseMessages.getString(
      PKG, "HTTPPOSTDialog.CloseIdleConnectionsTime.Label" ) );
    props.setLook( wlCloseIdleConnectionsTime );
    FormData fdlCloseIdleConnectionsTime = new FormData();
    fdlCloseIdleConnectionsTime.top = new FormAttachment( wSocketTimeOut, margin );
    fdlCloseIdleConnectionsTime.left = new FormAttachment( 0, 0 );
    fdlCloseIdleConnectionsTime.right = new FormAttachment( middle, -margin );
    wlCloseIdleConnectionsTime.setLayoutData( fdlCloseIdleConnectionsTime );
    wCloseIdleConnectionsTime = new TextVar( transMeta, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wCloseIdleConnectionsTime.addModifyListener( lsMod );
    wCloseIdleConnectionsTime.setToolTipText( BaseMessages.getString(
      PKG, "HTTPPOSTDialog.CloseIdleConnectionsTime.Tooltip" ) );
    props.setLook( wCloseIdleConnectionsTime );
    FormData fdCloseIdleConnectionsTime = new FormData();
    fdCloseIdleConnectionsTime.top = new FormAttachment( wSocketTimeOut, margin );
    fdCloseIdleConnectionsTime.left = new FormAttachment( middle, 0 );
    fdCloseIdleConnectionsTime.right = new FormAttachment( 100, 0 );
    wCloseIdleConnectionsTime.setLayoutData( fdCloseIdleConnectionsTime );

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment( 0, 0 );
    fdSettings.right = new FormAttachment( 100, 0 );
    fdSettings.top = new FormAttachment( wStepname, margin );
    gSettings.setLayoutData( fdSettings );

    // END Output Settings GROUP
    // ////////////////////////

    // ////////////////////////
    // START Output Fields GROUP

    Group gOutputFields = new Group( wGeneralComp, SWT.SHADOW_ETCHED_IN );
    gOutputFields.setText( BaseMessages.getString( PKG, "HTTPDialog.OutputFieldsGroup.Label" ) );
    FormLayout OutputFieldsLayout = new FormLayout();
    OutputFieldsLayout.marginWidth = 3;
    OutputFieldsLayout.marginHeight = 3;
    gOutputFields.setLayout( OutputFieldsLayout );
    props.setLook( gOutputFields );

    // Result line...
    wlResult = new Label( gOutputFields, SWT.RIGHT );
    wlResult.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.Result.Label" ) );
    props.setLook( wlResult );
    fdlResult = new FormData();
    fdlResult.left = new FormAttachment( 0, 0 );
    fdlResult.right = new FormAttachment( middle, -margin );
    fdlResult.top = new FormAttachment( wPostAFile, margin );
    wlResult.setLayoutData( fdlResult );
    wResult = new TextVar( transMeta, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResult );
    wResult.addModifyListener( lsMod );
    fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, 0 );
    fdResult.top = new FormAttachment( wPostAFile, margin );
    fdResult.right = new FormAttachment( 100, -margin );
    wResult.setLayoutData( fdResult );

    // Resultcode line...
    wlResultCode = new Label( gOutputFields, SWT.RIGHT );
    wlResultCode.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.ResultCode.Label" ) );
    props.setLook( wlResultCode );
    fdlResultCode = new FormData();
    fdlResultCode.left = new FormAttachment( 0, 0 );
    fdlResultCode.right = new FormAttachment( middle, -margin );
    fdlResultCode.top = new FormAttachment( wResult, margin );
    wlResultCode.setLayoutData( fdlResultCode );
    wResultCode = new TextVar( transMeta, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResultCode );
    wResultCode.addModifyListener( lsMod );
    fdResultCode = new FormData();
    fdResultCode.left = new FormAttachment( middle, 0 );
    fdResultCode.top = new FormAttachment( wResult, margin );
    fdResultCode.right = new FormAttachment( 100, -margin );
    wResultCode.setLayoutData( fdResultCode );

    // Response time line...
    wlResponseTime = new Label( gOutputFields, SWT.RIGHT );
    wlResponseTime.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.ResponseTime.Label" ) );
    props.setLook( wlResponseTime );
    fdlResponseTime = new FormData();
    fdlResponseTime.left = new FormAttachment( 0, 0 );
    fdlResponseTime.right = new FormAttachment( middle, -margin );
    fdlResponseTime.top = new FormAttachment( wResultCode, margin );
    wlResponseTime.setLayoutData( fdlResponseTime );
    wResponseTime = new TextVar( transMeta, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResponseTime );
    wResponseTime.addModifyListener( lsMod );
    fdResponseTime = new FormData();
    fdResponseTime.left = new FormAttachment( middle, 0 );
    fdResponseTime.top = new FormAttachment( wResultCode, margin );
    fdResponseTime.right = new FormAttachment( 100, 0 );
    wResponseTime.setLayoutData( fdResponseTime );
    // Response header line...
    wlResponseHeader = new Label( gOutputFields, SWT.RIGHT );
    wlResponseHeader.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.ResponseHeader.Label" ) );
    props.setLook( wlResponseHeader );
    fdlResponseHeader = new FormData();
    fdlResponseHeader.left = new FormAttachment( 0, 0 );
    fdlResponseHeader.right = new FormAttachment( middle, -margin );
    fdlResponseHeader.top = new FormAttachment( wResponseTime, margin );
    wlResponseHeader.setLayoutData( fdlResponseHeader );
    wResponseHeader = new TextVar( transMeta, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wResponseHeader );
    wResponseHeader.addModifyListener( lsMod );
    fdResponseHeader = new FormData();
    fdResponseHeader.left = new FormAttachment( middle, 0 );
    fdResponseHeader.top = new FormAttachment( wResponseTime, margin );
    fdResponseHeader.right = new FormAttachment( 100, 0 );
    wResponseHeader.setLayoutData( fdResponseHeader );

    FormData fdOutputFields = new FormData();
    fdOutputFields.left = new FormAttachment( 0, 0 );
    fdOutputFields.right = new FormAttachment( 100, 0 );
    fdOutputFields.top = new FormAttachment( gSettings, margin );
    gOutputFields.setLayoutData( fdOutputFields );

    // END Output Fields GROUP
    // ////////////////////////

    // ////////////////////////
    // START HTTP AUTH GROUP

    Group gHttpAuth = new Group( wGeneralComp, SWT.SHADOW_ETCHED_IN );
    gHttpAuth.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.HttpAuthGroup.Label" ) );
    FormLayout httpAuthLayout = new FormLayout();
    httpAuthLayout.marginWidth = 3;
    httpAuthLayout.marginHeight = 3;
    gHttpAuth.setLayout( httpAuthLayout );
    props.setLook( gHttpAuth );

    // HTTP Login
    wlHttpLogin = new Label( gHttpAuth, SWT.RIGHT );
    wlHttpLogin.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.HttpLogin.Label" ) );
    props.setLook( wlHttpLogin );
    FormData fdlHttpLogin = new FormData();
    fdlHttpLogin.top = new FormAttachment( 0, margin );
    fdlHttpLogin.left = new FormAttachment( 0, 0 );
    fdlHttpLogin.right = new FormAttachment( middle, -margin );
    wlHttpLogin.setLayoutData( fdlHttpLogin );
    wHttpLogin = new TextVar( transMeta, gHttpAuth, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wHttpLogin.addModifyListener( lsMod );
    wHttpLogin.setToolTipText( BaseMessages.getString( PKG, "HTTPPOSTDialog.HttpLogin.Tooltip" ) );
    props.setLook( wHttpLogin );
    FormData fdHttpLogin = new FormData();
    fdHttpLogin.top = new FormAttachment( 0, margin );
    fdHttpLogin.left = new FormAttachment( middle, 0 );
    fdHttpLogin.right = new FormAttachment( 100, 0 );
    wHttpLogin.setLayoutData( fdHttpLogin );

    // HTTP Password
    wlHttpPassword = new Label( gHttpAuth, SWT.RIGHT );
    wlHttpPassword.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.HttpPassword.Label" ) );
    props.setLook( wlHttpPassword );
    FormData fdlHttpPassword = new FormData();
    fdlHttpPassword.top = new FormAttachment( wHttpLogin, margin );
    fdlHttpPassword.left = new FormAttachment( 0, 0 );
    fdlHttpPassword.right = new FormAttachment( middle, -margin );
    wlHttpPassword.setLayoutData( fdlHttpPassword );
    wHttpPassword = new PasswordTextVar( transMeta, gHttpAuth, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wHttpPassword.addModifyListener( lsMod );
    wHttpPassword.setToolTipText( BaseMessages.getString( PKG, "HTTPPOSTDialog.HttpPassword.Tooltip" ) );
    props.setLook( wHttpPassword );
    FormData fdHttpPassword = new FormData();
    fdHttpPassword.top = new FormAttachment( wHttpLogin, margin );
    fdHttpPassword.left = new FormAttachment( middle, 0 );
    fdHttpPassword.right = new FormAttachment( 100, 0 );
    wHttpPassword.setLayoutData( fdHttpPassword );

    FormData fdHttpAuth = new FormData();
    fdHttpAuth.left = new FormAttachment( 0, 0 );
    fdHttpAuth.right = new FormAttachment( 100, 0 );
    fdHttpAuth.top = new FormAttachment( gOutputFields, margin );
    gHttpAuth.setLayoutData( fdHttpAuth );

    // END HTTP AUTH GROUP
    // ////////////////////////

    // ////////////////////////
    // START PROXY GROUP

    Group gProxy = new Group( wGeneralComp, SWT.SHADOW_ETCHED_IN );
    gProxy.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.ProxyGroup.Label" ) );
    FormLayout proxyLayout = new FormLayout();
    proxyLayout.marginWidth = 3;
    proxyLayout.marginHeight = 3;
    gProxy.setLayout( proxyLayout );
    props.setLook( gProxy );

    // HTTP Login
    wlProxyHost = new Label( gProxy, SWT.RIGHT );
    wlProxyHost.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.ProxyHost.Label" ) );
    props.setLook( wlProxyHost );
    FormData fdlProxyHost = new FormData();
    fdlProxyHost.top = new FormAttachment( 0, margin );
    fdlProxyHost.left = new FormAttachment( 0, 0 );
    fdlProxyHost.right = new FormAttachment( middle, -margin );
    wlProxyHost.setLayoutData( fdlProxyHost );
    wProxyHost = new TextVar( transMeta, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wProxyHost.addModifyListener( lsMod );
    wProxyHost.setToolTipText( BaseMessages.getString( PKG, "HTTPPOSTDialog.ProxyHost.Tooltip" ) );
    props.setLook( wProxyHost );
    FormData fdProxyHost = new FormData();
    fdProxyHost.top = new FormAttachment( 0, margin );
    fdProxyHost.left = new FormAttachment( middle, 0 );
    fdProxyHost.right = new FormAttachment( 100, 0 );
    wProxyHost.setLayoutData( fdProxyHost );

    // HTTP Password
    wlProxyPort = new Label( gProxy, SWT.RIGHT );
    wlProxyPort.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.ProxyPort.Label" ) );
    props.setLook( wlProxyPort );
    FormData fdlProxyPort = new FormData();
    fdlProxyPort.top = new FormAttachment( wProxyHost, margin );
    fdlProxyPort.left = new FormAttachment( 0, 0 );
    fdlProxyPort.right = new FormAttachment( middle, -margin );
    wlProxyPort.setLayoutData( fdlProxyPort );
    wProxyPort = new TextVar( transMeta, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wProxyPort.addModifyListener( lsMod );
    wProxyPort.setToolTipText( BaseMessages.getString( PKG, "HTTPPOSTDialog.ProxyPort.Tooltip" ) );
    props.setLook( wProxyPort );
    FormData fdProxyPort = new FormData();
    fdProxyPort.top = new FormAttachment( wProxyHost, margin );
    fdProxyPort.left = new FormAttachment( middle, 0 );
    fdProxyPort.right = new FormAttachment( 100, 0 );
    wProxyPort.setLayoutData( fdProxyPort );

    FormData fdProxy = new FormData();
    fdProxy.left = new FormAttachment( 0, 0 );
    fdProxy.right = new FormAttachment( 100, 0 );
    fdProxy.top = new FormAttachment( gHttpAuth, margin );
    gProxy.setLayoutData( fdProxy );

    // END HTTP AUTH GROUP
    // ////////////////////////

    fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( wStepname, margin );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.layout();
    wGeneralTab.setControl( wGeneralComp );

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // Additional tab...
    //
    wAdditionalTab = new CTabItem( wTabFolder, SWT.NONE );
    wAdditionalTab.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.FieldsTab.Title" ) );

    FormLayout addLayout = new FormLayout();
    addLayout.marginWidth = Const.FORM_MARGIN;
    addLayout.marginHeight = Const.FORM_MARGIN;

    wAdditionalComp = new Composite( wTabFolder, SWT.NONE );
    wAdditionalComp.setLayout( addLayout );
    props.setLook( wAdditionalComp );

    wlFields = new Label( wAdditionalComp, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.Parameters.Label" ) );
    props.setLook( wlFields );
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( gProxy, margin );
    wlFields.setLayoutData( fdlFields );

    final int FieldsRows = input.getArgumentField().length;

    colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "HTTPPOSTDialog.ColumnInfo.Name" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          new String[] { "" }, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "HTTPPOSTDialog.ColumnInfo.Parameter" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "HTTPPOSTDialog.ColumnInfo.Header" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          YES_NO_COMBO ), };
    colinf[ 1 ].setUsingVariables( true );
    wFields =
      new TableView(
        transMeta, wAdditionalComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod,
        props );

    wGetBodyParam = new Button( wAdditionalComp, SWT.PUSH );
    wGetBodyParam.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.GetFields.Button" ) );
    fdGetBodyParam = new FormData();
    fdGetBodyParam.top = new FormAttachment( wlFields, margin );
    fdGetBodyParam.right = new FormAttachment( 100, 0 );
    wGetBodyParam.setLayoutData( fdGetBodyParam );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( wGetBodyParam, -margin );
    fdFields.bottom = new FormAttachment( wlFields, 200 );
    wFields.setLayoutData( fdFields );

    wlQuery = new Label( wAdditionalComp, SWT.NONE );
    wlQuery.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.QueryParameters.Label" ) );
    props.setLook( wlQuery );
    fdlQuery = new FormData();
    fdlQuery.left = new FormAttachment( 0, 0 );
    fdlQuery.top = new FormAttachment( wFields, margin );
    wlQuery.setLayoutData( fdlQuery );

    final int QueryRows = input.getQueryParameter().length;

    colinfquery =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "HTTPPOSTDialog.ColumnInfo.QueryName" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "HTTPPOSTDialog.ColumnInfo.QueryParameter" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };
    colinfquery[ 1 ].setUsingVariables( true );
    wQuery =
      new TableView(
        transMeta, wAdditionalComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinfquery, QueryRows,
        lsMod, props );

    wGet = new Button( wAdditionalComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "HTTPPOSTDialog.GetFields.Button" ) );
    fdGet = new FormData();
    fdGet.top = new FormAttachment( wlQuery, margin );
    fdGet.right = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    fdQuery = new FormData();
    fdQuery.left = new FormAttachment( 0, 0 );
    fdQuery.top = new FormAttachment( wlQuery, margin );
    fdQuery.right = new FormAttachment( wGet, -margin );
    fdQuery.bottom = new FormAttachment( 100, -margin );
    wQuery.setLayoutData( fdQuery );

    //
    // Search the fields in the background
    //

    final Runnable runnable = new Runnable() {
      public void run() {
        StepMeta stepMeta = transMeta.findStep( stepname );
        if ( stepMeta != null ) {
          try {
            RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

            // Remember these fields...
            for ( int i = 0; i < row.size(); i++ ) {
              inputFields.put( row.getValueMeta( i ).getName(), Integer.valueOf( i ) );
            }

            setComboBoxes();
          } catch ( HopException e ) {
            logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
          }
        }
      }
    };
    new Thread( runnable ).start();
    fdAdditionalComp = new FormData();
    fdAdditionalComp.left = new FormAttachment( 0, 0 );
    fdAdditionalComp.top = new FormAttachment( wStepname, margin );
    fdAdditionalComp.right = new FormAttachment( 100, 0 );
    fdAdditionalComp.bottom = new FormAttachment( 100, 0 );
    wAdditionalComp.setLayoutData( fdAdditionalComp );

    wAdditionalComp.layout();
    wAdditionalTab.setControl( wAdditionalComp );
    // ////// END of Additional Tab

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wStepname, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    // THE BUTTONS
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, wTabFolder );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        getQueryFields();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsGetBodyParam = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wGet.addListener( SWT.Selection, lsGet );
    wCancel.addListener( SWT.Selection, lsCancel );
    wGetBodyParam.addListener( SWT.Selection, lsGetBodyParam );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wUrl.addSelectionListener( lsDef );
    wResult.addSelectionListener( lsDef );
    wResultCode.addSelectionListener( lsDef );
    wResponseTime.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    lsResize = new Listener() {
      public void handleEvent( Event event ) {
        Point size = shell.getSize();
        wFields.setSize( size.x - 10, size.y - 50 );
        wFields.table.setSize( size.x - 10, size.y - 50 );
        wFields.redraw();
      }
    };
    shell.addListener( SWT.Resize, lsResize );

    // Set the shell size, based upon previous time...
    setSize();
    wTabFolder.setSelection( 0 );
    getData();
    activeUrlInfield();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<String, Integer>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    fieldNames = entries.toArray( new String[ entries.size() ] );

    Const.sortStrings( fieldNames );
    colinf[ 0 ].setComboValues( fieldNames );
    colinfquery[ 0 ].setComboValues( fieldNames );
  }

  private void setStreamFields() {
    if ( !gotPreviousFields ) {
      String urlfield = wUrlField.getText();
      wUrlField.removeAll();
      wUrlField.setItems( fieldNames );
      if ( urlfield != null ) {
        wUrlField.setText( urlfield );
      }

      String request = wrequestEntity.getText();
      wrequestEntity.removeAll();
      wrequestEntity.setItems( fieldNames );
      if ( request != null ) {
        wrequestEntity.setText( request );
      }

      gotPreviousFields = true;
    }
  }

  private void setEncodings() {
    // Encoding of the text file:
    if ( !gotEncodings ) {
      gotEncodings = true;

      wEncoding.removeAll();
      List<Charset> values = new ArrayList<Charset>( Charset.availableCharsets().values() );
      for ( int i = 0; i < values.size(); i++ ) {
        Charset charSet = values.get( i );
        wEncoding.add( charSet.displayName() );
      }

      // Now select the default!
      String defEncoding = Const.getEnvironmentVariable( "file.encoding", "UTF-8" );
      int idx = Const.indexOfString( defEncoding, wEncoding.getItems() );
      if ( idx >= 0 ) {
        wEncoding.select( idx );
      }
    }
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
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "HTTPPOSTDialog.Log.GettingKeyInfo" ) );
    }

    if ( input.getArgumentField() != null ) {
      for ( int i = 0; i < input.getArgumentField().length; i++ ) {
        TableItem item = wFields.table.getItem( i );
        if ( input.getArgumentField()[ i ] != null ) {
          item.setText( 1, input.getArgumentField()[ i ] );
        }
        if ( input.getArgumentParameter()[ i ] != null ) {
          item.setText( 2, input.getArgumentParameter()[ i ] );
        }
        item.setText( 3, ( input.getArgumentHeader()[ i ] ) ? YES : NO );
      }
    }
    if ( input.getQueryField() != null ) {
      for ( int i = 0; i < input.getQueryField().length; i++ ) {
        TableItem item = wQuery.table.getItem( i );
        if ( input.getQueryField()[ i ] != null ) {
          item.setText( 1, input.getQueryField()[ i ] );
        }
        if ( input.getQueryParameter()[ i ] != null ) {
          item.setText( 2, input.getQueryParameter()[ i ] );
        }
      }
    }
    if ( input.getUrl() != null ) {
      wUrl.setText( input.getUrl() );
    }
    wUrlInField.setSelection( input.isUrlInField() );
    if ( input.getUrlField() != null ) {
      wUrlField.setText( input.getUrlField() );
    }
    if ( input.getRequestEntity() != null ) {
      wrequestEntity.setText( input.getRequestEntity() );
    }
    if ( input.getFieldName() != null ) {
      wResult.setText( input.getFieldName() );
    }
    if ( input.getResultCodeFieldName() != null ) {
      wResultCode.setText( input.getResultCodeFieldName() );
    }
    if ( input.getResponseTimeFieldName() != null ) {
      wResponseTime.setText( input.getResponseTimeFieldName() );
    }
    if ( input.getEncoding() != null ) {
      wEncoding.setText( input.getEncoding() );
    }
    wPostAFile.setSelection( input.isPostAFile() );

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
    if ( input.getResponseHeaderFieldName() != null ) {
      wResponseHeader.setText( input.getResponseHeaderFieldName() );
    }

    wSocketTimeOut.setText( Const.NVL( input.getSocketTimeout(), "" ) );
    wConnectionTimeOut.setText( Const.NVL( input.getConnectionTimeout(), "" ) );
    wCloseIdleConnectionsTime.setText( Const.NVL( input.getCloseIdleConnectionsTime(), "" ) );

    wFields.setRowNums();
    wFields.optWidth( true );

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

    int nrargs = wFields.nrNonEmpty();
    input.allocate( nrargs );

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "HTTPPOSTDialog.Log.FoundArguments", String.valueOf( nrargs ) ) );
    }
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrargs; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      input.getArgumentField()[ i ] = item.getText( 1 );
      input.getArgumentParameter()[ i ] = item.getText( 2 );
      input.getArgumentHeader()[ i ] = YES.equals( item.getText( 3 ) );
    }

    int nrqueryparams = wQuery.nrNonEmpty();
    input.allocateQuery( nrqueryparams );

    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "HTTPPOSTDialog.Log.FoundQueryParameters", String
        .valueOf( nrqueryparams ) ) );
    }
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrqueryparams; i++ ) {
      TableItem item = wQuery.getNonEmpty( i );
      input.getQueryField()[ i ] = item.getText( 1 );
      input.getQueryParameter()[ i ] = item.getText( 2 );
    }

    input.setUrl( wUrl.getText() );
    input.setUrlField( wUrlField.getText() );
    input.setRequestEntity( wrequestEntity.getText() );
    input.setUrlInField( wUrlInField.getSelection() );
    input.setFieldName( wResult.getText() );
    input.setResultCodeFieldName( wResultCode.getText() );
    input.setResponseTimeFieldName( wResponseTime.getText() );
    input.setResponseHeaderFieldName( wResponseHeader.getText() );
    input.setEncoding( wEncoding.getText() );
    input.setPostAFile( wPostAFile.getSelection() );
    input.setHttpLogin( wHttpLogin.getText() );
    input.setHttpPassword( wHttpPassword.getText() );
    input.setProxyHost( wProxyHost.getText() );
    input.setProxyPort( wProxyPort.getText() );
    input.setSocketTimeout( wSocketTimeOut.getText() );
    input.setConnectionTimeout( wConnectionTimeOut.getText() );
    input.setCloseIdleConnectionsTime( wCloseIdleConnectionsTime.getText() );

    stepname = wStepname.getText(); // return value

    dispose();
  }

  private void get() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null && !r.isEmpty() ) {
        TableItemInsertListener listener = new TableItemInsertListener() {
          public boolean tableItemInserted( TableItem tableItem, ValueMetaInterface v ) {
            tableItem.setText( 3, NO ); // default is "N"
            return true;
          }
        };
        BaseStepDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1, 2 }, null, -1, -1, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "HTTPPOSTDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "HTTPPOSTDialog.FailedToGetFields.DialogMessage" ), ke );
    }

  }

  private void getQueryFields() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null && !r.isEmpty() ) {
        BaseStepDialog.getFieldsFromPrevious( r, wQuery, 1, new int[] { 1, 2 }, new int[] { 3 }, -1, -1, null );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "HTTPPOSTDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "HTTPPOSTDialog.FailedToGetFields.DialogMessage" ), ke );
    }

  }

}
