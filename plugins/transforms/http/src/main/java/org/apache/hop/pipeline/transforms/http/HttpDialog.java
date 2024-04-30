/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.http;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class HttpDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = HttpMeta.class; // For Translator

  private Label wlUrl;
  private TextVar wUrl;

  private TextVar wResult;

  private TableView wFields;

  private TableView wHeaders;

  private Button wUrlInField;

  private Button wIgnoreSsl;

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

  private final List<String> inputFields = new ArrayList<>();

  private boolean gotEncodings = false;

  private TextVar wConnectionTimeOut;

  private TextVar wSocketTimeOut;

  private TextVar wCloseIdleConnectionsTime;

  public HttpDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (HttpMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "HTTPDialog.Shell.Title"));

    int margin = PropsUi.getMargin();

    setupButtons();

    Control lastControl = setupTransformNameField(lsMod);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, PropsUi.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "HTTPDialog.GeneralTab.Title"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wGeneralComp.setLayout(fileLayout);

    // ////////////////////////
    // START Settings GROUP

    Group gSettings = setupSettingGroup(wGeneralComp);
    lastControl = setupUrlLine(lsMod, lastControl, gSettings);
    lastControl = setupUrlInFieldLine(lastControl, gSettings);
    lastControl = setupIgnoreSslLine(lastControl, gSettings);
    lastControl = setupUrlFieldNameLine(lsMod, lastControl, gSettings);
    lastControl = setupEncodingLine(lsMod, lastControl, gSettings);
    setupConnectionTimeoutLine(lsMod, gSettings);
    setupSocketTimeoutLine(lsMod, gSettings);
    setupCloseWaitTimeLine(lsMod, gSettings);

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment(0, 0);
    fdSettings.right = new FormAttachment(100, 0);
    fdSettings.top = new FormAttachment(lastControl, margin);
    gSettings.setLayoutData(fdSettings);

    // END Output Settings GROUP
    // ////////////////////////
    lastControl = gSettings;
    // ////////////////////////
    // START Output Fields GROUP

    Group gOutputFields = setupOutputFieldGroup(wGeneralComp);
    setupResultLine(lsMod, lastControl, gOutputFields);
    setupStatusCodeLine(lsMod, gOutputFields);
    setupResponseTimeLine(lsMod, gOutputFields);
    setupResponseHeaderLine(lsMod, gOutputFields);

    FormData fdOutputFields = new FormData();
    fdOutputFields.left = new FormAttachment(0, 0);
    fdOutputFields.right = new FormAttachment(100, 0);
    fdOutputFields.top = new FormAttachment(lastControl);
    gOutputFields.setLayoutData(fdOutputFields);

    // END Output Fields GROUP
    // ////////////////////////

    lastControl = gOutputFields;

    // ////////////////////////
    // START Http AUTH GROUP

    Group gHttpAuth = setupHttpAuthGroup(wGeneralComp);
    setupHttpLoginLine(lsMod, gHttpAuth);
    setupHttpPasswordLine(lsMod, gHttpAuth);

    FormData fdHttpAuth = new FormData();
    fdHttpAuth.left = new FormAttachment(0, 0);
    fdHttpAuth.right = new FormAttachment(100, 0);
    fdHttpAuth.top = new FormAttachment(lastControl, margin);
    gHttpAuth.setLayoutData(fdHttpAuth);

    // END Http AUTH GROUP
    // ////////////////////////

    lastControl = gHttpAuth;

    // ////////////////////////
    // START PROXY GROUP

    Group gProxy = setupProxyGroup(wGeneralComp);
    setupProxyHost(lsMod, gProxy);
    setupProxyPort(lsMod, lastControl, gProxy);

    // END Http Proxy GROUP
    // ////////////////////////
    lastControl = gProxy;

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(wTransformName, margin);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////
    // Additional tab...
    //
    CTabItem wAdditionalTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdditionalTab.setFont(GuiResource.getInstance().getFontDefault());
    wAdditionalTab.setText(BaseMessages.getString(PKG, "HTTPDialog.FieldsTab.Title"));

    FormLayout addLayout = new FormLayout();
    addLayout.marginWidth = PropsUi.getFormMargin();
    addLayout.marginHeight = PropsUi.getFormMargin();

    Composite wAdditionalComp = new Composite(wTabFolder, SWT.NONE);
    wAdditionalComp.setLayout(addLayout);
    PropsUi.setLook(wAdditionalComp);

    setupParamBlock(lsMod, lastControl, wAdditionalComp);
    setupHeadBlock(lsMod, wAdditionalComp);

    //
    // Search the fields in the background

    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.add(row.getValueMeta(i).getName());
              }
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    FormData fdAdditionalComp = new FormData();
    fdAdditionalComp.left = new FormAttachment(0, 0);
    fdAdditionalComp.top = new FormAttachment(wTransformName, margin);
    fdAdditionalComp.right = new FormAttachment(100, 0);
    fdAdditionalComp.bottom = new FormAttachment(100, 0);
    wAdditionalComp.setLayoutData(fdAdditionalComp);

    wAdditionalComp.layout();
    wAdditionalTab.setControl(wAdditionalComp);
    // ////// END of Additional Tab

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    lsResize =
        event -> {
          Point size = shell.getSize();
          wFields.setSize(size.x - 10, size.y - 50);
          wFields.table.setSize(size.x - 10, size.y - 50);
          wFields.redraw();
        };
    shell.addListener(SWT.Resize, lsResize);

    getData();
    wTabFolder.setSelection(0);
    activeUrlInfield();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void setupHeadBlock(ModifyListener lsMod, Composite wAdditionalComp) {
    int margin = props.getMargin();
    Label wlHeaders = new Label(wAdditionalComp, SWT.NONE);
    wlHeaders.setText(BaseMessages.getString(PKG, "HTTPDialog.Headers.Label"));
    PropsUi.setLook(wlHeaders);
    FormData fdlHeaders = new FormData();
    fdlHeaders.left = new FormAttachment(0, 0);
    fdlHeaders.top = new FormAttachment(wFields, margin);
    wlHeaders.setLayoutData(fdlHeaders);

    final int HeadersRows = input.getHeaderParameter().length;

    colinfHeaders =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "HTTPDialog.ColumnInfo.Field"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "HTTPDialog.ColumnInfo.Header"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };
    colinfHeaders[1].setUsingVariables(true);
    wHeaders =
        new TableView(
            variables,
            wAdditionalComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinfHeaders,
            HeadersRows,
            lsMod,
            props);

    Button wGetHeaders = new Button(wAdditionalComp, SWT.PUSH);
    wGetHeaders.setText(BaseMessages.getString(PKG, "HTTPDialog.GetHeaders.Button"));
    FormData fdGetHeaders = new FormData();
    fdGetHeaders.top = new FormAttachment(wlHeaders, margin);
    fdGetHeaders.right = new FormAttachment(100, 0);
    wGetHeaders.setLayoutData(fdGetHeaders);
    wGetHeaders.addListener(SWT.Selection, e -> getHeadersFields());

    FormData fdHeaders = new FormData();
    fdHeaders.left = new FormAttachment(0, 0);
    fdHeaders.top = new FormAttachment(wlHeaders, margin);
    fdHeaders.right = new FormAttachment(wGetHeaders, -margin);
    fdHeaders.bottom = new FormAttachment(100, -margin);
    wHeaders.setLayoutData(fdHeaders);
  }

  private void setupParamBlock(
      ModifyListener lsMod, Control lastControl, Composite wAdditionalComp) {
    int margin = props.getMargin();
    Label wlFields = new Label(wAdditionalComp, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "HTTPDialog.Parameters.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(lastControl, margin);
    wlFields.setLayoutData(fdlFields);
    lastControl = wlFields;

    Button wGet = new Button(wAdditionalComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "HTTPDialog.GetFields.Button"));
    FormData fdGet = new FormData();
    fdGet.top = new FormAttachment(wlFields, margin);
    fdGet.right = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);
    wGet.addListener(SWT.Selection, e -> get());

    final int FieldsRows = input.getArgumentField().length;

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "HTTPDialog.ColumnInfo.Name"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "HTTPDialog.ColumnInfo.Parameter"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    wFields =
        new TableView(
            variables,
            wAdditionalComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(wGet, -margin);
    fdFields.bottom = new FormAttachment(wlFields, 200);
    wFields.setLayoutData(fdFields);
  }

  private void setupProxyPort(ModifyListener lsMod, Control lastControl, Group gProxy) {
    // Proxy port
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlProxyPort = new Label(gProxy, SWT.RIGHT);
    wlProxyPort.setText(BaseMessages.getString(PKG, "HTTPDialog.ProxyPort.Label"));
    PropsUi.setLook(wlProxyPort);
    FormData fdlProxyPort = new FormData();
    fdlProxyPort.top = new FormAttachment(wProxyHost, margin);
    fdlProxyPort.left = new FormAttachment(0, 0);
    fdlProxyPort.right = new FormAttachment(middle, -margin);
    wlProxyPort.setLayoutData(fdlProxyPort);
    wProxyPort = new TextVar(variables, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wProxyPort.addModifyListener(lsMod);
    wProxyPort.setToolTipText(BaseMessages.getString(PKG, "HTTPDialog.ProxyPort.Tooltip"));
    PropsUi.setLook(wProxyPort);
    FormData fdProxyPort = new FormData();
    fdProxyPort.top = new FormAttachment(wProxyHost, margin);
    fdProxyPort.left = new FormAttachment(middle, 0);
    fdProxyPort.right = new FormAttachment(100, 0);
    wProxyPort.setLayoutData(fdProxyPort);

    FormData fdProxy = new FormData();
    fdProxy.left = new FormAttachment(0, 0);
    fdProxy.right = new FormAttachment(100, 0);
    fdProxy.top = new FormAttachment(lastControl, margin);
    gProxy.setLayoutData(fdProxy);
  }

  private void setupProxyHost(ModifyListener lsMod, Group gProxy) {
    // Proxy host
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlProxyHost = new Label(gProxy, SWT.RIGHT);
    wlProxyHost.setText(BaseMessages.getString(PKG, "HTTPDialog.ProxyHost.Label"));
    PropsUi.setLook(wlProxyHost);
    FormData fdlProxyHost = new FormData();
    fdlProxyHost.top = new FormAttachment(0, margin);
    fdlProxyHost.left = new FormAttachment(0, 0);
    fdlProxyHost.right = new FormAttachment(middle, -margin);
    wlProxyHost.setLayoutData(fdlProxyHost);
    wProxyHost = new TextVar(variables, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wProxyHost.addModifyListener(lsMod);
    wProxyHost.setToolTipText(BaseMessages.getString(PKG, "HTTPDialog.ProxyHost.Tooltip"));
    PropsUi.setLook(wProxyHost);
    FormData fdProxyHost = new FormData();
    fdProxyHost.top = new FormAttachment(0, margin);
    fdProxyHost.left = new FormAttachment(middle, 0);
    fdProxyHost.right = new FormAttachment(100, 0);
    wProxyHost.setLayoutData(fdProxyHost);
  }

  private Group setupProxyGroup(Composite wGeneralComp) {
    Group gProxy = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN);
    gProxy.setText(BaseMessages.getString(PKG, "HTTPDialog.ProxyGroup.Label"));
    FormLayout proxyLayout = new FormLayout();
    proxyLayout.marginWidth = 3;
    proxyLayout.marginHeight = 3;
    gProxy.setLayout(proxyLayout);
    PropsUi.setLook(gProxy);
    return gProxy;
  }

  private void setupHttpPasswordLine(ModifyListener lsMod, Group gHttpAuth) {
    // Http Password
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlHttpPassword = new Label(gHttpAuth, SWT.RIGHT);
    wlHttpPassword.setText(BaseMessages.getString(PKG, "HTTPDialog.HttpPassword.Label"));
    PropsUi.setLook(wlHttpPassword);
    FormData fdlHttpPassword = new FormData();
    fdlHttpPassword.top = new FormAttachment(wHttpLogin, margin);
    fdlHttpPassword.left = new FormAttachment(0, 0);
    fdlHttpPassword.right = new FormAttachment(middle, -margin);
    wlHttpPassword.setLayoutData(fdlHttpPassword);
    wHttpPassword = new PasswordTextVar(variables, gHttpAuth, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHttpPassword.addModifyListener(lsMod);
    wHttpPassword.setToolTipText(BaseMessages.getString(PKG, "HTTPDialog.HttpPassword.Tooltip"));
    PropsUi.setLook(wHttpPassword);
    FormData fdHttpPassword = new FormData();
    fdHttpPassword.top = new FormAttachment(wHttpLogin, margin);
    fdHttpPassword.left = new FormAttachment(middle, 0);
    fdHttpPassword.right = new FormAttachment(100, 0);
    wHttpPassword.setLayoutData(fdHttpPassword);
  }

  private void setupHttpLoginLine(ModifyListener lsMod, Group gHttpAuth) {
    // Http Login
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlHttpLogin = new Label(gHttpAuth, SWT.RIGHT);
    wlHttpLogin.setText(BaseMessages.getString(PKG, "HTTPDialog.HttpLogin.Label"));
    PropsUi.setLook(wlHttpLogin);
    FormData fdlHttpLogin = new FormData();
    fdlHttpLogin.top = new FormAttachment(0, margin);
    fdlHttpLogin.left = new FormAttachment(0, 0);
    fdlHttpLogin.right = new FormAttachment(middle, -margin);
    wlHttpLogin.setLayoutData(fdlHttpLogin);
    wHttpLogin = new TextVar(variables, gHttpAuth, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHttpLogin.addModifyListener(lsMod);
    wHttpLogin.setToolTipText(BaseMessages.getString(PKG, "HTTPDialog.HttpLogin.Tooltip"));
    PropsUi.setLook(wHttpLogin);
    FormData fdHttpLogin = new FormData();
    fdHttpLogin.top = new FormAttachment(0, margin);
    fdHttpLogin.left = new FormAttachment(middle, 0);
    fdHttpLogin.right = new FormAttachment(100, 0);
    wHttpLogin.setLayoutData(fdHttpLogin);
  }

  private Group setupHttpAuthGroup(Composite wGeneralComp) {
    Group gHttpAuth = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN);
    gHttpAuth.setText(BaseMessages.getString(PKG, "HTTPDialog.HttpAuthGroup.Label"));
    FormLayout httpAuthLayout = new FormLayout();
    httpAuthLayout.marginWidth = 3;
    httpAuthLayout.marginHeight = 3;
    gHttpAuth.setLayout(httpAuthLayout);
    PropsUi.setLook(gHttpAuth);
    return gHttpAuth;
  }

  private void setupResponseHeaderLine(ModifyListener lsMod, Group gOutputFields) {
    // Response header line...
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlResponseHeader = new Label(gOutputFields, SWT.RIGHT);
    wlResponseHeader.setText(BaseMessages.getString(PKG, "HTTPDialog.ResponseHeader.Label"));
    PropsUi.setLook(wlResponseHeader);
    FormData fdlResponseHeader = new FormData();
    fdlResponseHeader.left = new FormAttachment(0, 0);
    fdlResponseHeader.right = new FormAttachment(middle, -margin);
    fdlResponseHeader.top = new FormAttachment(wResponseTime, margin);
    wlResponseHeader.setLayoutData(fdlResponseHeader);
    wResponseHeader = new TextVar(variables, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wResponseHeader);
    wResponseHeader.addModifyListener(lsMod);
    FormData fdResponseHeader = new FormData();
    fdResponseHeader.left = new FormAttachment(middle, 0);
    fdResponseHeader.top = new FormAttachment(wResponseTime, margin);
    fdResponseHeader.right = new FormAttachment(100, 0);
    wResponseHeader.setLayoutData(fdResponseHeader);
  }

  private void setupResponseTimeLine(ModifyListener lsMod, Group gOutputFields) {
    // Response time line...
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlResponseTime = new Label(gOutputFields, SWT.RIGHT);
    wlResponseTime.setText(BaseMessages.getString(PKG, "HTTPDialog.ResponseTime.Label"));
    PropsUi.setLook(wlResponseTime);
    FormData fdlResponseTime = new FormData();
    fdlResponseTime.left = new FormAttachment(0, 0);
    fdlResponseTime.right = new FormAttachment(middle, -margin);
    fdlResponseTime.top = new FormAttachment(wResultCode, margin);
    wlResponseTime.setLayoutData(fdlResponseTime);
    wResponseTime = new TextVar(variables, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wResponseTime);
    wResponseTime.addModifyListener(lsMod);
    FormData fdResponseTime = new FormData();
    fdResponseTime.left = new FormAttachment(middle, 0);
    fdResponseTime.top = new FormAttachment(wResultCode, margin);
    fdResponseTime.right = new FormAttachment(100, 0);
    wResponseTime.setLayoutData(fdResponseTime);
  }

  private void setupStatusCodeLine(ModifyListener lsMod, Group gOutputFields) {
    // Resultcode line...
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlResultCode = new Label(gOutputFields, SWT.RIGHT);
    wlResultCode.setText(BaseMessages.getString(PKG, "HTTPDialog.ResultCode.Label"));
    PropsUi.setLook(wlResultCode);
    FormData fdlResultCode = new FormData();
    fdlResultCode.left = new FormAttachment(0, 0);
    fdlResultCode.right = new FormAttachment(middle, -margin);
    fdlResultCode.top = new FormAttachment(wResult, margin);
    wlResultCode.setLayoutData(fdlResultCode);
    wResultCode = new TextVar(variables, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wResultCode);
    wResultCode.addModifyListener(lsMod);
    FormData fdResultCode = new FormData();
    fdResultCode.left = new FormAttachment(middle, 0);
    fdResultCode.top = new FormAttachment(wResult, margin);
    fdResultCode.right = new FormAttachment(100, 0);
    wResultCode.setLayoutData(fdResultCode);
  }

  private void setupResultLine(ModifyListener lsMod, Control lastControl, Group gOutputFields) {
    // Result line...
    //
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlResult = new Label(gOutputFields, SWT.RIGHT);
    wlResult.setText(BaseMessages.getString(PKG, "HTTPDialog.Result.Label"));
    PropsUi.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment(0, 0);
    fdlResult.right = new FormAttachment(middle, -margin);
    fdlResult.top = new FormAttachment(lastControl, margin);
    wlResult.setLayoutData(fdlResult);
    wResult = new TextVar(variables, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wResult);
    wResult.addModifyListener(lsMod);
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment(middle, 0);
    fdResult.top = new FormAttachment(lastControl, margin);
    fdResult.right = new FormAttachment(100, 0);
    wResult.setLayoutData(fdResult);
  }

  private Group setupOutputFieldGroup(Composite wGeneralComp) {
    Group gOutputFields = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN);
    gOutputFields.setText(BaseMessages.getString(PKG, "HTTPDialog.OutputFieldsGroup.Label"));
    FormLayout outputFieldsLayout = new FormLayout();
    outputFieldsLayout.marginWidth = 3;
    outputFieldsLayout.marginHeight = 3;
    gOutputFields.setLayout(outputFieldsLayout);
    PropsUi.setLook(gOutputFields);
    return gOutputFields;
  }

  private void setupCloseWaitTimeLine(ModifyListener lsMod, Group gSettings) {
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlCloseIdleConnectionsTime = new Label(gSettings, SWT.RIGHT);
    wlCloseIdleConnectionsTime.setText(
        BaseMessages.getString(PKG, "HTTPDialog.CloseIdleConnectionsTime.Label"));
    PropsUi.setLook(wlCloseIdleConnectionsTime);
    FormData fdlCloseIdleConnectionsTime = new FormData();
    fdlCloseIdleConnectionsTime.top = new FormAttachment(wSocketTimeOut, margin);
    fdlCloseIdleConnectionsTime.left = new FormAttachment(0, 0);
    fdlCloseIdleConnectionsTime.right = new FormAttachment(middle, -margin);
    wlCloseIdleConnectionsTime.setLayoutData(fdlCloseIdleConnectionsTime);
    wCloseIdleConnectionsTime =
        new TextVar(variables, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wCloseIdleConnectionsTime.addModifyListener(lsMod);
    wCloseIdleConnectionsTime.setToolTipText(
        BaseMessages.getString(PKG, "HTTPDialog.CloseIdleConnectionsTime.Tooltip"));
    PropsUi.setLook(wCloseIdleConnectionsTime);
    FormData fdCloseIdleConnectionsTime = new FormData();
    fdCloseIdleConnectionsTime.top = new FormAttachment(wSocketTimeOut, margin);
    fdCloseIdleConnectionsTime.left = new FormAttachment(middle, 0);
    fdCloseIdleConnectionsTime.right = new FormAttachment(100, 0);
    wCloseIdleConnectionsTime.setLayoutData(fdCloseIdleConnectionsTime);
  }

  private void setupSocketTimeoutLine(ModifyListener lsMod, Group gSettings) {
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlSocketTimeOut = new Label(gSettings, SWT.RIGHT);
    wlSocketTimeOut.setText(BaseMessages.getString(PKG, "HTTPDialog.SocketTimeOut.Label"));
    PropsUi.setLook(wlSocketTimeOut);
    FormData fdlSocketTimeOut = new FormData();
    fdlSocketTimeOut.top = new FormAttachment(wConnectionTimeOut, margin);
    fdlSocketTimeOut.left = new FormAttachment(0, 0);
    fdlSocketTimeOut.right = new FormAttachment(middle, -margin);
    wlSocketTimeOut.setLayoutData(fdlSocketTimeOut);
    wSocketTimeOut = new TextVar(variables, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSocketTimeOut.addModifyListener(lsMod);
    wSocketTimeOut.setToolTipText(BaseMessages.getString(PKG, "HTTPDialog.SocketTimeOut.Tooltip"));
    PropsUi.setLook(wSocketTimeOut);
    FormData fdSocketTimeOut = new FormData();
    fdSocketTimeOut.top = new FormAttachment(wConnectionTimeOut, margin);
    fdSocketTimeOut.left = new FormAttachment(middle, 0);
    fdSocketTimeOut.right = new FormAttachment(100, 0);
    wSocketTimeOut.setLayoutData(fdSocketTimeOut);
  }

  private void setupConnectionTimeoutLine(ModifyListener lsMod, Group gSettings) {
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlConnectionTimeOut = new Label(gSettings, SWT.RIGHT);
    wlConnectionTimeOut.setText(BaseMessages.getString(PKG, "HTTPDialog.ConnectionTimeOut.Label"));
    PropsUi.setLook(wlConnectionTimeOut);
    FormData fdlConnectionTimeOut = new FormData();
    fdlConnectionTimeOut.top = new FormAttachment(wEncoding, margin);
    fdlConnectionTimeOut.left = new FormAttachment(0, 0);
    fdlConnectionTimeOut.right = new FormAttachment(middle, -margin);
    wlConnectionTimeOut.setLayoutData(fdlConnectionTimeOut);
    wConnectionTimeOut = new TextVar(variables, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wConnectionTimeOut.addModifyListener(lsMod);
    wConnectionTimeOut.setToolTipText(
        BaseMessages.getString(PKG, "HTTPDialog.ConnectionTimeOut.Tooltip"));
    PropsUi.setLook(wConnectionTimeOut);
    FormData fdConnectionTimeOut = new FormData();
    fdConnectionTimeOut.top = new FormAttachment(wEncoding, margin);
    fdConnectionTimeOut.left = new FormAttachment(middle, 0);
    fdConnectionTimeOut.right = new FormAttachment(100, 0);
    wConnectionTimeOut.setLayoutData(fdConnectionTimeOut);
  }

  private Control setupEncodingLine(ModifyListener lsMod, Control lastControl, Group gSettings) {
    // Encoding
    //
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlEncoding = new Label(gSettings, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "HTTPDialog.Encoding.Label"));
    PropsUi.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.top = new FormAttachment(lastControl, margin);
    fdlEncoding.left = new FormAttachment(0, 0);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new ComboVar(variables, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEncoding);
    wEncoding.addModifyListener(lsMod);
    FormData fdEncoding = new FormData();
    fdEncoding.top = new FormAttachment(lastControl, margin);
    fdEncoding.left = new FormAttachment(middle, 0);
    fdEncoding.right = new FormAttachment(100, 0);
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {}

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            setEncodings();
            shell.setCursor(null);
            busy.dispose();
          }
        });
    lastControl = wEncoding;
    return lastControl;
  }

  private Control setupUrlFieldNameLine(
      ModifyListener lsMod, Control lastControl, Group gSettings) {
    // UrlField Line
    //
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    wlUrlField = new Label(gSettings, SWT.RIGHT);
    wlUrlField.setText(BaseMessages.getString(PKG, "HTTPDialog.UrlField.Label"));
    PropsUi.setLook(wlUrlField);
    FormData fdlUrlField = new FormData();
    fdlUrlField.left = new FormAttachment(0, 0);
    fdlUrlField.right = new FormAttachment(middle, -margin);
    fdlUrlField.top = new FormAttachment(lastControl, margin);
    wlUrlField.setLayoutData(fdlUrlField);

    wUrlField = new ComboVar(variables, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wUrlField.setToolTipText(BaseMessages.getString(PKG, "HTTPDialog.UrlField.Tooltip"));
    PropsUi.setLook(wUrlField);
    wUrlField.addModifyListener(lsMod);
    FormData fdUrlField = new FormData();
    fdUrlField.left = new FormAttachment(middle, 0);
    fdUrlField.top = new FormAttachment(lastControl, margin);
    fdUrlField.right = new FormAttachment(100, 0);
    wUrlField.setLayoutData(fdUrlField);
    wUrlField.setEnabled(false);
    wUrlField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {}

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            BaseTransformDialog.getFieldsFromPrevious(
                variables, wUrlField, pipelineMeta, transformMeta);
            shell.setCursor(null);
            busy.dispose();
          }
        });
    lastControl = wUrlField;
    return lastControl;
  }

  private Control setupUrlInFieldLine(Control lastControl, Group gSettings) {
    // UrlInField line
    //
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlUrlInField = new Label(gSettings, SWT.RIGHT);
    wlUrlInField.setText(BaseMessages.getString(PKG, "HTTPDialog.UrlInField.Label"));
    PropsUi.setLook(wlUrlInField);
    FormData fdlUrlInField = new FormData();
    fdlUrlInField.left = new FormAttachment(0, 0);
    fdlUrlInField.top = new FormAttachment(lastControl, margin);
    fdlUrlInField.right = new FormAttachment(middle, -margin);
    wlUrlInField.setLayoutData(fdlUrlInField);
    wUrlInField = new Button(gSettings, SWT.CHECK);
    PropsUi.setLook(wUrlInField);
    FormData fdUrlInField = new FormData();
    fdUrlInField.left = new FormAttachment(middle, 0);
    fdUrlInField.top = new FormAttachment(wlUrlInField, 0, SWT.CENTER);
    fdUrlInField.right = new FormAttachment(100, 0);
    wUrlInField.setLayoutData(fdUrlInField);
    wUrlInField.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            activeUrlInfield();
          }
        });
    lastControl = wUrlInField;
    return lastControl;
  }

  private Control setupIgnoreSslLine(Control lastControl, Group gSettings) {
    // ignoreSsl line
    //
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    Label wlIgnoreSsl = new Label(gSettings, SWT.RIGHT);
    wlIgnoreSsl.setText(BaseMessages.getString(PKG, "HTTPDialog.IgnoreSsl.Label"));
    PropsUi.setLook(wlIgnoreSsl);
    FormData fdlIgnoreSsl = new FormData();
    fdlIgnoreSsl.left = new FormAttachment(0, 0);
    fdlIgnoreSsl.top = new FormAttachment(lastControl, margin);
    fdlIgnoreSsl.right = new FormAttachment(middle, -margin);
    wlIgnoreSsl.setLayoutData(fdlIgnoreSsl);
    wIgnoreSsl = new Button(gSettings, SWT.CHECK);
    PropsUi.setLook(wIgnoreSsl);
    FormData fdIgnoreSsl = new FormData();
    fdIgnoreSsl.left = new FormAttachment(middle, 0);
    fdIgnoreSsl.top = new FormAttachment(wlIgnoreSsl, 0, SWT.CENTER);
    fdIgnoreSsl.right = new FormAttachment(100, 0);
    wIgnoreSsl.setLayoutData(fdIgnoreSsl);
    wIgnoreSsl.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });
    lastControl = wIgnoreSsl;
    return lastControl;
  }

  private Control setupUrlLine(
      ModifyListener lsMod, Control transformNameControl, Group gSettings) {
    // The URL to use
    //
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    wlUrl = new Label(gSettings, SWT.RIGHT);
    wlUrl.setText(BaseMessages.getString(PKG, "HTTPDialog.URL.Label"));
    PropsUi.setLook(wlUrl);
    FormData fdlUrl = new FormData();
    fdlUrl.left = new FormAttachment(0, 0);
    fdlUrl.right = new FormAttachment(middle, -margin);
    fdlUrl.top = new FormAttachment(transformNameControl, margin);
    wlUrl.setLayoutData(fdlUrl);

    wUrl = new TextVar(variables, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUrl);
    wUrl.addModifyListener(lsMod);
    FormData fdUrl = new FormData();
    fdUrl.left = new FormAttachment(middle, 0);
    fdUrl.top = new FormAttachment(transformNameControl, margin);
    fdUrl.right = new FormAttachment(100, 0);
    wUrl.setLayoutData(fdUrl);
    transformNameControl = wUrl;
    return transformNameControl;
  }

  private Group setupSettingGroup(Composite wGeneralComp) {
    Group gSettings = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN);
    gSettings.setText(BaseMessages.getString(PKG, "HTTPDialog.SettingsGroup.Label"));
    FormLayout settingsLayout = new FormLayout();
    settingsLayout.marginWidth = 3;
    settingsLayout.marginHeight = 3;
    gSettings.setLayout(settingsLayout);
    PropsUi.setLook(gSettings);
    return gSettings;
  }

  private Control setupTransformNameField(ModifyListener lsMod) {
    // TransformName line
    //
    int margin = props.getMargin();
    int middle = props.getMiddlePct();
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "HTTPDialog.TransformName.Label"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;
    return lastControl;
  }

  private void setupButtons() {
    // THE BUTTONS
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, props.getMargin(), null);
  }

  private void setEncodings() {
    // Encoding of the text file:
    if (!gotEncodings) {
      gotEncodings = true;

      wEncoding.removeAll();
      List<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
      for (Charset charSet : values) {
        wEncoding.add(charSet.displayName());
      }

      // Now select the default!
      String defEncoding = Const.getEnvironmentVariable("file.encoding", "UTF-8");
      int idx = Const.indexOfString(defEncoding, wEncoding.getItems());
      if (idx >= 0) {
        wEncoding.select(idx);
      }
    }
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    colinf[0].setComboValues(fieldNames);
    colinfHeaders[0].setComboValues(fieldNames);
  }

  private void activeUrlInfield() {
    wlUrlField.setEnabled(wUrlInField.getSelection());
    wUrlField.setEnabled(wUrlInField.getSelection());
    wlUrl.setEnabled(!wUrlInField.getSelection());
    wUrl.setEnabled(!wUrlInField.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "HTTPDialog.Log.GettingKeyInfo"));
    }

    if (input.getArgumentField() != null) {
      for (int i = 0; i < input.getArgumentField().length; i++) {
        TableItem item = wFields.table.getItem(i);
        item.setText(1, Const.NVL(input.getArgumentField()[i], ""));
        item.setText(2, Const.NVL(input.getArgumentParameter()[i], ""));
      }
    }

    if (input.getHeaderField() != null) {
      for (int i = 0; i < input.getHeaderField().length; i++) {
        TableItem item = wHeaders.table.getItem(i);
        if (input.getHeaderField()[i] != null) {
          item.setText(1, input.getHeaderField()[i]);
        }
        if (input.getHeaderParameter()[i] != null) {
          item.setText(2, input.getHeaderParameter()[i]);
        }
      }
    }
    wSocketTimeOut.setText(Const.NVL(input.getSocketTimeout(), ""));
    wConnectionTimeOut.setText(Const.NVL(input.getConnectionTimeout(), ""));
    wCloseIdleConnectionsTime.setText(Const.NVL(input.getCloseIdleConnectionsTime(), ""));

    wUrl.setText(Const.NVL(input.getUrl(), ""));
    wUrlInField.setSelection(input.isUrlInField());
    wIgnoreSsl.setSelection(input.isIgnoreSsl());
    wUrlField.setText(Const.NVL(input.getUrlField(), ""));
    wEncoding.setText(Const.NVL(input.getEncoding(), ""));

    wResult.setText(Const.NVL(input.getFieldName(), ""));
    if (input.getHttpLogin() != null) {
      wHttpLogin.setText(input.getHttpLogin());
    }
    if (input.getHttpPassword() != null) {
      wHttpPassword.setText(input.getHttpPassword());
    }
    if (input.getProxyHost() != null) {
      wProxyHost.setText(input.getProxyHost());
    }
    if (input.getProxyPort() != null) {
      wProxyPort.setText(input.getProxyPort());
    }
    if (input.getResultCodeFieldName() != null) {
      wResultCode.setText(input.getResultCodeFieldName());
    }
    if (input.getResponseTimeFieldName() != null) {
      wResponseTime.setText(input.getResponseTimeFieldName());
    }
    if (input.getResponseHeaderFieldName() != null) {
      wResponseHeader.setText(input.getResponseHeaderFieldName());
    }

    wFields.setRowNums();
    wFields.optWidth(true);
    wHeaders.setRowNums();
    wHeaders.optWidth(true);

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

    int nrargs = wFields.nrNonEmpty();
    int nrheaders = wHeaders.nrNonEmpty();

    input.allocate(nrargs, nrheaders);

    if (isDebug()) {
      logDebug(
          BaseMessages.getString(PKG, "HTTPDialog.Log.FoundArguments", String.valueOf(nrargs)));
    }
    for (int i = 0; i < nrargs; i++) {
      TableItem item = wFields.getNonEmpty(i);
      input.getArgumentField()[i] = item.getText(1);
      input.getArgumentParameter()[i] = item.getText(2);
    }

    if (log.isDebug()) {
      logDebug(
          BaseMessages.getString(PKG, "HTTPDialog.Log.FoundHeaders", String.valueOf(nrheaders)));
    }
    for (int i = 0; i < nrheaders; i++) {
      TableItem item = wHeaders.getNonEmpty(i);
      input.getHeaderField()[i] = item.getText(1);
      input.getHeaderParameter()[i] = item.getText(2);
    }

    input.setUrl(wUrl.getText());
    input.setUrlField(wUrlField.getText());
    input.setUrlInField(wUrlInField.getSelection());
    input.setIgnoreSsl(wIgnoreSsl.getSelection());
    input.setFieldName(wResult.getText());
    input.setEncoding(wEncoding.getText());
    input.setHttpLogin(wHttpLogin.getText());
    input.setHttpPassword(wHttpPassword.getText());
    input.setProxyHost(wProxyHost.getText());
    input.setProxyPort(wProxyPort.getText());
    input.setResultCodeFieldName(wResultCode.getText());
    input.setResponseTimeFieldName(wResponseTime.getText());
    input.setResponseHeaderFieldName(wResponseHeader.getText());
    input.setSocketTimeout(wSocketTimeOut.getText());
    input.setConnectionTimeout(wConnectionTimeOut.getText());
    input.setCloseIdleConnectionsTime(wCloseIdleConnectionsTime.getText());

    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1, 2}, new int[] {3}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HTTPDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "HTTPDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void getHeadersFields() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wHeaders, 1, new int[] {1, 2}, new int[] {3}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HTTPDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "HTTPDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }
}
