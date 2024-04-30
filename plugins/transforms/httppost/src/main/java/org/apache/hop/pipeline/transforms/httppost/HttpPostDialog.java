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

package org.apache.hop.pipeline.transforms.httppost;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
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
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
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
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class HttpPostDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = HttpPostMeta.class; // For Translator

  private static final String[] YES_NO_COMBO =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  private static final String YES = BaseMessages.getString(PKG, "System.Combo.Yes");
  private static final String NO = BaseMessages.getString(PKG, "System.Combo.No");

  private Label wlUrl;
  private TextVar wUrl;

  private TextVar wResult;

  private TextVar wResultCode;

  private TextVar wResponseTime;
  private TextVar wResponseHeader;

  private TableView wFields;

  private TableView wQuery;

  private Button wUrlInField;

  private Button wIgnoreSsl;

  private Label wlUrlField;
  private ComboVar wUrlField;

  private ComboVar wRequestEntity;

  private TextVar wHttpLogin;

  private TextVar wHttpPassword;

  private TextVar wProxyHost;

  private TextVar wProxyPort;

  private final HttpPostMeta input;

  private final List<String> inputFields = new ArrayList<>();

  private ColumnInfo[] colinf;
  private ColumnInfo[] colinfquery;

  private String[] fieldNames;

  private boolean gotPreviousFields = false;

  private ComboVar wEncoding;
  private Button wMultiPartUpload;

  private Button wPostAFile;

  private boolean gotEncodings = false;

  private TextVar wConnectionTimeOut;

  private TextVar wSocketTimeOut;

  private TextVar wCloseIdleConnectionsTime;

  public HttpPostDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (HttpPostMeta) in;
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
    shell.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.Shell.Title"));

    int margin = PropsUi.getMargin();

    setupButtons(margin);
    setupTransformNameField(lsMod);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.GeneralTab.Title"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wGeneralComp.setLayout(fileLayout);

    // ////////////////////////
    // START Settings GROUP

    Group gSettings = setupSettingGroup(wGeneralComp);

    setupUrlLine(lsMod, gSettings);
    setupUrlInFieldLine(gSettings);
    setupIgnoreSslLine(gSettings);
    setupUrlFieldNameLine(lsMod, gSettings);
    setupEncodingLine(lsMod, gSettings);
    setupRequestEntityLine(lsMod, gSettings);
    setupMultiPartUpload(gSettings);
    setupPostFileLine(gSettings);
    setupConnectionTimeoutLine(lsMod, gSettings);
    setupSocketTimeout(lsMod, gSettings);
    setupCloseWaitConnectionLine(lsMod, gSettings);

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment(0, 0);
    fdSettings.right = new FormAttachment(100, 0);
    fdSettings.top = new FormAttachment(wTransformName, margin);
    gSettings.setLayoutData(fdSettings);

    // END Output Settings GROUP
    // ////////////////////////

    // ////////////////////////
    // START Output Fields GROUP

    Group gOutputFields = setupOutputFieldGroup(wGeneralComp);
    setupResultLine(lsMod, gOutputFields);
    setupStatusCodeLine(lsMod, gOutputFields);
    setupResponseTimeLine(lsMod, gOutputFields);
    setupResponseHeaderLine(lsMod, gOutputFields);

    FormData fdOutputFields = new FormData();
    fdOutputFields.left = new FormAttachment(0, 0);
    fdOutputFields.right = new FormAttachment(100, 0);
    fdOutputFields.top = new FormAttachment(gSettings, margin);
    gOutputFields.setLayoutData(fdOutputFields);

    // END Output Fields GROUP
    // ////////////////////////

    // ////////////////////////
    // START HTTP AUTH GROUP

    Group gHttpAuth = setupHttpAuthGroup(wGeneralComp);

    setupHttpLoginLine(lsMod, gHttpAuth);
    setupHttpPasswordLine(lsMod, gHttpAuth);

    FormData fdHttpAuth = new FormData();
    fdHttpAuth.left = new FormAttachment(0, 0);
    fdHttpAuth.right = new FormAttachment(100, 0);
    fdHttpAuth.top = new FormAttachment(gOutputFields, margin);
    gHttpAuth.setLayoutData(fdHttpAuth);

    // END HTTP AUTH GROUP
    // ////////////////////////

    // ////////////////////////
    // START PROXY GROUP

    Group gProxy = setupProxyHostGroup(wGeneralComp);
    setupProxyHost(lsMod, gProxy);
    setupProxyPort(lsMod, gProxy);

    FormData fdProxy = new FormData();
    fdProxy.left = new FormAttachment(0, 0);
    fdProxy.right = new FormAttachment(100, 0);
    fdProxy.top = new FormAttachment(gHttpAuth, margin);
    gProxy.setLayoutData(fdProxy);

    // END HTTP AUTH GROUP
    // ////////////////////////

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
    wAdditionalTab.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.FieldsTab.Title"));

    FormLayout addLayout = new FormLayout();
    addLayout.marginWidth = PropsUi.getFormMargin();
    addLayout.marginHeight = PropsUi.getFormMargin();

    Composite wAdditionalComp = new Composite(wTabFolder, SWT.NONE);
    wAdditionalComp.setLayout(addLayout);
    PropsUi.setLook(wAdditionalComp);

    setupBodyParamBlock(lsMod, gProxy, wAdditionalComp);
    setupQueryParamBlock(lsMod, wAdditionalComp);

    //
    // Search the fields in the background
    //

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

    // Add listeners
    wGet.addListener(SWT.Selection, e -> getQueryFields());

    lsResize =
        event -> {
          Point size = shell.getSize();
          wFields.setSize(size.x - 10, size.y - 50);
          wFields.table.setSize(size.x - 10, size.y - 50);
          wFields.redraw();
        };
    shell.addListener(SWT.Resize, lsResize);

    wTabFolder.setSelection(0);
    getData();
    activeUrlInfield();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void setupQueryParamBlock(ModifyListener lsMod, Composite wAdditionalComp) {
    int margin = PropsUi.getMargin();
    Label wlQuery = new Label(wAdditionalComp, SWT.NONE);
    wlQuery.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.QueryParameters.Label"));
    PropsUi.setLook(wlQuery);
    FormData fdlQuery = new FormData();
    fdlQuery.left = new FormAttachment(0, 0);
    fdlQuery.top = new FormAttachment(wFields, margin);
    wlQuery.setLayoutData(fdlQuery);

    int queryRows = 0;
    if (input.getLookupFields().get(0).getQueryField() != null) {
      queryRows = input.getLookupFields().get(0).getQueryField().size();
    }

    colinfquery =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "HTTPPOSTDialog.ColumnInfo.QueryName"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "HTTPPOSTDialog.ColumnInfo.QueryParameter"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };
    colinfquery[1].setUsingVariables(true);
    wQuery =
        new TableView(
            variables,
            wAdditionalComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinfquery,
            queryRows,
            lsMod,
            props);

    wGet = new Button(wAdditionalComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.GetFields.Button"));
    fdGet = new FormData();
    fdGet.top = new FormAttachment(wlQuery, margin);
    fdGet.right = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);

    FormData fdQuery = new FormData();
    fdQuery.left = new FormAttachment(0, 0);
    fdQuery.top = new FormAttachment(wlQuery, margin);
    fdQuery.right = new FormAttachment(wGet, -margin);
    fdQuery.bottom = new FormAttachment(100, -margin);
    wQuery.setLayoutData(fdQuery);
  }

  private Button setupBodyParamBlock(
      ModifyListener lsMod, Group gProxy, Composite wAdditionalComp) {
    int margin = PropsUi.getMargin();
    Label wlFields = new Label(wAdditionalComp, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.Parameters.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(gProxy, margin);
    wlFields.setLayoutData(fdlFields);

    int fieldsRows = 0;
    if (input.getLookupFields().get(0).getArgumentField() != null) {
      fieldsRows = input.getLookupFields().get(0).getArgumentField().size();
    }

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "HTTPPOSTDialog.ColumnInfo.Name"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "HTTPPOSTDialog.ColumnInfo.Parameter"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "HTTPPOSTDialog.ColumnInfo.Header"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              YES_NO_COMBO),
        };
    colinf[1].setUsingVariables(true);
    wFields =
        new TableView(
            variables,
            wAdditionalComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            fieldsRows,
            lsMod,
            props);

    Button wGetBodyParam = new Button(wAdditionalComp, SWT.PUSH);
    wGetBodyParam.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.GetFields.Button"));
    FormData fdGetBodyParam = new FormData();
    fdGetBodyParam.top = new FormAttachment(wlFields, margin);
    fdGetBodyParam.right = new FormAttachment(100, 0);
    wGetBodyParam.setLayoutData(fdGetBodyParam);
    wGetBodyParam.addListener(SWT.Selection, e -> get());

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(wGetBodyParam, -margin);
    fdFields.bottom = new FormAttachment(wlFields, 200);
    wFields.setLayoutData(fdFields);
    return wGetBodyParam;
  }

  private void setupProxyPort(ModifyListener lsMod, Group gProxy) {
    // Proxy port
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlProxyPort = new Label(gProxy, SWT.RIGHT);
    wlProxyPort.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.ProxyPort.Label"));
    PropsUi.setLook(wlProxyPort);
    FormData fdlProxyPort = new FormData();
    fdlProxyPort.top = new FormAttachment(wProxyHost, margin);
    fdlProxyPort.left = new FormAttachment(0, 0);
    fdlProxyPort.right = new FormAttachment(middle, -margin);
    wlProxyPort.setLayoutData(fdlProxyPort);
    wProxyPort = new TextVar(variables, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wProxyPort.addModifyListener(lsMod);
    wProxyPort.setToolTipText(BaseMessages.getString(PKG, "HTTPPOSTDialog.ProxyPort.Tooltip"));
    PropsUi.setLook(wProxyPort);
    FormData fdProxyPort = new FormData();
    fdProxyPort.top = new FormAttachment(wProxyHost, margin);
    fdProxyPort.left = new FormAttachment(middle, 0);
    fdProxyPort.right = new FormAttachment(100, 0);
    wProxyPort.setLayoutData(fdProxyPort);
  }

  private void setupProxyHost(ModifyListener lsMod, Group gProxy) {
    // Proxy host
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlProxyHost = new Label(gProxy, SWT.RIGHT);
    wlProxyHost.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.ProxyHost.Label"));
    PropsUi.setLook(wlProxyHost);
    FormData fdlProxyHost = new FormData();
    fdlProxyHost.top = new FormAttachment(0, margin);
    fdlProxyHost.left = new FormAttachment(0, 0);
    fdlProxyHost.right = new FormAttachment(middle, -margin);
    wlProxyHost.setLayoutData(fdlProxyHost);
    wProxyHost = new TextVar(variables, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wProxyHost.addModifyListener(lsMod);
    wProxyHost.setToolTipText(BaseMessages.getString(PKG, "HTTPPOSTDialog.ProxyHost.Tooltip"));
    PropsUi.setLook(wProxyHost);
    FormData fdProxyHost = new FormData();
    fdProxyHost.top = new FormAttachment(0, margin);
    fdProxyHost.left = new FormAttachment(middle, 0);
    fdProxyHost.right = new FormAttachment(100, 0);
    wProxyHost.setLayoutData(fdProxyHost);
  }

  private Group setupProxyHostGroup(Composite wGeneralComp) {
    Group gProxy = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN);
    gProxy.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.ProxyGroup.Label"));
    FormLayout proxyLayout = new FormLayout();
    proxyLayout.marginWidth = 3;
    proxyLayout.marginHeight = 3;
    gProxy.setLayout(proxyLayout);
    PropsUi.setLook(gProxy);
    return gProxy;
  }

  private void setupHttpPasswordLine(ModifyListener lsMod, Group gHttpAuth) {
    // HTTP Password
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlHttpPassword = new Label(gHttpAuth, SWT.RIGHT);
    wlHttpPassword.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.HttpPassword.Label"));
    PropsUi.setLook(wlHttpPassword);
    FormData fdlHttpPassword = new FormData();
    fdlHttpPassword.top = new FormAttachment(wHttpLogin, margin);
    fdlHttpPassword.left = new FormAttachment(0, 0);
    fdlHttpPassword.right = new FormAttachment(middle, -margin);
    wlHttpPassword.setLayoutData(fdlHttpPassword);
    wHttpPassword = new PasswordTextVar(variables, gHttpAuth, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHttpPassword.addModifyListener(lsMod);
    wHttpPassword.setToolTipText(
        BaseMessages.getString(PKG, "HTTPPOSTDialog.HttpPassword.Tooltip"));
    PropsUi.setLook(wHttpPassword);
    FormData fdHttpPassword = new FormData();
    fdHttpPassword.top = new FormAttachment(wHttpLogin, margin);
    fdHttpPassword.left = new FormAttachment(middle, 0);
    fdHttpPassword.right = new FormAttachment(100, 0);
    wHttpPassword.setLayoutData(fdHttpPassword);
  }

  private void setupHttpLoginLine(ModifyListener lsMod, Group gHttpAuth) {
    // HTTP Login
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlHttpLogin = new Label(gHttpAuth, SWT.RIGHT);
    wlHttpLogin.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.HttpLogin.Label"));
    PropsUi.setLook(wlHttpLogin);
    FormData fdlHttpLogin = new FormData();
    fdlHttpLogin.top = new FormAttachment(0, margin);
    fdlHttpLogin.left = new FormAttachment(0, 0);
    fdlHttpLogin.right = new FormAttachment(middle, -margin);
    wlHttpLogin.setLayoutData(fdlHttpLogin);
    wHttpLogin = new TextVar(variables, gHttpAuth, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHttpLogin.addModifyListener(lsMod);
    wHttpLogin.setToolTipText(BaseMessages.getString(PKG, "HTTPPOSTDialog.HttpLogin.Tooltip"));
    PropsUi.setLook(wHttpLogin);
    FormData fdHttpLogin = new FormData();
    fdHttpLogin.top = new FormAttachment(0, margin);
    fdHttpLogin.left = new FormAttachment(middle, 0);
    fdHttpLogin.right = new FormAttachment(100, 0);
    wHttpLogin.setLayoutData(fdHttpLogin);
  }

  private Group setupHttpAuthGroup(Composite wGeneralComp) {
    Group gHttpAuth = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN);
    gHttpAuth.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.HttpAuthGroup.Label"));
    FormLayout httpAuthLayout = new FormLayout();
    httpAuthLayout.marginWidth = 3;
    httpAuthLayout.marginHeight = 3;
    gHttpAuth.setLayout(httpAuthLayout);
    PropsUi.setLook(gHttpAuth);
    return gHttpAuth;
  }

  private void setupResponseHeaderLine(ModifyListener lsMod, Group gOutputFields) {
    // Response header line...
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlResponseHeader = new Label(gOutputFields, SWT.RIGHT);
    wlResponseHeader.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.ResponseHeader.Label"));
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
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlResponseTime = new Label(gOutputFields, SWT.RIGHT);
    wlResponseTime.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.ResponseTime.Label"));
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
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlResultCode = new Label(gOutputFields, SWT.RIGHT);
    wlResultCode.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.ResultCode.Label"));
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
    fdResultCode.right = new FormAttachment(100, -margin);
    wResultCode.setLayoutData(fdResultCode);
  }

  private void setupResultLine(ModifyListener lsMod, Group gOutputFields) {
    // Result line...
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlResult = new Label(gOutputFields, SWT.RIGHT);
    wlResult.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.Result.Label"));
    PropsUi.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment(0, 0);
    fdlResult.right = new FormAttachment(middle, -margin);
    fdlResult.top = new FormAttachment(wPostAFile, margin);
    wlResult.setLayoutData(fdlResult);
    wResult = new TextVar(variables, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wResult);
    wResult.addModifyListener(lsMod);
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment(middle, 0);
    fdResult.top = new FormAttachment(wPostAFile, margin);
    fdResult.right = new FormAttachment(100, -margin);
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

  private void setupCloseWaitConnectionLine(ModifyListener lsMod, Group gSettings) {
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlCloseIdleConnectionsTime = new Label(gSettings, SWT.RIGHT);
    wlCloseIdleConnectionsTime.setText(
        BaseMessages.getString(PKG, "HTTPPOSTDialog.CloseIdleConnectionsTime.Label"));
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
        BaseMessages.getString(PKG, "HTTPPOSTDialog.CloseIdleConnectionsTime.Tooltip"));
    PropsUi.setLook(wCloseIdleConnectionsTime);
    FormData fdCloseIdleConnectionsTime = new FormData();
    fdCloseIdleConnectionsTime.top = new FormAttachment(wSocketTimeOut, margin);
    fdCloseIdleConnectionsTime.left = new FormAttachment(middle, 0);
    fdCloseIdleConnectionsTime.right = new FormAttachment(100, 0);
    wCloseIdleConnectionsTime.setLayoutData(fdCloseIdleConnectionsTime);
  }

  private void setupSocketTimeout(ModifyListener lsMod, Group gSettings) {
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlSocketTimeOut = new Label(gSettings, SWT.RIGHT);
    wlSocketTimeOut.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.SocketTimeOut.Label"));
    PropsUi.setLook(wlSocketTimeOut);
    FormData fdlSocketTimeOut = new FormData();
    fdlSocketTimeOut.top = new FormAttachment(wConnectionTimeOut, margin);
    fdlSocketTimeOut.left = new FormAttachment(0, 0);
    fdlSocketTimeOut.right = new FormAttachment(middle, -margin);
    wlSocketTimeOut.setLayoutData(fdlSocketTimeOut);
    wSocketTimeOut = new TextVar(variables, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSocketTimeOut.addModifyListener(lsMod);
    wSocketTimeOut.setToolTipText(
        BaseMessages.getString(PKG, "HTTPPOSTDialog.SocketTimeOut.Tooltip"));
    PropsUi.setLook(wSocketTimeOut);
    FormData fdSocketTimeOut = new FormData();
    fdSocketTimeOut.top = new FormAttachment(wConnectionTimeOut, margin);
    fdSocketTimeOut.left = new FormAttachment(middle, 0);
    fdSocketTimeOut.right = new FormAttachment(100, 0);
    wSocketTimeOut.setLayoutData(fdSocketTimeOut);
  }

  private void setupConnectionTimeoutLine(ModifyListener lsMod, Group gSettings) {
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlConnectionTimeOut = new Label(gSettings, SWT.RIGHT);
    wlConnectionTimeOut.setText(
        BaseMessages.getString(PKG, "HTTPPOSTDialog.ConnectionTimeOut.Label"));
    PropsUi.setLook(wlConnectionTimeOut);
    FormData fdlConnectionTimeOut = new FormData();
    fdlConnectionTimeOut.top = new FormAttachment(wPostAFile, margin);
    fdlConnectionTimeOut.left = new FormAttachment(0, 0);
    fdlConnectionTimeOut.right = new FormAttachment(middle, -margin);
    wlConnectionTimeOut.setLayoutData(fdlConnectionTimeOut);
    wConnectionTimeOut = new TextVar(variables, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wConnectionTimeOut.addModifyListener(lsMod);
    wConnectionTimeOut.setToolTipText(
        BaseMessages.getString(PKG, "HTTPPOSTDialog.ConnectionTimeOut.Tooltip"));
    PropsUi.setLook(wConnectionTimeOut);
    FormData fdConnectionTimeOut = new FormData();
    fdConnectionTimeOut.top = new FormAttachment(wPostAFile, margin);
    fdConnectionTimeOut.left = new FormAttachment(middle, 0);
    fdConnectionTimeOut.right = new FormAttachment(100, 0);
    wConnectionTimeOut.setLayoutData(fdConnectionTimeOut);
  }

  private void setupMultiPartUpload(Group gSettings) {
    // MultiPart Upload?
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlMultiPartUpload = new Label(gSettings, SWT.RIGHT);
    wlMultiPartUpload.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.MultiPartUpload.Label"));
    PropsUi.setLook(wlMultiPartUpload);
    FormData fdlMultiPartUpload = new FormData();
    fdlMultiPartUpload.left = new FormAttachment(0, 0);
    fdlMultiPartUpload.right = new FormAttachment(middle, -margin);
    fdlMultiPartUpload.top = new FormAttachment(wRequestEntity, margin);
    wlMultiPartUpload.setLayoutData(fdlMultiPartUpload);
    wMultiPartUpload = new Button(gSettings, SWT.CHECK);
    wMultiPartUpload.setToolTipText(
        BaseMessages.getString(PKG, "HTTPPOSTDialog.MultiPartUpload.Tooltip"));
    PropsUi.setLook(wMultiPartUpload);
    FormData fdMultiPartUpload = new FormData();
    fdMultiPartUpload.left = new FormAttachment(middle, 0);
    fdMultiPartUpload.top = new FormAttachment(wlMultiPartUpload, 0, SWT.CENTER);
    fdMultiPartUpload.right = new FormAttachment(100, 0);
    wMultiPartUpload.setLayoutData(fdMultiPartUpload);
    wMultiPartUpload.addSelectionListener(new ComponentSelectionListener(input));
  }

  private void setupPostFileLine(Group gSettings) {
    // Post file?
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlPostAFile = new Label(gSettings, SWT.RIGHT);
    wlPostAFile.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.postAFile.Label"));
    PropsUi.setLook(wlPostAFile);
    FormData fdlPostAFile = new FormData();
    fdlPostAFile.left = new FormAttachment(0, 0);
    fdlPostAFile.right = new FormAttachment(middle, -margin);
    fdlPostAFile.top = new FormAttachment(wMultiPartUpload, margin);
    wlPostAFile.setLayoutData(fdlPostAFile);
    wPostAFile = new Button(gSettings, SWT.CHECK);
    wPostAFile.setToolTipText(BaseMessages.getString(PKG, "HTTPPOSTDialog.postAFile.Tooltip"));
    PropsUi.setLook(wPostAFile);
    FormData fdPostAFile = new FormData();
    fdPostAFile.left = new FormAttachment(middle, 0);
    fdPostAFile.top = new FormAttachment(wlPostAFile, 0, SWT.CENTER);
    fdPostAFile.right = new FormAttachment(100, 0);
    wPostAFile.setLayoutData(fdPostAFile);
    wPostAFile.addSelectionListener(new ComponentSelectionListener(input));
  }

  private void setupRequestEntityLine(ModifyListener lsMod, Group gSettings) {
    // requestEntity Line
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlRequestEntity = new Label(gSettings, SWT.RIGHT);
    wlRequestEntity.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.requestEntity.Label"));
    PropsUi.setLook(wlRequestEntity);
    FormData fdlRequestEntity = new FormData();
    fdlRequestEntity.left = new FormAttachment(0, 0);
    fdlRequestEntity.right = new FormAttachment(middle, -margin);
    fdlRequestEntity.top = new FormAttachment(wEncoding, margin);
    wlRequestEntity.setLayoutData(fdlRequestEntity);

    wRequestEntity = new ComboVar(variables, gSettings, SWT.BORDER | SWT.READ_ONLY);
    wRequestEntity.setEditable(true);
    PropsUi.setLook(wRequestEntity);
    wRequestEntity.addModifyListener(lsMod);
    FormData fdRequestEntity = new FormData();
    fdRequestEntity.left = new FormAttachment(middle, 0);
    fdRequestEntity.top = new FormAttachment(wEncoding, margin);
    fdRequestEntity.right = new FormAttachment(100, -margin);
    wRequestEntity.setLayoutData(fdRequestEntity);
    wRequestEntity.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable focuslost event
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            setStreamFields();
            shell.setCursor(null);
            busy.dispose();
          }
        });
  }

  private void setupEncodingLine(ModifyListener lsMod, Group gSettings) {
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlEncoding = new Label(gSettings, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.Encoding.Label"));
    PropsUi.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment(0, 0);
    fdlEncoding.top = new FormAttachment(wUrlField, margin);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new ComboVar(variables, gSettings, SWT.BORDER | SWT.READ_ONLY);
    wEncoding.setEditable(true);
    PropsUi.setLook(wEncoding);
    wEncoding.addModifyListener(lsMod);
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment(middle, 0);
    fdEncoding.top = new FormAttachment(wUrlField, margin);
    fdEncoding.right = new FormAttachment(100, -margin);
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable focuslost event
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            setEncodings();
            shell.setCursor(null);
            busy.dispose();
          }
        });
  }

  private void setupUrlFieldNameLine(ModifyListener lsMod, Group gSettings) {
    // UrlField Line
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    wlUrlField = new Label(gSettings, SWT.RIGHT);
    wlUrlField.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.UrlField.Label"));
    PropsUi.setLook(wlUrlField);
    FormData fdlUrlField = new FormData();
    fdlUrlField.left = new FormAttachment(0, 0);
    fdlUrlField.right = new FormAttachment(middle, -margin);
    fdlUrlField.top = new FormAttachment(wIgnoreSsl, margin);
    wlUrlField.setLayoutData(fdlUrlField);

    wUrlField = new ComboVar(variables, gSettings, SWT.BORDER | SWT.READ_ONLY);
    wUrlField.setEditable(true);
    PropsUi.setLook(wUrlField);
    wUrlField.addModifyListener(lsMod);
    FormData fdUrlField = new FormData();
    fdUrlField.left = new FormAttachment(middle, 0);
    fdUrlField.top = new FormAttachment(wIgnoreSsl, margin);
    fdUrlField.right = new FormAttachment(100, -margin);
    wUrlField.setLayoutData(fdUrlField);
    wUrlField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable focuslost event
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            setStreamFields();
            shell.setCursor(null);
            busy.dispose();
          }
        });
  }

  private void setupUrlInFieldLine(Group gSettings) {
    // UrlInField line
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlUrlInField = new Label(gSettings, SWT.RIGHT);
    wlUrlInField.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.UrlInField.Label"));
    PropsUi.setLook(wlUrlInField);
    FormData fdlUrlInField = new FormData();
    fdlUrlInField.left = new FormAttachment(0, 0);
    fdlUrlInField.top = new FormAttachment(wUrl, margin);
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
  }

  private void setupIgnoreSslLine(Group gSettings) {
    // ignoreSsl line
    //
    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();
    Label wlIgnoreSsl = new Label(gSettings, SWT.RIGHT);
    wlIgnoreSsl.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.IgnoreSsl.Label"));
    PropsUi.setLook(wlIgnoreSsl);
    FormData fdlIgnoreSsl = new FormData();
    fdlIgnoreSsl.left = new FormAttachment(0, 0);
    fdlIgnoreSsl.top = new FormAttachment(wUrlInField, margin);
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
  }

  private void setupUrlLine(ModifyListener lsMod, Group gSettings) {
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    wlUrl = new Label(gSettings, SWT.RIGHT);
    wlUrl.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.URL.Label"));
    PropsUi.setLook(wlUrl);
    FormData fdlUrl = new FormData();
    fdlUrl.left = new FormAttachment(0, 0);
    fdlUrl.right = new FormAttachment(middle, -margin);
    fdlUrl.top = new FormAttachment(wTransformName, margin);
    wlUrl.setLayoutData(fdlUrl);

    wUrl = new TextVar(variables, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUrl);
    wUrl.addModifyListener(lsMod);
    FormData fdUrl = new FormData();
    fdUrl.left = new FormAttachment(middle, 0);
    fdUrl.top = new FormAttachment(wTransformName, margin);
    fdUrl.right = new FormAttachment(100, 0);
    wUrl.setLayoutData(fdUrl);
  }

  private Group setupSettingGroup(Composite wGeneralComp) {
    Group gSettings = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN);
    gSettings.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.SettingsGroup.Label"));
    FormLayout settingsLayout = new FormLayout();
    settingsLayout.marginWidth = 3;
    settingsLayout.marginHeight = 3;
    gSettings.setLayout(settingsLayout);
    PropsUi.setLook(gSettings);
    return gSettings;
  }

  private void setupTransformNameField(ModifyListener lsMod) {
    // TransformName line
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.TransformName.Label"));
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
  }

  private void setupButtons(int margin) {
    // THE BUTTONS
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    fieldNames = ConstUi.sortFieldNames(inputFields);
    colinf[0].setComboValues(fieldNames);
    colinfquery[0].setComboValues(fieldNames);
  }

  private void setStreamFields() {
    if (!gotPreviousFields) {
      String urlfield = wUrlField.getText();
      wUrlField.removeAll();
      wUrlField.setItems(fieldNames);
      if (urlfield != null) {
        wUrlField.setText(urlfield);
      }

      String request = wRequestEntity.getText();
      wRequestEntity.removeAll();
      wRequestEntity.setItems(fieldNames);
      if (request != null) {
        wRequestEntity.setText(request);
      }

      gotPreviousFields = true;
    }
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

  private void activeUrlInfield() {
    wlUrlField.setEnabled(wUrlInField.getSelection());
    wUrlField.setEnabled(wUrlInField.getSelection());
    wlUrl.setEnabled(!wUrlInField.getSelection());
    wUrl.setEnabled(!wUrlInField.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "HTTPPOSTDialog.Log.GettingKeyInfo"));
    }

    if (input.getLookupFields().get(0).getArgumentField() != null) {
      for (int i = 0; i < input.getLookupFields().get(0).getArgumentField().size(); i++) {
        TableItem item = wFields.table.getItem(i);
        if (input.getLookupFields().get(0).getArgumentField().get(i).getName() != null) {
          item.setText(1, input.getLookupFields().get(0).getArgumentField().get(i).getName());
        }
        if (input.getLookupFields().get(0).getArgumentField().get(i).getParameter() != null) {
          item.setText(2, input.getLookupFields().get(0).getArgumentField().get(i).getParameter());
        }
        item.setText(
            3, (input.getLookupFields().get(0).getArgumentField().get(i).isHeader()) ? YES : NO);
      }
    }
    if (input.getLookupFields().get(0).getQueryField() != null) {
      for (int i = 0; i < input.getLookupFields().get(0).getQueryField().size(); i++) {
        TableItem item = wQuery.table.getItem(i);
        if (input.getLookupFields().get(0).getQueryField().get(i).getName() != null) {
          item.setText(1, input.getLookupFields().get(0).getQueryField().get(i).getName());
        }
        if (input.getLookupFields().get(0).getQueryField().get(i).getParameter() != null) {
          item.setText(2, input.getLookupFields().get(0).getQueryField().get(i).getParameter());
        }
      }
    }
    if (input.getUrl() != null) {
      wUrl.setText(input.getUrl());
    }
    wUrlInField.setSelection(input.isUrlInField());
    wIgnoreSsl.setSelection(input.isIgnoreSsl());
    if (input.getUrlField() != null) {
      wUrlField.setText(input.getUrlField());
    }
    if (input.getRequestEntity() != null) {
      wRequestEntity.setText(input.getRequestEntity());
    }
    if (input.getResultFields().get(0).getName() != null) {
      wResult.setText(input.getResultFields().get(0).getName());
    }
    if (input.getResultFields().get(0).getCode() != null) {
      wResultCode.setText(input.getResultFields().get(0).getCode());
    }
    if (input.getResultFields().get(0).getResponseTimeFieldName() != null) {
      wResponseTime.setText(input.getResultFields().get(0).getResponseTimeFieldName());
    }
    if (input.getEncoding() != null) {
      wEncoding.setText(input.getEncoding());
    }
    wPostAFile.setSelection(input.isPostAFile());
    wMultiPartUpload.setSelection(input.isMultipartupload());

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
    if (input.getResultFields().get(0).getResponseHeaderFieldName() != null) {
      wResponseHeader.setText(input.getResultFields().get(0).getResponseHeaderFieldName());
    }

    wSocketTimeOut.setText(Const.NVL(input.getSocketTimeout(), ""));
    wConnectionTimeOut.setText(Const.NVL(input.getConnectionTimeout(), ""));
    wCloseIdleConnectionsTime.setText(Const.NVL(input.getCloseIdleConnectionsTime(), ""));

    wFields.setRowNums();
    wFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    HttpPostLookupField loookupField = new HttpPostLookupField();
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    int nrargs = wFields.nrNonEmpty();

    if (log.isDebug()) {
      logDebug(
          BaseMessages.getString(PKG, "HTTPPOSTDialog.Log.FoundArguments", String.valueOf(nrargs)));
    }
    for (int i = 0; i < nrargs; i++) {
      TableItem item = wFields.getNonEmpty(i);
      HttpPostArgumentField argumentField =
          new HttpPostArgumentField(item.getText(1), item.getText(2), YES.equals(item.getText(3)));
      loookupField.getArgumentField().add(argumentField);
    }

    int nrqueryparams = wQuery.nrNonEmpty();

    if (log.isDebug()) {
      logDebug(
          BaseMessages.getString(
              PKG, "HTTPPOSTDialog.Log.FoundQueryParameters", String.valueOf(nrqueryparams)));
    }
    for (int i = 0; i < nrqueryparams; i++) {
      TableItem item = wQuery.getNonEmpty(i);
      input.getLookupFields().get(0).getQueryField().clear();
      HttpPostQuery httpPostQuery = new HttpPostQuery(item.getText(1), item.getText(2));
      loookupField.getQueryField().add(httpPostQuery);
    }

    List<HttpPostLookupField> listLookupField = new ArrayList<>();
    listLookupField.add(loookupField);
    input.setLookupFields(listLookupField);

    input.setUrl(wUrl.getText());
    input.setUrlField(wUrlField.getText());
    input.setRequestEntity(wRequestEntity.getText());
    input.setUrlInField(wUrlInField.getSelection());
    input.setIgnoreSsl(wIgnoreSsl.getSelection());

    HttpPostResultField httpPostResultField =
        new HttpPostResultField(
            wResultCode.getText(),
            wResult.getText(),
            wResponseTime.getText(),
            wResponseHeader.getText());

    List<HttpPostResultField> listHttpPostResultField = new ArrayList<>();
    listHttpPostResultField.add(httpPostResultField);
    input.setResultFields(listHttpPostResultField);

    input.setEncoding(wEncoding.getText());
    input.setPostAFile(wPostAFile.getSelection());
    input.setMultipartupload(wMultiPartUpload.getSelection());
    input.setHttpLogin(wHttpLogin.getText());
    input.setHttpPassword(wHttpPassword.getText());
    input.setProxyHost(wProxyHost.getText());
    input.setProxyPort(wProxyPort.getText());
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
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              tableItem.setText(3, NO); // default is "N"
              return true;
            };
        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1, 2}, null, -1, -1, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HTTPPOSTDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "HTTPPOSTDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void getQueryFields() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wQuery, 1, new int[] {1, 2}, new int[] {3}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HTTPPOSTDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "HTTPPOSTDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }
}
