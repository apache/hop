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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.rest.RestConnection;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboItems;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
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

public class HttpPostDialog extends BaseTransformDialog {
  private static final Class<?> PKG = HttpPostMeta.class;

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

  private MetaSelectionLine<RestConnection> wConnection;

  private TextVar wProxyHost;

  private TextVar wProxyPort;

  private TextVar wProxyUsername;

  private TextVar wProxyPassword;

  private TextVar wNonProxyHosts;

  private final HttpPostMeta input;

  private final List<String> inputFields = new ArrayList<>();

  private ColumnInfo[] colinf;
  private ColumnInfo[] colinfquery;

  private String[] fieldNames;

  private boolean gotPreviousFields = false;

  private ComboVar wEncoding;
  private ComboVar wContentType;
  private Button wMultiPartUpload;

  private Button wPostAFile;

  private boolean gotEncodings = false;

  private TextVar wConnectionTimeOut;

  private TextVar wSocketTimeOut;

  private TextVar wCloseIdleConnectionsTime;

  public HttpPostDialog(
      Shell parent, IVariables variables, HttpPostMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "HTTPPOSTDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    setupButtons(margin);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    Composite wGeneralComp = addTab(wTabFolder, "HTTPPOSTDialog.GeneralTab.Title");

    // ////////////////////////
    // START Settings GROUP

    Group gSettings = setupSettingGroup(wGeneralComp);

    setupConnectionLine(gSettings);
    setupUrlLine(lsMod, gSettings);
    setupUrlInFieldLine(gSettings);
    setupUrlFieldNameLine(lsMod, gSettings);
    setupEncodingLine(lsMod, gSettings);
    setupContentTypeLine(lsMod, gSettings);
    setupRequestEntityLine(lsMod, gSettings);
    setupMultiPartUpload(gSettings);
    setupPostFileLine(gSettings);
    setupConnectionTimeoutLine(lsMod, gSettings);
    setupSocketTimeout(lsMod, gSettings);
    setupCloseWaitConnectionLine(lsMod, gSettings);

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment(0, 0);
    fdSettings.right = new FormAttachment(100, 0);
    fdSettings.top = new FormAttachment(0, margin);
    gSettings.setLayoutData(fdSettings);

    // END Settings GROUP
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

    finishTab(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF AUTHENTICATION TAB ///
    // ////////////////////////

    Composite wAuthComp = addTab(wTabFolder, "HTTPPOSTDialog.AuthenticationTab.Title");

    Group gHttpAuth = setupHttpAuthGroup(wAuthComp);
    setupHttpLoginLine(lsMod, gHttpAuth);
    setupHttpPasswordLine(lsMod, gHttpAuth);

    FormData fdHttpAuth = new FormData();
    fdHttpAuth.left = new FormAttachment(0, 0);
    fdHttpAuth.right = new FormAttachment(100, 0);
    fdHttpAuth.top = new FormAttachment(0, margin);
    gHttpAuth.setLayoutData(fdHttpAuth);

    finishTab(wAuthComp);

    // ///////////////////////////////////////////////////////////
    // / END OF AUTHENTICATION TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF PROXY TAB ///
    // ////////////////////////

    Composite wProxyComp = addTab(wTabFolder, "HTTPPOSTDialog.ProxyTab.Title");

    Group gProxy = setupProxyHostGroup(wProxyComp);
    setupProxyHost(lsMod, gProxy);
    setupProxyPort(lsMod, gProxy);
    setupProxyUsername(lsMod, gProxy);
    setupProxyPassword(lsMod, gProxy);
    setupNonProxyHosts(lsMod, gProxy);

    FormData fdProxy = new FormData();
    fdProxy.left = new FormAttachment(0, 0);
    fdProxy.right = new FormAttachment(100, 0);
    fdProxy.top = new FormAttachment(0, margin);
    gProxy.setLayoutData(fdProxy);

    finishTab(wProxyComp);

    // ///////////////////////////////////////////////////////////
    // / END OF PROXY TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF SSL TAB ///
    // ////////////////////////

    Composite wSslComp = addTab(wTabFolder, "HTTPPOSTDialog.SslTab.Title");

    Group gSsl = setupSslGroup(wSslComp);
    setupIgnoreSslLine(gSsl);

    FormData fdSsl = new FormData();
    fdSsl.left = new FormAttachment(0, 0);
    fdSsl.right = new FormAttachment(100, 0);
    fdSsl.top = new FormAttachment(0, margin);
    gSsl.setLayoutData(fdSsl);

    finishTab(wSslComp);

    // ///////////////////////////////////////////////////////////
    // / END OF SSL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF BODY PARAMETERS TAB ///
    // ////////////////////////

    Composite wBodyComp = addTab(wTabFolder, "HTTPPOSTDialog.BodyParametersTab.Title");
    setupBodyParamBlock(lsMod, wBodyComp);
    finishTab(wBodyComp);

    // ////////////////////////
    // START OF QUERY PARAMETERS TAB ///
    // ////////////////////////

    Composite wQueryComp = addTab(wTabFolder, "HTTPPOSTDialog.QueryParametersTab.Title");
    setupQueryParamBlock(lsMod, wQueryComp);
    finishTab(wQueryComp);

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
    BackgroundThreadFacade.start(runnable);
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
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
    focusTransformName();
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
    fdlQuery.top = new FormAttachment(0, margin);
    wlQuery.setLayoutData(fdlQuery);

    int queryRows = 0;
    if (input.getFirstLookupField().getQueryField() != null) {
      queryRows = input.getFirstLookupField().getQueryField().size();
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

  /**
   * A tab whose content scrolls: the options of one tab can outgrow a small dialog, and without a
   * scrolled composite the widgets below the fold are simply unreachable.
   */
  private Composite addTab(CTabFolder wTabFolder, String titleKey) {
    CTabItem tab = new CTabItem(wTabFolder, SWT.NONE);
    tab.setFont(GuiResource.getInstance().getFontDefault());
    tab.setText(BaseMessages.getString(PKG, titleKey));

    ScrolledComposite scrolled = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    scrolled.setLayout(new FillLayout());
    PropsUi.setLook(scrolled);

    Composite composite = new Composite(scrolled, SWT.NONE);
    PropsUi.setLook(composite);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    composite.setLayout(layout);

    tab.setControl(scrolled);
    return composite;
  }

  /** Sizes the scrolled content of a tab built by {@link #addTab}. */
  private void finishTab(Composite composite) {
    FormData fdComp = new FormData();
    fdComp.left = new FormAttachment(0, 0);
    fdComp.top = new FormAttachment(0, 0);
    fdComp.right = new FormAttachment(100, 0);
    fdComp.bottom = new FormAttachment(100, 0);
    composite.setLayoutData(fdComp);

    composite.layout();
    composite.pack();
    Rectangle bounds = composite.getBounds();

    ScrolledComposite scrolled = (ScrolledComposite) composite.getParent();
    scrolled.setContent(composite);
    scrolled.setExpandHorizontal(true);
    scrolled.setExpandVertical(true);
    scrolled.setMinWidth(bounds.width);
    scrolled.setMinHeight(bounds.height);
  }

  private Group setupSslGroup(Composite wSslComp) {
    Group gSsl = new Group(wSslComp, SWT.SHADOW_NONE);
    gSsl.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.SslGroup.Label"));
    FormLayout sslLayout = new FormLayout();
    sslLayout.marginWidth = 3;
    sslLayout.marginHeight = 3;
    gSsl.setLayout(sslLayout);
    PropsUi.setLook(gSsl);
    return gSsl;
  }

  private Button setupBodyParamBlock(ModifyListener lsMod, Composite wAdditionalComp) {
    int margin = PropsUi.getMargin();
    Label wlFields = new Label(wAdditionalComp, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.Parameters.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(0, margin);
    wlFields.setLayoutData(fdlFields);

    int fieldsRows = 0;
    if (input.getFirstLookupField().getArgumentField() != null) {
      fieldsRows = input.getFirstLookupField().getArgumentField().size();
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
    fdFields.bottom = new FormAttachment(100, -margin);
    wFields.setLayoutData(fdFields);
    return wGetBodyParam;
  }

  private void setupProxyPort(ModifyListener lsMod, Group gProxy) {
    // Proxy port
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
    Group gProxy = new Group(wGeneralComp, SWT.SHADOW_NONE);
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
    Group gHttpAuth = new Group(wGeneralComp, SWT.SHADOW_NONE);
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
    Group gOutputFields = new Group(wGeneralComp, SWT.SHADOW_NONE);
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
    fdlRequestEntity.top = new FormAttachment(wContentType, margin);
    wlRequestEntity.setLayoutData(fdlRequestEntity);

    wRequestEntity = new ComboVar(variables, gSettings, SWT.BORDER | SWT.READ_ONLY);
    wRequestEntity.setEditable(true);
    PropsUi.setLook(wRequestEntity);
    wRequestEntity.addModifyListener(lsMod);
    FormData fdRequestEntity = new FormData();
    fdRequestEntity.left = new FormAttachment(middle, 0);
    fdRequestEntity.top = new FormAttachment(wContentType, margin);
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

  private void setupContentTypeLine(ModifyListener lsMod, Group gSettings) {
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlContentType = new Label(gSettings, SWT.RIGHT);
    wlContentType.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.ContentType.Label"));
    wlContentType.setToolTipText(BaseMessages.getString(PKG, "HTTPPOSTDialog.ContentType.Tooltip"));
    PropsUi.setLook(wlContentType);
    FormData fdlContentType = new FormData();
    fdlContentType.left = new FormAttachment(0, 0);
    fdlContentType.top = new FormAttachment(wEncoding, margin);
    fdlContentType.right = new FormAttachment(middle, -margin);
    wlContentType.setLayoutData(fdlContentType);
    wContentType = new ComboVar(variables, gSettings, SWT.BORDER);
    wContentType.setToolTipText(BaseMessages.getString(PKG, "HTTPPOSTDialog.ContentType.Tooltip"));
    PropsUi.setLook(wContentType);
    wContentType.addModifyListener(lsMod);
    FormData fdContentType = new FormData();
    fdContentType.left = new FormAttachment(middle, 0);
    fdContentType.top = new FormAttachment(wEncoding, margin);
    fdContentType.right = new FormAttachment(100, -margin);
    wContentType.setLayoutData(fdContentType);
    wContentType.setItems(
        new String[] {
          "text/xml",
          "application/json",
          "application/xml",
          "text/plain",
          "application/x-www-form-urlencoded"
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
    fdlUrlField.top = new FormAttachment(wUrlInField, margin);
    wlUrlField.setLayoutData(fdlUrlField);

    wUrlField = new ComboVar(variables, gSettings, SWT.BORDER | SWT.READ_ONLY);
    wUrlField.setEditable(true);
    PropsUi.setLook(wUrlField);
    wUrlField.addModifyListener(lsMod);
    FormData fdUrlField = new FormData();
    fdUrlField.left = new FormAttachment(middle, 0);
    fdUrlField.top = new FormAttachment(wUrlInField, margin);
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

  private void setupIgnoreSslLine(Group gSsl) {
    // ignoreSsl line
    //
    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();
    Label wlIgnoreSsl = new Label(gSsl, SWT.RIGHT);
    wlIgnoreSsl.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.IgnoreSsl.Label"));
    PropsUi.setLook(wlIgnoreSsl);
    FormData fdlIgnoreSsl = new FormData();
    fdlIgnoreSsl.left = new FormAttachment(0, 0);
    fdlIgnoreSsl.top = new FormAttachment(0, margin);
    fdlIgnoreSsl.right = new FormAttachment(middle, -margin);
    wlIgnoreSsl.setLayoutData(fdlIgnoreSsl);
    wIgnoreSsl = new Button(gSsl, SWT.CHECK);
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
    fdlUrl.top = new FormAttachment(wConnection, margin);
    wlUrl.setLayoutData(fdlUrl);

    wUrl = new TextVar(variables, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUrl);
    wUrl.addModifyListener(lsMod);
    FormData fdUrl = new FormData();
    fdUrl.left = new FormAttachment(middle, 0);
    fdUrl.top = new FormAttachment(wConnection, margin);
    fdUrl.right = new FormAttachment(100, 0);
    wUrl.setLayoutData(fdUrl);
  }

  private void setupConnectionLine(Group gSettings) {
    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            RestConnection.class,
            gSettings,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "HTTPPOSTDialog.Connection.Label"),
            BaseMessages.getString(PKG, "HTTPPOSTDialog.Connection.Tooltip"));
    PropsUi.setLook(wConnection);
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.top = new FormAttachment(wSpacer, PropsUi.getMargin());
    fdConnection.right = new FormAttachment(100, 0);
    wConnection.setLayoutData(fdConnection);
    wConnection.addListener(SWT.Selection, e -> connectionChanged());
    wConnection.addModifyListener(e -> connectionChanged());
    try {
      wConnection.fillItems();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
          BaseMessages.getString(PKG, "HTTPPOSTDialog.Error.ListingConnections"),
          e);
    }
  }

  private void connectionChanged() {
    input.setChanged();
    activateConnectionSupersededFields();
  }

  /**
   * A selected REST connection supplies the whole client, so the transform's own authentication,
   * proxy and SSL fields stop being read. Grey them out rather than leave them looking as though
   * they still do something. The values are kept: deselecting the connection brings them back.
   */
  private void activateConnectionSupersededFields() {
    boolean editable = Utils.isEmpty(wConnection.getText());
    for (Control control :
        new Control[] {
          wHttpLogin,
          wHttpPassword,
          wProxyHost,
          wProxyPort,
          wProxyUsername,
          wProxyPassword,
          wNonProxyHosts,
          wIgnoreSsl,
          wConnectionTimeOut,
          wSocketTimeOut
        }) {
      if (control != null && !control.isDisposed()) {
        control.setEnabled(editable);
      }
    }
  }

  private void setupProxyUsername(ModifyListener lsMod, Group gProxy) {
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlProxyUsername = new Label(gProxy, SWT.RIGHT);
    wlProxyUsername.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.ProxyUsername.Label"));
    PropsUi.setLook(wlProxyUsername);
    FormData fdlProxyUsername = new FormData();
    fdlProxyUsername.top = new FormAttachment(wProxyPort, margin);
    fdlProxyUsername.left = new FormAttachment(0, 0);
    fdlProxyUsername.right = new FormAttachment(middle, -margin);
    wlProxyUsername.setLayoutData(fdlProxyUsername);
    wProxyUsername = new TextVar(variables, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wProxyUsername.addModifyListener(lsMod);
    wProxyUsername.setToolTipText(
        BaseMessages.getString(PKG, "HTTPPOSTDialog.ProxyUsername.Tooltip"));
    PropsUi.setLook(wProxyUsername);
    FormData fdProxyUsername = new FormData();
    fdProxyUsername.top = new FormAttachment(wProxyPort, margin);
    fdProxyUsername.left = new FormAttachment(middle, 0);
    fdProxyUsername.right = new FormAttachment(100, 0);
    wProxyUsername.setLayoutData(fdProxyUsername);
  }

  private void setupProxyPassword(ModifyListener lsMod, Group gProxy) {
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlProxyPassword = new Label(gProxy, SWT.RIGHT);
    wlProxyPassword.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.ProxyPassword.Label"));
    PropsUi.setLook(wlProxyPassword);
    FormData fdlProxyPassword = new FormData();
    fdlProxyPassword.top = new FormAttachment(wProxyUsername, margin);
    fdlProxyPassword.left = new FormAttachment(0, 0);
    fdlProxyPassword.right = new FormAttachment(middle, -margin);
    wlProxyPassword.setLayoutData(fdlProxyPassword);
    wProxyPassword = new PasswordTextVar(variables, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wProxyPassword.addModifyListener(lsMod);
    wProxyPassword.setToolTipText(
        BaseMessages.getString(PKG, "HTTPPOSTDialog.ProxyPassword.Tooltip"));
    PropsUi.setLook(wProxyPassword);
    FormData fdProxyPassword = new FormData();
    fdProxyPassword.top = new FormAttachment(wProxyUsername, margin);
    fdProxyPassword.left = new FormAttachment(middle, 0);
    fdProxyPassword.right = new FormAttachment(100, 0);
    wProxyPassword.setLayoutData(fdProxyPassword);
  }

  private void setupNonProxyHosts(ModifyListener lsMod, Group gProxy) {
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    Label wlNonProxyHosts = new Label(gProxy, SWT.RIGHT);
    wlNonProxyHosts.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.NonProxyHosts.Label"));
    PropsUi.setLook(wlNonProxyHosts);
    FormData fdlNonProxyHosts = new FormData();
    fdlNonProxyHosts.top = new FormAttachment(wProxyPassword, margin);
    fdlNonProxyHosts.left = new FormAttachment(0, 0);
    fdlNonProxyHosts.right = new FormAttachment(middle, -margin);
    wlNonProxyHosts.setLayoutData(fdlNonProxyHosts);
    wNonProxyHosts = new TextVar(variables, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wNonProxyHosts.addModifyListener(lsMod);
    wNonProxyHosts.setToolTipText(
        BaseMessages.getString(PKG, "HTTPPOSTDialog.NonProxyHosts.Tooltip"));
    PropsUi.setLook(wNonProxyHosts);
    FormData fdNonProxyHosts = new FormData();
    fdNonProxyHosts.top = new FormAttachment(wProxyPassword, margin);
    fdNonProxyHosts.left = new FormAttachment(middle, 0);
    fdNonProxyHosts.right = new FormAttachment(100, 0);
    wNonProxyHosts.setLayoutData(fdNonProxyHosts);
  }

  private Group setupSettingGroup(Composite wGeneralComp) {
    Group gSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    gSettings.setText(BaseMessages.getString(PKG, "HTTPPOSTDialog.SettingsGroup.Label"));
    FormLayout settingsLayout = new FormLayout();
    settingsLayout.marginWidth = 3;
    settingsLayout.marginHeight = 3;
    gSettings.setLayout(settingsLayout);
    PropsUi.setLook(gSettings);
    return gSettings;
  }

  private void setupButtons(int margin) {
    // THE BUTTONS
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
      // Don't rely on the background fetch: it may have failed or still be running.
      String[] names = ConstUi.sortFieldNames(previousFields().getFieldNames().clone());
      ComboItems.setItemsKeepingText(wUrlField, names);
      ComboItems.setItemsKeepingText(wRequestEntity, names);

      gotPreviousFields = true;
    }
  }

  private void setEncodings() {
    // Encoding of the text file:
    if (!gotEncodings) {
      gotEncodings = true;

      String encoding = wEncoding.getText();
      wEncoding.setItems(ConstUi.getEncodings());
      wEncoding.setText(Const.NVL(encoding, ""));
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

    HttpPostLookupField lookupField = input.getFirstLookupField();
    if (lookupField.getArgumentField() != null) {
      for (int i = 0; i < lookupField.getArgumentField().size(); i++) {
        TableItem item = wFields.table.getItem(i);
        if (lookupField.getArgumentField().get(i).getName() != null) {
          item.setText(1, lookupField.getArgumentField().get(i).getName());
        }
        if (lookupField.getArgumentField().get(i).getParameter() != null) {
          item.setText(2, lookupField.getArgumentField().get(i).getParameter());
        }
        item.setText(3, lookupField.getArgumentField().get(i).isHeader() ? YES : NO);
      }
    }
    if (lookupField.getQueryField() != null) {
      for (int i = 0; i < lookupField.getQueryField().size(); i++) {
        TableItem item = wQuery.table.getItem(i);
        if (lookupField.getQueryField().get(i).getName() != null) {
          item.setText(1, lookupField.getQueryField().get(i).getName());
        }
        if (lookupField.getQueryField().get(i).getParameter() != null) {
          item.setText(2, lookupField.getQueryField().get(i).getParameter());
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
    HttpPostResultField resultField = input.getFirstResultField();
    if (resultField.getName() != null) {
      wResult.setText(resultField.getName());
    }
    if (resultField.getCode() != null) {
      wResultCode.setText(resultField.getCode());
    }
    if (resultField.getResponseTimeFieldName() != null) {
      wResponseTime.setText(resultField.getResponseTimeFieldName());
    }
    if (input.getEncoding() != null) {
      wEncoding.setText(input.getEncoding());
    }
    if (input.getContentType() != null) {
      wContentType.setText(input.getContentType());
    }
    wPostAFile.setSelection(input.isPostAFile());
    wMultiPartUpload.setSelection(input.isMultipartupload());

    if (input.getHttpLogin() != null) {
      wHttpLogin.setText(input.getHttpLogin());
    }
    if (input.getHttpPassword() != null) {
      wHttpPassword.setText(input.getHttpPassword());
    }
    wConnection.setText(Const.NVL(input.getConnectionName(), ""));
    if (input.getProxyHost() != null) {
      wProxyHost.setText(input.getProxyHost());
    }
    if (input.getProxyPort() != null) {
      wProxyPort.setText(input.getProxyPort());
    }
    wProxyUsername.setText(Const.NVL(input.getProxyUsername(), ""));
    wProxyPassword.setText(Const.NVL(input.getProxyPassword(), ""));
    wNonProxyHosts.setText(Const.NVL(input.getNonProxyHosts(), ""));
    if (resultField.getResponseHeaderFieldName() != null) {
      wResponseHeader.setText(resultField.getResponseHeaderFieldName());
    }

    wSocketTimeOut.setText(Const.NVL(input.getSocketTimeout(), ""));
    wConnectionTimeOut.setText(Const.NVL(input.getConnectionTimeout(), ""));
    wCloseIdleConnectionsTime.setText(Const.NVL(input.getCloseIdleConnectionsTime(), ""));

    wFields.setRowNums();
    wFields.optWidth(true);
    activateConnectionSupersededFields();
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
    input.setContentType(wContentType.getText());
    input.setPostAFile(wPostAFile.getSelection());
    input.setMultipartupload(wMultiPartUpload.getSelection());
    input.setHttpLogin(wHttpLogin.getText());
    input.setHttpPassword(wHttpPassword.getText());
    input.setConnectionName(wConnection.getText());
    input.setProxyHost(wProxyHost.getText());
    input.setProxyPort(wProxyPort.getText());
    input.setProxyUsername(wProxyUsername.getText());
    input.setProxyPassword(wProxyPassword.getText());
    input.setNonProxyHosts(wNonProxyHosts.getText());
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
