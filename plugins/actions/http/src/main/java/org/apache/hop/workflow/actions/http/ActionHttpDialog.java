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

package org.apache.hop.workflow.actions.http;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.HttpProtocol;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
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

/**
 * This dialog allows you to edit the SQL action settings. (select the connection and the sql script
 * to be executed)
 */
public class ActionHttpDialog extends ActionDialog {
  private static final Class<?> PKG = ActionHttp.class;

  private static final String[] FILETYPES =
      new String[] {BaseMessages.getString(PKG, "ActionHTTP.Filetype.All")};

  private Text wName;

  private Label wlURL;

  private TextVar wURL;

  private Button wIgnoreSsl;

  private Button wRunEveryRow;

  private Label wlFieldURL;

  private TextVar wFieldURL;

  private Label wlFieldUpload;

  private TextVar wFieldUpload;

  private Label wlFieldTarget;

  private TextVar wFieldTarget;

  private Label wlTargetFile;

  private TextVar wTargetFile;

  private Button wbTargetFile;

  private Label wlAppend;

  private Button wAppend;

  private Label wlDateTimeAdded;

  private Button wDateTimeAdded;

  private Label wlTargetExt;

  private TextVar wTargetExt;

  private Label wlUploadFile;

  private TextVar wUploadFile;

  private Button wbUploadFile;

  private TextVar wUserName;

  private TextVar wPassword;

  private TextVar wProxyServer;

  private TextVar wProxyPort;

  private TextVar wNonProxyHosts;

  private TableView wHeaders;

  private Button wAddFilenameToResult;

  private ActionHttp action;

  private boolean changed;

  public ActionHttpDialog(
      Shell parent, ActionHttp action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionHTTP.Name.Default"));
    }
  }

  @Override
  public IAction open() {

    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionHTTP.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    Button wOk = setupButtons(margin);
    setupActionName(lsMod, middle, margin);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "ActionHTTP.Tab.General.Label"));
    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);
    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    setupUrlLine(lsMod, middle, margin, wGeneralComp);
    setupIgnoreSslLine(middle, margin, wGeneralComp);
    setupRunEveryRwoLine(middle, margin, wGeneralComp);
    setupUrlFieldLine(lsMod, middle, margin, wGeneralComp);
    setupUploadFileLine(lsMod, middle, margin, wGeneralComp);
    setupDestFileLine(lsMod, middle, margin, wGeneralComp);

    // ////////////////////////
    // START OF AuthenticationGROUP///
    // /
    Group wAuthentication = setupAuthGroup(wGeneralComp);

    setupUsernameLine(lsMod, middle, margin, wAuthentication);
    setupPasswordLine(lsMod, middle, margin, wAuthentication);
    setupProxyServerLine(lsMod, middle, margin, wAuthentication);
    setupProxyPortLine(lsMod, middle, margin, wAuthentication);
    setupIgnoreHostLine(lsMod, middle, margin, wAuthentication);

    FormData fdAuthentication = new FormData();
    fdAuthentication.left = new FormAttachment(0, margin);
    fdAuthentication.top = new FormAttachment(wFieldTarget, margin);
    fdAuthentication.right = new FormAttachment(100, -margin);
    wAuthentication.setLayoutData(fdAuthentication);
    // ///////////////////////////////////////////////////////////
    // / END OF AuthenticationGROUP GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF UpLoadFileGROUP///
    // /
    Group wUpLoadFile = setupUploadFileGroup(wGeneralComp);

    setupUploadFileLine(lsMod, middle, margin, wAuthentication, wUpLoadFile);

    FormData fdUpLoadFile = new FormData();
    fdUpLoadFile.left = new FormAttachment(0, margin);
    fdUpLoadFile.top = new FormAttachment(wAuthentication, margin);
    fdUpLoadFile.right = new FormAttachment(100, -margin);
    wUpLoadFile.setLayoutData(fdUpLoadFile);
    // ///////////////////////////////////////////////////////////
    // / END OF UpLoadFileGROUP GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF TargetFileGroupGROUP///
    // /
    Group wTargetFileGroup = setupWebServerReplyGroup(wGeneralComp);

    setupTargetFileLine(lsMod, middle, margin, wTargetFileGroup);
    setupAppendFileLine(middle, margin, wTargetFileGroup);
    setupAddDateTimeLine(middle, margin, wTargetFileGroup);
    setupTargetExtensionLine(lsMod, middle, margin, wTargetFileGroup);
    setupAddFilenameLine(middle, margin, wTargetFileGroup);

    FormData fdTargetFileGroup = new FormData();
    fdTargetFileGroup.left = new FormAttachment(0, margin);
    fdTargetFileGroup.top = new FormAttachment(wUpLoadFile, margin);
    fdTargetFileGroup.right = new FormAttachment(100, -margin);
    wTargetFileGroup.setLayoutData(fdTargetFileGroup);
    // ///////////////////////////////////////////////////////////
    // / END OF TargetFileGroupGROUP GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(wName, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Headers TAB ///
    // ////////////////////////

    CTabItem wHeadersTab = new CTabItem(wTabFolder, SWT.NONE);
    wHeadersTab.setFont(GuiResource.getInstance().getFontDefault());
    wHeadersTab.setText(BaseMessages.getString(PKG, "ActionHTTP.Tab.Headers.Label"));
    Composite wHeadersComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wHeadersComp);
    FormLayout headersLayout = new FormLayout();
    headersLayout.marginWidth = 3;
    headersLayout.marginHeight = 3;
    wHeadersComp.setLayout(headersLayout);

    setupHeaderTable(lsMod, margin, wHeadersTab, wHeadersComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Headers TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    getData();
    wTabFolder.setSelection(0);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void setupHeaderTable(
      ModifyListener lsMod, int margin, CTabItem wHeadersTab, Composite wHeadersComp) {
    int rows =
        action.getHeaders() == null
            ? 1
            : (action.getHeaders().isEmpty() ? 0 : action.getHeaders().size());

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionHTTP.ColumnInfo.Name"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              HttpProtocol.getRequestHeaders(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionHTTP.ColumnInfo.Value"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };
    colinf[0].setUsingVariables(true);
    colinf[1].setUsingVariables(true);

    wHeaders =
        new TableView(
            variables,
            wHeadersComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            rows,
            lsMod,
            props);

    FormData fdHeaders = new FormData();
    fdHeaders.left = new FormAttachment(0, margin);
    fdHeaders.top = new FormAttachment(wName, margin);
    fdHeaders.right = new FormAttachment(100, -margin);
    fdHeaders.bottom = new FormAttachment(100, -margin);
    wHeaders.setLayoutData(fdHeaders);

    FormData fdHeadersComp = new FormData();
    fdHeadersComp.left = new FormAttachment(0, 0);
    fdHeadersComp.top = new FormAttachment(0, 0);
    fdHeadersComp.right = new FormAttachment(100, 0);
    fdHeadersComp.bottom = new FormAttachment(100, 0);
    wHeadersComp.setLayoutData(fdHeadersComp);

    wHeadersComp.layout();
    wHeadersTab.setControl(wHeadersComp);
  }

  private void setupAddFilenameLine(int middle, int margin, Group wTargetFileGroup) {
    // Add filenames to result filenames...
    Label wlAddFilenameToResult = new Label(wTargetFileGroup, SWT.RIGHT);
    wlAddFilenameToResult.setText(
        BaseMessages.getString(PKG, "ActionHTTP.AddFilenameToResult.Label"));
    PropsUi.setLook(wlAddFilenameToResult);
    FormData fdlAddFilenameToResult = new FormData();
    fdlAddFilenameToResult.left = new FormAttachment(0, 0);
    fdlAddFilenameToResult.top = new FormAttachment(wTargetExt, margin);
    fdlAddFilenameToResult.right = new FormAttachment(middle, -margin);
    wlAddFilenameToResult.setLayoutData(fdlAddFilenameToResult);
    wAddFilenameToResult = new Button(wTargetFileGroup, SWT.CHECK);
    wAddFilenameToResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionHTTP.AddFilenameToResult.Tooltip"));
    PropsUi.setLook(wAddFilenameToResult);
    FormData fdAddFilenameToResult = new FormData();
    fdAddFilenameToResult.left = new FormAttachment(middle, 0);
    fdAddFilenameToResult.top = new FormAttachment(wlAddFilenameToResult, 0, SWT.CENTER);
    fdAddFilenameToResult.right = new FormAttachment(100, 0);
    wAddFilenameToResult.setLayoutData(fdAddFilenameToResult);
  }

  private void setupTargetExtensionLine(
      ModifyListener lsMod, int middle, int margin, Group wTargetFileGroup) {
    // TargetExt line
    wlTargetExt = new Label(wTargetFileGroup, SWT.RIGHT);
    wlTargetExt.setText(BaseMessages.getString(PKG, "ActionHTTP.TargetFileExt.Label"));
    PropsUi.setLook(wlTargetExt);
    FormData fdlTargetExt = new FormData();
    fdlTargetExt.left = new FormAttachment(0, 0);
    fdlTargetExt.top = new FormAttachment(wlDateTimeAdded, 2 * margin);
    fdlTargetExt.right = new FormAttachment(middle, -margin);
    wlTargetExt.setLayoutData(fdlTargetExt);
    wTargetExt = new TextVar(variables, wTargetFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTargetExt);
    wTargetExt.setToolTipText(BaseMessages.getString(PKG, "ActionHTTP.TargetFileExt.Tooltip"));
    wTargetExt.addModifyListener(lsMod);
    FormData fdTargetExt = new FormData();
    fdTargetExt.left = new FormAttachment(middle, 0);
    fdTargetExt.top = new FormAttachment(wlTargetExt, 0, SWT.CENTER);
    fdTargetExt.right = new FormAttachment(100, 0);
    wTargetExt.setLayoutData(fdTargetExt);
  }

  private void setupAddDateTimeLine(int middle, int margin, Group wTargetFileGroup) {
    // DateTimeAdded line
    wlDateTimeAdded = new Label(wTargetFileGroup, SWT.RIGHT);
    wlDateTimeAdded.setText(BaseMessages.getString(PKG, "ActionHTTP.TargetFilenameAddDate.Label"));
    PropsUi.setLook(wlDateTimeAdded);
    FormData fdlDateTimeAdded = new FormData();
    fdlDateTimeAdded.left = new FormAttachment(0, 0);
    fdlDateTimeAdded.top = new FormAttachment(wlAppend, 2 * margin);
    fdlDateTimeAdded.right = new FormAttachment(middle, -margin);
    wlDateTimeAdded.setLayoutData(fdlDateTimeAdded);
    wDateTimeAdded = new Button(wTargetFileGroup, SWT.CHECK);
    PropsUi.setLook(wDateTimeAdded);
    wDateTimeAdded.setToolTipText(
        BaseMessages.getString(PKG, "ActionHTTP.TargetFilenameAddDate.Tooltip"));
    FormData fdDateTimeAdded = new FormData();
    fdDateTimeAdded.left = new FormAttachment(middle, 0);
    fdDateTimeAdded.top = new FormAttachment(wlDateTimeAdded, 0, SWT.CENTER);
    fdDateTimeAdded.right = new FormAttachment(100, 0);
    wDateTimeAdded.setLayoutData(fdDateTimeAdded);
    wDateTimeAdded.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setFlags();
          }
        });
  }

  private void setupAppendFileLine(int middle, int margin, Group wTargetFileGroup) {
    // Append line
    wlAppend = new Label(wTargetFileGroup, SWT.RIGHT);
    wlAppend.setText(BaseMessages.getString(PKG, "ActionHTTP.TargetFileAppend.Label"));
    PropsUi.setLook(wlAppend);
    FormData fdlAppend = new FormData();
    fdlAppend.left = new FormAttachment(0, 0);
    fdlAppend.top = new FormAttachment(wTargetFile, margin);
    fdlAppend.right = new FormAttachment(middle, -margin);
    wlAppend.setLayoutData(fdlAppend);
    wAppend = new Button(wTargetFileGroup, SWT.CHECK);
    PropsUi.setLook(wAppend);
    wAppend.setToolTipText(BaseMessages.getString(PKG, "ActionHTTP.TargetFileAppend.Tooltip"));
    FormData fdAppend = new FormData();
    fdAppend.left = new FormAttachment(middle, 0);
    fdAppend.top = new FormAttachment(wlAppend, 0, SWT.CENTER);
    fdAppend.right = new FormAttachment(100, 0);
    wAppend.setLayoutData(fdAppend);
  }

  private void setupTargetFileLine(
      ModifyListener lsMod, int middle, int margin, Group wTargetFileGroup) {
    // TargetFile line
    wlTargetFile = new Label(wTargetFileGroup, SWT.RIGHT);
    wlTargetFile.setText(BaseMessages.getString(PKG, "ActionHTTP.TargetFile.Label"));
    PropsUi.setLook(wlTargetFile);
    FormData fdlTargetFile = new FormData();
    fdlTargetFile.left = new FormAttachment(0, 0);
    fdlTargetFile.top = new FormAttachment(wUploadFile, margin);
    fdlTargetFile.right = new FormAttachment(middle, -margin);
    wlTargetFile.setLayoutData(fdlTargetFile);

    wbTargetFile = new Button(wTargetFileGroup, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTargetFile);
    wbTargetFile.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbTargetFile = new FormData();
    fdbTargetFile.right = new FormAttachment(100, 0);
    fdbTargetFile.top = new FormAttachment(wUploadFile, margin);
    wbTargetFile.setLayoutData(fdbTargetFile);

    wTargetFile = new TextVar(variables, wTargetFileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTargetFile);
    wTargetFile.setToolTipText(BaseMessages.getString(PKG, "ActionHTTP.TargetFile.Tooltip"));
    wTargetFile.addModifyListener(lsMod);
    FormData fdTargetFile = new FormData();
    fdTargetFile.left = new FormAttachment(middle, 0);
    fdTargetFile.top = new FormAttachment(wUploadFile, margin);
    fdTargetFile.right = new FormAttachment(wbTargetFile, -margin);
    wTargetFile.setLayoutData(fdTargetFile);

    wbTargetFile.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wTargetFile, variables, new String[] {"*"}, FILETYPES, true));
  }

  private Group setupWebServerReplyGroup(Composite wGeneralComp) {
    Group wTargetFileGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wTargetFileGroup);
    wTargetFileGroup.setText(BaseMessages.getString(PKG, "ActionHTTP.TargetFileGroup.Group.Label"));

    FormLayout targetFileGroupgroupLayout = new FormLayout();
    targetFileGroupgroupLayout.marginWidth = 10;
    targetFileGroupgroupLayout.marginHeight = 10;
    wTargetFileGroup.setLayout(targetFileGroupgroupLayout);
    return wTargetFileGroup;
  }

  private void setupUploadFileLine(
      ModifyListener lsMod, int middle, int margin, Group wAuthentication, Group wUpLoadFile) {
    // UploadFile line
    wlUploadFile = new Label(wUpLoadFile, SWT.RIGHT);
    wlUploadFile.setText(BaseMessages.getString(PKG, "ActionHTTP.UploadFile.Label"));
    PropsUi.setLook(wlUploadFile);
    FormData fdlUploadFile = new FormData();
    fdlUploadFile.left = new FormAttachment(0, 0);
    fdlUploadFile.top = new FormAttachment(wAuthentication, margin);
    fdlUploadFile.right = new FormAttachment(middle, -margin);
    wlUploadFile.setLayoutData(fdlUploadFile);

    wbUploadFile = new Button(wUpLoadFile, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbUploadFile);
    wbUploadFile.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbUploadFile = new FormData();
    fdbUploadFile.right = new FormAttachment(100, 0);
    fdbUploadFile.top = new FormAttachment(wAuthentication, margin);
    wbUploadFile.setLayoutData(fdbUploadFile);

    wUploadFile = new TextVar(variables, wUpLoadFile, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUploadFile);
    wUploadFile.setToolTipText(BaseMessages.getString(PKG, "ActionHTTP.UploadFile.Tooltip"));
    wUploadFile.addModifyListener(lsMod);
    FormData fdUploadFile = new FormData();
    fdUploadFile.left = new FormAttachment(middle, 0);
    fdUploadFile.top = new FormAttachment(wAuthentication, margin);
    fdUploadFile.right = new FormAttachment(wbUploadFile, -margin);
    wUploadFile.setLayoutData(fdUploadFile);

    // Whenever something changes, set the tooltip to the expanded version:
    wUploadFile.addModifyListener(
        e -> wUploadFile.setToolTipText(variables.resolve(wUploadFile.getText())));

    wbUploadFile.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wUploadFile, variables, new String[] {"*"}, FILETYPES, true));
  }

  private Group setupUploadFileGroup(Composite wGeneralComp) {
    Group wUpLoadFile = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wUpLoadFile);
    wUpLoadFile.setText(BaseMessages.getString(PKG, "ActionHTTP.UpLoadFile.Group.Label"));

    FormLayout upLoadFilegroupLayout = new FormLayout();
    upLoadFilegroupLayout.marginWidth = 10;
    upLoadFilegroupLayout.marginHeight = 10;
    wUpLoadFile.setLayout(upLoadFilegroupLayout);
    return wUpLoadFile;
  }

  private void setupIgnoreHostLine(
      ModifyListener lsMod, int middle, int margin, Group wAuthentication) {
    // IgnoreHosts line
    Label wlNonProxyHosts = new Label(wAuthentication, SWT.RIGHT);
    wlNonProxyHosts.setText(BaseMessages.getString(PKG, "ActionHTTP.ProxyIgnoreRegexp.Label"));
    PropsUi.setLook(wlNonProxyHosts);
    FormData fdlNonProxyHosts = new FormData();
    fdlNonProxyHosts.left = new FormAttachment(0, 0);
    fdlNonProxyHosts.top = new FormAttachment(wProxyPort, margin);
    fdlNonProxyHosts.right = new FormAttachment(middle, -margin);
    wlNonProxyHosts.setLayoutData(fdlNonProxyHosts);
    wNonProxyHosts = new TextVar(variables, wAuthentication, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wNonProxyHosts);
    wNonProxyHosts.setToolTipText(
        BaseMessages.getString(PKG, "ActionHTTP.ProxyIgnoreRegexp.Tooltip"));
    wNonProxyHosts.addModifyListener(lsMod);
    FormData fdNonProxyHosts = new FormData();
    fdNonProxyHosts.left = new FormAttachment(middle, 0);
    fdNonProxyHosts.top = new FormAttachment(wProxyPort, margin);
    fdNonProxyHosts.right = new FormAttachment(100, 0);
    wNonProxyHosts.setLayoutData(fdNonProxyHosts);
  }

  private void setupProxyPortLine(
      ModifyListener lsMod, int middle, int margin, Group wAuthentication) {
    // ProxyPort line
    Label wlProxyPort = new Label(wAuthentication, SWT.RIGHT);
    wlProxyPort.setText(BaseMessages.getString(PKG, "ActionHTTP.ProxyPort.Label"));
    PropsUi.setLook(wlProxyPort);
    FormData fdlProxyPort = new FormData();
    fdlProxyPort.left = new FormAttachment(0, 0);
    fdlProxyPort.top = new FormAttachment(wProxyServer, margin);
    fdlProxyPort.right = new FormAttachment(middle, -margin);
    wlProxyPort.setLayoutData(fdlProxyPort);
    wProxyPort = new TextVar(variables, wAuthentication, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wProxyPort);
    wProxyPort.setToolTipText(BaseMessages.getString(PKG, "ActionHTTP.ProxyPort.Tooltip"));
    wProxyPort.addModifyListener(lsMod);
    FormData fdProxyPort = new FormData();
    fdProxyPort.left = new FormAttachment(middle, 0);
    fdProxyPort.top = new FormAttachment(wProxyServer, margin);
    fdProxyPort.right = new FormAttachment(100, 0);
    wProxyPort.setLayoutData(fdProxyPort);
  }

  private void setupProxyServerLine(
      ModifyListener lsMod, int middle, int margin, Group wAuthentication) {
    // ProxyServer line
    Label wlProxyServer = new Label(wAuthentication, SWT.RIGHT);
    wlProxyServer.setText(BaseMessages.getString(PKG, "ActionHTTP.ProxyHost.Label"));
    PropsUi.setLook(wlProxyServer);
    FormData fdlProxyServer = new FormData();
    fdlProxyServer.left = new FormAttachment(0, 0);
    fdlProxyServer.top = new FormAttachment(wPassword, 3 * margin);
    fdlProxyServer.right = new FormAttachment(middle, -margin);
    wlProxyServer.setLayoutData(fdlProxyServer);
    wProxyServer = new TextVar(variables, wAuthentication, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wProxyServer);
    wProxyServer.setToolTipText(BaseMessages.getString(PKG, "ActionHTTP.ProxyHost.Tooltip"));
    wProxyServer.addModifyListener(lsMod);
    FormData fdProxyServer = new FormData();
    fdProxyServer.left = new FormAttachment(middle, 0);
    fdProxyServer.top = new FormAttachment(wPassword, 3 * margin);
    fdProxyServer.right = new FormAttachment(100, 0);
    wProxyServer.setLayoutData(fdProxyServer);
  }

  private void setupPasswordLine(
      ModifyListener lsMod, int middle, int margin, Group wAuthentication) {
    // Password line
    Label wlPassword = new Label(wAuthentication, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "ActionHTTP.UploadPassword.Label"));
    PropsUi.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.top = new FormAttachment(wUserName, margin);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new PasswordTextVar(variables, wAuthentication, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPassword);
    wPassword.setToolTipText(BaseMessages.getString(PKG, "ActionHTTP.UploadPassword.Tooltip"));
    wPassword.addModifyListener(lsMod);
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.top = new FormAttachment(wUserName, margin);
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);
  }

  private void setupUsernameLine(
      ModifyListener lsMod, int middle, int margin, Group wAuthentication) {
    // UserName line
    Label wlUserName = new Label(wAuthentication, SWT.RIGHT);
    wlUserName.setText(BaseMessages.getString(PKG, "ActionHTTP.UploadUser.Label"));
    PropsUi.setLook(wlUserName);
    FormData fdlUserName = new FormData();
    fdlUserName.left = new FormAttachment(0, 0);
    fdlUserName.top = new FormAttachment(wFieldTarget, margin);
    fdlUserName.right = new FormAttachment(middle, -margin);
    wlUserName.setLayoutData(fdlUserName);
    wUserName = new TextVar(variables, wAuthentication, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUserName);
    wUserName.setToolTipText(BaseMessages.getString(PKG, "ActionHTTP.UploadUser.Tooltip"));
    wUserName.addModifyListener(lsMod);
    FormData fdUserName = new FormData();
    fdUserName.left = new FormAttachment(middle, 0);
    fdUserName.top = new FormAttachment(wFieldTarget, margin);
    fdUserName.right = new FormAttachment(100, 0);
    wUserName.setLayoutData(fdUserName);
  }

  private Group setupAuthGroup(Composite wGeneralComp) {
    Group wAuthentication = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wAuthentication);
    wAuthentication.setText(BaseMessages.getString(PKG, "ActionHTTP.Authentication.Group.Label"));

    FormLayout authenticationgroupLayout = new FormLayout();
    authenticationgroupLayout.marginWidth = 10;
    authenticationgroupLayout.marginHeight = 10;
    wAuthentication.setLayout(authenticationgroupLayout);
    return wAuthentication;
  }

  private void setupDestFileLine(
      ModifyListener lsMod, int middle, int margin, Composite wGeneralComp) {
    // FieldTarget line
    wlFieldTarget = new Label(wGeneralComp, SWT.RIGHT);
    wlFieldTarget.setText(BaseMessages.getString(PKG, "ActionHTTP.InputFieldDest.Label"));
    PropsUi.setLook(wlFieldTarget);
    FormData fdlFieldTarget = new FormData();
    fdlFieldTarget.left = new FormAttachment(0, 0);
    fdlFieldTarget.top = new FormAttachment(wFieldUpload, margin);
    fdlFieldTarget.right = new FormAttachment(middle, -margin);
    wlFieldTarget.setLayoutData(fdlFieldTarget);
    wFieldTarget = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFieldTarget);
    wFieldTarget.setToolTipText(BaseMessages.getString(PKG, "ActionHTTP.InputFieldDest.Tooltip"));
    wFieldTarget.addModifyListener(lsMod);
    FormData fdFieldTarget = new FormData();
    fdFieldTarget.left = new FormAttachment(middle, 0);
    fdFieldTarget.top = new FormAttachment(wFieldUpload, margin);
    fdFieldTarget.right = new FormAttachment(100, 0);
    wFieldTarget.setLayoutData(fdFieldTarget);
  }

  private void setupUploadFileLine(
      ModifyListener lsMod, int middle, int margin, Composite wGeneralComp) {
    // FieldUpload line
    wlFieldUpload = new Label(wGeneralComp, SWT.RIGHT);
    wlFieldUpload.setText(BaseMessages.getString(PKG, "ActionHTTP.InputFieldUpload.Label"));
    PropsUi.setLook(wlFieldUpload);
    FormData fdlFieldUpload = new FormData();
    fdlFieldUpload.left = new FormAttachment(0, 0);
    fdlFieldUpload.top = new FormAttachment(wFieldURL, margin);
    fdlFieldUpload.right = new FormAttachment(middle, -margin);
    wlFieldUpload.setLayoutData(fdlFieldUpload);
    wFieldUpload = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFieldUpload);
    wFieldUpload.setToolTipText(BaseMessages.getString(PKG, "ActionHTTP.InputFieldUpload.Tooltip"));
    wFieldUpload.addModifyListener(lsMod);
    FormData fdFieldUpload = new FormData();
    fdFieldUpload.left = new FormAttachment(middle, 0);
    fdFieldUpload.top = new FormAttachment(wFieldURL, margin);
    fdFieldUpload.right = new FormAttachment(100, 0);
    wFieldUpload.setLayoutData(fdFieldUpload);
  }

  private void setupUrlFieldLine(
      ModifyListener lsMod, int middle, int margin, Composite wGeneralComp) {
    // FieldURL line
    wlFieldURL = new Label(wGeneralComp, SWT.RIGHT);
    wlFieldURL.setText(BaseMessages.getString(PKG, "ActionHTTP.InputField.Label"));
    PropsUi.setLook(wlFieldURL);
    FormData fdlFieldURL = new FormData();
    fdlFieldURL.left = new FormAttachment(0, 0);
    fdlFieldURL.top = new FormAttachment(wRunEveryRow, 2 * margin);
    fdlFieldURL.right = new FormAttachment(middle, -margin);
    wlFieldURL.setLayoutData(fdlFieldURL);
    wFieldURL = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFieldURL);
    wFieldURL.setToolTipText(BaseMessages.getString(PKG, "ActionHTTP.InputField.Tooltip"));
    wFieldURL.addModifyListener(lsMod);
    FormData fdFieldURL = new FormData();
    fdFieldURL.left = new FormAttachment(middle, 0);
    fdFieldURL.top = new FormAttachment(wRunEveryRow, 2 * margin);
    fdFieldURL.right = new FormAttachment(100, 0);
    wFieldURL.setLayoutData(fdFieldURL);
  }

  private void setupRunEveryRwoLine(int middle, int margin, Composite wGeneralComp) {
    // RunEveryRow line
    Label wlRunEveryRow = new Label(wGeneralComp, SWT.RIGHT);
    wlRunEveryRow.setText(BaseMessages.getString(PKG, "ActionHTTP.RunForEveryRow.Label"));
    PropsUi.setLook(wlRunEveryRow);
    FormData fdlRunEveryRow = new FormData();
    fdlRunEveryRow.left = new FormAttachment(0, 0);
    fdlRunEveryRow.top = new FormAttachment(wIgnoreSsl, margin);
    fdlRunEveryRow.right = new FormAttachment(middle, -margin);
    wlRunEveryRow.setLayoutData(fdlRunEveryRow);
    wRunEveryRow = new Button(wGeneralComp, SWT.CHECK);
    wRunEveryRow.setToolTipText(BaseMessages.getString(PKG, "ActionHTTP.RunForEveryRow.Tooltip"));
    PropsUi.setLook(wRunEveryRow);
    FormData fdRunEveryRow = new FormData();
    fdRunEveryRow.left = new FormAttachment(middle, 0);
    fdRunEveryRow.top = new FormAttachment(wlRunEveryRow, 0, SWT.CENTER);
    fdRunEveryRow.right = new FormAttachment(100, 0);
    wRunEveryRow.setLayoutData(fdRunEveryRow);
    wRunEveryRow.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setFlags();
          }
        });
  }

  private Label setupIgnoreSslLine(int middle, int margin, Composite wGeneralComp) {
    // Ignore ssl
    Label wlIgnoreSsl = new Label(wGeneralComp, SWT.RIGHT);
    wlIgnoreSsl.setText(BaseMessages.getString(PKG, "ActionHTTP.IgnoreSsl.Label"));
    PropsUi.setLook(wlIgnoreSsl);
    FormData fdlIgnoreSsl = new FormData();
    fdlIgnoreSsl.left = new FormAttachment(0, 0);
    fdlIgnoreSsl.top = new FormAttachment(wURL, margin);
    fdlIgnoreSsl.right = new FormAttachment(middle, -margin);
    wlIgnoreSsl.setLayoutData(fdlIgnoreSsl);
    wIgnoreSsl = new Button(wGeneralComp, SWT.CHECK);
    wIgnoreSsl.setToolTipText(BaseMessages.getString(PKG, "ActionHTTP.IgnoreSsl.Tooltip"));
    PropsUi.setLook(wIgnoreSsl);
    FormData fdIgnoreSsl = new FormData();
    fdIgnoreSsl.left = new FormAttachment(middle, 0);
    fdIgnoreSsl.top = new FormAttachment(wlIgnoreSsl, 0, SWT.CENTER);
    fdIgnoreSsl.right = new FormAttachment(100, 0);
    wIgnoreSsl.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });
    wIgnoreSsl.setLayoutData(fdIgnoreSsl);

    return wlIgnoreSsl;
  }

  private void setupUrlLine(ModifyListener lsMod, int middle, int margin, Composite wGeneralComp) {
    // URL line
    wlURL = new Label(wGeneralComp, SWT.RIGHT);
    wlURL.setText(BaseMessages.getString(PKG, "ActionHTTP.URL.Label"));
    PropsUi.setLook(wlURL);
    FormData fdlURL = new FormData();
    fdlURL.left = new FormAttachment(0, 0);
    fdlURL.top = new FormAttachment(wName, 2 * margin);
    fdlURL.right = new FormAttachment(middle, -margin);
    wlURL.setLayoutData(fdlURL);
    wURL =
        new TextVar(
            variables,
            wGeneralComp,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionHTTP.URL.Tooltip"));
    PropsUi.setLook(wURL);
    wURL.addModifyListener(lsMod);
    FormData fdURL = new FormData();
    fdURL.left = new FormAttachment(middle, 0);
    fdURL.top = new FormAttachment(wName, 2 * margin);
    fdURL.right = new FormAttachment(100, 0);
    wURL.setLayoutData(fdURL);
  }

  private void setupActionName(ModifyListener lsMod, int middle, int margin) {
    // Action name line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionHTTP.Name.Label"));
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);
  }

  private Button setupButtons(int margin) {
    // Buttons go at the very bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);
    return wOk;
  }

  private void setFlags() {
    wlURL.setEnabled(!wRunEveryRow.getSelection());
    wURL.setEnabled(!wRunEveryRow.getSelection());
    wlFieldURL.setEnabled(wRunEveryRow.getSelection());
    wFieldURL.setEnabled(wRunEveryRow.getSelection());
    wlFieldUpload.setEnabled(wRunEveryRow.getSelection());
    wFieldUpload.setEnabled(wRunEveryRow.getSelection());
    wlFieldTarget.setEnabled(wRunEveryRow.getSelection());
    wFieldTarget.setEnabled(wRunEveryRow.getSelection());

    wlUploadFile.setEnabled(!wRunEveryRow.getSelection());
    wUploadFile.setEnabled(!wRunEveryRow.getSelection());
    wbUploadFile.setEnabled(!wRunEveryRow.getSelection());
    wlTargetFile.setEnabled(!wRunEveryRow.getSelection());
    wbTargetFile.setEnabled(!wRunEveryRow.getSelection());
    wTargetFile.setEnabled(!wRunEveryRow.getSelection());
    wlDateTimeAdded.setEnabled(!wRunEveryRow.getSelection());
    wDateTimeAdded.setEnabled(!wRunEveryRow.getSelection());
    wlAppend.setEnabled(wRunEveryRow.getSelection() ? false : !wDateTimeAdded.getSelection());
    wAppend.setEnabled(wRunEveryRow.getSelection() ? false : !wDateTimeAdded.getSelection());
    wlTargetExt.setEnabled(wRunEveryRow.getSelection() ? false : wDateTimeAdded.getSelection());
    wTargetExt.setEnabled(wRunEveryRow.getSelection() ? false : wDateTimeAdded.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));

    wURL.setText(Const.NVL(action.getUrl(), ""));
    wRunEveryRow.setSelection(action.isRunForEveryRow());
    wIgnoreSsl.setSelection(action.isIgnoreSsl());
    wFieldURL.setText(Const.NVL(action.getUrlFieldname(), ""));
    wFieldUpload.setText(Const.NVL(action.getUploadFieldname(), ""));
    wFieldTarget.setText(Const.NVL(action.getDestinationFieldname(), ""));
    wTargetFile.setText(Const.NVL(action.getTargetFilename(), ""));
    wAppend.setSelection(action.isFileAppended());
    wDateTimeAdded.setSelection(action.isDateTimeAdded());
    wTargetExt.setText(Const.NVL(action.getTargetFilenameExtension(), ""));

    wUploadFile.setText(Const.NVL(action.getUploadFilename(), ""));

    action.setDateTimeAdded(wDateTimeAdded.getSelection());
    action.setTargetFilenameExtension(wTargetExt.getText());

    wUserName.setText(Const.NVL(action.getUsername(), ""));
    wPassword.setText(Const.NVL(action.getPassword(), ""));

    wProxyServer.setText(Const.NVL(action.getProxyHostname(), ""));
    wProxyPort.setText(Const.NVL(action.getProxyPort(), ""));
    wNonProxyHosts.setText(Const.NVL(action.getNonProxyHosts(), ""));
    if (action.getHeaders() != null) {
      String[] headerNames = new String[action.getHeaders().size()];
      String[] headerValues = new String[action.getHeaders().size()];

      for (int i = 0; i < action.getHeaders().size(); i++) {
        headerNames[i] = action.getHeaders().get(i).getHeaderName();
        headerValues[i] = action.getHeaders().get(i).getHeaderValue();
      }

      if (headerNames != null) {
        for (int i = 0; i < headerNames.length; i++) {
          TableItem ti = wHeaders.table.getItem(i);
          if (headerNames[i] != null) {
            ti.setText(1, headerNames[i]);
          }
          if (headerValues[i] != null) {
            ti.setText(2, headerValues[i]);
          }
        }
        wHeaders.setRowNums();
        wHeaders.optWidth(true);
      }
    }

    wAddFilenameToResult.setSelection(action.isAddFilenameToResult());
    setFlags();

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    action.setName(wName.getText());
    action.setUrl(wURL.getText());
    action.setRunForEveryRow(wRunEveryRow.getSelection());
    action.setIgnoreSsl(wIgnoreSsl.getSelection());
    action.setUrlFieldname(wFieldURL.getText());
    action.setUploadFieldname(wFieldUpload.getText());
    action.setDestinationFieldname(wFieldTarget.getText());

    action.setUsername(wUserName.getText());
    action.setPassword(wPassword.getText());
    action.setProxyHostname(wProxyServer.getText());
    action.setProxyPort(wProxyPort.getText());
    action.setNonProxyHosts(wNonProxyHosts.getText());

    action.setUploadFilename(wUploadFile.getText());

    action.setTargetFilename(wRunEveryRow.getSelection() ? "" : wTargetFile.getText());
    action.setFileAppended(wRunEveryRow.getSelection() ? false : wAppend.getSelection());
    action.setDateTimeAdded(wRunEveryRow.getSelection() ? false : wDateTimeAdded.getSelection());
    action.setTargetFilenameExtension(wRunEveryRow.getSelection() ? "" : wTargetExt.getText());
    action.setAddFilenameToResult(wAddFilenameToResult.getSelection());
    List<ActionHttp.Header> headers = new ArrayList<>();
    for (int i = 0; i < wHeaders.nrNonEmpty(); i++) {
      String varname = wHeaders.getNonEmpty(i).getText(1);
      String varvalue = wHeaders.getNonEmpty(i).getText(2);
      ActionHttp.Header header = new ActionHttp.Header();

      if (varname != null && !varname.isEmpty()) {
        header.setHeaderName(varname);
        header.setHeaderValue(varvalue);
      }
      headers.add(header);
    }
    action.setHeaders(headers);
    dispose();
  }
}
